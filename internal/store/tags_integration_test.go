package store

import (
	"context"
	"errors"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"

	"github.com/thomas-maurice/cortex/internal/memory"
)

func tagsTestStore(t *testing.T) *Store {
	t.Helper()
	rest := os.Getenv("CORTEX_TEST_WEAVIATE_REST")
	grpc := os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST and CORTEX_TEST_WEAVIATE_GRPC to run this integration test")
	}
	ctx := context.Background()
	st, err := New(rest, grpc)
	require.NoError(t, err)
	st.SetMultiTenant(os.Getenv("CORTEX_TEST_MULTI_TENANT") != "")
	_ = st.DeleteClass(ctx)
	require.NoError(t, st.EnsureSchema(ctx))
	t.Cleanup(func() { _ = st.DeleteClass(ctx) })
	return st
}

// putTagged stores a memory plus one chunk (as the worker would) carrying tags.
func putTagged(t *testing.T, ts *TenantStore, ns string, tags []string, vec []float32) string {
	t.Helper()
	ctx := context.Background()
	rec := memory.Record{ID: uuid.NewString(), Text: "mem " + uuid.NewString(), Namespace: ns, Tags: tags, CreatedAt: time.Now().UTC()}
	require.NoError(t, ts.Upsert(ctx, rec, vec))
	require.NoError(t, ts.ReplaceChunks(ctx, rec, []ChunkVec{{Index: 0, Text: rec.Text, Vector: vec}}))
	return rec.ID
}

func sortedTags(t *testing.T, ts *TenantStore, id string) []string {
	t.Helper()
	rec, ok, err := ts.Get(context.Background(), id)
	require.NoError(t, err)
	require.True(t, ok)
	out := append([]string{}, rec.Tags...)
	sort.Strings(out)
	return out
}

func chunkTags(t *testing.T, ts *TenantStore, memoryID string) [][]string {
	t.Helper()
	var out [][]string
	require.NoError(t, ts.s.scanAll(context.Background(), memory.ChunkClassName, ts.t, []string{"memoryId", "tags"}, func(r graphql.SearchResult) {
		if propString(r.Properties, "memoryId") == memoryID {
			tg := append([]string{}, propStrings(r.Properties, "tags")...)
			sort.Strings(tg)
			out = append(out, tg)
		}
	}))
	return out
}

func statMap(t *testing.T, ts *TenantStore, ns string) map[string]TagStat {
	t.Helper()
	stats, err := ts.ListTags(context.Background(), ns)
	require.NoError(t, err)
	m := map[string]TagStat{}
	for _, s := range stats {
		m[s.Name] = s
	}
	return m
}

// TestTagOperations pins the tag admin surface against a real Weaviate (non-MT,
// or MT when CORTEX_TEST_MULTI_TENANT is set). Why each case matters:
//   - paging: counts and rewrites must not stop at one scan page (or the 10k cap),
//     or a big store silently keeps stale tags;
//   - chunks: search matches chunks by THEIR tags, so a rename that only touched
//     the memory would make tag-filtered search miss the new tag / hit the old;
//   - merge: renaming into an existing tag must dedup, not store the tag twice;
//   - delete removes the tag only, never the memory;
//   - scoping: a namespace-limited rename must not touch other namespaces;
//   - no re-embed: tags are metadata, the vector must be byte-identical.
func TestTagOperations(t *testing.T) {
	st := tagsTestStore(t)
	ctx := context.Background()
	old := scanPageSize
	scanPageSize = 3
	t.Cleanup(func() { scanPageSize = old })

	ts := st.Tenant("user-tags")
	vec := []float32{1, 0, 0, 0}

	// 8 memories (> 2 pages of 3): k8s on 5 (alpha 3 + beta 2), kubernetes on 2, both on 1.
	var k8sAlpha, k8sBeta []string
	for i := 0; i < 3; i++ {
		k8sAlpha = append(k8sAlpha, putTagged(t, ts, "alpha", []string{"k8s", "keep"}, vec))
	}
	for i := 0; i < 2; i++ {
		k8sBeta = append(k8sBeta, putTagged(t, ts, "beta", []string{"k8s"}, vec))
	}
	kubeOnly := putTagged(t, ts, "alpha", []string{"kubernetes"}, vec)
	both := putTagged(t, ts, "alpha", []string{"k8s", "kubernetes"}, vec)
	untagged := putTagged(t, ts, "alpha", nil, vec)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		n, err := ts.Count(ctx, "")
		require.NoError(c, err)
		assert.Equal(c, 8, n)
	}, 10*time.Second, 200*time.Millisecond)

	t.Run("ListTags counts across pages, optionally per namespace", func(t *testing.T) {
		m := statMap(t, ts, "")
		assert.Equal(t, 6, m["k8s"].MemoryCount)
		assert.Equal(t, 2, m["kubernetes"].MemoryCount)
		assert.Equal(t, 3, m["keep"].MemoryCount)
		assert.False(t, m["k8s"].LastActivity.IsZero())
		assert.Equal(t, 2, statMap(t, ts, "beta")["k8s"].MemoryCount)
		assert.NotContains(t, statMap(t, ts, "beta"), "keep")
		assert.Equal(t, 6, statMap(t, ts, "*")["k8s"].MemoryCount)
	})

	t.Run("namespace-scoped rename leaves other namespaces alone", func(t *testing.T) {
		n, err := ts.RenameTag(ctx, "k8s", "kube", "beta", nil)
		require.NoError(t, err)
		assert.Equal(t, 2, n)
		for _, id := range k8sBeta {
			assert.Equal(t, []string{"kube"}, sortedTags(t, ts, id))
		}
		for _, id := range k8sAlpha {
			assert.Equal(t, []string{"k8s", "keep"}, sortedTags(t, ts, id), "alpha must not be touched by a beta-scoped rename")
		}
		// restore for the next cases
		_, err = ts.RenameTag(ctx, "kube", "k8s", "beta", nil)
		require.NoError(t, err)
	})

	t.Run("rename moves memories AND chunks; search finds new tag only; vector untouched", func(t *testing.T) {
		before := vectorOf(t, ts, k8sAlpha[0])
		n, err := ts.RenameTag(ctx, "k8s", "kubernetes", "", nil)
		require.NoError(t, err)
		assert.Equal(t, 6, n, "all memories with k8s, across pages and namespaces")

		for _, id := range k8sAlpha {
			assert.Equal(t, []string{"keep", "kubernetes"}, sortedTags(t, ts, id))
			assert.Equal(t, [][]string{{"keep", "kubernetes"}}, chunkTags(t, ts, id), "chunk tags must follow the memory")
		}
		assert.Equal(t, []string{"kubernetes"}, sortedTags(t, ts, both), "merge: dedup when the target already exists")
		assert.Equal(t, [][]string{{"kubernetes"}}, chunkTags(t, ts, both))
		assert.Equal(t, []string{"kubernetes"}, sortedTags(t, ts, kubeOnly))
		assert.Empty(t, sortedTags(t, ts, untagged))

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			newHits, err := ts.Search(ctx, vec, SearchOpts{IncludeTags: []string{"kubernetes"}, Limit: 20})
			require.NoError(c, err)
			assert.Len(c, newHits, 8-1) // every memory but the untagged one
			oldHits, err := ts.Search(ctx, vec, SearchOpts{IncludeTags: []string{"k8s"}, Limit: 20})
			require.NoError(c, err)
			assert.Empty(c, oldHits, "old tag must no longer match")
		}, 10*time.Second, 200*time.Millisecond)

		assert.Equal(t, before, vectorOf(t, ts, k8sAlpha[0]), "tag rename must not re-embed")
		m := statMap(t, ts, "")
		assert.NotContains(t, m, "k8s")
		assert.Equal(t, 7, m["kubernetes"].MemoryCount)
	})

	t.Run("delete removes only the tag, keeps memories and other tags", func(t *testing.T) {
		n, err := ts.DeleteTag(ctx, "keep", "", nil)
		require.NoError(t, err)
		assert.Equal(t, 3, n)
		for _, id := range k8sAlpha {
			assert.Equal(t, []string{"kubernetes"}, sortedTags(t, ts, id))
			assert.Equal(t, [][]string{{"kubernetes"}}, chunkTags(t, ts, id))
		}
		c, err := ts.Count(ctx, "")
		require.NoError(t, err)
		assert.Equal(t, 8, c, "memories survive a tag delete")
		n, err = ts.DeleteTag(ctx, "kubernetes", "", nil)
		require.NoError(t, err)
		assert.Equal(t, 7, n)
		assert.Empty(t, statMap(t, ts, ""), "no tags left")
		assert.Equal(t, [][]string{{}}, chunkTags(t, ts, both))
	})
}

func vectorOf(t *testing.T, ts *TenantStore, id string) []float32 {
	t.Helper()
	g := ts.s.client.Data().ObjectsGetter().WithClassName(memory.ClassName).WithID(id).WithVector()
	if ts.t != "" {
		g = g.WithTenant(ts.t)
	}
	objs, err := g.Do(context.Background())
	require.NoError(t, err)
	require.Len(t, objs, 1)
	require.NotEmpty(t, objs[0].Vector)
	return append([]float32{}, objs[0].Vector...)
}

// TestTagOpsTenantIsolation: in MT mode another tenant's identically tagged
// memories must be invisible to list and untouched by rename/delete. Skipped in
// non-MT mode (there is only one tenant).
func TestTagOpsTenantIsolation(t *testing.T) {
	st := tagsTestStore(t)
	if !st.multiTenant {
		t.Skip("MT-only: set CORTEX_TEST_MULTI_TENANT=1")
	}
	ctx := context.Background()
	a, b := st.Tenant("user-a"), st.Tenant("user-b")
	vec := []float32{1, 0, 0, 0}
	ida := putTagged(t, a, "n", []string{"shared"}, vec)
	idb := putTagged(t, b, "n", []string{"shared"}, vec)

	assert.Equal(t, 1, statMap(t, a, "")["shared"].MemoryCount)
	n, err := a.RenameTag(ctx, "shared", "renamed", "", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, []string{"renamed"}, sortedTags(t, a, ida))
	assert.Equal(t, []string{"shared"}, sortedTags(t, b, idb), "other tenant must be untouched")
	assert.Equal(t, [][]string{{"shared"}}, chunkTags(t, b, idb))
	n, err = a.DeleteTag(ctx, "shared", "", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.Equal(t, []string{"shared"}, sortedTags(t, b, idb))

	// A tenant that never wrote anything has no tags and renames nothing.
	ghost := st.Tenant("user-ghost")
	stats, err := ghost.ListTags(ctx, "")
	require.NoError(t, err)
	assert.Empty(t, stats)
	n, err = ghost.RenameTag(ctx, "x", "y", "", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

// TestRenameTagReadsUnderLock pins the concurrency design: the new tag set is
// computed from a read taken AFTER the per-memory lock is held, never from the
// scan. The locker below plays an index commit that lands between the scan and
// the lock acquisition (it adds the tag "late", as the worker's commit would
// while holding the lock); a rename built from the stale scan would erase it.
func TestRenameTagReadsUnderLock(t *testing.T) {
	st := tagsTestStore(t)
	ctx := context.Background()
	ts := st.Tenant("user-race")
	vec := []float32{1, 0, 0, 0}
	id := putTagged(t, ts, "n", []string{"old"}, vec)

	var locked []string
	lock := func(ctx context.Context, memID string) (context.Context, func(), error) {
		locked = append(locked, memID)
		rec, _, err := ts.Get(ctx, memID)
		require.NoError(t, err)
		rec.Tags = append(rec.Tags, "late")
		require.NoError(t, ts.UpdateIndexFields(ctx, rec, vec))
		return ctx, func() {}, nil
	}
	n, err := ts.RenameTag(ctx, "old", "new", "", lock)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, []string{id}, locked, "every rewritten memory is locked first")
	assert.Equal(t, []string{"late", "new"}, sortedTags(t, ts, id), "the concurrent index write must survive the rename")

	// And a memory whose tag vanished between scan and lock is skipped, not recreated.
	id2 := putTagged(t, ts, "n", []string{"gone"}, vec)
	lock2 := func(ctx context.Context, memID string) (context.Context, func(), error) {
		rec, _, err := ts.Get(ctx, memID)
		require.NoError(t, err)
		rec.Tags = []string{"other"}
		require.NoError(t, ts.UpdateIndexFields(ctx, rec, vec))
		return ctx, func() {}, nil
	}
	n, err = ts.DeleteTag(ctx, "gone", "", lock2)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.Equal(t, []string{"other"}, sortedTags(t, ts, id2))
}

// TestRenameTagChunkFailureIsRepairable: chunks are updated before the memory, so
// a failed chunk merge leaves the memory still carrying the old tag and a retry
// repairs everything. (The reverse order would make the retry skip the memory and
// leave chunk tags stale forever.) Also pins that the partial count is returned.
func TestRenameTagChunkFailureIsRepairable(t *testing.T) {
	st := tagsTestStore(t)
	ctx := context.Background()
	ts := st.Tenant("user-partial")
	vec := []float32{1, 0, 0, 0}
	first := putTagged(t, ts, "n", []string{"old"}, vec)
	second := putTagged(t, ts, "n", []string{"old"}, vec)

	failed := false
	t.Cleanup(func() { mergeTagsHook = nil })
	chunkMerges := 0
	mergeTagsHook = func(className, id string) error {
		if className != memory.ChunkClassName {
			return nil
		}
		chunkMerges++
		if chunkMerges == 2 && !failed { // second memory's chunk
			failed = true
			return errors.New("injected chunk merge failure")
		}
		return nil
	}

	n, err := ts.RenameTag(ctx, "old", "new", "", nil)
	require.Error(t, err)
	assert.Equal(t, 1, n, "the count of memories already changed is returned with the error")
	var done, pending string
	if got := sortedTags(t, ts, first); len(got) == 1 && got[0] == "new" {
		done, pending = first, second
	} else {
		done, pending = second, first
	}
	assert.Equal(t, []string{"new"}, sortedTags(t, ts, done))
	assert.Equal(t, []string{"old"}, sortedTags(t, ts, pending), "memory must keep the old tag when its chunk merge failed, so a retry finds it")

	n, err = ts.RenameTag(ctx, "old", "new", "", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	for _, id := range []string{first, second} {
		assert.Equal(t, []string{"new"}, sortedTags(t, ts, id))
		assert.Equal(t, [][]string{{"new"}}, chunkTags(t, ts, id), "chunk tags consistent after the retry")
	}
}

// TestTagOpsMatchExactly: tags are compared as stored. Deleting "k8s " (trailing
// space, e.g. a legacy write) must not delete "k8s" — silently widening the match
// would destroy tags the user never selected.
func TestTagOpsMatchExactly(t *testing.T) {
	st := tagsTestStore(t)
	ctx := context.Background()
	ts := st.Tenant("user-exact")
	vec := []float32{1, 0, 0, 0}
	plain := putTagged(t, ts, "n", []string{"k8s"}, vec)
	spaced := putTagged(t, ts, "n", []string{"k8s "}, vec)

	m := statMap(t, ts, "")
	assert.Equal(t, 1, m["k8s"].MemoryCount)
	assert.Equal(t, 1, m["k8s "].MemoryCount, "look-alike is a separate row")

	n, err := ts.DeleteTag(ctx, "k8s ", "", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, []string{"k8s"}, sortedTags(t, ts, plain))
	assert.Empty(t, sortedTags(t, ts, spaced))

	n, err = ts.RenameTag(ctx, "k8s", "kubernetes", "", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

// Restore republishes the tags stored on the Trash entry, so a rename/delete that
// skipped the trash would resurrect the old tag (or a deleted one such as
// `preference`) the moment a memory is restored. Also pins that the namespace
// scope and exact matching apply to trash entries like they do to memories.
func TestTagOpsReachTrashEntries(t *testing.T) {
	st, _ := trashStore(t)
	ctx := context.Background()
	ts := st.Tenant("tenant-a")

	trashed := func(ns string, tags []string) string {
		id := putTagged(t, ts, ns, tags, trashVec)
		require.NoError(t, ts.Delete(ctx, id))
		return id
	}
	trashTags := func(id string) []string {
		e, ok, err := ts.GetTrash(ctx, id)
		require.NoError(t, err)
		require.True(t, ok)
		out := append([]string{}, e.Tags...)
		sort.Strings(out)
		return out
	}

	inNS := trashed("ns", []string{"k8s", "keep", "kubernetes"})
	otherNS := trashed("other", []string{"k8s"})
	exact := trashed("ns", []string{"K8s"})
	live := putTagged(t, ts, "ns", []string{"k8s"}, trashVec)

	n, err := ts.RenameTag(ctx, "k8s", "kubernetes", "ns", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "return value counts live memories only")
	assert.Equal(t, []string{"keep", "kubernetes"}, trashTags(inNS), "renamed and merged (deduped) in the trash")
	assert.Equal(t, []string{"k8s"}, trashTags(otherNS), "namespace scope applies to the trash")
	assert.Equal(t, []string{"K8s"}, trashTags(exact), "matching is exact in the trash too")
	assert.Equal(t, []string{"kubernetes"}, sortedTags(t, ts, live))

	pref := trashed("ns", []string{"preference", "x"})
	_, err = ts.DeleteTag(ctx, "preference", "*", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"x"}, trashTags(pref), "deleted tag must not come back on restore")
}
