package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/thomas-maurice/cortex/internal/memory"
)

func expRec(ns, text string, validUntil time.Time) memory.Record {
	return memory.Record{
		ID: uuid.NewString(), Text: text, Namespace: ns, Source: "s",
		CreatedAt: time.Now().UTC().Add(-time.Hour).Truncate(time.Second), ValidUntil: validUntil.UTC().Truncate(time.Second),
	}
}

func idsOf(hits []memory.Hit) []string {
	out := make([]string, 0, len(hits))
	for _, h := range hits {
		out = append(out, h.ID)
	}
	return out
}

func recIDs(recs []memory.Record) []string {
	out := make([]string, 0, len(recs))
	for _, r := range recs {
		out = append(out, r.ID)
	}
	return out
}

// An expired memory is a fact that has stopped being true, so recall must not
// serve it — on EVERY retrieval path (chunk search, the whole-memory fallback for
// chunkless memories, the whole-memory vector search, and list) — unless the
// caller explicitly asks. Memories with no expiry (including ones written without
// the property at all) must be unaffected: the filter is a complement, not a
// "validUntil > now" that would silently drop null.
func TestExpiryExcludedFromSearchAndList(t *testing.T) {
	st, _ := trashStore(t)
	ctx := context.Background()
	ts := st.Tenant("tenant-a")
	const ns = "exp"
	vec := []float32{1, 0, 0, 0}
	now := time.Now()

	expiredChunked := expRec(ns, "expired chunked", now.Add(-time.Hour))
	expiredBare := expRec(ns, "expired no chunks", now.Add(-time.Hour)) // exercises the chunk-search fallback
	future := expRec(ns, "valid for a day", now.Add(24*time.Hour))
	never := expRec(ns, "never expires", time.Time{})
	for _, r := range []memory.Record{expiredChunked, expiredBare, future, never} {
		require.NoError(t, ts.Upsert(ctx, r, vec))
	}
	for _, r := range []memory.Record{expiredChunked, future, never} {
		require.NoError(t, ts.ReplaceChunks(ctx, r, []ChunkVec{{Index: 0, Text: r.Text, Vector: vec}}))
	}

	live := []string{future.ID, never.ID}
	all := append([]string{expiredChunked.ID, expiredBare.ID}, live...)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		hits, err := ts.Search(ctx, vec, SearchOpts{Namespace: ns, Limit: 10, IncludeExpired: true})
		require.NoError(c, err)
		assert.ElementsMatch(c, all, idsOf(hits))
	}, 10*time.Second, 200*time.Millisecond)

	t.Run("chunk search (with whole-memory fallback) hides expired by default", func(t *testing.T) {
		hits, err := ts.Search(ctx, vec, SearchOpts{Namespace: ns, Limit: 10})
		require.NoError(t, err)
		assert.ElementsMatch(t, live, idsOf(hits))
	})
	t.Run("whole-memory vector search hides expired by default", func(t *testing.T) {
		hits, err := ts.SearchMemoryVectors(ctx, vec, SearchOpts{Namespace: ns, Limit: 10})
		require.NoError(t, err)
		assert.ElementsMatch(t, live, idsOf(hits))
		hits, err = ts.SearchMemoryVectors(ctx, vec, SearchOpts{Namespace: ns, Limit: 10, IncludeExpired: true})
		require.NoError(t, err)
		assert.ElementsMatch(t, all, idsOf(hits), "include_expired (and the worker's dedup scan) must see expired memories")
	})
	t.Run("hybrid query keeps the filter", func(t *testing.T) {
		hits, err := ts.Search(ctx, vec, SearchOpts{Namespace: ns, Limit: 10, Query: "memory"})
		require.NoError(t, err)
		for _, h := range hits {
			assert.NotContains(t, []string{expiredChunked.ID, expiredBare.ID}, h.ID)
		}
	})
	t.Run("list hides expired by default and returns them on request", func(t *testing.T) {
		recs, err := ts.List(ctx, ListOpts{Namespace: ns, Limit: 10})
		require.NoError(t, err)
		assert.ElementsMatch(t, live, recIDs(recs))
		recs, err = ts.List(ctx, ListOpts{Namespace: ns, Limit: 10, IncludeExpired: true})
		require.NoError(t, err)
		assert.ElementsMatch(t, all, recIDs(recs))
	})
	t.Run("find-similar hides expired by default", func(t *testing.T) {
		hits, err := ts.SearchByID(ctx, never.ID, SearchOpts{Namespace: ns, Limit: 10})
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{future.ID}, idsOf(hits))
	})
	t.Run("expiry round-trips through Get and List", func(t *testing.T) {
		got, ok, err := ts.Get(ctx, future.ID)
		require.NoError(t, err)
		require.True(t, ok)
		assert.True(t, future.ValidUntil.Equal(got.ValidUntil), "got %v want %v", got.ValidUntil, future.ValidUntil)
		got, _, err = ts.Get(ctx, never.ID)
		require.NoError(t, err)
		assert.True(t, got.ValidUntil.IsZero())
	})
}

// A deployment upgrading to this version has a Memory class WITHOUT validUntil and
// memories that therefore have no value for it. After EnsureSchema adds the
// property those memories must still be returned (no backfill needed) — this is
// why the filter is NOT(validUntil <= now) and not validUntil > now.
func TestExpiryFilterKeepsPreExistingMemories(t *testing.T) {
	st, _ := trashStore(t)
	tenant := "tenant-a"
	ctx := context.Background()
	require.NoError(t, st.DeleteAllClasses(ctx))
	for _, c := range []*models.Class{memoryClass(st.multiTenant), chunkClass(st.multiTenant), trashClass(st.multiTenant)} {
		var old []*models.Property
		for _, p := range c.Properties {
			if p.Name != "validUntil" {
				old = append(old, p)
			}
		}
		c.Properties = old
		require.NoError(t, st.client.Schema().ClassCreator().WithClass(c).Do(ctx))
	}
	ts := st.Tenant(tenant)
	vec := []float32{1, 0, 0, 0}
	old := expRec("ns", "written before expiry existed", time.Time{})
	require.NoError(t, ts.Upsert(ctx, old, vec))

	require.NoError(t, st.EnsureSchema(ctx), "upgrade adds validUntil to existing classes")

	hits, err := ts.SearchMemoryVectors(ctx, vec, SearchOpts{Namespace: "ns", Limit: 5})
	require.NoError(t, err)
	assert.Equal(t, []string{old.ID}, idsOf(hits), "a memory with no validUntil value is never-expiring")
	recs, err := ts.List(ctx, ListOpts{Namespace: "ns"})
	require.NoError(t, err)
	assert.Equal(t, []string{old.ID}, recIDs(recs))

	// And it can be given an expiry afterwards.
	require.NoError(t, ts.SetValidUntil(ctx, old.ID, time.Now().Add(-time.Minute)))
	hits, err = ts.SearchMemoryVectors(ctx, vec, SearchOpts{Namespace: "ns", Limit: 5})
	require.NoError(t, err)
	assert.Empty(t, hits)
}

// Changing only the expiry must be a metadata merge: the vector (and so the
// embedding the worker paid for), text, tags and chunks stay as they are, and a
// concurrent link/reinforcement merge is not lost. Clearing must really clear.
func TestSetValidUntilIsMetadataOnly(t *testing.T) {
	st, _ := trashStore(t)
	tenant := "tenant-a"
	ctx := context.Background()
	ts := st.Tenant(tenant)
	vec := []float32{0, 1, 0, 0}

	rec := expRec("ns", "original text", time.Time{})
	rec.Tags = []string{"keep"}
	require.NoError(t, ts.Upsert(ctx, rec, vec))
	require.NoError(t, ts.ReplaceChunks(ctx, rec, []ChunkVec{{Index: 0, Text: rec.Text, Vector: vec}}))

	until := time.Now().Add(48 * time.Hour).UTC().Truncate(time.Second)
	require.NoError(t, ts.SetValidUntil(ctx, rec.ID, until))
	got, ok, err := ts.Get(ctx, rec.ID)
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, until.Equal(got.ValidUntil))
	assert.Equal(t, "original text", got.Text)
	assert.Equal(t, []string{"keep"}, got.Tags)
	hits, err := ts.SearchMemoryVectors(ctx, vec, SearchOpts{Namespace: "ns", Limit: 3})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Less(t, hits[0].Distance, float32(0.01), "the stored vector must be untouched (no re-embed)")
	chunkHits, err := ts.Search(ctx, vec, SearchOpts{Namespace: "ns", Limit: 3})
	require.NoError(t, err)
	assert.Len(t, chunkHits, 1)

	require.NoError(t, ts.SetValidUntil(ctx, rec.ID, time.Time{}))
	got, _, err = ts.Get(ctx, rec.ID)
	require.NoError(t, err)
	assert.True(t, got.ValidUntil.IsZero(), "a zero time must CLEAR the stored expiry")

	require.ErrorIs(t, ts.SetValidUntil(ctx, uuid.NewString(), until), ErrNotFound)

	t.Run("concurrent link and reinforcement merges are not lost", func(t *testing.T) {
		const n = 30
		var wg sync.WaitGroup
		errs := make(chan error, 3*n)
		wg.Go(func() {
			for i := 1; i <= n; i++ {
				errs <- ts.SetLinks(ctx, rec.ID, []string{fmt.Sprintf("l-%d", i)})
			}
		})
		wg.Go(func() {
			for i := 1; i <= n; i++ {
				errs <- ts.Reinforce(ctx, rec.ID, 100+i, time.Now())
			}
		})
		wg.Go(func() {
			for i := 1; i <= n; i++ {
				errs <- ts.SetValidUntil(ctx, rec.ID, until.Add(time.Duration(i)*time.Hour))
			}
		})
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		got, _, err := ts.Get(ctx, rec.ID)
		require.NoError(t, err)
		assert.Equal(t, []string{fmt.Sprintf("l-%d", n)}, got.LinkedIDs)
		assert.Equal(t, 100+n, got.AccessCount)
		assert.True(t, until.Add(n*time.Hour).Equal(got.ValidUntil))
	})
}

// validUntil is index-owned: a full (re-)index of an existing memory writes it
// (so a text edit that also changes the expiry lands), and an index message that
// carries no expiry clears it (so dropping the expiry on edit works).
func TestUpdateIndexFieldsOwnsValidUntil(t *testing.T) {
	st, _ := trashStore(t)
	tenant := "tenant-a"
	ctx := context.Background()
	ts := st.Tenant(tenant)
	vec := []float32{1, 0, 0, 0}
	until := time.Now().Add(time.Hour).UTC().Truncate(time.Second)

	rec := expRec("ns", "v1", until)
	require.NoError(t, ts.Upsert(ctx, rec, vec))
	got, _, err := ts.Get(ctx, rec.ID)
	require.NoError(t, err)
	assert.True(t, until.Equal(got.ValidUntil), "create path persists the expiry")

	rec.Text, rec.ValidUntil = "v2", until.Add(time.Hour)
	require.NoError(t, ts.UpdateIndexFields(ctx, rec, vec))
	got, _, _ = ts.Get(ctx, rec.ID)
	assert.True(t, until.Add(time.Hour).Equal(got.ValidUntil))

	rec.ValidUntil = time.Time{}
	require.NoError(t, ts.UpdateIndexFields(ctx, rec, vec))
	got, _, _ = ts.Get(ctx, rec.ID)
	assert.True(t, got.ValidUntil.IsZero(), "an index message without an expiry clears it")
}

// The sweep moves only memories expired for longer than the grace to Trash (reason
// "expired", chunks gone), leaves in-grace/valid/never memories alone, drains more
// than one batch, and stays tenant-isolated: each tenant's expired memory lands in
// THAT tenant's trash.
func TestSweepExpired(t *testing.T) {
	st, mt := trashStore(t)
	ctx := context.Background()
	vec := []float32{1, 0, 0, 0}
	old := sweepBatch
	sweepBatch = 2 // force several drain rounds
	t.Cleanup(func() { sweepBatch = old })

	grace := 168 * time.Hour
	now := time.Now()
	seed := func(ts *TenantStore, tag string) (past []memory.Record, keep []memory.Record) {
		for i := range 5 {
			r := expRec("ns", fmt.Sprintf("%s long expired %d", tag, i), now.Add(-grace-time.Hour*time.Duration(i+1)))
			require.NoError(t, ts.Upsert(ctx, r, vec))
			require.NoError(t, ts.ReplaceChunks(ctx, r, []ChunkVec{{Index: 0, Text: r.Text, Vector: vec}}))
			past = append(past, r)
		}
		for _, r := range []memory.Record{
			expRec("ns", tag+" expired recently", now.Add(-time.Hour)),
			expRec("ns", tag+" still valid", now.Add(time.Hour)),
			expRec("ns", tag+" never expires", time.Time{}),
		} {
			require.NoError(t, ts.Upsert(ctx, r, vec))
			keep = append(keep, r)
		}
		return
	}

	tenants := []string{"tenant-a"}
	if mt {
		tenants = append(tenants, "tenant-b")
	}
	pastBy := map[string][]memory.Record{}
	keepBy := map[string][]memory.Record{}
	for _, tn := range tenants {
		pastBy[tn], keepBy[tn] = seed(st.Tenant(tn), tn)
	}

	n, err := st.SweepExpired(ctx, now.Add(-grace))
	require.NoError(t, err)
	assert.Equal(t, 5*len(tenants), n)

	for _, tn := range tenants {
		ts := st.Tenant(tn)
		trash, err := ts.ListTrash(ctx, "")
		require.NoError(t, err)
		require.Len(t, trash, 5, "tenant %s trash holds exactly its own expired memories", tn)
		for _, e := range trash {
			assert.Equal(t, memory.TrashReasonExpired, e.Reason)
			assert.Contains(t, e.Text, tn, "tenant isolation: another tenant's memory must not appear here")
			assert.False(t, e.ValidUntil.IsZero(), "the expiry travels with the trash entry")
		}
		for _, r := range pastBy[tn] {
			_, live, err := ts.Get(ctx, r.ID)
			require.NoError(t, err)
			assert.False(t, live, "past-grace memory must leave Memory")
		}
		for _, r := range keepBy[tn] {
			_, live, err := ts.Get(ctx, r.ID)
			require.NoError(t, err)
			assert.True(t, live, "%q must survive the sweep", r.Text)
		}
		hits, err := ts.Search(ctx, vec, SearchOpts{Namespace: "ns", Limit: 20, IncludeExpired: true})
		require.NoError(t, err)
		for _, h := range hits {
			assert.NotContains(t, h.Text, "long expired", "chunks of swept memories must be gone")
		}
	}

	n, err = st.SweepExpired(ctx, now.Add(-grace))
	require.NoError(t, err)
	assert.Zero(t, n, "a second sweep is a no-op")

	t.Run("grace 0 moves anything already expired", func(t *testing.T) {
		n, err := st.SweepExpired(ctx, time.Now())
		require.NoError(t, err)
		assert.Equal(t, len(tenants), n, "only the recently-expired memory of each tenant")
	})
}

// A persistently failing memory (here: more failures than the batch size) must not
// starve the healthy expired memories behind it.
func TestSweepExpiredNotStarvedByFailures(t *testing.T) {
	st, _ := trashStore(t)
	ctx := context.Background()
	ts := st.Tenant("tenant-a")
	vec := []float32{1, 0, 0, 0}
	old := sweepBatch
	sweepBatch = 2
	oldTrash := sweepTrash
	t.Cleanup(func() { sweepBatch, sweepTrash = old, oldTrash })

	now := time.Now()
	bad := map[string]bool{}
	var good []string
	// The 3 oldest (sorted first) fail forever; 4 healthy ones come after.
	for i := range 7 {
		r := expRec("ns", fmt.Sprintf("m%d", i), now.Add(-time.Duration(100-i)*time.Hour))
		require.NoError(t, ts.Upsert(ctx, r, vec))
		if i < 3 {
			bad[r.ID] = true
		} else {
			good = append(good, r.ID)
		}
	}
	sweepTrash = func(ctx context.Context, ts *TenantStore, id string) error {
		if bad[id] {
			return fmt.Errorf("injected failure")
		}
		return oldTrash(ctx, ts, id)
	}

	n, err := st.SweepExpired(ctx, now)
	require.Error(t, err, "failures are reported")
	assert.Equal(t, len(good), n, "every healthy memory is still swept")
	for _, id := range good {
		_, live, err := ts.Get(ctx, id)
		require.NoError(t, err)
		assert.False(t, live)
	}
	for id := range bad {
		_, live, _ := ts.Get(ctx, id)
		assert.True(t, live, "failed memory stays put")
	}
}
