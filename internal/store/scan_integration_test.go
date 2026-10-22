package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// TestScanAllPagesEveryRow guards the "fetch everything" paths (backup, reindex,
// migrate-mt, Count, namespace ops). A single WithLimit(AllLimit) query silently
// truncates at Weaviate's 10000-result cap, so those paths cursor-page instead.
// With the page size shrunk to 3 we exercise multi-page iteration with a tiny
// dataset: every id must come back EXACTLY once (a broken cursor shows up as
// dupes or misses), including when the total is an exact multiple of the page
// size (the trailing empty page must not loop or drop rows).
//
// Runs non-MT by default and MT when CORTEX_TEST_MULTI_TENANT is set.
func TestScanAllPagesEveryRow(t *testing.T) {
	rest := os.Getenv("CORTEX_TEST_WEAVIATE_REST")
	grpc := os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST and CORTEX_TEST_WEAVIATE_GRPC to run this integration test")
	}
	mt := os.Getenv("CORTEX_TEST_MULTI_TENANT") != ""

	old := scanPageSize
	scanPageSize = 3
	t.Cleanup(func() { scanPageSize = old })

	ctx := context.Background()
	st, err := New(rest, grpc)
	require.NoError(t, err)
	st.SetMultiTenant(mt)
	_ = st.DeleteAllClasses(ctx)
	require.NoError(t, st.EnsureSchema(ctx))
	t.Cleanup(func() { _ = st.DeleteAllClasses(ctx) })

	ts := st.Tenant("user-scan")
	other := st.Tenant("user-other")
	vec := []float32{1, 0, 0, 0}
	now := time.Now().UTC()

	// 9 memories (multiple of the page size): 5 in alpha, 4 in beta. 7 summaries.
	wantAll := map[string]bool{}
	wantAlpha := map[string]bool{}
	var flagged []string // ids with dupCandidates, in creation (oldest-first) order
	for i := 0; i < 9; i++ {
		ns := "beta"
		if i < 5 {
			ns = "alpha"
		}
		id := uuid.NewString()
		rec := memory.Record{ID: id, Text: fmt.Sprintf("m%d", i), Namespace: ns, CreatedAt: now.Add(time.Duration(i) * time.Minute)}
		if i%2 == 0 { // 0,2,4,6,8 flagged: alpha 0,2,4 / beta 6,8
			rec.DupCandidates = []string{"some-other-id"}
			flagged = append(flagged, id)
		}
		require.NoError(t, ts.Upsert(ctx, rec, vec))
		wantAll[id] = true
		if ns == "alpha" {
			wantAlpha[id] = true
		}
	}
	// A foreign tenant's row must never leak into a scan (only meaningful in MT).
	if mt {
		require.NoError(t, other.Upsert(ctx, memory.Record{ID: uuid.NewString(), Text: "x", Namespace: "alpha", CreatedAt: now}, vec))
	}
	wantSums := map[string]bool{}
	for i := 0; i < 7; i++ {
		cid := fmt.Sprintf("conv-%d", i)
		require.NoError(t, ts.UpsertSummary(ctx, memory.Summary{ConversationID: cid, Text: "s", Namespace: "alpha", CreatedAt: now, UpdatedAt: now}, vec))
		wantSums[cid] = true
	}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		recs, err := ts.ListAll(ctx, "")
		require.NoError(c, err)
		assert.Len(c, recs, 9)
	}, 10*time.Second, 200*time.Millisecond, "memories did not become queryable")

	ids := func(recs []memory.Record) map[string]bool {
		m := map[string]bool{}
		for _, r := range recs {
			assert.False(t, m[r.ID], "id %s returned twice", r.ID)
			m[r.ID] = true
		}
		return m
	}

	recs, err := ts.ListAll(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, wantAll, ids(recs), "ListAll must return every id exactly once")

	recs, err = ts.ListAll(ctx, "alpha")
	require.NoError(t, err)
	assert.Equal(t, wantAlpha, ids(recs), "namespace-filtered ListAll must filter in Go across pages")

	sums, err := ts.ListAllSummaries(ctx)
	require.NoError(t, err)
	gotSums := map[string]bool{}
	for _, s := range sums {
		assert.False(t, gotSums[s.ConversationID], "summary %s returned twice", s.ConversationID)
		gotSums[s.ConversationID] = true
	}
	assert.Equal(t, wantSums, gotSums)

	// ListWithCandidates scans all pages, filters in Go, sorts newest-first and
	// applies the limit AFTER — a page-local limit/sort would drop or misorder.
	cands, err := ts.ListWithCandidates(ctx, "", 50)
	require.NoError(t, err)
	require.Len(t, cands, 5)
	for i, c := range cands {
		assert.Equal(t, flagged[len(flagged)-1-i], c.ID, "candidates must be newest-first across pages")
	}
	cands, err = ts.ListWithCandidates(ctx, "alpha", 2)
	require.NoError(t, err)
	require.Len(t, cands, 2, "limit applies after the full scan")
	assert.Equal(t, []string{flagged[2], flagged[1]}, []string{cands[0].ID, cands[1].ID})

	n, err := ts.Count(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, 9, n, "Count must be exact across pages")
	n, err = ts.Count(ctx, "beta")
	require.NoError(t, err)
	assert.Equal(t, 4, n)

	stats, err := ts.ListNamespaces(ctx)
	require.NoError(t, err)
	byName := map[string]NamespaceStat{}
	for _, s := range stats {
		byName[s.Name] = s
	}
	assert.Equal(t, 5, byName["alpha"].MemoryCount)
	assert.Equal(t, 7, byName["alpha"].SummaryCount)
	assert.Equal(t, 4, byName["beta"].MemoryCount)

	// A tenant that was never written to reads as empty, not an error.
	if mt {
		empty, err := st.Tenant("user-never").ListAll(ctx, "")
		require.NoError(t, err)
		assert.Empty(t, empty)
	}

	// Non-MT only: the unscoped migrate-mt snapshot helpers.
	if !mt {
		all, err := st.ListAllRecords(ctx)
		require.NoError(t, err)
		assert.Equal(t, wantAll, ids(all))
		allSums, err := st.ListAllSummaries(ctx)
		require.NoError(t, err)
		assert.Len(t, allSums, 7)
	}
}
