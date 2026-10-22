package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/memory"
)

const day = 24 * time.Hour

// Stale review exists so never/no-longer-recalled memories can be cleaned up
// without ever flagging something that is merely new or actively used. This pins
// the classification (never vs long-ago vs recent vs too-new), the oldest-first
// order, namespace/tenant scoping, and that the Go-side scan pages correctly
// (page size shrunk to 2 so 8 rows span several cursor pages). Runs non-MT and MT.
func TestListStaleClassification(t *testing.T) {
	st, mt := trashStore(t)
	old := scanPageSize
	scanPageSize = 2
	t.Cleanup(func() { scanPageSize = old })
	ctx := context.Background()
	ts := st.Tenant("tenant-stale")
	now := time.Now().UTC().Truncate(time.Second)

	mk := func(ns string, created time.Duration, count int, last time.Duration) memory.Record {
		r := memory.Record{ID: uuid.NewString(), Text: "x", Namespace: ns, CreatedAt: now.Add(-created), AccessCount: count}
		if last > 0 {
			r.LastAccessedAt = now.Add(-last)
		}
		require.NoError(t, ts.Upsert(ctx, r, trashVec))
		return r
	}
	neverOld := mk("a", 200*day, 0, 0)             // never recalled, old
	neverOlder := mk("a", 400*day, 0, 0)           // never recalled, older
	neverNew := mk("a", 2*day, 0, 0)               // never recalled but brand new: guard
	longAgo := mk("a", 300*day, 5, 120*day)        // recalled, but 120d ago
	recent := mk("a", 300*day, 9, 3*day)           // recalled 3d ago: active
	longAgoOtherNS := mk("b", 300*day, 1, 200*day) // other namespace
	midNever := mk("a", 100*day, 0, 0)             // never recalled, 100d old
	if mt {                                        // a foreign tenant's rows must never appear
		require.NoError(t, st.Tenant("tenant-other").Upsert(ctx, memory.Record{ID: uuid.NewString(), Text: "x", Namespace: "a", CreatedAt: now.Add(-500 * day)}, trashVec))
	}

	ids := func(recs []memory.Record) []string {
		out := make([]string, 0, len(recs))
		for _, r := range recs {
			out = append(out, r.ID)
		}
		return out
	}

	t.Run("never recalled honours the min-age guard", func(t *testing.T) {
		recs, total, err := ts.ListStale(ctx, StaleOpts{Namespace: "a", MinAge: 30 * day, Now: now})
		require.NoError(t, err)
		assert.Equal(t, 3, total)
		assert.Equal(t, []string{neverOlder.ID, neverOld.ID, midNever.ID}, ids(recs), "oldest first; new memory and recalled ones excluded")
		assert.NotContains(t, ids(recs), neverNew.ID, "a 2-day-old memory has had no chance to be recalled")
	})

	t.Run("min-age 0 disables the guard", func(t *testing.T) {
		recs, _, err := ts.ListStale(ctx, StaleOpts{Namespace: "a", Now: now})
		require.NoError(t, err)
		assert.Contains(t, ids(recs), neverNew.ID)
	})

	t.Run("not recalled since N days", func(t *testing.T) {
		recs, total, err := ts.ListStale(ctx, StaleOpts{Namespace: "a", MinAge: 30 * day, IdleFor: 90 * day, Now: now})
		require.NoError(t, err)
		assert.Equal(t, 4, total)
		assert.NotContains(t, ids(recs), recent.ID, "recalled 3 days ago is active, not stale")
		assert.Equal(t, []string{neverOlder.ID, neverOld.ID, longAgo.ID, midNever.ID}, ids(recs),
			"oldest reference time first (last recall, else creation); recent (3d) and new are excluded")

		recs, _, err = ts.ListStale(ctx, StaleOpts{Namespace: "a", MinAge: 30 * day, IdleFor: 150 * day, Now: now})
		require.NoError(t, err)
		assert.Equal(t, []string{neverOlder.ID, neverOld.ID}, ids(recs), "recalled 120d ago drops out at a 150d threshold; never-recalled falls back to createdAt")
	})

	t.Run("pagination across namespaces", func(t *testing.T) {
		opts := StaleOpts{MinAge: 30 * day, IdleFor: 90 * day, Now: now, Limit: 2}
		p1, total, err := ts.ListStale(ctx, opts)
		require.NoError(t, err)
		assert.Equal(t, 5, total, "namespace b's long-ago memory joins when no namespace is given")
		opts.Offset = 2
		p2, _, err := ts.ListStale(ctx, opts)
		require.NoError(t, err)
		opts.Offset = 4
		p3, _, err := ts.ListStale(ctx, opts)
		require.NoError(t, err)
		opts.Offset = 9
		p4, _, err := ts.ListStale(ctx, opts)
		require.NoError(t, err)
		assert.Len(t, p1, 2)
		assert.Len(t, p2, 2)
		assert.Len(t, p3, 1)
		assert.Empty(t, p4)
		got := append(append(ids(p1), ids(p2)...), ids(p3)...)
		assert.ElementsMatch(t, []string{neverOlder.ID, neverOld.ID, longAgoOtherNS.ID, longAgo.ID, midNever.ID}, got, "every row exactly once across pages")
		assert.Equal(t, neverOlder.ID, got[0])
		assert.Equal(t, midNever.ID, got[4])
	})

	t.Run("tag filters", func(t *testing.T) {
		tagged := memory.Record{ID: uuid.NewString(), Text: "x", Namespace: "a", Tags: []string{"keep"}, CreatedAt: now.Add(-300 * day)}
		require.NoError(t, ts.Upsert(ctx, tagged, trashVec))
		recs, _, err := ts.ListStale(ctx, StaleOpts{Namespace: "a", MinAge: 30 * day, ExcludeTags: []string{"keep"}, Now: now})
		require.NoError(t, err)
		assert.NotContains(t, ids(recs), tagged.ID)
		recs, _, err = ts.ListStale(ctx, StaleOpts{Namespace: "a", MinAge: 30 * day, IncludeTags: []string{"keep"}, Now: now})
		require.NoError(t, err)
		assert.Equal(t, []string{tagged.ID}, ids(recs))
	})
}
