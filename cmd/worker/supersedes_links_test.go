package main

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/bus"
	"github.com/thomas-maurice/cortex/internal/dlock"
	"github.com/thomas-maurice/cortex/internal/memory"
)

// A merge (save with supersedes) must not silently drop the knowledge graph: the
// merged memory inherits the sources' links, and the neighbours stop pointing at
// the trashed ids. Runs through the real index + link consumers.
func TestSupersedesCarriesLinks(t *testing.T) {
	s := newStack(t, 1, time.Minute)
	s.startWorker(2)
	const tenant = "tenant-merge"
	ctx := tenantCtx(tenant)

	a, b, x, y, m := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	// A-X, B-Y, and A-B (a link between the two sources themselves).
	seed(t, s, tenant, memory.Record{ID: a, Text: "a", LinkedIDs: []string{x, b}})
	seed(t, s, tenant, memory.Record{ID: b, Text: "b", LinkedIDs: []string{y, a}})
	seed(t, s, tenant, memory.Record{ID: x, Text: "x", LinkedIDs: []string{a}})
	seed(t, s, tenant, memory.Record{ID: y, Text: "y", LinkedIDs: []string{b}})

	rec := memory.Record{ID: m, Text: "merged", Namespace: "ns", CreatedAt: time.Now().UTC(), Supersedes: []string{a, b}}
	require.NoError(t, bus.PublishIndex(ctx, s.js, rec))
	s.waitIdle()

	got, ok := s.fs.peek(tenant, m)
	require.True(t, ok)
	assert.ElementsMatch(t, []string{x, y}, got.LinkedIDs, "merged memory must inherit the sources' neighbours, and never a source or itself")
	for _, n := range []string{x, y} {
		nr, ok := s.fs.peek(tenant, n)
		require.True(t, ok)
		assert.Equal(t, []string{m}, nr.LinkedIDs, "neighbour must link to the merged memory only, no dangling source id")
	}
	for _, src := range []string{a, b} {
		_, ok := s.fs.peek(tenant, src)
		assert.False(t, ok, "source must be trashed")
	}
	assert.ElementsMatch(t, []string{a + " supersedes:" + m, b + " supersedes:" + m}, s.fs.deleteReasons)
	s.assertNoLocksHeld()
	assert.Zero(t, outcome(s.m, kindLink, outcomeTerm))
	assert.Zero(t, outcome(s.m, kindLink, outcomeDead))

	// Redelivery after a crash between "links queued" and "source deleted": the
	// source is still live when the index message re-runs. Nothing may duplicate
	// or resurrect.
	seed(t, s, tenant, memory.Record{ID: a, Text: "a", LinkedIDs: []string{x}})
	s.fs.mu.Lock()
	s.fs.deleteReasons = nil
	s.fs.mu.Unlock()
	require.NoError(t, bus.PublishIndex(ctx, s.js, rec))
	s.waitIdle()

	got, _ = s.fs.peek(tenant, m)
	assert.ElementsMatch(t, []string{x, y}, got.LinkedIDs, "re-running supersedes must be idempotent")
	xr, _ := s.fs.peek(tenant, x)
	assert.Equal(t, []string{m}, xr.LinkedIDs)
	_, ok = s.fs.peek(tenant, a)
	assert.False(t, ok, "re-run finishes the half-done delete")
	_, ok = s.fs.peek(tenant, b)
	assert.False(t, ok, "an already-trashed source is not resurrected")
	assert.Contains(t, s.fs.deleteReasons, a+" supersedes:"+m, "the still-live source is deleted again")
}

// Neighbours that no longer exist are not linked (an ADD to a missing endpoint
// would burn the link retry budget) and a source linking the merged memory is
// unlinked from it.
func TestApplySupersedesSkipsDeadNeighbours(t *testing.T) {
	s := newStack(t, 1, time.Minute)
	ts := s.fs.tenant("")
	a, gone, live, m := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	seed(t, s, "", memory.Record{ID: a, Text: "a", LinkedIDs: []string{gone, live, m}})
	seed(t, s, "", memory.Record{ID: live, Text: "l"})
	seed(t, s, "", memory.Record{ID: m, Text: "m"})

	type op struct {
		op   memory.LinkOp
		a, b string
	}
	var ops []op
	err := applySupersedes(context.Background(), testLog, ts, memory.Record{ID: m, Supersedes: []string{a}}, func(o memory.LinkOp, x, y string) error {
		ops = append(ops, op{o, x, y})
		return nil
	}, nil)
	require.NoError(t, err)
	assert.ElementsMatch(t, []op{
		{memory.LinkOpAdd, m, live},
		{memory.LinkOpRemove, a, live},
		{memory.LinkOpRemove, a, m},
	}, ops)
}

// Everything else a source carried must survive the merge too: reinforcement,
// dismissals (so reviewed pairs do not come back), provenance, and the merged
// memory must not be flagged as a duplicate of its own sources.
func TestSupersedesCarriesMetadata(t *testing.T) {
	s := newStack(t, 2, time.Minute)
	s.startWorker(2)
	const tenant = "tenant-meta"
	ctx := tenantCtx(tenant)
	old, mid := time.Now().Add(-48*time.Hour).UTC().Truncate(time.Second), time.Now().Add(-24*time.Hour).UTC().Truncate(time.Second)
	last := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)

	a, b, c, m := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	seed(t, s, tenant, memory.Record{ID: a, Text: "a", CreatedAt: old, ConversationID: "conv-old", AccessCount: 3, LastAccessedAt: last.Add(-time.Hour), NotDuplicateOf: []string{c}})
	seed(t, s, tenant, memory.Record{ID: b, Text: "b", CreatedAt: mid, ConversationID: "conv-mid", AccessCount: 4, LastAccessedAt: last})
	seed(t, s, tenant, memory.Record{ID: c, Text: "c", NotDuplicateOf: []string{a}})

	rec := memory.Record{ID: m, Text: "merged", Namespace: "ns", CreatedAt: time.Now().UTC(), Supersedes: []string{a, b}}
	require.NoError(t, bus.PublishIndex(ctx, s.js, rec))
	s.waitIdle()

	got, ok := s.fs.peek(tenant, m)
	require.True(t, ok)
	assert.True(t, got.CreatedAt.Equal(old), "createdAt = oldest of the sources, got %v", got.CreatedAt)
	assert.Equal(t, "conv-old", got.ConversationID, "oldest source's conversation when the merged memory has none")
	assert.Equal(t, 7, got.AccessCount)
	assert.True(t, got.LastAccessedAt.Equal(last))
	assert.Equal(t, []string{c}, got.NotDuplicateOf, "dismissals are inherited")
	cr, _ := s.fs.peek(tenant, c)
	assert.Equal(t, []string{m}, cr.NotDuplicateOf, "counterpart's dismissal is rewritten source -> merged")

	// A later index message with the pre-merge createdAt (stale edit, redelivery)
	// must not revert provenance.
	require.NoError(t, bus.PublishIndex(ctx, s.js, rec))
	s.waitIdle()
	got, _ = s.fs.peek(tenant, m)
	assert.True(t, got.CreatedAt.Equal(old), "createdAt must not be reverted")
	assert.Equal(t, "conv-old", got.ConversationID)
	assert.Equal(t, 7, got.AccessCount, "sources already gone: nothing re-added")
}

func TestFindCandidatesExcludesSupersededSources(t *testing.T) {
	s := newStack(t, 3, time.Minute)
	a, other := uuid.NewString(), uuid.NewString()
	s.fs.dupHits = []string{a, other}
	rec := memory.Record{ID: uuid.NewString(), Supersedes: []string{a}}
	got := findCandidates(context.Background(), testLog, s.fs.tenant(""), rec, []float32{1}, 0.5)
	assert.Equal(t, []string{other}, got, "a merge's own sources are about to be trashed, never flag them")
}

// A message that can never get its locks (here: a source id held by a "crashed"
// worker for far longer than the cap) must not be kept alive forever: past
// maxProcessing the handler is cancelled, keepalives stop, and it settles as an
// ordinary failure that counts toward dead-lettering (NOT as a shutdown hand-back).
func TestMaxProcessingEndsWedgedMessage(t *testing.T) {
	s := newStack(t, 5, time.Minute)
	src, id := uuid.NewString(), uuid.NewString()
	s.crashedHolder(dlock.Key("mem", "", src), time.Minute)
	oldMax, oldKA := maxProcessing, keepaliveInterval
	maxProcessing, keepaliveInterval = 700*time.Millisecond, 100*time.Millisecond
	defer func() { maxProcessing, keepaliveInterval = oldMax, oldKA }()

	msg := indexMsg(t, 1, memory.Record{ID: id, Text: "merge", CreatedAt: time.Now(), Supersedes: []string{src}})
	msg.delivered = memory.MaxDeliver
	start := time.Now()
	process(context.Background(), msg, func(ctx context.Context, _ jetstream.Msg) {
		handleIndex(ctx, testLog, s.m, s.js, s.locker, s.stream, s.embedder, s.stores, s.chunker, msg, 0)
	})
	assert.Less(t, time.Since(start), 10*time.Second, "must not wait for the lock")
	assert.EqualValues(t, 1, outcome(s.m, kindIndex, outcomeDead), "settled as a failure and dead-lettered")
	assert.EqualValues(t, 1, msg.acked.Load())
	progress := msg.progress.Load()
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, progress, msg.progress.Load(), "keepalives stop once the limit is hit")
}

// A merge naming more sources than the cap is refused before any lock is taken and
// dead-lettered with a clear error, rather than locking everything it names.
func TestSupersedesOverCapIsDeadLettered(t *testing.T) {
	s := newStack(t, 6, time.Minute)
	var srcs []string
	for range 51 {
		srcs = append(srcs, uuid.NewString())
	}
	msg := indexMsg(t, 1, memory.Record{ID: uuid.NewString(), Text: "merge", CreatedAt: time.Now(), Supersedes: srcs})
	msg.delivered = memory.MaxDeliver
	runIndex(t, s, msg)
	assert.EqualValues(t, 1, outcome(s.m, kindIndex, outcomeDead))
	assert.Empty(t, s.fs.deleteReasons, "nothing is deleted")
}

// A merge of only time-bound facts must not outlive them: the survivor inherits
// the LATEST source expiry (set via SetValidUntil, since validUntil is not
// index-owned). A permanent source keeps it permanent, and a survivor that
// already has an expiry keeps its own.
func TestSupersedesInheritsExpiry(t *testing.T) {
	soon := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Second)
	later := soon.Add(72 * time.Hour)
	own := soon.Add(500 * time.Hour)
	cases := []struct {
		name       string
		srcA, srcB time.Time
		keep       time.Time
		want       time.Time
	}{
		{"all sources expire: latest wins", soon, later, time.Time{}, later},
		{"one permanent source: stays permanent", soon, time.Time{}, time.Time{}, time.Time{}},
		{"survivor's own expiry is kept", soon, later, own, own},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newStack(t, 1, time.Minute)
			s.startWorker(1)
			const tenant = "tenant-expiry"
			ctx := tenantCtx(tenant)
			a, b, m := uuid.NewString(), uuid.NewString(), uuid.NewString()
			seed(t, s, tenant, memory.Record{ID: a, Text: "a", ValidUntil: tc.srcA})
			seed(t, s, tenant, memory.Record{ID: b, Text: "b", ValidUntil: tc.srcB})
			rec := memory.Record{ID: m, Text: "merged", Namespace: "ns", CreatedAt: time.Now().UTC(), ValidUntil: tc.keep, Supersedes: []string{a, b}}
			require.NoError(t, bus.PublishIndex(ctx, s.js, rec))
			s.waitIdle()
			got, ok := s.fs.peek(tenant, m)
			require.True(t, ok)
			assert.True(t, tc.want.Equal(got.ValidUntil), "want %v got %v", tc.want, got.ValidUntil)
		})
	}
}

// Redelivery after a source was already trashed must not turn a permanent merge
// into an expiring one (the remaining sources alone look "all expiring").
func TestSupersedesRedeliveryDoesNotInventExpiry(t *testing.T) {
	s := newStack(t, 1, time.Minute)
	s.startWorker(1)
	const tenant = "tenant-expiry-redeliver"
	ctx := tenantCtx(tenant)
	a, b, m := uuid.NewString(), uuid.NewString(), uuid.NewString()
	soon := time.Now().Add(time.Hour).UTC()
	seed(t, s, tenant, memory.Record{ID: a, Text: "a"}) // permanent
	seed(t, s, tenant, memory.Record{ID: b, Text: "b", ValidUntil: soon})
	rec := memory.Record{ID: m, Text: "merged", CreatedAt: time.Now().UTC(), Supersedes: []string{a, b}}
	require.NoError(t, bus.PublishIndex(ctx, s.js, rec))
	s.waitIdle()
	// Crash simulation: the permanent source is gone, the expiring one is live again.
	seed(t, s, tenant, memory.Record{ID: b, Text: "b", ValidUntil: soon})
	require.NoError(t, bus.PublishIndex(ctx, s.js, rec))
	s.waitIdle()
	got, ok := s.fs.peek(tenant, m)
	require.True(t, ok)
	assert.True(t, got.ValidUntil.IsZero())
}
