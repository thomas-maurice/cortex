package main

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/bus"
	"github.com/thomas-maurice/cortex/internal/memory"
)

const simTenant = "tenant-sim"

// TestMultiWorkerConvergence is the replica-safety test. 3-4 real worker replicas
// (real handlers, real JetStream consumers sharing the durables, real KV locks)
// race through a randomised mix of edits, link adds/removes and server-side merges
// (reinforcement, not-a-duplicate dismissals) against a fake store that models
// per-field merge vs full-write and a non-atomic read-modify-write. At quiescence:
//
//   - text is the LAST published edit per id (stale-write guard: edit v1 finishing
//     after v2 must not win);
//   - the link set is exactly the initial (imported) links minus removes plus adds,
//     symmetric (no lost edge from a concurrent read-modify-write, none clobbered
//     by an index upsert carrying a stale snapshot);
//   - accessCount / notDuplicateOf written by the server are never lost to an index
//     write (field ownership);
//   - chunks match the final text, no lock is left held, nothing was dead-lettered.
//
// Removing the lock, or reverting the update path to a full Upsert, makes this
// fail (verified by hand when the feature was written).
func TestMultiWorkerConvergence(t *testing.T) {
	seeds := 10
	if testing.Short() {
		seeds = 2
	}
	for seed := range seeds {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			t.Parallel()
			runConvergence(t, uint64(seed)+1)
		})
	}
}

func runConvergence(t *testing.T, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, 7))
	s := newStack(t, seed, time.Minute)
	ctx := tenantCtx(simTenant)
	ts := s.fs.tenant(simTenant).(*fakeTenant)

	const nIDs = 5
	ids := make([]string, nIDs)
	for i := range ids {
		ids[i] = uuid.NewString()
	}
	type pair struct{ a, b string }
	var pairs []pair
	for i := range nIDs {
		for j := i + 1; j < nIDs; j++ {
			pairs = append(pairs, pair{ids[i], ids[j]})
		}
	}

	// Phase 1: import-style creation with pre-existing links (new memories must keep
	// the links their message carries).
	initial := map[string]bool{} // "a|b" present in the initial graph
	initLinks := map[string][]string{}
	for _, p := range pairs {
		if rng.IntN(2) == 0 {
			initial[p.a+"|"+p.b] = true
			initLinks[p.a] = append(initLinks[p.a], p.b)
			initLinks[p.b] = append(initLinks[p.b], p.a)
		}
	}
	for range 3 + rng.IntN(2) {
		s.startWorker(1 + rng.IntN(3))
	}
	for _, id := range ids {
		require.NoError(t, bus.PublishIndex(ctx, s.js, memory.Record{
			ID: id, Text: id + " v0", Namespace: "ns", Source: "t", CreatedAt: time.Now().UTC(), LinkedIDs: initLinks[id],
		}))
	}
	s.waitIdle()
	for _, id := range ids {
		got, ok := s.fs.peek(simTenant, id)
		require.True(t, ok)
		assert.ElementsMatch(t, initLinks[id], got.LinkedIDs, "import of a NEW memory must keep its links")
	}

	// Phase 2: the storm. Link ops touch each edge at most once (an add for a
	// missing edge, a remove for an existing one) so the final graph is independent
	// of processing order. The whole plan is drawn from rng up front (rand.Rand is
	// not goroutine-safe); goroutines only execute it.
	want := map[string]bool{}
	var lops []func()
	for _, p := range pairs {
		key := p.a + "|" + p.b
		want[key] = initial[key]
		switch {
		case initial[key] && rng.IntN(2) == 0:
			want[key] = false
			lops = append(lops, func() { assert.NoError(t, bus.PublishLink(ctx, s.js, memory.LinkOpRemove, p.a, p.b)) })
		case !initial[key] && rng.IntN(2) == 0:
			want[key] = true
			lops = append(lops, func() { assert.NoError(t, bus.PublishLink(ctx, s.js, memory.LinkOpAdd, p.a, p.b)) })
		}
	}
	rng.Shuffle(len(lops), func(i, j int) { lops[i], lops[j] = lops[j], lops[i] })

	lastText := map[string]string{}
	edits := map[string]int{}
	for _, id := range ids {
		edits[id] = 1 + rng.IntN(4)
		lastText[id] = fmt.Sprintf("%s v%d", id, edits[id])
	}
	srvPlan := make([]int, 25)
	for i := range srvPlan {
		srvPlan[i] = rng.IntN(nIDs)
	}

	reinforces := map[string]int{}
	dismissed := map[string][]string{}
	var wg sync.WaitGroup
	// One publisher goroutine per id, sequential: its order IS the truth for that id.
	for _, id := range ids {
		wg.Go(func() {
			for k := 1; k <= edits[id]; k++ {
				// Like UpdateMemory: read the stored record NOW (it is stale by the
				// time a worker applies it: links, counters), edit text, republish.
				rec, ok := s.fs.peek(simTenant, id)
				if !assert.True(t, ok) {
					return
				}
				rec.Text = fmt.Sprintf("%s v%d", id, k)
				assert.NoError(t, bus.PublishIndex(ctx, s.js, rec))
				time.Sleep(time.Millisecond)
			}
		})
	}
	wg.Go(func() {
		for _, o := range lops {
			o()
			time.Sleep(time.Millisecond)
		}
	})
	// "Server": sequential reinforcement + dismissals. It is the sole writer of
	// those fields and serialised, so the expected totals are exact.
	wg.Go(func() {
		for i, idx := range srvPlan {
			id := ids[idx]
			cur, _, _ := ts.Get(t.Context(), id)
			if i%3 == 0 {
				tok := fmt.Sprintf("not-dup-%d", i)
				assert.NoError(t, ts.SetNotDuplicateOf(id, append(cur.NotDuplicateOf, tok)))
				dismissed[id] = append(dismissed[id], tok)
			} else {
				assert.NoError(t, ts.Reinforce(id, cur.AccessCount+1))
				reinforces[id]++
			}
		}
	})
	wg.Wait()
	s.waitIdle()

	for _, id := range ids {
		got, ok := s.fs.peek(simTenant, id)
		require.True(t, ok)
		assert.Equal(t, lastText[id], got.Text, "stale index message overwrote a newer edit (id %s)", id)
		assert.Equal(t, []string{lastText[id]}, s.fs.chunks[simTenant+"|"+id], "chunks must match the final text (id %s)", id)
		assert.Equal(t, reinforces[id], got.AccessCount, "reinforcement lost to an index write (id %s)", id)
		assert.ElementsMatch(t, dismissed[id], got.NotDuplicateOf, "notDuplicateOf lost to an index write (id %s)", id)
		assert.Positive(t, got.IndexedSeq)

		var wantLinks []string
		for _, p := range pairs {
			if !want[p.a+"|"+p.b] {
				continue
			}
			switch id {
			case p.a:
				wantLinks = append(wantLinks, p.b)
			case p.b:
				wantLinks = append(wantLinks, p.a)
			}
		}
		assert.ElementsMatch(t, wantLinks, got.LinkedIDs, "link set diverged for %s (lost or clobbered edge)", id)
	}
	s.assertNoLocksHeld()
	for _, kind := range []string{kindIndex, kindLink} {
		assert.Zero(t, outcome(s.m, kind, outcomeDead), kind)
		assert.Zero(t, outcome(s.m, kind, outcomeTerm), kind)
	}
}
