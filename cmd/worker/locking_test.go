package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/bus"
	"github.com/thomas-maurice/cortex/internal/dlock"
	"github.com/thomas-maurice/cortex/internal/memory"
)

var testLog = slog.New(slog.NewTextHandler(io.Discard, nil))

func seed(t *testing.T, s *stack, tenant string, rec memory.Record) {
	t.Helper()
	require.NoError(t, s.fs.tenant(tenant).Upsert(context.Background(), rec, nil))
}

func runIndex(t *testing.T, s *stack, msg *fakeMsg) {
	t.Helper()
	handleIndex(context.Background(), testLog, s.m, s.js, s.locker, s.embedder, s.stores, s.chunker, msg, 0)
}

// The link read-modify-write must be exclusive: SetLinks replaces the whole list,
// so two concurrent adds to the same hub both read [] and the later write erases
// the earlier edge. Without the lock this loses edges.
func TestHandleLinkConcurrentAddsLoseNothing(t *testing.T) {
	s := newStack(t, 1, time.Minute)
	const n = 16
	hub := uuid.NewString()
	seed(t, s, "", memory.Record{ID: hub, Text: "hub"})
	spokes := make([]string, n)
	for i := range spokes {
		spokes[i] = uuid.NewString()
		seed(t, s, "", memory.Record{ID: spokes[i], Text: "spoke"})
	}

	var wg sync.WaitGroup
	msgs := make([]*fakeMsg, n)
	for i, sp := range spokes {
		msgs[i] = linkMsg(t, memory.LinkMsg{Op: memory.LinkOpAdd, A: hub, B: sp})
		wg.Go(func() { handleLink(context.Background(), testLog, s.m, s.js, s.locker, s.stores, msgs[i]) })
	}
	wg.Wait()

	got, _ := s.fs.peek("", hub)
	assert.ElementsMatch(t, spokes, got.LinkedIDs, "an edge was lost to a concurrent read-modify-write")
	for i, sp := range spokes {
		r, _ := s.fs.peek("", sp)
		assert.Equal(t, []string{hub}, r.LinkedIDs, "spoke %d must point back (symmetric)", i)
		assert.EqualValues(t, 1, msgs[i].acked.Load())
	}
	s.assertNoLocksHeld()
}

// Opposing endpoint order must not deadlock: (a,b) and (b,a) arrive concurrently.
func TestHandleLinkOpposingOrderNoDeadlock(t *testing.T) {
	s := newStack(t, 1, time.Minute)
	a, b := uuid.NewString(), uuid.NewString()
	seed(t, s, "", memory.Record{ID: a, Text: "a"})
	seed(t, s, "", memory.Record{ID: b, Text: "b"})
	var wg sync.WaitGroup
	for i := range 20 {
		lm := memory.LinkMsg{Op: memory.LinkOpAdd, A: a, B: b}
		if i%2 == 1 {
			lm.A, lm.B = b, a
		}
		m := linkMsg(t, lm)
		wg.Go(func() { handleLink(context.Background(), testLog, s.m, s.js, s.locker, s.stores, m) })
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("deadlock")
	}
	ra, _ := s.fs.peek("", a)
	assert.Equal(t, []string{b}, ra.LinkedIDs, "idempotent: one edge, not twenty")
}

// Index must hold the SAME lock as link: an edit applied between a link op's read
// and write must not be lost either way.
func TestIndexAndLinkSerialiseOnSameID(t *testing.T) {
	s := newStack(t, 2, time.Minute)
	a := uuid.NewString()
	seed(t, s, "", memory.Record{ID: a, Text: "a0", IndexVersion: 1})

	var wg sync.WaitGroup
	// 10 edits of a racing with 10 link adds a<->each of 10 fresh memories.
	var others []string
	for range 10 {
		o := uuid.NewString()
		seed(t, s, "", memory.Record{ID: o, Text: "o"})
		others = append(others, o)
	}
	for i, o := range others {
		lm := linkMsg(t, memory.LinkMsg{Op: memory.LinkOpAdd, A: a, B: o})
		wg.Go(func() { handleLink(context.Background(), testLog, s.m, s.js, s.locker, s.stores, lm) })
		im := indexMsg(t, int64(10+i), memory.Record{ID: a, Text: fmt.Sprintf("a%d", i+1), CreatedAt: time.Now()})
		wg.Go(func() { runIndex(t, s, im) })
	}
	wg.Wait()
	got, _ := s.fs.peek("", a)
	assert.ElementsMatch(t, others, got.LinkedIDs, "an index write erased a link")
	assert.Equal(t, "a10", got.Text, "highest version wins")
}

// ---- stale guard ----------------------------------------------------------

func TestStaleGuard(t *testing.T) {
	newRec := func(id, text string) memory.Record {
		return memory.Record{ID: id, Text: text, Namespace: "ns", CreatedAt: time.Now()}
	}

	// WHY: an edit v1 whose worker finishes after v2's must not revert v2, whatever
	// the processing order (two replicas, a slow embed, a redelivery).
	t.Run("older message after newer is skipped and counted stale", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		id := uuid.NewString()
		runIndex(t, s, indexMsg(t, 20, newRec(id, "v2")))
		old := indexMsg(t, 10, newRec(id, "v1"))
		runIndex(t, s, old)

		got, _ := s.fs.peek("", id)
		assert.Equal(t, "v2", got.Text, "stale v1 overwrote v2")
		assert.EqualValues(t, 20, got.IndexVersion)
		assert.EqualValues(t, 1, old.acked.Load(), "stale message is acked, not retried")
		assert.EqualValues(t, 1, outcome(s.m, kindIndex, outcomeStale))
		assert.EqualValues(t, 1, outcome(s.m, kindIndex, outcomeAck))
		assert.Equal(t, []string{"v2"}, s.fs.chunks["|"+id], "chunks must not be replaced by the stale message")
	})

	t.Run("newer after older is applied", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		id := uuid.NewString()
		runIndex(t, s, indexMsg(t, 10, newRec(id, "v1")))
		runIndex(t, s, indexMsg(t, 20, newRec(id, "v2")))
		got, _ := s.fs.peek("", id)
		assert.Equal(t, "v2", got.Text)
		assert.EqualValues(t, 20, got.IndexVersion)
		assert.Zero(t, outcome(s.m, kindIndex, outcomeStale))
	})

	t.Run("pre-existing object without indexVersion counts as 0", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		id := uuid.NewString()
		seed(t, s, "", memory.Record{ID: id, Text: "legacy"}) // IndexVersion 0, like a pre-upgrade object
		runIndex(t, s, indexMsg(t, 5, newRec(id, "edited")))
		got, _ := s.fs.peek("", id)
		assert.Equal(t, "edited", got.Text)
		assert.EqualValues(t, 5, got.IndexVersion)
	})

	t.Run("redelivery of the same version re-applies idempotently", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		id := uuid.NewString()
		runIndex(t, s, indexMsg(t, 7, newRec(id, "v")))
		again := indexMsg(t, 7, newRec(id, "v"))
		runIndex(t, s, again)
		got, _ := s.fs.peek("", id)
		assert.Equal(t, "v", got.Text)
		assert.Zero(t, outcome(s.m, kindIndex, outcomeStale), "same version is a redelivery, not stale")
		assert.EqualValues(t, 1, again.acked.Load())
	})

	t.Run("stale message does not run supersedes", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		id, victim := uuid.NewString(), uuid.NewString()
		seed(t, s, "", memory.Record{ID: victim, Text: "src"})
		runIndex(t, s, indexMsg(t, 20, newRec(id, "new")))
		rec := newRec(id, "old")
		rec.Supersedes = []string{victim}
		runIndex(t, s, indexMsg(t, 10, rec))
		_, ok := s.fs.peek("", victim)
		assert.True(t, ok, "a stale message must not delete anything")
	})

	t.Run("summaries get the same guard", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		sum := func(text string, version int64) memory.Summary {
			return memory.Summary{ConversationID: "c1", Text: text, UpdatedAt: time.Now(), IndexVersion: version}
		}
		ts := s.fs.tenant("")
		ok, err := commitSummary(context.Background(), s.locker, ts, sum("new", 20), nil)
		require.NoError(t, err)
		assert.True(t, ok)
		ok, err = commitSummary(context.Background(), s.locker, ts, sum("old", 10), nil)
		require.NoError(t, err)
		assert.False(t, ok, "older summary must be skipped")
		assert.Equal(t, "new", s.fs.sums["|c1"].Text)
		ok, err = commitSummary(context.Background(), s.locker, ts, sum("redelivered", 20), nil)
		require.NoError(t, err)
		assert.True(t, ok, "equal version is a redelivery: re-applied")
		assert.Equal(t, "redelivered", s.fs.sums["|c1"].Text)
		ok, err = commitSummary(context.Background(), s.locker, ts, sum("newer", 30), nil)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "newer", s.fs.sums["|c1"].Text)
		assert.EqualValues(t, 30, s.fs.sums["|c1"].IndexVersion)
	})

	t.Run("summary stored without a version (legacy) is overwritten", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		ts := s.fs.tenant("")
		require.NoError(t, ts.UpsertSummary(context.Background(), memory.Summary{ConversationID: "c2", Text: "legacy"}, nil))
		ok, err := commitSummary(context.Background(), s.locker, ts,
			memory.Summary{ConversationID: "c2", Text: "edited", UpdatedAt: time.Now(), IndexVersion: 5}, nil)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "edited", s.fs.sums["|c2"].Text)
	})

	// A message from a pre-upgrade server carries version 0: oldest, so it never
	// beats a versioned object, but still creates / overwrites a legacy one.
	t.Run("unversioned message loses to a versioned object", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		id := uuid.NewString()
		seed(t, s, "", memory.Record{ID: id, Text: "versioned", IndexVersion: 3})
		runIndex(t, s, indexMsg(t, 0, newRec(id, "unversioned")))
		got, _ := s.fs.peek("", id)
		assert.Equal(t, "versioned", got.Text)
	})
}

// ---- field ownership ------------------------------------------------------

func TestIndexFieldOwnership(t *testing.T) {
	t.Run("update of an existing memory never writes fields owned by others", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		id := uuid.NewString()
		seed(t, s, "", memory.Record{ID: id, Text: "old", LinkedIDs: []string{"l1", "l2"},
			AccessCount: 9, LastAccessedAt: time.Unix(1000, 0), NotDuplicateOf: []string{"nd"}, IndexVersion: 1})
		s.fs.resetAudit()
		// The queued edit carries a STALE snapshot: no links, no counters.
		runIndex(t, s, indexMsg(t, 5, memory.Record{ID: id, Text: "new", Namespace: "ns", CreatedAt: time.Now()}))

		got, _ := s.fs.peek("", id)
		assert.Equal(t, "new", got.Text)
		assert.Equal(t, []string{"l1", "l2"}, got.LinkedIDs, "link consumer's field was clobbered")
		assert.Equal(t, 9, got.AccessCount, "server's reinforcement was clobbered")
		assert.True(t, got.LastAccessedAt.Equal(time.Unix(1000, 0)))
		assert.Equal(t, []string{"nd"}, got.NotDuplicateOf)
		for _, op := range s.fs.auditOps() {
			assert.NotEqual(t, "upsert "+id, op, "an existing memory must be merged, never fully replaced")
		}
	})

	t.Run("a NEW memory keeps the links its message carries (import/restore)", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		id := uuid.NewString()
		runIndex(t, s, indexMsg(t, 5, memory.Record{ID: id, Text: "imported", LinkedIDs: []string{"x", "y"},
			AccessCount: 3, NotDuplicateOf: []string{"z"}, CreatedAt: time.Now()}))
		got, _ := s.fs.peek("", id)
		assert.Equal(t, []string{"x", "y"}, got.LinkedIDs)
		assert.Equal(t, 3, got.AccessCount)
		assert.Equal(t, []string{"z"}, got.NotDuplicateOf)
	})

	t.Run("dismissed duplicate pairs are not re-flagged by a stale snapshot", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		id := uuid.NewString()
		seed(t, s, "", memory.Record{ID: id, Text: "t", NotDuplicateOf: []string{"dismissed"}})
		// The worker's neighbour search (run BEFORE the lock, from the message's
		// stale snapshot of notDuplicateOf) proposes both; the dismissal stored
		// since must win.
		s.fs.dupHits = []string{"dismissed", "other"}
		handleIndex(context.Background(), testLog, s.m, s.js, s.locker, s.embedder, s.stores, s.chunker,
			indexMsg(t, 5, memory.Record{ID: id, Text: "t2", CreatedAt: time.Now()}), 1)
		got, _ := s.fs.peek("", id)
		assert.Equal(t, []string{"other"}, got.DupCandidates)
	})

	// Tags are written by the index path AND by tag rename. A queued message with a
	// pre-rename snapshot must not undo the rename when it does not intend to
	// change tags (UpdateMemory without replace_tags, reindex): keepTags.
	t.Run("keepTags keeps the stored tags (memory AND chunks) over a stale snapshot", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		id := uuid.NewString()
		seed(t, s, "", memory.Record{ID: id, Text: "t", Tags: []string{"kubernetes"}, IndexVersion: 1}) // already renamed k8s -> kubernetes
		runIndex(t, s, indexMsg(t, 5, memory.Record{ID: id, Text: "edited", Tags: []string{"k8s"}, KeepTags: true, CreatedAt: time.Now()}))
		got, _ := s.fs.peek("", id)
		assert.Equal(t, "edited", got.Text)
		assert.Equal(t, []string{"kubernetes"}, got.Tags, "queued edit restored the pre-rename tag")
		s.fs.mu.Lock()
		defer s.fs.mu.Unlock()
		assert.Equal(t, []string{"kubernetes"}, s.fs.chunkTags["|"+id], "chunks must follow the stored tags")
	})

	t.Run("without keepTags the message's tags win (replace_tags / import are point-in-time)", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		id := uuid.NewString()
		seed(t, s, "", memory.Record{ID: id, Text: "t", Tags: []string{"kubernetes"}, IndexVersion: 1})
		runIndex(t, s, indexMsg(t, 5, memory.Record{ID: id, Text: "t", Tags: []string{"explicit", " explicit ", ""}, CreatedAt: time.Now()}))
		got, _ := s.fs.peek("", id)
		assert.Equal(t, []string{"explicit"}, got.Tags, "tags are normalised (trim, dedup, drop empty) at the worker too")
	})

	t.Run("keepTags on a memory that does not exist yet still creates it with the message tags", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		id := uuid.NewString()
		runIndex(t, s, indexMsg(t, 5, memory.Record{ID: id, Text: "t", Tags: []string{"a"}, KeepTags: true, CreatedAt: time.Now()}))
		got, ok := s.fs.peek("", id)
		require.True(t, ok)
		assert.Equal(t, []string{"a"}, got.Tags)
	})

	t.Run("tenants are isolated", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		id := uuid.NewString()
		rec := memory.Record{ID: id, Text: "mine", UserID: "u1", CreatedAt: time.Now()}
		runIndex(t, s, indexMsg(t, 5, rec))
		_, ok := s.fs.peek("u1", id)
		assert.True(t, ok)
		_, ok = s.fs.peek("u2", id)
		assert.False(t, ok)
	})
}

// ---- contention -----------------------------------------------------------

// A crashed worker wedges an id until its lock lease (the TTL) expires. The message that
// hits it must wait (keeping itself alive with InProgress), then apply once the
// lease expires. It must never be NAKed into the delivery budget, dead-lettered or
// Term'd for what is perfectly valid work, even if it is already on its last
// delivery.
func TestContendedIDWaitsAndAppliesAfterExpiry(t *testing.T) {
	s := newStack(t, 1, 2*time.Second)
	id := uuid.NewString()

	s.crashedHolder(dlock.Key("mem", "", id), 2*time.Second) // a dead worker holds the id

	// Keepalive is the pool's job (whole message lifetime): run through it.
	oldKA := keepaliveInterval
	keepaliveInterval = 500 * time.Millisecond
	defer func() { keepaliveInterval = oldKA }()

	msg := indexMsg(t, 3, memory.Record{ID: id, Text: "valid work", CreatedAt: time.Now()})
	msg.delivered = memory.MaxDeliver // last delivery: a Nak or failure would dead-letter it
	start := time.Now()
	process(context.Background(), msg, func(ctx context.Context, _ jetstream.Msg) {
		handleIndex(ctx, testLog, s.m, s.js, s.locker, s.embedder, s.stores, s.chunker, msg, 0)
	})

	assert.GreaterOrEqual(t, time.Since(start), time.Second, "must have waited for the lease to expire")
	got, ok := s.fs.peek("", id)
	require.True(t, ok)
	assert.Equal(t, "valid work", got.Text)
	assert.EqualValues(t, 1, msg.acked.Load())
	assert.Zero(t, msg.nakked.Load(), "contention must not NAK (every NAK costs a delivery)")
	assert.Zero(t, msg.termed.Load())
	assert.Zero(t, outcome(s.m, kindIndex, outcomeDead))
	assert.Zero(t, outcome(s.m, kindIndex, outcomeRetry))
	assert.GreaterOrEqual(t, msg.progress.Load(), int32(1), "must extend AckWait while waiting")
	s.assertNoLocksHeld()
}

func TestContendedLinkWaitsAndApplies(t *testing.T) {
	s := newStack(t, 1, 2*time.Second)
	a, b := uuid.NewString(), uuid.NewString()
	seed(t, s, "", memory.Record{ID: a, Text: "a"})
	seed(t, s, "", memory.Record{ID: b, Text: "b"})
	s.crashedHolder(dlock.Key("mem", "", b), 2*time.Second)

	msg := linkMsg(t, memory.LinkMsg{Op: memory.LinkOpAdd, A: a, B: b})
	handleLink(context.Background(), testLog, s.m, s.js, s.locker, s.stores, msg)
	ra, _ := s.fs.peek("", a)
	assert.Equal(t, []string{b}, ra.LinkedIDs)
	assert.EqualValues(t, 1, msg.acked.Load())
	assert.Zero(t, msg.nakked.Load())
}

// ---- review findings -------------------------------------------------------

// A NATS restart/wipe loses the lock bucket. Lock infrastructure errors must be
// repaired and waited out like contention: they must never reach the caller as an
// error that burns the message's delivery budget (here: already on its last
// delivery, so any failure path would dead-letter valid work).
func TestLockBucketLostMidFlightRecovers(t *testing.T) {
	s := newStack(t, 1, time.Minute)
	id := uuid.NewString()
	runIndex(t, s, indexMsg(t, 1, memory.Record{ID: id, Text: "v1", CreatedAt: time.Now()}))

	require.NoError(t, s.js.DeleteKeyValue(context.Background(), lockBucket))

	msg := indexMsg(t, 2, memory.Record{ID: id, Text: "v2", CreatedAt: time.Now()})
	msg.delivered = memory.MaxDeliver
	runIndex(t, s, msg)

	got, _ := s.fs.peek("", id)
	assert.Equal(t, "v2", got.Text)
	assert.EqualValues(t, 1, msg.acked.Load())
	assert.Zero(t, msg.nakked.Load())
	assert.Zero(t, outcome(s.m, kindIndex, outcomeDead), "lock infra failure must not dead-letter valid work")
	s.assertNoLocksHeld()

	// Links go through the same path.
	require.NoError(t, s.js.DeleteKeyValue(context.Background(), lockBucket))
	a, b := uuid.NewString(), uuid.NewString()
	seed(t, s, "", memory.Record{ID: a, Text: "a"})
	seed(t, s, "", memory.Record{ID: b, Text: "b"})
	lm := linkMsg(t, memory.LinkMsg{Op: memory.LinkOpAdd, A: a, B: b})
	handleLink(context.Background(), testLog, s.m, s.js, s.locker, s.stores, lm)
	ra, _ := s.fs.peek("", a)
	assert.Equal(t, []string{b}, ra.LinkedIDs)
	assert.Zero(t, lm.nakked.Load())
}

// One replica must not hoard a backlog: each replica holds at most `concurrency`
// messages (it pulls one only when a slot is free). With a backlog already queued
// and replica 1 busy, a replica that joins LATER still gets a fair share. (A
// streaming consume whose callback returns at once would have pulled everything
// into replica 1.)
func TestReplicasDoNotHoardBacklog(t *testing.T) {
	s := newStack(t, 1, time.Minute)
	s.fs.writeDelay = 250 * time.Millisecond
	ctx := tenantCtx("")
	for range 16 {
		require.NoError(t, bus.PublishIndex(ctx, s.js, memory.Record{ID: uuid.NewString(), Text: "x", CreatedAt: time.Now()}))
	}
	w1 := s.startWorker(1)
	time.Sleep(300 * time.Millisecond)
	w2 := s.startWorker(1)
	s.waitIdle()
	assert.GreaterOrEqual(t, w1.handled.Load(), int32(3), "worker 1 starved")
	assert.GreaterOrEqual(t, w2.handled.Load(), int32(3), "worker 2 starved: worker 1 hoarded the backlog")
	assert.EqualValues(t, 16, w1.handled.Load()+w2.handled.Load(), "each message handled exactly once")
}

// A handler that runs longer than AckWait must not be redelivered mid-flight (it
// would run twice concurrently and burn a delivery): the pool keeps the message
// alive for its whole life, including the embed and locked-store phases.
func TestSlowHandlerNotRedeliveredPastAckWait(t *testing.T) {
	oldKA := keepaliveInterval
	keepaliveInterval = 200 * time.Millisecond
	t.Cleanup(func() { keepaliveInterval = oldKA }) // runs after the worker stops

	s := newStack(t, 1, time.Minute)
	s.ackWait = time.Second
	s.fs.writeDelay = 3 * time.Second // > 2x AckWait, inside the locked store phase
	s.startWorker(2)                  // a second free loop is what would pick up an (undue) redelivery
	require.NoError(t, bus.PublishIndex(tenantCtx(""), s.js, memory.Record{ID: uuid.NewString(), Text: "slow", CreatedAt: time.Now()}))
	s.waitIdle()
	assert.EqualValues(t, 1, s.fs.writes.Load(), "message was redelivered while still being processed")
}

// Shutdown: an in-flight handler keeps a live context for the grace period so it
// can finish its commit, and only then is cancelled; a message fetched after the
// worker context was cancelled is handed back without running.
func TestProcessShutdown(t *testing.T) {
	old := shutdownGrace
	shutdownGrace = 400 * time.Millisecond
	defer func() { shutdownGrace = old }()

	ctx, cancel := context.WithCancel(context.Background())
	inflight := &fakeMsg{}
	started := make(chan struct{})
	var aliveDuringGrace, cancelledAfterGrace atomic.Bool
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		process(ctx, inflight, func(wctx context.Context, _ jetstream.Msg) {
			close(started)
			time.Sleep(150 * time.Millisecond) // well inside the grace
			aliveDuringGrace.Store(wctx.Err() == nil)
			<-wctx.Done()
			cancelledAfterGrace.Store(true)
		})
	}()
	<-started
	cancel()
	<-finished
	assert.True(t, aliveDuringGrace.Load(), "in-flight handler lost its context immediately on shutdown")
	assert.True(t, cancelledAfterGrace.Load(), "in-flight handler must be cancelled after the grace")

	late := &fakeMsg{}
	var ran atomic.Bool
	process(ctx, late, func(context.Context, jetstream.Msg) { ran.Store(true) })
	assert.False(t, ran.Load(), "a message fetched after shutdown must not start")
	assert.EqualValues(t, 1, late.nakked.Load(), "it must be handed back")
}

func TestProcessKeepsMessageAliveForItsWholeLife(t *testing.T) {
	old := keepaliveInterval
	keepaliveInterval = 20 * time.Millisecond
	defer func() { keepaliveInterval = old }()
	msg := &fakeMsg{}
	process(context.Background(), msg, func(context.Context, jetstream.Msg) { time.Sleep(200 * time.Millisecond) })
	assert.GreaterOrEqual(t, msg.progress.Load(), int32(5))
}

// A handler that fails only because the worker is shutting down must hand the
// message back, never settle it as a processing failure (on its last delivery that
// would dead-letter valid work).
func TestShutdownFailureIsNotDeadLettered(t *testing.T) {
	s := newStack(t, 1, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	msg := indexMsg(t, 3, memory.Record{ID: uuid.NewString(), Text: "x", CreatedAt: time.Now()})
	msg.delivered = memory.MaxDeliver
	handleIndex(ctx, testLog, s.m, s.js, s.locker, s.embedder, s.stores, s.chunker, msg, 0)
	assert.EqualValues(t, 1, msg.nakked.Load())
	assert.Zero(t, outcome(s.m, kindIndex, outcomeDead))
	assert.Zero(t, msg.acked.Load())

	lm := linkMsg(t, memory.LinkMsg{Op: memory.LinkOpAdd, A: uuid.NewString(), B: uuid.NewString()})
	handleLink(ctx, testLog, s.m, s.js, s.locker, s.stores, lm)
	assert.EqualValues(t, 1, lm.nakked.Load())
}

// Link ids that can never exist must not burn the 50-delivery retry budget.
func TestInvalidLinkIDsTerminate(t *testing.T) {
	s := newStack(t, 1, time.Minute)
	lm := linkMsg(t, memory.LinkMsg{Op: memory.LinkOpAdd, A: uuid.NewString(), B: "not-a-uuid"})
	handleLink(context.Background(), testLog, s.m, s.js, s.locker, s.stores, lm)
	assert.EqualValues(t, 1, lm.termed.Load())
	assert.Zero(t, lm.nakked.Load())
	assert.EqualValues(t, 1, outcome(s.m, kindLink, outcomeTerm))
}

// A critical section cut off by the hold deadline is counted and retried (not
// dead-lettered, not acked).
func TestHoldDeadlineIsCountedAndRetried(t *testing.T) {
	s := newStack(t, 1, 2*time.Second) // hold = 1.5s
	s.fs.writeDelay = 10 * time.Second
	msg := indexMsg(t, 3, memory.Record{ID: uuid.NewString(), Text: "slow store", CreatedAt: time.Now()})
	runIndex(t, s, msg)
	assert.EqualValues(t, 1, testutil.ToFloat64(s.m.holdTimeouts))
	assert.EqualValues(t, 1, msg.nakked.Load(), "retried")
	assert.Zero(t, msg.acked.Load())
	assert.Zero(t, outcome(s.m, kindIndex, outcomeDead))
	s.assertNoLocksHeld()
}

// Removes keep their old meaning with a non-UUID endpoint: nothing to clear on the
// missing side, but the edge is still removed from the valid side. Adds terminate.
func TestLinkRemoveWithInvalidEndpoint(t *testing.T) {
	s := newStack(t, 1, time.Minute)
	b := uuid.NewString()
	seed(t, s, "", memory.Record{ID: b, Text: "b", LinkedIDs: []string{"bad-id", "x"}})

	rm := linkMsg(t, memory.LinkMsg{Op: memory.LinkOpRemove, A: "bad-id", B: b})
	handleLink(context.Background(), testLog, s.m, s.js, s.locker, s.stores, rm)
	got, _ := s.fs.peek("", b)
	assert.Equal(t, []string{"x"}, got.LinkedIDs, "dangling link to a bad id must be removable")
	assert.EqualValues(t, 1, rm.acked.Load())
	assert.Zero(t, rm.termed.Load())

	add := linkMsg(t, memory.LinkMsg{Op: memory.LinkOpAdd, A: "bad-id", B: b})
	handleLink(context.Background(), testLog, s.m, s.js, s.locker, s.stores, add)
	assert.EqualValues(t, 1, add.termed.Load())
}

// MEMORY retains by interest: acked history is gone, so a deleted durable consumer
// can be recreated without replaying anything (no memory re-embedded, no link op
// re-run). The worker therefore recreates it on its own, and a wiped STREAM is
// recovered the same way.
func TestConsumerDeletionRecreatesWithoutReplay(t *testing.T) {
	s := newStack(t, 1, time.Minute)
	s.startWorker(1)
	ctx := tenantCtx("")
	first, third := uuid.NewString(), uuid.NewString()
	require.NoError(t, bus.PublishIndex(ctx, s.js, memory.Record{ID: first, Text: "one", CreatedAt: time.Now()}))
	s.waitIdle()
	require.EqualValues(t, 1, s.fs.writes.Load())

	require.NoError(t, s.js.DeleteConsumer(context.Background(), memory.StreamName, memory.ConsumerName))
	require.Eventually(t, func() bool {
		_, err := s.js.Consumer(context.Background(), memory.StreamName, memory.ConsumerName)
		return err == nil
	}, 30*time.Second, 100*time.Millisecond, "worker never recreated its consumer")
	time.Sleep(500 * time.Millisecond)
	assert.EqualValues(t, 1, s.fs.writes.Load(), "recreating the consumer must not replay acked history")

	// Stream wiped: nothing to replay, so the worker re-ensures stream + consumer.
	require.NoError(t, s.js.DeleteStream(context.Background(), memory.StreamName))
	require.Eventually(t, func() bool {
		_, err := s.js.Consumer(context.Background(), memory.StreamName, memory.ConsumerName)
		return err == nil
	}, 30*time.Second, 100*time.Millisecond, "worker never recovered from a wiped stream")
	require.NoError(t, bus.PublishIndex(ctx, s.js, memory.Record{ID: third, Text: "three", CreatedAt: time.Now()}))
	require.Eventually(t, func() bool { _, ok := s.fs.peek("", third); return ok },
		30*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 2, s.fs.writes.Load(), "only the new message is indexed after recovery")
}

func TestFetchHealthTurnsUnreadyAfterContinuousFailure(t *testing.T) {
	h := &fetchHealth{}
	assert.NoError(t, h.err(50*time.Millisecond), "healthy at start")
	h.fail()
	assert.NoError(t, h.err(50*time.Millisecond), "a brief blip is tolerated")
	time.Sleep(80 * time.Millisecond)
	assert.Error(t, h.err(50*time.Millisecond), "continuous failure must fail readiness")
	h.ok()
	assert.NoError(t, h.err(50*time.Millisecond), "one successful pull restores readiness")
}

// A superseded source must be deleted WITH the trash reason naming its replacement
// (so a bad consolidation is recoverable from the trash and attributable); the
// merged record itself is indexed normally.
func TestSupersedesGoesToTrash(t *testing.T) {
	s := newStack(t, 1, time.Minute)
	id, victim := uuid.NewString(), uuid.NewString()
	seed(t, s, "", memory.Record{ID: victim, Text: "src"})
	rec := memory.Record{ID: id, Text: "merged", Namespace: "ns", CreatedAt: time.Now()}
	rec.Supersedes = []string{victim}
	runIndex(t, s, indexMsg(t, 1, rec))

	_, ok := s.fs.peek("", victim)
	assert.False(t, ok, "source is deleted from the live store")
	assert.Equal(t, []string{victim + " supersedes:" + id}, s.fs.deleteReasons)
}

// Unlink/purge-time removals must tolerate a MISSING (valid-UUID) endpoint: the
// edge is still cleared from the side that exists, and the message is acked (not
// retried), so purging a trashed memory strips the dangling link on its neighbour.
func TestLinkRemoveWithMissingEndpoint(t *testing.T) {
	s := newStack(t, 1, time.Minute)
	b, gone := uuid.NewString(), uuid.NewString()
	seed(t, s, "", memory.Record{ID: b, Text: "b", LinkedIDs: []string{gone, "x"}})

	rm := linkMsg(t, memory.LinkMsg{Op: memory.LinkOpRemove, A: gone, B: b})
	handleLink(context.Background(), testLog, s.m, s.js, s.locker, s.stores, rm)
	got, _ := s.fs.peek("", b)
	assert.Equal(t, []string{"x"}, got.LinkedIDs)
	assert.EqualValues(t, 1, rm.acked.Load())
	assert.Zero(t, rm.nakked.Load())
}
