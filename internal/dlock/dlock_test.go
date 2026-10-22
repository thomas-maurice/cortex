package dlock

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/natstest"
)

func newLocker(t *testing.T, ttl time.Duration, owner string) *Locker {
	t.Helper()
	_, js := natstest.Start(t)
	l, err := New(context.Background(), js, "locks", ttl, owner)
	require.NoError(t, err)
	return l
}

// Mutual exclusion is the whole point: if two holders can be inside the critical
// section at once, concurrent workers lose each other's link edits.
func TestMutualExclusion(t *testing.T) {
	t.Parallel()
	l := newLocker(t, time.Minute, "w")
	ctx := context.Background()
	var inside, maxInside, total atomic.Int32
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			rel, err := l.Acquire(ctx, "k", 30*time.Second)
			if !assert.NoError(t, err) {
				return
			}
			n := inside.Add(1)
			for {
				cur := maxInside.Load()
				if n <= cur || maxInside.CompareAndSwap(cur, n) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			total.Add(1)
			inside.Add(-1)
			assert.NoError(t, rel())
		})
	}
	wg.Wait()
	assert.Equal(t, int32(12), total.Load())
	assert.Equal(t, int32(1), maxInside.Load(), "more than one holder inside the critical section")
}

func TestSecondAcquireTimesOutWhileHeld(t *testing.T) {
	t.Parallel()
	l := newLocker(t, time.Minute, "w")
	ctx := context.Background()
	rel, err := l.Acquire(ctx, "k", time.Second)
	require.NoError(t, err)

	_, err = l.Acquire(ctx, "k", 300*time.Millisecond)
	require.ErrorIs(t, err, ErrTimeout)

	require.NoError(t, rel())
	rel2, err := l.Acquire(ctx, "k", time.Second)
	require.NoError(t, err, "lock must be free after release")
	require.NoError(t, rel2())
}

// A holder whose lock expired (and was re-taken) must not be able to delete the
// new holder's lock when it finally calls release: otherwise a slow, stale holder
// would silently drop someone else's mutual exclusion.
func TestReleaseOnlyByOwnerRevision(t *testing.T) {
	t.Parallel()
	l := newLocker(t, 700*time.Millisecond, "w")
	ctx := context.Background()
	staleRel, err := l.Acquire(ctx, "k", time.Second)
	require.NoError(t, err)

	// Waiting for the TTL to lapse is exactly what Acquire does.
	newRel, err := l.Acquire(ctx, "k", 10*time.Second)
	require.NoError(t, err, "expired lock must be re-acquirable")

	require.NoError(t, staleRel(), "stale release is a harmless no-op")
	_, err = l.Acquire(ctx, "k", 200*time.Millisecond)
	require.ErrorIs(t, err, ErrTimeout, "stale release must NOT have freed the new holder's lock")

	require.NoError(t, newRel())
	rel, err := l.Acquire(ctx, "k", time.Second)
	require.NoError(t, err)
	require.NoError(t, rel())
}

// A crashed holder never releases; the bucket TTL is the only thing that frees
// the id.
func TestTTLExpiryFreesCrashedHolder(t *testing.T) {
	t.Parallel()
	l := newLocker(t, time.Second, "dead-worker")
	ctx := context.Background()
	_, err := l.Acquire(ctx, "k", time.Second) // never released
	require.NoError(t, err)

	start := time.Now()
	rel, err := l.Acquire(ctx, "k", 10*time.Second)
	require.NoError(t, err, "TTL expiry must let a contender in")
	assert.GreaterOrEqual(t, time.Since(start), 500*time.Millisecond, "must have actually waited for expiry")
	require.NoError(t, rel())
}

func TestContextCancelStopsWaiting(t *testing.T) {
	t.Parallel()
	l := newLocker(t, time.Minute, "w")
	rel, err := l.Acquire(context.Background(), "k", time.Second)
	require.NoError(t, err)
	defer func() { _ = rel() }()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = l.Acquire(ctx, "k", time.Minute)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// Sorted acquisition makes opposing multi-key lockers deadlock-free: A wants (x,y),
// B wants (y,x). Without a global order each would hold one and wait forever.
func TestAcquireAllIsDeadlockFree(t *testing.T) {
	t.Parallel()
	l := newLocker(t, time.Minute, "w")
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := range 20 {
		keys := []string{"x", "y"}
		if i%2 == 1 {
			keys = []string{"y", "x"}
		}
		wg.Go(func() {
			_, rel, err := l.AcquireAll(ctx, keys, 30*time.Second)
			if assert.NoError(t, err) {
				time.Sleep(5 * time.Millisecond)
				rel()
			}
		})
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("deadlock")
	}
}

func TestAcquireAllReleasesOnFailureAndDedups(t *testing.T) {
	t.Parallel()
	l := newLocker(t, time.Minute, "w")
	ctx := context.Background()
	blocker, err := l.Acquire(ctx, "b", time.Second)
	require.NoError(t, err)

	_, _, err = l.AcquireAll(ctx, []string{"a", "b"}, 200*time.Millisecond)
	require.ErrorIs(t, err, ErrTimeout)
	// "a" was taken first, then "b" failed: "a" must have been given back.
	rel, err := l.Acquire(ctx, "a", 200*time.Millisecond)
	require.NoError(t, err, "partial acquisition leaked a lock")
	require.NoError(t, rel())
	require.NoError(t, blocker())

	_, relAll, err := l.AcquireAll(ctx, []string{"a", "a"}, time.Second)
	require.NoError(t, err, "a duplicate key (self-link) must not self-deadlock")
	relAll()
}

func TestKeyHashedAndCollisionFree(t *testing.T) {
	t.Parallel()
	pairs := [][2]string{
		{"", "abc"}, {"t1", "id"}, {"t2", "id"}, {"t", "a.b"}, {"t", "a_b"}, {"t", "a b"},
		{"a", "b.c"}, {"a.b", "c"}, {"ab", "c"}, {"a", "bc"},
		{"t", strings.Repeat("x", 190)}, {"t", strings.Repeat("x", 5000)}, {"t", strings.Repeat("x", 5000) + "y"},
	}
	seen := map[string][2]string{}
	for _, p := range pairs {
		k := Key("mem", p[0], p[1])
		prev, dup := seen[k]
		assert.False(t, dup, "%v and %v collide on key %q", p, prev, k)
		seen[k] = p
		assert.Equal(t, k, Key("mem", p[0], p[1]), "same pair must give the same key")
		assert.Len(t, k, len("mem.")+64, "key length is constant regardless of input")
	}
	assert.NotEqual(t, Key("mem", "t", "id"), Key("sum", "t", "id"), "memory and summary locks are distinct")
}

func TestKeyIsValidKVKey(t *testing.T) {
	t.Parallel()
	l := newLocker(t, time.Minute, "w")
	ctx := context.Background()
	for _, id := range []string{"a b", "a/b*", "a>b", "ünï", "x.y", strings.Repeat("x", 5000), fmt.Sprintf("%x", "weird\x00\n")} {
		key := Key("mem", "ten ant>", id)
		rel, err := l.Acquire(ctx, key, time.Second)
		require.NoError(t, err, "key %q must be a valid KV key", key)
		require.NoError(t, rel())
	}
}

func TestBucketRecreatedAfterLoss(t *testing.T) {
	t.Parallel()
	_, js := natstest.Start(t)
	ctx := context.Background()
	l, err := New(ctx, js, "locks", time.Minute, "w")
	require.NoError(t, err)
	rel, err := l.Acquire(ctx, "k", time.Second)
	require.NoError(t, err)

	require.NoError(t, js.DeleteKeyValue(ctx, "locks")) // NATS wiped / bucket lost
	require.NoError(t, rel(), "releasing a lock whose bucket vanished is a no-op")

	rel2, err := l.Acquire(ctx, "k", 10*time.Second)
	require.NoError(t, err, "Acquire must repair the bucket and succeed, not fail the caller")
	_, err = l.Acquire(ctx, "k", 200*time.Millisecond)
	require.ErrorIs(t, err, ErrTimeout, "recreated bucket must still provide exclusion")
	require.NoError(t, rel2())
}

// The critical section must end before the EARLIEST lease it depends on expires.
// Waiting a long time for the second key must not let the first lease lapse while
// the section still runs: the hold deadline is measured from the first lease and
// a too-slow multi-key acquisition is abandoned (everything released) and retried.
func TestAcquireAllDeadlineFromFirstLease(t *testing.T) {
	t.Parallel()
	_, js := natstest.Start(t)
	ctx := context.Background()
	const ttl = 3 * time.Second
	l, err := New(ctx, js, "locks", ttl, "w")
	require.NoError(t, err)
	other, err := New(ctx, js, "locks", ttl, "other")
	require.NoError(t, err)
	_, err = other.Acquire(ctx, "b", time.Second) // never released: b frees at ~ttl
	require.NoError(t, err)

	hctx, rel, err := l.AcquireAll(ctx, []string{"a", "b"}, 30*time.Second)
	require.NoError(t, err)
	defer rel()
	dl, ok := hctx.Deadline()
	require.True(t, ok)
	assert.Less(t, time.Until(dl), l.Hold()-500*time.Millisecond,
		"hold window must be anchored at the first lease, not at the last acquisition")
	assert.Positive(t, time.Until(dl))

	// "a" must have been given up and re-taken while waiting on "b", i.e. be held by us now.
	_, err = l.Acquire(ctx, "a", 200*time.Millisecond)
	require.ErrorIs(t, err, ErrTimeout)
}

func TestInfraErrorsAreLoggedAndCounted(t *testing.T) {
	t.Parallel()
	_, js := natstest.Start(t)
	ctx := context.Background()
	l, err := New(ctx, js, "locks", time.Minute, "w")
	require.NoError(t, err)
	var infra, waits atomic.Int32
	var buf syncBuf
	l.Instrument(slog.New(slog.NewTextHandler(&buf, nil)), func() { infra.Add(1) }, func(time.Duration) { waits.Add(1) })

	require.NoError(t, js.DeleteKeyValue(ctx, "locks"))
	rel, err := l.Acquire(ctx, "k", 10*time.Second)
	require.NoError(t, err)
	require.NoError(t, rel())
	assert.GreaterOrEqual(t, infra.Load(), int32(1))
	assert.EqualValues(t, 1, waits.Load())
	out := buf.String()
	assert.Contains(t, out, "lock infrastructure error")
	assert.Contains(t, out, "lock bucket re-created")
	assert.Equal(t, 1, strings.Count(out, "lock infrastructure error"), "warn only on the first infra error of an Acquire")
}

type syncBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// If every attempt exhausts the lease budget, AcquireAll must give up at the
// overall wait with ErrTimeout instead of retrying forever (which would pin a
// worker slot, with keepalives, indefinitely).
func TestAcquireAllTerminatesWhenLeaseBudgetAlwaysExpires(t *testing.T) {
	t.Parallel()
	l := newLocker(t, time.Minute, "w")
	l.ttl = 0 // Hold() == 0: the budget is spent before the section can start
	done := make(chan error, 1)
	go func() {
		_, _, err := l.AcquireAll(context.Background(), []string{"a", "b"}, 700*time.Millisecond)
		done <- err
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrTimeout)
	case <-time.After(10 * time.Second):
		t.Fatal("AcquireAll did not terminate within the overall wait")
	}
}
