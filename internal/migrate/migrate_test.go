package migrate

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/natstest"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func noop(context.Context, *Env) error { return nil }

// The registry is the schema's history. A reordered, renumbered or duplicated ID
// would make a store at version N skip or re-run the wrong migrations, silently
// corrupting an upgrade; this test is what stops a bad merge from shipping.
func TestRegistryIsOrdered(t *testing.T) {
	require.NoError(t, validate(registry))
	assert.Equal(t, registry[len(registry)-1].ID, Latest())
	assert.Equal(t, 1, registry[0].ID, "history starts at 1; 0 means 'nothing applied'")
}

func TestValidateRejectsBadHistories(t *testing.T) {
	cases := map[string][]Migration{
		"duplicate":  {{ID: 1, Name: "a", Up: noop}, {ID: 1, Name: "b", Up: noop}},
		"descending": {{ID: 2, Name: "a", Up: noop}, {ID: 1, Name: "b", Up: noop}},
		"zero id":    {{ID: 0, Name: "a", Up: noop}},
		"no name":    {{ID: 1, Up: noop}},
		"no up":      {{ID: 1, Name: "a"}},
	}
	for name, ms := range cases {
		t.Run(name, func(t *testing.T) { assert.Error(t, validate(ms)) })
	}
}

// testRunner builds a Runner over an embedded NATS with fake migrations and no
// Weaviate (EnsureSchema stubbed): everything here is about ordering, state and
// locking, not schema work.
func testRunner(t *testing.T, js jetstream.JetStream, owner string, ms []Migration) *Runner {
	t.Helper()
	kv, err := OpenMeta(context.Background(), js)
	require.NoError(t, err)
	env := &Env{Log: quiet(), Maint: NewMaintenance(kv, owner, quiet()), DrainTimeout: -1}
	r := NewRunner(kv, env, owner)
	r.Migrations = ms
	r.EnsureSchema = func(context.Context) error { return nil }
	r.Poll = 10 * time.Millisecond
	r.LeaseTTL = 300 * time.Millisecond
	return r
}

func recorder(order *[]int, mu *sync.Mutex, ids ...int) []Migration {
	var ms []Migration
	for _, id := range ids {
		ms = append(ms, Migration{ID: id, Name: "m", Up: func(context.Context, *Env) error {
			mu.Lock()
			*order = append(*order, id)
			mu.Unlock()
			return nil
		}})
	}
	return ms
}

func TestRunnerAppliesPendingInOrderOnce(t *testing.T) {
	_, js := natstest.Start(t)
	var order []int
	var mu sync.Mutex
	r := testRunner(t, js, "a", recorder(&order, &mu, 1, 2, 3))
	ctx := context.Background()

	require.NoError(t, r.Run(ctx))
	assert.Equal(t, []int{1, 2, 3}, order)

	v, _, err := SchemaVersion(ctx, r.KV)
	require.NoError(t, err)
	assert.Equal(t, 3, v)
	st, err := r.Status(ctx)
	require.NoError(t, err)
	require.Len(t, st.Migrations, 3)
	for _, m := range st.Migrations {
		assert.True(t, m.Applied, "migration %d must be recorded", m.ID)
		assert.False(t, m.AppliedAt.IsZero())
	}
	assert.Equal(t, 3, st.SchemaVersion)

	// A restart (or a second replica) finds nothing to do: re-running a migration
	// that rebuilt a class would be wasted downtime at best.
	require.NoError(t, r.Run(ctx))
	assert.Equal(t, []int{1, 2, 3}, order)

	// A new release only runs the new tail.
	r.Migrations = append(r.Migrations, recorder(&order, &mu, 4)...)
	require.NoError(t, r.Run(ctx))
	assert.Equal(t, []int{1, 2, 3, 4}, order)
}

// Two replicas booting together must not both run a migration: both would copy
// the same class into the same target concurrently.
func TestConcurrentRunnersApplyEachMigrationOnce(t *testing.T) {
	_, js := natstest.Start(t)
	var runs atomic.Int32
	slow := []Migration{{ID: 1, Name: "slow", Up: func(ctx context.Context, _ *Env) error {
		runs.Add(1)
		time.Sleep(400 * time.Millisecond) // longer than LeaseTTL: forces lease refreshes
		return nil
	}}}
	a, b := testRunner(t, js, "a", slow), testRunner(t, js, "b", slow)

	var wg sync.WaitGroup
	for _, r := range []*Runner{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, r.Run(context.Background()))
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, runs.Load(), "exactly one replica applies the migration, the other waits and finds it done")
}

// A failed migration must (a) not advance the version, so the next boot resumes,
// (b) surface as an error so the server refuses to serve, and (c) never leave the
// maintenance flag set -- that would freeze every writer in the cluster.
func TestFailedMigrationKeepsVersionAndClearsMaintenance(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := context.Background()
	fail := true
	ms := []Migration{
		{ID: 1, Name: "ok", Up: noop},
		{ID: 2, Name: "flaky", Up: func(ctx context.Context, env *Env) error {
			if err := env.Maint.Enable(ctx, "test"); err != nil {
				return err
			}
			if fail {
				return assert.AnError
			}
			return nil
		}},
	}
	r := testRunner(t, js, "a", ms)

	err := r.Run(ctx)
	require.ErrorIs(t, err, assert.AnError)
	v, _, _ := SchemaVersion(ctx, r.KV)
	assert.Equal(t, 1, v, "only the migration that finished is recorded")
	st, err := r.Env.Maint.Get(ctx)
	require.NoError(t, err)
	assert.Nil(t, st, "maintenance flag must be cleared on failure")
	assert.Empty(t, holder(ctx, r.KV), "the lock must be released on failure")

	fail = false
	require.NoError(t, r.Run(ctx), "the next run resumes at the failed migration")
	v, _, _ = SchemaVersion(ctx, r.KV)
	assert.Equal(t, 2, v)
}

// A holder that dies without releasing must not wedge the cluster forever.
func TestLeaseIsTakenOverAfterExpiry(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := context.Background()
	kv, err := OpenMeta(ctx, js)
	require.NoError(t, err)

	dead := &lease{kv: kv, owner: "dead", ttl: 100 * time.Millisecond}
	ok, _, err := dead.tryAcquire(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	live := &lease{kv: kv, owner: "live", ttl: time.Minute}
	ok, who, err := live.tryAcquire(ctx)
	require.NoError(t, err)
	assert.False(t, ok, "an unexpired lease is respected")
	assert.Equal(t, "dead", who)

	time.Sleep(150 * time.Millisecond)
	ok, _, err = live.tryAcquire(ctx)
	require.NoError(t, err)
	assert.True(t, ok, "an expired lease is taken over")
	assert.Error(t, dead.refresh(ctx), "the old holder learns it lost the lease")
}

func TestMaintenanceFlag(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := context.Background()
	kv, err := OpenMeta(ctx, js)
	require.NoError(t, err)
	owner := NewMaintenance(kv, "owner", quiet())
	owner.CacheFor = 0
	reader := NewMaintenance(kv, "reader", quiet())
	reader.CacheFor = 0

	assert.False(t, reader.Active(ctx))
	require.NoError(t, owner.Enable(ctx, "copying"))
	assert.True(t, reader.Active(ctx), "other processes see the flag")

	reader.Disable(ctx)
	assert.True(t, reader.Active(ctx), "only the owner can clear it")

	owner.Disable(ctx)
	assert.False(t, reader.Active(ctx))
	owner.Disable(ctx) // idempotent
}

// If the migrator dies mid-copy the flag must lapse on its own (stale-flag guard),
// otherwise the worker would stay paused until someone edits NATS by hand.
func TestMaintenanceFlagLapsesAndIsRenewed(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := context.Background()
	kv, err := OpenMeta(ctx, js)
	require.NoError(t, err)

	m := NewMaintenance(kv, "owner", quiet())
	m.TTL, m.CacheFor = 300*time.Millisecond, 0
	require.NoError(t, m.Enable(ctx, "x"))
	time.Sleep(700 * time.Millisecond) // > 2 TTLs: only the renewer keeps it alive
	assert.True(t, m.Active(ctx), "a live migrator keeps renewing the flag")

	// Simulate a crash: stop renewing without clearing.
	m.mu.Lock()
	m.stopRenew()
	m.mu.Unlock()
	time.Sleep(500 * time.Millisecond)
	assert.False(t, m.Active(ctx), "a dead migrator's flag lapses")
}

func TestRunClearsFlagLeftByCrashedMigrator(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := context.Background()
	r := testRunner(t, js, "new", nil)
	crashed := NewMaintenance(r.KV, "crashed", quiet())
	require.NoError(t, crashed.Enable(ctx, "x"))
	crashed.mu.Lock()
	crashed.stopRenew() // the old process is gone: nobody renews
	crashed.mu.Unlock()

	require.NoError(t, r.Run(ctx))
	st, err := r.Env.Maint.Get(ctx)
	require.NoError(t, err)
	assert.Nil(t, st, "a new lock holder clears the previous holder's flag at once")
}

func TestWaitUntilCurrent(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := context.Background()
	kv, err := OpenMeta(ctx, js)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- WaitUntilCurrent(ctx, kv, 2, quiet(), 10*time.Millisecond) }()
	select {
	case err := <-done:
		t.Fatalf("must keep waiting while the schema is older than the worker's (returned %v)", err)
	case <-time.After(150 * time.Millisecond):
	}
	require.NoError(t, setSchemaVersion(ctx, kv, 2, 0))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("did not return once the schema reached the needed version")
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	assert.ErrorIs(t, WaitUntilCurrent(cctx, kv, 99, quiet(), time.Millisecond), context.Canceled)
}
