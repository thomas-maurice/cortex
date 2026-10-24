package migrate

import (
	"context"
	"encoding/json"
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
	r.Meta = newFakeMeta()
	r.EnsureMetaClass = func(context.Context) error { return nil }
	// An existing store of unknown version: every migration runs from 0.
	r.ClassExists = func(context.Context) (bool, error) { return true, nil }
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

	v, err := SchemaVersion(ctx, r.Meta)
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
	b.Meta = a.Meta // replicas share one durable store

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
	v, _ := SchemaVersion(ctx, r.Meta)
	assert.Equal(t, 1, v, "only the migration that finished is recorded")
	st, err := r.Env.Maint.Get(ctx)
	require.NoError(t, err)
	assert.Nil(t, st, "maintenance flag must be cleared on failure")
	assert.Empty(t, holder(ctx, r.KV), "the lock must be released on failure")

	fail = false
	require.NoError(t, r.Run(ctx), "the next run resumes at the failed migration")
	v, _ = SchemaVersion(ctx, r.Meta)
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
	ctx := context.Background()
	kv := newFakeMeta()

	done := make(chan error, 1)
	go func() { done <- WaitUntilCurrent(ctx, kv, 2, quiet(), 10*time.Millisecond) }()
	select {
	case err := <-done:
		t.Fatalf("must keep waiting while the schema is older than the worker's (returned %v)", err)
	case <-time.After(150 * time.Millisecond):
	}
	require.NoError(t, setSchemaVersion(ctx, kv, 2))
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

func countingMigrations(runs *[]int, mu *sync.Mutex, ids ...int) []Migration {
	return recorder(runs, mu, ids...)
}

// A brand-new store is born current, so running the history on it is pointless
// work (and, for a rebuild migration, pointless downtime). It must be stamped at
// Latest, with records flagged "assumed" so `migrate status` does not claim they ran.
func TestFreshStoreIsStampedCurrentWithoutRunning(t *testing.T) {
	_, js := natstest.Start(t)
	var order []int
	var mu sync.Mutex
	r := testRunner(t, js, "a", countingMigrations(&order, &mu, 1, 2))
	r.ClassExists = func(context.Context) (bool, error) { return false, nil }
	ctx := context.Background()

	require.NoError(t, r.Run(ctx))
	assert.Empty(t, order, "nothing may run on a fresh store")
	v, err := SchemaVersion(ctx, r.Meta)
	require.NoError(t, err)
	assert.Equal(t, 2, v)
	st, err := r.Status(ctx)
	require.NoError(t, err)
	for _, m := range st.Migrations {
		assert.True(t, m.Applied)
		assert.True(t, m.Assumed, "migration %d was never run: status must say so", m.ID)
	}
}

// Data exists but the version is lost (NATS reset before versions lived in
// Weaviate). Assuming Latest could silently skip a migration on an old store, so
// everything re-runs (idempotent migrations no-op on a current store).
func TestExistingStoreWithUnknownVersionRerunsEverything(t *testing.T) {
	_, js := natstest.Start(t)
	var order []int
	var mu sync.Mutex
	r := testRunner(t, js, "a", countingMigrations(&order, &mu, 1, 2))
	ctx := context.Background()

	require.NoError(t, r.Run(ctx))
	assert.Equal(t, []int{1, 2}, order)
	v, err := SchemaVersion(ctx, r.Meta)
	require.NoError(t, err)
	assert.Equal(t, 2, v)
	st, err := r.Status(ctx)
	require.NoError(t, err)
	assert.False(t, st.Migrations[0].Assumed, "these really ran")
}

// Wiping NATS must lose nothing durable: a fresh KV over the same durable store
// finds the version and applies nothing, and Status still shows the history.
func TestNatsWipeKeepsSchemaState(t *testing.T) {
	_, js1 := natstest.Start(t)
	var order []int
	var mu sync.Mutex
	ms := countingMigrations(&order, &mu, 1, 2)
	r1 := testRunner(t, js1, "a", ms)
	ctx := context.Background()
	require.NoError(t, r1.Run(ctx))
	require.Equal(t, []int{1, 2}, order)

	_, js2 := natstest.Start(t) // brand-new NATS
	r2 := testRunner(t, js2, "b", ms)
	r2.Meta = r1.Meta
	require.NoError(t, r2.Run(ctx))
	assert.Equal(t, []int{1, 2}, order, "no migration may re-run after a NATS wipe")
	st, err := r2.Status(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, st.SchemaVersion)
	for _, m := range st.Migrations {
		assert.True(t, m.Applied)
		assert.False(t, m.AppliedAt.IsZero())
	}
	require.NoError(t, WaitUntilCurrent(ctx, r1.Meta, 2, quiet(), time.Millisecond))
}

// State written by older builds into the KV is moved to the durable store once,
// verbatim (AppliedAt survives), and the KV keys stay for rollback.
func TestBackfillFromLegacyKV(t *testing.T) {
	_, js := natstest.Start(t)
	var order []int
	var mu sync.Mutex
	r := testRunner(t, js, "a", countingMigrations(&order, &mu, 1, 2))
	ctx := context.Background()
	when := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	for _, id := range []int{1, 2} {
		rec, _ := json.Marshal(Applied{ID: id, Name: "m", AppliedAt: when, Duration: 3 * time.Second})
		_, err := r.KV.Put(ctx, appliedKey(id), rec)
		require.NoError(t, err)
	}
	_, err := r.KV.Put(ctx, keySchemaVersion, []byte("2"))
	require.NoError(t, err)

	require.NoError(t, r.Run(ctx))
	assert.Empty(t, order, "the legacy version says everything is applied")
	st, err := r.Status(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, st.SchemaVersion)
	for _, m := range st.Migrations {
		assert.True(t, when.Equal(m.AppliedAt), "original AppliedAt preserved")
		assert.Equal(t, 3*time.Second, m.Duration)
	}
	_, err = r.KV.Get(ctx, keySchemaVersion)
	assert.NoError(t, err, "legacy keys are kept so an older binary keeps working")

	// Once the durable store has a version the KV is never consulted again.
	_, err = r.KV.Put(ctx, keySchemaVersion, []byte("1"))
	require.NoError(t, err)
	require.NoError(t, r.Run(ctx))
	v, err := SchemaVersion(ctx, r.Meta)
	require.NoError(t, err)
	assert.Equal(t, 2, v)
}

// Crash after EnsureSchema created the classes but before the version was stamped:
// on retry Memory exists, yet the store is still fresh (born current). Re-running
// every migration from 0 there would rebuild an empty store for nothing.
func TestFreshBootCrashMidStampStaysFresh(t *testing.T) {
	_, js := natstest.Start(t)
	var order []int
	var mu sync.Mutex
	r := testRunner(t, js, "a", countingMigrations(&order, &mu, 1, 2))
	fm := r.Meta.(*fakeMeta)
	fm.failAfter = 2 // marker + first assumed record, then the stamp breaks
	r.ClassExists = func(context.Context) (bool, error) { return false, nil }
	ctx := context.Background()
	require.Error(t, r.Run(ctx))

	fm.heal()
	r.ClassExists = func(context.Context) (bool, error) { return true, nil } // EnsureSchema made it
	require.NoError(t, r.Run(ctx))
	assert.Empty(t, order, "no Up on a store that was fresh")
	v, err := SchemaVersion(ctx, r.Meta)
	require.NoError(t, err)
	assert.Equal(t, 2, v)
	st, err := r.Status(ctx)
	require.NoError(t, err)
	for _, m := range st.Migrations {
		assert.True(t, m.Assumed)
	}
	_, pending, _ := fm.GetMeta(ctx, keyFreshPending)
	assert.False(t, pending, "the marker is removed once the version is stamped")
}

// A fresh Weaviate with a legacy KV version: the real history wins over stamping.
func TestBackfillBeatsFreshStamp(t *testing.T) {
	_, js := natstest.Start(t)
	var order []int
	var mu sync.Mutex
	r := testRunner(t, js, "a", countingMigrations(&order, &mu, 1, 2))
	r.ClassExists = func(context.Context) (bool, error) { return false, nil }
	ctx := context.Background()
	when := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	for _, id := range []int{1, 2} {
		rec, _ := json.Marshal(Applied{ID: id, Name: "m", AppliedAt: when})
		_, err := r.KV.Put(ctx, appliedKey(id), rec)
		require.NoError(t, err)
	}
	_, err := r.KV.Put(ctx, keySchemaVersion, []byte("2"))
	require.NoError(t, err)

	require.NoError(t, r.Run(ctx))
	assert.Empty(t, order)
	st, err := r.Status(ctx)
	require.NoError(t, err)
	for _, m := range st.Migrations {
		assert.False(t, m.Assumed)
		assert.True(t, when.Equal(m.AppliedAt))
	}
	_, pending, _ := r.Meta.GetMeta(ctx, keyFreshPending)
	assert.False(t, pending)
}

// A crash halfway through the KV -> CortexMeta copy must resume, not stamp or re-run.
func TestBackfillCrashMidCopyResumes(t *testing.T) {
	_, js := natstest.Start(t)
	var order []int
	var mu sync.Mutex
	r := testRunner(t, js, "a", countingMigrations(&order, &mu, 1, 2))
	fm := r.Meta.(*fakeMeta)
	ctx := context.Background()
	when := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	for _, id := range []int{1, 2} {
		rec, _ := json.Marshal(Applied{ID: id, Name: "m", AppliedAt: when})
		_, err := r.KV.Put(ctx, appliedKey(id), rec)
		require.NoError(t, err)
	}
	_, err := r.KV.Put(ctx, keySchemaVersion, []byte("2"))
	require.NoError(t, err)

	fm.failAfter = 1
	require.Error(t, r.Run(ctx))
	_, found, _ := fm.GetMeta(ctx, keySchemaVersion)
	assert.False(t, found, "the version is written last, so a partial copy is redone")

	fm.heal()
	require.NoError(t, r.Run(ctx))
	assert.Empty(t, order)
	st, err := r.Status(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, st.SchemaVersion)
	for _, m := range st.Migrations {
		assert.True(t, m.Applied)
		assert.False(t, m.Assumed)
	}
}

// Rolling upgrade: an old-build worker's KV-based WaitUntilCurrent must see the
// version the new server just applied.
func TestVersionMirroredToKVForOldBinaries(t *testing.T) {
	_, js := natstest.Start(t)
	var order []int
	var mu sync.Mutex
	r := testRunner(t, js, "a", countingMigrations(&order, &mu, 1, 2))
	ctx := context.Background()
	require.NoError(t, r.Run(ctx))
	e, err := r.KV.Get(ctx, keySchemaVersion)
	require.NoError(t, err)
	assert.Equal(t, "2", string(e.Value()))
	_, err = r.KV.Get(ctx, appliedKey(2))
	assert.NoError(t, err)
}
