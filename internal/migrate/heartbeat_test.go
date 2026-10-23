package migrate

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/natstest"
)

// A worker message that is still being processed when maintenance starts (a slow
// embed, a merge) will write into the class being replaced. The drain must wait for
// it however long it takes, not for a fixed time: here the handler outlasts the old
// 15s fixed drain, and its write must land before Enter returns (i.e. before the copy).
func TestEnterWaitsForSlowInflightHandler(t *testing.T) {
	if testing.Short() {
		t.Skip("takes >15s by design")
	}
	_, js := natstest.Start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kv, err := OpenMeta(ctx, js)
	require.NoError(t, err)

	migrator := NewMaintenance(kv, "migrator", quiet())
	workerSide := NewMaintenance(kv, "worker", quiet())
	workerSide.CacheFor = 0

	var inflight atomic.Int32
	var written atomic.Bool
	// A handler already in flight when the flag goes up.
	inflight.Store(1)
	hb := NewHeartbeat(kv, "w1", quiet(), func() (int64, int) { return workerSide.ActiveGen(ctx), int(inflight.Load()) })
	hb.Poll = 50 * time.Millisecond
	go hb.Run(ctx)
	go func() {
		time.Sleep(18 * time.Second)
		written.Store(true) // its write lands in the OLD class
		inflight.Store(0)
	}()
	require.Eventually(t, func() bool { // the heartbeat has published inflight=1
		e, err := kv.Get(ctx, "worker.w1")
		return err == nil && len(e.Value()) > 0
	}, 5*time.Second, 20*time.Millisecond)

	start := time.Now()
	require.NoError(t, migrator.Enable(ctx, "test"))
	defer migrator.Disable(ctx)
	require.NoError(t, migrator.WaitWorkersIdle(ctx, time.Minute))

	assert.True(t, written.Load(), "the in-flight write must have landed before the copy may start")
	assert.GreaterOrEqual(t, time.Since(start), 15*time.Second, "it waited for the handler, not a fixed drain")
}

func TestWaitWorkersIdleTimesOutInsteadOfProceeding(t *testing.T) {
	_, js := natstest.Start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kv, err := OpenMeta(ctx, js)
	require.NoError(t, err)
	m := NewMaintenance(kv, "migrator", quiet())

	hb := NewHeartbeat(kv, "stuck", quiet(), func() (int64, int) { return 0, 1 })
	hb.Poll = 20 * time.Millisecond
	go hb.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	require.NoError(t, m.Enable(ctx, "test"))
	err = m.WaitWorkersIdle(ctx, 600*time.Millisecond)
	require.Error(t, err, "never copy on a timeout")
	assert.Contains(t, err.Error(), "stuck")
}

// Through the Env hook: a worker that never goes idle makes the swap's Enter fail
// and leaves no maintenance flag behind (the migration aborts, version unchanged).
func TestEnvEnterFailsAndClearsFlagOnDrainTimeout(t *testing.T) {
	_, js := natstest.Start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kv, err := OpenMeta(ctx, js)
	require.NoError(t, err)
	hb := NewHeartbeat(kv, "stuck", quiet(), func() (int64, int) { return 0, 3 })
	hb.Poll = 20 * time.Millisecond
	go hb.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	env := &Env{Log: quiet(), Maint: NewMaintenance(kv, "migrator", quiet()), DrainTimeout: 500 * time.Millisecond}
	m := Migration{ID: 1, Name: "drain", Up: func(ctx context.Context, e *Env) error {
		// What store.SwapToVersion does around Enter/Exit.
		defer e.Maint.Disable(ctx)
		if err := e.Maint.Enable(ctx, "x"); err != nil {
			return err
		}
		return e.Maint.WaitWorkersIdle(ctx, e.DrainTimeout)
	}}
	r := testRunner(t, js, "migrator", []Migration{m})
	r.Env = env
	require.Error(t, r.Run(ctx))
	v, _, _ := SchemaVersion(ctx, kv)
	assert.Equal(t, 0, v)
	st, err := env.Maint.Get(ctx)
	require.NoError(t, err)
	assert.Nil(t, st)
}

// A heartbeat that stopped updating is a dead worker and counts as idle; a worker
// that exits cleanly removes its key.
func TestDeadAndExitedWorkersCountAsIdle(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := context.Background()
	kv, err := OpenMeta(ctx, js)
	require.NoError(t, err)
	m := NewMaintenance(kv, "migrator", quiet())

	stale, _ := json.Marshal(WorkerStatus{ID: "dead", HeldGen: 0, Inflight: 5, UpdatedAt: time.Now().Add(-time.Minute)})
	_, err = kv.Put(ctx, "worker.dead", stale)
	require.NoError(t, err)
	require.NoError(t, m.Enable(ctx, "test"))
	require.NoError(t, m.WaitWorkersIdle(ctx, time.Second), "a stale heartbeat means a dead worker")

	hctx, stop := context.WithCancel(ctx)
	hb := NewHeartbeat(kv, "w", quiet(), func() (int64, int) { return 0, 1 })
	hb.Poll = 20 * time.Millisecond
	done := make(chan struct{})
	go func() { hb.Run(hctx); close(done) }()
	time.Sleep(100 * time.Millisecond)
	assert.Error(t, m.WaitWorkersIdle(ctx, 200*time.Millisecond), "a live busy worker blocks")
	stop()
	<-done
	hb.Remove(ctx) // what the worker does after its pools drained
	assert.NoError(t, m.WaitWorkersIdle(ctx, time.Second), "a cleanly exited worker no longer counts")
}

func TestCancelOnActive(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := context.Background()
	kv, err := OpenMeta(ctx, js)
	require.NoError(t, err)
	reader := NewMaintenance(kv, "r", quiet())
	reader.CacheFor = 0
	cctx, cancel := reader.CancelOnActive(ctx)
	defer cancel()
	require.NoError(t, cctx.Err())
	owner := NewMaintenance(kv, "o", quiet())
	require.NoError(t, owner.Enable(ctx, "x"))
	defer owner.Disable(ctx)
	select {
	case <-cctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("a sweep context must be cancelled once maintenance starts")
	}
}

// m001 runs Enter/Exit per class back to back. A worker whose Active cache still
// says "off" keeps publishing the PREVIOUS cycle's held state; the next drain must not
// accept that, or the worker could fetch and write into the class being copied.
func TestSecondEnterRejectsHeartbeatFromPreviousCycle(t *testing.T) {
	_, js := natstest.Start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kv, err := OpenMeta(ctx, js)
	require.NoError(t, err)

	migrator := NewMaintenance(kv, "migrator", quiet())
	laggy := NewMaintenance(kv, "worker", quiet())
	laggy.CacheFor = 1500 * time.Millisecond // slow to notice flag changes
	hb := NewHeartbeat(kv, "w", quiet(), func() (int64, int) { return laggy.ActiveGen(ctx), 0 })
	hb.Poll = 20 * time.Millisecond
	go hb.Run(ctx)

	// Cycle 1: the worker notices and parks.
	require.NoError(t, migrator.Enable(ctx, "one"))
	gen1 := migrator.gen
	require.NoError(t, migrator.WaitWorkersIdle(ctx, 10*time.Second))
	migrator.Disable(ctx)

	// Cycle 2 immediately: the worker's cache (and heartbeat) still show gen1.
	require.NoError(t, migrator.Enable(ctx, "two"))
	gen2 := migrator.gen
	require.NotEqual(t, gen1, gen2)
	require.NoError(t, migrator.WaitWorkersIdle(ctx, 10*time.Second))

	e, err := kv.Get(ctx, "worker.w")
	require.NoError(t, err)
	var w WorkerStatus
	require.NoError(t, json.Unmarshal(e.Value(), &w))
	assert.Equal(t, gen2, w.HeldGen, "the second drain must only complete on a heartbeat for the NEW generation")
	migrator.Disable(ctx)
}

// Without an Enable there is no generation to compare against, and an unparked
// worker (heldGen 0) would look idle: the wait must refuse rather than pass.
func TestWaitWorkersIdleRequiresActiveCycle(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := context.Background()
	kv, err := OpenMeta(ctx, js)
	require.NoError(t, err)
	require.Error(t, NewMaintenance(kv, "migrator", quiet()).WaitWorkersIdle(ctx, time.Second))
}
