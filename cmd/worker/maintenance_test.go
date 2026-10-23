package main

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/bus"
	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/migrate"
)

// During a schema migration the server copies a class and swaps it in. A worker
// write landing in the class being replaced after it was copied would be lost, so
// the worker must stop FETCHING (NATS buffers the messages) while maintenance mode
// is on, and drain the backlog the moment it ends.
func TestWorkerPausesFetchingDuringMaintenance(t *testing.T) {
	s := newStack(t, 1, 5*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var held atomic.Bool
	held.Store(true)
	d := &workerDeps{
		log: slog.New(slog.NewTextHandler(io.Discard, nil)), m: s.m, js: s.js, locker: s.locker, stream: s.stream,
		embedder: s.embedder, chunker: s.chunker, concurrency: 2, ackWait: s.ackWait, stores: s.stores,
		hold: func(context.Context) bool { return held.Load() },
	}
	ip, lp, err := d.startPools(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { cancel(); ip.wait(); lp.wait() })

	id := uuid.NewString()
	require.NoError(t, bus.PublishIndex(ctx, s.js, memory.Record{
		ID: id, Text: "buffered while paused", Namespace: "ns", Source: "t", CreatedAt: time.Now().UTC(),
	}))

	time.Sleep(1500 * time.Millisecond) // several fetch cycles
	_, ok := s.fs.peek("", id)
	assert.False(t, ok, "nothing may be written while maintenance mode is on")

	held.Store(false)
	require.Eventually(t, func() bool { _, ok := s.fs.peek("", id); return ok },
		10*time.Second, 50*time.Millisecond, "the buffered message is processed once maintenance ends")
}

// The drain handshake reads busy/held off the pools. "held && busy==0" must only
// become true once a message that was in flight when maintenance began has FINISHED,
// otherwise the migrator would copy while that message can still write.
func TestPoolReportsBusyUntilInflightFinishesThenHeld(t *testing.T) {
	s := newStack(t, 1, 5*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var held atomic.Bool
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	d := &workerDeps{
		log: slog.New(slog.NewTextHandler(io.Discard, nil)), m: s.m, js: s.js, locker: s.locker, stream: s.stream,
		embedder: s.embedder, chunker: s.chunker, concurrency: 1, ackWait: s.ackWait,
		stores: func(string) tenantStore { started <- struct{}{}; <-release; return s.stores("") },
		hold:   func(context.Context) bool { return held.Load() },
	}
	ip, lp, err := d.startPools(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { cancel(); ip.wait(); lp.wait() })

	require.NoError(t, bus.PublishIndex(ctx, s.js, memory.Record{ID: uuid.NewString(), Text: "slow", Namespace: "ns", CreatedAt: time.Now().UTC()}))
	<-started // the handler is running

	held.Store(true) // maintenance begins mid-message
	time.Sleep(2 * time.Second)
	assert.EqualValues(t, 1, ip.busy.Load(), "still busy: the handler has not finished")

	close(release)
	require.Eventually(t, func() bool {
		return ip.busy.Load() == 0 && ip.held.Load() != 0 && lp.busy.Load() == 0 && lp.held.Load() != 0
	},
		10*time.Second, 50*time.Millisecond, "parked and idle once the in-flight message is done")
}

// A worker that is shutting down still has its message in flight for up to
// shutdownGrace. Its heartbeat must keep saying so until the pools have drained; if
// the key vanished at ctx-cancel the migrator would treat it as gone and copy while
// that message can still write.
func TestHeartbeatReportsInflightThroughShutdown(t *testing.T) {
	s := newStack(t, 1, 5*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	kv, err := migrate.OpenMeta(ctx, s.js)
	require.NoError(t, err)

	release := make(chan struct{})
	started := make(chan struct{}, 1)
	d := &workerDeps{
		log: slog.New(slog.NewTextHandler(io.Discard, nil)), m: s.m, js: s.js, locker: s.locker, stream: s.stream,
		embedder: s.embedder, chunker: s.chunker, concurrency: 1, ackWait: s.ackWait,
		stores: func(string) tenantStore { started <- struct{}{}; <-release; return s.stores("") },
	}
	ip, lp, err := d.startPools(ctx)
	require.NoError(t, err)
	hb := migrate.NewHeartbeat(kv, "w", slog.New(slog.NewTextHandler(io.Discard, nil)), func() (int64, int) { return poolsState(ip, lp) })
	hb.Poll = 20 * time.Millisecond
	shutdown := startHeartbeat(ctx, hb, ip, lp)

	require.NoError(t, bus.PublishIndex(ctx, s.js, memory.Record{ID: uuid.NewString(), Text: "slow", Namespace: "ns", CreatedAt: time.Now().UTC()}))
	<-started
	cancel() // worker shutdown begins with the message still in flight
	done := make(chan struct{})
	go func() { shutdown(); close(done) }()

	m := migrate.NewMaintenance(kv, "migrator", slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, m.Enable(context.Background(), "test"))
	time.Sleep(300 * time.Millisecond)
	err = m.WaitWorkersIdle(context.Background(), 600*time.Millisecond)
	require.Error(t, err, "a shutting-down worker with a message in flight is NOT idle")
	assert.Contains(t, err.Error(), "inflight=1")

	close(release)
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("shutdown did not finish")
	}
	assert.NoError(t, m.WaitWorkersIdle(context.Background(), 2*time.Second), "once drained, the key is removed")
}
