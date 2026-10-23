package migrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// The drain handshake. Setting the maintenance flag does not stop a worker that is
// already mid-message (a slow embed, a merge): its write would land in the class
// being replaced after it was copied and be lost. So every worker publishes a
// heartbeat {held, inflight} under worker.<id> in cortex_meta, and the migrator
// waits until every LIVE worker reports held && inflight==0 before it copies.
// A heartbeat older than DefaultHeartbeatStale means the worker is dead, which
// counts as idle. Workers built before this mechanism publish nothing and cannot be
// detected: upgrade/stop them first (docs/DESIGN.md "Schema migrations").

const (
	keyWorkerPrefix = "worker."
	// DefaultHeartbeatInterval is the refresh period of an unchanged heartbeat; a
	// change of state is published within HeartbeatPoll.
	DefaultHeartbeatInterval = 3 * time.Second
	// DefaultHeartbeatStale is the age after which a heartbeat is considered dead.
	DefaultHeartbeatStale = 15 * time.Second
	// DefaultDrainTimeout bounds the wait for workers to go idle.
	DefaultDrainTimeout = 2 * time.Minute
	heartbeatPoll       = 250 * time.Millisecond
	staleKeyPurgeAfter  = time.Hour
)

// WorkerStatus is the heartbeat value.
type WorkerStatus struct {
	ID string `json:"id"`
	// HeldGen is the maintenance generation all of the worker's consumer loops are
	// parked for (0 = not parked).
	HeldGen   int64     `json:"heldGen"`
	Inflight  int       `json:"inflight"`
	UpdatedAt time.Time `json:"updatedAt"`
}

var badKeyChars = regexp.MustCompile(`[^A-Za-z0-9_=./-]`)

// Heartbeat publishes one worker's drain state.
type Heartbeat struct {
	kv    jetstream.KeyValue
	id    string
	log   *slog.Logger
	state func() (heldGen int64, inflight int)
	// Interval/Poll are exposed for tests.
	Interval time.Duration
	Poll     time.Duration
}

// NewHeartbeat reports state() (heldGen: the maintenance generation all consumer
// loops are parked for, 0 if not; inflight: loops currently working) under a key derived from id.
func NewHeartbeat(kv jetstream.KeyValue, id string, log *slog.Logger, state func() (int64, int)) *Heartbeat {
	if log == nil {
		log = slog.Default()
	}
	return &Heartbeat{kv: kv, id: id, log: log, state: state, Interval: DefaultHeartbeatInterval, Poll: heartbeatPoll}
}

func (h *Heartbeat) key() string { return keyWorkerPrefix + badKeyChars.ReplaceAllString(h.id, "_") }

// PublishNow writes a heartbeat synchronously. Call it before starting the
// consumers so the migrator never sees a worker that is fetching but unannounced.
func (h *Heartbeat) PublishNow(ctx context.Context, heldGen int64, inflight int) {
	h.put(ctx, heldGen, inflight)
}

// Remove deletes the heartbeat key. Call it only once nothing can write any more.
func (h *Heartbeat) Remove(ctx context.Context) { _ = h.kv.Delete(ctx, h.key()) }

func (h *Heartbeat) put(ctx context.Context, held int64, inflight int) {
	b, _ := json.Marshal(WorkerStatus{ID: h.id, HeldGen: held, Inflight: inflight, UpdatedAt: time.Now().UTC()}) // plain struct
	if _, err := h.kv.Put(ctx, h.key(), b); err != nil && ctx.Err() == nil {
		h.log.Warn("worker heartbeat failed", "err", err)
	}
}

// Run publishes until ctx ends. It does NOT remove the key: a shutting-down worker
// may still have a message in flight, so the caller calls Remove after its pools
// have drained.
func (h *Heartbeat) Run(ctx context.Context) {
	held, inflight := h.state()
	h.put(ctx, held, inflight)
	last := time.Now()
	t := time.NewTicker(h.Poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			nh, ni := h.state()
			if nh != held || ni != inflight || time.Since(last) >= h.Interval {
				held, inflight, last = nh, ni, time.Now()
				h.put(ctx, held, inflight)
			}
		}
	}
}

// WaitWorkersIdle blocks until every live worker heartbeat reports held FOR THE
// CURRENT maintenance generation with nothing in flight. It errors on timeout (the caller must then NOT copy). A
// negative timeout skips the wait.
func (m *Maintenance) WaitWorkersIdle(ctx context.Context, timeout time.Duration) error {
	if timeout < 0 {
		return nil
	}
	if timeout == 0 {
		timeout = DefaultDrainTimeout
	}
	m.mu.Lock()
	gen := m.gen
	m.mu.Unlock()
	if gen == 0 {
		// Without an active cycle, unparked workers (heldGen 0) would pass as idle.
		return errors.New("migrate: WaitWorkersIdle called without Enable")
	}
	deadline := time.Now().Add(timeout)
	lastLog := time.Now()
	for {
		busy, err := m.busyWorkers(ctx, gen)
		if err != nil {
			return err
		}
		if len(busy) == 0 {
			return nil
		}
		if time.Since(lastLog) >= 10*time.Second {
			m.log.Info("waiting for workers to go idle", "workers", busy)
			lastLog = time.Now()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("workers did not go idle within %s: %s (a worker built before the drain handshake cannot be detected; stop it)", timeout, strings.Join(busy, ", "))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(heartbeatPoll):
		}
	}
}

// busyWorkers lists live workers that are not yet held and idle. Heartbeats older
// than DefaultHeartbeatStale are dead = idle; very old ones are purged.
func (m *Maintenance) busyWorkers(ctx context.Context, gen int64) ([]string, error) {
	keys, err := m.kv.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list worker heartbeats: %w", err)
	}
	var busy []string
	for _, k := range keys {
		if !strings.HasPrefix(k, keyWorkerPrefix) {
			continue
		}
		e, err := m.kv.Get(ctx, k)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", k, err)
		}
		var w WorkerStatus
		if json.Unmarshal(e.Value(), &w) != nil {
			continue
		}
		age := time.Since(w.UpdatedAt)
		if age > staleKeyPurgeAfter {
			_ = m.kv.Delete(ctx, k)
			continue
		}
		if age > DefaultHeartbeatStale {
			continue // dead worker
		}
		// A heartbeat parked for an OLDER generation is stale news from the previous
		// Enter/Exit cycle (the worker's cache may still say "off"): not idle.
		if w.HeldGen != gen || w.Inflight > 0 {
			busy = append(busy, fmt.Sprintf("%s(heldGen=%d want=%d,inflight=%d)", w.ID, w.HeldGen, gen, w.Inflight))
		}
	}
	return busy, nil
}
