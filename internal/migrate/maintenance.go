package migrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Maintenance is the cluster-wide "writers stop" flag, a KV entry carrying its
// owner and an expiry. The migrator sets it around data copies and keeps pushing
// the expiry out; if it dies the flag lapses on its own (DefaultMaintenanceTTL), so
// a crash can never wedge the cluster. Readers (the worker's pool, the server's
// write RPCs) call Active, which is cached briefly.
type Maintenance struct {
	kv    jetstream.KeyValue
	owner string
	log   *slog.Logger
	// TTL is how long one refresh keeps the flag alive; CacheFor how long Active
	// trusts its last read. Set before use.
	TTL      time.Duration
	CacheFor time.Duration

	mu        sync.Mutex
	cachedAt  time.Time
	cachedGen int64
	gen       int64 // generation of the flag THIS process currently holds (0 = none)
	stopRenew context.CancelFunc
	renewDone chan struct{}
}

// Defaults for Maintenance.
const (
	DefaultMaintenanceTTL = 2 * time.Minute
	defaultCacheFor       = time.Second
)

// MaintenanceState is the decoded flag.
type MaintenanceState struct {
	// Gen identifies one Enable..Disable cycle (its start time in unix nanos). A
	// worker reports the generation it is parked for, so a heartbeat left over from
	// the previous cycle cannot satisfy the next drain.
	Gen     int64     `json:"gen"`
	Owner   string    `json:"owner"`
	Reason  string    `json:"reason"`
	Since   time.Time `json:"since"`
	Expires time.Time `json:"expires"`
}

// NewMaintenance wraps the meta bucket. owner identifies this process.
func NewMaintenance(kv jetstream.KeyValue, owner string, log *slog.Logger) *Maintenance {
	if log == nil {
		log = slog.Default()
	}
	return &Maintenance{kv: kv, owner: owner, log: log, TTL: DefaultMaintenanceTTL, CacheFor: defaultCacheFor}
}

// Get returns the flag if set and not expired.
func (m *Maintenance) Get(ctx context.Context) (*MaintenanceState, error) {
	if m == nil {
		return nil, nil
	}
	e, err := m.kv.Get(ctx, keyMaintenance)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read maintenance flag: %w", err)
	}
	var st MaintenanceState
	if err := json.Unmarshal(e.Value(), &st); err != nil {
		// Garbage must not freeze writers forever.
		return nil, nil
	}
	if expired(st.Expires) {
		return nil, nil
	}
	if st.Gen == 0 {
		st.Gen = 1
	}
	return &st, nil
}

// Active reports whether maintenance mode is on. Read errors count as "off":
// blocking writers because the KV hiccuped would be worse than the race.
func (m *Maintenance) Active(ctx context.Context) bool { return m.ActiveGen(ctx) != 0 }

// ActiveGen is the generation of the flag if on, else 0 (cached for CacheFor).
func (m *Maintenance) ActiveGen(ctx context.Context) int64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	if time.Since(m.cachedAt) < m.CacheFor {
		v := m.cachedGen
		m.mu.Unlock()
		return v
	}
	m.mu.Unlock()
	st, err := m.Get(ctx)
	if err != nil {
		m.log.Warn("maintenance flag unreadable, treating as off", "err", err)
	}
	var gen int64
	if st != nil {
		gen = st.Gen
	}
	m.mu.Lock()
	m.cachedGen, m.cachedAt = gen, time.Now()
	m.mu.Unlock()
	return gen
}

// CachedGen is the generation seen by the most recent ActiveGen/Active read (0 if
// off), without touching the KV. A consumer loop reads it right after Active said
// "on" to learn which cycle it is parked for.
func (m *Maintenance) CachedGen() int64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cachedGen
}

func (m *Maintenance) forget() {
	m.mu.Lock()
	m.cachedAt, m.cachedGen = time.Time{}, 0
	m.mu.Unlock()
}

func (m *Maintenance) put(ctx context.Context, gen int64, since time.Time, reason string) error {
	b, _ := json.Marshal(MaintenanceState{Gen: gen, Owner: m.owner, Reason: reason, Since: since, Expires: time.Now().Add(m.TTL)}) // plain struct
	_, err := m.kv.Put(ctx, keyMaintenance, b)
	return err
}

// Enable turns maintenance on and keeps it alive until Disable (or ctx ends).
// Calling it while already enabled by this process only updates the reason.
func (m *Maintenance) Enable(ctx context.Context, reason string) error {
	now := time.Now()
	m.mu.Lock()
	if m.gen == 0 {
		m.gen = now.UnixNano() // a repeated Enable keeps its cycle
	}
	gen := m.gen
	m.mu.Unlock()
	if err := m.put(ctx, gen, now, reason); err != nil {
		return fmt.Errorf("set maintenance flag: %w", err)
	}
	m.forget()
	m.log.Warn("maintenance mode ON: writes are paused", "reason", reason)

	m.mu.Lock()
	if m.stopRenew != nil {
		m.mu.Unlock()
		return nil
	}
	rctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	m.stopRenew = cancel
	done := make(chan struct{})
	m.renewDone = done
	m.mu.Unlock()
	go func() {
		defer close(done)
		t := time.NewTicker(m.TTL / 3)
		defer t.Stop()
		for {
			select {
			case <-rctx.Done():
				return
			case <-t.C:
				if err := m.put(rctx, gen, now, reason); err != nil && rctx.Err() == nil {
					m.log.Warn("maintenance flag refresh failed", "err", err)
				}
			}
		}
	}()
	return nil
}

// Disable turns the flag off if this process owns it. Safe to call when it was
// never enabled (a no-op), and meant for defers.
func (m *Maintenance) Disable(ctx context.Context) {
	if m == nil {
		return
	}
	m.mu.Lock()
	stop, done := m.stopRenew, m.renewDone
	m.stopRenew, m.renewDone = nil, nil
	m.gen = 0
	m.mu.Unlock()
	if stop != nil {
		stop()
		<-done // a refresh still in flight must not resurrect the flag after the delete
	}
	if st, err := m.Get(ctx); err == nil && st != nil && st.Owner == m.owner {
		if err := m.kv.Delete(ctx, keyMaintenance); err != nil {
			m.log.Error("could not clear the maintenance flag; it will lapse on its own", "expires", st.Expires, "err", err)
		} else {
			m.log.Info("maintenance mode OFF")
		}
	}
	m.forget()
}

// clearStale removes whatever flag exists regardless of owner. Only valid while
// holding the migration lease: any flag then belongs to a dead previous holder.
func (m *Maintenance) clearStale(ctx context.Context) {
	if st, err := m.Get(ctx); err == nil && st != nil {
		m.log.Warn("clearing a maintenance flag left by a crashed migrator", "owner", st.Owner)
		_ = m.kv.Delete(ctx, keyMaintenance)
	}
	m.forget()
}

// CancelOnActive returns a context that is cancelled as soon as maintenance mode
// turns on, so a long-running background writer (the expiry and trash sweeps) stops
// between its requests instead of finishing a pass into a class being replaced. The
// returned cancel must be called.
func (m *Maintenance) CancelOnActive(ctx context.Context) (context.Context, context.CancelFunc) {
	cctx, cancel := context.WithCancel(ctx)
	go func() {
		t := time.NewTicker(cancelPoll)
		defer t.Stop()
		for {
			select {
			case <-cctx.Done():
				return
			case <-t.C:
				if m.Active(cctx) {
					cancel()
					return
				}
			}
		}
	}()
	return cctx, cancel
}

// Settle waits long enough for every in-process reader to see a flag that was just
// set: the Active cache plus the CancelOnActive poll plus a margin. Call it after
// Enable and before copying, so server-side writers (RPCs past the interceptor, sweep
// batches) have stopped. Residual limit: a single request already inside a handler
// for longer than this.
func (m *Maintenance) Settle(ctx context.Context) error {
	t := time.NewTimer(m.CacheFor + cancelPoll + 500*time.Millisecond)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

const cancelPoll = 250 * time.Millisecond
