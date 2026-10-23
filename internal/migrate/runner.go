package migrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Applied is the persisted record of one finished migration.
type Applied struct {
	ID        int           `json:"id"`
	Name      string        `json:"name"`
	AppliedAt time.Time     `json:"applied_at"`
	Duration  time.Duration `json:"duration"`
}

// Runner applies pending migrations. Construct with NewRunner.
type Runner struct {
	KV    jetstream.KeyValue
	Env   *Env
	Owner string
	Log   *slog.Logger

	// EnsureSchema runs under the lock before any migration (default: the store's).
	EnsureSchema func(context.Context) error
	// Migrations is the history to apply (default: the package registry).
	Migrations []Migration
	// LeaseTTL is the migration lock's lifetime between refreshes; MaxWait bounds how
	// long a replica waits for another's run; Poll is the wait loop interval.
	LeaseTTL time.Duration
	MaxWait  time.Duration
	Poll     time.Duration
}

// NewRunner returns a runner over the registered migrations with production timings.
func NewRunner(kv jetstream.KeyValue, env *Env, owner string) *Runner {
	log := env.Log
	if log == nil {
		log = slog.Default()
	}
	return &Runner{
		KV: kv, Env: env, Owner: owner, Log: log, Migrations: registry,
		EnsureSchema: env.Store.EnsureSchema,
		LeaseTTL:     30 * time.Second, MaxWait: 30 * time.Minute, Poll: time.Second,
	}
}

// Run brings the store up to date: take the migration lock (waiting, with logs, if
// another replica holds it), EnsureSchema, then apply every migration newer than the
// stored schema version, in order, recording each. With nothing pending it is a
// cheap no-op. Any failure aborts the run (the caller should refuse to serve) and
// clears the maintenance flag. EnsureSchema runs under the same lock so a fresh
// boot can never observe another replica's half-done cutover.
func (r *Runner) Run(ctx context.Context) (err error) {
	if err := validate(r.Migrations); err != nil {
		return err
	}
	ls := &lease{kv: r.KV, owner: r.Owner, ttl: r.LeaseTTL}
	if err := r.acquire(ctx, ls); err != nil {
		return err
	}
	// From here on we own the cluster-wide migration slot. A lost lease cancels
	// the work: someone else may be running now.
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stopRefresh := r.keepLease(runCtx, ls, cancel)
	defer func() {
		stopRefresh()
		ls.release(context.WithoutCancel(ctx))
	}()

	// Whatever happens, never leave writers paused.
	defer r.Env.Maint.Disable(context.WithoutCancel(ctx))
	r.Env.Maint.clearStale(runCtx)

	if err := r.EnsureSchema(runCtx); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}

	version, rev, err := SchemaVersion(runCtx, r.KV)
	if err != nil {
		return err
	}
	if latest := latest(r.Migrations); version > latest {
		r.Log.Warn("the store is at a NEWER schema version than this binary knows; running an older build against it",
			"stored", version, "this_build", latest)
	}
	for _, m := range r.Migrations {
		if m.ID <= version {
			continue
		}
		log := r.Log.With("migration", m.ID, "name", m.Name)
		log.Info("applying schema migration", "description", m.Description)
		start := time.Now()
		env := *r.Env
		env.Log = log
		if err := m.Up(runCtx, &env); err != nil {
			if cause := context.Cause(runCtx); cause != nil && !errors.Is(cause, context.Canceled) {
				err = errors.Join(err, cause)
			}
			return fmt.Errorf("schema migration %d %q failed (the store is left as-is; fix the cause and restart, it resumes): %w", m.ID, m.Name, err)
		}
		dur := time.Since(start)
		rec, _ := json.Marshal(Applied{ID: m.ID, Name: m.Name, AppliedAt: time.Now().UTC(), Duration: dur}) // plain struct
		if _, err := r.KV.Put(runCtx, appliedKey(m.ID), rec); err != nil {
			return fmt.Errorf("record migration %d: %w", m.ID, err)
		}
		if err := setSchemaVersion(runCtx, r.KV, m.ID, rev); err != nil {
			return err
		}
		_, rev, _ = SchemaVersion(runCtx, r.KV) // fresh revision for the next CAS
		version = m.ID
		log.Info("schema migration applied", "took", dur.Round(time.Millisecond))
	}
	return nil
}

// acquire waits for the lease up to MaxWait, logging while it does.
func (r *Runner) acquire(ctx context.Context, ls *lease) error {
	deadline := time.Now().Add(r.MaxWait)
	lastLog := time.Time{}
	for {
		ok, who, err := ls.tryAcquire(ctx)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if time.Since(lastLog) >= 10*time.Second {
			r.Log.Info("another replica holds the migration lock, waiting", "holder", who)
			lastLog = time.Now()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("gave up waiting %s for the migration lock held by %q", r.MaxWait, who)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.Poll):
		}
	}
}

// keepLease refreshes the lease until stopped; a refresh failure means it was lost
// (e.g. we stalled past the TTL and another replica took over), which cancels the run.
func (r *Runner) keepLease(ctx context.Context, ls *lease, cancel context.CancelCauseFunc) (stop func()) {
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		t := time.NewTicker(ls.ttl / 3)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				if err := ls.refresh(ctx); err != nil && ctx.Err() == nil {
					cancel(fmt.Errorf("migration lock lost: %w", err))
					return
				}
			}
		}
	}()
	return func() { close(done); <-stopped }
}

// MigrationStatus is one row of Status.
type MigrationStatus struct {
	Migration
	Applied   bool
	AppliedAt time.Time
	Duration  time.Duration
}

// Status is the report behind `cortex migrate status`.
type Status struct {
	SchemaVersion int // highest applied migration in the store
	Latest        int // highest migration this binary knows
	Migrations    []MigrationStatus
	Maintenance   *MaintenanceState
	LockHolder    string
}

// Status reads applied state from the meta bucket (read-only).
func (r *Runner) Status(ctx context.Context) (*Status, error) {
	version, _, err := SchemaVersion(ctx, r.KV)
	if err != nil {
		return nil, err
	}
	applied, err := listApplied(ctx, r.KV)
	if err != nil {
		return nil, err
	}
	st := &Status{SchemaVersion: version, Latest: latest(r.Migrations), LockHolder: holder(ctx, r.KV)}
	for _, m := range r.Migrations {
		ms := MigrationStatus{Migration: m}
		if a, ok := applied[m.ID]; ok {
			ms.Applied, ms.AppliedAt, ms.Duration = true, a.AppliedAt, a.Duration
		}
		st.Migrations = append(st.Migrations, ms)
	}
	st.Maintenance, err = r.Env.Maint.Get(ctx)
	return st, err
}

func listApplied(ctx context.Context, kv jetstream.KeyValue) (map[int]Applied, error) {
	keys, err := kv.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list meta keys: %w", err)
	}
	sort.Strings(keys)
	out := map[int]Applied{}
	for _, k := range keys {
		if !strings.HasPrefix(k, keyAppliedPrefix) {
			continue
		}
		e, err := kv.Get(ctx, k)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", k, err)
		}
		var a Applied
		if err := json.Unmarshal(e.Value(), &a); err != nil {
			return nil, fmt.Errorf("parse %s: %w", k, err)
		}
		out[a.ID] = a
	}
	return out, nil
}

// WaitUntilCurrent blocks until the store's schema version reaches need (normally
// Latest()). The worker calls it before consuming so a worker can never write into
// a store another replica is still migrating (or that predates its schema).
func WaitUntilCurrent(ctx context.Context, kv jetstream.KeyValue, need int, log *slog.Logger, poll time.Duration) error {
	if log == nil {
		log = slog.Default()
	}
	lastLog := time.Time{}
	for {
		v, _, err := SchemaVersion(ctx, kv)
		if err == nil && v >= need {
			return nil
		}
		if time.Since(lastLog) >= 15*time.Second {
			log.Info("waiting for the server to migrate the schema", "have", v, "need", need, "err", err)
			lastLog = time.Now()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(poll):
		}
	}
}
