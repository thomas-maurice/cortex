package migrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// Applied is the persisted record of one finished migration.
type Applied struct {
	ID        int           `json:"id"`
	Name      string        `json:"name"`
	AppliedAt time.Time     `json:"applied_at"`
	Duration  time.Duration `json:"duration"`
	// Assumed marks a record stamped without running the migration: the store was
	// created fresh (born current), so the migration had nothing to do.
	Assumed bool `json:"assumed,omitempty"`
}

// Runner applies pending migrations. Construct with NewRunner.
type Runner struct {
	// KV holds COORDINATION state only (lock, maintenance, heartbeats), plus the
	// legacy schema_version/migration.* keys read once by the backfill.
	KV jetstream.KeyValue
	// Meta holds the durable schema version and applied records (default: the store).
	Meta  MetaStore
	Env   *Env
	Owner string
	Log   *slog.Logger

	// ClassExists reports whether the Memory class exists; checked BEFORE EnsureSchema
	// to tell a fresh store (born current) from existing data (default: the store's).
	ClassExists func(context.Context) (bool, error)
	// EnsureMetaClass creates the CortexMeta class alone (default: the store's); the
	// fresh-install marker needs it before EnsureSchema runs.
	EnsureMetaClass func(context.Context) error
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
	r := &Runner{
		KV: kv, Env: env, Owner: owner, Log: log, Migrations: registry,
		LeaseTTL: 30 * time.Second, MaxWait: 30 * time.Minute, Poll: time.Second,
	}
	if env.Store != nil {
		r.Meta = env.Store
		r.EnsureSchema = env.Store.EnsureSchema
		r.EnsureMetaClass = env.Store.EnsureMetaClass
		r.ClassExists = func(ctx context.Context) (bool, error) { return env.Store.ClassExists(ctx, memory.ClassName) }
	}
	return r
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

	existed, err := r.ClassExists(runCtx)
	if err != nil {
		return fmt.Errorf("check existing store: %w", err)
	}
	if !existed {
		// Fresh store: remember it durably before the Memory class appears. An empty
		// pre-existing class is NOT fresh (it still has the old shape).
		if err := r.EnsureMetaClass(runCtx); err != nil {
			return fmt.Errorf("ensure meta class: %w", err)
		}
		if err := r.Meta.PutMeta(runCtx, keyFreshPending, []byte("1")); err != nil {
			return fmt.Errorf("record fresh install: %w", err)
		}
	}
	if err := r.EnsureSchema(runCtx); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}
	if err := r.backfillFromKV(runCtx); err != nil {
		return err
	}
	version, err := r.resolveVersion(runCtx, existed)
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
		if err := r.Meta.PutMeta(runCtx, appliedKey(m.ID), rec); err != nil {
			return fmt.Errorf("record migration %d: %w", m.ID, err)
		}
		if err := setSchemaVersion(runCtx, r.Meta, m.ID); err != nil {
			return err
		}
		r.mirrorToKV(runCtx, appliedKey(m.ID), rec)
		r.mirrorToKV(runCtx, keySchemaVersion, []byte(strconv.Itoa(m.ID)))
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
	Assumed   bool // stamped on a fresh install, never actually run
}

// Status is the report behind `cortex migrate status`.
type Status struct {
	SchemaVersion int // highest applied migration in the store
	Latest        int // highest migration this binary knows
	Migrations    []MigrationStatus
	Maintenance   *MaintenanceState
	LockHolder    string
}

// Status reads applied state from the durable metadata (read-only).
func (r *Runner) Status(ctx context.Context) (*Status, error) {
	version, err := SchemaVersion(ctx, r.Meta)
	if err != nil {
		return nil, err
	}
	applied, err := listApplied(ctx, r.Meta)
	if err != nil {
		return nil, err
	}
	st := &Status{SchemaVersion: version, Latest: latest(r.Migrations), LockHolder: holder(ctx, r.KV)}
	for _, m := range r.Migrations {
		ms := MigrationStatus{Migration: m}
		if a, ok := applied[m.ID]; ok {
			ms.Applied, ms.AppliedAt, ms.Duration, ms.Assumed = true, a.AppliedAt, a.Duration, a.Assumed
		}
		st.Migrations = append(st.Migrations, ms)
	}
	st.Maintenance, err = r.Env.Maint.Get(ctx)
	return st, err
}

func listApplied(ctx context.Context, m MetaStore) (map[int]Applied, error) {
	entries, err := m.ListMeta(ctx, keyAppliedPrefix)
	if err != nil {
		return nil, err
	}
	out := map[int]Applied{}
	for _, e := range entries {
		var a Applied
		if err := json.Unmarshal(e.Value, &a); err != nil {
			return nil, fmt.Errorf("parse %s: %w", e.Key, err)
		}
		out[a.ID] = a
	}
	return out, nil
}

// backfillFromKV is the one-time move of durable state out of NATS: when the durable
// store has no schema_version but the legacy KV bucket does, copy it and every
// migration.NNNN record (verbatim, so AppliedAt/Duration survive). schema_version is
// written LAST so a crash mid-copy is simply redone. The KV keys are left in place
// (an older binary keeps working) and are never read again once the durable
// store has a version.
func (r *Runner) backfillFromKV(ctx context.Context) error {
	if _, found, err := r.Meta.GetMeta(ctx, keySchemaVersion); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	} else if found {
		return nil
	}
	e, err := r.KV.Get(ctx, keySchemaVersion)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read legacy schema version: %w", err)
	}
	if _, err := strconv.Atoi(string(e.Value())); err != nil {
		return fmt.Errorf("parse legacy schema version %q: %w", e.Value(), err)
	}
	keys, err := r.KV.Keys(ctx)
	if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
		return fmt.Errorf("list legacy meta keys: %w", err)
	}
	sort.Strings(keys)
	n := 0
	for _, k := range keys {
		if !strings.HasPrefix(k, keyAppliedPrefix) {
			continue
		}
		ke, err := r.KV.Get(ctx, k)
		if err != nil {
			return fmt.Errorf("read legacy %s: %w", k, err)
		}
		if err := r.Meta.PutMeta(ctx, k, ke.Value()); err != nil {
			return fmt.Errorf("backfill %s: %w", k, err)
		}
		n++
	}
	if err := r.Meta.PutMeta(ctx, keySchemaVersion, e.Value()); err != nil {
		return fmt.Errorf("backfill schema version: %w", err)
	}
	r.Log.Info("moved schema state from the NATS KV to the durable store (CortexMeta)", "schema_version", string(e.Value()), "applied_records", n)
	return nil
}

// resolveVersion returns the stored schema version. With none recorded anywhere:
//   - a FRESH store (the Memory class did not exist before this boot's EnsureSchema,
//     or the fresh_install_pending marker survives from a boot that crashed)
//     was born current: stamp Latest and an "assumed" record per migration;
//   - EXISTING data of unknown version (NATS was reset before versions moved to
//     Weaviate) starts from 0: migrations are idempotent and shape-checked, so they
//     no-op on a current store and fix an old one. Assuming Latest would silently
//     skip a migration on a pre-migration store.
func (r *Runner) resolveVersion(ctx context.Context, existed bool) (int, error) {
	_, found, err := r.Meta.GetMeta(ctx, keySchemaVersion)
	if err != nil {
		return 0, fmt.Errorf("read schema version: %w", err)
	}
	_, pending, err := r.Meta.GetMeta(ctx, keyFreshPending)
	if err != nil {
		return 0, fmt.Errorf("read fresh-install marker: %w", err)
	}
	if found {
		if pending {
			if err := r.Meta.DeleteMeta(ctx, keyFreshPending); err != nil {
				return 0, err
			}
		}
		return SchemaVersion(ctx, r.Meta)
	}
	if existed && !pending {
		r.Log.Warn("the store has data but no recorded schema version (NATS state was lost before versions lived in Weaviate); re-running every migration from 0, they are idempotent and no-op on a current store")
		return 0, nil
	}
	now := time.Now().UTC()
	for _, m := range r.Migrations {
		rec, _ := json.Marshal(Applied{ID: m.ID, Name: m.Name, AppliedAt: now, Assumed: true}) // plain struct
		if err := r.Meta.PutMeta(ctx, appliedKey(m.ID), rec); err != nil {
			return 0, fmt.Errorf("record assumed migration %d: %w", m.ID, err)
		}
		r.mirrorToKV(ctx, appliedKey(m.ID), rec)
	}
	v := latest(r.Migrations)
	if err := setSchemaVersion(ctx, r.Meta, v); err != nil {
		return 0, err
	}
	r.mirrorToKV(ctx, keySchemaVersion, []byte(strconv.Itoa(v)))
	if err := r.Meta.DeleteMeta(ctx, keyFreshPending); err != nil {
		return 0, err
	}
	r.Log.Info("fresh store: born at the current schema, stamped without running migrations", "schema_version", v)
	return v, nil
}

// mirrorToKV copies a durable schema-state key to the NATS KV, best-effort. It exists
// ONLY for old binaries (a rolling upgrade: an old worker's WaitUntilCurrent still
// reads schema_version from the KV) and can be removed in a later release. New code
// never reads these keys from the KV except in the one-time backfill. NATS is
// disposable, so a failure only logs.
func (r *Runner) mirrorToKV(ctx context.Context, key string, val []byte) {
	if _, err := r.KV.Put(ctx, key, val); err != nil {
		r.Log.Warn("could not mirror schema state to the NATS KV (old-build workers may wait)", "key", key, "err", err)
	}
}

// WaitUntilCurrent blocks until the store's schema version reaches need (normally
// Latest()). The worker calls it before consuming so a worker can never write into
// a store another replica is still migrating (or that predates its schema).
func WaitUntilCurrent(ctx context.Context, m MetaStore, need int, log *slog.Logger, poll time.Duration) error {
	if log == nil {
		log = slog.Default()
	}
	lastLog := time.Time{}
	for {
		v, err := SchemaVersion(ctx, m)
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
