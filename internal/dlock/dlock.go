// Package dlock is a small distributed mutex over a NATS JetStream KV bucket.
// The worker uses it to serialise read-modify-write cycles on one memory id
// across any number of worker replicas.
//
// Acquire is kv.Create(key, owner), which fails if the key exists; release is a
// delete conditioned on the revision Create returned, so a holder whose lock
// already expired (bucket TTL) and was re-taken by someone else can never delete
// the new holder's lock. The bucket TTL is the crash-safety net: a holder that
// dies without releasing wedges the id for at most TTL.
//
// The lock is advisory and time-bounded, so critical sections MUST finish well
// inside TTL. AcquireAll therefore returns a context whose deadline is derived from
// the FIRST lease it took.
//
// Infrastructure errors (bucket or stream gone after a NATS restart or wipe, NATS
// briefly unreachable) are NOT lock failures: Acquire re-ensures the bucket and
// keeps polling until its wait expires, exactly like contention, so they never
// surface to callers as errors that burn a message's delivery budget.
package dlock

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// ErrTimeout is returned when the lock could not be acquired within the wait.
var ErrTimeout = errors.New("dlock: timed out waiting for lock")

// errLeaseBudget means a multi-key acquisition spent its lease budget waiting on a
// later key; the partial set was released and the caller retries from scratch.
var errLeaseBudget = errors.New("dlock: lease budget exhausted while acquiring")

const (
	// pollMin/pollJitter spread contenders so they do not poll in lock-step.
	pollMin    = 50 * time.Millisecond
	pollJitter = 200 * time.Millisecond
	// ensureMinInterval rate-limits bucket re-creation attempts.
	ensureMinInterval = 500 * time.Millisecond
)

// Locker hands out per-key locks backed by one KV bucket.
type Locker struct {
	js     jetstream.JetStream
	bucket string
	owner  string
	ttl    time.Duration

	mu         sync.RWMutex
	kv         jetstream.KeyValue
	lastEnsure time.Time

	// Optional instrumentation, set once via Instrument before use.
	log     *slog.Logger
	onInfra func()
	onWait  func(time.Duration)
}

// Instrument wires optional logging and metrics hooks (any may be nil). Call it
// before the Locker is shared. onInfra fires per infrastructure error; onWait
// receives the time each successful Acquire took.
func (l *Locker) Instrument(log *slog.Logger, onInfra func(), onWait func(time.Duration)) {
	l.log, l.onInfra, l.onWait = log, onInfra, onWait
}

// New creates (or updates) the bucket with the given TTL and returns a Locker.
// owner identifies this process in the stored value, for debugging only. The bucket
// is file-backed so held locks survive a NATS restart.
func New(ctx context.Context, js jetstream.JetStream, bucket string, ttl time.Duration, owner string) (*Locker, error) {
	l := &Locker{js: js, bucket: bucket, owner: owner, ttl: ttl}
	kv, err := l.ensure(ctx)
	if err != nil {
		return nil, err
	}
	l.kv = kv
	return l, nil
}

func (l *Locker) ensure(ctx context.Context) (jetstream.KeyValue, error) {
	kv, err := l.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      l.bucket,
		Description: "Cortex per-memory worker locks",
		TTL:         l.ttl,
		History:     1,
		Storage:     jetstream.FileStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("ensure lock bucket %q: %w", l.bucket, err)
	}
	return kv, nil
}

func (l *Locker) current() jetstream.KeyValue {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.kv
}

// reensure recreates the bucket after an infrastructure error. bad is the handle
// that failed: if another goroutine already replaced it, nothing is done.
func (l *Locker) reensure(ctx context.Context, bad jetstream.KeyValue) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.kv != bad || time.Since(l.lastEnsure) < ensureMinInterval {
		return
	}
	l.lastEnsure = time.Now()
	kv, err := l.ensure(ctx)
	if err != nil {
		if l.log != nil {
			l.log.Warn("lock bucket re-create failed, will retry", "bucket", l.bucket, "err", err)
		}
		return
	}
	l.kv = kv
	if l.log != nil {
		l.log.Warn("lock bucket re-created", "bucket", l.bucket)
	}
}

// Wait is how long a contender should poll before giving up: longer than the TTL,
// so a single wait always outlasts a crashed holder's lock.
func (l *Locker) Wait() time.Duration { return l.ttl + 30*time.Second }

// Hold is the longest a critical section may run, measured from the first lease:
// comfortably inside the TTL, so a stalled call is cancelled before the lock can
// expire and be re-taken underneath it.
func (l *Locker) Hold() time.Duration { return l.ttl * 3 / 4 }

// Key builds a KV-safe lock key for a (tenant, id) pair: "<kind>.<sha256 hex of
// tenant NUL id>". Hashing means ids (which can be client-supplied link targets)
// never need sanitising, distinct (tenant, id) pairs cannot collide, and keys have
// a constant length.
func Key(kind, tenant, id string) string {
	sum := sha256.Sum256([]byte(tenant + "\x00" + id))
	return kind + "." + hex.EncodeToString(sum[:])
}

// pause sleeps for d or until ctx ends, reusing timer.
func pause(ctx context.Context, timer *time.Timer, d time.Duration) error {
	timer.Reset(d)
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Acquire takes the lock on key, polling with jitter for up to wait (callers
// normally pass Wait()). The returned release func only removes the lock if it is
// still ours.
func (l *Locker) Acquire(ctx context.Context, key string, wait time.Duration) (release func() error, err error) {
	start := time.Now()
	deadline := start.Add(wait)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	var lastInfra error
	for {
		kv := l.current()
		rev, cerr := kv.Create(ctx, key, []byte(l.owner))
		if cerr == nil {
			if l.onWait != nil {
				l.onWait(time.Since(start))
			}
			return func() error { return releaseKey(kv, key, rev) }, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !errors.Is(cerr, jetstream.ErrKeyExists) {
			// Infrastructure error (bucket/stream gone, NATS unreachable): repair
			// and keep waiting rather than failing the caller.
			if l.onInfra != nil {
				l.onInfra()
			}
			if lastInfra == nil && l.log != nil { // first one per Acquire
				l.log.Warn("lock infrastructure error, repairing and retrying", "key", key, "err", cerr)
			}
			lastInfra = cerr
			l.reensure(ctx, kv)
		}
		if time.Now().After(deadline) {
			if lastInfra != nil {
				return nil, fmt.Errorf("acquire %s: %w", key, lastInfra)
			}
			return nil, fmt.Errorf("%w: %s", ErrTimeout, key)
		}
		if perr := pause(ctx, timer, pollMin+rand.N(pollJitter)); perr != nil {
			return nil, perr
		}
	}
}

// releaseKey deletes key only if it is still at revision rev. A mismatch means the
// lock expired and was re-taken (or already removed, or the bucket was recreated);
// that is not an error for the caller to act on, so it is swallowed.
func releaseKey(kv jetstream.KeyValue, key string, rev uint64) error {
	// Detached context: release must still run when the handler ctx is cancelled.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := kv.Delete(ctx, key, jetstream.LastRevision(rev))
	if err == nil || isWrongRevision(err) || isGone(err) {
		return nil
	}
	return fmt.Errorf("release %s: %w", key, err)
}

func isWrongRevision(err error) bool {
	if errors.Is(err, jetstream.ErrKeyExists) {
		return true
	}
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence ||
			apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant
	}
	return false
}

// isGone reports the lock (or its whole bucket) no longer exists.
func isGone(err error) bool {
	return errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrBucketNotFound) ||
		errors.Is(err, jetstream.ErrStreamNotFound) || errors.Is(err, jetstream.ErrNoStreamResponse)
}

// AcquireAll locks every key in sorted order (a global order makes concurrent
// multi-key acquirers deadlock-free). It returns a context for the critical
// section, whose deadline is Hold() after the FIRST lease was taken (the earliest
// to expire), and one release that cancels it and frees the locks in reverse.
//
// If waiting for a later key would push the critical section past the first
// lease's budget, everything is released and acquisition restarts (within the
// overall wait), so the section can never outlive a lease it depends on. Duplicate
// keys are locked once.
func (l *Locker) AcquireAll(ctx context.Context, keys []string, wait time.Duration) (hctx context.Context, release func(), err error) {
	sorted := slices.Clone(keys)
	slices.Sort(sorted)
	sorted = slices.Compact(sorted)
	overall := time.Now().Add(wait)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		hctx, release, err = l.tryAll(ctx, sorted, overall)
		if !errors.Is(err, errLeaseBudget) {
			return hctx, release, err
		}
		if perr := pause(ctx, timer, pollMin+rand.N(pollJitter)); perr != nil {
			return nil, nil, perr
		}
	}
}

func (l *Locker) tryAll(ctx context.Context, keys []string, overall time.Time) (context.Context, func(), error) {
	var rels []func() error
	freeAll := func() {
		for i := len(rels) - 1; i >= 0; i-- {
			_ = rels[i]() // best-effort: TTL reclaims a lock we fail to delete
		}
	}
	var holdDeadline time.Time
	for i, k := range keys {
		limit := overall
		budgeted := false
		if i > 0 && holdDeadline.Before(limit) {
			limit, budgeted = holdDeadline, true
		}
		r, err := l.Acquire(ctx, k, time.Until(limit))
		if err != nil {
			freeAll()
			if budgeted && errors.Is(err, ErrTimeout) {
				return nil, nil, errLeaseBudget
			}
			return nil, nil, err
		}
		rels = append(rels, r)
		if i == 0 {
			holdDeadline = time.Now().Add(l.Hold())
		}
	}
	if !time.Now().Before(holdDeadline) {
		freeAll()
		return nil, nil, errLeaseBudget
	}
	hctx, cancel := context.WithDeadline(ctx, holdDeadline)
	return hctx, func() { cancel(); freeAll() }, nil
}
