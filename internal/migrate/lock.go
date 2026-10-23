package migrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// lease is the distributed migration lock: a KV entry {owner, expires} that the
// holder refreshes. Not the dlock package: that one has a fixed short bucket TTL
// and no refresh, while a data copy can run for minutes. A crashed holder's lease
// simply expires and is taken over with a revision-checked Update.
type lease struct {
	kv    jetstream.KeyValue
	owner string
	ttl   time.Duration
	rev   uint64
}

type leaseValue struct {
	Owner   string    `json:"owner"`
	Expires time.Time `json:"expires"`
}

func (l *lease) value() []byte {
	b, _ := json.Marshal(leaseValue{Owner: l.owner, Expires: time.Now().Add(l.ttl)}) // plain struct: cannot fail
	return b
}

// tryAcquire takes the lease if it is free or expired. holder names the current
// holder when it is busy.
func (l *lease) tryAcquire(ctx context.Context) (ok bool, holder string, err error) {
	rev, err := l.kv.Create(ctx, keyLock, l.value())
	if err == nil {
		l.rev = rev
		return true, "", nil
	}
	if !errors.Is(err, jetstream.ErrKeyExists) {
		return false, "", fmt.Errorf("take migration lock: %w", err)
	}
	e, err := l.kv.Get(ctx, keyLock)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, "", nil // released between our two calls: retry
	}
	if err != nil {
		return false, "", fmt.Errorf("read migration lock: %w", err)
	}
	var cur leaseValue
	if json.Unmarshal(e.Value(), &cur) == nil && !expired(cur.Expires) {
		return false, cur.Owner, nil
	}
	// Free (released holders delete the key) or expired/garbage: take over.
	rev, err = l.kv.Update(ctx, keyLock, l.value(), e.Revision())
	if err != nil {
		return false, "", nil // someone else won the takeover
	}
	l.rev = rev
	return true, "", nil
}

// refresh extends the lease. An error means it was lost (revision moved on).
func (l *lease) refresh(ctx context.Context) error {
	rev, err := l.kv.Update(ctx, keyLock, l.value(), l.rev)
	if err != nil {
		return fmt.Errorf("refresh migration lock: %w", err)
	}
	l.rev = rev
	return nil
}

// release deletes the lease only if we still hold it.
func (l *lease) release(ctx context.Context) {
	_ = l.kv.Delete(ctx, keyLock, jetstream.LastRevision(l.rev)) // best effort: it expires anyway
}

// holder reports the current unexpired holder, "" if free.
func holder(ctx context.Context, kv jetstream.KeyValue) string {
	e, err := kv.Get(ctx, keyLock)
	if err != nil {
		return ""
	}
	var cur leaseValue
	if json.Unmarshal(e.Value(), &cur) != nil || expired(cur.Expires) {
		return ""
	}
	return cur.Owner
}
