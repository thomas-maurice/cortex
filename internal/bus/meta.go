package bus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// MetaBucket is the small file-backed KV bucket for server-wide COORDINATION
	// state (migration lock, maintenance flag, worker heartbeats). Durable facts
	// live in Weaviate's CortexMeta class so NATS can be wiped (see store/meta.go).
	MetaBucket = "cortex_meta"
	// keyUsageTrackingSince is where older builds kept the usage-tracking epoch.
	// It is now durable in CortexMeta; this key is only read once to seed it.
	keyUsageTrackingSince = "usage_tracking_since"
)

// LegacyUsageTrackingSince reads the usage-tracking epoch an older build recorded
// in the NATS KV, so the move to CortexMeta does not reset the stale-review epoch.
// found is false when the bucket or key is absent. It never creates anything.
func LegacyUsageTrackingSince(ctx context.Context, js jetstream.JetStream) (t time.Time, found bool, err error) {
	kv, err := js.KeyValue(ctx, MetaBucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("open meta bucket %q: %w", MetaBucket, err)
	}
	e, err := kv.Get(ctx, keyUsageTrackingSince)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("read legacy usage tracking start: %w", err)
	}
	t, err = time.Parse(time.RFC3339, string(e.Value()))
	if err != nil {
		return time.Time{}, false, fmt.Errorf("parse legacy usage tracking start %q: %w", e.Value(), err)
	}
	return t, true, nil
}
