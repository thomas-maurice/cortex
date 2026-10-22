package bus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// MetaBucket is the small file-backed KV bucket for server-wide metadata.
	MetaBucket = "cortex_meta"
	// keyUsageTrackingSince holds the RFC3339 instant usage tracking (RecordUse)
	// first ran on this deployment. Memories cannot be judged "unused" for any
	// period before it.
	keyUsageTrackingSince = "usage_tracking_since"
)

// EnsureUsageTrackingSince returns the instant usage tracking started, recording
// now if it was never set. The write is an atomic create, so replicas starting
// together agree on one value; an existing value is never overwritten.
func EnsureUsageTrackingSince(ctx context.Context, js jetstream.JetStream, now time.Time) (time.Time, error) {
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      MetaBucket,
		Description: "Cortex server-wide metadata",
		History:     1,
		Storage:     jetstream.FileStorage,
	})
	if err != nil {
		return time.Time{}, fmt.Errorf("ensure meta bucket %q: %w", MetaBucket, err)
	}
	if _, err := kv.Create(ctx, keyUsageTrackingSince, []byte(now.UTC().Format(time.RFC3339))); err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
		return time.Time{}, fmt.Errorf("record usage tracking start: %w", err)
	}
	e, err := kv.Get(ctx, keyUsageTrackingSince)
	if err != nil {
		return time.Time{}, fmt.Errorf("read usage tracking start: %w", err)
	}
	t, err := time.Parse(time.RFC3339, string(e.Value()))
	if err != nil {
		return time.Time{}, fmt.Errorf("parse usage tracking start %q: %w", e.Value(), err)
	}
	return t, nil
}
