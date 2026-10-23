package migrate

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/thomas-maurice/cortex/internal/bus"
)

// Keys in the cortex_meta bucket owned by this package.
const (
	keySchemaVersion = "schema_version"
	keyLock          = "migrate_lock"
	keyMaintenance   = "maintenance"
	keyAppliedPrefix = "migration."
)

func appliedKey(id int) string { return fmt.Sprintf("%s%04d", keyAppliedPrefix, id) }

// OpenMeta opens the cortex_meta KV bucket, creating it (same config as
// bus.EnsureUsageTrackingSince) if absent. It never rewrites an existing bucket.
func OpenMeta(ctx context.Context, js jetstream.JetStream) (jetstream.KeyValue, error) {
	kv, err := js.KeyValue(ctx, bus.MetaBucket)
	if err == nil {
		return kv, nil
	}
	if !errors.Is(err, jetstream.ErrBucketNotFound) {
		return nil, fmt.Errorf("open meta bucket %q: %w", bus.MetaBucket, err)
	}
	kv, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      bus.MetaBucket,
		Description: "Cortex server-wide metadata",
		History:     1,
		Storage:     jetstream.FileStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("ensure meta bucket %q: %w", bus.MetaBucket, err)
	}
	return kv, nil
}

// SchemaVersion returns the highest applied migration ID (0 = none) and the KV
// revision of that entry (0 if absent), for compare-and-set updates.
func SchemaVersion(ctx context.Context, kv jetstream.KeyValue) (int, uint64, error) {
	e, err := kv.Get(ctx, keySchemaVersion)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("read schema version: %w", err)
	}
	v, err := strconv.Atoi(string(e.Value()))
	if err != nil {
		return 0, 0, fmt.Errorf("parse schema version %q: %w", e.Value(), err)
	}
	return v, e.Revision(), nil
}

// setSchemaVersion advances the version with a revision check, so two writers can
// never silently overwrite each other (the lease makes that unlikely; this makes
// it impossible).
func setSchemaVersion(ctx context.Context, kv jetstream.KeyValue, v int, rev uint64) error {
	val := []byte(strconv.Itoa(v))
	var err error
	if rev == 0 {
		_, err = kv.Create(ctx, keySchemaVersion, val)
	} else {
		_, err = kv.Update(ctx, keySchemaVersion, val, rev)
	}
	if err != nil {
		return fmt.Errorf("record schema version %d (revision %d): %w", v, rev, err)
	}
	return nil
}

// expired reports whether a lease/flag deadline has passed.
func expired(t time.Time) bool { return !time.Now().Before(t) }
