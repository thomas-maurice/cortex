package migrate

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/thomas-maurice/cortex/internal/bus"
	"github.com/thomas-maurice/cortex/internal/store"
)

// Keys in the cortex_meta bucket owned by this package.
const (
	keySchemaVersion = "schema_version"
	keyLock          = "migrate_lock"
	keyMaintenance   = "maintenance"
	keyAppliedPrefix = "migration."
	// keyFreshPending (CortexMeta) marks a boot that found no Memory class, written
	// BEFORE EnsureSchema creates it, so a crash before the version is stamped does not
	// turn "fresh" into "existing data of unknown version" on the retry.
	keyFreshPending = "fresh_install_pending"
)

func appliedKey(id int) string { return fmt.Sprintf("%s%04d", keyAppliedPrefix, id) }

// OpenMeta opens the cortex_meta KV bucket, creating it (coordination state only:
// lock, maintenance, heartbeats) if absent. It never rewrites an existing bucket.
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

// MetaStore is the durable metadata store (the Weaviate CortexMeta class, i.e.
// *store.Store). Schema version and applied-migration records live there, NOT in
// NATS, so wiping NATS loses no durable fact. The migration lock, maintenance flag
// and heartbeats are coordination and stay in the KV bucket.
type MetaStore interface {
	GetMeta(ctx context.Context, key string) (value []byte, found bool, err error)
	PutMeta(ctx context.Context, key string, value []byte) error
	DeleteMeta(ctx context.Context, key string) error
	ListMeta(ctx context.Context, prefix string) ([]store.MetaEntry, error)
}

// SchemaVersion returns the highest applied migration ID (0 = none).
func SchemaVersion(ctx context.Context, m MetaStore) (int, error) {
	v, found, err := m.GetMeta(ctx, keySchemaVersion)
	if err != nil {
		return 0, fmt.Errorf("read schema version: %w", err)
	}
	if !found {
		return 0, nil
	}
	n, err := strconv.Atoi(string(v))
	if err != nil {
		return 0, fmt.Errorf("parse schema version %q: %w", v, err)
	}
	return n, nil
}

// setSchemaVersion advances the version. No compare-and-set: it is only written
// under the migration lease (the NATS lock), which already serialises writers.
func setSchemaVersion(ctx context.Context, m MetaStore, v int) error {
	if err := m.PutMeta(ctx, keySchemaVersion, []byte(strconv.Itoa(v))); err != nil {
		return fmt.Errorf("record schema version %d: %w", v, err)
	}
	return nil
}

// expired reports whether a lease/flag deadline has passed.
func expired(t time.Time) bool { return !time.Now().Before(t) }
