package migrate

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// spied wraps the registry so a test can tell whether a migration actually ran.
func spied(runs *atomic.Int32) []Migration {
	ms := All()
	for i := range ms {
		up := ms[i].Up
		ms[i].Up = func(ctx context.Context, env *Env) error { runs.Add(1); return up(ctx, env) }
	}
	return ms
}

// The point of moving schema state into Weaviate: wiping NATS must not make the
// next boot think the store is unmigrated (it would re-run, i.e. rebuild, classes).
func TestNatsWipeKeepsSchemaStateIntegration(t *testing.T) {
	mt := testMT()
	st := testStore(t, mt)
	installLegacy(t, st)
	seed(t, st, tenants(mt)[0])

	var runs atomic.Int32
	r1 := newRunner(t, st)
	r1.Migrations = spied(&runs)
	require.NoError(t, r1.Run(ctx0))
	require.EqualValues(t, len(registry), runs.Load(), "the legacy store really needed the migration")

	r2 := newRunner(t, st) // a brand-new NATS, same Weaviate
	r2.Migrations = spied(&runs)
	before := runs.Load()
	require.NoError(t, r2.Run(ctx0))
	assert.Equal(t, before, runs.Load(), "nothing is re-applied after a NATS wipe")

	stt, err := r2.Status(ctx0)
	require.NoError(t, err)
	assert.Equal(t, Latest(), stt.SchemaVersion)
	for _, m := range stt.Migrations {
		assert.True(t, m.Applied, "history survives the wipe")
		assert.False(t, m.Assumed, "this one really ran")
		assert.False(t, m.AppliedAt.IsZero())
	}

	start := time.Now()
	require.NoError(t, WaitUntilCurrent(ctx0, st, Latest(), quiet(), time.Hour))
	assert.Less(t, time.Since(start), 5*time.Second, "the worker does not wait when Weaviate already has the version")
}

// An upgrade from an older build: state is in the KV only. It must be copied
// (original AppliedAt kept) and the migration must not run again.
func TestBackfillFromKVIntegration(t *testing.T) {
	mt := testMT()
	st := testStore(t, mt)
	installLegacy(t, st)
	seed(t, st, tenants(mt)[0])
	r := newRunner(t, st)
	var runs atomic.Int32
	r.Migrations = spied(&runs)

	when := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	for _, m := range registry {
		rec, _ := json.Marshal(Applied{ID: m.ID, Name: m.Name, AppliedAt: when, Duration: 2 * time.Second})
		_, err := r.KV.Put(ctx0, appliedKey(m.ID), rec)
		require.NoError(t, err)
	}
	_, err := r.KV.Put(ctx0, keySchemaVersion, []byte("1"))
	require.NoError(t, err)

	// (the legacy classes were not actually migrated; the KV claims they were —
	// the point here is that the KV is trusted exactly once and nothing re-runs)
	require.NoError(t, r.Run(ctx0))
	assert.EqualValues(t, 0, runs.Load())

	entries, err := st.ListMeta(ctx0, "migration.")
	require.NoError(t, err)
	require.Len(t, entries, len(registry))
	var a Applied
	require.NoError(t, json.Unmarshal(entries[0].Value, &a))
	assert.True(t, when.Equal(a.AppliedAt), "original AppliedAt preserved")
	assert.Equal(t, 2*time.Second, a.Duration)
	v, found, err := st.GetMeta(ctx0, "schema_version")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "1", string(v))
}

// A fresh Weaviate is born current: stamped, flagged assumed, nothing run.
func TestFreshStoreStampedIntegration(t *testing.T) {
	st := testStore(t, testMT())
	r := newRunner(t, st)
	var runs atomic.Int32
	r.Migrations = spied(&runs)
	require.NoError(t, r.Run(ctx0))
	assert.EqualValues(t, 0, runs.Load())
	stt, err := r.Status(ctx0)
	require.NoError(t, err)
	assert.Equal(t, Latest(), stt.SchemaVersion)
	for _, m := range stt.Migrations {
		assert.True(t, m.Assumed)
	}
}

// Existing data, no version anywhere (NATS reset before this feature): the
// migrations run from 0 rather than being assumed done.
func TestExistingStoreUnknownVersionIntegration(t *testing.T) {
	mt := testMT()
	st := testStore(t, mt)
	installLegacy(t, st)
	seed(t, st, tenants(mt)[0])
	r := newRunner(t, st)
	var runs atomic.Int32
	r.Migrations = spied(&runs)
	require.NoError(t, r.Run(ctx0))
	assert.EqualValues(t, len(registry), runs.Load())
	phys, err := st.PhysicalClass(ctx0, memory.ClassName)
	require.NoError(t, err)
	assert.Equal(t, "Memory_v1", phys)
}
