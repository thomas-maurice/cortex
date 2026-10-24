package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// metaStore is trashStore plus a guaranteed-empty CortexMeta (it deliberately
// survives DeleteAllClasses).
func metaStore(t *testing.T) (*Store, bool) {
	t.Helper()
	st, mt := trashStore(t)
	ctx := context.Background()
	require.NoError(t, st.DeleteMetaClass(ctx))
	require.NoError(t, st.EnsureSchema(ctx))
	t.Cleanup(func() { _ = st.DeleteMetaClass(ctx) })
	return st, mt
}

// CortexMeta describes the store, not the data: a reindex rebuild (DeleteClass) or a
// migrate-mt (DeleteAllClasses) dropping it would forget the schema version and
// re-run every migration on a populated store.
func TestMetaSurvivesDeleteClasses(t *testing.T) {
	st, _ := metaStore(t)
	ctx := context.Background()
	require.NoError(t, st.PutMeta(ctx, "schema_version", []byte("3")))

	require.NoError(t, st.DeleteClass(ctx))
	v, found, err := st.GetMeta(ctx, "schema_version")
	require.NoError(t, err)
	require.True(t, found, "DeleteClass must keep CortexMeta")
	assert.Equal(t, "3", string(v))

	require.NoError(t, st.DeleteAllClasses(ctx))
	v, found, err = st.GetMeta(ctx, "schema_version")
	require.NoError(t, err)
	require.True(t, found, "DeleteAllClasses must keep CortexMeta")
	assert.Equal(t, "3", string(v))

	require.NoError(t, st.EnsureSchema(ctx), "and EnsureSchema afterwards is a no-op for it")
	_, found, _ = st.GetMeta(ctx, "schema_version")
	assert.True(t, found)
}

// The class must be plain, vector-less and single-tenant in EVERY mode, or an MT
// deployment could not read the schema version without a tenant.
func TestMetaClassShape(t *testing.T) {
	st, mt := metaStore(t)
	ctx := context.Background()
	c, err := st.client.Schema().ClassGetter().WithClassName(memory.MetaClassName).Do(ctx)
	require.NoError(t, err, "plain class, reachable by its own name (mt=%v)", mt)
	assert.False(t, c.MultiTenancyConfig != nil && c.MultiTenancyConfig.Enabled)
	assert.Equal(t, "none", c.Vectorizer)
	cfg, _ := c.VectorIndexConfig.(map[string]any)
	assert.Equal(t, true, cfg["skip"], "no vector index")
	targets, err := st.aliasTargets(ctx)
	require.NoError(t, err)
	assert.NotContains(t, targets, memory.MetaClassName, "never alias-backed")
}

func TestMetaGetPutCreateList(t *testing.T) {
	st, _ := metaStore(t)
	ctx := context.Background()

	_, found, err := st.GetMeta(ctx, "k")
	require.NoError(t, err)
	assert.False(t, found)

	require.NoError(t, st.PutMeta(ctx, "migration.0001", []byte(`{"id":1}`)))
	require.NoError(t, st.PutMeta(ctx, "migration.0002", []byte(`{"id":2}`)))
	require.NoError(t, st.PutMeta(ctx, "schema_version", []byte("1")))
	require.NoError(t, st.PutMeta(ctx, "schema_version", []byte("2")), "put overwrites")
	v, _, _ := st.GetMeta(ctx, "schema_version")
	assert.Equal(t, "2", string(v))

	got, err := st.ListMeta(ctx, "migration.")
	require.NoError(t, err)
	require.Len(t, got, 2, "prefix filters")
	assert.Equal(t, "migration.0001", got[0].Key)
	assert.JSONEq(t, `{"id":1}`, string(got[0].Value))
	assert.False(t, got[0].UpdatedAt.IsZero())

	existing, created, err := st.CreateMetaIfAbsent(ctx, "once", []byte("first"))
	require.NoError(t, err)
	assert.True(t, created)
	assert.Equal(t, "first", string(existing))
	existing, created, err = st.CreateMetaIfAbsent(ctx, "once", []byte("second"))
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, "first", string(existing), "the first writer wins")
}

// Replicas booting together must agree on one epoch.
func TestEnsureUsageTrackingSince(t *testing.T) {
	st, _ := metaStore(t)
	ctx := context.Background()
	legacy := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	calls := 0
	legacyFn := func(context.Context) (time.Time, bool, error) { calls++; return legacy, true, nil }

	got, err := st.EnsureUsageTrackingSince(ctx, time.Now(), legacyFn)
	require.NoError(t, err)
	assert.True(t, legacy.Equal(got), "the old KV epoch is carried over, not reset to now")

	got, err = st.EnsureUsageTrackingSince(ctx, time.Now().Add(48*time.Hour), legacyFn)
	require.NoError(t, err)
	assert.True(t, legacy.Equal(got), "create-once: second call returns the same value")
	assert.Equal(t, 1, calls, "the legacy KV is not read once CortexMeta has the value")

	require.NoError(t, st.DeleteMetaClass(ctx))
	require.NoError(t, st.EnsureSchema(ctx))
	now := time.Date(2026, 5, 5, 5, 5, 5, 0, time.UTC)
	got, err = st.EnsureUsageTrackingSince(ctx, now, func(context.Context) (time.Time, bool, error) { return time.Time{}, false, nil })
	require.NoError(t, err)
	assert.True(t, now.Equal(got), "no legacy value: now")

	_, err = st.EnsureUsageTrackingSince(ctx, now, func(context.Context) (time.Time, bool, error) { return time.Time{}, false, errors.New("boom") })
	require.NoError(t, err, "legacy is not consulted when the value already exists")
}

// Weaviate's create is not atomic for simultaneous creators (documented on
// CreateMetaIfAbsent): what must hold is that a value always ends up stored and at
// least one caller is told it created it.
func TestCreateMetaIfAbsentRace(t *testing.T) {
	st, _ := metaStore(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	var mu sync.Mutex
	created := 0
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, c, err := st.CreateMetaIfAbsent(ctx, "race", []byte{byte('a' + i)}); err == nil && c {
				mu.Lock()
				created++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	assert.GreaterOrEqual(t, created, 1)
	v, found, err := st.GetMeta(ctx, "race")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Len(t, v, 1)
}
