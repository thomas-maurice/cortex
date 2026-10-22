package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// These tests pin WHY Weaviate must run with AUTOSCHEMA_ENABLED=false (scripts/
// wv-test.sh, compose files and CI all set it). With auto-schema on, a worker
// write landing while the server drops/recreates classes (reindex rebuild,
// migrate-mt) would silently create a wrong-shaped class (non-MT, word
// tokenization on exact-match keys) and the server's own recreate would then fail.
// With it off the write fails and the worker's retry applies it once the schema
// exists. They need a Weaviate started with auto-schema off; against one with it
// on they fail, which is the intended loud signal.

func TestAutoSchemaOffMissingClassWriteFails(t *testing.T) {
	st, tenant := integrationStore(t) // classes dropped, none ensured
	ctx := context.Background()
	ts := st.Tenant(tenant)

	err := ts.Upsert(ctx, memory.Record{ID: uuid.NewString(), Text: "x", Namespace: "ns", CreatedAt: time.Now()}, []float32{1, 0, 0, 0})
	require.Error(t, err, "a write to a missing class must fail, not auto-create it")

	exists, err := st.client.Schema().ClassExistenceChecker().WithClassName(memory.ClassName).Do(ctx)
	require.NoError(t, err)
	assert.False(t, exists, "the failed write must not have auto-created the Memory class")

	err = ts.UpsertSummary(ctx, memory.Summary{ConversationID: "c", Text: "s", CreatedAt: time.Now(), UpdatedAt: time.Now()}, []float32{1, 0, 0, 0})
	require.Error(t, err)
	exists, err = st.client.Schema().ClassExistenceChecker().WithClassName(memory.SummaryClassName).Do(ctx)
	require.NoError(t, err)
	assert.False(t, exists, "the failed write must not have auto-created ConversationSummary")

	// Once the schema is ensured the same write succeeds (the worker retry path).
	require.NoError(t, st.EnsureSchema(ctx))
	require.NoError(t, ts.Upsert(ctx, memory.Record{ID: uuid.NewString(), Text: "x", Namespace: "ns", CreatedAt: time.Now()}, []float32{1, 0, 0, 0}))
}

func TestAutoSchemaOffUndeclaredPropertyFails(t *testing.T) {
	st, tenant := integrationStore(t)
	ctx := context.Background()
	require.NoError(t, st.EnsureSchema(ctx))

	creator := st.client.Data().Creator().WithClassName(memory.ClassName).WithID(uuid.NewString()).
		WithProperties(map[string]interface{}{"text": "x", "notDeclaredAnywhere": "boom"}).
		WithVector([]float32{1, 0, 0, 0})
	if tenant != "" {
		creator = creator.WithTenant(tenant)
	}
	_, err := creator.Do(ctx)
	require.Error(t, err, "an undeclared property must be rejected, not silently added to the schema")

	class, err := st.client.Schema().ClassGetter().WithClassName(memory.ClassName).Do(ctx)
	require.NoError(t, err)
	for _, p := range class.Properties {
		assert.NotEqual(t, "notDeclaredAnywhere", p.Name)
	}
}
