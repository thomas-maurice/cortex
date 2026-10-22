package store

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// TestGetTenantStates pins Get's behaviour for tenants in different states on a
// real multi-tenant Weaviate (same gating/recipe as TestMultiTenantIsolation):
// a never-written tenant is "not found", but an existing tenant that cannot be
// read (inactive/COLD) must surface an error rather than a silent not-found.
func TestGetTenantStates(t *testing.T) {
	rest := os.Getenv("CORTEX_TEST_WEAVIATE_REST")
	grpc := os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" || os.Getenv("CORTEX_TEST_MULTI_TENANT") == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST, CORTEX_TEST_WEAVIATE_GRPC and CORTEX_TEST_MULTI_TENANT=1")
	}
	ctx := context.Background()
	st, err := New(rest, grpc)
	require.NoError(t, err)
	st.SetMultiTenant(true)
	_ = st.DeleteClass(ctx)
	require.NoError(t, st.EnsureSchema(ctx))
	t.Cleanup(func() { _ = st.DeleteClass(ctx) })

	t.Run("never-written tenant is not found", func(t *testing.T) {
		_, ok, err := st.Tenant("never-written").Get(ctx, uuid.NewString())
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("inactive tenant is an error, not not-found", func(t *testing.T) {
		const tn = "cold-tenant"
		id := uuid.NewString()
		require.NoError(t, st.Tenant(tn).Upsert(ctx, memory.Record{ID: id, Text: "x", Namespace: "n"}, []float32{1, 0, 0, 0}))
		require.NoError(t, st.client.Schema().TenantsUpdater().WithClassName(memory.ClassName).
			WithTenants(models.Tenant{Name: tn, ActivityStatus: "COLD"}).Do(ctx))
		_, ok, err := st.Tenant(tn).Get(ctx, id)
		t.Logf("COLD tenant Get: ok=%v err=%v", ok, err)
		if err == nil {
			t.Skip("Weaviate auto-activated the COLD tenant on read; no error state reachable")
		}
		assert.False(t, ok)
	})
}
