package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// The Storage view is what an operator trusts after a crash, so against a real
// Weaviate it must report: the alias -> physical mapping and version after a swap,
// the dims actually written, a leftover Memory_v1 as an orphan, and per-tenant
// counts (from the lagging node status, so only bounded here).
func TestStorageInfoIntegration(t *testing.T) {
	st, mt := trashStore(t)
	ctx := context.Background()

	// Alias-backed class at version 2 (EnsureSchema made v1; the swap drops it).
	want := st.ClassDefinition(memory.ClassName)
	want.Properties = append(want.Properties, &models.Property{Name: "extra", DataType: []string{"text"}})
	require.NoError(t, st.SwapToVersion(ctx, memory.ClassName, want, SwapHooks{}))

	// A stray v1, as an interrupted migration would leave behind.
	stray := memoryClass(mt)
	stray.Class = "Memory_v1"
	require.NoError(t, st.client.Schema().ClassCreator().WithClass(stray).Do(ctx))

	const n = 5
	vec := []float32{1, 0, 0, 0}
	tenants := tenantsFor(st)
	for _, tn := range tenants {
		ts := st.Tenant(tn)
		for i := 0; i < n; i++ {
			rec := mkRec("ns", "storage")
			rec.Dims = len(vec)
			require.NoError(t, ts.Upsert(ctx, rec, vec))
		}
	}

	info, err := st.StorageInfo(ctx, "")
	require.NoError(t, err)
	var mem *ClassStorage
	for i := range info.Classes {
		if info.Classes[i].Logical == memory.ClassName {
			mem = &info.Classes[i]
		}
	}
	require.NotNil(t, mem)

	// Counts come from Aggregate, so they are exact (node status would read 0 here).
	assert.EqualValues(t, n*len(tenants), mem.ObjectCount)

	assert.Equal(t, "Memory_v2", mem.Physical)
	assert.Equal(t, 2, mem.Version)
	assert.True(t, mem.Aliased)
	assert.Equal(t, mt, mem.MultiTenant)
	assert.Equal(t, len(vec), mem.StoredDims, "stored dims come from the dims property")
	assert.Equal(t, "hnsw", mem.VectorIndexType)
	assert.NotEmpty(t, mem.Distance)
	assert.Contains(t, info.Orphans, "Memory_v1")
	assert.NotContains(t, info.Orphans, "Memory_v2", "the live alias target is never an orphan")
	assert.NotEmpty(t, info.Nodes)

	if mt {
		require.Len(t, mem.Tenants, len(tenants))
		assert.Equal(t, len(tenants), mem.TenantCount)
		for _, tt := range mem.Tenants {
			assert.EqualValues(t, n, tt.ObjectCount, tt.Tenant)
			assert.NotEmpty(t, tt.Activity)
		}
	}

	// Trash has no vectors: dims must stay unknown, not be invented.
	for _, c := range info.Classes {
		if c.Logical == memory.TrashClassName {
			assert.Zero(t, c.StoredDims)
		}
	}

	if mt {
		// A scoped call lists, counts and samples only the caller's tenant.
		one, err := st.StorageInfo(ctx, tenants[0])
		require.NoError(t, err)
		for _, c := range one.Classes {
			if c.Logical == memory.ClassName {
				require.Len(t, c.Tenants, 1)
				assert.Equal(t, tenants[0], c.Tenants[0].Tenant)
				assert.EqualValues(t, n, c.ObjectCount)
				assert.Equal(t, len(vec), c.StoredDims)
			}
		}
		// Constant cost: a scoped call never reads cluster-wide node status, whose
		// size grows with every tenant in the cluster.
		assert.Empty(t, one.Nodes)

		// A caller with no shard in a class gets no tenant row (and no error):
		// TenantsExists, not a full tenant listing, answers it.
		none, err := st.StorageInfo(ctx, "tenant-that-does-not-exist")
		require.NoError(t, err)
		for _, c := range none.Classes {
			if c.MultiTenant {
				assert.Empty(t, c.Tenants, c.Logical)
				assert.Zero(t, c.ObjectCount, c.Logical)
			}
		}
	}
}
