package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/store"
)

// realStack is the worker stack with the fake store swapped for a real
// *store.TenantStore on a throwaway Weaviate (env-gated like internal/store's
// integration tests; CORTEX_TEST_MULTI_TENANT selects tenancy).
func realStack(t *testing.T) (*stack, *store.TenantStore) {
	t.Helper()
	rest, grpc := os.Getenv("CORTEX_TEST_WEAVIATE_REST"), os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST and CORTEX_TEST_WEAVIATE_GRPC to run this integration test")
	}
	st, err := store.New(rest, grpc)
	require.NoError(t, err)
	mt := os.Getenv("CORTEX_TEST_MULTI_TENANT") != ""
	st.SetMultiTenant(mt)
	ctx := context.Background()
	_ = st.DeleteAllClasses(ctx)
	t.Cleanup(func() { _ = st.DeleteAllClasses(ctx) })
	require.NoError(t, st.EnsureSchema(ctx))
	tenant := ""
	if mt {
		tenant = "worker-it-tenant"
	}
	ts := st.Tenant(tenant)
	s := newStack(t, 1, time.Minute)
	s.stores = func(string) tenantStore { return ts }
	return s, ts
}

// WHY: the stale/create-only guards compare values read back from Weaviate
// (indexVersion int round-trip, Trash lookup); fakes cannot prove those hold.
func TestWorkerGuardsAgainstRealWeaviate(t *testing.T) {
	ctx := context.Background()
	newRec := func(id, text string) memory.Record {
		return memory.Record{ID: id, Text: text, Namespace: "ns", CreatedAt: time.Now().UTC()}
	}

	t.Run("first edit delivered after second does not revert it", func(t *testing.T) {
		s, ts := realStack(t)
		id := uuid.NewString()
		const v1, v2 = int64(1760000000000001), int64(1760000000000002)
		runIndex(t, s, indexMsg(t, v2, newRec(id, "second edit")))
		old := indexMsg(t, v1, newRec(id, "first edit"))
		runIndex(t, s, old)

		got, ok, err := ts.Get(ctx, id)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, "second edit", got.Text, "stale first edit overwrote the second")
		assert.EqualValues(t, v2, got.IndexVersion)
		assert.EqualValues(t, 1, old.acked.Load())
		assert.EqualValues(t, 1, outcome(s.m, kindIndex, outcomeStale))
	})

	t.Run("create-only replay of a trashed id is skipped", func(t *testing.T) {
		s, ts := realStack(t)
		id := uuid.NewString()
		runIndex(t, s, createOnlyMsg(t, 10, newRec(id, "original")))
		require.NoError(t, ts.DeleteWithReason(ctx, id, "delete"))

		replay := createOnlyMsg(t, 20, newRec(id, "original"))
		runIndex(t, s, replay)

		_, live, err := ts.Get(ctx, id)
		require.NoError(t, err)
		assert.False(t, live, "replay resurrected a trashed memory")
		_, inTrash, err := ts.GetTrash(ctx, id)
		require.NoError(t, err)
		assert.True(t, inTrash)
		assert.EqualValues(t, 1, replay.acked.Load())
		assert.EqualValues(t, 1, outcome(s.m, kindIndex, outcomeSkipped))
	})
}
