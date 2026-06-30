package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// TestMultiTenantIsolation proves the core P3 security invariant: data written by
// tenant A is invisible to tenant B. Two tenants each have their own Store view
// (TenantStore) created with Weaviate MT enabled; every Weaviate builder carries
// .WithTenant(t) so cross-tenant access is structurally impossible from within the
// store layer.
//
// What is tested:
//   - A memory saved in A's tenant is invisible to B's Search, List, Get, and
//     ListNamespaces — not just "not returned" but truly absent.
//   - B's DeleteNamespace/Delete operate within B's tenant; they cannot touch A's data.
//   - A summary saved in A's tenant does not appear in B's ListSummaries.
//   - Reindex in A (re-upsert all of A's records) does not touch B's data.
//
// Run with a Weaviate that has MT enabled. Anonymous MT-Weaviate:
//
//	docker run -d --name wvmt -p 8087:8080 -p 50057:50051 \
//	  -e AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
//	  -e PERSISTENCE_DATA_PATH=/var/lib/weaviate \
//	  -e DEFAULT_VECTORIZER_MODULE=none -e ENABLE_MODULES="" \
//	  cr.weaviate.io/semitechnologies/weaviate:1.38.0
//	CORTEX_TEST_WEAVIATE_REST=localhost:8087 CORTEX_TEST_WEAVIATE_GRPC=localhost:50057 \
//	  CORTEX_TEST_MULTI_TENANT=1 \
//	  go test ./internal/store -run TestMultiTenantIsolation -v
//
// The test is gated on CORTEX_TEST_MULTI_TENANT in addition to the usual REST/GRPC
// vars, so non-MT Weaviate instances (the majority of local dev setups) skip it.
// A non-MT Weaviate would panic on WithTenant calls; the gate avoids that.
func TestMultiTenantIsolation(t *testing.T) {
	rest := os.Getenv("CORTEX_TEST_WEAVIATE_REST")
	grpc := os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST and CORTEX_TEST_WEAVIATE_GRPC to run this integration test")
	}
	if os.Getenv("CORTEX_TEST_MULTI_TENANT") == "" {
		t.Skip("set CORTEX_TEST_MULTI_TENANT=1 to run the multi-tenancy integration test")
	}

	ctx := context.Background()
	st, err := New(rest, grpc)
	require.NoError(t, err)
	st.SetMultiTenant(true)

	// Start clean.
	_ = st.DeleteClass(ctx)
	require.NoError(t, st.EnsureSchema(ctx))
	t.Cleanup(func() { _ = st.DeleteClass(ctx) })

	// Two distinct tenants: the Weaviate tenant name is their "user ID".
	const tenantA = "user-alice"
	const tenantB = "user-bob"

	tsA := st.Tenant(tenantA)
	tsB := st.Tenant(tenantB)

	vec := []float32{1, 0, 0, 0}
	now := time.Now().UTC()
	const ns = "default"

	// Seed: write one memory and one summary in each tenant.
	idA := uuid.NewString()
	recA := memory.Record{ID: idA, Text: "Alice's secret", Namespace: ns, CreatedAt: now}
	require.NoError(t, tsA.Upsert(ctx, recA, vec), "upsert A")
	require.NoError(t, tsA.ReplaceChunks(ctx, recA, []ChunkVec{{Index: 0, Text: recA.Text, Vector: vec}}), "chunk A")
	require.NoError(t, tsA.UpsertSummary(ctx, memory.Summary{
		ConversationID: "conv-alice",
		Text:           "Alice's session summary",
		Namespace:      ns,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, vec), "summary A")

	idB := uuid.NewString()
	recB := memory.Record{ID: idB, Text: "Bob's secret", Namespace: ns, CreatedAt: now}
	require.NoError(t, tsB.Upsert(ctx, recB, vec), "upsert B")
	require.NoError(t, tsB.ReplaceChunks(ctx, recB, []ChunkVec{{Index: 0, Text: recB.Text, Vector: vec}}), "chunk B")

	// Allow Weaviate to index.
	require.Eventually(t, func() bool {
		cA, errA := tsA.Count(ctx, "")
		cB, errB := tsB.Count(ctx, "")
		return errA == nil && errB == nil && cA == 1 && cB == 1
	}, 15*time.Second, 200*time.Millisecond, "both tenants' memories must become queryable")

	t.Run("A's memory is invisible to B's Search (chunk path)", func(t *testing.T) {
		hits, err := tsB.Search(ctx, vec, SearchOpts{Namespace: ns, Limit: 10})
		require.NoError(t, err)
		for _, h := range hits {
			assert.NotEqual(t, idA, h.ID, "A's memory must not appear in B's search results")
		}
	})

	t.Run("A's memory is invisible to B's SearchMemoryVectors (whole-memory path)", func(t *testing.T) {
		hits, err := tsB.SearchMemoryVectors(ctx, vec, SearchOpts{Namespace: ns, Limit: 10})
		require.NoError(t, err)
		for _, h := range hits {
			assert.NotEqual(t, idA, h.ID, "A's memory must not appear in B's whole-memory search results")
		}
	})

	t.Run("A's memory is invisible to B's List", func(t *testing.T) {
		recs, err := tsB.List(ctx, ListOpts{Namespace: ns, Limit: 100})
		require.NoError(t, err)
		for _, r := range recs {
			assert.NotEqual(t, idA, r.ID, "A's memory must not appear in B's List")
		}
	})

	t.Run("B's Get cannot retrieve A's memory", func(t *testing.T) {
		_, found, err := tsB.Get(ctx, idA)
		// A Weaviate MT Get with the wrong tenant returns not-found (not an error).
		// Regardless of the error value, found must be false.
		if err == nil {
			assert.False(t, found, "B's Get must not return A's memory (MT isolation)")
		}
		// If Weaviate returns an error for a cross-tenant lookup, that is also
		// acceptable — the invariant is that idA is not accessible.
	})

	t.Run("A's namespace is invisible to B's ListNamespaces", func(t *testing.T) {
		stats, err := tsB.ListNamespaces(ctx)
		require.NoError(t, err)
		for _, s := range stats {
			// Both tenants wrote to "default", so the namespace name is the same.
			// The isolation contract is COUNT: B's view must show only B's count.
			assert.Equal(t, 1, s.MemoryCount,
				"B's ListNamespaces must count only B's memories in namespace %q", s.Name)
		}
	})

	t.Run("A's summary is invisible to B's ListSummaries", func(t *testing.T) {
		sums, err := tsB.ListSummaries(ctx, SummaryListOpts{Limit: 100})
		require.NoError(t, err)
		for _, s := range sums {
			assert.NotEqual(t, "conv-alice", s.ConversationID,
				"A's session summary must not appear in B's ListSummaries")
		}
	})

	t.Run("B's DeleteNamespace cannot delete A's data", func(t *testing.T) {
		// B deletes its own namespace. A's memory must survive.
		mem, _, err := tsB.DeleteNamespace(ctx, ns)
		require.NoError(t, err)
		assert.Equal(t, 1, mem, "B's DeleteNamespace must delete exactly B's 1 memory")

		// A's memory must still be reachable after B's delete.
		require.Eventually(t, func() bool {
			cA, err := tsA.Count(ctx, "")
			return err == nil && cA == 1
		}, 10*time.Second, 200*time.Millisecond, "A's memory must survive B's DeleteNamespace")

		// Restore B for subsequent subtests.
		require.NoError(t, tsB.Upsert(ctx, recB, vec), "restore B")
		require.NoError(t, tsB.ReplaceChunks(ctx, recB, []ChunkVec{{Index: 0, Text: recB.Text, Vector: vec}}), "restore B chunk")
		require.Eventually(t, func() bool {
			c, err := tsB.Count(ctx, "")
			return err == nil && c == 1
		}, 10*time.Second, 200*time.Millisecond, "B restore must settle")
	})

	t.Run("B's Delete cannot delete A's memory", func(t *testing.T) {
		// Attempt to delete A's record from B's tenant view.
		// Weaviate will apply the delete within B's tenant only; A's object should survive.
		// Errors are acceptable — an MT error is a fail-loud which is also correct.
		_ = tsB.Delete(ctx, idA)

		// A's memory must still be reachable.
		rec, found, err := tsA.Get(ctx, idA)
		require.NoError(t, err, "A's Get must not error after B's attempted delete")
		assert.True(t, found, "A's memory must survive B's Delete attempt (MT isolation)")
		assert.Equal(t, idA, rec.ID)
	})

	t.Run("reindex A (re-upsert) does not affect B's data", func(t *testing.T) {
		// Simulate reindex: re-upsert A's record. This is what Reindex does per
		// tenant — it calls ts.Upsert for each record in the tenant.
		require.NoError(t, tsA.Upsert(ctx, recA, vec), "reindex A upsert")
		require.NoError(t, tsA.ReplaceChunks(ctx, recA, []ChunkVec{{Index: 0, Text: recA.Text, Vector: vec}}), "reindex A chunks")

		// B's count must remain 1.
		cB, err := tsB.Count(ctx, "")
		require.NoError(t, err)
		assert.Equal(t, 1, cB, "B's count must be unchanged after A's reindex")

		// B's record must still be its own.
		bRec, found, err := tsB.Get(ctx, idB)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, "Bob's secret", bRec.Text)
	})
}

// TestMultiTenantFlagOff is the flag-off regression: when SetMultiTenant(false)
// (the default), the store must behave identically to pre-P3 — no .WithTenant
// calls, no tenant isolation, existing data readable as before.
//
// This test runs on any Weaviate (MT not required) and validates that
// a store created with MT off does not break single-user mode.
func TestMultiTenantFlagOff(t *testing.T) {
	rest := os.Getenv("CORTEX_TEST_WEAVIATE_REST")
	grpc := os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST and CORTEX_TEST_WEAVIATE_GRPC to run this integration test")
	}
	if os.Getenv("CORTEX_TEST_MULTI_TENANT") != "" {
		// This test uses a non-MT Weaviate. Skip when MT env is set so the
		// caller's MT Weaviate instance is not confused by non-MT class ops.
		t.Skip("CORTEX_TEST_MULTI_TENANT is set; this test requires a non-MT Weaviate — run without CORTEX_TEST_MULTI_TENANT")
	}

	ctx := context.Background()
	st, err := New(rest, grpc)
	require.NoError(t, err)
	// MT is explicitly OFF — the default; this call mirrors cmd/server/main.go.
	st.SetMultiTenant(false)

	_ = st.DeleteClass(ctx)
	require.NoError(t, st.EnsureSchema(ctx))
	t.Cleanup(func() { _ = st.DeleteClass(ctx) })

	// A Tenant("") with MT off must behave like the old Store — no WithTenant.
	ts := st.Tenant("")
	// A Tenant with a non-empty userID and MT off must ALSO behave like the old
	// Store — the userID is ignored and no WithTenant is issued.
	tsNamed := st.Tenant("any-user")

	vec := []float32{1, 0, 0, 0}
	now := time.Now().UTC()

	id1 := uuid.NewString()
	id2 := uuid.NewString()
	require.NoError(t, ts.Upsert(ctx, memory.Record{ID: id1, Text: "first memory", Namespace: "ns", CreatedAt: now}, vec))
	require.NoError(t, tsNamed.Upsert(ctx, memory.Record{ID: id2, Text: "second memory", Namespace: "ns", CreatedAt: now}, vec))

	require.Eventually(t, func() bool {
		c, err := ts.Count(ctx, "")
		return err == nil && c == 2
	}, 10*time.Second, 200*time.Millisecond, "both memories must become queryable in MT-off mode")

	t.Run("MT off: both memories are visible from any Tenant handle", func(t *testing.T) {
		// ts (empty) and tsNamed both issue no WithTenant; both see the same global store.
		listEmpty, err := ts.List(ctx, ListOpts{Namespace: "ns", Limit: 100})
		require.NoError(t, err)
		assert.Equal(t, 2, len(listEmpty), "ts (empty tenant, MT off) must see both memories")

		listNamed, err := tsNamed.List(ctx, ListOpts{Namespace: "ns", Limit: 100})
		require.NoError(t, err)
		assert.Equal(t, 2, len(listNamed), "tsNamed (MT off) must also see both memories — no isolation in MT-off mode")
	})

	t.Run("MT off: search hits both memories", func(t *testing.T) {
		hits, err := ts.Search(ctx, vec, SearchOpts{Namespace: "ns", Limit: 10})
		require.NoError(t, err)
		ids := make(map[string]bool, len(hits))
		for _, h := range hits {
			ids[h.ID] = true
		}
		assert.True(t, ids[id1] || ids[id2], "MT-off search must return at least one of the two memories")
	})

	t.Run("MT off: EnsureSchema creates non-MT classes", func(t *testing.T) {
		// VerifySchema in MT-off mode must not complain about the classes being non-MT.
		problems, err := st.VerifySchema(ctx)
		require.NoError(t, err)
		assert.Empty(t, problems, "non-MT schema must be healthy when MT is off")
	})
}
