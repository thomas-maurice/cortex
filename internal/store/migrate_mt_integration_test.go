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

// TestMigrateMTFlow exercises the end-to-end migration path using real Weaviate:
//   - Seeds a NON-MT Weaviate with N memories and M summaries.
//   - Calls the migration-equivalent store operations directly (no RPC):
//     ListAllRecords, ListAllSummaries, DeleteClass, EnsureSchema (MT mode).
//   - Re-upserts each record into the bootstrap tenant (simulating what the
//     worker does after the server calls PublishIndex).
//   - Asserts: classes are now MT, the bootstrap tenant holds all N memories,
//     search works, summaries are present.
//   - Runs the guard check: IsClassMultiTenant returns true after migration.
//
// This is a store-layer test. The full RPC path (MigrateMT handler + NATS)
// requires a live server+NATS+worker and is verified manually against the dev
// stack (see SKILL.md §1). The store layer is the correctness-critical piece:
// the handler is a thin orchestrator over these store primitives.
//
// Gate: both CORTEX_TEST_WEAVIATE_REST/GRPC must be set. The test is skipped
// when CORTEX_TEST_MULTI_TENANT is already set (that env implies an MT Weaviate,
// but migration starts from NON-MT; this test needs a fresh, non-MT state).
//
// Run with a throwaway non-MT Weaviate (same instance as the schema tests):
//
//	docker run -d --name wv-migrate -p 8085:8080 -p 50055:50051 \
//	  -e AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
//	  -e PERSISTENCE_DATA_PATH=/var/lib/weaviate \
//	  -e DEFAULT_VECTORIZER_MODULE=none -e ENABLE_MODULES="" \
//	  cr.weaviate.io/semitechnologies/weaviate:1.38.0
//	CORTEX_TEST_WEAVIATE_REST=localhost:8085 CORTEX_TEST_WEAVIATE_GRPC=localhost:50055 \
//	  go test ./internal/store -run TestMigrateMTFlow -v
//	docker rm -f wv-migrate
func TestMigrateMTFlow(t *testing.T) {
	rest := os.Getenv("CORTEX_TEST_WEAVIATE_REST")
	grpc := os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST and CORTEX_TEST_WEAVIATE_GRPC to run this integration test")
	}
	if os.Getenv("CORTEX_TEST_MULTI_TENANT") != "" {
		// Migration starts from a non-MT store. Running against an MT Weaviate
		// (used by TestMultiTenantIsolation) would make the setup step fail.
		t.Skip("CORTEX_TEST_MULTI_TENANT is set; migration test requires a non-MT Weaviate — run without CORTEX_TEST_MULTI_TENANT")
	}

	ctx := context.Background()

	// --- Phase 0: setup non-MT store and seed N memories + M summaries. ---
	stNonMT, err := New(rest, grpc)
	require.NoError(t, err)
	// MT is OFF — simulates a pre-migration single-user deployment.
	stNonMT.SetMultiTenant(false)

	_ = stNonMT.DeleteClass(ctx) // ensure a clean slate
	require.NoError(t, stNonMT.EnsureSchema(ctx))
	t.Cleanup(func() { _ = stNonMT.DeleteClass(ctx) })

	// Confirm classes are NOT MT before migration.
	isMT, err := stNonMT.IsClassMultiTenant(ctx, memory.ClassName)
	require.NoError(t, err)
	assert.False(t, isMT, "Memory class must NOT be MT before migration")

	ts := stNonMT.Tenant("") // no tenant in non-MT mode
	vec := []float32{1, 0, 0, 0}
	now := time.Now().UTC()

	const numMemories = 5
	const numSummaries = 2
	memIDs := make([]string, numMemories)
	for i := 0; i < numMemories; i++ {
		id := uuid.NewString()
		memIDs[i] = id
		rec := memory.Record{
			ID:        id,
			Text:      "pre-migration memory number " + id,
			Namespace: "migration-test",
			CreatedAt: now,
			Tags:      []string{"migrated"},
		}
		require.NoError(t, ts.Upsert(ctx, rec, vec), "seed memory %d", i)
	}
	convIDs := make([]string, numSummaries)
	for i := 0; i < numSummaries; i++ {
		convID := uuid.NewString()
		convIDs[i] = convID
		require.NoError(t, ts.UpsertSummary(ctx, memory.Summary{
			ConversationID: convID,
			Text:           "pre-migration summary " + convID,
			Namespace:      "migration-test",
			CreatedAt:      now,
			UpdatedAt:      now,
		}, vec), "seed summary %d", i)
	}

	// Wait for all objects to be queryable.
	require.Eventually(t, func() bool {
		c, err := ts.Count(ctx, "")
		return err == nil && c == numMemories
	}, 15*time.Second, 200*time.Millisecond, "seeded memories must be queryable")

	// --- Phase 1: snapshot (simulate the MigrateMT handler's snapshot step). ---
	recs, err := stNonMT.ListAllRecords(ctx)
	require.NoError(t, err)
	assert.Equal(t, numMemories, len(recs), "ListAllRecords must return all seeded memories")

	sums, err := stNonMT.ListAllSummaries(ctx)
	require.NoError(t, err)
	assert.Equal(t, numSummaries, len(sums), "ListAllSummaries must return all seeded summaries")

	// Snapshot must include all seeded ids.
	snappedIDs := make(map[string]bool, len(recs))
	for _, r := range recs {
		snappedIDs[r.ID] = true
		assert.Equal(t, "migration-test", r.Namespace, "snapshot preserves namespace")
	}
	for _, id := range memIDs {
		assert.True(t, snappedIDs[id], "memory %s must appear in the snapshot", id)
	}

	// --- Phase 2: rebuild as MT (drop + EnsureSchema with MT on). ---
	require.NoError(t, stNonMT.DeleteClass(ctx), "drop non-MT classes")

	// Now create a NEW store handle with MT enabled — this is what the server
	// does: it was booted with SetMultiTenant(true) before MigrateMT is called.
	stMT, err := New(rest, grpc)
	require.NoError(t, err)
	stMT.SetMultiTenant(true)
	require.NoError(t, stMT.EnsureSchema(ctx), "recreate classes with MT enabled")

	// Confirm classes are now MT.
	isMTAfter, err := stMT.IsClassMultiTenant(ctx, memory.ClassName)
	require.NoError(t, err)
	assert.True(t, isMTAfter, "Memory class must be MT after migration")

	isMTChunk, err := stMT.IsClassMultiTenant(ctx, memory.ChunkClassName)
	require.NoError(t, err)
	assert.True(t, isMTChunk, "MemoryChunk class must be MT after migration")

	isMTSummary, err := stMT.IsClassMultiTenant(ctx, memory.SummaryClassName)
	require.NoError(t, err)
	assert.True(t, isMTSummary, "ConversationSummary class must be MT after migration")

	// --- Phase 3: re-import into the calling admin's tenant. ---
	// In production the MigrateMT handler targets the CALLER's tenant (the admin
	// running migrate-mt), so the migrated data is visible to that admin's own
	// JWT/api-key. We mirror that here with the bootstrap admin's real tenant id
	// (memory.UserID), NOT a fixed sentinel. In production the server calls
	// bus.PublishIndex and the worker upserts; here we Upsert directly to keep the
	// test self-contained with no NATS dependency.
	adminTenant := memory.UserID("admin")
	btsTenant := stMT.Tenant(adminTenant)
	for _, r := range recs {
		require.NoError(t, btsTenant.Upsert(ctx, r, vec), "re-import memory %s", r.ID)
	}
	for _, s := range sums {
		require.NoError(t, btsTenant.UpsertSummary(ctx, s, vec), "re-import summary %s", s.ConversationID)
	}

	// Wait for all re-imported memories to be queryable in the bootstrap tenant.
	require.Eventually(t, func() bool {
		c, err := btsTenant.Count(ctx, "")
		return err == nil && c == numMemories
	}, 15*time.Second, 200*time.Millisecond, "re-imported memories must be queryable in bootstrap tenant")

	// --- Assertions ---

	t.Run("bootstrap tenant holds all N memories after migration", func(t *testing.T) {
		list, err := btsTenant.List(ctx, ListOpts{Limit: allCount})
		require.NoError(t, err)
		assert.Equal(t, numMemories, len(list), "bootstrap tenant must hold exactly numMemories records")
		for _, r := range list {
			assert.Equal(t, "migration-test", r.Namespace, "migrated memory must preserve namespace")
		}
	})

	t.Run("search works in bootstrap tenant after migration", func(t *testing.T) {
		hits, err := btsTenant.SearchMemoryVectors(ctx, vec, SearchOpts{Limit: allCount})
		require.NoError(t, err)
		assert.NotEmpty(t, hits, "search must return at least one hit in bootstrap tenant")
		for _, h := range hits {
			assert.Contains(t, memIDs, h.ID, "search hit must be one of the migrated memory ids") // changed to use Contains
		}
	})

	t.Run("all original memory IDs are present after migration", func(t *testing.T) {
		list, err := btsTenant.List(ctx, ListOpts{Limit: allCount})
		require.NoError(t, err)
		foundIDs := make(map[string]bool, len(list))
		for _, r := range list {
			foundIDs[r.ID] = true
		}
		for _, id := range memIDs {
			assert.True(t, foundIDs[id], "memory %s must be present after migration", id)
		}
	})

	t.Run("summaries re-imported into bootstrap tenant", func(t *testing.T) {
		sumList, err := btsTenant.ListSummaries(ctx, SummaryListOpts{Limit: allCount})
		require.NoError(t, err)
		assert.Equal(t, numSummaries, len(sumList), "bootstrap tenant must hold all summaries after migration")
		foundConvIDs := make(map[string]bool, len(sumList))
		for _, s := range sumList {
			foundConvIDs[s.ConversationID] = true
		}
		for _, convID := range convIDs {
			assert.True(t, foundConvIDs[convID], "summary for conversation %s must be present", convID)
		}
	})

	t.Run("a second migration attempt is refused (guard: already MT)", func(t *testing.T) {
		// After migration IsClassMultiTenant must be true, so the MigrateMT
		// handler's guard refuses. We test the guard primitive directly.
		isMT, err := stMT.IsClassMultiTenant(ctx, memory.ClassName)
		require.NoError(t, err)
		assert.True(t, isMT, "IsClassMultiTenant must be true after migration — guard will refuse a second run")
	})

	t.Run("non-bootstrap tenant cannot see bootstrap data (MT isolation)", func(t *testing.T) {
		// Confirm that a different tenant sees no data (MT isolation is structural).
		otherTenant := stMT.Tenant("other-user-" + uuid.NewString())
		list, err := otherTenant.List(ctx, ListOpts{Limit: allCount})
		require.NoError(t, err)
		assert.Empty(t, list, "a different tenant must see no data after migration (MT isolation)")
	})
}
