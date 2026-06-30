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

// TestNamespaceOperations proves the namespace admin surface end-to-end against a
// REAL Weaviate (gated on env like the other integration tests, so CI skips it):
//
//	CORTEX_TEST_WEAVIATE_REST=localhost:8085 \
//	  CORTEX_TEST_WEAVIATE_GRPC=localhost:50055 \
//	  go test ./internal/store -run TestNamespaceOperations -v
//
// It encodes the WHY of each operation, not just the WHAT:
//   - ListNamespaces must count memories AND summaries per namespace, because the
//     view exists to show the true contents of a namespace, not just its memories.
//   - RenameNamespace must move BOTH memories and summaries, or a rename would
//     orphan a namespace's summaries (session recall would break for the old name).
//   - DeleteNamespace must remove BOTH and leave OTHER namespaces untouched, since
//     it is a scoped bulk delete, not a wipe.
func TestNamespaceOperations(t *testing.T) {
	rest := os.Getenv("CORTEX_TEST_WEAVIATE_REST")
	grpc := os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST and CORTEX_TEST_WEAVIATE_GRPC to run this integration test")
	}

	ctx := context.Background()
	st, err := New(rest, grpc)
	require.NoError(t, err)

	// Fresh classes so prior runs don't interfere.
	_ = st.DeleteClass(ctx)
	require.NoError(t, st.EnsureSchema(ctx))
	t.Cleanup(func() { _ = st.DeleteClass(ctx) })

	// MT off (empty tenant) — identical to pre-P3 single-user behaviour.
	ts := st.Tenant("")

	vec := []float32{1, 0, 0, 0}
	now := time.Now().UTC()

	// alpha: 2 memories + 1 summary; beta: 1 memory. Renaming/deleting alpha must
	// never touch beta.
	require.NoError(t, ts.Upsert(ctx, memory.Record{ID: uuid.NewString(), Text: "alpha one", Namespace: "alpha", CreatedAt: now}, vec))
	require.NoError(t, ts.Upsert(ctx, memory.Record{ID: uuid.NewString(), Text: "alpha two", Namespace: "alpha", CreatedAt: now}, vec))
	require.NoError(t, ts.Upsert(ctx, memory.Record{ID: uuid.NewString(), Text: "beta one", Namespace: "beta", CreatedAt: now}, vec))
	require.NoError(t, ts.UpsertSummary(ctx, memory.Summary{ConversationID: "conv-alpha", Text: "alpha session", Namespace: "alpha", CreatedAt: now, UpdatedAt: now}, vec))

	require.Eventually(t, func() bool {
		c, err := ts.Count(ctx, "")
		return err == nil && c == 3
	}, 10*time.Second, 200*time.Millisecond, "memories did not become queryable")

	statByName := func() map[string]NamespaceStat {
		stats, err := ts.ListNamespaces(ctx)
		require.NoError(t, err)
		m := map[string]NamespaceStat{}
		for _, s := range stats {
			m[s.Name] = s
		}
		return m
	}

	t.Run("ListNamespaces counts memories and summaries per namespace", func(t *testing.T) {
		m := statByName()
		require.Contains(t, m, "alpha")
		require.Contains(t, m, "beta")
		assert.Equal(t, 2, m["alpha"].MemoryCount)
		assert.Equal(t, 1, m["alpha"].SummaryCount, "summaries must be counted, not just memories")
		assert.Equal(t, 1, m["beta"].MemoryCount)
		assert.Equal(t, 0, m["beta"].SummaryCount)
	})

	t.Run("RenameNamespace moves both memories and summaries, leaving others alone", func(t *testing.T) {
		mem, sum, err := ts.RenameNamespace(ctx, "alpha", "gamma")
		require.NoError(t, err)
		assert.Equal(t, 2, mem)
		assert.Equal(t, 1, sum, "the summary must move too, or session recall orphans on the old name")

		require.Eventually(t, func() bool {
			m := statByName()
			_, alphaGone := m["alpha"]
			return !alphaGone && m["gamma"].MemoryCount == 2 && m["gamma"].SummaryCount == 1 && m["beta"].MemoryCount == 1
		}, 10*time.Second, 200*time.Millisecond, "rename did not settle: alpha→gamma with beta untouched")
	})

	t.Run("DeleteNamespace removes both and leaves other namespaces intact", func(t *testing.T) {
		mem, sum, err := ts.DeleteNamespace(ctx, "gamma")
		require.NoError(t, err)
		assert.Equal(t, 2, mem)
		assert.Equal(t, 1, sum, "the summary must be deleted too, not orphaned")

		require.Eventually(t, func() bool {
			m := statByName()
			_, gammaGone := m["gamma"]
			return !gammaGone && m["beta"].MemoryCount == 1
		}, 10*time.Second, 200*time.Millisecond, "delete did not settle: gamma gone, beta untouched")
	})
}
