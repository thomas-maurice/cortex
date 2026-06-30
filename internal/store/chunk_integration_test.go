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

// TestChunkSearchGroupingAndCascade proves the chunking data model end-to-end
// against a REAL Weaviate (env-gated like the other integration tests; CI skips
// it). It encodes WHY each behaviour matters:
//   - Search runs over chunks but returns PARENT memories, deduped — a memory
//     with several matching chunks must appear once, not once per chunk.
//   - Deleting a memory cascades to its chunks — no orphaned chunk may keep
//     surfacing a deleted memory in search.
//   - Renaming/deleting a namespace moves/removes the chunks too — chunks carry
//     the namespace for filter push-down, so a stale chunk namespace would make a
//     namespace-scoped search wrong.
//
//	docker run -d --name wv -p 8085:8080 -p 50055:50051 \
//	  -e AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true -e PERSISTENCE_DATA_PATH=/var/lib/weaviate \
//	  -e DEFAULT_VECTORIZER_MODULE=none -e ENABLE_MODULES="" cr.weaviate.io/semitechnologies/weaviate:1.38.0
//	CORTEX_TEST_WEAVIATE_REST=localhost:8085 CORTEX_TEST_WEAVIATE_GRPC=localhost:50055 \
//	  go test ./internal/store -run TestChunkSearchGroupingAndCascade -v
func TestChunkSearchGroupingAndCascade(t *testing.T) {
	rest := os.Getenv("CORTEX_TEST_WEAVIATE_REST")
	grpc := os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST and CORTEX_TEST_WEAVIATE_GRPC to run this integration test")
	}

	ctx := context.Background()
	st, err := New(rest, grpc)
	require.NoError(t, err)
	_ = st.DeleteClass(ctx)
	require.NoError(t, st.EnsureSchema(ctx))
	t.Cleanup(func() { _ = st.DeleteClass(ctx) })

	const ns = "chunk-test"
	now := time.Now().UTC()
	query := []float32{1, 0, 0, 0}

	// Parent A: two chunks, BOTH near the query (it should still appear once).
	idA := uuid.NewString()
	recA := memory.Record{ID: idA, Text: "alpha parent", Namespace: ns, CreatedAt: now}
	require.NoError(t, st.Upsert(ctx, recA, []float32{1, 0, 0, 0}))
	require.NoError(t, st.ReplaceChunks(ctx, recA, []ChunkVec{
		{Index: 0, Text: "alpha chunk zero", Vector: []float32{1, 0, 0, 0}},
		{Index: 1, Text: "alpha chunk one", Vector: []float32{0.9, 0.1, 0, 0}},
	}))

	// Parent B: one chunk, far from the query.
	idB := uuid.NewString()
	recB := memory.Record{ID: idB, Text: "beta parent", Namespace: ns, CreatedAt: now}
	require.NoError(t, st.Upsert(ctx, recB, []float32{0, 1, 0, 0}))
	require.NoError(t, st.ReplaceChunks(ctx, recB, []ChunkVec{
		{Index: 0, Text: "beta chunk zero", Vector: []float32{0, 1, 0, 0}},
	}))

	require.Eventually(t, func() bool {
		hits, err := st.Search(ctx, query, SearchOpts{Namespace: ns, Limit: 10})
		return err == nil && len(hits) == 2
	}, 10*time.Second, 200*time.Millisecond, "both parents should become searchable via their chunks")

	t.Run("two matching chunks of one parent yield ONE deduped hit, best-distance first", func(t *testing.T) {
		hits, err := st.Search(ctx, query, SearchOpts{Namespace: ns, Limit: 10})
		require.NoError(t, err)
		assert.Equal(t, 1, countID(hits, idA), "parent A must appear exactly once despite two matching chunks")
		require.NotEmpty(t, hits)
		assert.Equal(t, idA, hits[0].ID, "the parent whose chunk is closest must rank first")
	})

	t.Run("deleting a memory cascades to its chunks", func(t *testing.T) {
		require.NoError(t, st.Delete(ctx, idA))
		require.Eventually(t, func() bool {
			hits, err := st.Search(ctx, query, SearchOpts{Namespace: ns, Limit: 10})
			return err == nil && countID(hits, idA) == 0
		}, 10*time.Second, 200*time.Millisecond, "a deleted memory's chunks must stop surfacing it")
	})

	t.Run("renaming a namespace moves the chunks too", func(t *testing.T) {
		_, _, err := st.RenameNamespace(ctx, ns, "chunk-test-2")
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			inNew, err1 := st.Search(ctx, []float32{0, 1, 0, 0}, SearchOpts{Namespace: "chunk-test-2", Limit: 10})
			inOld, err2 := st.Search(ctx, []float32{0, 1, 0, 0}, SearchOpts{Namespace: ns, Limit: 10})
			return err1 == nil && err2 == nil && countID(inNew, idB) == 1 && len(inOld) == 0
		}, 10*time.Second, 200*time.Millisecond, "chunks must be findable under the new namespace and gone from the old")
	})

	t.Run("deleting a namespace removes the chunks too", func(t *testing.T) {
		_, _, err := st.DeleteNamespace(ctx, "chunk-test-2")
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			hits, err := st.Search(ctx, []float32{0, 1, 0, 0}, SearchOpts{Namespace: "chunk-test-2", Limit: 10})
			return err == nil && len(hits) == 0
		}, 10*time.Second, 200*time.Millisecond, "a deleted namespace's chunks must be gone")
	})
}

// TestSearchFallbackForUnchunkedMemories proves the backward-compatibility
// guarantee: a memory that has NO chunks — a store indexed before chunking, or one
// mid-reindex — is still found by Search, via the whole-memory fallback. Without
// it, enabling chunking on an existing store would make everything vanish from
// search until a full reindex.
func TestSearchFallbackForUnchunkedMemories(t *testing.T) {
	rest := os.Getenv("CORTEX_TEST_WEAVIATE_REST")
	grpc := os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST and CORTEX_TEST_WEAVIATE_GRPC to run this integration test")
	}

	ctx := context.Background()
	st, err := New(rest, grpc)
	require.NoError(t, err)
	_ = st.DeleteClass(ctx)
	require.NoError(t, st.EnsureSchema(ctx))
	t.Cleanup(func() { _ = st.DeleteClass(ctx) })

	const ns = "fallback-test"
	now := time.Now().UTC()

	// "Old" memory: upserted WITHOUT chunks (simulates a pre-chunking store).
	idOld := uuid.NewString()
	require.NoError(t, st.Upsert(ctx, memory.Record{ID: idOld, Text: "legacy unchunked memory", Namespace: ns, CreatedAt: now}, []float32{1, 0, 0, 0}))

	// "New" memory: upserted WITH a chunk (post-chunking).
	idNew := uuid.NewString()
	recNew := memory.Record{ID: idNew, Text: "freshly chunked memory", Namespace: ns, CreatedAt: now}
	require.NoError(t, st.Upsert(ctx, recNew, []float32{0, 1, 0, 0}))
	require.NoError(t, st.ReplaceChunks(ctx, recNew, []ChunkVec{{Index: 0, Text: "freshly chunked memory", Vector: []float32{0, 1, 0, 0}}}))

	t.Run("an un-chunked memory is still found via the whole-memory fallback", func(t *testing.T) {
		require.Eventually(t, func() bool {
			hits, err := st.Search(ctx, []float32{1, 0, 0, 0}, SearchOpts{Namespace: ns, Limit: 5})
			return err == nil && countID(hits, idOld) == 1
		}, 10*time.Second, 200*time.Millisecond, "the chunkless legacy memory must surface via fallback")
	})

	t.Run("chunked and un-chunked memories are both returned, no duplicates", func(t *testing.T) {
		hits, err := st.Search(ctx, []float32{1, 0, 0, 0}, SearchOpts{Namespace: ns, Limit: 5})
		require.NoError(t, err)
		assert.Equal(t, 1, countID(hits, idOld), "legacy memory present exactly once")
		assert.Equal(t, 1, countID(hits, idNew), "chunked memory present exactly once (not double-counted by chunk + fallback)")
	})
}

// TestReimportReplacesChunks proves the re-import contract: re-indexing a memory
// whose text changed REPLACES its chunk set wholesale (drop-all-by-parent, then
// write), so a chunk from the OLD text can never linger and surface the memory.
func TestReimportReplacesChunks(t *testing.T) {
	rest := os.Getenv("CORTEX_TEST_WEAVIATE_REST")
	grpc := os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST and CORTEX_TEST_WEAVIATE_GRPC to run this integration test")
	}

	ctx := context.Background()
	st, err := New(rest, grpc)
	require.NoError(t, err)
	_ = st.DeleteClass(ctx)
	require.NoError(t, st.EnsureSchema(ctx))
	t.Cleanup(func() { _ = st.DeleteClass(ctx) })

	const ns = "reimport-test"
	id := uuid.NewString()
	// Whole-memory vector deliberately orthogonal to the probe, so the fallback
	// can't mask a leftover chunk — only a real chunk could surface the memory.
	rec := memory.Record{ID: id, Text: "v1", Namespace: ns, CreatedAt: time.Now().UTC()}
	require.NoError(t, st.Upsert(ctx, rec, []float32{1, 0, 0, 0}))

	// v1: three chunks, including one at the probe direction [0,1,0,0].
	require.NoError(t, st.ReplaceChunks(ctx, rec, []ChunkVec{
		{Index: 0, Text: "v1 a", Vector: []float32{1, 0, 0, 0}},
		{Index: 1, Text: "v1 b", Vector: []float32{0, 1, 0, 0}},
		{Index: 2, Text: "v1 c", Vector: []float32{0, 0, 1, 0}},
	}))
	probe := []float32{0, 1, 0, 0}
	require.Eventually(t, func() bool {
		hits, err := st.Search(ctx, probe, SearchOpts{Namespace: ns, Limit: 5, MaxDistance: 0.1})
		return err == nil && countID(hits, id) == 1
	}, 10*time.Second, 200*time.Millisecond, "v1 chunk at the probe direction should be found")

	// Re-import with changed, shorter text: a single chunk, NOT at the probe.
	require.NoError(t, st.ReplaceChunks(ctx, rec, []ChunkVec{
		{Index: 0, Text: "v2 only", Vector: []float32{1, 0, 0, 0}},
	}))

	t.Run("the old chunk is gone after re-import", func(t *testing.T) {
		require.Eventually(t, func() bool {
			hits, err := st.Search(ctx, probe, SearchOpts{Namespace: ns, Limit: 5, MaxDistance: 0.1})
			return err == nil && len(hits) == 0
		}, 10*time.Second, 200*time.Millisecond, "the v1 chunk must be dropped — re-import replaces the whole chunk set")
	})
}

func countID(hits []memory.Hit, id string) int {
	n := 0
	for _, h := range hits {
		if h.ID == id {
			n++
		}
	}
	return n
}
