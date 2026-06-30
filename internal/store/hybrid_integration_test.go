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

// TestHybridSurfacesKeyword proves the reason hybrid search exists: an exact
// token that does NOT embed near its meaning (a codename like "mega-fucker")
// must still be found. It is an integration test against a REAL Weaviate, gated
// on env so CI (which has no Weaviate) skips it. Run locally with:
//
//	docker run -d --name wv -p 8085:8080 -p 50055:50051 \
//	  -e AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
//	  -e PERSISTENCE_DATA_PATH=/var/lib/weaviate \
//	  -e DEFAULT_VECTORIZER_MODULE=none -e ENABLE_MODULES="" \
//	  cr.weaviate.io/semitechnologies/weaviate:1.38.0
//	CORTEX_TEST_WEAVIATE_REST=localhost:8085 \
//	  CORTEX_TEST_WEAVIATE_GRPC=localhost:50055 \
//	  go test ./internal/store -run TestHybridSurfacesKeyword -v
//
// Vectors are crafted (no Ollama): the keyword doc is placed FAR from the query
// vector and the distractors NEAR it, so pure-vector search ranks the keyword
// doc last. Hybrid then has to use BM25 to pull it to the top — which is exactly
// the behaviour we are proving.
func TestHybridSurfacesKeyword(t *testing.T) {
	rest := os.Getenv("CORTEX_TEST_WEAVIATE_REST")
	grpc := os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST and CORTEX_TEST_WEAVIATE_GRPC to run this integration test")
	}

	ctx := context.Background()
	st, err := New(rest, grpc)
	require.NoError(t, err)

	// Fresh class so prior runs don't interfere.
	_ = st.DeleteClass(ctx)
	require.NoError(t, st.EnsureSchema(ctx))

	const ns = "hybrid-test"
	// 4-dim crafted vectors. "near" points one way, the keyword doc the opposite.
	near := []float32{1, 0, 0, 0}
	far := []float32{0, 0, 0, 1}
	query := []float32{1, 0, 0, 0} // identical to near, orthogonal to far

	// Weaviate object ids must be UUIDs; keep stable ones so assertions can refer
	// to roles by name.
	nearID1 := uuid.NewString()
	nearID2 := uuid.NewString()
	keywordID := uuid.NewString()

	docs := []struct {
		id, text string
		vec      []float32
	}{
		{nearID1, "my favourite programming language is Go", near},
		{nearID2, "the deployment pipeline runs on a schedule", near},
		// The only doc containing the literal token, deliberately far in vector space.
		{keywordID, "the production NAS host is codenamed mega-fucker and handles most traffic", far},
	}
	for _, d := range docs {
		rec := memory.Record{ID: d.id, Text: d.text, Namespace: ns, CreatedAt: time.Now().UTC()}
		require.NoError(t, st.Upsert(ctx, rec, d.vec))
		// Search now matches against MemoryChunk, so each doc needs a chunk. These
		// texts are short → a single chunk carrying the whole text and the same
		// crafted vector, which preserves the hybrid keyword-vs-vector behaviour the
		// test exercises (BM25 runs over the chunk text == the full text).
		require.NoError(t, st.ReplaceChunks(ctx, rec, []ChunkVec{{Index: 0, Text: d.text, Vector: d.vec}}))
	}

	// Wait until all three are queryable (Weaviate indexing is near-real-time).
	require.Eventually(t, func() bool {
		c, err := st.Count(ctx, ns)
		return err == nil && c == 3
	}, 10*time.Second, 200*time.Millisecond, "memories did not become searchable")

	rankOf := func(hits []memory.Hit, id string) int {
		for i, h := range hits {
			if h.ID == id {
				return i
			}
		}
		return -1
	}

	// This is the exact bug reported: searching the codename returns nothing
	// because the doc is far in vector space and the relevance cutoff drops it.
	t.Run("pure vector + cutoff drops the keyword doc entirely (the bug)", func(t *testing.T) {
		hits, err := st.Search(ctx, query, SearchOpts{Namespace: ns, Limit: 5, MaxDistance: 0.6})
		require.NoError(t, err)
		assert.Equal(t, -1, rankOf(hits, keywordID), "vector search with a cutoff must NOT return the orthogonal keyword doc — reproduces the '0 insight' report")
	})

	// The fix: hybrid makes the exact-token doc a candidate via BM25, so the same
	// cutoff no longer hides it.
	t.Run("hybrid retrieves the keyword doc through the same cutoff (the fix)", func(t *testing.T) {
		hits, err := st.Search(ctx, query, SearchOpts{Namespace: ns, Limit: 5, MaxDistance: 0.6, Query: "mega-fucker", Alpha: 0.5})
		require.NoError(t, err)
		assert.NotEqual(t, -1, rankOf(hits, keywordID), "hybrid must surface the exact-token doc that pure vector dropped")
	})

	// And with keyword weight it ranks first.
	t.Run("keyword-leaning blend ranks the exact-token doc first", func(t *testing.T) {
		hits, err := st.Search(ctx, query, SearchOpts{Namespace: ns, Limit: 5, Query: "mega-fucker", Alpha: 0.2})
		require.NoError(t, err)
		require.NotEmpty(t, hits)
		assert.Equal(t, keywordID, hits[0].ID, "with keyword weight the exact-token doc must be the top hit")
	})

	_ = st.DeleteClass(ctx)
}
