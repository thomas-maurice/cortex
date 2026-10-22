package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/chunk"
	"github.com/thomas-maurice/cortex/internal/memory"
)

// TestSearchReturnsMatchedChunk proves WHY the winning chunk is surfaced: for a
// long memory only one late section matches the query, and the user (UI/MCP) must
// be shown THAT section, located in the full text, rather than the whole document.
// Run in non-MT and (CORTEX_TEST_MULTI_TENANT=1) MT mode.
func TestSearchReturnsMatchedChunk(t *testing.T) {
	rest := os.Getenv("CORTEX_TEST_WEAVIATE_REST")
	grpc := os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST and CORTEX_TEST_WEAVIATE_GRPC to run this integration test")
	}
	mt := os.Getenv("CORTEX_TEST_MULTI_TENANT") != ""

	ctx := context.Background()
	st, err := New(rest, grpc)
	require.NoError(t, err)
	st.SetMultiTenant(mt)
	_ = st.DeleteClass(ctx)
	require.NoError(t, st.EnsureSchema(ctx))
	t.Cleanup(func() { _ = st.DeleteClass(ctx) })
	tenant := ""
	if mt {
		tenant = "user-match"
	}
	ts := st.Tenant(tenant)

	// A long memory made of paragraphs (blank-line separated, so the chunker's
	// space-joined chunks are NOT byte-exact substrings: Locate must cope).
	var paras []string
	for i := 0; i < 60; i++ {
		paras = append(paras, fmt.Sprintf("Section %d covers topic number %d in some detail. It has a second sentence about topic %d.", i, i, i))
	}
	long := strings.Join(paras, "\n\n")
	ck, err := chunk.New(64, 16)
	require.NoError(t, err)
	parts := ck.Split(long)
	require.Greater(t, len(parts), 3, "fixture must produce several chunks")
	late := len(parts) - 2

	const ns = "match-test"
	now := time.Now().UTC()
	query := []float32{1, 0, 0, 0}

	idLong := uuid.NewString()
	recLong := memory.Record{ID: idLong, Text: long, Namespace: ns, CreatedAt: now}
	require.NoError(t, ts.Upsert(ctx, recLong, []float32{0, 0, 1, 0})) // whole-memory vector is far from the query
	cvs := make([]ChunkVec, len(parts))
	for i, p := range parts {
		v := []float32{0, 1, 0, 0}
		switch i {
		case late:
			v = []float32{1, 0, 0, 0}
		case late - 1: // a second, worse chunk of the SAME memory: grouping must keep the best
			v = []float32{0.7, 0.7, 0, 0}
		}
		cvs[i] = ChunkVec{Index: i, Text: p, Vector: v}
	}
	require.NoError(t, ts.ReplaceChunks(ctx, recLong, cvs))

	// Un-chunked memory: only found through the whole-memory fallback.
	idOld := uuid.NewString()
	require.NoError(t, ts.Upsert(ctx, memory.Record{ID: idOld, Text: "legacy unchunked", Namespace: ns, CreatedAt: now}, []float32{0.9, 0.1, 0, 0}))

	var hits []memory.Hit
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var err error
		hits, err = ts.Search(ctx, query, SearchOpts{Namespace: ns, Limit: 5})
		require.NoError(c, err)
		require.Equal(c, 1, countID(hits, idLong))
		require.Equal(c, 1, countID(hits, idOld))
	}, 10*time.Second, 200*time.Millisecond)

	var hl, ho memory.Hit
	for _, h := range hits {
		switch h.ID {
		case idLong:
			hl = h
		case idOld:
			ho = h
		}
	}

	require.NotNil(t, hl.Match, "a chunk-found hit must carry its winning chunk")
	assert.Equal(t, late, hl.Match.ChunkIndex, "grouping must keep the BEST chunk, not the first/last seen")
	assert.Equal(t, parts[late], hl.Match.Text)
	require.Greater(t, hl.Match.End, hl.Match.Start, "the chunk must be located in the full text")
	span := string([]rune(long)[hl.Match.Start:hl.Match.End])
	assert.Equal(t, strings.Fields(parts[late]), strings.Fields(span),
		"offsets must point at the matched chunk inside the full memory text (modulo whitespace)")
	assert.Greater(t, hl.Match.Start, len([]rune(long))/2, "only a LATE chunk matched, so the span must be late in the text")

	assert.Nil(t, ho.Match, "whole-memory fallback hits have no excerpt")
}
