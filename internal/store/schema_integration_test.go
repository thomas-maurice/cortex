package store

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// TestVerifySchema proves the boot-time schema check: after EnsureSchema the
// schema is reported healthy, and a missing class is detected and named. This is
// what surfaces, loudly and on boot, a Weaviate that is missing the MemoryChunk
// class or was created before the tokenization fix.
func TestVerifySchema(t *testing.T) {
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

	t.Run("a freshly ensured schema is healthy", func(t *testing.T) {
		problems, err := st.VerifySchema(ctx)
		require.NoError(t, err)
		assert.Empty(t, problems, "EnsureSchema must produce a schema VerifySchema considers healthy")
	})

	t.Run("a missing class is detected and named", func(t *testing.T) {
		require.NoError(t, st.client.Schema().ClassDeleter().WithClassName(memory.ChunkClassName).Do(ctx))
		problems, err := st.VerifySchema(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, problems)
		joined := strings.Join(problems, " | ")
		assert.Contains(t, joined, memory.ChunkClassName)
		assert.Contains(t, strings.ToLower(joined), "missing")
	})
}
