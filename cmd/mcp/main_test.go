package main

import (
	"context"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A slow handler must fail FAST: withTimeout has to return promptly with a clear
// timeout error rather than letting the call block Claude. This is the whole point
// — a slow or unreachable Cortex server can't hang the session.
func TestWithTimeoutFailsFast(t *testing.T) {
	slow := func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, struct{}, error) {
		<-ctx.Done() // block until the bounded context is cancelled
		return nil, struct{}{}, ctx.Err()
	}
	start := time.Now()
	_, _, err := withTimeout(50*time.Millisecond, slow)(context.Background(), nil, struct{}{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out", "the caller must get a clear timeout error")
	assert.Less(t, time.Since(start), time.Second, "must return promptly, not hang")
}

// A handler that finishes within the deadline must pass through unchanged — the
// timeout is a safety net, not a tax on the normal fast path.
func TestWithTimeoutPassesThroughFastCalls(t *testing.T) {
	type out struct{ N int }
	fast := func(_ context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, out, error) {
		return nil, out{N: 7}, nil
	}
	_, o, err := withTimeout(2*time.Second, fast)(context.Background(), nil, struct{}{})
	require.NoError(t, err)
	assert.Equal(t, 7, o.N)
}
