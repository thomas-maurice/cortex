package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/gen/cortex/v1/cortexv1connect"
	"github.com/thomas-maurice/cortex/internal/recallstate"
)

// fakeClient serves one memory for both GetMemory and Search; every other RPC
// panics via the nil embedded interface, which is fine — the tests don't call them.
type fakeClient struct {
	cortexv1connect.MemoryServiceClient
	mem *cortexv1.Memory
}

func (f fakeClient) GetMemory(context.Context, *connect.Request[cortexv1.GetMemoryRequest]) (*connect.Response[cortexv1.GetMemoryResponse], error) {
	return connect.NewResponse(&cortexv1.GetMemoryResponse{Memory: f.mem}), nil
}

func (f fakeClient) Search(context.Context, *connect.Request[cortexv1.SearchRequest]) (*connect.Response[cortexv1.SearchResponse], error) {
	return connect.NewResponse(&cortexv1.SearchResponse{Hits: []*cortexv1.Hit{{Memory: f.mem}}}), nil
}

// Opening a memory by id puts its full text in the model's context, so a later
// search must stub it (in-process AND via the shared recallstate the hook reads);
// the get itself must never be stubbed, even when the id was already delivered.
func TestGetMarksDeliveredSoSearchStubs(t *testing.T) {
	long := strings.Repeat("x", 500)
	mem := &cortexv1.Memory{Id: "m1", Text: long, Namespace: "ns", LinkedIds: []string{"m2"}}
	d := &deps{
		client:             fakeClient{mem: mem},
		seen:               map[string]bool{},
		statePath:          filepath.Join(t.TempDir(), "cortex.db"),
		conversationID:     "sess-1",
		realSession:        true,
		defaultSearchLimit: 5,
	}

	_, hit, err := d.get(context.Background(), nil, GetIn{ID: " m1 "})
	require.NoError(t, err)
	assert.Equal(t, long, hit.Text, "get returns full text")
	assert.False(t, hit.Stub)
	assert.Equal(t, []string{"m2"}, hit.LinkedIDs)
	assert.True(t, recallstate.Load(d.statePath, "sess-1").Seen("m1"), "shared state must record the delivery for the hook")

	_, out, err := d.search(context.Background(), nil, SearchIn{Query: "q"})
	require.NoError(t, err)
	require.Len(t, out.Hits, 1)
	assert.True(t, out.Hits[0].Stub, "search after get must stub the already-delivered memory")
	assert.Less(t, len(out.Hits[0].Text), len(long))

	_, hit, err = d.get(context.Background(), nil, GetIn{ID: "m1"})
	require.NoError(t, err)
	assert.Equal(t, long, hit.Text, "get is never stubbed")
	assert.False(t, hit.Stub)

	_, _, err = d.get(context.Background(), nil, GetIn{ID: "  "})
	assert.Error(t, err, "empty id rejected")
}

// The tool schema must mark id required so the model can't call it bare.
func TestGetToolRequiresID(t *testing.T) {
	d := &deps{client: fakeClient{mem: &cortexv1.Memory{Id: "m1"}}, seen: map[string]bool{}}
	srv := mcp.NewServer(&mcp.Implementation{Name: "t", Version: "0"}, nil)
	mcp.AddTool(srv, &mcp.Tool{Name: "cortex_memory_get", Description: "d"}, d.get)

	ct, st := mcp.NewInMemoryTransports()
	ctx := context.Background()
	ss, err := srv.Connect(ctx, st, nil)
	require.NoError(t, err)
	defer func() { _ = ss.Close() }()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "c", Version: "0"}, nil).Connect(ctx, ct, nil)
	require.NoError(t, err)
	defer func() { _ = cs.Close() }()

	tools, err := cs.ListTools(ctx, nil)
	require.NoError(t, err)
	require.Len(t, tools.Tools, 1)
	b, err := json.Marshal(tools.Tools[0].InputSchema)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"required":["id"]`)
}

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

type tagsClient struct {
	cortexv1connect.MemoryServiceClient
	gotNS string
}

func (f *tagsClient) ListTags(_ context.Context, r *connect.Request[cortexv1.ListTagsRequest]) (*connect.Response[cortexv1.ListTagsResponse], error) {
	f.gotNS = r.Msg.GetNamespace()
	return connect.NewResponse(&cortexv1.ListTagsResponse{Tags: []*cortexv1.TagInfo{
		{Name: "k8s", MemoryCount: 5}, {Name: "kubernetes", MemoryCount: 2},
	}}), nil
}

// cortex_tags_list is registered, read-only, and returns tag + count so the
// model reuses existing spellings; destructive tag tools must NOT exist.
func TestTagsListTool(t *testing.T) {
	fc := &tagsClient{}
	d := &deps{client: fc, seen: map[string]bool{}}
	srv := mcp.NewServer(&mcp.Implementation{Name: "t", Version: "0"}, nil)
	addTagsTool(srv, d, time.Second)

	ct, st := mcp.NewInMemoryTransports()
	ctx := context.Background()
	ss, err := srv.Connect(ctx, st, nil)
	require.NoError(t, err)
	defer func() { _ = ss.Close() }()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "c", Version: "0"}, nil).Connect(ctx, ct, nil)
	require.NoError(t, err)
	defer func() { _ = cs.Close() }()

	tools, err := cs.ListTools(ctx, nil)
	require.NoError(t, err)
	require.Len(t, tools.Tools, 1)
	assert.Equal(t, "cortex_tags_list", tools.Tools[0].Name)

	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "cortex_tags_list", Arguments: map[string]any{"namespace": " proj "}})
	require.NoError(t, err)
	require.False(t, res.IsError)
	assert.Equal(t, "proj", fc.gotNS)
	b, err := json.Marshal(res.StructuredContent)
	require.NoError(t, err)
	assert.JSONEq(t, `{"tags":[{"tag":"k8s","count":5},{"tag":"kubernetes","count":2}]}`, string(b))
}
