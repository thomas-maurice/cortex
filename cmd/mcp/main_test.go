package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"sync"
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
	// match, when set, is attached to the search hit (a chunk-found hit).
	match *cortexv1.Match
	uses  *useLog
}

// useLog collects RecordUse calls (they are fire-and-forget goroutines).
type useLog struct {
	mu   sync.Mutex
	ids  []string
	call chan struct{}
}

func newUseLog() *useLog { return &useLog{call: make(chan struct{}, 16)} }

func (f fakeClient) RecordUse(_ context.Context, r *connect.Request[cortexv1.RecordUseRequest]) (*connect.Response[cortexv1.RecordUseResponse], error) {
	if f.uses != nil {
		f.uses.mu.Lock()
		f.uses.ids = append(f.uses.ids, r.Msg.GetSource()+":"+strings.Join(r.Msg.GetIds(), ","))
		f.uses.mu.Unlock()
		f.uses.call <- struct{}{}
	}
	return connect.NewResponse(&cortexv1.RecordUseResponse{}), nil
}

func (f fakeClient) Consolidate(context.Context, *connect.Request[cortexv1.ConsolidateRequest]) (*connect.Response[cortexv1.ConsolidateResponse], error) {
	return connect.NewResponse(&cortexv1.ConsolidateResponse{Cluster: []*cortexv1.Memory{f.mem}, Manifest: []string{f.mem.GetId()}}), nil
}

func (f fakeClient) RecallSession(context.Context, *connect.Request[cortexv1.RecallSessionRequest]) (*connect.Response[cortexv1.RecallSessionResponse], error) {
	return connect.NewResponse(&cortexv1.RecallSessionResponse{Matched: true, Summary: &cortexv1.ConversationSummary{Text: "s"}, Facts: []*cortexv1.Memory{f.mem}}), nil
}

// waitUse blocks for the next RecordUse call and returns everything logged so far.
func (l *useLog) waitUse(t *testing.T) []string {
	t.Helper()
	select {
	case <-l.call:
	case <-time.After(2 * time.Second):
		t.Fatal("expected a RecordUse call")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.ids...)
}

// noUse asserts no further RecordUse arrives shortly.
func (l *useLog) noUse(t *testing.T) {
	t.Helper()
	select {
	case <-l.call:
		t.Fatal("unexpected RecordUse call")
	case <-time.After(150 * time.Millisecond):
	}
}

// Only memories delivered with FULL text count as used: get, a first search, a
// fresh=true re-fetch, recall_session facts and consolidate clusters do; a stub
// (already in context) does not, or a repeated search would inflate "use".
func TestRecordUseOnlyForFullDeliveries(t *testing.T) {
	mem := &cortexv1.Memory{Id: "m1", Text: strings.Repeat("x", 500), Namespace: "ns"}
	log := newUseLog()
	d := &deps{client: fakeClient{mem: mem, uses: log}, seen: map[string]bool{}, defaultSearchLimit: 5}
	ctx := context.Background()

	_, _, err := d.search(ctx, nil, SearchIn{Query: "q"})
	require.NoError(t, err)
	assert.Equal(t, []string{"mcp-search:m1"}, log.waitUse(t))

	_, out, err := d.search(ctx, nil, SearchIn{Query: "q"})
	require.NoError(t, err)
	require.True(t, out.Hits[0].Stub)
	log.noUse(t)

	_, _, err = d.search(ctx, nil, SearchIn{Query: "q", Fresh: true})
	require.NoError(t, err)
	assert.Len(t, log.waitUse(t), 2, "fresh=true re-delivers full text")

	_, _, err = d.get(ctx, nil, GetIn{ID: "m1"})
	require.NoError(t, err)
	assert.Equal(t, "mcp-get:m1", log.waitUse(t)[2])

	_, _, err = d.consolidate(ctx, nil, ConsolidateIn{Topic: "t"})
	require.NoError(t, err)
	assert.Equal(t, "mcp-consolidate:m1", log.waitUse(t)[3])

	_, _, err = d.recall(ctx, nil, RecallIn{Query: "q"})
	require.NoError(t, err)
	assert.Equal(t, "mcp-recall-session:m1", log.waitUse(t)[4])
}

func (f fakeClient) GetMemory(context.Context, *connect.Request[cortexv1.GetMemoryRequest]) (*connect.Response[cortexv1.GetMemoryResponse], error) {
	return connect.NewResponse(&cortexv1.GetMemoryResponse{Memory: f.mem}), nil
}

func (f fakeClient) Search(context.Context, *connect.Request[cortexv1.SearchRequest]) (*connect.Response[cortexv1.SearchResponse], error) {
	return connect.NewResponse(&cortexv1.SearchResponse{Hits: []*cortexv1.Hit{{Memory: f.mem, Match: f.match}}}), nil
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

// For an already-delivered (stubbed) memory the generic 120-char prefix says
// nothing about the NEW query, so the stub must carry the matched excerpt
// instead. A first (non-stub) delivery returns the full text plus `matched`, and
// fresh=true bypasses the stub entirely.
func TestSearchMatchedExcerpt(t *testing.T) {
	long := strings.Repeat("filler sentence. ", 100) + "The needle fact lives here. " + strings.Repeat("more filler. ", 50)
	excerpt := "The needle fact lives here."
	mem := &cortexv1.Memory{Id: "m1", Text: long, Namespace: "ns"}
	d := &deps{
		client:             fakeClient{mem: mem, match: &cortexv1.Match{ChunkIndex: 2, Text: excerpt}},
		seen:               map[string]bool{},
		defaultSearchLimit: 5,
	}
	ctx := context.Background()

	_, out, err := d.search(ctx, nil, SearchIn{Query: "q"})
	require.NoError(t, err)
	require.Len(t, out.Hits, 1)
	assert.False(t, out.Hits[0].Stub)
	assert.Equal(t, long, out.Hits[0].Text, "first delivery keeps the full text (existing semantics)")
	assert.Equal(t, excerpt, out.Hits[0].Matched)

	_, out, err = d.search(ctx, nil, SearchIn{Query: "q"})
	require.NoError(t, err)
	assert.True(t, out.Hits[0].Stub)
	assert.Equal(t, excerpt, out.Hits[0].Text, "stub carries the matched excerpt, not the 120-char prefix")

	_, out, err = d.search(ctx, nil, SearchIn{Query: "q", Fresh: true})
	require.NoError(t, err)
	assert.False(t, out.Hits[0].Stub)
	assert.Equal(t, long, out.Hits[0].Text, "fresh=true returns the full text")

	// The stub excerpt is capped: an uncapped ~2k-char chunk would defeat the
	// stub's token saving.
	bigExcerpt := strings.Repeat("e", 2000)
	d.client = fakeClient{mem: &cortexv1.Memory{Id: "m1", Text: long + bigExcerpt}, match: &cortexv1.Match{Text: bigExcerpt}}
	_, out, err = d.search(ctx, nil, SearchIn{Query: "q"})
	require.NoError(t, err)
	assert.True(t, out.Hits[0].Stub)
	assert.LessOrEqual(t, len([]rune(out.Hits[0].Text)), matchedTextChars+1, "stub excerpt must be capped (+1 for the ellipsis)")

	// No excerpt (whole-memory hit): the stub falls back to the prefix.
	d.client = fakeClient{mem: mem}
	_, out, err = d.search(ctx, nil, SearchIn{Query: "q"})
	require.NoError(t, err)
	assert.True(t, out.Hits[0].Stub)
	assert.Equal(t, truncateRunes(long, stubTextChars), out.Hits[0].Text)
	assert.Empty(t, out.Hits[0].Matched)

	// An excerpt equal to the whole (short) memory is redundant: omitted.
	d = &deps{client: fakeClient{mem: &cortexv1.Memory{Id: "s", Text: "short"}, match: &cortexv1.Match{Text: "short"}}, seen: map[string]bool{}, defaultSearchLimit: 5}
	_, out, err = d.search(ctx, nil, SearchIn{Query: "q"})
	require.NoError(t, err)
	assert.Empty(t, out.Hits[0].Matched)
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

// A slow server must not make RecordUse goroutines pile up: when every slot is
// busy the record is dropped (it is a best-effort signal), never queued.
func TestRecordUseDropsWhenSaturated(t *testing.T) {
	for range recordUseSlots {
		recordUseSem <- struct{}{}
	}
	t.Cleanup(func() {
		for range recordUseSlots {
			<-recordUseSem
		}
	})
	log := newUseLog()
	d := &deps{client: fakeClient{mem: &cortexv1.Memory{Id: "m1"}, uses: log}}
	d.recordUse("mcp-get", []string{"m1"})
	log.noUse(t)
}
