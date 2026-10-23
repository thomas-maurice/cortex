package main

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/gen/cortex/v1/cortexv1connect"
)

type recClient struct {
	cortexv1connect.MemoryServiceClient
	save   *cortexv1.SaveRequest
	search *cortexv1.SearchRequest
	hit    *cortexv1.Memory
}

func (r *recClient) Save(_ context.Context, req *connect.Request[cortexv1.SaveRequest]) (*connect.Response[cortexv1.SaveResponse], error) {
	r.save = req.Msg
	return connect.NewResponse(&cortexv1.SaveResponse{Id: "id1", Status: "queued"}), nil
}

func (r *recClient) Search(_ context.Context, req *connect.Request[cortexv1.SearchRequest]) (*connect.Response[cortexv1.SearchResponse], error) {
	r.search = req.Msg
	return connect.NewResponse(&cortexv1.SearchResponse{Hits: []*cortexv1.Hit{{Memory: r.hit}}}), nil
}

// The model sets expiresIn for time-bound facts; it must reach the server
// verbatim (the server is the one validator) and be absent when not set.
func TestSavePlumbsExpiresIn(t *testing.T) {
	c := &recClient{}
	d := &deps{client: c, seen: map[string]bool{}}
	_, _, err := d.save(context.Background(), nil, SaveIn{Text: "deploy freeze until Friday", ExpiresIn: " 3d "})
	require.NoError(t, err)
	assert.Equal(t, "3d", c.save.GetExpiresIn())
	assert.Nil(t, c.save.GetValidUntil())

	_, _, err = d.save(context.Background(), nil, SaveIn{Text: "durable"})
	require.NoError(t, err)
	assert.Empty(t, c.save.GetExpiresIn())
	assert.False(t, c.save.GetNeverExpires(), "never_expires is opt-in")

	_, _, err = d.save(context.Background(), nil, SaveIn{Text: "permanent merge", NeverExpires: true})
	require.NoError(t, err)
	assert.True(t, c.save.GetNeverExpires(), "an explicit never must reach the server or the merge inherits a source expiry")
}

// includeExpired must reach the server, and an expired hit must be labelled so the
// model cannot mistake it for a current fact.
func TestSearchPlumbsIncludeExpiredAndFlagsExpiredHits(t *testing.T) {
	c := &recClient{hit: &cortexv1.Memory{Id: "m1", Text: "old freeze", ValidUntil: timestamppb.New(time.Now().Add(-time.Hour))}}
	d := &deps{client: c, seen: map[string]bool{}, defaultSearchLimit: 5}

	_, out, err := d.search(context.Background(), nil, SearchIn{Query: "freeze"})
	require.NoError(t, err)
	assert.False(t, c.search.GetIncludeExpired(), "expired memories are hidden unless asked for")
	require.Len(t, out.Hits, 1)
	assert.True(t, out.Hits[0].Expired)
	assert.NotEmpty(t, out.Hits[0].ValidUntil)

	_, _, err = d.search(context.Background(), nil, SearchIn{Query: "freeze", IncludeExpired: true})
	require.NoError(t, err)
	assert.True(t, c.search.GetIncludeExpired())

	c.hit = &cortexv1.Memory{Id: "m2", Text: "no expiry"}
	_, out, err = d.search(context.Background(), nil, SearchIn{Query: "x"})
	require.NoError(t, err)
	assert.False(t, out.Hits[0].Expired)
	assert.Empty(t, out.Hits[0].ValidUntil)
}

type fullClient struct {
	cortexv1connect.MemoryServiceClient
	mem    *cortexv1.Memory
	update *cortexv1.UpdateMemoryRequest
	groups []*cortexv1.DuplicateGroup
	cons   []*cortexv1.Memory
}

func (f *fullClient) GetMemory(context.Context, *connect.Request[cortexv1.GetMemoryRequest]) (*connect.Response[cortexv1.GetMemoryResponse], error) {
	return connect.NewResponse(&cortexv1.GetMemoryResponse{Memory: f.mem}), nil
}

func (f *fullClient) UpdateMemory(_ context.Context, r *connect.Request[cortexv1.UpdateMemoryRequest]) (*connect.Response[cortexv1.UpdateMemoryResponse], error) {
	f.update = r.Msg
	return connect.NewResponse(&cortexv1.UpdateMemoryResponse{Id: r.Msg.Id, Status: "updated"}), nil
}

func (f *fullClient) ListDuplicateCandidates(context.Context, *connect.Request[cortexv1.ListDuplicateCandidatesRequest]) (*connect.Response[cortexv1.ListDuplicateCandidatesResponse], error) {
	return connect.NewResponse(&cortexv1.ListDuplicateCandidatesResponse{Groups: f.groups}), nil
}

func (f *fullClient) Consolidate(context.Context, *connect.Request[cortexv1.ConsolidateRequest]) (*connect.Response[cortexv1.ConsolidateResponse], error) {
	return connect.NewResponse(&cortexv1.ConsolidateResponse{Cluster: f.cons}), nil
}

// RecordUse is fire-and-forget from get/consolidate; accept it (a nil embedded
// client would panic in the background goroutine).
func (f *fullClient) RecordUse(context.Context, *connect.Request[cortexv1.RecordUseRequest]) (*connect.Response[cortexv1.RecordUseResponse], error) {
	return connect.NewResponse(&cortexv1.RecordUseResponse{}), nil
}

func resultText(t *testing.T, r *mcp.CallToolResult) string {
	t.Helper()
	require.NotNil(t, r)
	return r.Content[0].(*mcp.TextContent).Text
}

// Every tool that returns a memory must say when it expires: otherwise an agent
// that opens a time-bound memory by id (or sees it in a consolidation cluster)
// treats a stale fact as current.
func TestExpiryShownByGetReviewAndConsolidate(t *testing.T) {
	past := timestamppb.New(time.Now().Add(-time.Hour))
	future := timestamppb.New(time.Now().Add(48 * time.Hour))
	c := &fullClient{mem: &cortexv1.Memory{Id: "m1", Text: "freeze", ValidUntil: past}}
	d := &deps{client: c, seen: map[string]bool{}}

	res, hit, err := d.get(context.Background(), nil, GetIn{ID: "m1"})
	require.NoError(t, err)
	assert.True(t, hit.Expired)
	assert.Contains(t, resultText(t, res), "[EXPIRED")

	c.groups = []*cortexv1.DuplicateGroup{{
		Memory:     &cortexv1.Memory{Id: "a", Text: "x", ValidUntil: future},
		Candidates: []*cortexv1.Memory{{Id: "b", Text: "y", ValidUntil: past}},
	}}
	res, rev, err := d.reviewCandidates(context.Background(), nil, ReviewIn{})
	require.NoError(t, err)
	assert.NotEmpty(t, rev.Groups[0].Memory.ValidUntil)
	assert.True(t, rev.Groups[0].Candidates[0].Expired)
	assert.Contains(t, resultText(t, res), "(expires")
	assert.Contains(t, resultText(t, res), "[EXPIRED")

	c.cons = []*cortexv1.Memory{{Id: "a", Text: "x", ValidUntil: future}, {Id: "b", Text: "y", ValidUntil: timestamppb.New(time.Now().Add(72 * time.Hour))}}
	res, cons, err := d.consolidate(context.Background(), nil, ConsolidateIn{Topic: "t"})
	require.NoError(t, err)
	assert.Equal(t, "3d", cons.SuggestedExpiresIn, "all sources expire: inherit the latest (72h -> 3d)")
	assert.Contains(t, resultText(t, res), `expiresIn="3d"`)

	c.cons = append(c.cons, &cortexv1.Memory{Id: "perm", Text: "forever"})
	res, cons, err = d.consolidate(context.Background(), nil, ConsolidateIn{Topic: "t"})
	require.NoError(t, err)
	assert.Empty(t, cons.SuggestedExpiresIn, "a permanent source: leave the decision to the agent")
	assert.Contains(t, resultText(t, res), "some are permanent")
}

// The agent can extend or drop an expiry with cortex_memory_edit, text-less.
func TestEditPlumbsExpiry(t *testing.T) {
	c := &fullClient{}
	d := &deps{client: c, seen: map[string]bool{}}

	_, out, err := d.edit(context.Background(), nil, EditIn{ID: "m1", ExpiresIn: " 7d "})
	require.NoError(t, err)
	assert.Equal(t, "updated", out.Status)
	assert.Equal(t, "7d", c.update.GetExpiresIn())
	assert.Empty(t, c.update.GetText())

	_, _, err = d.edit(context.Background(), nil, EditIn{ID: "m1", ClearValidUntil: true})
	require.NoError(t, err)
	assert.True(t, c.update.GetClearValidUntil())

	_, _, err = d.edit(context.Background(), nil, EditIn{ID: "m1"})
	assert.Error(t, err, "no text and no expiry change is still an error")
}

func (r *recClient) RecordUse(context.Context, *connect.Request[cortexv1.RecordUseRequest]) (*connect.Response[cortexv1.RecordUseResponse], error) {
	return connect.NewResponse(&cortexv1.RecordUseResponse{}), nil
}
