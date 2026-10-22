package main

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
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
