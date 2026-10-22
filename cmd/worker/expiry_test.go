package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/store"
)

type optsRecorder struct {
	tenantStore
	got *store.SearchOpts
}

func (o optsRecorder) SearchMemoryVectors(ctx context.Context, vec []float32, opts store.SearchOpts) ([]memory.Hit, error) {
	*o.got = opts
	return o.tenantStore.SearchMemoryVectors(ctx, vec, opts)
}

// Search/recall hide expired memories by default, but dedup must NOT: an expired
// near-duplicate is still a duplicate, and flagging it lets review merge or drop
// the stale copy instead of leaving two competing versions of one fact.
func TestDedupSearchIncludesExpired(t *testing.T) {
	var opts store.SearchOpts
	ts := optsRecorder{tenantStore: newFakeStore(1).tenant(""), got: &opts}
	_ = findCandidates(context.Background(), testLog, ts, memory.Record{ID: "new", Namespace: "ns"}, []float32{1}, 0.2)
	require.Equal(t, "ns", opts.Namespace, "the dedup search must have run")
	assert.True(t, opts.IncludeExpired)
}
