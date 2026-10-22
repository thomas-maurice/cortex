package recall

import (
	"context"
	"errors"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
)

func hit(id string, dist float32) *cortexv1.Hit {
	return &cortexv1.Hit{Memory: &cortexv1.Memory{Id: id, Namespace: "ns", Text: "text " + id}, Distance: dist}
}

func ids(hs []*cortexv1.Hit) []string {
	out := []string{}
	for _, h := range hs {
		out = append(out, h.GetMemory().GetId())
	}
	return out
}

func matchIDs(ms []Match) []string {
	out := []string{}
	for _, m := range ms {
		out = append(out, m.Hit.GetMemory().GetId())
	}
	return out
}

// fakeSearcher behaves like the server's Search: a fixed corpus per query,
// ordered by distance, cut at maxDistance (when >0) and truncated at limit. That
// fidelity is the point: the preview's near-miss logic only makes sense if the
// searcher honours the same limit/cutoff contract the hook relies on.
type fakeSearcher struct {
	mu     sync.Mutex
	byTerm map[string][]*cortexv1.Hit // query substring -> hits
	err    error
	calls  []call
}

type call struct {
	query    string
	limit    int
	maxDist  float32
	wideCall bool
}

func (f *fakeSearcher) Search(_ context.Context, q string, limit int, maxDist float32) ([]*cortexv1.Hit, error) {
	f.mu.Lock()
	f.calls = append(f.calls, call{q, limit, maxDist, maxDist == 0})
	f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	var all []*cortexv1.Hit
	for term, hs := range f.byTerm {
		if strings.Contains(q, term) {
			all = append(all, hs...)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].GetDistance() < all[j].GetDistance() })
	var out []*cortexv1.Hit
	for _, h := range all {
		if maxDist > 0 && h.GetDistance() > maxDist {
			continue
		}
		out = append(out, h)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func TestSkipReason(t *testing.T) {
	cases := []struct {
		prompt string
		min    int
		want   string
	}{
		{"/clear", 12, SkipDirective},
		{"  /code-review the diff please", 12, SkipDirective},
		{"! ls -la", 12, SkipDirective},
		{"fix it pls", 12, SkipTooShort},
		{"   ", 12, SkipTooShort},
		{"fix it pls", 0, ""},
		{"why does the palette show no results in prod?", 12, ""},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, SkipReason(c.prompt, c.min), "prompt %q min %d", c.prompt, c.min)
		assert.Equal(t, c.want == "", ShouldRecall(c.prompt, c.min))
	}
}

// Sampling must keep the first and last chunks: the actual ask in a long prompt
// (pasted logs, diffs) almost always lives at one of the two ends.
func TestSelectChunks(t *testing.T) {
	chunks := []string{"a", "b", "c", "d", "e", "f", "g", "h"}

	assert.Equal(t, chunks, SelectChunks(chunks, 8))
	assert.Equal(t, chunks, SelectChunks(chunks, 0), "0 = no cap")

	picked := SelectChunks(chunks, 3)
	require.Len(t, picked, 3)
	assert.Equal(t, "a", picked[0])
	assert.Equal(t, "h", picked[2])

	assert.Equal(t, []string{"a"}, SelectChunks(chunks, 1))
}

func TestQueries(t *testing.T) {
	q, total := Queries("  a short prompt  ", 4)
	assert.Equal(t, []string{"a short prompt"}, q, "a short prompt is one trimmed query")
	assert.Equal(t, 1, total)

	long := strings.Repeat("The quick brown fox jumps over the lazy dog near the riverbank. ", 600)
	all, total := Queries(long, 0)
	require.Greater(t, total, 4, "fixture must split into many chunks")
	assert.Len(t, all, total, "0 = every chunk")
	sampled, total2 := Queries(long, 3)
	assert.Equal(t, total, total2, "total is the pre-sampling count")
	assert.Len(t, sampled, 3)
	assert.Equal(t, all[0], sampled[0])
	assert.Equal(t, all[len(all)-1], sampled[2])
}

// Per-chunk searches overlap heavily; the merged ranking must keep one entry
// per memory at its best distance, or a memory matching two chunks would crowd
// out genuinely distinct results within the injection limit.
func TestMergeBestDistanceAcrossChunks(t *testing.T) {
	merged := Merge([][]*cortexv1.Hit{
		{hit("m1", 0.4), hit("m2", 0.2)},
		{hit("m1", 0.1), hit("m2", 0.3)},
	}, 2)
	require.Len(t, merged, 2)
	assert.Equal(t, "m1", merged[0].Hit.GetMemory().GetId())
	assert.InDelta(t, 0.1, merged[0].Hit.GetDistance(), 1e-6, "best distance per id wins")
	assert.Equal(t, 1, merged[0].Query, "m1's best distance came from chunk 1")
	assert.Equal(t, "m2", merged[1].Hit.GetMemory().GetId())
	assert.Equal(t, 0, merged[1].Query, "m2's best distance came from chunk 0")

	assert.Empty(t, Merge(nil, 3))
}

// A prompt full of pasted logs produces chunks whose hits can all outrank the
// ask's own matches. The ask lives in the first/last chunk, so their best hits
// must survive the limit — otherwise the hook recalls what the logs resemble
// and drops what was actually asked.
func TestMergeGuaranteesEndChunks(t *testing.T) {
	got := MergeHits([][]*cortexv1.Hit{
		{hit("ask-first", 0.45)},
		{hit("noise1", 0.05), hit("noise2", 0.06)},
		{hit("noise3", 0.07), hit("noise4", 0.08)},
		{hit("ask-last", 0.40), hit("ask-runnerup", 0.44)},
	}, 3)
	require.Len(t, got, 3)
	assert.Equal(t, []string{"noise1", "ask-last", "ask-first"}, ids(got), "end-chunk bests kept, remaining slot by global distance, sorted by distance")
}

func TestMergeLimitZeroKeepsAll(t *testing.T) {
	got := MergeHits([][]*cortexv1.Hit{{hit("a", 0.3), hit("b", 0.1)}}, 0)
	assert.Equal(t, []string{"b", "a"}, ids(got))
}

func TestRunSkipsNothingItIsGiven(t *testing.T) {
	f := &fakeSearcher{byTerm: map[string][]*cortexv1.Hit{"redis": {hit("r1", 0.2), hit("r2", 0.3), hit("r3", 0.9)}}}
	res := Run(context.Background(), f, "how do we configure redis", Params{Limit: 2, MaxDistance: 0.5})
	assert.Equal(t, []string{"r1", "r2"}, matchIDs(res.Injected))
	require.Len(t, f.calls, 1)
	assert.Equal(t, 2, f.calls[0].limit, "hook passes its limit to each search")
	assert.InDelta(t, 0.5, f.calls[0].maxDist, 1e-6, "hook passes its cutoff to each search")
}

// Search failures are collected, not fatal: the hook stays fail-open for the
// chunks that did work, while the preview can choose to treat them as errors.
func TestRunCollectsErrors(t *testing.T) {
	f := &fakeSearcher{err: errors.New("boom")}
	res := Run(context.Background(), f, "a prompt long enough", Params{Limit: 3, MaxDistance: 0.5})
	assert.Empty(t, res.Injected)
	require.Len(t, res.Errs, 1)
}

// Preview classification: why each non-injected result missed. The injected set
// must equal Run's (the preview must show what the hook does), and every near
// miss must carry the right reason so users know which knob to turn.
func TestPreviewNearMisses(t *testing.T) {
	corpus := []*cortexv1.Hit{
		hit("a", 0.10), hit("b", 0.20), hit("c", 0.30), // within cutoff, limit is 2
		hit("d", 0.45),                                                                 // within cutoff but over the limit
		hit("e", 0.60), hit("f", 0.70), hit("g", 0.80), hit("h", 0.90), hit("i", 0.95), // beyond cutoff
	}
	f := &fakeSearcher{byTerm: map[string][]*cortexv1.Hit{"query": corpus}}
	p := Params{Limit: 2, MaxDistance: 0.5}

	res, misses := Preview(context.Background(), f, "some query text here", p)

	assert.Equal(t, matchIDs(Run(context.Background(), f, "some query text here", p).Injected), matchIDs(res.Injected),
		"preview's injected set must equal the hook's")
	assert.Equal(t, []string{"a", "b"}, matchIDs(res.Injected))

	require.Len(t, misses, NearMissCount)
	got := map[string]string{}
	var order []string
	for _, m := range misses {
		got[m.Match.Hit.GetMemory().GetId()] = m.Reason
		order = append(order, m.Match.Hit.GetMemory().GetId())
	}
	assert.Equal(t, []string{"c", "d", "e", "f", "g"}, order, "next-best results in distance order, injected ones excluded")
	assert.Equal(t, ReasonOverLimit, got["c"])
	assert.Equal(t, ReasonOverLimit, got["d"])
	assert.Equal(t, ReasonOverMaxDistance, got["e"])
	assert.Equal(t, ReasonOverMaxDistance, got["g"])

	var wide *call
	for i := range f.calls {
		if f.calls[i].wideCall {
			wide = &f.calls[i]
		}
	}
	require.NotNil(t, wide, "near misses need a no-cutoff search")
	assert.Equal(t, p.Limit+NearMissCount, wide.limit)
}

// With no cutoff (max distance 0) nothing can be "over max distance"; every
// exclusion is the limit.
func TestPreviewNoCutoffOnlyOverLimit(t *testing.T) {
	f := &fakeSearcher{byTerm: map[string][]*cortexv1.Hit{"query": {hit("a", 0.1), hit("b", 0.9), hit("c", 1.5)}}}
	_, misses := Preview(context.Background(), f, "some query text here", Params{Limit: 1})
	require.Len(t, misses, 2)
	for _, m := range misses {
		assert.Equal(t, ReasonOverLimit, m.Reason)
	}
}

func TestPreviewSearchErrorShortCircuits(t *testing.T) {
	f := &fakeSearcher{err: errors.New("boom")}
	res, misses := Preview(context.Background(), f, "some query text here", Params{Limit: 3, MaxDistance: 0.5})
	assert.Len(t, res.Errs, 1)
	assert.Empty(t, misses)
	assert.Len(t, f.calls, 1, "no wide search after the exact one failed")
}

type gaugeSearcher struct{ inflight, max atomic.Int32 }

func (g *gaugeSearcher) Search(context.Context, string, int, float32) ([]*cortexv1.Hit, error) {
	n := g.inflight.Add(1)
	for {
		m := g.max.Load()
		if n <= m || g.max.CompareAndSwap(m, n) {
			break
		}
	}
	for range 200 {
		runtime.Gosched()
	}
	g.inflight.Add(-1)
	return nil, nil
}

// Each preview chunk is an embed + search; unbounded fan-out from a long prompt
// would hammer Ollama/Weaviate.
func TestSearchConcurrencyBounded(t *testing.T) {
	long := strings.Repeat("The quick brown fox jumps over the lazy dog near the riverbank. ", 4000)
	qs, _ := Queries(long, 0)
	require.Greater(t, len(qs), MaxConcurrency, "fixture must exceed the bound")
	g := &gaugeSearcher{}
	Run(context.Background(), g, long, Params{Limit: 3, MaxDistance: 0.5})
	assert.LessOrEqual(t, int(g.max.Load()), MaxConcurrency)
	assert.Greater(t, int(g.max.Load()), 1, "still concurrent")
}
