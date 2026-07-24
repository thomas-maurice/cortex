package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
)

// The hook must never fire on harness directives (slash/bang commands) or short
// continuation prompts: injecting recall there is pure noise, and noise trains
// the model to ignore the recall block entirely.
func TestShouldRecall(t *testing.T) {
	assert.False(t, shouldRecall("/clear", 12))
	assert.False(t, shouldRecall("  /code-review the diff please", 12))
	assert.False(t, shouldRecall("! ls -la", 12))
	assert.False(t, shouldRecall("fix it pls", 12))
	assert.False(t, shouldRecall("   ", 12))
	assert.True(t, shouldRecall("why does the palette show no results in prod?", 12))
}

func TestParseHookEvent(t *testing.T) {
	ev, err := parseHookEvent(strings.NewReader(`{"session_id":"abc-123","prompt":"hello there world","cwd":"/x"}`))
	require.NoError(t, err)
	assert.Equal(t, "abc-123", ev.SessionID)
	assert.Equal(t, "hello there world", ev.Prompt)

	_, err = parseHookEvent(strings.NewReader("not json"))
	assert.Error(t, err)
}

// Sampling must keep the first and last chunks: the actual ask in a long prompt
// (pasted logs, diffs) almost always lives at one of the two ends.
func TestSelectChunks(t *testing.T) {
	chunks := []string{"a", "b", "c", "d", "e", "f", "g", "h"}

	assert.Equal(t, chunks, selectChunks(chunks, 8))
	assert.Equal(t, chunks, selectChunks(chunks, 0), "0 = no cap")

	picked := selectChunks(chunks, 3)
	require.Len(t, picked, 3)
	assert.Equal(t, "a", picked[0])
	assert.Equal(t, "h", picked[2])

	assert.Equal(t, []string{"a"}, selectChunks(chunks, 1))
}

func hit(id string, dist float32) *cortexv1.Hit {
	return &cortexv1.Hit{Memory: &cortexv1.Memory{Id: id, Namespace: "ns", Text: "text " + id}, Distance: dist}
}

// Per-chunk searches overlap heavily; the merged ranking must keep one entry
// per memory at its best distance, or a memory matching two chunks would crowd
// out genuinely distinct results within the injection limit.
func TestMergeHits(t *testing.T) {
	merged := mergeHits([][]*cortexv1.Hit{
		{hit("m1", 0.4), hit("m2", 0.2)},
		{hit("m1", 0.1), hit("m2", 0.3)},
	}, 2)
	require.Len(t, merged, 2)
	assert.Equal(t, "m1", merged[0].GetMemory().GetId())
	assert.InDelta(t, 0.1, merged[0].GetDistance(), 1e-6, "best distance per id wins")
	assert.Equal(t, "m2", merged[1].GetMemory().GetId())

	assert.Empty(t, mergeHits(nil, 3))
}

// A prompt full of pasted logs produces chunks whose hits can all outrank the
// ask's own matches. The ask lives in the first/last chunk, so their best hits
// must survive the limit — otherwise the hook recalls what the logs resemble
// and drops what was actually asked.
func TestMergeHitsGuaranteesEndChunks(t *testing.T) {
	merged := mergeHits([][]*cortexv1.Hit{
		{hit("ask-first", 0.45)},                           // first chunk: the ask
		{hit("noise1", 0.05), hit("noise2", 0.06)},         // pasted-material chunks
		{hit("noise3", 0.07), hit("noise4", 0.08)},         //
		{hit("ask-last", 0.40), hit("ask-runnerup", 0.44)}, // last chunk: the ask
	}, 3)
	require.Len(t, merged, 3)
	ids := []string{}
	for _, h := range merged {
		ids = append(ids, h.GetMemory().GetId())
	}
	assert.Contains(t, ids, "ask-last", "best hit of the last chunk is guaranteed")
	assert.Contains(t, ids, "ask-first", "best hit of the first chunk is guaranteed")
	assert.Contains(t, ids, "noise1", "remaining slot filled by global best distance")
	assert.Equal(t, "noise1", ids[0], "output stays sorted by distance")
}

// The injected block must carry the memory id (so the model can edit/link it)
// and honor the truncation cap (memories can be pages long; the hook must not
// flood the context).
func TestFormatRecall(t *testing.T) {
	assert.Empty(t, formatRecall(nil, 100))

	long := hit("mem-1", 0.25)
	long.Memory.Text = strings.Repeat("x", 200)
	out := formatRecall([]*cortexv1.Hit{long}, 50)
	assert.Contains(t, out, "id=mem-1")
	assert.Contains(t, out, "[ns]")
	assert.Contains(t, out, "[truncated]")
	assert.NotContains(t, out, strings.Repeat("x", 51))
	assert.Contains(t, out, "<cortex-recall>")
	assert.Contains(t, out, "</cortex-recall>")
}

// Session-level dedup itself lives in internal/recallstate (tested there); the
// hook's use of it is exercised end-to-end against a dev stack.
