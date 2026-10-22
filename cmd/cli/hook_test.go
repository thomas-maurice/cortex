package main

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/recall"
)

func TestParseHookEvent(t *testing.T) {
	ev, err := parseHookEvent(strings.NewReader(`{"session_id":"abc-123","prompt":"hello there world","cwd":"/x"}`))
	require.NoError(t, err)
	assert.Equal(t, "abc-123", ev.SessionID)
	assert.Equal(t, "hello there world", ev.Prompt)

	_, err = parseHookEvent(strings.NewReader("not json"))
	assert.Error(t, err)
}

func hit(id string, dist float32) *cortexv1.Hit {
	return &cortexv1.Hit{Memory: &cortexv1.Memory{Id: id, Namespace: "ns", Text: "text " + id}, Distance: dist}
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

// corpusSearcher serves a fixed corpus with the server's limit/cutoff contract.
type corpusSearcher struct{ hits []*cortexv1.Hit }

func (c corpusSearcher) Search(_ context.Context, _ string, limit int, maxDistance float32) ([]*cortexv1.Hit, error) {
	var out []*cortexv1.Hit
	for _, h := range c.hits {
		if maxDistance > 0 && h.GetDistance() > maxDistance {
			continue
		}
		if len(out) < limit {
			out = append(out, h)
		}
	}
	return out, nil
}

// The refactor that moved the query logic into internal/recall must not change
// what the hook injects: this pins the exact stdout block (golden) for a fixed
// corpus, plus the skip rules and per-session dedup that wrap the shared logic.
func TestRunRecallOutputIsStable(t *testing.T) {
	s := corpusSearcher{hits: []*cortexv1.Hit{hit("m1", 0.12), hit("m2", 0.31), hit("m3", 0.44), hit("far", 0.8)}}
	p := recall.Params{Limit: 2, MaxDistance: 0.5, MinChars: 12, MaxQueryChunks: 4}
	state := filepath.Join(t.TempDir(), "state.db")
	ev := hookEvent{SessionID: "sess-1", Prompt: "how do I configure the recall hook?"}

	var out bytes.Buffer
	runRecall(context.Background(), &out, s, ev, p, 1500, state)
	const golden = "<cortex-recall>\n" +
		"Memories auto-recalled from Cortex for this prompt (semantic matches — possibly stale or irrelevant; verify before relying on them). Use cortex_memory_search for more.\n" +
		"\n### [ns] dist=0.12 id=m1\ntext m1\n" +
		"\n### [ns] dist=0.31 id=m2\ntext m2\n" +
		"</cortex-recall>\n"
	assert.Equal(t, golden, out.String())

	// Same session, same prompt: everything was already delivered, so silence.
	out.Reset()
	runRecall(context.Background(), &out, s, ev, p, 1500, state)
	assert.Empty(t, out.String(), "per-session dedup must suppress re-injection")

	// A different session is unaffected by the first one's state.
	out.Reset()
	runRecall(context.Background(), &out, s, hookEvent{SessionID: "sess-2", Prompt: ev.Prompt}, p, 1500, state)
	assert.Equal(t, golden, out.String())

	for _, prompt := range []string{"/clear", "ok thanks"} {
		out.Reset()
		runRecall(context.Background(), &out, s, hookEvent{SessionID: "sess-3", Prompt: prompt}, p, 1500, state)
		assert.Empty(t, out.String(), "prompt %q must be skipped", prompt)
	}
}

// --max-chars bounds how much of each memory reaches the context; the cap must
// apply to the injected block, not just exist as a flag.
func TestRunRecallTruncatesToMaxChars(t *testing.T) {
	h := hit("m1", 0.1)
	h.Memory.Text = strings.Repeat("y", 400)
	var out bytes.Buffer
	runRecall(context.Background(), &out, corpusSearcher{hits: []*cortexv1.Hit{h}},
		hookEvent{SessionID: "s", Prompt: "a prompt long enough to search"},
		recall.Params{Limit: 3, MaxDistance: 0.5, MinChars: 12, MaxQueryChunks: 4}, 100, "")
	assert.Contains(t, out.String(), strings.Repeat("y", 100)+"… [truncated]")
	assert.NotContains(t, out.String(), strings.Repeat("y", 101))
}

type failSearcher struct{ err error }

func (f failSearcher) Search(context.Context, string, int, float32) ([]*cortexv1.Hit, error) {
	return nil, f.err
}

type blockSearcher struct{}

func (blockSearcher) Search(ctx context.Context, _ string, _ int, _ float32) ([]*cortexv1.Hit, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// Fail-open is the hook's contract: a broken or slow server must never put
// anything on stdout (it would be injected as context) or block the prompt.
func TestRunRecallFailsOpen(t *testing.T) {
	p := recall.Params{Limit: 3, MaxDistance: 0.5, MinChars: 12, MaxQueryChunks: 4}
	ev := hookEvent{SessionID: "s", Prompt: "a prompt long enough to search"}

	var out bytes.Buffer
	runRecall(context.Background(), &out, failSearcher{errors.New("boom")}, ev, p, 1500, "")
	assert.Empty(t, out.String(), "search error")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	out.Reset()
	runRecall(ctx, &out, blockSearcher{}, ev, p, 1500, "")
	assert.Empty(t, out.String(), "context timeout")
}

// End to end through the cobra command: an unreachable server must still exit
// nil with empty stdout.
func TestHookRecallCommandFailsOpenWhenServerDown(t *testing.T) {
	old := serverURL
	serverURL = "http://127.0.0.1:1"
	t.Cleanup(func() { serverURL = old })

	cmd := hookRecallCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetIn(strings.NewReader(`{"session_id":"s","prompt":"a prompt long enough to search"}`))
	cmd.SetArgs([]string{"--timeout", "2s", "--state-db", ""})
	require.NoError(t, cmd.Execute())
	assert.Empty(t, out.String())
}

func TestPromptFromInput(t *testing.T) {
	assert.Equal(t, "hello from the hook", promptFromInput(`{"session_id":"x","prompt":"hello from the hook"}`))
	assert.Equal(t, "raw text prompt", promptFromInput("raw text prompt"))
	assert.Equal(t, `{"not":"an event"}`, promptFromInput(`{"not":"an event"}`), "JSON without a prompt is treated as raw text")
}

func TestPrintRecallPreview(t *testing.T) {
	var b bytes.Buffer
	printRecallPreview(&b, &cortexv1.PreviewRecallResponse{
		Queries: []string{"q"}, TotalChunks: 1, Limit: 1, MaxDistance: 0.5, MinChars: 12, MaxQueryChunks: 4,
		Injected:   []*cortexv1.RecallPreviewHit{{Memory: &cortexv1.Memory{Id: "m1", Namespace: "ns", Text: "hello"}, Distance: 0.1}},
		NearMisses: []*cortexv1.RecallPreviewHit{{Memory: &cortexv1.Memory{Id: "m2", Text: "x"}, Distance: 0.7, Reason: recall.ReasonOverMaxDistance}},
		Note:       "dedup not simulated",
	}, "*", 1500)
	out := b.String()
	assert.Contains(t, out, "Would inject (1)")
	assert.Contains(t, out, "m1")
	assert.Contains(t, out, "over_max_distance")
	assert.Contains(t, out, "dedup not simulated")

	b.Reset()
	printRecallPreview(&b, &cortexv1.PreviewRecallResponse{Skipped: true, SkipReason: recall.SkipTooShort, Note: "n"}, "*", 1500)
	assert.Contains(t, b.String(), "SKIP")
	assert.NotContains(t, b.String(), "Would inject")
}
