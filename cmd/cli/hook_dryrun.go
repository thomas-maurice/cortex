package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/recall"
	"github.com/thomas-maurice/cortex/internal/recallstate"
)

// dryRunMaxLimit mirrors the server's PreviewRecall limit cap.
const dryRunMaxLimit = 50

// dryRunDefaultTimeout applies when --timeout is not set explicitly: a preview
// does ~2x the hook's searches, so the hook's 5s is too tight.
const dryRunDefaultTimeout = 30 * time.Second

// dryRunMaxQueryChunks mirrors the server's max_query_chunks cap.
const dryRunMaxQueryChunks = 32

type recallDryRunOpts struct {
	prompt    string
	sessionID string
	showBlock bool
	stateDB   string
	namespace string
	timeout   time.Duration
	params    recall.Params
}

// runRecallDryRun previews what `cortex hook recall` would inject for a prompt.
// Unlike the real hook it is NOT fail-open (a human is watching, so errors are
// returned) and it never touches the local recall-state db: the server does the
// work via PreviewRecall, which is tenant-scoped and never reinforces.
func runRecallDryRun(cmd *cobra.Command, o recallDryRunOpts) error {
	prompt, session := o.prompt, o.sessionID
	if prompt == "" {
		raw, err := io.ReadAll(cmd.InOrStdin())
		if err != nil {
			return fmt.Errorf("read stdin: %w", err)
		}
		var evSession string
		prompt, evSession = parseDryRunInput(string(raw))
		if session == "" {
			session = evSession
		}
	}
	if strings.TrimSpace(prompt) == "" {
		return fmt.Errorf("empty prompt: pass --prompt or pipe it on stdin")
	}

	// Hook semantics that the RPC bounds differently: a negative cutoff means
	// "no cutoff" (RPC: 0), 0 limit means unlimited (RPC max), and chunk caps
	// above the server's are clamped.
	errw := cmd.ErrOrStderr()
	if o.params.MaxDistance < 0 {
		o.params.MaxDistance = 0
	}
	if o.params.Limit <= 0 {
		// The hook treats 0 as unlimited; the RPC bounds it.
		fpf(errw, "note: --limit %d means unlimited in the hook; previewing with the maximum, %d\n", o.params.Limit, dryRunMaxLimit)
		o.params.Limit = dryRunMaxLimit
	}
	hookChunkCap := o.params.MaxQueryChunks
	if o.params.MaxQueryChunks > dryRunMaxQueryChunks {
		o.params.MaxQueryChunks = dryRunMaxQueryChunks
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
	defer cancel()
	limit, minChars, maxChunks, maxChars := int32(o.params.Limit), int32(o.params.MinChars), int32(o.params.MaxQueryChunks), int32(o.params.MaxChars)
	resp, err := client().PreviewRecall(ctx, connect.NewRequest(&cortexv1.PreviewRecallRequest{
		Prompt:         prompt,
		Namespace:      o.namespace,
		Limit:          &limit,
		MaxDistance:    &o.params.MaxDistance,
		MinChars:       &minChars,
		MaxQueryChunks: &maxChunks,
		MaxChars:       &maxChars,
	}))
	if err != nil {
		return err
	}
	if (hookChunkCap == 0 || hookChunkCap > dryRunMaxQueryChunks) && int(resp.Msg.GetTotalChunks()) > dryRunMaxQueryChunks {
		fpf(errw, "warning: the prompt has %d chunks; the preview samples %d (server cap) but the hook with --max-query-chunks %d would search %s, so results may differ\n",
			resp.Msg.GetTotalChunks(), dryRunMaxQueryChunks, hookChunkCap, map[bool]string{true: "every chunk", false: "more"}[hookChunkCap == 0])
	}
	// Read-only: this never writes (or creates) the recall-state db.
	var seen *recallstate.State
	if session != "" {
		seen = recallstate.LoadReadOnly(o.stateDB, session)
	}
	printRecallPreview(cmd.OutOrStdout(), resp.Msg, o.namespace, seen, o.showBlock)
	return nil
}

// parseDryRunInput accepts either a Claude Code hook event (JSON with a prompt
// field, so a captured hook payload can be replayed as-is; its session_id is
// returned too) or raw prompt text.
func parseDryRunInput(in string) (prompt, sessionID string) {
	if t := strings.TrimSpace(in); strings.HasPrefix(t, "{") {
		if ev, err := parseHookEvent(strings.NewReader(t)); err == nil && ev.Prompt != "" {
			return ev.Prompt, ev.SessionID
		}
	}
	return in, ""
}

func snippet(text string, n int) string {
	t := strings.Join(strings.Fields(text), " ")
	if len(t) > n {
		t = t[:n] + "…"
	}
	return t
}

// fpf/fpl discard write errors: the destination is a terminal, and there is
// nothing useful to do about a failed write to it.
func fpf(w io.Writer, format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }
func fpl(w io.Writer, a ...any)                { _, _ = fmt.Fprintln(w, a...) }

func printRecallPreview(w io.Writer, r *cortexv1.PreviewRecallResponse, namespace string, seen *recallstate.State, showBlock bool) {
	fpl(w, "Recall preview (dry run: nothing injected, no recall state written, no reinforcement)")
	fpf(w, "Settings: namespace=%s limit=%d max-distance=%.2f min-chars=%d max-query-chunks=%d max-chars=%d\n",
		namespace, r.GetLimit(), r.GetMaxDistance(), r.GetMinChars(), r.GetMaxQueryChunks(), r.GetMaxChars())
	if r.GetSkipped() {
		why := map[string]string{
			recall.SkipDirective: "slash/bang commands are never searched",
			recall.SkipTooShort:  "prompt is shorter than --min-chars",
		}[r.GetSkipReason()]
		fpf(w, "\nThe hook would SKIP this prompt (%s): %s.\n", r.GetSkipReason(), why)
		fpl(w, "\nNote:", r.GetNote())
		return
	}
	fpf(w, "Query chunks: %d searched (prompt split into %d)\n", len(r.GetQueries()), r.GetTotalChunks())

	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fpf(w, "\nWould inject (%d):\n", len(r.GetInjected()))
	if len(r.GetInjected()) == 0 {
		fpl(w, "  (nothing: the hook would stay silent)")
	} else {
		fpl(tw, "  DIST\tCHUNK\tNAMESPACE\tID\tINJECTED CHARS\tTEXT")
		for _, h := range r.GetInjected() {
			chars := fmt.Sprintf("%d", h.GetTextChars())
			if h.GetTruncated() {
				chars = fmt.Sprintf("%d of %d (TRUNCATED)", r.GetMaxChars(), h.GetTextChars())
			}
			text := h.GetMemory().GetText()
			if h.GetExcerptFirst() {
				chars += " (EXCERPT FIRST)"
				text = h.GetMatch().GetText()
			}
			fpf(tw, "  %.3f\t%d\t%s\t%s\t%s\t%s\n", h.GetDistance(), h.GetQueryIndex(), h.GetMemory().GetNamespace(), h.GetMemory().GetId(), chars, snippet(text, 70))
		}
		_ = tw.Flush()
		if seen != nil {
			for _, h := range r.GetInjected() {
				if seen.Seen(h.GetMemory().GetId()) {
					fpf(w, "  %s: already injected this session (dedup) — the hook would skip it\n", h.GetMemory().GetId())
				}
			}
		}
		if showBlock {
			fpf(w, "\nInjected block (exact hook output; before session dedup):\n%s", r.GetInjectedBlock())
		}
	}

	fpf(w, "\nNear misses (%d):\n", len(r.GetNearMisses()))
	if len(r.GetNearMisses()) > 0 {
		fpl(tw, "  DIST\tCHUNK\tEXCLUDED BECAUSE\tID\tTEXT")
		for _, h := range r.GetNearMisses() {
			fpf(tw, "  %.3f\t%d\t%s\t%s\t%s\n", h.GetDistance(), h.GetQueryIndex(), h.GetReason(), h.GetMemory().GetId(), snippet(h.GetMemory().GetText(), 70))
		}
		_ = tw.Flush()
	}
	fpl(w, "\nNote:", r.GetNote())
}
