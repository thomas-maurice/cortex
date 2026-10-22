// Claude Code hook integration. `cortex hook recall` is meant to be wired as a
// UserPromptSubmit hook: it reads the hook event JSON on stdin, runs a semantic
// search for the prompt against the Cortex server, and prints matching memories
// on stdout — which Claude Code injects into the model's context. Recall stops
// depending on the model deciding to search: every prompt is searched.
//
// Contract: the hook NEVER blocks or fails the prompt. Any error (bad stdin,
// unreachable server, timeout) is logged to stderr and exits 0 with no output,
// and the whole run is bounded by a hard --timeout (default 5s).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/gen/cortex/v1/cortexv1connect"
	"github.com/thomas-maurice/cortex/internal/recall"
	"github.com/thomas-maurice/cortex/internal/recallstate"
)

// hookEvent is the subset of the Claude Code hook input we use. UserPromptSubmit
// delivers at least session_id and prompt.
type hookEvent struct {
	SessionID string `json:"session_id"`
	Prompt    string `json:"prompt"`
}

// parseHookEvent decodes the hook JSON from r.
func parseHookEvent(r io.Reader) (hookEvent, error) {
	var ev hookEvent
	if err := json.NewDecoder(r).Decode(&ev); err != nil {
		return hookEvent{}, fmt.Errorf("decode hook input: %w", err)
	}
	return ev, nil
}

func hookCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "hook",
		Short: "Claude Code hook endpoints (wired from settings.json, not run by hand)",
		Args:  cobra.NoArgs,
	}
	cmd.AddCommand(hookRecallCmd())
	return cmd
}

func hookRecallCmd() *cobra.Command {
	var (
		timeout        time.Duration
		namespace      string
		limit          int
		maxDistance    float32
		minChars       int
		maxChars       int
		reinforce      bool
		stateDB        string
		maxQueryChunks int
		dryRun         bool
		dryPrompt      string
		drySession     string
		dryBlock       bool
	)
	cmd := &cobra.Command{
		Use:   "recall",
		Short: "UserPromptSubmit hook: search Cortex for the prompt and print matches as context",
		Long: "Reads a Claude Code UserPromptSubmit event on stdin, semantically searches the\n" +
			"memory store for the prompt, and prints relevant memories on stdout, which\n" +
			"Claude Code injects into the model's context. Fail-open by design: any error or\n" +
			"the --timeout elapsing produces no output and exit 0, so a slow or unreachable\n" +
			"Cortex server never delays or blocks the prompt.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if dryRun {
				if !cmd.Flags().Changed("timeout") {
					timeout = dryRunDefaultTimeout
				}
				return runRecallDryRun(cmd, recallDryRunOpts{
					prompt: dryPrompt, sessionID: drySession, showBlock: dryBlock, stateDB: stateDB, namespace: namespace, timeout: timeout,
					params: recall.Params{Limit: limit, MaxDistance: maxDistance, MinChars: minChars, MaxQueryChunks: maxQueryChunks, MaxChars: maxChars},
				})
			}
			ev, err := parseHookEvent(cmd.InOrStdin())
			if err != nil {
				fmt.Fprintln(os.Stderr, "cortex hook recall:", err)
				return nil
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
			defer cancel()
			s := &rpcSearcher{c: client(), namespace: namespace, reinforce: reinforce}
			runRecall(ctx, cmd.OutOrStdout(), s, ev,
				recall.Params{Limit: limit, MaxDistance: maxDistance, MinChars: minChars, MaxQueryChunks: maxQueryChunks},
				maxChars, stateDB)
			return nil
		},
	}
	cmd.Flags().DurationVar(&timeout, "timeout", 5*time.Second, "hard deadline for the whole hook; on expiry it exits silently rather than delay the prompt")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "*", `namespace to search; default "*" recalls across all projects`)
	cmd.Flags().IntVarP(&limit, "limit", "l", recall.DefaultLimit, "max memories injected per prompt")
	cmd.Flags().Float32VarP(&maxDistance, "max-distance", "d", recall.DefaultMaxDistance, "relevance cutoff; stricter than interactive search so silence is the common case")
	cmd.Flags().IntVar(&minChars, "min-chars", recall.DefaultMinChars, "skip prompts shorter than this (short continuations need no recall)")
	cmd.Flags().IntVar(&maxChars, "max-chars", recall.DefaultMaxChars, "truncate each injected memory to this many characters (0 = no cap)")
	cmd.Flags().BoolVar(&reinforce, "reinforce", false, "count injections as recalls for the living-memory signal")
	cmd.Flags().StringVar(&stateDB, "state-db", recallstate.DefaultPath(), "bbolt database for per-session dedup state, shared with the MCP server (empty disables dedup)")
	cmd.Flags().IntVar(&maxQueryChunks, "max-query-chunks", recall.DefaultMaxQueryChunks, "long prompts are split into ~512-token chunks searched concurrently; beyond this many, chunks are sampled (first + last + evenly spaced middles). 0 = search every chunk")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "preview what the hook WOULD inject (server-side PreviewRecall) and print a table (--timeout defaults to 30s here; --limit 0 previews with 50); the prompt comes from --prompt or stdin (raw text or hook-event JSON); writes no recall state")
	cmd.Flags().StringVar(&drySession, "session-id", "", "with --dry-run: session whose local recall state marks already-injected memories (default: session_id of hook-event JSON on stdin); read-only")
	cmd.Flags().BoolVar(&dryBlock, "show-block", false, "with --dry-run: also print the exact <cortex-recall> block the hook would inject")
	cmd.Flags().StringVar(&dryPrompt, "prompt", "", "with --dry-run: the prompt to preview (default: read stdin)")
	return cmd
}

// rpcSearcher is the hook's recall.Searcher: one Search RPC per query chunk.
type rpcSearcher struct {
	c         cortexv1connect.MemoryServiceClient
	namespace string
	reinforce bool
}

func (r *rpcSearcher) Search(ctx context.Context, query string, limit int, maxDistance float32) ([]*cortexv1.Hit, error) {
	resp, err := r.c.Search(ctx, connect.NewRequest(&cortexv1.SearchRequest{
		Query:       query,
		Namespace:   r.namespace,
		Limit:       int32(limit),
		MaxDistance: maxDistance,
		// Auto-injection is not the agent choosing to recall, so it does not feed
		// the living-memory signal unless --reinforce opts in.
		NoReinforce: !r.reinforce,
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetHits(), nil
}

// runRecall is the hook proper: skip rules, query (shared with the server's
// PreviewRecall via internal/recall), then per-session dedup and output. It is
// fail-open: search errors are logged to stderr and never returned.
func runRecall(ctx context.Context, out io.Writer, s recall.Searcher, ev hookEvent, p recall.Params, maxChars int, stateDB string) {
	if !recall.ShouldRecall(ev.Prompt, p.MinChars) {
		return
	}
	res := recall.Run(ctx, s, ev.Prompt, p)
	for _, err := range res.Errs {
		fmt.Fprintln(os.Stderr, "cortex hook recall:", err)
	}

	seen := recallstate.Load(stateDB, ev.SessionID)
	var fresh []*cortexv1.Hit
	var freshIDs []string
	for _, m := range res.Injected {
		if id := m.Hit.GetMemory().GetId(); !seen.Seen(id) {
			fresh = append(fresh, m.Hit)
			freshIDs = append(freshIDs, id)
		}
	}
	if len(fresh) == 0 {
		return
	}
	_, _ = fmt.Fprint(out, recall.FormatBlock(fresh, maxChars))
	seen.Add(freshIDs...)
}
