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
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/chunk"
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

// shouldRecall reports whether a prompt is worth searching for. Slash commands
// are harness directives, and very short prompts ("fix it", "yes") are
// continuations that embed poorly and need no recall.
func shouldRecall(prompt string, minChars int) bool {
	p := strings.TrimSpace(prompt)
	if strings.HasPrefix(p, "/") || strings.HasPrefix(p, "!") {
		return false
	}
	return len(p) >= minChars
}

// selectChunks bounds how many query chunks a long prompt costs: the first and
// last chunks are always kept (the actual ask usually lives at one end of a
// prompt full of pasted material), with the middle sampled evenly.
func selectChunks(chunks []string, max int) []string {
	if max <= 0 || len(chunks) <= max {
		return chunks
	}
	if max == 1 {
		return chunks[:1]
	}
	picked := make([]string, 0, max)
	seen := map[int]bool{}
	for i := range max {
		idx := int(math.Round(float64(i) * float64(len(chunks)-1) / float64(max-1)))
		if !seen[idx] {
			seen[idx] = true
			picked = append(picked, chunks[idx])
		}
	}
	return picked
}

// mergeHits folds per-chunk result lists into one ranking: best (lowest)
// distance wins per memory, sorted ascending, capped at limit. The first and
// last chunks hold the prompt's actual ask far more often than sampled middles,
// so each of their best hits is guaranteed a slot — otherwise a prompt full of
// pasted logs recalls only what the logs resemble and drops what was asked.
func mergeHits(lists [][]*cortexv1.Hit, limit int) []*cortexv1.Hit {
	best := map[string]*cortexv1.Hit{}
	for _, hits := range lists {
		for _, h := range hits {
			id := h.GetMemory().GetId()
			if cur, ok := best[id]; !ok || h.GetDistance() < cur.GetDistance() {
				best[id] = h
			}
		}
	}
	merged := make([]*cortexv1.Hit, 0, len(best))
	for _, h := range best {
		merged = append(merged, h)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].GetDistance() < merged[j].GetDistance() })
	if limit <= 0 || len(merged) <= limit {
		return merged
	}

	sel := map[string]bool{}
	if len(lists) > 1 {
		// Last chunk first: in chat the ask most often trails the pasted material.
		for _, l := range [][]*cortexv1.Hit{lists[len(lists)-1], lists[0]} {
			var top *cortexv1.Hit
			for _, h := range l {
				if top == nil || h.GetDistance() < top.GetDistance() {
					top = h
				}
			}
			if top != nil && len(sel) < limit {
				sel[top.GetMemory().GetId()] = true
			}
		}
	}
	for _, h := range merged {
		if len(sel) >= limit {
			break
		}
		sel[h.GetMemory().GetId()] = true
	}
	out := merged[:0]
	for _, h := range merged {
		if sel[h.GetMemory().GetId()] {
			out = append(out, h)
		}
	}
	return out
}

// formatRecall renders hits as the context block injected into the prompt. Each
// memory carries its id so the model can edit/link/delete it, and the preamble
// warns that matches are semantic and possibly stale.
func formatRecall(hits []*cortexv1.Hit, maxChars int) string {
	if len(hits) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("<cortex-recall>\n")
	b.WriteString("Memories auto-recalled from Cortex for this prompt (semantic matches — possibly stale or irrelevant; verify before relying on them). Use cortex_memory_search for more.\n")
	for _, h := range hits {
		m := h.GetMemory()
		text := strings.TrimSpace(m.GetText())
		if maxChars > 0 && len(text) > maxChars {
			text = text[:maxChars] + "… [truncated]"
		}
		fmt.Fprintf(&b, "\n### [%s] dist=%.2f id=%s\n%s\n", m.GetNamespace(), h.GetDistance(), m.GetId(), text)
	}
	b.WriteString("</cortex-recall>\n")
	return b.String()
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
			ev, err := parseHookEvent(cmd.InOrStdin())
			if err != nil {
				fmt.Fprintln(os.Stderr, "cortex hook recall:", err)
				return nil
			}
			if !shouldRecall(ev.Prompt, minChars) {
				return nil
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
			defer cancel()

			// Long prompts (pasted logs, diffs) embed as one diluted blob, so
			// they are split into worker-sized chunks and each chunk searched
			// separately; queries beyond --max-query-chunks are sampled, not all
			// run. A short prompt yields a single chunk = a single search.
			queries := []string{strings.TrimSpace(ev.Prompt)}
			if ck, err := chunk.New(chunk.DefaultMaxTokens, 0); err == nil {
				queries = selectChunks(ck.Split(queries[0]), maxQueryChunks)
			}

			c := client()
			lists := make([][]*cortexv1.Hit, len(queries))
			var wg sync.WaitGroup
			for i, q := range queries {
				wg.Add(1)
				go func(i int, q string) {
					defer wg.Done()
					resp, err := c.Search(ctx, connect.NewRequest(&cortexv1.SearchRequest{
						Query:       q,
						Namespace:   namespace,
						Limit:       int32(limit),
						MaxDistance: maxDistance,
						// Auto-injection is not the agent choosing to recall, so it
						// does not feed the living-memory signal unless --reinforce
						// opts in.
						NoReinforce: !reinforce,
					}))
					if err != nil {
						fmt.Fprintln(os.Stderr, "cortex hook recall:", err)
						return
					}
					lists[i] = resp.Msg.GetHits()
				}(i, q)
			}
			wg.Wait()

			seen := recallstate.Load(stateDB, ev.SessionID)
			var fresh []*cortexv1.Hit
			var freshIDs []string
			for _, h := range mergeHits(lists, limit) {
				if id := h.GetMemory().GetId(); !seen.Seen(id) {
					fresh = append(fresh, h)
					freshIDs = append(freshIDs, id)
				}
			}
			if len(fresh) == 0 {
				return nil
			}
			_, _ = fmt.Fprint(cmd.OutOrStdout(), formatRecall(fresh, maxChars))
			seen.Add(freshIDs...)
			return nil
		},
	}
	cmd.Flags().DurationVar(&timeout, "timeout", 5*time.Second, "hard deadline for the whole hook; on expiry it exits silently rather than delay the prompt")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "*", `namespace to search; default "*" recalls across all projects`)
	cmd.Flags().IntVarP(&limit, "limit", "l", 3, "max memories injected per prompt")
	cmd.Flags().Float32VarP(&maxDistance, "max-distance", "d", 0.5, "relevance cutoff; stricter than interactive search so silence is the common case")
	cmd.Flags().IntVar(&minChars, "min-chars", 12, "skip prompts shorter than this (short continuations need no recall)")
	cmd.Flags().IntVar(&maxChars, "max-chars", 1500, "truncate each injected memory to this many characters (0 = no cap)")
	cmd.Flags().BoolVar(&reinforce, "reinforce", false, "count injections as recalls for the living-memory signal")
	cmd.Flags().StringVar(&stateDB, "state-db", recallstate.DefaultPath(), "bbolt database for per-session dedup state, shared with the MCP server (empty disables dedup)")
	cmd.Flags().IntVar(&maxQueryChunks, "max-query-chunks", 4, "long prompts are split into ~512-token chunks searched concurrently; beyond this many, chunks are sampled (first + last + evenly spaced middles). 0 = search every chunk")
	return cmd
}
