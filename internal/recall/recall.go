// Package recall holds the query logic of the Claude Code UserPromptSubmit
// recall hook (`cortex hook recall`): skip rules, splitting long prompts into
// sampled query chunks, concurrent per-chunk search, merge by best distance and
// the injection limit. It is shared by the CLI hook and the server's
// PreviewRecall RPC so the preview is, by construction, what the hook does.
//
// It is transport-agnostic: callers supply a Searcher (an RPC client in the
// hook, the embedder+store in the server).
package recall

import (
	"context"
	"math"
	"sort"
	"strings"
	"sync"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/chunk"
)

// Hook defaults. The CLI flags and the PreviewRecall RPC both fall back to these.
const (
	DefaultLimit          = 3
	DefaultMaxDistance    = float32(0.5)
	DefaultMinChars       = 12
	DefaultMaxQueryChunks = 4

	// NearMissCount is how many results beyond the injection set a preview shows.
	NearMissCount = 5

	// MaxConcurrency bounds in-flight searches per Run/Preview phase.
	MaxConcurrency = 8
)

// Skip reasons reported by SkipReason.
const (
	SkipDirective = "directive"
	SkipTooShort  = "too_short"
)

// Near-miss exclusion reasons.
const (
	ReasonOverMaxDistance = "over_max_distance"
	ReasonOverLimit       = "over_limit"
)

// Searcher runs one semantic search. maxDistance <= 0 means no cutoff.
type Searcher interface {
	Search(ctx context.Context, query string, limit int, maxDistance float32) ([]*cortexv1.Hit, error)
}

// Params are the knobs shared by the hook flags and the preview RPC.
type Params struct {
	Limit          int
	MaxDistance    float32
	MinChars       int
	MaxQueryChunks int
}

// SkipReason reports why a prompt is not worth searching, or "" if it is. Slash
// commands are harness directives, and very short prompts ("fix it", "yes") are
// continuations that embed poorly and need no recall.
func SkipReason(prompt string, minChars int) string {
	p := strings.TrimSpace(prompt)
	if strings.HasPrefix(p, "/") || strings.HasPrefix(p, "!") {
		return SkipDirective
	}
	if len(p) < minChars {
		return SkipTooShort
	}
	return ""
}

// ShouldRecall reports whether a prompt is worth searching for.
func ShouldRecall(prompt string, minChars int) bool {
	return SkipReason(prompt, minChars) == ""
}

// SelectChunks bounds how many query chunks a long prompt costs: the first and
// last chunks are always kept (the actual ask usually lives at one end of a
// prompt full of pasted material), with the middle sampled evenly.
func SelectChunks(chunks []string, max int) []string {
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

// Queries splits a prompt into worker-sized chunks and samples them down to max.
// Long prompts (pasted logs, diffs) embed as one diluted blob, so each chunk is
// searched separately. A short prompt yields a single query. total is the chunk
// count before sampling.
func Queries(prompt string, max int) (queries []string, total int) {
	queries = []string{strings.TrimSpace(prompt)}
	total = 1
	if ck, err := chunk.New(chunk.DefaultMaxTokens, 0); err == nil {
		all := ck.Split(queries[0])
		total = len(all)
		queries = SelectChunks(all, max)
	}
	return queries, total
}

// Match is a merged hit plus the index of the query chunk that matched it best.
type Match struct {
	Hit   *cortexv1.Hit
	Query int
}

// rank folds per-chunk result lists into one ranking: best (lowest) distance
// wins per memory, sorted ascending.
func rank(lists [][]*cortexv1.Hit) []Match {
	best := map[string]Match{}
	for qi, hits := range lists {
		for _, h := range hits {
			id := h.GetMemory().GetId()
			if cur, ok := best[id]; !ok || h.GetDistance() < cur.Hit.GetDistance() {
				best[id] = Match{Hit: h, Query: qi}
			}
		}
	}
	merged := make([]Match, 0, len(best))
	for _, m := range best {
		merged = append(merged, m)
	}
	sort.Slice(merged, func(i, j int) bool {
		if a, b := merged[i].Hit.GetDistance(), merged[j].Hit.GetDistance(); a != b {
			return a < b
		}
		return merged[i].Hit.GetMemory().GetId() < merged[j].Hit.GetMemory().GetId()
	})
	return merged
}

// Merge folds per-chunk result lists into one ranking capped at limit. The
// first and last chunks hold the prompt's actual ask far more often than sampled
// middles, so each of their best hits is guaranteed a slot — otherwise a prompt
// full of pasted logs recalls only what the logs resemble and drops what was
// asked.
func Merge(lists [][]*cortexv1.Hit, limit int) []Match {
	merged := rank(lists)
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
	for _, m := range merged {
		if len(sel) >= limit {
			break
		}
		sel[m.Hit.GetMemory().GetId()] = true
	}
	out := merged[:0]
	for _, m := range merged {
		if sel[m.Hit.GetMemory().GetId()] {
			out = append(out, m)
		}
	}
	return out
}

// MergeHits is Merge without the matched-chunk bookkeeping.
func MergeHits(lists [][]*cortexv1.Hit, limit int) []*cortexv1.Hit {
	ms := Merge(lists, limit)
	out := make([]*cortexv1.Hit, len(ms))
	for i, m := range ms {
		out[i] = m.Hit
	}
	return out
}

// Result is the outcome of one Run.
type Result struct {
	Queries  []string
	Total    int // chunks before sampling
	Lists    [][]*cortexv1.Hit
	Injected []Match
	// Errs are per-query search failures. A failed query contributes no hits;
	// the hook logs and carries on (fail-open), the preview treats them as fatal.
	Errs []error
}

func searchAll(ctx context.Context, s Searcher, queries []string, limit int, maxDistance float32) ([][]*cortexv1.Hit, []error) {
	lists := make([][]*cortexv1.Hit, len(queries))
	errs := make([]error, len(queries))
	var wg sync.WaitGroup
	sem := make(chan struct{}, MaxConcurrency)
	for i, q := range queries {
		wg.Add(1)
		go func(i int, q string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			hits, err := s.Search(ctx, q, limit, maxDistance)
			if err != nil {
				errs[i] = err
				return
			}
			lists[i] = hits
		}(i, q)
	}
	wg.Wait()
	var nonNil []error
	for _, e := range errs {
		if e != nil {
			nonNil = append(nonNil, e)
		}
	}
	return lists, nonNil
}

// Run does what the hook does for one prompt that passed SkipReason: sample
// query chunks, search each concurrently with the hook's limit and cutoff, and
// merge. It does not apply session dedup (client-local state).
func Run(ctx context.Context, s Searcher, prompt string, p Params) Result {
	queries, total := Queries(prompt, p.MaxQueryChunks)
	lists, errs := searchAll(ctx, s, queries, p.Limit, p.MaxDistance)
	return Result{
		Queries:  queries,
		Total:    total,
		Lists:    lists,
		Injected: Merge(lists, p.Limit),
		Errs:     errs,
	}
}

// NearMiss is a result the hook would NOT inject, with the reason.
type NearMiss struct {
	Match  Match
	Reason string
}

// Preview is Run plus the next few results the hook did not inject. The
// injected set comes from the exact hook search, so it is faithful; near misses
// come from a second, wider search (limit+NearMissCount per chunk, no cutoff)
// whose distances can differ slightly from the first (hybrid score fusion is
// normalised over the fetched pool).
func Preview(ctx context.Context, s Searcher, prompt string, p Params) (Result, []NearMiss) {
	res := Run(ctx, s, prompt, p)
	if len(res.Errs) > 0 {
		return res, nil
	}
	wide, errs := searchAll(ctx, s, res.Queries, p.Limit+NearMissCount, 0)
	if len(errs) > 0 {
		res.Errs = errs
		return res, nil
	}
	injected := map[string]bool{}
	for _, m := range res.Injected {
		injected[m.Hit.GetMemory().GetId()] = true
	}
	var misses []NearMiss
	for _, m := range rank(wide) {
		if injected[m.Hit.GetMemory().GetId()] {
			continue
		}
		reason := ReasonOverLimit
		if p.MaxDistance > 0 && m.Hit.GetDistance() > p.MaxDistance {
			reason = ReasonOverMaxDistance
		}
		misses = append(misses, NearMiss{Match: m, Reason: reason})
		if len(misses) >= NearMissCount {
			break
		}
	}
	return res, misses
}
