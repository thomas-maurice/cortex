package store

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// StaleOpts configures ListStale.
type StaleOpts struct {
	Namespace string // exact namespace; "" = all namespaces
	// MinAge: only memories created at least this long ago can be stale, so a
	// brand-new memory (which simply has not had time to be recalled) is never flagged.
	MinAge time.Duration
	// IdleFor == 0 selects "never recalled" (accessCount == 0 and no
	// lastAccessedAt). IdleFor > 0 selects "not recalled in IdleFor": last access
	// older than IdleFor, or never recalled AND created longer ago than IdleFor.
	IdleFor     time.Duration
	IncludeTags []string // memory must carry ALL of these
	AnyTags     []string // memory must carry AT LEAST ONE of these
	ExcludeTags []string // drop memories carrying ANY of these
	Limit       int      // page size (default 50)
	Offset      int
	Now         time.Time // zero = time.Now(); test seam
}

// staleRef is the instant a memory was last "useful": its last recall, or its
// creation when it has never been recalled.
func staleRef(r memory.Record) time.Time {
	if !r.LastAccessedAt.IsZero() {
		return r.LastAccessedAt
	}
	return r.CreatedAt
}

func hasAll(have, want []string) bool {
	set := make(map[string]bool, len(have))
	for _, t := range have {
		set[t] = true
	}
	for _, t := range want {
		if !set[t] {
			return false
		}
	}
	return true
}

func hasAny(have, want []string) bool {
	for _, h := range have {
		for _, w := range want {
			if h == w {
				return true
			}
		}
	}
	return false
}

// isStale is the classification used by ListStale (pure, unit-testable).
//
// Deliberate edge cases (when in doubt, do NOT flag: stale feeds deletion):
//   - missing createdAt with MinAge > 0: age is unknown, so the min-age guard
//     cannot be satisfied -> not stale.
//   - accessCount > 0 but no lastAccessedAt: it HAS been recalled, so it is not
//     "never recalled"; for the idle-since mode its reference time falls back to
//     createdAt.
//   - no lastAccessedAt AND missing createdAt in idle-since mode: no reference
//     time at all (a zero time would otherwise look infinitely old) -> not stale.
//     In never-recalled mode with MinAge == 0 such a record IS listed: that mode
//     needs no age.
func isStale(r memory.Record, o StaleOpts, now time.Time) bool {
	if o.MinAge > 0 && (r.CreatedAt.IsZero() || r.CreatedAt.After(now.Add(-o.MinAge))) {
		return false
	}
	if o.IdleFor <= 0 {
		return r.AccessCount == 0 && r.LastAccessedAt.IsZero()
	}
	ref := staleRef(r)
	if ref.IsZero() {
		return false
	}
	return ref.Before(now.Add(-o.IdleFor))
}

// ListStale returns stale memories oldest-last-access first (ties by id, so
// pages are stable) plus the total match count before pagination.
//
// It cursor-scans the whole class and classifies in Go (like ListWithCandidates):
// the gRPC filter API cannot express "lastAccessedAt missing" without an
// IsNull filter, which needs indexNullState on the class (absent on existing
// deployments), and the id cursor cannot be combined with where/sort. The scan
// is correct beyond the 10000-result query cap; only the (small) matching set
// is held and sorted.
func (ts *TenantStore) ListStale(ctx context.Context, opts StaleOpts) ([]memory.Record, int, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 50
	}
	now := opts.Now
	if now.IsZero() {
		now = time.Now()
	}
	var matched []memory.Record
	err := ts.s.scanAll(ctx, memory.ClassName, ts.t, memoryProps, func(r graphql.SearchResult) {
		if opts.Namespace != "" && propString(r.Properties, "namespace") != opts.Namespace {
			return
		}
		rec := resultToRecord(r)
		if !isStale(rec, opts, now) {
			return
		}
		if !hasAll(rec.Tags, opts.IncludeTags) {
			return
		}
		if len(opts.AnyTags) > 0 && !hasAny(rec.Tags, opts.AnyTags) {
			return
		}
		if len(opts.ExcludeTags) > 0 && hasAny(rec.Tags, opts.ExcludeTags) {
			return
		}
		matched = append(matched, rec)
	})
	if err != nil {
		if isTenantNotFound(err) {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("list stale query: %w", err)
	}
	sort.SliceStable(matched, func(i, j int) bool {
		ri, rj := staleRef(matched[i]), staleRef(matched[j])
		if !ri.Equal(rj) {
			return ri.Before(rj)
		}
		return matched[i].ID < matched[j].ID
	})
	total := len(matched)
	if opts.Offset < 0 || opts.Offset >= total {
		return []memory.Record{}, total, nil
	}
	end := min(opts.Offset+limit, total)
	return matched[opts.Offset:end], total, nil
}
