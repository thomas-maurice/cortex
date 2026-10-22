package store

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// StaleOpts configures ListStale. Staleness is based on ACTUAL USE (useCount /
// lastUsedAt, recorded by RecordUse), not on search reinforcement.
type StaleOpts struct {
	Namespace string // exact namespace; "" = all namespaces
	// MinAge: a memory can only be stale once it has had this long to be used. The
	// clock starts at createdAt, or at TrackingSince for a never-used memory (usage
	// before tracking began was invisible, so it cannot count against it).
	MinAge time.Duration
	// TrackingSince is when usage tracking started on this deployment (zero =
	// unknown/ignored). A never-used memory's reference time is
	// max(createdAt, TrackingSince).
	TrackingSince time.Time
	// IdleFor == 0 selects "never used" (useCount == 0 and no lastUsedAt).
	// IdleFor > 0 selects "not used in IdleFor": last use older than IdleFor, or
	// never used AND its reference time older than IdleFor.
	IdleFor     time.Duration
	IncludeTags []string // memory must carry ALL of these
	AnyTags     []string // memory must carry AT LEAST ONE of these
	ExcludeTags []string // drop memories carrying ANY of these
	// IncludeExpired keeps memories whose validUntil has passed. Default (false)
	// skips them: the expiry sweep owns expired memories, so offering them for
	// bulk "stale-review" trashing would double-handle them.
	IncludeExpired bool
	Limit          int // page size (default 50)
	Offset         int
	Now            time.Time // zero = time.Now(); test seam
}

// staleRef is the instant a memory was last "useful": its last use, or for a
// never-used one max(createdAt, trackingSince).
func staleRef(r memory.Record, since time.Time) time.Time {
	if !r.LastUsedAt.IsZero() {
		return r.LastUsedAt
	}
	if since.After(r.CreatedAt) {
		return since
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
//   - useCount > 0 but no lastUsedAt: it HAS been used, so it is not "never
//     used"; for the idle-since mode its reference time falls back to
//     max(createdAt, trackingSince).
//   - missing createdAt in idle-since mode (IdleFor > 0): never stale, whatever
//     MinAge is (a zero time would look infinitely old, and the tracking epoch
//     must not stand in for an unknown age). In never-used mode with MinAge == 0
//     such a record IS listed: that mode needs no age.
func isStale(r memory.Record, o StaleOpts, now time.Time) bool {
	used := r.UseCount > 0 || !r.LastUsedAt.IsZero()
	if o.IdleFor > 0 && r.CreatedAt.IsZero() {
		return false
	}
	if o.MinAge > 0 {
		if r.CreatedAt.IsZero() {
			return false
		}
		start := r.CreatedAt
		if !used && o.TrackingSince.After(start) {
			start = o.TrackingSince
		}
		if start.After(now.Add(-o.MinAge)) {
			return false
		}
	}
	if o.IdleFor <= 0 {
		return !used
	}
	ref := staleRef(r, o.TrackingSince)
	if ref.IsZero() {
		return false
	}
	return ref.Before(now.Add(-o.IdleFor))
}

// ListStale returns stale memories oldest-last-use first (ties by id, so
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
		if !opts.IncludeExpired && rec.Expired(now) {
			return
		}
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
		ri, rj := staleRef(matched[i], opts.TrackingSince), staleRef(matched[j], opts.TrackingSince)
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
