// Package merge carries everything worth keeping from memories that are being
// folded into another one (a Save with supersedes, or the MergeInto RPC) onto the
// surviving memory, BEFORE the originals are trashed: their links, access
// statistics, "not a duplicate" dismissals and provenance. A merge must never
// silently drop the knowledge graph or the user's earlier decisions.
//
// It is shared by the worker (cmd/worker applySupersedes) and the server
// (MergeInto) so both paths behave identically.
package merge

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// Store is the slice of the tenant-scoped store Carry needs.
type Store interface {
	Get(ctx context.Context, id string) (memory.Record, bool, error)
	Reinforce(ctx context.Context, id string, count int, at time.Time) error
	RecordUse(ctx context.Context, id string, count int, at time.Time) error
	SetNotDuplicateOf(ctx context.Context, id string, ids []string) error
}

// LinkPublisher queues one tenant-stamped link mutation onto the link queue.
type LinkPublisher func(op memory.LinkOp, a, b string) error

// Provenance returns the createdAt and conversationId the surviving memory
// should carry: createdAt is the oldest non-zero of keep and the sources (it
// records when the knowledge was first captured); conversationId is keep's if set,
// else that of the oldest source that has one (which is the shared value when the
// sources agree).
func Provenance(keep memory.Record, sources []memory.Record) (time.Time, string) {
	created := keep.CreatedAt
	conv := keep.ConversationID
	ordered := slices.Clone(sources)
	slices.SortStableFunc(ordered, func(a, b memory.Record) int { return a.CreatedAt.Compare(b.CreatedAt) })
	for _, s := range ordered {
		if !s.CreatedAt.IsZero() && (created.IsZero() || s.CreatedAt.Before(created)) {
			created = s.CreatedAt
		}
		if conv == "" {
			conv = s.ConversationID
		}
	}
	return created, conv
}

// InheritedExpiry returns the expiry a merged memory should adopt, or the zero
// time for "change nothing". A merged memory made only of time-bound facts must
// not outlive them, so when keep has no expiry (keepUntil zero) and EVERY one of
// the wanted sources has one, it is the LATEST source expiry. If any source is
// permanent the merge stays permanent, and if fewer than wanted sources are
// present (one already trashed by an earlier, interrupted attempt, or a bogus id)
// the answer is unknown, so nothing is inherited. A keep that already has an
// expiry is never changed.
func InheritedExpiry(keepUntil time.Time, sources []memory.Record, wanted int) time.Time {
	if !keepUntil.IsZero() || wanted == 0 || len(sources) != wanted {
		return time.Time{}
	}
	var latest time.Time
	for _, s := range sources {
		if s.ValidUntil.IsZero() {
			return time.Time{}
		}
		if s.ValidUntil.After(latest) {
			latest = s.ValidUntil
		}
	}
	return latest
}

// Bounds on one merge. A lock set is held for the whole carry and a worker slot is
// pinned while it is acquired, so neither the source list nor the (sources +
// dismissal counterparts) lock set may be attacker-sized.
const (
	MaxSources = 50
	MaxLockSet = 200
)

// ErrTooLarge means a merge exceeds MaxSources / MaxLockSet; it is permanent (retrying
// cannot help), so callers fail loudly instead of locking everything.
var ErrTooLarge = errors.New("merge too large")

// ErrLockSetChanged means a source's dismissal counterparts changed between
// computing the lock set (LockIDs) and carrying: the caller must release, recompute
// the lock set and retry, since a counterpart's notDuplicateOf is only safe to
// rewrite while its per-memory lock is held.
var ErrLockSetChanged = errors.New("merge: counterpart set changed since locking, retry")

// LockIDs returns every memory id a merge of sourceIDs into keepID read-modify-
// writes: keep, the sources, and the counterparts of the sources' dismissals. The
// caller takes all their per-memory locks (sorted, one AcquireAll) before Carry.
// It reads unlocked; Carry re-checks the counterparts against the locked set.
func LockIDs(ctx context.Context, st Store, keepID string, sourceIDs []string) ([]string, error) {
	if len(sourceIDs) > MaxSources {
		return nil, fmt.Errorf("%w: %d sources (max %d)", ErrTooLarge, len(sourceIDs), MaxSources)
	}
	ids := append([]string{keepID}, sourceIDs...)
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		seen[id] = true
	}
	for _, id := range sourceIDs {
		src, found, err := st.Get(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("read superseded %s: %w", id, err)
		}
		if !found {
			continue
		}
		for _, c := range src.NotDuplicateOf {
			if c == "" || seen[c] {
				continue
			}
			seen[c] = true
			ids = append(ids, c)
			if len(ids) > MaxLockSet {
				return nil, fmt.Errorf("%w: lock set exceeds %d memories", ErrTooLarge, MaxLockSet)
			}
		}
	}
	return ids, nil
}

// Carry moves ONE source's links, dismissals and access stats onto keepID
// (siblings is the whole set being merged, so links/dismissals between two sources
// are dropped rather than carried). A source that no longer exists is skipped, so
// re-running after it was trashed is a no-op. It does NOT delete anything. Callers
// loop Carry -> delete per source, so a partial failure leaves earlier sources
// fully done and a retry only re-processes what is still live.
//
// locked, when non-nil, is the set of ids whose per-memory locks the caller holds
// (from LockIDs); a counterpart outside it yields ErrLockSetChanged.
//
// Links go through the link queue (idempotent set ops; ordering, retries and
// locks are the link consumer's). Everything is idempotent EXCEPT accessCount
// (summed), which is therefore written LAST: a failure or lease expiry earlier
// (e.g. a long carry under the lock's deadline) leaves nothing double-counted on
// retry. If it fails between that write and the caller's delete, the retry
// re-adds this one source's count (heuristic over-count, accepted; the search-path
// Reinforce is likewise unlocked, so counts are best-effort by design).
//
// Cost: one Get per distinct neighbour/counterpart, all inside the caller's lock
// lease. The caller must pass the lease-bounded context: an overlong carry then
// fails with a deadline error instead of half-applying silently.
func Carry(ctx context.Context, st Store, keepID, sourceID string, siblings []string, pub LinkPublisher, locked []string) error {
	inSet := make(map[string]bool, len(siblings)+1)
	for _, id := range siblings {
		inSet[id] = true
	}
	inSet[sourceID] = true
	keep, ok, err := st.Get(ctx, keepID)
	if err != nil {
		return fmt.Errorf("read merged %s: %w", keepID, err)
	}
	if !ok {
		return fmt.Errorf("merged memory %s not found", keepID)
	}
	src, found, err := st.Get(ctx, sourceID)
	if err != nil {
		return fmt.Errorf("read superseded %s: %w", sourceID, err)
	}
	if !found {
		return nil
	}
	if locked != nil {
		for _, c := range src.NotDuplicateOf {
			if c != "" && c != keepID && !inSet[c] && !slices.Contains(locked, c) {
				return ErrLockSetChanged
			}
		}
	}
	type got struct {
		rec memory.Record
		ok  bool
	}
	cache := map[string]got{} // one Get per distinct neighbour/counterpart
	exists := func(id string) (memory.Record, bool, error) {
		if g, hit := cache[id]; hit {
			return g.rec, g.ok, nil
		}
		r, ok, err := st.Get(ctx, id)
		if err == nil {
			cache[id] = got{r, ok}
		}
		return r, ok, err
	}

	// Links. ADD keep<->N for each live neighbour that is neither keep nor another
	// source; REMOVE source<->N for every neighbour (trashing does not touch the
	// neighbours' linkedIds, so they would keep a dangling id).
	for _, n := range src.LinkedIDs {
		if n == "" || n == src.ID || inSet[n] {
			continue
		}
		if n != keepID {
			// Only live neighbours: an ADD to a missing endpoint would burn the
			// link consumer's whole retry budget.
			if _, err := uuid.Parse(n); err != nil {
				continue
			}
			if _, ok, err := exists(n); err != nil {
				return fmt.Errorf("read neighbour %s: %w", n, err)
			} else if !ok {
				continue
			}
			if err := pub(memory.LinkOpAdd, keepID, n); err != nil {
				return fmt.Errorf("carry link %s<->%s: %w", keepID, n, err)
			}
		}
		if err := pub(memory.LinkOpRemove, src.ID, n); err != nil {
			return fmt.Errorf("unlink %s<->%s: %w", src.ID, n, err)
		}
	}

	// Dismissals: keep inherits every "not a duplicate of C" the source had, and
	// each counterpart C is rewritten source -> keep, so a reviewed pair does not
	// come back as a candidate. Sources themselves and keep are never listed.
	keepNot := slices.DeleteFunc(slices.Clone(keep.NotDuplicateOf), func(id string) bool { return inSet[id] })
	for _, c := range src.NotDuplicateOf {
		if c == "" || c == keepID || inSet[c] {
			continue
		}
		cr, found, err := exists(c)
		if err != nil {
			return fmt.Errorf("read dismissed counterpart %s: %w", c, err)
		}
		if !found {
			continue
		}
		if !slices.Contains(keepNot, c) {
			keepNot = append(keepNot, c)
		}
		cNot := slices.DeleteFunc(slices.Clone(cr.NotDuplicateOf), func(id string) bool { return inSet[id] })
		if !slices.Contains(cNot, keepID) {
			cNot = append(cNot, keepID)
		}
		if !slices.Equal(cNot, cr.NotDuplicateOf) {
			if err := st.SetNotDuplicateOf(ctx, c, cNot); err != nil {
				return fmt.Errorf("rewrite dismissal on %s: %w", c, err)
			}
		}
	}
	if !slices.Equal(keepNot, keep.NotDuplicateOf) {
		if err := st.SetNotDuplicateOf(ctx, keepID, keepNot); err != nil {
			return fmt.Errorf("carry dismissals: %w", err)
		}
	}

	// Access statistics (the one non-idempotent write, hence last): sum of counts,
	// latest access.
	count, at := keep.AccessCount+src.AccessCount, keep.LastAccessedAt
	if src.LastAccessedAt.After(at) {
		at = src.LastAccessedAt
	}
	if count != keep.AccessCount || !at.Equal(keep.LastAccessedAt) {
		if at.IsZero() {
			at = time.Now().UTC()
		}
		if err := st.Reinforce(ctx, keepID, count, at); err != nil {
			return fmt.Errorf("carry access stats: %w", err)
		}
	}

	// Usage statistics (stale review's actual-use signal; server-owned like the
	// access stats, same rules): sum of counts, latest use. Without this a merged
	// memory whose sources were heavily used would look unused and be flagged.
	uses, usedAt := keep.UseCount+src.UseCount, keep.LastUsedAt
	if src.LastUsedAt.After(usedAt) {
		usedAt = src.LastUsedAt
	}
	if uses != keep.UseCount || !usedAt.Equal(keep.LastUsedAt) {
		if usedAt.IsZero() {
			usedAt = time.Now().UTC()
		}
		if err := st.RecordUse(ctx, keepID, uses, usedAt); err != nil {
			return fmt.Errorf("carry usage stats: %w", err)
		}
	}
	return nil
}
