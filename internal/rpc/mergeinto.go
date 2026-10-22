package rpc

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"connectrpc.com/connect"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/bus"
	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/merge"
	"github.com/thomas-maurice/cortex/internal/store"
)

// maxMergeInto bounds one MergeInto call (each id costs several store round trips).
const maxMergeInto = merge.MaxSources

// MergeInto folds remove_ids into keep_id: the removed memories' links, access
// stats, not-a-duplicate dismissals and provenance are carried onto keep_id with
// the same logic as Save.supersedes (internal/merge), then they are trashed with
// reason "merged-into:<keep_id>" so the merge is undoable via RestoreFromTrash.
// Used by the Duplicates page's "Keep left/right" so deleting the loser does not
// lose what it knew. Unlike supersedes there is no replacement text to wait for
// (keep is already live), so it runs synchronously. One in-flight call per
// tenant, and the keep/removed memories plus dismissal counterparts are held under
// the worker's per-memory distributed locks for the whole carry-and-trash.
func (s *Service) MergeInto(ctx context.Context, req *connect.Request[cortexv1.MergeIntoRequest]) (*connect.Response[cortexv1.MergeIntoResponse], error) {
	// Bound the raw list before doing any per-id work on it.
	if len(req.Msg.GetRemoveIds()) > maxMergeInto {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("at most %d remove_ids per call", maxMergeInto))
	}
	keepIDs, err := normalizeMemoryIDs("keep_id", []string{req.Msg.GetKeepId()})
	if err != nil {
		return nil, err
	}
	if len(keepIDs) != 1 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("keep_id must not be empty"))
	}
	keepID := keepIDs[0]
	ids, err := normalizeMemoryIDs("remove_ids", req.Msg.GetRemoveIds())
	if err != nil {
		return nil, err
	}
	var removeIDs []string
	for _, id := range ids {
		if id != keepID && !slices.Contains(removeIDs, id) {
			removeIDs = append(removeIDs, id)
		}
	}
	if len(removeIDs) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("remove_ids must name at least one memory other than keep_id"))
	}
	if len(removeIDs) > maxMergeInto {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("at most %d remove_ids per call", maxMergeInto))
	}
	ts, err := s.tenantStore(ctx)
	if err != nil {
		return nil, err
	}
	release, err := s.gated(ctx, &s.mergeGate, false, "a merge is already running for this account; try again when it finishes")
	if err != nil {
		return nil, err
	}
	defer release()

	merged, err := retryLockSetChanged(ctx, func() (int, error) { return s.mergeIntoOnce(ctx, ts, keepID, removeIDs) })
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&cortexv1.MergeIntoResponse{Merged: int32(merged)}), nil
}

// retryLockSetChanged re-runs attempt (with a short jittered backoff) while it
// reports merge.ErrLockSetChanged, i.e. a source's dismissal counterparts changed
// between computing the lock set and carrying. After the retries it gives up with
// Aborted so the client knows it is a transient concurrent change, not a failure.
func retryLockSetChanged(ctx context.Context, attempt func() (int, error)) (int, error) {
	const maxAttempts = 4
	for i := 1; ; i++ {
		n, err := attempt()
		if !errors.Is(err, merge.ErrLockSetChanged) {
			return n, err
		}
		if i >= maxAttempts {
			return 0, connect.NewError(connect.CodeAborted, errors.New("memories were changed concurrently during the merge; nothing further was applied, retry"))
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(20*time.Millisecond + rand.N(80*time.Millisecond)):
		}
	}
}

func (s *Service) mergeIntoOnce(ctx context.Context, ts *store.TenantStore, keepID string, removeIDs []string) (int, error) {
	// Validate first: a bad id must change nothing. An id that is no longer live
	// but sits in the trash with reason merged-into:<keep> was folded in by an
	// earlier (partially failed) call: it counts as done, so a retry succeeds.
	_, ok, err := ts.Get(ctx, keepID)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, connect.NewError(connect.CodeNotFound, fmt.Errorf("memory %s not found", keepID))
	}
	done := 0
	var todo []string
	for _, id := range removeIDs {
		_, live, err := ts.Get(ctx, id)
		if err != nil {
			return 0, err
		}
		if live {
			todo = append(todo, id)
			continue
		}
		e, inTrash, err := ts.GetTrash(ctx, id)
		if err != nil {
			return 0, err
		}
		if !inTrash || e.Reason != memory.TrashReasonMergedInto+keepID {
			return 0, connect.NewError(connect.CodeNotFound, fmt.Errorf("memory %s not found", id))
		}
		done++
	}
	if len(todo) == 0 {
		return done, nil
	}

	// Per-memory dlocks (shared with the worker, cross-replica): keep, the removed
	// memories and the counterparts of their dismissals are read-modify-written
	// below. hctx is bounded inside the first lease, so an overlong merge fails
	// with a deadline error (retry is safe) instead of running unlocked.
	lockIDs, err := merge.LockIDs(ctx, ts, keepID, todo)
	if errors.Is(err, merge.ErrTooLarge) {
		return 0, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	if err != nil {
		return 0, err
	}
	hctx, unlock, err := s.lockMemories(ctx, lockIDs...)
	if err != nil {
		return 0, err
	}
	defer unlock()

	keep, ok, err := ts.Get(hctx, keepID)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, connect.NewError(connect.CodeNotFound, fmt.Errorf("memory %s not found", keepID))
	}
	var srcs []memory.Record
	for _, id := range todo {
		r, live, err := ts.Get(hctx, id)
		if err != nil {
			return 0, err
		}
		if live {
			srcs = append(srcs, r)
		}
	}
	if created, conv := merge.Provenance(keep, srcs); !created.Equal(keep.CreatedAt) || conv != keep.ConversationID {
		if err := ts.SetProvenance(hctx, keepID, created, conv); err != nil {
			return 0, err
		}
	}
	pub := func(op memory.LinkOp, a, b string) error { return bus.PublishLink(ctx, s.js, op, a, b) }
	merged := done
	for _, src := range srcs {
		// Per source: carry, then trash. A failure leaves earlier sources fully
		// done; a retry treats them as done and re-processes only what is live.
		if err := merge.Carry(hctx, ts, keepID, src.ID, todo, pub, lockIDs); err != nil {
			if errors.Is(err, merge.ErrLockSetChanged) {
				return 0, err
			}
			return 0, fmt.Errorf("carry over %s (%d done, retry is safe): %w", src.ID, merged-done, err)
		}
		if err := ts.DeleteWithReason(hctx, src.ID, memory.TrashReasonMergedInto+keepID); err != nil {
			return 0, fmt.Errorf("trash %s after carry-over (%d done, retry is safe): %w", src.ID, merged-done, err)
		}
		merged++
	}
	// Best-effort: the removed ids are gone, stop listing them as candidates.
	if cleaned := slices.DeleteFunc(slices.Clone(keep.DupCandidates), func(id string) bool { return slices.Contains(removeIDs, id) }); len(cleaned) != len(keep.DupCandidates) {
		if err := ts.SetDupCandidates(hctx, keepID, cleaned); err != nil {
			s.log.Warn("merge-into: clearing dupCandidates failed", "id", keepID, "err", err)
		}
	}
	return merged, nil
}
