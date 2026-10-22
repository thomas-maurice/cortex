package rpc

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/store"
)

const (
	defaultMinAgeDays  = 30
	defaultStaleLimit  = 50
	maxDeleteManyIDs   = 500
	deleteManyWorkers  = 8
	errDeleteManyNoID  = "ids must not be empty"
	deleteManyFailed   = "failed" // generic on purpose: backend detail is logged, not returned
	deleteManyNotFound = "not found"
	staleBusyMsg       = "a stale-memory scan is already running for this account; try again shortly"
	deleteManyBusyMsg  = "a bulk delete is already running for this account; try again when it finishes"
	hoursPerDay        = 24
	maxRecordUseIDs    = 100
	recordUseWorkers   = 8
	useLockStripes     = 64
	maxStaleDays       = 36500 // 100 years; keeps days*24h inside time.Duration
)

// listStale serves List in stale-review mode (see store.ListStale).
func (s *Service) listStale(ctx context.Context, req *cortexv1.ListRequest) (*connect.Response[cortexv1.ListResponse], error) {
	// One full-tenant scan per call: single-flight per tenant like the other heavy RPCs.
	release, err := s.gated(ctx, &s.staleGate, false, staleBusyMsg)
	if err != nil {
		return nil, err
	}
	defer release()
	minAge := int32(defaultMinAgeDays)
	if req.MinAgeDays != nil {
		minAge = req.GetMinAgeDays()
	}
	if minAge < 0 || minAge > maxStaleDays {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("min_age_days out of range"))
	}
	opts := store.StaleOpts{
		Namespace:      resolveNamespace(req.GetNamespace(), s.cfg.DefaultNamespace),
		MinAge:         time.Duration(minAge) * hoursPerDay * time.Hour,
		TrackingSince:  s.cfg.UsageTrackingSince,
		IncludeTags:    req.GetTags(),
		AnyTags:        req.GetAnyTags(),
		ExcludeTags:    staleExcludeTags(req),
		Limit:          defaultStaleLimit,
		Offset:         int(req.GetOffset()),
		IncludeExpired: req.GetIncludeExpired(),
	}
	if req.GetLimit() > 0 {
		opts.Limit = clampProtoInt(req.GetLimit())
	}
	switch req.GetStale() {
	case cortexv1.StaleMode_STALE_MODE_NEVER_RECALLED:
	case cortexv1.StaleMode_STALE_MODE_NOT_RECALLED_SINCE:
		d := req.GetStaleDays()
		if d <= 0 || d > maxStaleDays {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("stale_days must be between 1 and 36500 for not-recalled-since mode"))
		}
		opts.IdleFor = time.Duration(d) * hoursPerDay * time.Hour
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("unknown stale mode"))
	}
	ts, err := s.tenantStore(ctx)
	if err != nil {
		return nil, err
	}
	recs, total, err := ts.ListStale(ctx, opts)
	if err != nil {
		return nil, err
	}
	out := &cortexv1.ListResponse{Memories: make([]*cortexv1.Memory, 0, len(recs)), Total: int32(total), TrackingSince: lastAccessedProto(s.cfg.UsageTrackingSince)}
	for _, r := range recs {
		out.Memories = append(out.Memories, recordToProto(r))
	}
	return connect.NewResponse(out), nil
}

// DeleteMany moves each id to the trash through the normal Delete path (so
// cascade-to-chunks and trash-first semantics are identical). Ids are
// normalized and de-duplicated; one bad UUID rejects the whole request before
// anything is deleted, but once running, failures are reported per id and do
// not stop the rest. The store is tenant-scoped, so another tenant's id is
// "not found" exactly like an unknown one — no cross-tenant effect or oracle.
func (s *Service) DeleteMany(ctx context.Context, req *connect.Request[cortexv1.DeleteManyRequest]) (*connect.Response[cortexv1.DeleteManyResponse], error) {
	ids, err := normalizeMemoryIDs("ids", req.Msg.GetIds())
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(ids))
	uniq := ids[:0]
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			uniq = append(uniq, id)
		}
	}
	if len(uniq) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New(errDeleteManyNoID))
	}
	reason, ok := memory.NormalizeDeleteReason(strings.TrimSpace(req.Msg.GetReason()))
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("reason %q is not allowed (use delete, %s or %s)", req.Msg.GetReason(), memory.TrashReasonStaleReview, memory.TrashReasonDuplicateReview))
	}
	if len(uniq) > maxDeleteManyIDs {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("at most %d ids per request", maxDeleteManyIDs))
	}
	release, err := s.gated(ctx, &s.deleteManyGate, false, deleteManyBusyMsg)
	if err != nil {
		return nil, err
	}
	defer release()
	ts, err := s.tenantStore(ctx)
	if err != nil {
		return nil, err
	}

	results := make([]*cortexv1.DeleteManyResult, len(uniq))
	var wg sync.WaitGroup
	sem := make(chan struct{}, deleteManyWorkers)
	for i, id := range uniq {
		results[i] = &cortexv1.DeleteManyResult{Id: id}
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			res := results[i]
			_, found, err := ts.Get(ctx, id)
			switch {
			case err != nil:
				s.log.Warn("delete-many: lookup failed", "id", id, "tenant", gateKey(ctx), "err", err)
				res.Error = deleteManyFailed
			case !found:
				res.Error = deleteManyNotFound
			default:
				if err := ts.DeleteWithReason(ctx, id, reason); err != nil {
					s.log.Warn("delete-many: delete failed", "id", id, "tenant", gateKey(ctx), "err", err)
					res.Error = deleteManyFailed
				} else {
					res.Deleted = true
				}
			}
		}()
	}
	wg.Wait()
	resp := &cortexv1.DeleteManyResponse{Results: results}
	for _, r := range results {
		if r.Deleted {
			resp.Deleted++
		}
	}
	return connect.NewResponse(resp), nil
}

// staleExcludeTags adds the "preference" tag to the caller's exclusions unless the
// caller opted in (include_preferences) or explicitly filters for that tag.
// Preferences are standing instructions loaded wholesale every session, so they
// look "unused" without being unwanted; deleting them from a bulk action would
// be a nasty surprise.
func staleExcludeTags(req *cortexv1.ListRequest) []string {
	excl := req.GetExcludeTags()
	if req.GetIncludePreferences() || slices.Contains(req.GetTags(), preferenceTag) || slices.Contains(req.GetAnyTags(), preferenceTag) {
		return excl
	}
	return append(slices.Clone(excl), preferenceTag)
}

func (s *Service) useLock(ctx context.Context, id string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(gateKey(ctx) + "/" + id))
	return &s.useLocks[h.Sum32()%useLockStripes]
}

// RecordUse notes that memories were delivered into an agent's context: useCount
// += 1, lastUsedAt = now, as a metadata-only merge (no re-embed). Best-effort by
// design: unknown ids (deleted, other tenant) are skipped, per-id failures are
// logged and do not fail the call.
//
// Lost increments: Weaviate has no atomic increment, so each id is a
// read-modify-write guarded by an in-process striped mutex (no NATS lock; a
// server round trip per delivery would cost more than the signal is worth).
// Across several server replicas two concurrent deliveries of one memory could
// lose a count — harmless, since stale review only needs "used at all" and
// "last used", and lastUsedAt is stamped either way.
func (s *Service) RecordUse(ctx context.Context, req *connect.Request[cortexv1.RecordUseRequest]) (*connect.Response[cortexv1.RecordUseResponse], error) {
	ids, err := normalizeMemoryIDs("ids", req.Msg.GetIds())
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(ids))
	uniq := ids[:0]
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			uniq = append(uniq, id)
		}
	}
	if len(uniq) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New(errDeleteManyNoID))
	}
	if len(uniq) > maxRecordUseIDs {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("at most %d ids per request", maxRecordUseIDs))
	}
	ts, err := s.tenantStore(ctx)
	if err != nil {
		return nil, err
	}
	var (
		wg       sync.WaitGroup
		recorded atomic.Int32
	)
	sem := make(chan struct{}, recordUseWorkers)
	now := time.Now().UTC()
	for _, id := range uniq {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			mu := s.useLock(ctx, id)
			mu.Lock()
			defer mu.Unlock()
			rec, ok, err := ts.Get(ctx, id)
			if err != nil {
				s.log.Warn("record-use: get failed, skipping", "id", id, "source", req.Msg.GetSource(), "err", err)
				return
			}
			if !ok {
				return
			}
			if err := ts.RecordUse(ctx, id, rec.UseCount+1, now); err != nil {
				s.log.Warn("record-use: write failed, skipping", "id", id, "source", req.Msg.GetSource(), "err", err)
				return
			}
			recorded.Add(1)
		}()
	}
	wg.Wait()
	return connect.NewResponse(&cortexv1.RecordUseResponse{Recorded: recorded.Load()}), nil
}
