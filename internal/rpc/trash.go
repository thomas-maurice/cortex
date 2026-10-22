package rpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/bus"
	"github.com/thomas-maurice/cortex/internal/identity"
	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/store"
)

const scanBusyMsg = "a namespace delete / bulk trash restore / trash purge is already running for this account; try again when it finishes"

const (
	defaultTrashLimit = 100
	maxTrashIDs       = 1000
)

func trashToProto(e memory.TrashEntry) *cortexv1.TrashEntry {
	return &cortexv1.TrashEntry{
		Id:             e.ID,
		Text:           e.Text,
		Namespace:      e.Namespace,
		Tags:           e.Tags,
		Source:         e.Source,
		ConversationId: e.ConversationID,
		CreatedAt:      timestamppb.New(e.CreatedAt),
		LinkedIds:      e.LinkedIDs,
		DeletedAt:      timestamppb.New(e.DeletedAt),
		Reason:         e.Reason,
	}
}

// ListTrash lists the caller's trashed memories, newest deletion first. The
// store is tenant-scoped from the request identity, so another tenant's trash is
// simply not visible.
func (s *Service) ListTrash(ctx context.Context, req *connect.Request[cortexv1.ListTrashRequest]) (*connect.Response[cortexv1.ListTrashResponse], error) {
	// Read-only: deliberately NOT gated, so overlapping listings (UI paging, MCP
	// cortex_trash_list) never fail with ResourceExhausted.
	ts, err := s.tenantStore(ctx)
	if err != nil {
		return nil, err
	}
	entries, err := filterTrash(ctx, ts, req.Msg.GetNamespace(), req.Msg.GetReason())
	if err != nil {
		return nil, err
	}
	limit, offset := int(req.Msg.GetLimit()), int(req.Msg.GetOffset())
	if limit <= 0 {
		limit = defaultTrashLimit
	}
	total := len(entries)
	if offset < 0 || offset > total {
		offset = total
	}
	end := min(offset+limit, total)
	out := &cortexv1.ListTrashResponse{
		Entries:          make([]*cortexv1.TrashEntry, 0, end-offset),
		Total:            int32(total),
		RetentionSeconds: int64(s.cfg.TrashRetention / time.Second),
	}
	for _, e := range entries[offset:end] {
		out.Entries = append(out.Entries, trashToProto(e))
	}
	return connect.NewResponse(out), nil
}

// trashIDs validates a non-empty id list (canonical UUIDs, deduplicated, capped).
func trashIDs(ids []string) ([]string, error) {
	norm, err := normalizeMemoryIDs("ids", ids)
	if err != nil {
		return nil, err
	}
	if len(norm) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("ids must not be empty"))
	}
	if len(norm) > maxTrashIDs {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("at most %d ids per request", maxTrashIDs))
	}
	seen := make(map[string]bool, len(norm))
	out := norm[:0]
	for _, id := range norm {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out, nil
}

// filterTrash lists the tenant's trash filtered by namespace ("" = all) and reason
// ("" = all; see memory.TrashReasonMatches), newest deletion first.
func filterTrash(ctx context.Context, ts *store.TenantStore, namespace, reason string) ([]memory.TrashEntry, error) {
	entries, err := ts.ListTrash(ctx, namespace)
	if err != nil {
		return nil, err
	}
	if reason == "" {
		return entries, nil
	}
	out := entries[:0]
	for _, e := range entries {
		if memory.TrashReasonMatches(e.Reason, reason) {
			out = append(out, e)
		}
	}
	return out, nil
}

const maxRestoreFailures = 10

// RestoreFromTrash re-queues trashed memories through the normal index path (the
// worker re-embeds and re-chunks), keeping their ids, access history and
// metadata. Two forms:
//
//   - ids: validated before anything is published, so a bad request restores
//     nothing: every id must be in the caller's trash (NotFound otherwise —
//     identical for an unknown id and another tenant's) and must NOT exist as a
//     live memory (AlreadyExists; a restore never overwrites).
//   - all (+ namespace/reason filters): every matching entry is restored
//     independently; entries that cannot be (e.g. the id is live again) are
//     counted in failed, not fatal. No id cap.
//
// Links go through the link queue, only to endpoints that are live or restored in
// the same request. A target still in the trash is NOT linked now: its own trash
// entry keeps the reciprocal edge, so restoring it later re-links it to this one
// (see restoreEntry).
func (s *Service) RestoreFromTrash(ctx context.Context, req *connect.Request[cortexv1.RestoreFromTrashRequest]) (*connect.Response[cortexv1.RestoreFromTrashResponse], error) {
	if req.Msg.GetAll() {
		return s.restoreAll(ctx, req.Msg)
	}
	ids, err := trashIDs(req.Msg.GetIds())
	if err != nil {
		return nil, err
	}
	ts, err := s.tenantStore(ctx)
	if err != nil {
		return nil, err
	}
	entries := make([]memory.TrashEntry, 0, len(ids))
	for _, id := range ids {
		e, ok, err := ts.GetTrash(ctx, id)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("trash entry %s not found", id))
		}
		_, live, err := ts.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if live {
			return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf("memory %s already exists; purge the trash entry instead", id))
		}
		entries = append(entries, e)
	}
	restoring := restoringSet(entries)
	queued := 0
	for _, e := range entries {
		if err := s.restoreEntry(ctx, ts, e, restoring); err != nil {
			return nil, fmt.Errorf("restore %s (%d already queued): %w", e.ID, queued, err)
		}
		queued++
	}
	return connect.NewResponse(&cortexv1.RestoreFromTrashResponse{Queued: int32(queued)}), nil
}

func restoringSet(entries []memory.TrashEntry) map[string]bool {
	m := make(map[string]bool, len(entries))
	for _, e := range entries {
		m[e.ID] = true
	}
	return m
}

func (s *Service) restoreAll(ctx context.Context, msg *cortexv1.RestoreFromTrashRequest) (*connect.Response[cortexv1.RestoreFromTrashResponse], error) {
	if len(msg.GetIds()) > 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("ids and all are mutually exclusive"))
	}
	release, err := s.gated(ctx, &s.scanGate, false, scanBusyMsg)
	if err != nil {
		return nil, err
	}
	defer release()
	ts, err := s.tenantStore(ctx)
	if err != nil {
		return nil, err
	}
	entries, err := filterTrash(ctx, ts, msg.GetNamespace(), msg.GetReason())
	if err != nil {
		return nil, err
	}
	restoring := restoringSet(entries)
	out := &cortexv1.RestoreFromTrashResponse{}
	fail := func(id string, err error) {
		out.Failed++
		if len(out.Failures) < maxRestoreFailures {
			out.Failures = append(out.Failures, id+": "+err.Error())
		}
	}
	for _, e := range entries {
		_, live, err := ts.Get(ctx, e.ID)
		if err != nil {
			fail(e.ID, err)
			continue
		}
		if live {
			fail(e.ID, errors.New("a live memory with this id already exists"))
			continue
		}
		if err := s.restoreEntry(ctx, ts, e, restoring); err != nil {
			fail(e.ID, err)
			continue
		}
		out.Queued++
	}
	return connect.NewResponse(out), nil
}

// restoreEntry publishes one trashed record for indexing, queues its links and
// drops the trash copy. The index payload keeps the access history (accessCount /
// lastAccessedAt) but carries no LinkedIDs: edges are re-applied through the link
// queue to endpoints that are live or being restored in this same call. A target
// that is still in the trash is skipped on purpose — it keeps the reciprocal edge
// on its own entry, so restoring it later (it then sees this record live) re-links
// the pair without relying on the link queue outliving a long gap. A target that
// was purged is dropped (its links were stripped at purge time).
func (s *Service) restoreEntry(ctx context.Context, ts *store.TenantStore, e memory.TrashEntry, restoring map[string]bool) error {
	rec := e.Record
	links := rec.LinkedIDs
	rec.LinkedIDs = nil
	rec.Supersedes = nil
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	if err := bus.PublishIndex(ctx, s.js, rec); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	for _, target := range links {
		if target == rec.ID {
			continue
		}
		if !restoring[target] {
			if _, exists, err := ts.Get(ctx, target); err != nil || !exists {
				continue
			}
		}
		if err := bus.PublishLink(ctx, s.js, memory.LinkOpAdd, rec.ID, target); err != nil {
			s.log.Warn("restore: publish link failed", "id", rec.ID, "target", target, "err", err)
		}
	}
	// Queued: drop the trash copy. The index message now owns the data (a failure
	// to index dead-letters it rather than losing it).
	if err := ts.DeleteTrash(ctx, rec.ID); err != nil {
		return fmt.Errorf("queued but removing it from trash failed (purge it manually): %w", err)
	}
	return nil
}

// stripDanglingLinks removes the purged ids from their live neighbours' linkedIds
// via the link queue (REMOVE; the worker tolerates a missing endpoint). A purged
// id that is live again (its stale trash copy was purged) keeps its edges. ctx
// must carry the tenant. Best-effort: failures are logged.
func (s *Service) stripDanglingLinks(ctx context.Context, ts *store.TenantStore, purged []memory.TrashEntry) {
	for _, e := range purged {
		if len(e.LinkedIDs) == 0 {
			continue
		}
		if _, live, err := ts.Get(ctx, e.ID); err != nil || live {
			continue
		}
		for _, target := range e.LinkedIDs {
			if target == e.ID {
				continue
			}
			if err := bus.PublishLink(ctx, s.js, memory.LinkOpRemove, e.ID, target); err != nil {
				s.log.Warn("purge: publish link removal failed", "id", e.ID, "target", target, "err", err)
			}
		}
	}
}

// PurgeTrash permanently deletes trash entries: the listed ids (all must exist in
// the caller's trash) or, with all=true, everything matching the optional
// namespace/reason filters. Links from live neighbours to purged ids are stripped.
func (s *Service) PurgeTrash(ctx context.Context, req *connect.Request[cortexv1.PurgeTrashRequest]) (*connect.Response[cortexv1.PurgeTrashResponse], error) {
	if req.Msg.GetAll() {
		if len(req.Msg.GetIds()) > 0 {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("ids and all are mutually exclusive"))
		}
		release, err := s.gated(ctx, &s.scanGate, false, scanBusyMsg)
		if err != nil {
			return nil, err
		}
		defer release()
		ts, err := s.tenantStore(ctx)
		if err != nil {
			return nil, err
		}
		entries, err := filterTrash(ctx, ts, req.Msg.GetNamespace(), req.Msg.GetReason())
		if err != nil {
			return nil, err
		}
		purged, err := ts.PurgeEntries(ctx, entries)
		s.stripDanglingLinks(ctx, ts, purged)
		if err != nil {
			return nil, err
		}
		return connect.NewResponse(&cortexv1.PurgeTrashResponse{Purged: int32(len(purged))}), nil
	}
	ids, err := trashIDs(req.Msg.GetIds())
	if err != nil {
		return nil, err
	}
	ts, err := s.tenantStore(ctx)
	if err != nil {
		return nil, err
	}
	entries := make([]memory.TrashEntry, 0, len(ids))
	for _, id := range ids {
		e, ok, err := ts.GetTrash(ctx, id)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("trash entry %s not found", id))
		}
		entries = append(entries, e)
	}
	purged, err := ts.PurgeEntries(ctx, entries)
	s.stripDanglingLinks(ctx, ts, purged)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&cortexv1.PurgeTrashResponse{Purged: int32(len(purged))}), nil
}

// SweepTrash is the retention sweep: it purges, across all tenants, trash entries
// older than cfg.TrashRetention and strips the purged ids from their neighbours'
// links. A zero retention keeps the trash forever (no-op).
func (s *Service) SweepTrash(ctx context.Context) (int, error) {
	if s.cfg.TrashRetention <= 0 {
		return 0, nil
	}
	byTenant, err := s.store.PurgeExpiredTrashEntries(ctx, time.Now().Add(-s.cfg.TrashRetention))
	n := 0
	for tenant, purged := range byTenant {
		n += len(purged)
		tctx := identity.Into(ctx, identity.Identity{UserID: tenant})
		s.stripDanglingLinks(tctx, s.store.Tenant(tenant), purged)
	}
	return n, err
}
