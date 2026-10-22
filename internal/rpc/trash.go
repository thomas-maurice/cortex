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
	"github.com/thomas-maurice/cortex/internal/memory"
)

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
	ts, err := s.tenantStore(ctx)
	if err != nil {
		return nil, err
	}
	entries, err := ts.ListTrash(ctx, req.Msg.GetNamespace())
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

// RestoreFromTrash re-queues trashed memories through the normal index path (the
// worker re-embeds and re-chunks), keeping their ids. Everything is validated
// before anything is published, so a bad request restores nothing: every id must
// be in the caller's trash (NotFound otherwise — identical for an unknown id and
// another tenant's) and must NOT exist as a live memory (AlreadyExists; a restore
// never overwrites). Links are restored via the link queue, only to endpoints
// that still exist (the trashed record's dangling links are not re-applied).
func (s *Service) RestoreFromTrash(ctx context.Context, req *connect.Request[cortexv1.RestoreFromTrashRequest]) (*connect.Response[cortexv1.RestoreFromTrashResponse], error) {
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

	// Ids restored in this same request are valid link endpoints too: the link
	// queue waits for each endpoint to be indexed before applying the edge.
	restoring := make(map[string]bool, len(entries))
	for _, e := range entries {
		restoring[e.ID] = true
	}
	queued := 0
	for _, e := range entries {
		rec := e.Record
		links := rec.LinkedIDs
		rec.LinkedIDs = nil // re-applied below via the link queue, existing endpoints only
		rec.Supersedes = nil
		if rec.CreatedAt.IsZero() {
			rec.CreatedAt = time.Now().UTC()
		}
		if err := bus.PublishIndex(ctx, s.js, rec); err != nil {
			return nil, fmt.Errorf("restore publish %s (%d already queued): %w", rec.ID, queued, err)
		}
		queued++
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
		// Queued: drop the trash copy. The index message now owns the data (a
		// failure to index dead-letters it rather than losing it).
		if err := ts.DeleteTrash(ctx, rec.ID); err != nil {
			return nil, fmt.Errorf("restore queued %s but removing it from trash failed (purge it manually): %w", rec.ID, err)
		}
	}
	return connect.NewResponse(&cortexv1.RestoreFromTrashResponse{Queued: int32(queued)}), nil
}

// PurgeTrash permanently deletes trash entries: the listed ids (all must exist in
// the caller's trash) or, with all=true, everything (optionally one namespace).
func (s *Service) PurgeTrash(ctx context.Context, req *connect.Request[cortexv1.PurgeTrashRequest]) (*connect.Response[cortexv1.PurgeTrashResponse], error) {
	ts, err := s.tenantStore(ctx)
	if err != nil {
		return nil, err
	}
	if req.Msg.GetAll() {
		if len(req.Msg.GetIds()) > 0 {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("ids and all are mutually exclusive"))
		}
		n, err := ts.PurgeTrash(ctx, req.Msg.GetNamespace())
		if err != nil {
			return nil, err
		}
		return connect.NewResponse(&cortexv1.PurgeTrashResponse{Purged: int32(n)}), nil
	}
	ids, err := trashIDs(req.Msg.GetIds())
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		if _, ok, err := ts.GetTrash(ctx, id); err != nil {
			return nil, err
		} else if !ok {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("trash entry %s not found", id))
		}
	}
	for _, id := range ids {
		if err := ts.DeleteTrash(ctx, id); err != nil {
			return nil, err
		}
	}
	return connect.NewResponse(&cortexv1.PurgeTrashResponse{Purged: int32(len(ids))}), nil
}

// SweepTrash is the retention sweep: it purges, across all tenants, trash entries
// older than cfg.TrashRetention. A zero retention keeps the trash forever (no-op).
func (s *Service) SweepTrash(ctx context.Context) (int, error) {
	if s.cfg.TrashRetention <= 0 {
		return 0, nil
	}
	return s.store.PurgeExpiredTrash(ctx, time.Now().Add(-s.cfg.TrashRetention))
}
