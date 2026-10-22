package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/weaviate/weaviate-go-client/v5/weaviate/fault"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// Trash is the soft-delete store. A delete (explicit, supersedes, namespace)
// copies the Memory record into the Trash class (same tenant, same object id as
// the memory) BEFORE removing it, so a misclick or bad consolidation is
// recoverable until the retention sweep or an explicit purge. It is a separate
// class on purpose: normal Memory queries carry no "deleted" filter and cannot
// see trashed rows. Trash objects have no vector; chunks are not trashed (restore
// re-queues the record, the worker re-embeds and re-chunks).

// trashClass is the authoritative Trash class definition: vectorizer none, no
// vector, multi-tenant exactly when the Memory class is.
func trashClass(multiTenant bool) *models.Class {
	c := &models.Class{
		Class:       memory.TrashClassName,
		Description: "A soft-deleted memory awaiting restore, purge or retention expiry",
		Vectorizer:  "none",
		Properties:  trashProperties(),
	}
	if multiTenant {
		c.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled:            true,
			AutoTenantCreation: true,
		}
	}
	return c
}

// trashProperties is the full property set for the Trash class. Autoschema is off,
// so every property written by putTrash must be declared here. memoryId/namespace
// are exact-match keys (field tokenization), as on Memory.
func trashProperties() []*models.Property {
	return []*models.Property{
		{Name: "memoryId", DataType: []string{"text"}, Tokenization: "field"},
		{Name: "text", DataType: []string{"text"}},
		{Name: "namespace", DataType: []string{"text"}, Tokenization: "field"},
		{Name: "tags", DataType: []string{"text[]"}},
		{Name: "source", DataType: []string{"text"}, Tokenization: "field"},
		{Name: "conversationId", DataType: []string{"text"}, Tokenization: "field"},
		{Name: "createdAt", DataType: []string{"date"}},
		{Name: "linkedIds", DataType: []string{"text[]"}},
		{Name: "notDuplicateOf", DataType: []string{"text[]"}},
		{Name: "useCount", DataType: []string{"int"}},
		{Name: "lastUsedAt", DataType: []string{"date"}},
		{Name: "deletedAt", DataType: []string{"date"}},
		{Name: "reason", DataType: []string{"text"}},
		// Every other Memory property that the worker does NOT recompute is kept so a
		// restore does not reset history: accessCount/lastAccessedAt (recall usage), useCount/lastUsedAt
		// (above; actual use, drives stale review),
		// model/dims (display). dupCandidates (recomputed at index), indexedSeq and
		// indexedStream (re-stamped by the worker from the new queue message) are
		// deliberately NOT kept.
		{Name: "accessCount", DataType: []string{"int"}},
		{Name: "lastAccessedAt", DataType: []string{"date"}},
		{Name: "model", DataType: []string{"text"}},
		{Name: "dims", DataType: []string{"int"}},
		// validUntil is carried so a restore keeps a still-future expiry (see
		// restoreEntry for what happens to one that has passed).
		{Name: "validUntil", DataType: []string{"date"}},
	}
}

var trashScanProps = []string{"memoryId", "text", "namespace", "tags", "source", "conversationId", "createdAt", "linkedIds", "notDuplicateOf", "deletedAt", "reason", "useCount", "lastUsedAt", "accessCount", "lastAccessedAt", "model", "dims", "validUntil"}

// ListAllTrash scans the Trash class without any tenant scope — the migrate-mt
// snapshot, taken while the store is still non-MT (like ListAllRecords). Must not
// be called against an MT-enabled class.
func (s *Store) ListAllTrash(ctx context.Context) ([]memory.TrashEntry, error) {
	out := make([]memory.TrashEntry, 0)
	err := s.scanAll(ctx, memory.TrashClassName, "", trashScanProps, func(r graphql.SearchResult) {
		out = append(out, trashFromResult(r))
	})
	if err != nil {
		return nil, fmt.Errorf("scan trash: %w", err)
	}
	return out, nil
}

// PutTrashEntry writes an existing trash entry (preserving its deletedAt and
// reason) into the tenant's trash. Used by migrate-mt to carry the trash over.
func (ts *TenantStore) PutTrashEntry(ctx context.Context, e memory.TrashEntry) error {
	at := e.DeletedAt
	if at.IsZero() {
		at = time.Now() // legacy undated entry: start its retention clock now
	}
	return ts.putTrash(ctx, e.Record, e.Reason, at)
}

// trashWriteHook is a failure-injection seam for tests: when set it is called
// before every trash write and a non-nil error fails that write.
var trashWriteHook func(id string) error

// putTrash writes rec into the Trash class (create-or-replace by memory id, so
// deleting a restored memory a second time just refreshes the entry).
func (ts *TenantStore) putTrash(ctx context.Context, rec memory.Record, reason string, at time.Time) error {
	if at.IsZero() {
		// A zero deletedAt would make an entry the retention sweep skips forever.
		return fmt.Errorf("refusing to trash %s without a deletion time", rec.ID)
	}
	if trashWriteHook != nil {
		if err := trashWriteHook(rec.ID); err != nil {
			return err
		}
	}
	nz := func(xs []string) []string {
		if xs == nil {
			return []string{}
		}
		return xs
	}
	props := map[string]interface{}{
		"memoryId":       rec.ID,
		"text":           rec.Text,
		"namespace":      rec.Namespace,
		"tags":           nz(rec.Tags),
		"source":         rec.Source,
		"conversationId": rec.ConversationID,
		"linkedIds":      nz(rec.LinkedIDs),
		"notDuplicateOf": nz(rec.NotDuplicateOf),
		"useCount":       rec.UseCount,
		"deletedAt":      at.UTC().Format(time.RFC3339),
		"reason":         reason,
		"accessCount":    rec.AccessCount,
		"model":          rec.Model,
		"dims":           rec.Dims,
	}
	if !rec.LastAccessedAt.IsZero() {
		props["lastAccessedAt"] = formatDate(rec.LastAccessedAt)
	}
	if !rec.CreatedAt.IsZero() {
		props["createdAt"] = rec.CreatedAt.UTC().Format(time.RFC3339)
	}
	if !rec.ValidUntil.IsZero() {
		props["validUntil"] = rec.ValidUntil.UTC().Format(time.RFC3339)
	}
	if !rec.LastUsedAt.IsZero() {
		props["lastUsedAt"] = rec.LastUsedAt.UTC().Format(time.RFC3339)
	}
	return ts.upsertObject(ctx, memory.TrashClassName, rec.ID, props, nil)
}

func trashFromResult(r graphql.SearchResult) memory.TrashEntry {
	e := memory.TrashEntry{Record: resultToRecord(r), Reason: propString(r.Properties, "reason")}
	if id := propString(r.Properties, "memoryId"); id != "" {
		e.ID = id
	}
	if d := propString(r.Properties, "deletedAt"); d != "" {
		e.DeletedAt, _ = time.Parse(time.RFC3339, d) // ignored: best-effort timestamp parse
	}
	return e
}

// ListTrash returns the tenant's trash newest-deletion first. namespace "" means
// all namespaces. The trash is small by design (retention-bounded), so it is
// scanned and filtered/sorted in Go like ListNamespaces.
func (ts *TenantStore) ListTrash(ctx context.Context, namespace string) ([]memory.TrashEntry, error) {
	out := make([]memory.TrashEntry, 0)
	err := ts.s.scanAll(ctx, memory.TrashClassName, ts.t, trashScanProps, func(r graphql.SearchResult) {
		e := trashFromResult(r)
		if namespace == "" || e.Namespace == namespace {
			out = append(out, e)
		}
	})
	if err != nil {
		if isTenantNotFound(err) {
			return out, nil
		}
		return nil, fmt.Errorf("scan trash: %w", err)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].DeletedAt.After(out[j].DeletedAt) })
	return out, nil
}

// GetTrash fetches one trashed memory by id. found is false when absent (or the
// tenant has no trash).
func (ts *TenantStore) GetTrash(ctx context.Context, id string) (memory.TrashEntry, bool, error) {
	props, ok, err := ts.getProps(ctx, memory.TrashClassName, id)
	if err != nil || !ok {
		return memory.TrashEntry{}, false, err
	}
	e := memory.TrashEntry{Record: objectToRecord(id, props)}
	if p, isMap := props.(map[string]interface{}); isMap {
		e.Reason = restString(p, "reason")
		if d := restString(p, "deletedAt"); d != "" {
			e.DeletedAt, _ = time.Parse(time.RFC3339, d) // ignored: best-effort timestamp parse
		}
	}
	return e, true, nil
}

// DeleteTrash permanently removes one trash entry. Removing an entry that is
// already gone is a no-op (purge/restore/sweep may race).
func (ts *TenantStore) DeleteTrash(ctx context.Context, id string) error {
	deleter := ts.s.client.Data().Deleter().WithClassName(memory.TrashClassName).WithID(id)
	if ts.t != "" {
		deleter = deleter.WithTenant(ts.t)
	}
	if err := deleter.Do(ctx); err != nil {
		var wErr *fault.WeaviateClientError
		if isTenantNotFound(err) || (errors.As(err, &wErr) && wErr.StatusCode == 404) {
			return nil
		}
		return fmt.Errorf("delete trash %s: %w", id, err)
	}
	return nil
}

// PurgeTrash permanently deletes every entry in namespace ("" = all) and returns
// how many were purged.
func (ts *TenantStore) PurgeTrash(ctx context.Context, namespace string) (int, error) {
	entries, err := ts.ListTrash(ctx, namespace)
	if err != nil {
		return 0, err
	}
	return ts.purgeEntries(ctx, entries)
}

// PurgeTrashOlderThan permanently deletes entries deleted before cutoff and
// returns how many were purged.
func (ts *TenantStore) PurgeTrashOlderThan(ctx context.Context, cutoff time.Time) (int, error) {
	purged, err := ts.PurgeTrashOlderThanEntries(ctx, cutoff)
	return len(purged), err
}

// PurgeTrashOlderThanEntries is PurgeTrashOlderThan returning the purged entries
// (so the caller can strip their now-dangling links).
func (ts *TenantStore) PurgeTrashOlderThanEntries(ctx context.Context, cutoff time.Time) ([]memory.TrashEntry, error) {
	entries, err := ts.ListTrash(ctx, "")
	if err != nil {
		return nil, err
	}
	old := entries[:0]
	for _, e := range entries {
		if e.DeletedAt.IsZero() {
			// Missing/unparsable deletedAt: age unknown, so never auto-purge it.
			slog.Warn("trash sweep: skipping entry with no valid deletedAt", "id", e.ID, "tenant", ts.t)
			continue
		}
		if e.DeletedAt.Before(cutoff) {
			old = append(old, e)
		}
	}
	return ts.PurgeEntries(ctx, old)
}

// CountTrash returns the number of entries in the tenant's trash (ids only scan).
func (ts *TenantStore) CountTrash(ctx context.Context) (int, error) {
	n := 0
	err := ts.s.scanAll(ctx, memory.TrashClassName, ts.t, nil, func(graphql.SearchResult) { n++ })
	if err != nil {
		if isTenantNotFound(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("count trash: %w", err)
	}
	return n, nil
}

func (ts *TenantStore) purgeEntries(ctx context.Context, entries []memory.TrashEntry) (int, error) {
	purged, err := ts.PurgeEntries(ctx, entries)
	return len(purged), err
}

// PurgeEntries permanently deletes the given trash entries and returns those that
// were deleted (on error, the ones deleted before it).
func (ts *TenantStore) PurgeEntries(ctx context.Context, entries []memory.TrashEntry) ([]memory.TrashEntry, error) {
	done := make([]memory.TrashEntry, 0, len(entries))
	for _, e := range entries {
		if err := ts.DeleteTrash(ctx, e.ID); err != nil {
			return done, err
		}
		done = append(done, e)
	}
	return done, nil
}

// PurgeExpiredTrash is the retention sweep: it purges, across EVERY tenant,
// entries deleted before cutoff, and returns the total purged. A failure on one
// tenant does not stop the others; the errors are joined.
func (s *Store) PurgeExpiredTrash(ctx context.Context, cutoff time.Time) (int, error) {
	purged, err := s.PurgeExpiredTrashEntries(ctx, cutoff)
	n := 0
	for _, es := range purged {
		n += len(es)
	}
	return n, err
}

// PurgeExpiredTrashEntries is PurgeExpiredTrash returning the purged entries by
// tenant (the tenant key is "" when multi-tenancy is off).
func (s *Store) PurgeExpiredTrashEntries(ctx context.Context, cutoff time.Time) (map[string][]memory.TrashEntry, error) {
	tenants := []string{""}
	if s.multiTenant {
		var err error
		if tenants, err = s.ListTenants(ctx, memory.TrashClassName); err != nil {
			return nil, err
		}
	}
	out := map[string][]memory.TrashEntry{}
	var errs []error
	for _, t := range tenants {
		purged, err := s.Tenant(t).PurgeTrashOlderThanEntries(ctx, cutoff)
		if len(purged) > 0 {
			out[t] = purged
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("tenant %q: %w", t, err))
		}
	}
	return out, errors.Join(errs...)
}
