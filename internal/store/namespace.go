package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// NamespaceStat aggregates one namespace's contents for the namespace admin view.
type NamespaceStat struct {
	Name         string
	MemoryCount  int
	SummaryCount int
	// LastUpdated is the most recent activity in the namespace: the newest memory
	// createdAt or summary updatedAt. Zero when the namespace carries no timestamps.
	LastUpdated time.Time
}

// ListNamespaces scans the Memory and ConversationSummary classes and aggregates
// per-namespace counts plus the most recent activity timestamp. There is no gRPC
// Aggregate in the stable client, so it scans the (small) store in Go — the same
// assumption Count and reindex already rely on.
func (ts *TenantStore) ListNamespaces(ctx context.Context) ([]NamespaceStat, error) {
	stats := map[string]*NamespaceStat{}
	get := func(name string) *NamespaceStat {
		st := stats[name]
		if st == nil {
			st = &NamespaceStat{Name: name}
			stats[name] = st
		}
		return st
	}
	bump := func(st *NamespaceStat, raw string) {
		if raw == "" {
			return
		}
		if t, err := time.Parse(time.RFC3339, raw); err == nil && t.After(st.LastUpdated) {
			st.LastUpdated = t
		}
	}

	err := ts.s.scanAll(ctx, memory.ClassName, ts.t, []string{"namespace", "createdAt"}, func(r graphql.SearchResult) {
		st := get(propString(r.Properties, "namespace"))
		st.MemoryCount++
		bump(st, propString(r.Properties, "createdAt"))
	})
	if err != nil && !isTenantNotFound(err) {
		return nil, fmt.Errorf("scan memories: %w", err)
	}

	err = ts.s.scanAll(ctx, memory.SummaryClassName, ts.t, []string{"namespace", "updatedAt"}, func(r graphql.SearchResult) {
		st := get(propString(r.Properties, "namespace"))
		st.SummaryCount++
		bump(st, propString(r.Properties, "updatedAt"))
	})
	if err != nil && !isTenantNotFound(err) {
		return nil, fmt.Errorf("scan summaries: %w", err)
	}

	out := make([]NamespaceStat, 0, len(stats))
	for _, st := range stats {
		out = append(out, *st)
	}
	// Busiest namespaces first, then alphabetical for a stable order.
	sort.Slice(out, func(i, j int) bool {
		if out[i].MemoryCount != out[j].MemoryCount {
			return out[i].MemoryCount > out[j].MemoryCount
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// RenameNamespace moves every memory AND conversation summary from one namespace
// to another. The namespace is filterable metadata, never embedded, so this is a
// per-object merge (PATCH) that never re-embeds — the same reasoning as SetLinks.
// Renaming into an existing namespace merges the two. Returns the number of
// memories and summaries updated.
func (ts *TenantStore) RenameNamespace(ctx context.Context, from, to string) (int, int, error) {
	mem, err := ts.renameInClass(ctx, memory.ClassName, from, to)
	if err != nil {
		return mem, 0, err
	}
	// Chunks carry their parent's namespace so namespace filters push down to the
	// chunk query; they must move too, or a chunk would keep the old namespace and
	// be missed by a namespace-scoped search. Not reported in the user-facing count.
	if _, err := ts.renameInClass(ctx, memory.ChunkClassName, from, to); err != nil {
		return mem, 0, err
	}
	sum, err := ts.renameInClass(ctx, memory.SummaryClassName, from, to)
	if err != nil {
		return mem, sum, err
	}
	return mem, sum, nil
}

// renameInClass sets the namespace property of every object in className that
// currently has namespace `from` to `to`, via a per-object merge.
func (ts *TenantStore) renameInClass(ctx context.Context, className, from, to string) (int, error) {
	ids, err := ts.idsInNamespace(ctx, className, from)
	if err != nil {
		return 0, err
	}
	updated := 0
	for _, id := range ids {
		updater := ts.s.client.Data().Updater().
			WithMerge().
			WithClassName(className).
			WithID(id).
			WithProperties(map[string]interface{}{"namespace": to})
		if ts.t != "" {
			updater = updater.WithTenant(ts.t)
		}
		if err := updater.Do(ctx); err != nil {
			return updated, fmt.Errorf("rename %s/%s: %w", className, id, err)
		}
		updated++
	}
	return updated, nil
}

// idsInNamespace returns the ids of every object in className whose namespace is
// exactly the given value.
func (ts *TenantStore) idsInNamespace(ctx context.Context, className, namespace string) ([]string, error) {
	ids := make([]string, 0)
	err := ts.s.scanAll(ctx, className, ts.t, []string{"namespace"}, func(r graphql.SearchResult) {
		if propString(r.Properties, "namespace") == namespace {
			ids = append(ids, r.ID)
		}
	})
	if err != nil {
		if isTenantNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("scan %s namespace %q: %w", className, namespace, err)
	}
	return ids, nil
}

// namespaceDeleteWorkers bounds the concurrent per-memory trash+delete round trips
// of a namespace delete.
const namespaceDeleteWorkers = 8

// trashAndDelete moves each record to Trash and then removes it (with its chunks),
// namespaceDeleteWorkers at a time. On the first error no further records are
// started and that error is returned together with the number of records that
// were fully trashed AND deleted. Records whose trash write failed are never
// deleted; records already trashed stay restorable.
func (ts *TenantStore) trashAndDelete(ctx context.Context, recs []memory.Record, reason string) (int, error) {
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		done     int
		firstErr error
	)
	// In-flight records always run to completion on the caller's ctx (never a
	// cancelled one), so a record is either fully trashed+deleted (counted) or its
	// trash write failed / it was never started — the returned count is exact.
	failed := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return firstErr != nil
	}
	now := time.Now()
	sem := make(chan struct{}, namespaceDeleteWorkers)
	for _, rec := range recs {
		sem <- struct{}{}
		if failed() || ctx.Err() != nil { // start nothing more after an error
			<-sem
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			err := ts.putTrash(ctx, rec, reason, now)
			if err != nil {
				err = fmt.Errorf("move %s to trash: %w", rec.ID, err)
			} else {
				err = ts.hardDelete(ctx, rec.ID)
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			done++
		}()
	}
	wg.Wait()
	if firstErr == nil && ctx.Err() != nil {
		firstErr = ctx.Err()
	}
	return done, firstErr
}

// DeleteNamespace deletes every memory AND conversation summary in a namespace.
// Memories are soft-deleted: each is copied to Trash (reason "namespace-delete")
// and then removed with its chunks, one by one, so a trash write failure stops
// the wipe with nothing lost beyond the already-trashed (recoverable) records.
// Conversation summaries are NOT trashed — they are permanently deleted via batch
// delete. Returns the number of memories and summaries deleted.
func (ts *TenantStore) DeleteNamespace(ctx context.Context, namespace string) (int, int, error) {
	var recs []memory.Record
	var mem int
	err := ts.s.scanAll(ctx, memory.ClassName, ts.t, memoryProps, func(r graphql.SearchResult) {
		if propString(r.Properties, "namespace") == namespace {
			recs = append(recs, resultToRecord(r))
		}
	})
	if err != nil && !isTenantNotFound(err) {
		return 0, 0, fmt.Errorf("scan memories in namespace %q: %w", namespace, err)
	}
	mem, err = ts.trashAndDelete(ctx, recs, memory.TrashReasonNamespaceDelete)
	if err != nil {
		return mem, 0, err
	}
	// Sweep the namespace's chunks so no orphaned chunk survives (a memory indexed
	// mid-wipe survives intact rather than being deleted untrashed). Not reported
	// in the user count.
	if _, err := ts.deleteInClass(ctx, memory.ChunkClassName, namespace); err != nil {
		return mem, 0, err
	}
	sum, err := ts.deleteInClass(ctx, memory.SummaryClassName, namespace)
	if err != nil {
		return mem, sum, err
	}
	return mem, sum, nil
}

// deleteInClass batch-deletes every object in className whose namespace is the
// given value, returning the count actually deleted.
func (ts *TenantStore) deleteInClass(ctx context.Context, className, namespace string) (int, error) {
	deleter := ts.s.client.Batch().ObjectsBatchDeleter().
		WithClassName(className).
		WithWhere(filters.Where().
			WithPath([]string{"namespace"}).
			WithOperator(filters.Equal).
			WithValueText(namespace))
	if ts.t != "" {
		deleter = deleter.WithTenant(ts.t)
	}
	res, err := deleter.Do(ctx)
	if err != nil {
		if isTenantNotFound(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("batch delete %s namespace %q: %w", className, namespace, err)
	}
	if res != nil && res.Results != nil {
		return int(res.Results.Successful), nil
	}
	return 0, nil
}
