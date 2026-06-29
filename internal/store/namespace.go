package store

import (
	"context"
	"fmt"
	"sort"
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
func (s *Store) ListNamespaces(ctx context.Context) ([]NamespaceStat, error) {
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

	memRes, err := s.client.Experimental().Search().
		WithCollection(memory.ClassName).
		WithProperties("namespace", "createdAt").
		WithMetadata(&graphql.Metadata{ID: true}).
		WithLimit(allCount).
		Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("scan memories: %w", err)
	}
	for _, r := range memRes {
		st := get(propString(r.Properties, "namespace"))
		st.MemoryCount++
		bump(st, propString(r.Properties, "createdAt"))
	}

	sumRes, err := s.client.Experimental().Search().
		WithCollection(memory.SummaryClassName).
		WithProperties("namespace", "updatedAt").
		WithMetadata(&graphql.Metadata{ID: true}).
		WithLimit(allCount).
		Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("scan summaries: %w", err)
	}
	for _, r := range sumRes {
		st := get(propString(r.Properties, "namespace"))
		st.SummaryCount++
		bump(st, propString(r.Properties, "updatedAt"))
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
func (s *Store) RenameNamespace(ctx context.Context, from, to string) (int, int, error) {
	mem, err := s.renameInClass(ctx, memory.ClassName, from, to)
	if err != nil {
		return mem, 0, err
	}
	sum, err := s.renameInClass(ctx, memory.SummaryClassName, from, to)
	if err != nil {
		return mem, sum, err
	}
	return mem, sum, nil
}

// renameInClass sets the namespace property of every object in className that
// currently has namespace `from` to `to`, via a per-object merge.
func (s *Store) renameInClass(ctx context.Context, className, from, to string) (int, error) {
	ids, err := s.idsInNamespace(ctx, className, from)
	if err != nil {
		return 0, err
	}
	updated := 0
	for _, id := range ids {
		if err := s.client.Data().Updater().
			WithMerge().
			WithClassName(className).
			WithID(id).
			WithProperties(map[string]interface{}{"namespace": to}).
			Do(ctx); err != nil {
			return updated, fmt.Errorf("rename %s/%s: %w", className, id, err)
		}
		updated++
	}
	return updated, nil
}

// idsInNamespace returns the ids of every object in className whose namespace is
// exactly the given value.
func (s *Store) idsInNamespace(ctx context.Context, className, namespace string) ([]string, error) {
	res, err := s.client.Experimental().Search().
		WithCollection(className).
		WithMetadata(&graphql.Metadata{ID: true}).
		WithLimit(allCount).
		WithWhere(filters.Where().
			WithPath([]string{"namespace"}).
			WithOperator(filters.Equal).
			WithValueText(namespace)).
		Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("scan %s namespace %q: %w", className, namespace, err)
	}
	ids := make([]string, 0, len(res))
	for _, r := range res {
		ids = append(ids, r.ID)
	}
	return ids, nil
}

// DeleteNamespace permanently deletes every memory AND conversation summary in a
// namespace via Weaviate batch delete (by namespace filter). Returns the number
// of memories and summaries deleted.
func (s *Store) DeleteNamespace(ctx context.Context, namespace string) (int, int, error) {
	mem, err := s.deleteInClass(ctx, memory.ClassName, namespace)
	if err != nil {
		return mem, 0, err
	}
	sum, err := s.deleteInClass(ctx, memory.SummaryClassName, namespace)
	if err != nil {
		return mem, sum, err
	}
	return mem, sum, nil
}

// deleteInClass batch-deletes every object in className whose namespace is the
// given value, returning the count actually deleted.
func (s *Store) deleteInClass(ctx context.Context, className, namespace string) (int, error) {
	res, err := s.client.Batch().ObjectsBatchDeleter().
		WithClassName(className).
		WithWhere(filters.Where().
			WithPath([]string{"namespace"}).
			WithOperator(filters.Equal).
			WithValueText(namespace)).
		Do(ctx)
	if err != nil {
		return 0, fmt.Errorf("batch delete %s namespace %q: %w", className, namespace, err)
	}
	if res != nil && res.Results != nil {
		return int(res.Results.Successful), nil
	}
	return 0, nil
}
