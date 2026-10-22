package store

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// TagStat aggregates one tag's usage for the tag admin view. Summaries carry no
// tags (see summaryProperties), so only memories are counted.
type TagStat struct {
	Name        string
	// MemoryCount counts LIVE memories; ExpiredCount the expired ones (hidden from
	// search/list, awaiting the sweep) carrying the tag. A tag only on expired
	// memories still gets a row (count 0) so it can be renamed/deleted.
	MemoryCount  int
	ExpiredCount int
	// LastActivity is the newest createdAt among memories carrying the tag. Zero
	// when none has a timestamp.
	LastActivity time.Time
}

// MemoryLocker takes the per-memory lock (the same lock the worker's index path
// holds while writing that memory: dlock key "mem"/tenant/id). It returns a ctx
// bounded to the critical section and a release func. A nil MemoryLocker means
// "no locking" (unit tests, or deployments without a lock service).
type MemoryLocker func(ctx context.Context, memoryID string) (context.Context, func(), error)

// matchesNamespace: empty namespace or "*" means every namespace.
func matchesNamespace(want, got string) bool {
	return want == "" || want == "*" || want == got
}

// ListTags aggregates tags over the tenant's memories (cursor-scanned, so correct
// beyond the 10k query cap), optionally limited to one namespace. Tags are
// compared exactly (case-sensitive): that is the point, `k8s` and `K8s` show up
// as separate rows so they can be merged. Counts are of live memories; expired ones
// are counted separately (ExpiredCount). Busiest first, then alphabetical.
func (ts *TenantStore) ListTags(ctx context.Context, namespace string) ([]TagStat, error) {
	stats := map[string]*TagStat{}
	now := time.Now()
	err := ts.s.scanAll(ctx, memory.ClassName, ts.t, []string{"namespace", "tags", "createdAt", "validUntil"}, func(r graphql.SearchResult) {
		if !matchesNamespace(namespace, propString(r.Properties, "namespace")) {
			return
		}
		var created time.Time
		if raw := propString(r.Properties, "createdAt"); raw != "" {
			created, _ = time.Parse(time.RFC3339, raw)
		}
		expired := expiredProp(r.Properties, now)
		// A memory counts once per distinct tag even if its list repeats one.
		seen := map[string]bool{}
		for _, tag := range propStrings(r.Properties, "tags") {
			if seen[tag] {
				continue
			}
			seen[tag] = true
			st := stats[tag]
			if st == nil {
				st = &TagStat{Name: tag}
				stats[tag] = st
			}
			if expired {
				st.ExpiredCount++
			} else {
				st.MemoryCount++
			}
			if created.After(st.LastActivity) {
				st.LastActivity = created
			}
		}
	})
	if err != nil {
		if isTenantNotFound(err) {
			return []TagStat{}, nil
		}
		return nil, fmt.Errorf("scan memories: %w", err)
	}
	out := make([]TagStat, 0, len(stats))
	for _, st := range stats {
		out = append(out, *st)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].MemoryCount != out[j].MemoryCount {
			return out[i].MemoryCount > out[j].MemoryCount
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// RenameTag replaces tag `from` with `to` on every memory (and its chunks) that
// carries `from`, optionally limited to one namespace. If a memory already has
// `to` the result is deduplicated, so renaming into an existing tag merges the
// two. Metadata-only: a per-object merge of `tags`, never a re-embed and never a
// worker round-trip. Returns the number of memories changed.
func (ts *TenantStore) RenameTag(ctx context.Context, from, to, namespace string, lock MemoryLocker) (int, error) {
	return ts.mutateTag(ctx, from, namespace, lock, func(tags []string) []string {
		out := make([]string, 0, len(tags))
		for _, t := range tags {
			if t == from {
				t = to
			}
			if !slices.Contains(out, t) {
				out = append(out, t)
			}
		}
		return out
	})
}

// DeleteTag removes `tag` from every memory (and its chunks) that carries it,
// optionally limited to one namespace. The memories themselves are kept. Returns
// the number of memories changed.
func (ts *TenantStore) DeleteTag(ctx context.Context, tag, namespace string, lock MemoryLocker) (int, error) {
	return ts.mutateTag(ctx, tag, namespace, lock, func(tags []string) []string {
		out := make([]string, 0, len(tags))
		for _, t := range tags {
			if t != tag {
				out = append(out, t)
			}
		}
		return out
	})
}

// mutateTag finds the memories carrying `tag` with one cursor scan, then for each
// one: takes the memory's lock, RE-READS its tags (the scan is stale by now: an
// index commit may have rewritten them), applies fn and merges only `tags` into
// the memory and its chunks. Reading under the lock is what makes the
// read-modify-write safe against a concurrent index commit, which holds the same
// lock while it writes tags (see docs/DESIGN.md, field ownership).
func (ts *TenantStore) mutateTag(ctx context.Context, tag, namespace string, lock MemoryLocker, fn func([]string) []string) (int, error) {
	var ids []string
	err := ts.s.scanAll(ctx, memory.ClassName, ts.t, []string{"namespace", "tags"}, func(r graphql.SearchResult) {
		if matchesNamespace(namespace, propString(r.Properties, "namespace")) &&
			slices.Contains(propStrings(r.Properties, "tags"), tag) {
			ids = append(ids, r.ID)
		}
	})
	if err != nil {
		if isTenantNotFound(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("scan memories for tag %q: %w", tag, err)
	}
	changed := 0
	for _, id := range ids {
		did, err := ts.mutateMemoryTags(ctx, id, tag, namespace, lock, fn)
		if err != nil {
			return changed, err
		}
		if did {
			changed++
		}
	}
	// Trash entries keep their tags and restore republishes them, so a rename or
	// delete must reach them too, or a restore would resurrect the old (or a
	// deleted, e.g. `preference`) tag. Not counted in the returned total, which
	// stays the number of live memories changed.
	if err := ts.mutateTrashTags(ctx, tag, namespace, lock, fn); err != nil {
		return changed, err
	}
	return changed, nil
}

// mutateTrashTags is mutateTag's pass over the tenant's Trash class: same exact
// match, namespace scope and lock-then-re-read discipline, tags merged on the
// Trash object only (trash entries have no chunks).
func (ts *TenantStore) mutateTrashTags(ctx context.Context, tag, namespace string, lock MemoryLocker, fn func([]string) []string) error {
	var ids []string
	err := ts.s.scanAll(ctx, memory.TrashClassName, ts.t, []string{"namespace", "tags"}, func(r graphql.SearchResult) {
		if matchesNamespace(namespace, propString(r.Properties, "namespace")) &&
			slices.Contains(propStrings(r.Properties, "tags"), tag) {
			ids = append(ids, r.ID)
		}
	})
	if err != nil {
		if isTenantNotFound(err) {
			return nil
		}
		return fmt.Errorf("scan trash for tag %q: %w", tag, err)
	}
	for _, id := range ids {
		if err := ts.mutateTrashEntryTags(ctx, id, tag, namespace, lock, fn); err != nil {
			return err
		}
	}
	return nil
}

func (ts *TenantStore) mutateTrashEntryTags(ctx context.Context, id, tag, namespace string, lock MemoryLocker, fn func([]string) []string) error {
	if lock != nil {
		hctx, release, err := lock(ctx, id)
		if err != nil {
			return fmt.Errorf("lock trash entry %s: %w", id, err)
		}
		defer release()
		ctx = hctx
	}
	e, found, err := ts.GetTrash(ctx, id)
	if err != nil {
		return fmt.Errorf("read trash entry %s: %w", id, err)
	}
	// Restored, purged or retagged since the scan: nothing to do.
	if !found || !slices.Contains(e.Tags, tag) || !matchesNamespace(namespace, e.Namespace) {
		return nil
	}
	return ts.mergeTags(ctx, memory.TrashClassName, id, fn(e.Tags))
}

func (ts *TenantStore) mutateMemoryTags(ctx context.Context, id, tag, namespace string, lock MemoryLocker, fn func([]string) []string) (bool, error) {
	if lock != nil {
		hctx, release, err := lock(ctx, id)
		if err != nil {
			return false, fmt.Errorf("lock memory %s: %w", id, err)
		}
		defer release()
		ctx = hctx
	}
	rec, found, err := ts.Get(ctx, id)
	if err != nil {
		return false, fmt.Errorf("read memory %s: %w", id, err)
	}
	// Gone, retagged or moved to another namespace since the scan: nothing to do.
	if !found || !slices.Contains(rec.Tags, tag) || !matchesNamespace(namespace, rec.Namespace) {
		return false, nil
	}
	newTags := fn(rec.Tags)
	// Chunks FIRST, then the memory. The memory's tags are what the scan (and a
	// retry) use to find work: if a chunk merge fails after the memory was
	// updated, the retry would skip the memory and leave chunk tags stale
	// forever. This way a failure leaves the memory untouched and a retry redoes
	// (idempotently) both. Chunks carry their parent's tags so tag filters push
	// down to the chunk query (the primary search path).
	chunkIDs, err := ts.chunkIDsOf(ctx, id)
	if err != nil {
		return false, err
	}
	for _, cid := range chunkIDs {
		if err := ts.mergeTags(ctx, memory.ChunkClassName, cid, newTags); err != nil {
			return false, err
		}
	}
	if err := ts.mergeTags(ctx, memory.ClassName, id, newTags); err != nil {
		return false, err
	}
	return true, nil
}

// mergeTagsHook is a test seam: when set, it runs before every tags merge and a
// non-nil return fails that merge. Always nil in production.
var mergeTagsHook func(className, id string) error

// mergeTags PATCHes only the tags property (no vector, so no re-embed).
func (ts *TenantStore) mergeTags(ctx context.Context, className, id string, tags []string) error {
	if mergeTagsHook != nil {
		if err := mergeTagsHook(className, id); err != nil {
			return err
		}
	}
	if tags == nil {
		tags = []string{}
	}
	updater := ts.s.client.Data().Updater().
		WithMerge().
		WithClassName(className).
		WithID(id).
		WithProperties(map[string]interface{}{"tags": tags})
	if ts.t != "" {
		updater = updater.WithTenant(ts.t)
	}
	if err := updater.Do(ctx); err != nil {
		return fmt.Errorf("update tags %s/%s: %w", className, id, err)
	}
	return nil
}

// chunkIDsOf returns the ids of every chunk of a memory (filter on the
// field-tokenized memoryId, so exact).
func (ts *TenantStore) chunkIDsOf(ctx context.Context, memoryID string) ([]string, error) {
	q := ts.s.client.Experimental().Search().
		WithCollection(memory.ChunkClassName).
		WithMetadata(&graphql.Metadata{ID: true}).
		WithWhere(filters.Where().WithPath([]string{"memoryId"}).WithOperator(filters.Equal).WithValueText(memoryID)).
		WithLimit(AllLimit)
	if ts.t != "" {
		q = q.WithTenant(ts.t)
	}
	res, err := q.Do(ctx)
	if err != nil {
		if isTenantNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list chunks of %s: %w", memoryID, err)
	}
	ids := make([]string, 0, len(res))
	for _, r := range res {
		ids = append(ids, r.ID)
	}
	return ids, nil
}
