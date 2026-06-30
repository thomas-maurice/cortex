package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// ChunkVec is one chunk's text plus the vector the worker computed for it. The
// store does no embedding; the worker passes these in.
type ChunkVec struct {
	Index  int
	Text   string
	Vector []float32
}

// chunkClass is the authoritative MemoryChunk class definition. Same vectorization
// invariant as Memory: vectorizer "none", so the ONLY vector a chunk has is the
// one the worker supplies (computed from the chunk text alone). The primary vector
// search runs against THIS class; hits are resolved back to their parent Memory.
func chunkClass(multiTenant bool) *models.Class {
	c := &models.Class{
		Class:       memory.ChunkClassName,
		Description: "A token-bounded, overlapping slice of a Memory's text, indexed for retrieval",
		Vectorizer:  "none",
		Properties:  chunkProperties(),
	}
	if multiTenant {
		c.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled:            true,
			AutoTenantCreation: true,
		}
	}
	return c
}

// chunkProperties is the full property set for the MemoryChunk class. namespace and
// tags are COPIED from the parent so namespace/tag filters can be pushed down to
// the chunk query; memoryId resolves a hit back to its parent Memory.
func chunkProperties() []*models.Property {
	return []*models.Property{
		{Name: "text", DataType: []string{"text"}},
		// memoryId and namespace are exact-match filter keys (parent resolution,
		// namespace scoping, cascade deletes), so they use "field" tokenization —
		// the default "word" tokenization splits UUIDs on hyphens and namespaces on
		// punctuation, which would match the wrong objects. See memoryProperties.
		{Name: "memoryId", DataType: []string{"text"}, Tokenization: "field"},
		{Name: "namespace", DataType: []string{"text"}, Tokenization: "field"},
		{Name: "tags", DataType: []string{"text[]"}},
		{Name: "chunkIndex", DataType: []string{"int"}},
		{Name: "createdAt", DataType: []string{"date"}},
		{Name: "model", DataType: []string{"text"}},
		{Name: "dims", DataType: []string{"int"}},
	}
}

// ReplaceChunks makes a memory's chunk set exactly match `chunks`: it first DROPS
// every chunk already stored for the memory (by parent id), then writes the new
// set. Delete-all-by-parent-then-write — rather than overwrite-in-place — means a
// re-import whose text changed (different chunk count AND content) can never leave
// an orphaned chunk behind, and it stays correct even if the chunk-id scheme ever
// changes. Passing an empty slice therefore PURGES all of a memory's chunks (used
// when chunking is disabled). The brief window with no chunks is invisible to
// callers because Search falls back to the whole-memory vector for chunkless
// memories. Chunk ids are still deterministic so a mid-write retry overwrites
// cleanly rather than duplicating.
func (ts *TenantStore) ReplaceChunks(ctx context.Context, rec memory.Record, chunks []ChunkVec) error {
	if err := ts.deleteChunksByMemory(ctx, rec.ID); err != nil {
		return err
	}
	for _, c := range chunks {
		props := map[string]interface{}{
			"text":       c.Text,
			"memoryId":   rec.ID,
			"namespace":  rec.Namespace,
			"tags":       rec.Tags,
			"chunkIndex": c.Index,
			"model":      rec.Model,
			"dims":       rec.Dims,
		}
		if !rec.CreatedAt.IsZero() {
			props["createdAt"] = rec.CreatedAt.UTC().Format(time.RFC3339)
		}
		if err := ts.upsertObject(ctx, memory.ChunkClassName, memory.ChunkID(rec.ID, c.Index), props, c.Vector); err != nil {
			return fmt.Errorf("upsert chunk %d of %s: %w", c.Index, rec.ID, err)
		}
	}
	return nil
}

// deleteChunksByMemory removes ALL chunks belonging to a memory. Used by
// ReplaceChunks (drop-then-write) and when the parent memory is deleted.
func (ts *TenantStore) deleteChunksByMemory(ctx context.Context, memoryID string) error {
	deleter := ts.s.client.Batch().ObjectsBatchDeleter().
		WithClassName(memory.ChunkClassName).
		WithWhere(filters.Where().WithPath([]string{"memoryId"}).WithOperator(filters.Equal).WithValueText(memoryID))
	if ts.t != "" {
		deleter = deleter.WithTenant(ts.t)
	}
	if _, err := deleter.Do(ctx); err != nil {
		// A brand-new MT tenant has never been written to, so AutoTenantCreation
		// hasn't fired yet. Weaviate returns 422 "tenant not found" on deletes
		// against such a tenant — which is semantically empty, so this is a no-op.
		if isTenantNotFound(err) {
			return nil
		}
		return fmt.Errorf("delete chunks of %s: %w", memoryID, err)
	}
	return nil
}

// chunkPoolFactor is how many chunk candidates to fetch per requested result, so
// that grouping chunks back to DISTINCT parent memories still yields `limit`
// parents even when several top chunks share one parent.
const chunkPoolFactor = 10

// Search runs the primary retrieval: a vector (or hybrid BM25+vector) query over
// MemoryChunk, then groups the chunk hits by parent memory (keeping each parent's
// BEST chunk distance) and resolves them to full Memory records. This is what lets
// a specific fact buried in a long memory surface — it is matched at the focused
// chunk level, not against the whole document's averaged vector. namespace/tag
// filters are pushed down to chunks (which carry the parent's namespace+tags);
// excludeTags and living-memory re-ranking are applied on the resolved parents.
func (ts *TenantStore) Search(ctx context.Context, vector []float32, opts SearchOpts) ([]memory.Hit, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 5
	}

	fetch := limit * chunkPoolFactor
	rerank := opts.RerankWeight > 0
	if rerank {
		fetch *= 2
	}
	if fetch < 50 {
		fetch = 50
	}

	query := ts.s.client.Experimental().Search().
		WithCollection(memory.ChunkClassName).
		WithProperties("memoryId").
		WithLimit(fetch)
	if ts.t != "" {
		query = query.WithTenant(ts.t)
	}

	hybrid := strings.TrimSpace(opts.Query) != ""
	if hybrid {
		h := (&graphql.HybridArgumentBuilder{}).
			WithQuery(opts.Query).
			WithVector(vector).
			WithProperties([]string{"text"}).
			WithFusionType(graphql.RelativeScore)
		if opts.Alpha > 0 {
			h = h.WithAlpha(opts.Alpha)
		}
		query = query.WithHybrid(h).WithMetadata(&graphql.Metadata{ID: true, Score: true})
	} else {
		nearVector := (&graphql.NearVectorArgumentBuilder{}).WithVector(vector)
		if opts.MaxDistance > 0 {
			nearVector = nearVector.WithDistance(opts.MaxDistance)
		}
		query = query.WithNearVector(nearVector).WithMetadata(&graphql.Metadata{ID: true, Distance: true})
	}
	if opts.Autocut > 0 {
		query = query.WithAutocut(opts.Autocut)
	}
	if where := buildWhere(opts.Namespace, "", opts.IncludeTags, opts.AnyTags); where != nil {
		query = query.WithWhere(where)
	}

	res, err := query.Do(ctx)
	if err != nil {
		if isTenantNotFound(err) {
			return nil, nil
		}
		return nil, searchError(err, len(vector))
	}

	// Weaviate returns chunk hits best-first, so the FIRST time a parent appears is
	// at its best distance — record that and the parent's order.
	bestDist := make(map[string]float32, len(res))
	order := make([]string, 0, len(res))
	for _, r := range res {
		dist := r.Metadata.Distance
		if hybrid {
			dist = hybridDistance(r.Metadata.Score)
			if opts.MaxDistance > 0 && dist > opts.MaxDistance {
				continue
			}
		}
		mid := propString(r.Properties, "memoryId")
		if mid == "" {
			continue
		}
		if _, seen := bestDist[mid]; !seen {
			bestDist[mid] = dist
			order = append(order, mid)
		}
	}

	// Resolve parents to full records. Overfetch parents when re-ranking so a
	// high-usage memory just outside the top `limit` by relevance can still float in.
	want := limit
	if rerank {
		want = limit * rerankOverfetch
	}
	hits := make([]memory.Hit, 0, want)
	for _, mid := range order {
		rec, found, err := ts.Get(ctx, mid)
		if err != nil {
			return nil, fmt.Errorf("resolve chunk parent %s: %w", mid, err)
		}
		if !found {
			continue // parent deleted since its chunk was indexed
		}
		hits = append(hits, memory.Hit{Record: rec, Distance: bestDist[mid]})
		if len(hits) >= want {
			break
		}
	}

	hits = excludeTagged(hits, func(h memory.Hit) []string { return h.Tags }, opts.ExcludeTags)
	if rerank {
		hits = rerankHits(hits, opts.RerankWeight, opts.RerankHalfLifeDays, time.Now())
	}
	if len(hits) > limit {
		hits = hits[:limit]
	}

	// Backward-compat / migration fallback. A memory with NO chunks — a store
	// indexed before chunking existed, or one mid-reindex — is invisible to the
	// chunk query above. When the chunk hits don't fill the page, top up from the
	// whole-memory vectors (which every memory always has) so those memories are
	// still found. A fully-chunked store returns >= limit from chunks and never
	// reaches here, so this costs nothing in steady state; an entirely un-chunked
	// store makes chunk search return nothing and this degrades cleanly to the
	// original whole-memory search.
	if len(hits) < limit {
		hits = ts.fillFromMemoryVectors(ctx, vector, opts, hits, limit)
	}
	return hits, nil
}

// fillFromMemoryVectors appends whole-memory matches not already present in hits,
// up to limit. It is the un-chunked-memory fallback for Search and is strictly
// best-effort: a fallback error is swallowed (the chunk hits already found stand),
// never failing the search.
func (ts *TenantStore) fillFromMemoryVectors(ctx context.Context, vector []float32, opts SearchOpts, hits []memory.Hit, limit int) []memory.Hit {
	seen := make(map[string]bool, len(hits))
	for _, h := range hits {
		seen[h.ID] = true
	}
	memOpts := opts
	memOpts.Limit = limit
	memOpts.RerankWeight = 0 // the appended tail just fills gaps; don't re-rank it
	memHits, err := ts.SearchMemoryVectors(ctx, vector, memOpts)
	if err != nil {
		return hits
	}
	for _, mh := range memHits {
		if seen[mh.ID] {
			continue
		}
		hits = append(hits, mh)
		seen[mh.ID] = true
		if len(hits) >= limit {
			break
		}
	}
	return hits
}
