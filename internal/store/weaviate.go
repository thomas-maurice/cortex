// Package store is the Weaviate-backed vector store for memories. It owns the
// schema and all read/write access to the vector DB.
package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	wgrpc "github.com/weaviate/weaviate-go-client/v5/weaviate/grpc"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// Store wraps the Weaviate client.
type Store struct {
	client *weaviate.Client
}

// SearchOpts configures a nearVector search. The zero value searches the
// default-resolved namespace with no relevance cutoff.
type SearchOpts struct {
	Namespace      string   // exact namespace; "" = all namespaces
	ConversationID string   // exact conversationId; "" = any conversation
	Limit          int      // max results (default 5)
	MaxDistance    float32  // server-side cutoff; <=0 disables (drop hits farther than this)
	Autocut        int      // Weaviate autocut jumps; <=0 disables
	IncludeTags    []string // memory must carry ALL of these tags (server-side)
	AnyTags        []string // memory must carry AT LEAST ONE of these tags (server-side)
	ExcludeTags    []string // drop memories carrying ANY of these tags (post-filter)
}

// ListOpts configures a non-vector listing.
type ListOpts struct {
	Namespace      string
	ConversationID string // exact conversationId; "" = any conversation
	Limit          int
	IncludeTags    []string
	AnyTags        []string
	ExcludeTags    []string
}

// SummarySearchOpts configures a nearVector search over conversation summaries.
type SummarySearchOpts struct {
	Namespace   string  // exact namespace; "" = all namespaces
	Limit       int     // max results (default 5)
	MaxDistance float32 // server-side cutoff; <=0 disables
}

// SummaryListOpts configures a non-vector listing of conversation summaries.
type SummaryListOpts struct {
	Namespace string
	Limit     int
}

// New connects to Weaviate. restHost is "host:port" for schema/data ops (e.g.
// "localhost:8080"); grpcHost is "host:port" for the gRPC query API (e.g.
// "localhost:50051"). Queries (Search/List/Count) go over gRPC; GraphQL is not
// used.
func New(restHost, grpcHost string) (*Store, error) {
	client, err := weaviate.NewClient(weaviate.Config{
		Host:       restHost,
		Scheme:     "http",
		GrpcConfig: &wgrpc.Config{Host: grpcHost},
	})
	if err != nil {
		return nil, fmt.Errorf("weaviate client: %w", err)
	}
	return &Store{client: client}, nil
}

// EnsureSchema creates the Memory and ConversationSummary classes if absent, or
// additively brings existing ones up to the current property set.
func (s *Store) EnsureSchema(ctx context.Context) error {
	if err := s.ensureClass(ctx, memoryClass(), memoryProperties()); err != nil {
		return err
	}
	if err := s.ensureClass(ctx, summaryClass(), summaryProperties()); err != nil {
		return err
	}
	return nil
}

// ensureClass creates the class if it is absent, else adds any missing
// properties (non-destructive), so upgrades don't require a wipe.
func (s *Store) ensureClass(ctx context.Context, class *models.Class, props []*models.Property) error {
	exists, err := s.client.Schema().ClassExistenceChecker().
		WithClassName(class.Class).Do(ctx)
	if err != nil {
		return fmt.Errorf("check class %s: %w", class.Class, err)
	}
	if !exists {
		if err := s.client.Schema().ClassCreator().WithClass(class).Do(ctx); err != nil {
			return fmt.Errorf("create class %s: %w", class.Class, err)
		}
		return nil
	}
	return s.ensureProps(ctx, class.Class, props)
}

// memoryClass is the authoritative Memory class definition.
//
// Vectorization invariant: Vectorizer is "none" and the class enables no vector
// module. Weaviate therefore never embeds anything itself — the ONLY vector an
// object ever has is the one the worker supplies, computed from rec.Text alone
// (see cmd/worker). Consequently a nearVector search matches purely on memory
// text; id, namespace, tags, source, etc. are filterable/sortable metadata that
// can never participate in semantic similarity. Do not set a vectorizer module
// here without also restricting it to the `text` property, or metadata will
// start polluting the vector space. TestMemoryClassVectorizerNone pins this.
func memoryClass() *models.Class {
	return &models.Class{
		Class:       memory.ClassName,
		Description: "A single stored memory in the Cortex second brain",
		Vectorizer:  "none",
		Properties:  memoryProperties(),
	}
}

// memoryProperties is the full property set for the Memory class.
func memoryProperties() []*models.Property {
	return []*models.Property{
		{Name: "text", DataType: []string{"text"}},
		{Name: "namespace", DataType: []string{"text"}},
		{Name: "tags", DataType: []string{"text[]"}},
		{Name: "source", DataType: []string{"text"}},
		{Name: "createdAt", DataType: []string{"date"}},
		{Name: "model", DataType: []string{"text"}},
		{Name: "dims", DataType: []string{"int"}},
		{Name: "conversationId", DataType: []string{"text"}},
		{Name: "linkedIds", DataType: []string{"text[]"}},
		{Name: "dupCandidates", DataType: []string{"text[]"}},
		{Name: "notDuplicateOf", DataType: []string{"text[]"}},
	}
}

// summaryClass is the authoritative ConversationSummary class definition. Same
// vectorization invariant as memoryClass: only `text` is ever embedded.
func summaryClass() *models.Class {
	return &models.Class{
		Class:       memory.SummaryClassName,
		Description: "An ever-current digest of one conversation, unique per conversationId",
		Vectorizer:  "none",
		Properties:  summaryProperties(),
	}
}

// summaryProperties is the full property set for the ConversationSummary class.
func summaryProperties() []*models.Property {
	return []*models.Property{
		{Name: "text", DataType: []string{"text"}},
		{Name: "conversationId", DataType: []string{"text"}},
		{Name: "namespace", DataType: []string{"text"}},
		{Name: "source", DataType: []string{"text"}},
		{Name: "createdAt", DataType: []string{"date"}},
		{Name: "updatedAt", DataType: []string{"date"}},
		{Name: "model", DataType: []string{"text"}},
		{Name: "dims", DataType: []string{"int"}},
	}
}

// ensureProps adds any property missing from an existing class.
func (s *Store) ensureProps(ctx context.Context, className string, props []*models.Property) error {
	class, err := s.client.Schema().ClassGetter().
		WithClassName(className).Do(ctx)
	if err != nil {
		return fmt.Errorf("get class %s: %w", className, err)
	}
	have := make(map[string]bool, len(class.Properties))
	for _, p := range class.Properties {
		have[p.Name] = true
	}
	for _, p := range props {
		if have[p.Name] {
			continue
		}
		if err := s.client.Schema().PropertyCreator().
			WithClassName(className).WithProperty(p).Do(ctx); err != nil {
			return fmt.Errorf("add property %q to %s: %w", p.Name, className, err)
		}
	}
	return nil
}

// DeleteClass drops the Memory AND ConversationSummary classes and all their
// objects. Used by reindex when a model change alters the vector dimension and
// the classes must be rebuilt — both are dropped so their stored vectors stay
// dimension-consistent. Facts are then republished by reindex; summaries start
// empty and are rebuilt as the agent re-summarises.
func (s *Store) DeleteClass(ctx context.Context) error {
	for _, name := range []string{memory.ClassName, memory.SummaryClassName} {
		if err := s.client.Schema().ClassDeleter().WithClassName(name).Do(ctx); err != nil {
			return fmt.Errorf("delete class %s: %w", name, err)
		}
	}
	return nil
}

// Upsert writes a record with its precomputed vector into the Memory class.
func (s *Store) Upsert(ctx context.Context, rec memory.Record, vector []float32) error {
	return s.upsertObject(ctx, memory.ClassName, rec.ID, map[string]interface{}{
		"text":           rec.Text,
		"namespace":      rec.Namespace,
		"tags":           rec.Tags,
		"source":         rec.Source,
		"createdAt":      rec.CreatedAt.UTC().Format(time.RFC3339),
		"model":          rec.Model,
		"dims":           rec.Dims,
		"conversationId": rec.ConversationID,
		"linkedIds":      rec.LinkedIDs,
		"dupCandidates":  rec.DupCandidates,
		"notDuplicateOf": rec.NotDuplicateOf,
	}, vector)
}

// UpsertSummary writes a conversation summary into the ConversationSummary
// class, keyed by the deterministic SummaryID so each conversation has exactly
// one, ever-current summary. Re-saving the same conversation replaces it.
func (s *Store) UpsertSummary(ctx context.Context, sum memory.Summary, vector []float32) error {
	return s.upsertObject(ctx, memory.SummaryClassName, memory.SummaryID(sum.ConversationID), map[string]interface{}{
		"text":           sum.Text,
		"conversationId": sum.ConversationID,
		"namespace":      sum.Namespace,
		"source":         sum.Source,
		"createdAt":      sum.CreatedAt.UTC().Format(time.RFC3339),
		"updatedAt":      sum.UpdatedAt.UTC().Format(time.RFC3339),
		"model":          sum.Model,
		"dims":           sum.Dims,
	}, vector)
}

// upsertObject writes props+vector under id in className. It is create-or-replace
// with NO check-then-act window, so it is safe under CONCURRENT workers (multiple
// JetStream consumers): it attempts a create, and on ANY failure — most commonly
// the object already existing (a concurrent worker, redelivery, reindex, or a
// summary update) — it replaces the object via PUT. Two workers racing to create
// the same id can't both "win the exists-check": the loser's create fails and it
// falls through to the idempotent PUT. A genuine non-"already exists" error still
// surfaces, because the PUT then fails too and its error is returned. PUT replaces
// the whole object (no WithMerge), refreshing vector + props.
func (s *Store) upsertObject(ctx context.Context, className, id string, props map[string]interface{}, vector []float32) error {
	_, err := s.client.Data().Creator().
		WithClassName(className).
		WithID(id).
		WithProperties(props).
		WithVector(vector).
		Do(ctx)
	if err == nil {
		return nil
	}
	if uerr := s.client.Data().Updater().
		WithClassName(className).
		WithID(id).
		WithProperties(props).
		WithVector(vector).
		Do(ctx); uerr != nil {
		return fmt.Errorf("upsert object %s/%s (create failed: %v): %w", className, id, err, uerr)
	}
	return nil
}

// Delete removes a memory by ID.
func (s *Store) Delete(ctx context.Context, id string) error {
	err := s.client.Data().Deleter().
		WithClassName(memory.ClassName).
		WithID(id).Do(ctx)
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	return nil
}

// Get fetches a single memory by ID over REST. found is false if it does not
// exist. Used by Link/Unlink to read the current link set before updating it.
func (s *Store) Get(ctx context.Context, id string) (memory.Record, bool, error) {
	exists, err := s.client.Data().Checker().
		WithClassName(memory.ClassName).WithID(id).Do(ctx)
	if err != nil {
		return memory.Record{}, false, fmt.Errorf("check object: %w", err)
	}
	if !exists {
		return memory.Record{}, false, nil
	}
	objs, err := s.client.Data().ObjectsGetter().
		WithClassName(memory.ClassName).WithID(id).Do(ctx)
	if err != nil {
		return memory.Record{}, false, fmt.Errorf("get object: %w", err)
	}
	if len(objs) == 0 {
		return memory.Record{}, false, nil
	}
	return objectToRecord(id, objs[0].Properties), true, nil
}

// SetLinks replaces a memory's linkedIds via a Weaviate merge (PATCH), leaving
// the vector and all other properties untouched — links never trigger a
// re-embed.
func (s *Store) SetLinks(ctx context.Context, id string, links []string) error {
	if links == nil {
		links = []string{}
	}
	err := s.client.Data().Updater().
		WithMerge().
		WithClassName(memory.ClassName).
		WithID(id).
		WithProperties(map[string]interface{}{"linkedIds": links}).
		Do(ctx)
	if err != nil {
		return fmt.Errorf("set links: %w", err)
	}
	return nil
}

// SetNotDuplicateOf replaces a memory's notDuplicateOf list via a Weaviate merge
// (PATCH), leaving the vector and all other properties untouched. Used by
// DismissDuplicate to record the bidirectional "confirmed not a duplicate"
// decision that the worker consults when recomputing candidates.
func (s *Store) SetNotDuplicateOf(ctx context.Context, id string, ids []string) error {
	if ids == nil {
		ids = []string{}
	}
	err := s.client.Data().Updater().
		WithMerge().
		WithClassName(memory.ClassName).
		WithID(id).
		WithProperties(map[string]interface{}{"notDuplicateOf": ids}).
		Do(ctx)
	if err != nil {
		return fmt.Errorf("set notDuplicateOf: %w", err)
	}
	return nil
}

// SetDupCandidates replaces a memory's dupCandidates list via a Weaviate merge
// (PATCH), leaving the vector and other properties untouched. Used by
// DismissDuplicate to drop a now-adjudicated pair from the review list
// immediately, without waiting for the next reindex to recompute it.
func (s *Store) SetDupCandidates(ctx context.Context, id string, ids []string) error {
	if ids == nil {
		ids = []string{}
	}
	err := s.client.Data().Updater().
		WithMerge().
		WithClassName(memory.ClassName).
		WithID(id).
		WithProperties(map[string]interface{}{"dupCandidates": ids}).
		Do(ctx)
	if err != nil {
		return fmt.Errorf("set dupCandidates: %w", err)
	}
	return nil
}

// Ready reports whether Weaviate is up and serving. Used by Status/Doctor.
func (s *Store) Ready(ctx context.Context) error {
	ok, err := s.client.Misc().ReadyChecker().Do(ctx)
	if err != nil {
		return fmt.Errorf("weaviate ready: %w", err)
	}
	if !ok {
		return fmt.Errorf("weaviate not ready")
	}
	return nil
}

// allCount caps the Count scan. Weaviate's default QUERY_MAXIMUM_RESULTS is
// 10000; a personal store stays well under it.
const allCount = 10000

// Count returns the number of stored memories, optionally scoped to a namespace
// ("" counts all). It runs an id-only gRPC query and counts the rows — there is
// no gRPC Aggregate in the stable client, and a personal store is small enough
// that scanning ids is cheap.
func (s *Store) Count(ctx context.Context, namespace string) (int, error) {
	q := s.client.Experimental().Search().
		WithCollection(memory.ClassName).
		WithLimit(allCount).
		WithMetadata(&graphql.Metadata{ID: true})
	if where := buildWhere(namespace, "", nil, nil); where != nil {
		q = q.WithWhere(where)
	}
	res, err := q.Do(ctx)
	if err != nil {
		return 0, fmt.Errorf("count query: %w", err)
	}
	return len(res), nil
}

// memoryProps is the property set fetched for both search and list.
var memoryProps = []string{"text", "namespace", "tags", "source", "createdAt", "model", "dims", "conversationId", "linkedIds", "dupCandidates", "notDuplicateOf"}

// buildWhere combines exact namespace + conversationId filters with tag
// filters: includeTags ("has ALL of these", ContainsAll) and anyTags ("has at
// least ONE of these", ContainsAny). When both are given they are ANDed, so a
// memory must carry every includeTag AND at least one anyTag. Returns nil when
// nothing constrains the query.
func buildWhere(namespace, conversationID string, includeTags, anyTags []string) *filters.WhereBuilder {
	var ops []*filters.WhereBuilder
	if namespace != "" {
		ops = append(ops, filters.Where().
			WithPath([]string{"namespace"}).
			WithOperator(filters.Equal).
			WithValueText(namespace))
	}
	if conversationID != "" {
		ops = append(ops, filters.Where().
			WithPath([]string{"conversationId"}).
			WithOperator(filters.Equal).
			WithValueText(conversationID))
	}
	if len(includeTags) > 0 {
		ops = append(ops, filters.Where().
			WithPath([]string{"tags"}).
			WithOperator(filters.ContainsAll).
			WithValueText(includeTags...))
	}
	if len(anyTags) > 0 {
		ops = append(ops, filters.Where().
			WithPath([]string{"tags"}).
			WithOperator(filters.ContainsAny).
			WithValueText(anyTags...))
	}
	switch len(ops) {
	case 0:
		return nil
	case 1:
		return ops[0]
	default:
		return filters.Where().WithOperator(filters.And).WithOperands(ops)
	}
}

// excludeTagged drops records carrying any of the excluded tags. Done in Go
// because Weaviate's where filter has no clean array-negation operator.
func excludeTagged[T any](items []T, tagsOf func(T) []string, exclude []string) []T {
	if len(exclude) == 0 {
		return items
	}
	bad := make(map[string]bool, len(exclude))
	for _, t := range exclude {
		bad[t] = true
	}
	out := items[:0]
	for _, it := range items {
		drop := false
		for _, t := range tagsOf(it) {
			if bad[t] {
				drop = true
				break
			}
		}
		if !drop {
			out = append(out, it)
		}
	}
	return out
}

// Search runs a nearVector query (over gRPC) with optional namespace, tag, and
// relevance filtering. ExcludeTags is applied after the query, so it can reduce
// the returned count below Limit.
func (s *Store) Search(ctx context.Context, vector []float32, opts SearchOpts) ([]memory.Hit, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 5
	}

	nearVector := (&graphql.NearVectorArgumentBuilder{}).WithVector(vector)
	if opts.MaxDistance > 0 {
		nearVector = nearVector.WithDistance(opts.MaxDistance)
	}

	query := s.client.Experimental().Search().
		WithCollection(memory.ClassName).
		WithProperties(memoryProps...).
		WithMetadata(&graphql.Metadata{ID: true, Distance: true}).
		WithNearVector(nearVector).
		WithLimit(limit)

	if opts.Autocut > 0 {
		query = query.WithAutocut(opts.Autocut)
	}
	if where := buildWhere(opts.Namespace, opts.ConversationID, opts.IncludeTags, opts.AnyTags); where != nil {
		query = query.WithWhere(where)
	}

	res, err := query.Do(ctx)
	if err != nil {
		return nil, searchError(err, len(vector))
	}

	hits := make([]memory.Hit, 0, len(res))
	for _, r := range res {
		hits = append(hits, memory.Hit{Record: resultToRecord(r), Distance: r.Metadata.Distance})
	}
	return excludeTagged(hits, func(h memory.Hit) []string { return h.Tags }, opts.ExcludeTags), nil
}

// List returns stored memories newest-first with optional namespace/tag
// filtering. Unlike Search it runs no vector query (still over gRPC), so results
// carry no distance.
func (s *Store) List(ctx context.Context, opts ListOpts) ([]memory.Record, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 20
	}

	query := s.client.Experimental().Search().
		WithCollection(memory.ClassName).
		WithProperties(memoryProps...).
		WithMetadata(&graphql.Metadata{ID: true}).
		WithSort(graphql.Sort{Path: []string{"createdAt"}, Order: graphql.Desc}).
		WithLimit(limit)

	if where := buildWhere(opts.Namespace, opts.ConversationID, opts.IncludeTags, opts.AnyTags); where != nil {
		query = query.WithWhere(where)
	}

	res, err := query.Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("list query: %w", err)
	}

	recs := make([]memory.Record, 0, len(res))
	for _, r := range res {
		recs = append(recs, resultToRecord(r))
	}
	return excludeTagged(recs, func(r memory.Record) []string { return r.Tags }, opts.ExcludeTags), nil
}

// ListWithCandidates returns memories that the worker flagged with at least one
// duplicate candidate, newest-first. It is the read side of the heuristic dedup:
// the review tool / UI surface these so a human or the agent can adjudicate.
//
// It scans the namespace and filters for a non-empty dupCandidates in Go rather
// than with a Weaviate IsNull filter, which would require the class to be created
// with indexNullState=true (not the case for existing deployments). A personal
// store is small enough that scanning is cheap — the same assumption Count and
// reindex already rely on. No vector query, so results carry no distance.
func (s *Store) ListWithCandidates(ctx context.Context, namespace string, limit int) ([]memory.Record, error) {
	if limit <= 0 {
		limit = 50
	}

	query := s.client.Experimental().Search().
		WithCollection(memory.ClassName).
		WithProperties(memoryProps...).
		WithMetadata(&graphql.Metadata{ID: true}).
		WithSort(graphql.Sort{Path: []string{"createdAt"}, Order: graphql.Desc}).
		WithLimit(allCount)
	if where := buildWhere(namespace, "", nil, nil); where != nil {
		query = query.WithWhere(where)
	}

	res, err := query.Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("list candidates query: %w", err)
	}

	recs := make([]memory.Record, 0)
	for _, r := range res {
		rec := resultToRecord(r)
		if len(rec.DupCandidates) == 0 {
			continue
		}
		recs = append(recs, rec)
		if len(recs) >= limit {
			break
		}
	}
	return recs, nil
}

// summaryProps is the property set fetched when reading conversation summaries.
var summaryProps = []string{"text", "conversationId", "namespace", "source", "createdAt", "updatedAt", "model", "dims"}

// SearchSummaries runs a nearVector query (over gRPC) over conversation
// summaries, newest match first by relevance. Used as the first hop of session
// recall: a hit yields a conversationId to fan out to its facts.
func (s *Store) SearchSummaries(ctx context.Context, vector []float32, opts SummarySearchOpts) ([]memory.SummaryHit, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 5
	}

	nearVector := (&graphql.NearVectorArgumentBuilder{}).WithVector(vector)
	if opts.MaxDistance > 0 {
		nearVector = nearVector.WithDistance(opts.MaxDistance)
	}

	query := s.client.Experimental().Search().
		WithCollection(memory.SummaryClassName).
		WithProperties(summaryProps...).
		WithMetadata(&graphql.Metadata{ID: true, Distance: true}).
		WithNearVector(nearVector).
		WithLimit(limit)

	if where := buildWhere(opts.Namespace, "", nil, nil); where != nil {
		query = query.WithWhere(where)
	}

	res, err := query.Do(ctx)
	if err != nil {
		return nil, searchError(err, len(vector))
	}

	hits := make([]memory.SummaryHit, 0, len(res))
	for _, r := range res {
		hits = append(hits, memory.SummaryHit{Summary: resultToSummary(r), Distance: r.Metadata.Distance})
	}
	return hits, nil
}

// ListSummaries returns stored conversation summaries, most-recently-updated
// first, with optional namespace filtering. No vector query (still over gRPC).
func (s *Store) ListSummaries(ctx context.Context, opts SummaryListOpts) ([]memory.Summary, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 20
	}

	query := s.client.Experimental().Search().
		WithCollection(memory.SummaryClassName).
		WithProperties(summaryProps...).
		WithMetadata(&graphql.Metadata{ID: true}).
		WithSort(graphql.Sort{Path: []string{"updatedAt"}, Order: graphql.Desc}).
		WithLimit(limit)

	if where := buildWhere(opts.Namespace, "", nil, nil); where != nil {
		query = query.WithWhere(where)
	}

	res, err := query.Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("list summaries query: %w", err)
	}

	sums := make([]memory.Summary, 0, len(res))
	for _, r := range res {
		sums = append(sums, resultToSummary(r))
	}
	return sums, nil
}

// searchError turns Weaviate's opaque query errors into actionable ones. The
// most common operational failure is a query/store embedding-dimension mismatch
// after a model change — surface it loudly with the fix.
func searchError(err error, queryDims int) error {
	if strings.Contains(err.Error(), "vector lengths don't match") {
		return fmt.Errorf("query was embedded to %d dims but the stored memories use a different dimension — "+
			"the search model differs from the model the worker indexed with (set OLLAMA_MODEL / --model to match, or run `cortex reindex`): %w", queryDims, err)
	}
	return fmt.Errorf("search query: %w", err)
}

// resultToRecord decodes one gRPC search result into a memory.Record. Property
// values arrive as the loosely-typed gRPC value map (text → string, text[] →
// *pb.ListValue, int → int64, date → RFC3339 string).
func resultToRecord(r graphql.SearchResult) memory.Record {
	p := r.Properties
	rec := memory.Record{
		ID:             r.ID,
		Text:           propString(p, "text"),
		Namespace:      propString(p, "namespace"),
		Tags:           propStrings(p, "tags"),
		Source:         propString(p, "source"),
		Model:          propString(p, "model"),
		Dims:           propInt(p, "dims"),
		ConversationID: propString(p, "conversationId"),
		LinkedIDs:      propStrings(p, "linkedIds"),
		DupCandidates:  propStrings(p, "dupCandidates"),
		NotDuplicateOf: propStrings(p, "notDuplicateOf"),
	}
	if ca := propString(p, "createdAt"); ca != "" {
		rec.CreatedAt, _ = time.Parse(time.RFC3339, ca)
	}
	return rec
}

// resultToSummary decodes one gRPC search result into a memory.Summary.
func resultToSummary(r graphql.SearchResult) memory.Summary {
	p := r.Properties
	sum := memory.Summary{
		ConversationID: propString(p, "conversationId"),
		Text:           propString(p, "text"),
		Namespace:      propString(p, "namespace"),
		Source:         propString(p, "source"),
		Model:          propString(p, "model"),
		Dims:           propInt(p, "dims"),
	}
	if ca := propString(p, "createdAt"); ca != "" {
		sum.CreatedAt, _ = time.Parse(time.RFC3339, ca)
	}
	if ua := propString(p, "updatedAt"); ua != "" {
		sum.UpdatedAt, _ = time.Parse(time.RFC3339, ua)
	}
	return sum
}

// objectToRecord decodes a REST-fetched object (Data().ObjectsGetter) into a
// memory.Record. REST property values are typed differently from the gRPC search
// path: text[] arrives as []interface{} of strings and int as float64.
func objectToRecord(id string, raw models.PropertySchema) memory.Record {
	p, _ := raw.(map[string]interface{})
	rec := memory.Record{
		ID:             id,
		Text:           restString(p, "text"),
		Namespace:      restString(p, "namespace"),
		Tags:           restStrings(p, "tags"),
		Source:         restString(p, "source"),
		Model:          restString(p, "model"),
		Dims:           restInt(p, "dims"),
		ConversationID: restString(p, "conversationId"),
		LinkedIDs:      restStrings(p, "linkedIds"),
		DupCandidates:  restStrings(p, "dupCandidates"),
		NotDuplicateOf: restStrings(p, "notDuplicateOf"),
	}
	if ca := restString(p, "createdAt"); ca != "" {
		rec.CreatedAt, _ = time.Parse(time.RFC3339, ca)
	}
	return rec
}

func restString(p map[string]interface{}, key string) string {
	s, _ := p[key].(string)
	return s
}

func restInt(p map[string]interface{}, key string) int {
	if f, ok := p[key].(float64); ok {
		return int(f)
	}
	return 0
}

func restStrings(p map[string]interface{}, key string) []string {
	arr, _ := p[key].([]interface{})
	out := make([]string, 0, len(arr))
	for _, v := range arr {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func propString(p map[string]any, key string) string {
	s, _ := p[key].(string)
	return s
}

func propInt(p map[string]any, key string) int {
	switch v := p[key].(type) {
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

// propStrings decodes a text[] property, which the gRPC client surfaces as a
// *pb.ListValue.
func propStrings(p map[string]any, key string) []string {
	lv, ok := p[key].(*pb.ListValue)
	if !ok || lv == nil {
		return nil
	}
	return lv.GetTextValues().GetValues()
}
