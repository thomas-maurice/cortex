// Package store is the Weaviate-backed vector store for memories. It owns the
// schema and all read/write access to the vector DB.
package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/fault"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	wgrpc "github.com/weaviate/weaviate-go-client/v5/weaviate/grpc"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// Store wraps the Weaviate client.
type Store struct {
	client      *weaviate.Client
	multiTenant bool // set via SetMultiTenant; governs MT schema and .WithTenant calls
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
	// IncludeExpired keeps memories whose validUntil has passed. The zero value
	// (false) hides them, so every caller is expiry-safe by default; only the
	// worker's dedup scan and an explicit include_expired request opt in.
	IncludeExpired bool
	// Query, when non-empty, switches Search from a pure nearVector query to a
	// HYBRID (BM25 keyword + vector) query using this raw text for the keyword
	// side and the supplied vector for the vector side. This is what lets exact
	// tokens that don't embed near their meaning — codenames, hostnames, IDs —
	// still surface. Empty Query keeps the original pure-vector behaviour (used by
	// dedup candidate scans, which have no query text).
	Query string
	// Alpha is the hybrid blend: 1=pure vector, 0=pure keyword. Only used when
	// Query is set; <=0 leaves Weaviate's default.
	Alpha float32
	// RerankWeight enables "living memory" re-ranking: after the relevance query
	// and the maxDistance cutoff, the surviving hits are RE-ORDERED by a blend of
	// relevance and usage (recency-decayed access). 0 disables it entirely (the
	// hits keep pure relevance order and the cutoff/distance are untouched); a
	// value in (0,1] is how much weight usage gets vs relevance. The Distance
	// field of each hit is NEVER modified — only the order changes — so cutoffs,
	// the UI, and clients see the same distance metric as before.
	RerankWeight float32
	// RerankHalfLifeDays is the recency half-life: a memory last accessed this many
	// days ago contributes half the recency it would if accessed just now. Only
	// used when RerankWeight>0; <=0 falls back to a sane default in rerankHits.
	RerankHalfLifeDays float32
}

// ListOpts configures a non-vector listing.
type ListOpts struct {
	Namespace      string
	ConversationID string // exact conversationId; "" = any conversation
	Limit          int
	IncludeTags    []string
	AnyTags        []string
	ExcludeTags    []string
	IncludeExpired bool // keep memories whose validUntil has passed (default: hide them)
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

// SetMultiTenant gates the multi-tenancy code path. Call it before EnsureSchema
// when CORTEX_MULTI_TENANT is on. When false (the default) the store behaves
// identically to the pre-MT code: no .WithTenant calls, non-MT class schema.
func (s *Store) SetMultiTenant(enabled bool) {
	s.multiTenant = enabled
}

// EnsureSchema creates the Memory and ConversationSummary classes if absent, or
// additively brings existing ones up to the current property set. When the Store
// was configured with SetMultiTenant(true) the three memory classes are created
// with MultiTenancyConfig{Enabled:true, AutoTenantCreation:true}. EnsureSchema
// never tries to flip an existing non-MT class to MT — that is detected by
// VerifySchema and points at `cortex migrate-mt`.
func (s *Store) EnsureSchema(ctx context.Context) error {
	if err := s.ensureClass(ctx, memoryClass(s.multiTenant), memoryProperties()); err != nil {
		return err
	}
	if err := s.ensureClass(ctx, chunkClass(s.multiTenant), chunkProperties()); err != nil {
		return err
	}
	if err := s.ensureClass(ctx, summaryClass(s.multiTenant), summaryProperties()); err != nil {
		return err
	}
	if err := s.ensureClass(ctx, trashClass(s.multiTenant), trashProperties()); err != nil {
		return err
	}
	// CortexMeta is single-tenant in every mode and not alias-backed.
	return s.EnsureMetaClass(ctx)
}

// ensureClass brings one logical class up to date. Absent (fresh install): it is
// created as <Name>_v1 behind the alias <Name>. Present, whether an alias or a
// plain pre-alias class (which migration #1 converts): any missing properties are
// added, non-destructively, to the PHYSICAL class. The class's MT config is NOT
// touched — Weaviate forbids flipping MT on an existing class anyway.
func (s *Store) ensureClass(ctx context.Context, class *models.Class, props []*models.Property) error {
	exists, err := s.client.Schema().ClassExistenceChecker().
		WithClassName(class.Class).Do(ctx)
	if err != nil {
		return fmt.Errorf("check class %s: %w", class.Class, err)
	}
	if !exists {
		return s.createAliased(ctx, class)
	}
	phys, err := s.PhysicalClass(ctx, class.Class)
	if err != nil {
		return err
	}
	return s.ensureProps(ctx, phys, props)
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
func memoryClass(multiTenant bool) *models.Class {
	c := &models.Class{
		Class:       memory.ClassName,
		Description: "A single stored memory in the Cortex second brain",
		Vectorizer:  "none",
		Properties:  memoryProperties(),
	}
	if multiTenant {
		c.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled:            true,
			AutoTenantCreation: true,
		}
	}
	return c
}

// memoryProperties is the full property set for the Memory class.
func memoryProperties() []*models.Property {
	return []*models.Property{
		{Name: "text", DataType: []string{"text"}},
		// namespace, source and conversationId are EXACT-MATCH filter keys, so they
		// must use "field" tokenization (whole value = one token). The default
		// "word" tokenization would make `namespace Equal "demo"` also match
		// "demo-2" (shared token) and a `conversationId`/UUID Equal match on the
		// hyphen-split tokens — i.e. fuzzy, wrong scoping. "field" makes them exact.
		{Name: "namespace", DataType: []string{"text"}, Tokenization: "field"},
		{Name: "tags", DataType: []string{"text[]"}, Tokenization: "field"},
		{Name: "source", DataType: []string{"text"}, Tokenization: "field"},
		{Name: "createdAt", DataType: []string{"date"}},
		{Name: "model", DataType: []string{"text"}},
		{Name: "dims", DataType: []string{"int"}},
		{Name: "conversationId", DataType: []string{"text"}, Tokenization: "field"},
		{Name: "linkedIds", DataType: []string{"text[]"}},
		{Name: "dupCandidates", DataType: []string{"text[]"}},
		{Name: "notDuplicateOf", DataType: []string{"text[]"}},
		{Name: "accessCount", DataType: []string{"int"}},
		{Name: "lastAccessedAt", DataType: []string{"date"}},
		// useCount / lastUsedAt: actual-use tracking (RecordUse), the basis of stale
		// review. Server-owned merge fields; the index path never writes them on an
		// existing memory.
		{Name: "useCount", DataType: []string{"int"}},
		{Name: "lastUsedAt", DataType: []string{"date"}},
		// indexVersion is the publish-time stamp (unix microseconds, set by
		// bus.PublishIndex) of the message that last wrote this object: the worker's
		// stale-write guard (an older message must not overwrite a newer one). Absent
		// on pre-existing objects, read as 0.
		{Name: "indexVersion", DataType: []string{"int"}},
		// DEPRECATED, no longer read or written (the old JetStream seq/stream guard,
		// replaced by indexVersion). Kept so a rolled-back binary's VerifySchema stays
		// clean; Weaviate cannot drop a property in place.
		{Name: "indexedSeq", DataType: []string{"int"}},
		{Name: "indexedStream", DataType: []string{"int"}},
		// validUntil is when the memory stops being true (null = never expires).
		// Index-owned. Memories written before this property existed simply have no
		// value, which the expiry filter treats as "never expires" (see notExpired).
		{Name: "validUntil", DataType: []string{"date"}},
	}
}

// summaryClass is the authoritative ConversationSummary class definition. Same
// vectorization invariant as memoryClass: only `text` is ever embedded.
func summaryClass(multiTenant bool) *models.Class {
	c := &models.Class{
		Class:       memory.SummaryClassName,
		Description: "An ever-current digest of one conversation, unique per conversationId",
		Vectorizer:  "none",
		Properties:  summaryProperties(),
	}
	if multiTenant {
		c.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled:            true,
			AutoTenantCreation: true,
		}
	}
	return c
}

// summaryProperties is the full property set for the ConversationSummary class.
func summaryProperties() []*models.Property {
	return []*models.Property{
		{Name: "text", DataType: []string{"text"}},
		{Name: "conversationId", DataType: []string{"text"}, Tokenization: "field"},
		{Name: "namespace", DataType: []string{"text"}, Tokenization: "field"},
		{Name: "source", DataType: []string{"text"}, Tokenization: "field"},
		{Name: "createdAt", DataType: []string{"date"}},
		{Name: "updatedAt", DataType: []string{"date"}},
		{Name: "model", DataType: []string{"text"}},
		{Name: "dims", DataType: []string{"int"}},
		// indexVersion: stale-write guard; indexedSeq/indexedStream: deprecated,
		// unused. See memoryProperties.
		{Name: "indexVersion", DataType: []string{"int"}},
		{Name: "indexedSeq", DataType: []string{"int"}},
		{Name: "indexedStream", DataType: []string{"int"}},
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

// expectedFieldTokenized lists, per class, the text properties that MUST use
// "field" tokenization for exact-match filtering. A class created before that fix
// (default "word" tokenization) makes `namespace`/UUID `Equal` filters fuzzy, and
// tokenization is IMMUTABLE on an existing property — ensureProps cannot correct
// it, only a class rebuild + reindex can — so VerifySchema surfaces it loudly.
func expectedFieldTokenized() map[string][]string {
	return map[string][]string{
		memory.ClassName:        {"namespace", "source", "conversationId", "tags"},
		memory.ChunkClassName:   {"memoryId", "namespace", "tags"},
		memory.SummaryClassName: {"namespace", "source", "conversationId"},
		memory.TrashClassName:   {"memoryId", "namespace", "tags"},
	}
}

// VerifySchema checks, on boot, that every class Cortex relies on exists with its
// full property set and that exact-match keys use "field" tokenization. When the
// Store was configured with SetMultiTenant(true) it also asserts that the three
// memory classes have MT enabled — a mismatch (non-MT class with MT flag on, or
// vice versa) is surfaced as a loud, actionable problem pointing at
// `cortex migrate-mt`. A hard error is returned only if Weaviate itself can't be
// queried. Problems are advisory — search keeps working — so callers log them
// loudly rather than crash.
func (s *Store) VerifySchema(ctx context.Context) ([]string, error) {
	expectProps := map[string][]*models.Property{
		memory.ClassName:        memoryProperties(),
		memory.ChunkClassName:   chunkProperties(),
		memory.SummaryClassName: summaryProperties(),
		memory.TrashClassName:   trashProperties(),
	}
	fieldKeys := expectedFieldTokenized()

	var problems []string
	for _, className := range []string{memory.ClassName, memory.ChunkClassName, memory.SummaryClassName, memory.TrashClassName} {
		exists, err := s.client.Schema().ClassExistenceChecker().WithClassName(className).Do(ctx)
		if err != nil {
			return nil, fmt.Errorf("check class %s: %w", className, err)
		}
		if !exists {
			problems = append(problems, fmt.Sprintf("class %s is MISSING (EnsureSchema should have created it)", className))
			continue
		}
		class, err := s.client.Schema().ClassGetter().WithClassName(className).Do(ctx)
		if err != nil {
			return nil, fmt.Errorf("get class %s: %w", className, err)
		}
		have := make(map[string]*models.Property, len(class.Properties))
		for _, p := range class.Properties {
			have[p.Name] = p
		}
		for _, want := range expectProps[className] {
			if _, ok := have[want.Name]; !ok {
				problems = append(problems, fmt.Sprintf("class %s missing property %q", className, want.Name))
			}
		}
		for _, key := range fieldKeys[className] {
			if p, ok := have[key]; ok && p.Tokenization != "field" {
				problems = append(problems, fmt.Sprintf(
					"class %s property %q has tokenization %q, want \"field\" — namespace/id/tag filters are fuzzy; schema migration #1 (or a rebuild + reindex) fixes it",
					className, key, p.Tokenization))
			}
		}

		// MT assertion: flag ON → class must have MT enabled; flag OFF → class must not.
		if s.multiTenant {
			enabled := class.MultiTenancyConfig != nil && class.MultiTenancyConfig.Enabled
			if !enabled {
				problems = append(problems, fmt.Sprintf(
					"class %s is NOT multi-tenant but CORTEX_MULTI_TENANT=true — run `cortex migrate-mt` to rebuild the store with multi-tenancy enabled",
					className))
			}
		} else {
			enabled := class.MultiTenancyConfig != nil && class.MultiTenancyConfig.Enabled
			if enabled {
				problems = append(problems, fmt.Sprintf(
					"class %s IS multi-tenant but CORTEX_MULTI_TENANT=false — set CORTEX_MULTI_TENANT=true or run `cortex migrate-mt` to reset",
					className))
			}
		}
	}
	return problems, nil
}

// DeleteClass drops the Memory, MemoryChunk and ConversationSummary classes
// and all their objects. It deliberately leaves Trash alone: trash objects carry
// no vector, so a dimension-change rebuild is irrelevant to them (see
// DeleteAllClasses for the callers that must drop it too). CortexMeta (store
// metadata) is never dropped. Used by reindex when a model change alters the vector
// dimension and the classes must be rebuilt — both are dropped so their stored vectors stay
// dimension-consistent. Facts are then republished by reindex; summaries start
// empty and are rebuilt as the agent re-summarises.
//
// WARNING: in multi-tenant mode this drops ALL tenants' data. Per-tenant reindex
// must NOT call this — it only republishes within the tenant. A dimension-change
// rebuild in MT mode is an admin-wide operation.
func (s *Store) DeleteClass(ctx context.Context) error {
	for _, name := range []string{memory.ClassName, memory.ChunkClassName, memory.SummaryClassName} {
		if err := s.dropLogical(ctx, name); err != nil {
			return err
		}
	}
	return nil
}

// DeleteTrashClass drops the Trash class (and its objects).
func (s *Store) DeleteTrashClass(ctx context.Context) error {
	return s.dropLogical(ctx, memory.TrashClassName)
}

// DeleteAllClasses drops every memory class including Trash. Needed only where
// the Trash tenancy mode must change too (migrate-mt, after snapshotting the
// trash) and by tests that want a clean slate. CortexMeta is still kept; tests
// drop it explicitly with DeleteMetaClass.
func (s *Store) DeleteAllClasses(ctx context.Context) error {
	if err := s.DeleteClass(ctx); err != nil {
		return err
	}
	return s.DeleteTrashClass(ctx)
}

// ClassExists reports whether className exists in the Weaviate schema.
func (s *Store) ClassExists(ctx context.Context, className string) (bool, error) {
	ok, err := s.client.Schema().ClassExistenceChecker().WithClassName(className).Do(ctx)
	if err != nil {
		return false, fmt.Errorf("check class %s: %w", className, err)
	}
	return ok, nil
}

// IsClassMultiTenant reports whether className currently has MT enabled in
// Weaviate. Used by the migration guard: if all three classes already have MT
// on, there is nothing to migrate. Returns (false, nil) when the class does not
// exist.
func (s *Store) IsClassMultiTenant(ctx context.Context, className string) (bool, error) {
	exists, err := s.client.Schema().ClassExistenceChecker().WithClassName(className).Do(ctx)
	if err != nil {
		return false, fmt.Errorf("check class %s: %w", className, err)
	}
	if !exists {
		return false, nil
	}
	class, err := s.client.Schema().ClassGetter().WithClassName(className).Do(ctx)
	if err != nil {
		return false, fmt.Errorf("get class %s: %w", className, err)
	}
	return class.MultiTenancyConfig != nil && class.MultiTenancyConfig.Enabled, nil
}

// ListTenants returns the tenant names registered for className in Weaviate.
// Returns an empty slice when the class has no tenants. Only meaningful when
// multi-tenancy is enabled on the class; callers must check s.multiTenant.
func (s *Store) ListTenants(ctx context.Context, className string) ([]string, error) {
	tenants, err := s.client.Schema().TenantsGetter().WithClassName(className).Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("list tenants %s: %w", className, err)
	}
	names := make([]string, 0, len(tenants))
	for _, t := range tenants {
		names = append(names, t.Name)
	}
	return names, nil
}

// ListAllTenants returns the union of tenant names across the Memory and
// ConversationSummary classes, so an orphaned summary tenant (one that has
// summaries but no memories, or vice versa) is still captured by BackupAll.
// The returned slice is sorted for deterministic output.
func (s *Store) ListAllTenants(ctx context.Context) ([]string, error) {
	memTenants, err := s.ListTenants(ctx, memory.ClassName)
	if err != nil {
		return nil, err
	}
	sumTenants, err := s.ListTenants(ctx, memory.SummaryClassName)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(memTenants)+len(sumTenants))
	for _, t := range memTenants {
		seen[t] = struct{}{}
	}
	for _, t := range sumTenants {
		seen[t] = struct{}{}
	}
	names := make([]string, 0, len(seen))
	for t := range seen {
		names = append(names, t)
	}
	sort.Strings(names)
	return names, nil
}

// scanPageSize is the page size of scanAll's cursor iteration. A var (not a
// const) so tests can shrink it and exercise multi-page iteration cheaply.
var scanPageSize = 1000

// scanAll visits EVERY object of collection (within tenant when non-empty) by
// paging with Weaviate's id cursor ("after"), so the result is not capped by
// QUERY_MAXIMUM_RESULTS (10000) like a single WithLimit(AllLimit) query is.
// The cursor cannot be combined with where filters or sort: callers needing
// either must filter/sort the visited rows in Go. Visit order is id order.
// isTenantNotFound is NOT swallowed here; callers decide what it means.
func (s *Store) scanAll(ctx context.Context, collection, tenant string, props []string, fn func(graphql.SearchResult)) error {
	after := ""
	for {
		q := s.client.Experimental().Search().
			WithCollection(collection).
			WithMetadata(&graphql.Metadata{ID: true}).
			WithLimit(scanPageSize)
		if len(props) > 0 {
			q = q.WithProperties(props...)
		}
		if tenant != "" {
			q = q.WithTenant(tenant)
		}
		if after != "" {
			q = q.WithAfter(after)
		}
		res, err := q.Do(ctx)
		if err != nil {
			return err
		}
		for _, r := range res {
			fn(r)
		}
		if len(res) < scanPageSize {
			return nil
		}
		after = res[len(res)-1].ID
	}
}

// ListAllRecords scans the Memory class without any tenant scope — used by the
// migration snapshot BEFORE the classes are dropped, when the store is still
// non-MT. It must not be called against an MT-enabled class (which requires a
// tenant). Returns every record (cursor-paged, not capped).
func (s *Store) ListAllRecords(ctx context.Context) ([]memory.Record, error) {
	recs := make([]memory.Record, 0)
	err := s.scanAll(ctx, memory.ClassName, "", memoryProps, func(r graphql.SearchResult) {
		recs = append(recs, resultToRecord(r))
	})
	if err != nil {
		return nil, fmt.Errorf("list all records: %w", err)
	}
	return recs, nil
}

// ListAllSummaries scans the ConversationSummary class without any tenant scope
// — used by the migration snapshot before the classes are dropped. Same caveat
// as ListAllRecords: call only on non-MT classes.
func (s *Store) ListAllSummaries(ctx context.Context) ([]memory.Summary, error) {
	sums := make([]memory.Summary, 0)
	err := s.scanAll(ctx, memory.SummaryClassName, "", summaryProps, func(r graphql.SearchResult) {
		sums = append(sums, resultToSummary(r))
	})
	if err != nil {
		return nil, fmt.Errorf("list all summaries: %w", err)
	}
	return sums, nil
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

// AllLimit is the cap for a single LIMIT-based query (user-facing list/search
// caps). Weaviate's default QUERY_MAXIMUM_RESULTS is 10000. Paths that need
// EVERY row (backup, reindex, migrate, Count) must NOT rely on it: they use the
// cursor-paged scanAll / TenantStore.ListAll instead.
const AllLimit = 10000

// allCount is the internal alias used throughout this file.
const allCount = AllLimit

// ---- TenantStore — all memory-class operations ----

// upsertObject writes props+vector under id in className. It is create-or-replace
// with NO check-then-act window, so it is safe under CONCURRENT workers (multiple
// JetStream consumers): it attempts a create, and on ANY failure — most commonly
// the object already existing (a concurrent worker, redelivery, reindex, or a
// summary update) — it replaces the object via PUT. Two workers racing to create
// the same id can't both "win the exists-check": the loser's create fails and it
// falls through to the idempotent PUT. A genuine non-"already exists" error still
// surfaces, because the PUT then fails too and its error is returned. PUT replaces
// the whole object (no WithMerge), refreshing vector + props.
func (ts *TenantStore) upsertObject(ctx context.Context, className, id string, props map[string]interface{}, vector []float32) error {
	creator := ts.s.client.Data().Creator().
		WithClassName(className).
		WithID(id).
		WithProperties(props).
		WithVector(vector)
	if ts.t != "" {
		creator = creator.WithTenant(ts.t)
	}
	_, err := creator.Do(ctx)
	if err == nil {
		return nil
	}
	updater := ts.s.client.Data().Updater().
		WithClassName(className).
		WithID(id).
		WithProperties(props).
		WithVector(vector)
	if ts.t != "" {
		updater = updater.WithTenant(ts.t)
	}
	if uerr := updater.Do(ctx); uerr != nil {
		return fmt.Errorf("upsert object %s/%s (create failed: %v): %w", className, id, err, uerr)
	}
	return nil
}

// indexOwnedProps is the property set the INDEX path owns (see UpdateIndexFields):
// the single source for both the create (Upsert) and the update (merge) paths.
// nil slices become [] so a merge clears them rather than sending null.
func indexOwnedProps(rec memory.Record) map[string]interface{} {
	tags, dups := rec.Tags, rec.DupCandidates
	if tags == nil {
		tags = []string{}
	}
	if dups == nil {
		dups = []string{}
	}
	return map[string]interface{}{
		"text":           rec.Text,
		"namespace":      rec.Namespace,
		"tags":           tags,
		"source":         rec.Source,
		"createdAt":      rec.CreatedAt.UTC().Format(time.RFC3339),
		"model":          rec.Model,
		"dims":           rec.Dims,
		"conversationId": rec.ConversationID,
		"dupCandidates":  dups,
		"indexVersion":   rec.IndexVersion,
	}
}

// validUntilValue renders an expiry for a Weaviate date property: nil (JSON null,
// which a merge treats as "clear") for the zero time, else RFC3339.
func validUntilValue(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339)
}

// Upsert writes a record with its precomputed vector into the Memory class,
// creating the object with EVERY field (the index-owned ones plus the fields other
// writers own, which a new memory such as an import carries). For an existing
// memory use UpdateIndexFields.
func (ts *TenantStore) Upsert(ctx context.Context, rec memory.Record, vector []float32) error {
	props := indexOwnedProps(rec)
	// validUntil is written ONLY here, on create: afterwards it belongs to
	// SetValidUntil, so a queued index message (reindex, an earlier text edit, a
	// restore) carrying an older snapshot can never revert an expiry edit.
	if !rec.ValidUntil.IsZero() {
		props["validUntil"] = validUntilValue(rec.ValidUntil)
	}
	props["linkedIds"] = rec.LinkedIDs
	props["notDuplicateOf"] = rec.NotDuplicateOf
	props["accessCount"] = rec.AccessCount
	// A Weaviate `date` property rejects an empty string — it must be a valid
	// RFC3339 date or absent. A never-reinforced memory has a zero lastAccessedAt,
	// so omit the key entirely (the property stays null) rather than send "".
	if !rec.LastAccessedAt.IsZero() {
		props["lastAccessedAt"] = formatDate(rec.LastAccessedAt)
	}
	props["useCount"] = rec.UseCount
	if !rec.LastUsedAt.IsZero() {
		props["lastUsedAt"] = formatDate(rec.LastUsedAt)
	}
	return ts.upsertObject(ctx, memory.ClassName, rec.ID, props, vector)
}

// UpdateIndexFields rewrites an EXISTING memory's index-owned fields and vector
// via a Weaviate merge (PATCH) and nothing else. Index-owned: text, vector,
// namespace, tags, source, createdAt, conversationId, model, dims, dupCandidates,
// indexVersion. Deliberately NOT written, because other writers own
// them and a queued index message only carries a stale snapshot:
//
//	validUntil                   server SetValidUntil (written by Upsert on create only)
//	linkedIds                    link consumer (SetLinks)
//	accessCount, lastAccessedAt  server Reinforce
//	useCount, lastUsedAt         server RecordUse
//	notDuplicateOf               server DismissDuplicate (SetNotDuplicateOf)
//
// Weaviate applies a merge per property under the object's lock, so disjoint-field
// merges from different processes cannot clobber each other (pinned by the
// integration test). Use Upsert to create a memory that does not exist yet.
func (ts *TenantStore) UpdateIndexFields(ctx context.Context, rec memory.Record, vector []float32) error {
	updater := ts.s.client.Data().Updater().
		WithMerge().
		WithClassName(memory.ClassName).
		WithID(rec.ID).
		WithVector(vector).
		WithProperties(indexOwnedProps(rec))
	if ts.t != "" {
		updater = updater.WithTenant(ts.t)
	}
	if err := updater.Do(ctx); err != nil {
		return fmt.Errorf("update index fields %s: %w", rec.ID, err)
	}
	return nil
}

// SetValidUntil is the sole writer of an existing memory's expiry; it changes ONLY the expiry (zero time = never expires) via a
// single-property Weaviate merge: no vector, no other field and no chunk is
// touched, so an expiry change never re-embeds. A merge is atomic per property, so
// it cannot clobber a concurrent tag/link/reinforcement merge, and being a scalar
// replace (not a read-modify-write) it needs no distributed lock. Returns
// ErrNotFound when the memory does not exist (yet).
func (ts *TenantStore) SetValidUntil(ctx context.Context, id string, until time.Time) error {
	updater := ts.s.client.Data().Updater().
		WithMerge().
		WithClassName(memory.ClassName).
		WithID(id).
		WithProperties(map[string]interface{}{"validUntil": validUntilValue(until)})
	if ts.t != "" {
		updater = updater.WithTenant(ts.t)
	}
	if err := updater.Do(ctx); err != nil {
		var wErr *fault.WeaviateClientError
		if isTenantNotFound(err) || (errors.As(err, &wErr) && wErr.StatusCode == 404) {
			return ErrNotFound
		}
		return fmt.Errorf("set validUntil %s: %w", id, err)
	}
	return nil
}

// ErrNotFound is returned by metadata-only writes for a memory that does not exist.
var ErrNotFound = errors.New("memory not found")

// formatDate renders a time for a Weaviate date property, returning "" for the
// zero time so the property stays null rather than serialising the year-0001
// sentinel (which would make a never-accessed memory look "accessed long ago").
func formatDate(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// UpsertSummary writes a conversation summary into the ConversationSummary
// class, keyed by the deterministic SummaryID so each conversation has exactly
// one, ever-current summary. Re-saving the same conversation replaces it.
func (ts *TenantStore) UpsertSummary(ctx context.Context, sum memory.Summary, vector []float32) error {
	return ts.upsertObject(ctx, memory.SummaryClassName, memory.SummaryID(sum.ConversationID), map[string]interface{}{
		"text":           sum.Text,
		"conversationId": sum.ConversationID,
		"namespace":      sum.Namespace,
		"source":         sum.Source,
		"createdAt":      sum.CreatedAt.UTC().Format(time.RFC3339),
		"updatedAt":      sum.UpdatedAt.UTC().Format(time.RFC3339),
		"model":          sum.Model,
		"dims":           sum.Dims,
		"indexVersion":   sum.IndexVersion,
	}, vector)
}

// Delete removes a memory by ID (soft: the record is copied to Trash first, see
// DeleteWithReason), cascading to its chunks.
func (ts *TenantStore) Delete(ctx context.Context, id string) error {
	return ts.DeleteWithReason(ctx, id, memory.TrashReasonDelete)
}

// DeleteWithReason moves a memory to Trash (recording reason) and then removes it,
// cascading to its chunks so no orphaned chunks are left behind to surface in
// search. The trash write comes FIRST: if it fails nothing is deleted (fail loud,
// the caller can retry), so a delete can never lose data that the trash would
// have kept. A memory that does not exist is not trashed and the delete stays a
// no-op. Chunks are deleted before the memory: if the memory delete then fails the
// caller retries, whereas the reverse order could leave the memory present but
// unsearchable-via-its-own-chunks momentarily.
func (ts *TenantStore) DeleteWithReason(ctx context.Context, id, reason string) error {
	rec, found, err := ts.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("read memory before trashing: %w", err)
	}
	if found {
		if err := ts.putTrash(ctx, rec, reason, time.Now()); err != nil {
			return fmt.Errorf("move %s to trash: %w", id, err)
		}
	}
	return ts.hardDelete(ctx, id)
}

// hardDelete permanently removes a memory and its chunks (no trash).
func (ts *TenantStore) hardDelete(ctx context.Context, id string) error {
	if err := ts.deleteChunksByMemory(ctx, id); err != nil {
		return err
	}
	deleter := ts.s.client.Data().Deleter().
		WithClassName(memory.ClassName).
		WithID(id)
	if ts.t != "" {
		deleter = deleter.WithTenant(ts.t)
	}
	if err := deleter.Do(ctx); err != nil {
		if isTenantNotFound(err) {
			return nil
		}
		return fmt.Errorf("delete object: %w", err)
	}
	return nil
}

// Get fetches a single memory by ID over REST. found is false if it does not
// exist. Used by Link/Unlink to read the current link set before updating it.
func (ts *TenantStore) Get(ctx context.Context, id string) (memory.Record, bool, error) {
	props, ok, err := ts.getProps(ctx, memory.ClassName, id)
	if err != nil || !ok {
		return memory.Record{}, false, err
	}
	return objectToRecord(id, props), true, nil
}

// GetSummaryVersion returns the indexVersion of a conversation's stored summary.
// A missing summary, or one that predates the property, reads as 0 (nil error),
// which the stale guard treats as "nothing newer stored".
func (ts *TenantStore) GetSummaryVersion(ctx context.Context, conversationID string) (int64, error) {
	props, ok, err := ts.getProps(ctx, memory.SummaryClassName, memory.SummaryID(conversationID))
	if err != nil || !ok {
		return 0, err
	}
	if p, isMap := props.(map[string]interface{}); isMap {
		return restInt64(p, "indexVersion"), nil
	}
	return 0, nil
}

// getProps fetches one object's raw REST properties from className, with the
// strict tenant handling documented below.
func (ts *TenantStore) getProps(ctx context.Context, className, id string) (models.PropertySchema, bool, error) {
	checker := ts.s.client.Data().Checker().
		WithClassName(className).WithID(id)
	if ts.t != "" {
		checker = checker.WithTenant(ts.t)
	}
	exists, err := checker.Do(ctx)
	if err != nil {
		// The existence check is a HEAD request, so Weaviate's "tenant not found"
		// 422 arrives with an empty body and isTenantNotFound can't see the message.
		// A bare 422 is ambiguous (absent tenant, inactive/offloaded tenant, bad
		// input), so ask the schema API whether the tenant really is absent.
		if isTenantNotFound(err) {
			return nil, false, nil
		}
		if ts.t != "" && isStatus422(err) {
			absent, cerr := tenantAbsent(ctx, func(ctx context.Context) (bool, error) {
				return ts.s.client.Schema().TenantsExists().
					WithClassName(className).WithTenant(ts.t).Do(ctx)
			})
			if cerr == nil && absent {
				return nil, false, nil
			}
			if cerr != nil {
				err = errors.Join(err, fmt.Errorf("tenant existence check: %w", cerr))
			}
		}
		return nil, false, fmt.Errorf("check object: %w", err)
	}
	if !exists {
		return nil, false, nil
	}
	getter := ts.s.client.Data().ObjectsGetter().
		WithClassName(className).WithID(id)
	if ts.t != "" {
		getter = getter.WithTenant(ts.t)
	}
	objs, err := getter.Do(ctx)
	if err != nil {
		if isTenantNotFound(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get object: %w", err)
	}
	if len(objs) == 0 {
		return nil, false, nil
	}
	return objs[0].Properties, true, nil
}

// SetLinks replaces a memory's linkedIds via a Weaviate merge (PATCH), leaving
// the vector and all other properties untouched — links never trigger a
// re-embed.
func (ts *TenantStore) SetLinks(ctx context.Context, id string, links []string) error {
	if links == nil {
		links = []string{}
	}
	updater := ts.s.client.Data().Updater().
		WithMerge().
		WithClassName(memory.ClassName).
		WithID(id).
		WithProperties(map[string]interface{}{"linkedIds": links})
	if ts.t != "" {
		updater = updater.WithTenant(ts.t)
	}
	if err := updater.Do(ctx); err != nil {
		return fmt.Errorf("set links: %w", err)
	}
	return nil
}

// SetNotDuplicateOf replaces a memory's notDuplicateOf list via a Weaviate merge
// (PATCH), leaving the vector and all other properties untouched. Used by
// DismissDuplicate to record the bidirectional "confirmed not a duplicate"
// decision that the worker consults when recomputing candidates.
func (ts *TenantStore) SetNotDuplicateOf(ctx context.Context, id string, ids []string) error {
	if ids == nil {
		ids = []string{}
	}
	updater := ts.s.client.Data().Updater().
		WithMerge().
		WithClassName(memory.ClassName).
		WithID(id).
		WithProperties(map[string]interface{}{"notDuplicateOf": ids})
	if ts.t != "" {
		updater = updater.WithTenant(ts.t)
	}
	if err := updater.Do(ctx); err != nil {
		return fmt.Errorf("set notDuplicateOf: %w", err)
	}
	return nil
}

// SetDupCandidates replaces a memory's dupCandidates list via a Weaviate merge
// (PATCH), leaving the vector and other properties untouched. Used by
// DismissDuplicate to drop a now-adjudicated pair from the review list
// immediately, without waiting for the next reindex to recompute it.
func (ts *TenantStore) SetDupCandidates(ctx context.Context, id string, ids []string) error {
	if ids == nil {
		ids = []string{}
	}
	updater := ts.s.client.Data().Updater().
		WithMerge().
		WithClassName(memory.ClassName).
		WithID(id).
		WithProperties(map[string]interface{}{"dupCandidates": ids})
	if ts.t != "" {
		updater = updater.WithTenant(ts.t)
	}
	if err := updater.Do(ctx); err != nil {
		return fmt.Errorf("set dupCandidates: %w", err)
	}
	return nil
}

// SetProvenance overwrites createdAt and conversationId via a Weaviate merge
// (PATCH). Used by MergeInto to carry the oldest capture time / conversation of
// the folded-in memories onto the surviving one. These are index-owned fields; the
// worker's commitIndex never moves createdAt later or blanks a conversationId, so
// a queued index message cannot revert this.
func (ts *TenantStore) SetProvenance(ctx context.Context, id string, createdAt time.Time, conversationID string) error {
	updater := ts.s.client.Data().Updater().
		WithMerge().
		WithClassName(memory.ClassName).
		WithID(id).
		WithProperties(map[string]interface{}{
			"createdAt":      createdAt.UTC().Format(time.RFC3339),
			"conversationId": conversationID,
		})
	if ts.t != "" {
		updater = updater.WithTenant(ts.t)
	}
	if err := updater.Do(ctx); err != nil {
		return fmt.Errorf("set provenance: %w", err)
	}
	return nil
}

// Reinforce records that a memory surfaced as a top search hit: it bumps
// accessCount and stamps lastAccessedAt via a Weaviate merge (PATCH), leaving the
// vector and every other property untouched — reinforcement never re-embeds. This
// is the write side of "living memory"; the read side (rerankHits) lets the
// freshened recency/frequency float the memory up in future searches. count is
// the NEW absolute access count (caller computes prev+1); the caller serialises
// concurrent reinforcements of the same id so the increment is not lost.
func (ts *TenantStore) Reinforce(ctx context.Context, id string, count int, at time.Time) error {
	updater := ts.s.client.Data().Updater().
		WithMerge().
		WithClassName(memory.ClassName).
		WithID(id).
		WithProperties(map[string]interface{}{
			"accessCount":    count,
			"lastAccessedAt": formatDate(at),
		})
	if ts.t != "" {
		updater = updater.WithTenant(ts.t)
	}
	if err := updater.Do(ctx); err != nil {
		return fmt.Errorf("reinforce: %w", err)
	}
	return nil
}

// expiredProp reports whether a scanned result's validUntil has passed.
func expiredProp(p map[string]any, now time.Time) bool {
	vu := propString(p, "validUntil")
	if vu == "" {
		return false
	}
	t, err := time.Parse(time.RFC3339, vu)
	return err == nil && !t.After(now)
}

// CountLiveExpired is Count split by expiry: live memories (what List/Search can
// return) and expired ones still stored (hidden, awaiting the sweep).
func (ts *TenantStore) CountLiveExpired(ctx context.Context) (live, expired int, err error) {
	now := time.Now()
	err = ts.s.scanAll(ctx, memory.ClassName, ts.t, []string{"validUntil"}, func(r graphql.SearchResult) {
		if expiredProp(r.Properties, now) {
			expired++
		} else {
			live++
		}
	})
	if err != nil {
		if isTenantNotFound(err) {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("count query: %w", err)
	}
	return live, expired, nil
}

// RecordUse stamps a memory as used: useCount = count and lastUsedAt = at via a
// Weaviate merge (PATCH) — vector and all other properties untouched, no re-embed.
// count is the NEW absolute count (caller computes prev+1 under its per-id lock).
func (ts *TenantStore) RecordUse(ctx context.Context, id string, count int, at time.Time) error {
	updater := ts.s.client.Data().Updater().
		WithMerge().
		WithClassName(memory.ClassName).
		WithID(id).
		WithProperties(map[string]interface{}{
			"useCount":   count,
			"lastUsedAt": formatDate(at),
		})
	if ts.t != "" {
		updater = updater.WithTenant(ts.t)
	}
	if err := updater.Do(ctx); err != nil {
		return fmt.Errorf("record use: %w", err)
	}
	return nil
}

// Count returns the number of stored memories for this tenant, optionally scoped
// to a namespace ("" counts all, expired included — see CountLiveExpired). There is no gRPC Aggregate in the stable
// client, so it cursor-scans the class and counts the rows (correct beyond the
// 10000-result query cap). The cursor cannot be combined with a where filter, so
// the namespace is matched in Go.
func (ts *TenantStore) Count(ctx context.Context, namespace string) (int, error) {
	var props []string
	if namespace != "" {
		props = []string{"namespace"}
	}
	n := 0
	err := ts.s.scanAll(ctx, memory.ClassName, ts.t, props, func(r graphql.SearchResult) {
		if namespace == "" || propString(r.Properties, "namespace") == namespace {
			n++
		}
	})
	if err != nil {
		if isTenantNotFound(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("count query: %w", err)
	}
	return n, nil
}

// ListAll returns EVERY memory of this tenant (optionally only those in
// namespace; "" = all), unsorted. Unlike List it is not capped: it cursor-pages
// and filters the namespace in Go. For backup/reindex-style "need all rows" paths.
func (ts *TenantStore) ListAll(ctx context.Context, namespace string) ([]memory.Record, error) {
	recs := make([]memory.Record, 0)
	err := ts.s.scanAll(ctx, memory.ClassName, ts.t, memoryProps, func(r graphql.SearchResult) {
		if namespace == "" || propString(r.Properties, "namespace") == namespace {
			recs = append(recs, resultToRecord(r))
		}
	})
	if err != nil {
		if isTenantNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list all query: %w", err)
	}
	return recs, nil
}

// ListAllSummaries returns EVERY conversation summary of this tenant, unsorted
// and uncapped (cursor-paged). Counterpart of ListAll.
func (ts *TenantStore) ListAllSummaries(ctx context.Context) ([]memory.Summary, error) {
	sums := make([]memory.Summary, 0)
	err := ts.s.scanAll(ctx, memory.SummaryClassName, ts.t, summaryProps, func(r graphql.SearchResult) {
		sums = append(sums, resultToSummary(r))
	})
	if err != nil {
		if isTenantNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list all summaries query: %w", err)
	}
	return sums, nil
}

// memoryProps is the property set fetched for both search and list.
var memoryProps = []string{"text", "namespace", "tags", "source", "createdAt", "model", "dims", "conversationId", "linkedIds", "dupCandidates", "notDuplicateOf", "accessCount", "lastAccessedAt", "validUntil", "useCount", "lastUsedAt"}

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

// notExpired ANDs "not expired" onto where. It is expressed as NOT(validUntil <=
// now) rather than `validUntil > now OR validUntil IS NULL`: IsNull filtering needs
// the class created with indexNullState (not the case for existing deployments and
// not changeable afterwards), whereas NOT is a complement over all objects, so a
// memory with no validUntil (never set, or written before the property existed)
// is kept without any backfill. includeExpired=true returns where unchanged.
func notExpired(where *filters.WhereBuilder, includeExpired bool, now time.Time) *filters.WhereBuilder {
	if includeExpired {
		return where
	}
	live := filters.Where().WithOperator(filters.Not).WithOperands([]*filters.WhereBuilder{
		filters.Where().
			WithPath([]string{"validUntil"}).
			WithOperator(filters.LessThanEqual).
			WithValueDate(now),
	})
	if where == nil {
		return live
	}
	return filters.Where().WithOperator(filters.And).WithOperands([]*filters.WhereBuilder{where, live})
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

// SearchMemoryVectors runs a nearVector/hybrid query against the FULL-memory
// vectors in the Memory class (over gRPC), with optional namespace, tag, and
// relevance filtering. ExcludeTags is applied after the query, so it can reduce
// the returned count below Limit.
//
// Since chunking, the PRIMARY retrieval path is Search (over MemoryChunk). This
// method is retained for the worker's duplicate-candidate detection, which
// genuinely wants whole-memory similarity (is this new memory a near-duplicate of
// an existing one?), not chunk-level matches.
func (ts *TenantStore) SearchMemoryVectors(ctx context.Context, vector []float32, opts SearchOpts) ([]memory.Hit, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 5
	}

	// Living-memory re-ranking re-orders the relevance survivors by usage, so a
	// frequently/recently used memory just outside the top `limit` by pure
	// relevance can still float in. Overfetch a wider candidate pool to give it
	// that chance; the cutoff and per-hit Distance are unaffected.
	fetchLimit := limit
	rerank := opts.RerankWeight > 0
	if rerank {
		fetchLimit = limit * rerankOverfetch
	}

	query := ts.s.client.Experimental().Search().
		WithCollection(memory.ClassName).
		WithProperties(memoryProps...).
		WithLimit(fetchLimit)
	if ts.t != "" {
		query = query.WithTenant(ts.t)
	}

	// Hybrid (query present) = BM25 keyword over `text` + vector, fused; the keyword
	// side surfaces exact tokens — codenames, hostnames, IDs — that the vector side
	// misses because they don't embed near their meaning. Otherwise a pure nearVector
	// search. Construction is shared with the chunk-level Search via applyVectorQuery.
	query, hybrid := applyVectorQuery(query, vector, opts)

	if opts.Autocut > 0 {
		query = query.WithAutocut(opts.Autocut)
	}
	if where := notExpired(buildWhere(opts.Namespace, opts.ConversationID, opts.IncludeTags, opts.AnyTags), opts.IncludeExpired, time.Now()); where != nil {
		query = query.WithWhere(where)
	}

	res, err := query.Do(ctx)
	if err != nil {
		if isTenantNotFound(err) {
			return nil, nil
		}
		return nil, searchError(err, len(vector))
	}

	hits := make([]memory.Hit, 0, len(res))
	for _, r := range res {
		dist, ok := effectiveDistance(r.Metadata.Distance, r.Metadata.Score, hybrid, opts.MaxDistance)
		if !ok {
			continue
		}
		hits = append(hits, memory.Hit{Record: resultToRecord(r), Distance: dist})
	}
	hits = excludeTagged(hits, func(h memory.Hit) []string { return h.Tags }, opts.ExcludeTags)
	if rerank {
		hits = rerankHits(hits, opts.RerankWeight, opts.RerankHalfLifeDays, time.Now())
		if len(hits) > limit {
			hits = hits[:limit]
		}
	}
	return hits, nil
}

// rerankOverfetch is how many times `limit` candidates Search pulls when
// re-ranking is on, so a high-usage memory ranked just outside the top `limit`
// by relevance still gets a chance to surface. A personal store is small, so a
// wide pool is cheap.
const rerankOverfetch = 5

// defaultHalfLifeDays is the recency half-life used when SearchOpts leaves it
// unset (<=0): a memory not accessed for this many days contributes half the
// recency of one accessed just now.
const defaultHalfLifeDays = 30.0

// freqWeight scales the (logarithmic) access-count bonus added to a memory's
// recency when computing its usage term. Kept small and internal: frequency
// nudges ordering, recency dominates, so a memory that was useful once long ago
// does not permanently outrank fresh relevant matches.
const freqWeight = 0.1

// rerankHits re-orders relevance survivors by a blend of relevance and usage.
// It is the heart of "living memory" and is a PURE function (now is injected) so
// the decay/frequency behaviour is unit-testable without a live store. It never
// mutates a hit's Distance — only the slice order changes — so every downstream
// consumer still sees the original relevance distance and its cutoff semantics.
//
//	score = (1-w)*relevance + w*usage
//	relevance = clamp(1 - distance, 0, 1)              // higher = closer match
//	usage     = min(1, recency + freqWeight*ln(1+accessCount))
//	recency   = 2^(-ageDays / halfLifeDays)            // 1 just-now → 0.5 at one half-life
//
// Age is measured from LastAccessedAt, falling back to CreatedAt when the memory
// was never reinforced; a memory with neither timestamp gets recency 0 (no usage
// signal), so it ranks purely on relevance.
func rerankHits(hits []memory.Hit, weight, halfLifeDays float32, now time.Time) []memory.Hit {
	if weight <= 0 || len(hits) < 2 {
		return hits
	}
	w := weight
	if w > 1 {
		w = 1
	}
	half := float64(halfLifeDays)
	if half <= 0 {
		half = defaultHalfLifeDays
	}

	scored := make([]struct {
		hit   memory.Hit
		score float64
	}, len(hits))
	for i, h := range hits {
		relevance := 1 - float64(h.Distance)
		if relevance < 0 {
			relevance = 0
		} else if relevance > 1 {
			relevance = 1
		}
		usage := math.Min(1, recencyScore(h, half, now)+freqWeight*math.Log1p(float64(h.AccessCount)))
		scored[i].hit = h
		scored[i].score = (1-float64(w))*relevance + float64(w)*usage
	}
	sort.SliceStable(scored, func(i, j int) bool { return scored[i].score > scored[j].score })

	out := make([]memory.Hit, len(scored))
	for i := range scored {
		out[i] = scored[i].hit
	}
	return out
}

// recencyScore is the time-decayed usage component of a hit: 2^(-ageDays/half),
// measured from the last access (or creation if never accessed). It is 0 when the
// memory carries neither timestamp (no usage signal at all) and 1 for a memory
// touched now or in the (clock-skew) future.
func recencyScore(h memory.Hit, halfLifeDays float64, now time.Time) float64 {
	ref := h.LastAccessedAt
	if ref.IsZero() {
		ref = h.CreatedAt
	}
	if ref.IsZero() {
		return 0
	}
	ageDays := now.Sub(ref).Hours() / 24
	if ageDays < 0 {
		ageDays = 0
	}
	return math.Pow(2, -ageDays/halfLifeDays)
}

// hybridDistance maps a relativeScoreFusion score (0..1, higher = more relevant)
// onto Cortex's distance metric (lower = more relevant, like cosine distance) as
// 1-score, so hybrid hits sort and filter against the SAME maxDistance cutoff as
// vector hits and the rest of the stack (UI, MCP) needs no special case.
func hybridDistance(score float32) float32 {
	d := 1 - score
	if d < 0 {
		return 0
	}
	return d
}

// applyVectorQuery configures q for either a hybrid (BM25 keyword + vector) search
// or a pure nearVector search, depending on whether opts carries a keyword query,
// and attaches the matching result metadata. The returned bool reports whether the
// query is hybrid — the caller needs it because hybrid results have no server-side
// distance bound and must be filtered by maxDistance in Go (see effectiveDistance).
//
// This is the single source of truth for query construction shared by the
// chunk-level Search and the whole-memory SearchMemoryVectors; keeping it in one
// place stops the two paths from drifting (e.g. a maxDistance handled on one but
// not the other) and silently returning inconsistent recall.
func applyVectorQuery(q *graphql.Search, vector []float32, opts SearchOpts) (*graphql.Search, bool) {
	if strings.TrimSpace(opts.Query) != "" {
		h := (&graphql.HybridArgumentBuilder{}).
			WithQuery(opts.Query).
			WithVector(vector).
			WithProperties([]string{"text"}).
			WithFusionType(graphql.RelativeScore)
		if opts.Alpha > 0 {
			h = h.WithAlpha(opts.Alpha)
		}
		return q.WithHybrid(h).WithMetadata(&graphql.Metadata{ID: true, Score: true}), true
	}
	nearVector := (&graphql.NearVectorArgumentBuilder{}).WithVector(vector)
	if opts.MaxDistance > 0 {
		nearVector = nearVector.WithDistance(opts.MaxDistance)
	}
	return q.WithNearVector(nearVector).WithMetadata(&graphql.Metadata{ID: true, Distance: true}), false
}

// effectiveDistance resolves a single search result to its Cortex distance and
// whether it clears the maxDistance cutoff. For a nearVector result the server
// already enforced the bound (distance is authoritative and it always passes);
// for a hybrid result there is no server-side distance, so the fused score is
// mapped via hybridDistance and the cutoff applied here in Go.
func effectiveDistance(dist, score float32, hybrid bool, maxDistance float32) (float32, bool) {
	if !hybrid {
		return dist, true
	}
	d := hybridDistance(score)
	if maxDistance > 0 && d > maxDistance {
		return d, false
	}
	return d, true
}

// SearchByID runs a nearObject query: it finds the stored memories most similar
// to the memory with the given id by REUSING that memory's existing stored
// vector — Weaviate never re-embeds and the text never leaves the store. This is
// the "find similar to this one" primitive; it must be preferred over embedding a
// memory's own text back into a query. The seed memory itself (distance 0) is
// excluded from the results.
func (ts *TenantStore) SearchByID(ctx context.Context, id string, opts SearchOpts) ([]memory.Hit, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 5
	}

	nearObject := (&graphql.NearObjectArgumentBuilder{}).WithID(id)
	if opts.MaxDistance > 0 {
		nearObject = nearObject.WithDistance(opts.MaxDistance)
	}

	// Request one extra so the seed object (always the closest hit) can be dropped
	// without shrinking the result set below the caller's limit.
	query := ts.s.client.Experimental().Search().
		WithCollection(memory.ClassName).
		WithProperties(memoryProps...).
		WithMetadata(&graphql.Metadata{ID: true, Distance: true}).
		WithNearObject(nearObject).
		WithLimit(limit + 1)
	if ts.t != "" {
		query = query.WithTenant(ts.t)
	}

	if opts.Autocut > 0 {
		query = query.WithAutocut(opts.Autocut)
	}
	if where := notExpired(buildWhere(opts.Namespace, opts.ConversationID, opts.IncludeTags, opts.AnyTags), opts.IncludeExpired, time.Now()); where != nil {
		query = query.WithWhere(where)
	}

	res, err := query.Do(ctx)
	if err != nil {
		if isTenantNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("near-object search: %w", err)
	}

	hits := make([]memory.Hit, 0, len(res))
	for _, r := range res {
		if r.ID == id {
			continue // the seed memory itself
		}
		hits = append(hits, memory.Hit{Record: resultToRecord(r), Distance: r.Metadata.Distance})
		if len(hits) >= limit {
			break
		}
	}
	return excludeTagged(hits, func(h memory.Hit) []string { return h.Tags }, opts.ExcludeTags), nil
}

// List returns stored memories newest-first with optional namespace/tag
// filtering. Unlike Search it runs no vector query (still over gRPC), so results
// carry no distance.
func (ts *TenantStore) List(ctx context.Context, opts ListOpts) ([]memory.Record, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 20
	}

	query := ts.s.client.Experimental().Search().
		WithCollection(memory.ClassName).
		WithProperties(memoryProps...).
		WithMetadata(&graphql.Metadata{ID: true}).
		WithSort(graphql.Sort{Path: []string{"createdAt"}, Order: graphql.Desc}).
		WithLimit(limit)
	if ts.t != "" {
		query = query.WithTenant(ts.t)
	}

	if where := notExpired(buildWhere(opts.Namespace, opts.ConversationID, opts.IncludeTags, opts.AnyTags), opts.IncludeExpired, time.Now()); where != nil {
		query = query.WithWhere(where)
	}

	res, err := query.Do(ctx)
	if err != nil {
		if isTenantNotFound(err) {
			return nil, nil
		}
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
// It cursor-scans the whole class and filters (namespace, non-empty
// dupCandidates) in Go, then sorts by createdAt desc and applies limit LAST — so
// candidates beyond the 10000-result query cap are not dropped. Go-side filtering
// also avoids a Weaviate IsNull filter, which would require the class to be
// created with indexNullState=true (not the case for existing deployments). No
// vector query, so results carry no distance. limit <= 0 means no cap (the RPC
// caps AFTER dropping candidates that no longer exist).
func (ts *TenantStore) ListWithCandidates(ctx context.Context, namespace string, limit int) ([]memory.Record, error) {
	recs := make([]memory.Record, 0)
	err := ts.s.scanAll(ctx, memory.ClassName, ts.t, memoryProps, func(r graphql.SearchResult) {
		if namespace != "" && propString(r.Properties, "namespace") != namespace {
			return
		}
		rec := resultToRecord(r)
		if len(rec.DupCandidates) == 0 {
			return
		}
		recs = append(recs, rec)
	})
	if err != nil {
		if isTenantNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list candidates query: %w", err)
	}

	sort.SliceStable(recs, func(i, j int) bool { return recs[i].CreatedAt.After(recs[j].CreatedAt) })
	if limit > 0 && len(recs) > limit {
		recs = recs[:limit]
	}
	return recs, nil
}

// summaryProps is the property set fetched when reading conversation summaries.
var summaryProps = []string{"text", "conversationId", "namespace", "source", "createdAt", "updatedAt", "model", "dims"}

// SearchSummaries runs a nearVector query (over gRPC) over conversation
// summaries, newest match first by relevance. Used as the first hop of session
// recall: a hit yields a conversationId to fan out to its facts.
func (ts *TenantStore) SearchSummaries(ctx context.Context, vector []float32, opts SummarySearchOpts) ([]memory.SummaryHit, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 5
	}

	nearVector := (&graphql.NearVectorArgumentBuilder{}).WithVector(vector)
	if opts.MaxDistance > 0 {
		nearVector = nearVector.WithDistance(opts.MaxDistance)
	}

	query := ts.s.client.Experimental().Search().
		WithCollection(memory.SummaryClassName).
		WithProperties(summaryProps...).
		WithMetadata(&graphql.Metadata{ID: true, Distance: true}).
		WithNearVector(nearVector).
		WithLimit(limit)
	if ts.t != "" {
		query = query.WithTenant(ts.t)
	}

	if where := buildWhere(opts.Namespace, "", nil, nil); where != nil {
		query = query.WithWhere(where)
	}

	res, err := query.Do(ctx)
	if err != nil {
		if isTenantNotFound(err) {
			return nil, nil
		}
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
func (ts *TenantStore) ListSummaries(ctx context.Context, opts SummaryListOpts) ([]memory.Summary, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 20
	}

	query := ts.s.client.Experimental().Search().
		WithCollection(memory.SummaryClassName).
		WithProperties(summaryProps...).
		WithMetadata(&graphql.Metadata{ID: true}).
		WithSort(graphql.Sort{Path: []string{"updatedAt"}, Order: graphql.Desc}).
		WithLimit(limit)
	if ts.t != "" {
		query = query.WithTenant(ts.t)
	}

	if where := buildWhere(opts.Namespace, "", nil, nil); where != nil {
		query = query.WithWhere(where)
	}

	res, err := query.Do(ctx)
	if err != nil {
		if isTenantNotFound(err) {
			return nil, nil
		}
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
		AccessCount:    propInt(p, "accessCount"),
		UseCount:       propInt(p, "useCount"),
	}
	if ca := propString(p, "createdAt"); ca != "" {
		rec.CreatedAt, _ = time.Parse(time.RFC3339, ca) // ignored: best-effort timestamp parse
	}
	if la := propString(p, "lastAccessedAt"); la != "" {
		rec.LastAccessedAt, _ = time.Parse(time.RFC3339, la) // ignored: best-effort timestamp parse
	}
	if vu := propString(p, "validUntil"); vu != "" {
		rec.ValidUntil, _ = time.Parse(time.RFC3339, vu) // ignored: best-effort timestamp parse
	}
	if lu := propString(p, "lastUsedAt"); lu != "" {
		rec.LastUsedAt, _ = time.Parse(time.RFC3339, lu) // ignored: best-effort timestamp parse
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
		sum.CreatedAt, _ = time.Parse(time.RFC3339, ca) // ignored: best-effort timestamp parse
	}
	if ua := propString(p, "updatedAt"); ua != "" {
		sum.UpdatedAt, _ = time.Parse(time.RFC3339, ua) // ignored: best-effort timestamp parse
	}
	return sum
}

// objectToRecord decodes a REST-fetched object (Data().ObjectsGetter) into a
// memory.Record. REST property values are typed differently from the gRPC search
// path: text[] arrives as []interface{} of strings and int as float64.
func objectToRecord(id string, raw models.PropertySchema) memory.Record {
	p, ok := raw.(map[string]interface{})
	if !ok {
		return memory.Record{ID: id}
	}
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
		AccessCount:    restInt(p, "accessCount"),
		UseCount:       restInt(p, "useCount"),
		IndexVersion:   restInt64(p, "indexVersion"),
	}
	if ca := restString(p, "createdAt"); ca != "" {
		rec.CreatedAt, _ = time.Parse(time.RFC3339, ca)
	}
	if la := restString(p, "lastAccessedAt"); la != "" {
		if t, err := time.Parse(time.RFC3339, la); err == nil {
			rec.LastAccessedAt = t
		}
		// err != nil: malformed timestamp; leave LastAccessedAt as zero time (explicit fallback)
	}
	if vu := restString(p, "validUntil"); vu != "" {
		if t, err := time.Parse(time.RFC3339, vu); err == nil {
			rec.ValidUntil = t
		}
	}
	if lu := restString(p, "lastUsedAt"); lu != "" {
		if t, err := time.Parse(time.RFC3339, lu); err == nil {
			rec.LastUsedAt = t
		}
	}
	return rec
}

func restString(p map[string]interface{}, key string) string {
	s, _ := p[key].(string)
	return s
}

// restInt64 decodes an int property as int64 (float64 on the REST path is exact
// below 2^53, which covers unix-microsecond timestamps).
func restInt64(p map[string]interface{}, key string) int64 {
	if f, ok := p[key].(float64); ok {
		return int64(f)
	}
	return 0
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
