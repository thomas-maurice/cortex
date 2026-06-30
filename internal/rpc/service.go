package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/gen/cortex/v1/cortexv1connect"
	"github.com/thomas-maurice/cortex/internal/bus"
	"github.com/thomas-maurice/cortex/internal/embed"
	"github.com/thomas-maurice/cortex/internal/identity"
	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/store"
)

// allLimit caps a full-store fetch (export/reindex). Weaviate's default
// QUERY_MAXIMUM_RESULTS is 10000; a personal store stays well under it.
const allLimit = 10000

// Config holds the server-side defaults applied to inbound requests.
type Config struct {
	DefaultNamespace string  // namespace stamped when a request omits one
	Source           string  // source stamped when a request omits one
	Version          string  // reported by Status
	BackupDir        string  // where Reindex writes its safety snapshot
	SearchAlpha      float32 // hybrid blend for text searches: 1=pure vector, 0=pure keyword; <=0 = Weaviate default
	// RerankWeight enables "living memory": when >0, Search re-orders relevance
	// survivors by a blend of relevance and recency-decayed usage (weight is the
	// usage share), and each search reinforces its top hits. 0 disables the whole
	// feature — no re-ordering and no reinforcement writes — so behaviour is
	// identical to before, opt-in like DEDUP_DISTANCE.
	RerankWeight float32
	// RerankHalfLifeDays is the recency half-life for the usage term; <=0 uses the
	// store default. Only meaningful when RerankWeight>0.
	RerankHalfLifeDays float32
	// ReinforceTopK is how many of a search's top hits get reinforced
	// (accessCount++ / lastAccessedAt=now). Only applied when RerankWeight>0;
	// defaults to 1 so a query strengthens its single best match, not the whole
	// returned page (which would be a noisy signal).
	ReinforceTopK int
	// ChunkingEnabled selects the primary retrieval path. When true, Search and
	// Consolidate match against the MemoryChunk index (with a whole-memory
	// fallback, so a store whose memories were never chunked still returns
	// results). When false, they match against whole-memory vectors only — the
	// pre-chunking behaviour, so disabling is a clean revert. Must agree with the
	// worker's CHUNKING_ENABLED (which gates whether chunks are written at all).
	ChunkingEnabled bool
	// MultiTenant turns on per-user isolation via Weaviate multi-tenancy (tenant =
	// user). When false Cortex is single-user (legacy mode); DEFAULT is ON. With it on:
	// no User/ApiKey lookups, the memory classes stay non-MT, auth is the existing
	// token+JWT. The whole feature is gated on this so existing deployments are
	// unaffected until they opt in and migrate (see cortex migrate-mt). Must agree
	// with the worker's CORTEX_MULTI_TENANT.
	MultiTenant bool
}

// Service implements the MemoryService Connect handler. It is the single owner
// of NATS and Weaviate/Ollama access; clients never touch those directly.
type Service struct {
	cortexv1connect.UnimplementedMemoryServiceHandler
	nc       *nats.Conn
	js       jetstream.JetStream
	store    *store.Store
	embedder *embed.Client
	cfg      Config
	log      *slog.Logger
	// linkMu serializes the read-modify-write that DismissDuplicate performs on
	// each memory's not-duplicate / candidate lists, preventing lost updates.
	// (Link/Unlink no longer mutate the store directly — they publish to
	// SubjectLink and the worker's link consumer serialises the apply.)
	linkMu sync.Mutex
	// reinforceMu serializes the read-modify-write of a memory's accessCount when
	// search hits are reinforced, so two concurrent searches reinforcing the same
	// memory cannot both read the old count and lose an increment.
	reinforceMu sync.Mutex
}

// NewService wires the handler with its backing clients.
func NewService(nc *nats.Conn, js jetstream.JetStream, st *store.Store, embedder *embed.Client, cfg Config, log *slog.Logger) *Service {
	return &Service{nc: nc, js: js, store: st, embedder: embedder, cfg: cfg, log: log}
}

// tenantStore resolves the caller's identity from the request context and returns
// a TenantStore scoped to that user. This is the ONLY place the tenant is
// derived from context in RPC handlers — no handler ever reads a tenant from
// req.Msg. When multi-tenancy is off the TenantStore is a no-op (no .WithTenant
// calls); when on, it applies the authenticated user's tenant to every Weaviate
// builder.
func (s *Service) tenantStore(ctx context.Context) *store.TenantStore {
	id, ok := identity.From(ctx)
	if !ok {
		// No identity on context means open/dev mode; use the bootstrap tenant so
		// single-user dev still works without authentication.
		return s.store.Tenant(identity.BootstrapTenant)
	}
	return s.store.Tenant(id.UserID)
}

func (s *Service) Save(ctx context.Context, req *connect.Request[cortexv1.SaveRequest]) (*connect.Response[cortexv1.SaveResponse], error) {
	text := strings.TrimSpace(req.Msg.GetText())
	if text == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("text must not be empty"))
	}
	ns := req.Msg.GetNamespace()
	if ns == "" {
		ns = s.cfg.DefaultNamespace
	}
	src := req.Msg.GetSource()
	if src == "" {
		src = s.cfg.Source
	}

	rec := memory.Record{
		ID:             uuid.NewString(),
		Text:           text,
		Namespace:      ns,
		Tags:           req.Msg.GetTags(),
		Source:         src,
		CreatedAt:      time.Now().UTC(),
		ConversationID: req.Msg.GetConversationId(),
		Supersedes:     req.Msg.GetSupersedes(),
	}
	// UserID is stamped by PublishIndex from context; not read from req.Msg.
	if err := bus.PublishIndex(ctx, s.js, rec); err != nil {
		return nil, err
	}
	// Requested links go onto the durable link subject, not the index payload:
	// the link consumer waits for this record to finish indexing (and for each
	// target to exist) before applying the edge, so order of arrival no longer
	// loses links. Best-effort per target — the record is already queued, so a
	// link publish failure must not fail the save (and re-saving would mint a new
	// id), it is logged loudly instead.
	for _, target := range req.Msg.GetLinkTo() {
		target = strings.TrimSpace(target)
		if target == "" || target == rec.ID {
			continue
		}
		// PublishLink stamps tenant from context.
		if err := bus.PublishLink(ctx, s.js, memory.LinkOpAdd, rec.ID, target); err != nil {
			s.log.Warn("publish link failed", "id", rec.ID, "target", target, "err", err)
		}
	}
	return connect.NewResponse(&cortexv1.SaveResponse{Id: rec.ID, Status: "queued"}), nil
}

// UpdateMemory edits an existing memory's text (and, opt-in, its tags/namespace)
// and republishes it for re-embedding — the same path Reindex uses per record,
// so the worker re-embeds, re-stamps provenance and recomputes dedup candidates.
// Reading the current record first preserves everything the edit does not touch:
// id, creation time, source, conversation, links and not-duplicate decisions.
func (s *Service) UpdateMemory(ctx context.Context, req *connect.Request[cortexv1.UpdateMemoryRequest]) (*connect.Response[cortexv1.UpdateMemoryResponse], error) {
	id := strings.TrimSpace(req.Msg.GetId())
	if id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id must not be empty"))
	}
	text := strings.TrimSpace(req.Msg.GetText())
	if text == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("text must not be empty"))
	}

	ts := s.tenantStore(ctx)
	rec, ok, err := ts.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("memory %s not found", id))
	}

	rec.Text = text
	if req.Msg.GetReplaceTags() {
		rec.Tags = req.Msg.GetTags()
	}
	if ns := strings.TrimSpace(req.Msg.GetNamespace()); ns != "" {
		rec.Namespace = ns
	}
	// DupCandidates are recomputed by the worker on every (re)index; clear the
	// stale set so a failed republish never leaves an old hint behind.
	rec.DupCandidates = nil

	// PublishIndex stamps the tenant from context.
	if err := bus.PublishIndex(ctx, s.js, rec); err != nil {
		return nil, err
	}
	return connect.NewResponse(&cortexv1.UpdateMemoryResponse{Id: id, Status: "queued"}), nil
}

// searchStore runs the configured primary retrieval and returns parent memories
// either way, so callers (Search, Consolidate) are oblivious to the mode. When
// chunking is enabled it matches the MemoryChunk index (store.Search, which falls
// back to whole-memory vectors for any memory that has no chunks); when disabled
// it matches whole-memory vectors only (the pre-chunking path), so a deployment
// can turn chunking off and behave exactly as before.
func (s *Service) searchStore(ctx context.Context, ts *store.TenantStore, vec []float32, opts store.SearchOpts) ([]memory.Hit, error) {
	if s.cfg.ChunkingEnabled {
		return ts.Search(ctx, vec, opts)
	}
	return ts.SearchMemoryVectors(ctx, vec, opts)
}

func (s *Service) Search(ctx context.Context, req *connect.Request[cortexv1.SearchRequest]) (*connect.Response[cortexv1.SearchResponse], error) {
	query := strings.TrimSpace(req.Msg.GetQuery())
	if query == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("query must not be empty"))
	}

	vec, err := s.embedder.Embed(ctx, query)
	if err != nil {
		return nil, err
	}
	ts := s.tenantStore(ctx)
	hits, err := s.searchStore(ctx, ts, vec, store.SearchOpts{
		Namespace:          resolveNamespace(req.Msg.GetNamespace(), s.cfg.DefaultNamespace),
		Limit:              int(req.Msg.GetLimit()),
		MaxDistance:        req.Msg.GetMaxDistance(),
		Autocut:            int(req.Msg.GetAutocut()),
		IncludeTags:        req.Msg.GetTags(),
		AnyTags:            req.Msg.GetAnyTags(),
		ExcludeTags:        req.Msg.GetExcludeTags(),
		Query:              query, // enables hybrid (BM25 + vector) so exact tokens resolve
		Alpha:              s.cfg.SearchAlpha,
		RerankWeight:       s.cfg.RerankWeight,
		RerankHalfLifeDays: s.cfg.RerankHalfLifeDays,
	})
	if err != nil {
		return nil, err
	}

	// Living memory: strengthen the best match(es) this query surfaced so they
	// rank higher next time. Fire-and-forget — reinforcement must never add
	// latency to or fail the search — and gated on the feature being enabled AND
	// the caller wanting to count (the UI and the CLI-by-default set NoReinforce
	// so human browsing does not inflate the usage signal; the MCP agent leaves
	// it false so genuine recalls count).
	if s.cfg.RerankWeight > 0 && !req.Msg.GetNoReinforce() {
		s.reinforce(ts, hits)
	}

	out := &cortexv1.SearchResponse{Hits: make([]*cortexv1.Hit, 0, len(hits))}
	for _, h := range hits {
		out.Hits = append(out.Hits, hitToProto(h))
	}
	return connect.NewResponse(out), nil
}

// reinforce asynchronously bumps the accessCount / lastAccessedAt of the top
// ReinforceTopK hits a search returned — the write side of "living memory". It
// runs in its own goroutine with a background context (the request context is
// cancelled once Search returns) and is strictly best-effort: any failure is
// logged, never surfaced, since a missed reinforcement only weakens a ranking
// signal, it does not lose data. reinforceMu serialises the per-id read-modify-
// write so concurrent searches can't both read the same count and lose a bump.
// ts is the caller's tenant store — captured before launching the goroutine so
// the background op still targets the same tenant after the request context ends.
func (s *Service) reinforce(ts *store.TenantStore, hits []memory.Hit) {
	topK := s.cfg.ReinforceTopK
	if topK <= 0 {
		topK = 1
	}
	if topK > len(hits) {
		topK = len(hits)
	}
	ids := make([]string, 0, topK)
	for _, h := range hits[:topK] {
		if h.ID != "" {
			ids = append(ids, h.ID)
		}
	}
	if len(ids) == 0 {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.reinforceMu.Lock()
		defer s.reinforceMu.Unlock()
		for _, id := range ids {
			rec, ok, err := ts.Get(ctx, id)
			if err != nil {
				s.log.Warn("reinforce: get failed, skipping", "id", id, "err", err)
				continue
			}
			if !ok {
				continue // raced with a delete; nothing to reinforce
			}
			if err := ts.Reinforce(ctx, id, rec.AccessCount+1, time.Now().UTC()); err != nil {
				s.log.Warn("reinforce: write failed, skipping", "id", id, "err", err)
			}
		}
	}()
}

// SearchSimilar finds neighbours of an existing memory by reusing its stored
// vector (Weaviate nearObject) — it never calls the embedder, so it costs no
// inference regardless of how large the seed memory is. This is the backend for
// the UI's "find similar"; the seed memory itself is excluded by the store.
func (s *Service) SearchSimilar(ctx context.Context, req *connect.Request[cortexv1.SearchSimilarRequest]) (*connect.Response[cortexv1.SearchResponse], error) {
	id := strings.TrimSpace(req.Msg.GetId())
	if id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id must not be empty"))
	}

	ts := s.tenantStore(ctx)
	hits, err := ts.SearchByID(ctx, id, store.SearchOpts{
		Namespace:   resolveNamespace(req.Msg.GetNamespace(), s.cfg.DefaultNamespace),
		Limit:       int(req.Msg.GetLimit()),
		MaxDistance: req.Msg.GetMaxDistance(),
		Autocut:     int(req.Msg.GetAutocut()),
		IncludeTags: req.Msg.GetTags(),
		AnyTags:     req.Msg.GetAnyTags(),
		ExcludeTags: req.Msg.GetExcludeTags(),
	})
	if err != nil {
		return nil, err
	}

	out := &cortexv1.SearchResponse{Hits: make([]*cortexv1.Hit, 0, len(hits))}
	for _, h := range hits {
		out.Hits = append(out.Hits, hitToProto(h))
	}
	return connect.NewResponse(out), nil
}

func (s *Service) List(ctx context.Context, req *connect.Request[cortexv1.ListRequest]) (*connect.Response[cortexv1.ListResponse], error) {
	ts := s.tenantStore(ctx)
	recs, err := ts.List(ctx, store.ListOpts{
		Namespace:   resolveNamespace(req.Msg.GetNamespace(), s.cfg.DefaultNamespace),
		Limit:       int(req.Msg.GetLimit()),
		IncludeTags: req.Msg.GetTags(),
		ExcludeTags: req.Msg.GetExcludeTags(),
	})
	if err != nil {
		return nil, err
	}
	out := &cortexv1.ListResponse{Memories: make([]*cortexv1.Memory, 0, len(recs))}
	for _, r := range recs {
		out.Memories = append(out.Memories, recordToProto(r))
	}
	return connect.NewResponse(out), nil
}

func (s *Service) Delete(ctx context.Context, req *connect.Request[cortexv1.DeleteRequest]) (*connect.Response[cortexv1.DeleteResponse], error) {
	id := strings.TrimSpace(req.Msg.GetId())
	if id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id must not be empty"))
	}
	ts := s.tenantStore(ctx)
	if err := ts.Delete(ctx, id); err != nil {
		return nil, err
	}
	return connect.NewResponse(&cortexv1.DeleteResponse{Status: "deleted"}), nil
}

func (s *Service) Status(ctx context.Context, _ *connect.Request[cortexv1.StatusRequest]) (*connect.Response[cortexv1.StatusResponse], error) {
	out := &cortexv1.StatusResponse{
		Model:   s.embedder.Model(),
		Version: s.cfg.Version,
		NatsOk:  s.nc.IsConnected(),
	}
	if err := s.store.Ready(ctx); err == nil {
		out.WeaviateOk = true
		// Count scoped to the caller's tenant — a normal user sees their own count,
		// not a cross-tenant total (which would be a privacy leak in MT mode).
		ts := s.tenantStore(ctx)
		if c, err := ts.Count(ctx, ""); err == nil {
			out.MemoryCount = int64(c)
		}
	}
	if err := s.embedder.Reachable(ctx); err == nil {
		out.OllamaOk = true
		if present, err := s.embedder.HasModel(ctx); err == nil && present {
			out.ModelPresent = true
			if vec, err := s.embedder.Embed(ctx, "ping"); err == nil {
				out.Dims = int32(len(vec))
			}
		}
	}
	return connect.NewResponse(out), nil
}

// PullModel asks the local Ollama instance to download the configured embedding
// model. It blocks until the pull completes (which can take minutes).
func (s *Service) PullModel(ctx context.Context, _ *connect.Request[cortexv1.PullModelRequest]) (*connect.Response[cortexv1.PullModelResponse], error) {
	if err := s.embedder.Pull(ctx); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("pull model %q: %w", s.embedder.Model(), err))
	}
	return connect.NewResponse(&cortexv1.PullModelResponse{Model: s.embedder.Model(), Status: "pulled"}), nil
}

func (s *Service) Doctor(ctx context.Context, _ *connect.Request[cortexv1.DoctorRequest]) (*connect.Response[cortexv1.DoctorResponse], error) {
	var checks []*cortexv1.Check
	add := func(name string, err error, okDetail string) {
		c := &cortexv1.Check{Name: name, Ok: err == nil}
		if err != nil {
			c.Detail = err.Error()
		} else {
			c.Detail = okDetail
		}
		checks = append(checks, c)
	}

	if s.nc.IsConnected() {
		add("nats", nil, "connected to "+s.nc.ConnectedUrl())
	} else {
		add("nats", errors.New("not connected"), "")
	}

	weaviateErr := s.store.Ready(ctx)
	add("weaviate", weaviateErr, "ready")
	if weaviateErr == nil {
		ts := s.tenantStore(ctx)
		if c, err := ts.Count(ctx, ""); err != nil {
			add("store-query", err, "")
		} else {
			add("store-query", nil, fmt.Sprintf("%d memories stored", c))
		}
	}

	if vec, err := s.embedder.Embed(ctx, "ping"); err != nil {
		add("ollama", fmt.Errorf("embed probe failed (is model %q pulled?): %w", s.embedder.Model(), err), "")
	} else {
		add("ollama", nil, fmt.Sprintf("model %s, %d dims", s.embedder.Model(), len(vec)))
	}

	healthy := true
	for _, c := range checks {
		if !c.Ok {
			healthy = false
			break
		}
	}
	return connect.NewResponse(&cortexv1.DoctorResponse{Checks: checks, Healthy: healthy}), nil
}

func (s *Service) Reindex(ctx context.Context, req *connect.Request[cortexv1.ReindexRequest]) (*connect.Response[cortexv1.ReindexResponse], error) {
	ns := resolveNamespace(req.Msg.GetNamespace(), s.cfg.DefaultNamespace)

	ts := s.tenantStore(ctx)
	recs, err := ts.List(ctx, store.ListOpts{Namespace: ns, Limit: allLimit})
	if err != nil {
		return nil, err
	}
	if len(recs) == 0 {
		return connect.NewResponse(&cortexv1.ReindexResponse{Message: "no memories to reindex"}), nil
	}

	// 1. Always snapshot first — the safety net before any destructive rebuild.
	backupPath, err := s.writeBackup(recs)
	if err != nil {
		return nil, err
	}

	// 2. Probe the target dimension and compare to what is stored.
	probe, err := s.embedder.Embed(ctx, "dimension probe")
	if err != nil {
		return nil, fmt.Errorf("probe embed (is the worker's model %q pulled?): %w", s.embedder.Model(), err)
	}
	newDims := len(probe)
	oldDims := firstDims(recs)
	rebuilt := false

	if oldDims != 0 && oldDims != newDims {
		if ns != "" {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				fmt.Errorf("dimension change (%d -> %d) requires rebuilding the whole class; reindex with namespace \"*\"", oldDims, newDims))
		}
		if !req.Msg.GetForceRebuild() {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				fmt.Errorf("dimension change (%d -> %d) requires dropping and recreating the Memory class; retry with force_rebuild=true (backup written to %s)", oldDims, newDims, backupPath))
		}
		// In MT mode, dropping the class drops ALL tenants — a per-tenant rebuild
		// is not possible. Refuse the destructive rebuild so a single user cannot
		// accidentally wipe other tenants' data.
		if s.cfg.MultiTenant {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				fmt.Errorf("dimension-change rebuild drops ALL tenants' data in multi-tenant mode; this operation must be performed by an admin via `cortex migrate-mt` with the server offline"))
		}
		if err := s.store.DeleteClass(ctx); err != nil {
			return nil, err
		}
		if err := s.store.EnsureSchema(ctx); err != nil {
			return nil, err
		}
		rebuilt = true
	}

	// 3. Republish every record within this tenant; the worker re-embeds and
	// re-stamps provenance. PublishIndex stamps the tenant from context so the
	// worker's store ops land in the same tenant.
	for _, r := range recs {
		if err := bus.PublishIndex(ctx, s.js, r); err != nil {
			return nil, fmt.Errorf("republish %s: %w", r.ID, err)
		}
	}

	return connect.NewResponse(&cortexv1.ReindexResponse{
		Republished: int32(len(recs)),
		Rebuilt:     rebuilt,
		OldDims:     int32(oldDims),
		NewDims:     int32(newDims),
		BackupPath:  backupPath,
		Message:     fmt.Sprintf("republished %d memories for re-embedding", len(recs)),
	}), nil
}

func (s *Service) Dead(ctx context.Context, req *connect.Request[cortexv1.DeadRequest]) (*connect.Response[cortexv1.DeadResponse], error) {
	// The dead-letter list is a SHARED, cross-tenant operational view: the records
	// it returns carry the failed memory's text from EVERY tenant (the dead-letter
	// NATS subject is global, not per-tenant). In multi-tenant mode that content
	// must not reach a normal user, so the whole dead-letter surface (list/requeue/
	// purge) is admin-only. Single-user mode (flag off) is unaffected.
	if s.cfg.MultiTenant {
		if err := requireAdmin(ctx); err != nil {
			return nil, err
		}
	}
	dls, err := bus.FetchDead(ctx, s.js)
	if err != nil {
		return nil, err
	}

	switch req.Msg.GetAction() {
	case cortexv1.DeadAction_DEAD_ACTION_REQUEUE:
		for _, dl := range dls {
			// PublishIndex stamps the tenant from context; but the dead-lettered
			// record already carries UserID from the original publish — preserve it
			// by NOT passing context tenant (the goroutine re-stamp would overwrite).
			// We publish with the background context carrying NO identity so the
			// UserID on the record itself (already set) flows through unchanged.
			// Actually: PublishIndex stamps rec.UserID from context, overwriting.
			// We need to preserve the original tenant. The solution: the dead-letter
			// record's UserID is already set; we publish on a context that has that
			// identity, or we set the tenant before calling publish.
			// Cleanest: stamp identity onto a child ctx, then publish.
			reqCtx := identity.Into(ctx, identity.Identity{UserID: dl.Record.UserID})
			if err := bus.PublishIndex(reqCtx, s.js, dl.Record); err != nil {
				return nil, fmt.Errorf("requeue %s: %w", dl.Record.ID, err)
			}
		}
		if len(dls) > 0 {
			if err := bus.PurgeDead(ctx, s.js); err != nil {
				return nil, err
			}
		}
		return connect.NewResponse(&cortexv1.DeadResponse{Affected: int32(len(dls))}), nil

	case cortexv1.DeadAction_DEAD_ACTION_PURGE:
		if err := bus.PurgeDead(ctx, s.js); err != nil {
			return nil, err
		}
		return connect.NewResponse(&cortexv1.DeadResponse{Affected: int32(len(dls))}), nil

	default: // LIST / UNSPECIFIED
		out := &cortexv1.DeadResponse{DeadLetters: make([]*cortexv1.DeadLetter, 0, len(dls))}
		for _, dl := range dls {
			out.DeadLetters = append(out.DeadLetters, deadToProto(dl))
		}
		return connect.NewResponse(out), nil
	}
}

func (s *Service) IndexQueue(ctx context.Context, _ *connect.Request[cortexv1.IndexQueueRequest]) (*connect.Response[cortexv1.IndexQueueResponse], error) {
	// The index queue is a single shared NATS stream across all tenants, so its
	// depth is a global operational metric, not per-user. Admin-only in MT mode
	// (with the dead-letter view); unaffected in single-user mode.
	if s.cfg.MultiTenant {
		if err := requireAdmin(ctx); err != nil {
			return nil, err
		}
	}
	out := &cortexv1.IndexQueueResponse{}

	cons, err := s.js.Consumer(ctx, memory.StreamName, memory.ConsumerName)
	if err != nil {
		// No consumer yet means the worker has never started; report an empty
		// queue rather than failing the probe.
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			return connect.NewResponse(out), nil
		}
		return nil, err
	}
	out.ConsumerPresent = true

	info, err := cons.Info(ctx)
	if err != nil {
		return nil, err
	}
	out.Pending = int64(info.NumPending)
	out.InFlight = int64(info.NumAckPending)

	dls, err := bus.FetchDead(ctx, s.js)
	if err != nil {
		return nil, err
	}
	out.Dead = int64(len(dls))

	return connect.NewResponse(out), nil
}

func (s *Service) SummarizeSession(ctx context.Context, req *connect.Request[cortexv1.SummarizeSessionRequest]) (*connect.Response[cortexv1.SummarizeSessionResponse], error) {
	convID := strings.TrimSpace(req.Msg.GetConversationId())
	if convID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("conversation_id must not be empty"))
	}
	text := strings.TrimSpace(req.Msg.GetText())
	if text == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("text must not be empty"))
	}
	ns := req.Msg.GetNamespace()
	if ns == "" {
		ns = s.cfg.DefaultNamespace
	}
	src := req.Msg.GetSource()
	if src == "" {
		src = s.cfg.Source
	}

	now := time.Now().UTC()
	sum := memory.Summary{
		ConversationID: convID,
		Text:           text,
		Namespace:      ns,
		Source:         src,
		CreatedAt:      now, // worker only overwrites this on first index; replaces keep it simple
		UpdatedAt:      now,
	}
	// PublishSummary stamps tenant from context.
	if err := bus.PublishSummary(ctx, s.js, sum); err != nil {
		return nil, err
	}
	return connect.NewResponse(&cortexv1.SummarizeSessionResponse{ConversationId: convID, Status: "queued"}), nil
}

func (s *Service) RecallSession(ctx context.Context, req *connect.Request[cortexv1.RecallSessionRequest]) (*connect.Response[cortexv1.RecallSessionResponse], error) {
	query := strings.TrimSpace(req.Msg.GetQuery())
	if query == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("query must not be empty"))
	}

	vec, err := s.embedder.Embed(ctx, query)
	if err != nil {
		return nil, err
	}
	ts := s.tenantStore(ctx)
	// First hop: best-matching summary.
	hits, err := ts.SearchSummaries(ctx, vec, store.SummarySearchOpts{
		Namespace:   resolveNamespace(req.Msg.GetNamespace(), s.cfg.DefaultNamespace),
		Limit:       1,
		MaxDistance: req.Msg.GetMaxDistance(),
	})
	if err != nil {
		return nil, err
	}
	if len(hits) == 0 {
		return connect.NewResponse(&cortexv1.RecallSessionResponse{Matched: false}), nil
	}
	best := hits[0]

	// Second hop: the facts saved during that conversation (across namespaces —
	// a conversation ID is unique on its own).
	factLimit := int(req.Msg.GetFactLimit())
	if factLimit <= 0 {
		factLimit = 50
	}
	facts, err := ts.List(ctx, store.ListOpts{
		ConversationID: best.ConversationID,
		Limit:          factLimit,
	})
	if err != nil {
		return nil, err
	}

	out := &cortexv1.RecallSessionResponse{
		Matched: true,
		Summary: summaryToProto(best),
		Facts:   make([]*cortexv1.Memory, 0, len(facts)),
	}
	for _, f := range facts {
		out.Facts = append(out.Facts, recordToProto(f))
	}
	return connect.NewResponse(out), nil
}

func (s *Service) ListSummaries(ctx context.Context, req *connect.Request[cortexv1.ListSummariesRequest]) (*connect.Response[cortexv1.ListSummariesResponse], error) {
	ts := s.tenantStore(ctx)
	sums, err := ts.ListSummaries(ctx, store.SummaryListOpts{
		Namespace: resolveNamespace(req.Msg.GetNamespace(), s.cfg.DefaultNamespace),
		Limit:     int(req.Msg.GetLimit()),
	})
	if err != nil {
		return nil, err
	}
	out := &cortexv1.ListSummariesResponse{Summaries: make([]*cortexv1.ConversationSummary, 0, len(sums))}
	for _, sum := range sums {
		out.Summaries = append(out.Summaries, summaryToProto(memory.SummaryHit{Summary: sum}))
	}
	return connect.NewResponse(out), nil
}

// Link queues a bidirectional edge between two memories. It is async: the edge
// is published to SubjectLink and applied by the worker's idempotent link
// consumer, which waits for both endpoints to exist before writing — so an
// endpoint that is still indexing (or arrives out of order) no longer drops the
// link. The response carries no resolved link set because the edge has not been
// applied yet; the caller observes the result by re-reading the memory.
func (s *Service) Link(ctx context.Context, req *connect.Request[cortexv1.LinkRequest]) (*connect.Response[cortexv1.LinkResponse], error) {
	if err := s.publishEdge(ctx, memory.LinkOpAdd, req.Msg.GetId(), req.Msg.GetTargetId()); err != nil {
		return nil, err
	}
	return connect.NewResponse(&cortexv1.LinkResponse{}), nil
}

// Unlink queues removal of the edge between two memories, applied asynchronously
// by the same idempotent link consumer as Link.
func (s *Service) Unlink(ctx context.Context, req *connect.Request[cortexv1.UnlinkRequest]) (*connect.Response[cortexv1.UnlinkResponse], error) {
	if err := s.publishEdge(ctx, memory.LinkOpRemove, req.Msg.GetId(), req.Msg.GetTargetId()); err != nil {
		return nil, err
	}
	return connect.NewResponse(&cortexv1.UnlinkResponse{}), nil
}

// publishEdge validates the two endpoint ids and publishes the edge mutation to
// the link subject. It validates shape only (non-empty, distinct); endpoint
// existence is intentionally not checked here, because the whole point of the
// async path is to allow linking a memory that is not indexed yet.
// PublishLink stamps the tenant from context.
func (s *Service) publishEdge(ctx context.Context, op memory.LinkOp, idA, idB string) error {
	idA = strings.TrimSpace(idA)
	idB = strings.TrimSpace(idB)
	if idA == "" || idB == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("id and target_id must not be empty"))
	}
	if idA == idB {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("cannot link a memory to itself"))
	}
	return bus.PublishLink(ctx, s.js, op, idA, idB)
}

// ListDuplicateCandidates returns memories the worker flagged as likely
// duplicates, each resolved to the full candidate memories it resembles, for
// human/agent review. Candidates deleted since flagging are dropped; a group
// whose candidates have all vanished is omitted entirely.
func (s *Service) ListDuplicateCandidates(ctx context.Context, req *connect.Request[cortexv1.ListDuplicateCandidatesRequest]) (*connect.Response[cortexv1.ListDuplicateCandidatesResponse], error) {
	ts := s.tenantStore(ctx)
	flagged, err := ts.ListWithCandidates(ctx,
		resolveNamespace(req.Msg.GetNamespace(), s.cfg.DefaultNamespace),
		int(req.Msg.GetLimit()))
	if err != nil {
		return nil, err
	}

	out := &cortexv1.ListDuplicateCandidatesResponse{Groups: make([]*cortexv1.DuplicateGroup, 0, len(flagged))}
	for _, rec := range flagged {
		cands := make([]*cortexv1.Memory, 0, len(rec.DupCandidates))
		for _, id := range rec.DupCandidates {
			c, found, err := ts.Get(ctx, id)
			if err != nil {
				s.log.Warn("resolve dup candidate failed, skipping", "id", rec.ID, "candidate", id, "err", err)
				continue
			}
			if !found {
				continue // deleted since it was flagged
			}
			cands = append(cands, recordToProto(c))
		}
		if len(cands) == 0 {
			continue
		}
		out.Groups = append(out.Groups, &cortexv1.DuplicateGroup{
			Memory:     recordToProto(rec),
			Candidates: cands,
		})
	}
	return connect.NewResponse(out), nil
}

// DismissDuplicate records that two memories are confirmed NOT duplicates of
// each other. It writes the decision bidirectionally (so neither side re-flags
// the other when the worker recomputes candidates) and strips each from the
// other's current dupCandidates so the pair leaves the review list immediately.
// Shares linkMu with Link/Unlink to serialize the read-modify-write.
func (s *Service) DismissDuplicate(ctx context.Context, req *connect.Request[cortexv1.DismissDuplicateRequest]) (*connect.Response[cortexv1.DismissDuplicateResponse], error) {
	s.linkMu.Lock()
	defer s.linkMu.Unlock()

	ts := s.tenantStore(ctx)
	a, b, err := s.linkEndpoints(ctx, ts, req.Msg.GetId(), req.Msg.GetTargetId())
	if err != nil {
		return nil, err
	}

	aNot := addUnique(a.NotDuplicateOf, b.ID)
	bNot := addUnique(b.NotDuplicateOf, a.ID)
	if err := ts.SetNotDuplicateOf(ctx, a.ID, aNot); err != nil {
		return nil, fmt.Errorf("dismiss %s<->%s: updating %s failed (retry): %w", a.ID, b.ID, a.ID, err)
	}
	if err := ts.SetNotDuplicateOf(ctx, b.ID, bNot); err != nil {
		return nil, fmt.Errorf("dismiss %s<->%s: updating %s failed (graph may be left asymmetric, retry): %w", a.ID, b.ID, b.ID, err)
	}

	// Best-effort immediate cleanup of the review list; the durable guarantee is
	// notDuplicateOf above, which the worker honours on the next reindex.
	if err := ts.SetDupCandidates(ctx, a.ID, removeString(a.DupCandidates, b.ID)); err != nil {
		s.log.Warn("dismiss: clearing dupCandidates failed", "id", a.ID, "err", err)
	}
	if err := ts.SetDupCandidates(ctx, b.ID, removeString(b.DupCandidates, a.ID)); err != nil {
		s.log.Warn("dismiss: clearing dupCandidates failed", "id", b.ID, "err", err)
	}

	return connect.NewResponse(&cortexv1.DismissDuplicateResponse{NotDuplicateOf: aNot}), nil
}

// consolidateDefaultLimit caps how many memories Consolidate gathers when the
// request omits a limit: large enough to pull a whole topic cluster, small
// enough to stay within a client's context budget.
const consolidateDefaultLimit = 25

// Consolidate gathers the cluster of memories about a topic for a client (the
// LLM) to merge, and is strictly READ-ONLY. The cluster is the topic's vector
// matches (most relevant first) expanded with each match's duplicate-candidate
// and linked neighbours, deduped and capped at limit. The client merges the
// cluster into fewer, richer memories and commits the result via
// Save(supersedes=...) using the returned manifest; the worker then deletes the
// superseded sources once the replacement is durably indexed. This RPC never
// writes or deletes — gathering and committing are deliberately separate so the
// destructive step only happens behind a durable save.
func (s *Service) Consolidate(ctx context.Context, req *connect.Request[cortexv1.ConsolidateRequest]) (*connect.Response[cortexv1.ConsolidateResponse], error) {
	topic := strings.TrimSpace(req.Msg.GetTopic())
	if topic == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("topic must not be empty"))
	}
	limit := int(req.Msg.GetLimit())
	if limit <= 0 {
		limit = consolidateDefaultLimit
	}

	vec, err := s.embedder.Embed(ctx, topic)
	if err != nil {
		return nil, err
	}
	ts := s.tenantStore(ctx)
	includeTags, anyTags, excludeTags := req.Msg.GetTags(), req.Msg.GetAnyTags(), req.Msg.GetExcludeTags()
	seeds, err := s.searchStore(ctx, ts, vec, store.SearchOpts{
		Namespace:   resolveNamespace(req.Msg.GetNamespace(), s.cfg.DefaultNamespace),
		Limit:       limit,
		MaxDistance: req.Msg.GetMaxDistance(),
		IncludeTags: includeTags,
		AnyTags:     anyTags,
		ExcludeTags: excludeTags,
		Query:       topic, // hybrid: a topic that is a literal token still gathers its cluster
		Alpha:       s.cfg.SearchAlpha,
	})
	if err != nil {
		return nil, err
	}

	// Expanded neighbours (links/dup candidates) are fetched by id, so the store's
	// tag filter never saw them — enforce the same scope here. This keeps the
	// whole cluster (and therefore the supersede-able manifest) inside the
	// requested tags: a tag-scoped consolidation must never pull an out-of-scope
	// memory into the set a merge can then delete.
	keep := tagFilter(includeTags, anyTags, excludeTags)
	cluster := assembleCluster(seeds, limit, func(id string) (memory.Record, bool, error) {
		r, found, err := ts.Get(ctx, id)
		if err != nil || !found || !keep(r.Tags) {
			return memory.Record{}, false, err
		}
		return r, true, nil
	})

	out := &cortexv1.ConsolidateResponse{
		Cluster:  make([]*cortexv1.Memory, 0, len(cluster)),
		Manifest: make([]string, 0, len(cluster)),
	}
	for _, r := range cluster {
		out.Cluster = append(out.Cluster, recordToProto(r))
		out.Manifest = append(out.Manifest, r.ID)
	}
	return connect.NewResponse(out), nil
}

// tagFilter returns a predicate matching the store's tag semantics (buildWhere +
// excludeTagged), for re-applying the same scope to records fetched by id during
// cluster expansion: must carry every include tag, at least one anyOf tag (when
// any are given), and none of the exclude tags. With no tags it admits everything.
func tagFilter(include, anyOf, exclude []string) func([]string) bool {
	if len(include) == 0 && len(anyOf) == 0 && len(exclude) == 0 {
		return func([]string) bool { return true }
	}
	return func(tags []string) bool {
		have := make(map[string]bool, len(tags))
		for _, t := range tags {
			have[t] = true
		}
		for _, t := range include {
			if !have[t] {
				return false
			}
		}
		for _, t := range exclude {
			if have[t] {
				return false
			}
		}
		if len(anyOf) > 0 {
			matched := false
			for _, t := range anyOf {
				if have[t] {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		}
		return true
	}
}

// assembleCluster builds the consolidation cluster: each seed hit (most relevant
// first) immediately followed by its own duplicate-candidate and linked
// neighbours, deduped and capped at limit. Only the seeds are expanded, so the
// cluster stays one hop from the topic and bounded. get resolves a neighbour id
// to its full record; a neighbour that errors or no longer exists is skipped.
// Pure but for get, so the dedup/cap/expansion contract is unit-testable with a
// fake getter.
//
// Seeds and their neighbours are INTERLEAVED (seed, its neighbours, next seed,
// …) rather than "all seeds, then all neighbours". The old ordering let a topic
// with `limit` matches of its own saturate the budget so that NOT ONE linked or
// duplicate neighbour was ever pulled in — the links were silently dropped.
// Interleaving guarantees the graph around the top matches is gathered before
// lower-ranked seeds consume the budget, while still degrading to "all seeds in
// order" when nothing has neighbours (no regression for the pure-dedup case).
func assembleCluster(seeds []memory.Hit, limit int, get func(string) (memory.Record, bool, error)) []memory.Record {
	if limit <= 0 {
		return nil
	}
	seen := make(map[string]bool, limit)
	cluster := make([]memory.Record, 0, limit)
	add := func(r memory.Record) {
		if r.ID == "" || seen[r.ID] || len(cluster) >= limit {
			return
		}
		seen[r.ID] = true
		cluster = append(cluster, r)
	}

	for _, h := range seeds {
		if len(cluster) >= limit {
			break
		}
		add(h.Record)
		// Duplicate candidates first — surfacing them for merge is the whole point
		// of consolidation — then explicit links.
		for _, id := range append(append([]string(nil), h.DupCandidates...), h.LinkedIDs...) {
			if len(cluster) >= limit {
				break
			}
			if id == "" || seen[id] {
				continue
			}
			r, found, err := get(id)
			if err != nil || !found {
				continue
			}
			add(r)
		}
	}
	return cluster
}

// RestoreMemories re-ingests a dump by publishing each record onto the SAME NATS
// index queue a save uses — no write bypass, so the worker re-embeds and upserts
// them with all the usual durability (retries, dead-lettering). Vectors are not
// carried; the worker recomputes them with its current model, so a restore is
// safe across model/dimension changes. A record with empty text is skipped
// (nothing to embed); a missing id/namespace/source is filled from defaults.
//
// SECURITY: the import lands in the CALLER'S tenant. PublishIndex stamps the
// tenant from context — no req.Msg field can override it. A user importing a
// backup can only import into their own tenant.
func (s *Service) RestoreMemories(ctx context.Context, req *connect.Request[cortexv1.RestoreMemoriesRequest]) (*connect.Response[cortexv1.RestoreMemoriesResponse], error) {
	queued := 0
	for _, m := range req.Msg.GetMemories() {
		rec := protoToRecord(m)
		rec.Text = strings.TrimSpace(rec.Text)
		if rec.Text == "" {
			continue // can't embed an empty memory
		}
		if rec.ID == "" {
			rec.ID = uuid.NewString()
		}
		if rec.Namespace == "" {
			rec.Namespace = s.cfg.DefaultNamespace
		}
		if rec.Source == "" {
			rec.Source = s.cfg.Source
		}
		if rec.CreatedAt.IsZero() {
			rec.CreatedAt = time.Now().UTC()
		}
		// PublishIndex stamps rec.UserID from context — the caller's tenant.
		if err := bus.PublishIndex(ctx, s.js, rec); err != nil {
			return nil, fmt.Errorf("restore publish %s: %w", rec.ID, err)
		}
		queued++
	}
	return connect.NewResponse(&cortexv1.RestoreMemoriesResponse{Queued: int32(queued)}), nil
}

// ListNamespaces aggregates the stored namespaces with per-namespace memory and
// summary counts and the most recent activity, for the UI's namespace admin view.
// Scoped to the caller's tenant — a user sees only their own namespaces.
func (s *Service) ListNamespaces(ctx context.Context, _ *connect.Request[cortexv1.ListNamespacesRequest]) (*connect.Response[cortexv1.ListNamespacesResponse], error) {
	ts := s.tenantStore(ctx)
	stats, err := ts.ListNamespaces(ctx)
	if err != nil {
		return nil, err
	}
	out := &cortexv1.ListNamespacesResponse{Namespaces: make([]*cortexv1.NamespaceInfo, 0, len(stats))}
	for _, st := range stats {
		out.Namespaces = append(out.Namespaces, namespaceStatToProto(st))
	}
	return connect.NewResponse(out), nil
}

// RenameNamespace moves every memory and conversation summary from one namespace
// to another. It is metadata-only (namespace is never embedded) so nothing is
// re-embedded; renaming into an existing namespace merges the two. Scoped to the
// caller's tenant — a user cannot rename namespaces belonging to another user.
func (s *Service) RenameNamespace(ctx context.Context, req *connect.Request[cortexv1.RenameNamespaceRequest]) (*connect.Response[cortexv1.RenameNamespaceResponse], error) {
	from := strings.TrimSpace(req.Msg.GetFrom())
	to := strings.TrimSpace(req.Msg.GetTo())
	if from == "" || to == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("from and to must not be empty"))
	}
	if from == to {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("from and to must differ"))
	}
	ts := s.tenantStore(ctx)
	mem, sum, err := ts.RenameNamespace(ctx, from, to)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&cortexv1.RenameNamespaceResponse{
		MemoriesUpdated:  int32(mem),
		SummariesUpdated: int32(sum),
	}), nil
}

// DeleteNamespace permanently deletes every memory and conversation summary in a
// namespace. Scoped to the caller's tenant — a user can only delete their own
// namespaces, never another user's.
func (s *Service) DeleteNamespace(ctx context.Context, req *connect.Request[cortexv1.DeleteNamespaceRequest]) (*connect.Response[cortexv1.DeleteNamespaceResponse], error) {
	ns := strings.TrimSpace(req.Msg.GetNamespace())
	if ns == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("namespace must not be empty"))
	}
	ts := s.tenantStore(ctx)
	mem, sum, err := ts.DeleteNamespace(ctx, ns)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&cortexv1.DeleteNamespaceResponse{
		MemoriesDeleted:  int32(mem),
		SummariesDeleted: int32(sum),
	}), nil
}

// linkEndpoints validates two distinct, existing memory ids and returns both
// records (with their current link sets). Shared by Link and Unlink.
func (s *Service) linkEndpoints(ctx context.Context, ts *store.TenantStore, idA, idB string) (memory.Record, memory.Record, error) {
	idA = strings.TrimSpace(idA)
	idB = strings.TrimSpace(idB)
	if idA == "" || idB == "" {
		return memory.Record{}, memory.Record{}, connect.NewError(connect.CodeInvalidArgument, errors.New("id and target_id must not be empty"))
	}
	if idA == idB {
		return memory.Record{}, memory.Record{}, connect.NewError(connect.CodeInvalidArgument, errors.New("cannot link a memory to itself"))
	}
	a, ok, err := ts.Get(ctx, idA)
	if err != nil {
		return memory.Record{}, memory.Record{}, err
	}
	if !ok {
		return memory.Record{}, memory.Record{}, connect.NewError(connect.CodeNotFound, fmt.Errorf("memory %s not found", idA))
	}
	b, ok, err := ts.Get(ctx, idB)
	if err != nil {
		return memory.Record{}, memory.Record{}, err
	}
	if !ok {
		return memory.Record{}, memory.Record{}, connect.NewError(connect.CodeNotFound, fmt.Errorf("memory %s not found", idB))
	}
	return a, b, nil
}

// addUnique appends v to xs if absent, preserving order.
func addUnique(xs []string, v string) []string {
	if slices.Contains(xs, v) {
		return xs
	}
	return append(xs, v)
}

// removeString returns xs without any element equal to v.
func removeString(xs []string, v string) []string {
	out := make([]string, 0, len(xs))
	for _, x := range xs {
		if x != v {
			out = append(out, x)
		}
	}
	return out
}

// MigrateMT performs the one-shot migration from a non-MT store to an MT store.
// It is a server-side operation because only the server owns Weaviate. The CLI
// command `cortex migrate-mt` calls this RPC; the server does all the work.
//
// Migration steps:
//  1. Guard: CORTEX_MULTI_TENANT must be on and the Memory class must NOT already
//     be MT (if it is, there is nothing to migrate).
//  2. Snapshot: list every memory and summary from the non-MT store and write a
//     backup JSON file (reusing writeBackup) — the safety net before anything
//     destructive.
//  3. Drop the three non-MT classes + recreate them with MT enabled (EnsureSchema
//     in MT mode; the store was booted with SetMultiTenant(true)).
//  4. Re-import every snapshotted memory into the bootstrap tenant via
//     PublishIndex and every summary via PublishSummary. The worker re-embeds and
//     rechunks on import. Chunks are NOT migrated — they are always regenerated.
//  5. Return counts + backup path so the operator can confirm.
//
// Idempotency: the guard refuses if the classes are already MT. If the migration
// crashes after step 3 but before completing step 4, the operator can use
// `cortex import <backup>` against the now-MT server to finish the re-import.
func (s *Service) MigrateMT(ctx context.Context, _ *connect.Request[cortexv1.MigrateMTRequest]) (*connect.Response[cortexv1.MigrateMTResponse], error) {
	// Guard 0: this is a destructive, global rebuild — admin only.
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	// The migrated data lands in the CALLING admin's tenant — the admin running
	// migrate-mt is who the legacy single-user store now belongs to (and is the
	// tenant their own JWT/api-key resolves to, so they actually see the data).
	// Using the caller's id, not a fixed sentinel, is what makes the migrated
	// memories visible to the admin afterwards.
	caller, _ := identity.From(ctx)
	targetTenant := caller.UserID

	// Guard 1: multi-tenancy flag must be on.
	if !s.cfg.MultiTenant {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("CORTEX_MULTI_TENANT is not enabled; set it to true and restart the server before migrating"))
	}

	// Guard 2: refuse if Memory class is already MT — nothing to migrate.
	isMT, err := s.store.IsClassMultiTenant(ctx, memory.ClassName)
	if err != nil {
		return nil, fmt.Errorf("check MT status: %w", err)
	}
	if isMT {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("the Memory class is already multi-tenant — nothing to migrate; this migration is a one-shot operation"))
	}

	// Snapshot: list all records and summaries from the non-MT store.
	// ListAllRecords/ListAllSummaries issue no .WithTenant call — correct for a
	// non-MT class. Do NOT call these after MT is enabled.
	recs, err := s.store.ListAllRecords(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot memories: %w", err)
	}
	sums, err := s.store.ListAllSummaries(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot summaries: %w", err)
	}

	// Write the backup before anything destructive. Uses the same cortex export
	// JSON format so the backup can be imported with `cortex import` if needed.
	backupPath, err := s.writeBackup(recs)
	if err != nil {
		return nil, fmt.Errorf("write backup before migration: %w", err)
	}
	s.log.Info("migrate-mt: snapshot complete",
		"memories", len(recs), "summaries", len(sums), "backup", backupPath)

	// Drop the three non-MT classes and recreate them with MT enabled.
	// The store was booted with SetMultiTenant(true), so EnsureSchema creates
	// MT classes. WARNING: DeleteClass is irreversible; the backup is the
	// recovery path.
	if err := s.store.DeleteClass(ctx); err != nil {
		return nil, fmt.Errorf("drop non-MT classes: %w", err)
	}
	if err := s.store.EnsureSchema(ctx); err != nil {
		return nil, fmt.Errorf("recreate MT classes: %w", err)
	}
	s.log.Info("migrate-mt: classes rebuilt with MT enabled")

	// Re-import every record into the bootstrap admin's tenant.
	// Build a context carrying the bootstrap identity so PublishIndex stamps
	// the correct UserID on the NATS payload.
	bootstrapCtx := identity.Into(ctx, identity.Identity{
		UserID:   targetTenant,
		Username: caller.Username,
		Role:     identity.RoleAdmin,
	})

	memoriesQueued := 0
	for _, r := range recs {
		if strings.TrimSpace(r.Text) == "" {
			continue // worker cannot embed an empty record; skip like RestoreMemories
		}
		if err := bus.PublishIndex(bootstrapCtx, s.js, r); err != nil {
			return nil, fmt.Errorf("re-import memory %s: %w", r.ID, err)
		}
		memoriesQueued++
	}

	summariesQueued := 0
	for _, sum := range sums {
		if strings.TrimSpace(sum.Text) == "" {
			continue
		}
		if err := bus.PublishSummary(bootstrapCtx, s.js, sum); err != nil {
			return nil, fmt.Errorf("re-import summary %s: %w", sum.ConversationID, err)
		}
		summariesQueued++
	}

	s.log.Info("migrate-mt: re-import queued",
		"memories_queued", memoriesQueued,
		"summaries_queued", summariesQueued,
		"tenant", targetTenant,
	)

	return connect.NewResponse(&cortexv1.MigrateMTResponse{
		BackupPath:        backupPath,
		MemoriesExported:  int32(len(recs)),
		SummariesExported: int32(len(sums)),
		MemoriesQueued:    int32(memoriesQueued),
		SummariesQueued:   int32(summariesQueued),
		Tenant:            targetTenant,
		Message: fmt.Sprintf(
			"migration complete: %d memories and %d summaries queued for re-import into tenant %q; "+
				"backup at %s; chunks will regenerate as the worker processes the queue",
			memoriesQueued, summariesQueued, targetTenant, backupPath,
		),
	}), nil
}

// writeBackup snapshots records (text + metadata, no vectors) to a timestamped
// JSON file in the configured backup dir, returning its path.
func (s *Service) writeBackup(recs []memory.Record) (string, error) {
	dir := s.cfg.BackupDir
	if dir == "" {
		dir = "."
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("backup dir: %w", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("cortex-backup-%s.json", time.Now().UTC().Format("20060102-150405")))
	data, err := json.MarshalIndent(recs, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", fmt.Errorf("write backup: %w", err)
	}
	s.log.Info("wrote reindex backup", "path", path, "count", len(recs))
	return path, nil
}

// firstDims returns the dims of the first record that carries provenance, or 0.
func firstDims(recs []memory.Record) int {
	for _, r := range recs {
		if r.Dims > 0 {
			return r.Dims
		}
	}
	return 0
}
