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
	// linkMu serializes Link/Unlink to prevent lost updates from concurrent
	// read-modify-write on each memory's linked-IDs list.
	linkMu sync.Mutex
}

// NewService wires the handler with its backing clients.
func NewService(nc *nats.Conn, js jetstream.JetStream, st *store.Store, embedder *embed.Client, cfg Config, log *slog.Logger) *Service {
	return &Service{nc: nc, js: js, store: st, embedder: embedder, cfg: cfg, log: log}
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
		LinkTo:         req.Msg.GetLinkTo(),
		Supersedes:     req.Msg.GetSupersedes(),
	}
	if err := bus.PublishIndex(ctx, s.js, rec); err != nil {
		return nil, err
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

	rec, ok, err := s.store.Get(ctx, id)
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
	// stale set so a failed republish never leaves an old hint behind. LinkTo is a
	// one-shot save instruction, never relevant to an edit.
	rec.DupCandidates = nil
	rec.LinkTo = nil

	if err := bus.PublishIndex(ctx, s.js, rec); err != nil {
		return nil, err
	}
	return connect.NewResponse(&cortexv1.UpdateMemoryResponse{Id: id, Status: "queued"}), nil
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
	hits, err := s.store.Search(ctx, vec, store.SearchOpts{
		Namespace:   resolveNamespace(req.Msg.GetNamespace(), s.cfg.DefaultNamespace),
		Limit:       int(req.Msg.GetLimit()),
		MaxDistance: req.Msg.GetMaxDistance(),
		Autocut:     int(req.Msg.GetAutocut()),
		IncludeTags: req.Msg.GetTags(),
		AnyTags:     req.Msg.GetAnyTags(),
		ExcludeTags: req.Msg.GetExcludeTags(),
		Query:       query, // enables hybrid (BM25 + vector) so exact tokens resolve
		Alpha:       s.cfg.SearchAlpha,
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

// SearchSimilar finds neighbours of an existing memory by reusing its stored
// vector (Weaviate nearObject) — it never calls the embedder, so it costs no
// inference regardless of how large the seed memory is. This is the backend for
// the UI's "find similar"; the seed memory itself is excluded by the store.
func (s *Service) SearchSimilar(ctx context.Context, req *connect.Request[cortexv1.SearchSimilarRequest]) (*connect.Response[cortexv1.SearchResponse], error) {
	id := strings.TrimSpace(req.Msg.GetId())
	if id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id must not be empty"))
	}

	hits, err := s.store.SearchByID(ctx, id, store.SearchOpts{
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
	recs, err := s.store.List(ctx, store.ListOpts{
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
	if err := s.store.Delete(ctx, id); err != nil {
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
		if c, err := s.store.Count(ctx, ""); err == nil {
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
		if c, err := s.store.Count(ctx, ""); err != nil {
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

	recs, err := s.store.List(ctx, store.ListOpts{Namespace: ns, Limit: allLimit})
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
		if err := s.store.DeleteClass(ctx); err != nil {
			return nil, err
		}
		if err := s.store.EnsureSchema(ctx); err != nil {
			return nil, err
		}
		rebuilt = true
	}

	// 3. Republish every record; the worker re-embeds and re-stamps provenance.
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
	dls, err := bus.FetchDead(ctx, s.js)
	if err != nil {
		return nil, err
	}

	switch req.Msg.GetAction() {
	case cortexv1.DeadAction_DEAD_ACTION_REQUEUE:
		for _, dl := range dls {
			if err := bus.PublishIndex(ctx, s.js, dl.Record); err != nil {
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
	// First hop: best-matching summary.
	hits, err := s.store.SearchSummaries(ctx, vec, store.SummarySearchOpts{
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
	facts, err := s.store.List(ctx, store.ListOpts{
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
	sums, err := s.store.ListSummaries(ctx, store.SummaryListOpts{
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

func (s *Service) Link(ctx context.Context, req *connect.Request[cortexv1.LinkRequest]) (*connect.Response[cortexv1.LinkResponse], error) {
	s.linkMu.Lock()
	defer s.linkMu.Unlock()

	a, b, err := s.linkEndpoints(ctx, req.Msg.GetId(), req.Msg.GetTargetId())
	if err != nil {
		return nil, err
	}
	aLinks := addUnique(a.LinkedIDs, b.ID)
	bLinks := addUnique(b.LinkedIDs, a.ID)
	if err := s.store.SetLinks(ctx, a.ID, aLinks); err != nil {
		return nil, fmt.Errorf("link %s<->%s: updating %s failed (graph may be left asymmetric, retry): %w", a.ID, b.ID, a.ID, err)
	}
	if err := s.store.SetLinks(ctx, b.ID, bLinks); err != nil {
		return nil, fmt.Errorf("link %s<->%s: updating %s failed (graph may be left asymmetric, retry): %w", a.ID, b.ID, b.ID, err)
	}
	return connect.NewResponse(&cortexv1.LinkResponse{LinkedIds: aLinks}), nil
}

func (s *Service) Unlink(ctx context.Context, req *connect.Request[cortexv1.UnlinkRequest]) (*connect.Response[cortexv1.UnlinkResponse], error) {
	s.linkMu.Lock()
	defer s.linkMu.Unlock()

	a, b, err := s.linkEndpoints(ctx, req.Msg.GetId(), req.Msg.GetTargetId())
	if err != nil {
		return nil, err
	}
	aLinks := removeString(a.LinkedIDs, b.ID)
	bLinks := removeString(b.LinkedIDs, a.ID)
	if err := s.store.SetLinks(ctx, a.ID, aLinks); err != nil {
		return nil, fmt.Errorf("link %s<->%s: updating %s failed (graph may be left asymmetric, retry): %w", a.ID, b.ID, a.ID, err)
	}
	if err := s.store.SetLinks(ctx, b.ID, bLinks); err != nil {
		return nil, fmt.Errorf("link %s<->%s: updating %s failed (graph may be left asymmetric, retry): %w", a.ID, b.ID, b.ID, err)
	}
	return connect.NewResponse(&cortexv1.UnlinkResponse{LinkedIds: aLinks}), nil
}

// ListDuplicateCandidates returns memories the worker flagged as likely
// duplicates, each resolved to the full candidate memories it resembles, for
// human/agent review. Candidates deleted since flagging are dropped; a group
// whose candidates have all vanished is omitted entirely.
func (s *Service) ListDuplicateCandidates(ctx context.Context, req *connect.Request[cortexv1.ListDuplicateCandidatesRequest]) (*connect.Response[cortexv1.ListDuplicateCandidatesResponse], error) {
	flagged, err := s.store.ListWithCandidates(ctx,
		resolveNamespace(req.Msg.GetNamespace(), s.cfg.DefaultNamespace),
		int(req.Msg.GetLimit()))
	if err != nil {
		return nil, err
	}

	out := &cortexv1.ListDuplicateCandidatesResponse{Groups: make([]*cortexv1.DuplicateGroup, 0, len(flagged))}
	for _, rec := range flagged {
		cands := make([]*cortexv1.Memory, 0, len(rec.DupCandidates))
		for _, id := range rec.DupCandidates {
			c, found, err := s.store.Get(ctx, id)
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

	a, b, err := s.linkEndpoints(ctx, req.Msg.GetId(), req.Msg.GetTargetId())
	if err != nil {
		return nil, err
	}

	aNot := addUnique(a.NotDuplicateOf, b.ID)
	bNot := addUnique(b.NotDuplicateOf, a.ID)
	if err := s.store.SetNotDuplicateOf(ctx, a.ID, aNot); err != nil {
		return nil, fmt.Errorf("dismiss %s<->%s: updating %s failed (retry): %w", a.ID, b.ID, a.ID, err)
	}
	if err := s.store.SetNotDuplicateOf(ctx, b.ID, bNot); err != nil {
		return nil, fmt.Errorf("dismiss %s<->%s: updating %s failed (graph may be left asymmetric, retry): %w", a.ID, b.ID, b.ID, err)
	}

	// Best-effort immediate cleanup of the review list; the durable guarantee is
	// notDuplicateOf above, which the worker honours on the next reindex.
	if err := s.store.SetDupCandidates(ctx, a.ID, removeString(a.DupCandidates, b.ID)); err != nil {
		s.log.Warn("dismiss: clearing dupCandidates failed", "id", a.ID, "err", err)
	}
	if err := s.store.SetDupCandidates(ctx, b.ID, removeString(b.DupCandidates, a.ID)); err != nil {
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
	includeTags, anyTags, excludeTags := req.Msg.GetTags(), req.Msg.GetAnyTags(), req.Msg.GetExcludeTags()
	seeds, err := s.store.Search(ctx, vec, store.SearchOpts{
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
		r, found, err := s.store.Get(ctx, id)
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

// assembleCluster builds the consolidation cluster: the seed hits (most relevant
// first) followed by their duplicate-candidate and linked neighbours, deduped
// and capped at limit. Only the seeds are expanded, so the cluster stays one hop
// from the topic and bounded. get resolves a neighbour id to its full record; a
// neighbour that errors or no longer exists is skipped. Pure but for get, so the
// dedup/cap/expansion contract is unit-testable with a fake getter.
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
			return cluster
		}
		add(h.Record)
	}
	for _, h := range seeds {
		// Duplicate candidates first — surfacing them for merge is the whole point
		// of consolidation — then explicit links.
		for _, id := range append(append([]string(nil), h.DupCandidates...), h.LinkedIDs...) {
			if len(cluster) >= limit {
				return cluster
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

// linkEndpoints validates two distinct, existing memory ids and returns both
// records (with their current link sets). Shared by Link and Unlink.
func (s *Service) linkEndpoints(ctx context.Context, idA, idB string) (memory.Record, memory.Record, error) {
	idA = strings.TrimSpace(idA)
	idB = strings.TrimSpace(idB)
	if idA == "" || idB == "" {
		return memory.Record{}, memory.Record{}, connect.NewError(connect.CodeInvalidArgument, errors.New("id and target_id must not be empty"))
	}
	if idA == idB {
		return memory.Record{}, memory.Record{}, connect.NewError(connect.CodeInvalidArgument, errors.New("cannot link a memory to itself"))
	}
	a, ok, err := s.store.Get(ctx, idA)
	if err != nil {
		return memory.Record{}, memory.Record{}, err
	}
	if !ok {
		return memory.Record{}, memory.Record{}, connect.NewError(connect.CodeNotFound, fmt.Errorf("memory %s not found", idA))
	}
	b, ok, err := s.store.Get(ctx, idB)
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
