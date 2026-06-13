package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
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
	DefaultNamespace string // namespace stamped when a request omits one
	Source           string // source stamped when a request omits one
	Version          string // reported by Status
	BackupDir        string // where Reindex writes its safety snapshot
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
	}
	if err := bus.PublishIndex(ctx, s.js, rec); err != nil {
		return nil, err
	}
	return connect.NewResponse(&cortexv1.SaveResponse{Id: rec.ID, Status: "queued"}), nil
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
		Model:    s.embedder.Model(),
		Version:  s.cfg.Version,
		NatsOk:   s.nc.IsConnected(),
	}
	if err := s.store.Ready(ctx); err == nil {
		out.WeaviateOk = true
		if c, err := s.store.Count(ctx, ""); err == nil {
			out.MemoryCount = int64(c)
		}
	}
	if vec, err := s.embedder.Embed(ctx, "ping"); err == nil {
		out.OllamaOk = true
		out.Dims = int32(len(vec))
	}
	return connect.NewResponse(out), nil
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
	for _, x := range xs {
		if x == v {
			return xs
		}
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
