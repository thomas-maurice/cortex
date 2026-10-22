package rpc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"connectrpc.com/connect"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/recall"
	"github.com/thomas-maurice/cortex/internal/store"
)

// Bounds on PreviewRecall inputs. The prompt cap and chunk cap bound the embed
// + search fan-out one call can trigger (each query chunk is embedded once and
// searched twice).
const (
	previewMaxPromptBytes = 64 * 1024
	previewMaxLimit       = 50
	previewMaxMinChars    = 10000
	previewMaxQueryChunks = 32
	previewMaxDistanceCap = 2 // cosine distance range is [0, 2]

	// previewDedupNote is returned on every response: the hook's per-session
	// dedup lives in a bbolt file on the client, which the server cannot see.
	previewDedupNote = "Per-session dedup is not simulated: the hook skips memories it already injected earlier in the same Claude Code session (client-local state the server cannot see), so a real session may inject fewer."
)

func invalidArg(format string, a ...any) error {
	return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(format, a...))
}

// previewParams validates a PreviewRecall request and applies the hook defaults
// to unset optional fields.
func previewParams(req *cortexv1.PreviewRecallRequest) (recall.Params, error) {
	p := recall.Params{
		Limit:          recall.DefaultLimit,
		MaxDistance:    recall.DefaultMaxDistance,
		MinChars:       recall.DefaultMinChars,
		MaxQueryChunks: recall.DefaultMaxQueryChunks,
	}
	if req.Limit != nil {
		p.Limit = int(*req.Limit)
		if p.Limit < 1 || p.Limit > previewMaxLimit {
			return p, invalidArg("limit must be between 1 and %d", previewMaxLimit)
		}
	}
	if req.MaxDistance != nil {
		p.MaxDistance = *req.MaxDistance
		// !(x >= 0) also rejects NaN.
		if !(p.MaxDistance >= 0) || p.MaxDistance > previewMaxDistanceCap {
			return p, invalidArg("max_distance must be between 0 and %d (0 = no cutoff)", previewMaxDistanceCap)
		}
	}
	if req.MinChars != nil {
		p.MinChars = int(*req.MinChars)
		if p.MinChars < 0 || p.MinChars > previewMaxMinChars {
			return p, invalidArg("min_chars must be between 0 and %d", previewMaxMinChars)
		}
	}
	if req.MaxQueryChunks != nil {
		p.MaxQueryChunks = int(*req.MaxQueryChunks)
		if p.MaxQueryChunks < 0 || p.MaxQueryChunks > previewMaxQueryChunks {
			return p, invalidArg("max_query_chunks must be between 0 and %d (0 = every chunk, capped)", previewMaxQueryChunks)
		}
	}
	// "Every chunk" is still bounded: the hook's 0 would fan out unboundedly.
	if p.MaxQueryChunks == 0 {
		p.MaxQueryChunks = previewMaxQueryChunks
	}
	return p, nil
}

// previewSearcher is the server-side recall.Searcher: it embeds the query and
// searches the caller's tenant exactly like Search does, but never reinforces.
// Vectors are cached per query text because Preview searches every chunk twice.
type previewSearcher struct {
	s         *Service
	ts        *store.TenantStore
	namespace string

	mu   sync.Mutex
	vecs map[string][]float32
}

func (p *previewSearcher) vector(ctx context.Context, q string) ([]float32, error) {
	p.mu.Lock()
	v, ok := p.vecs[q]
	p.mu.Unlock()
	if ok {
		return v, nil
	}
	v, err := p.s.embedder.Embed(ctx, q)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.vecs[q] = v
	p.mu.Unlock()
	return v, nil
}

func (p *previewSearcher) Search(ctx context.Context, query string, limit int, maxDistance float32) ([]*cortexv1.Hit, error) {
	vec, err := p.vector(ctx, query)
	if err != nil {
		return nil, err
	}
	hits, err := p.s.searchStore(ctx, p.ts, vec, p.s.hybridSearchOpts(query, p.namespace, int32(limit), maxDistance))
	if err != nil {
		return nil, err
	}
	out := make([]*cortexv1.Hit, 0, len(hits))
	for _, h := range hits {
		out = append(out, hitToProto(h))
	}
	return out, nil
}

// PreviewRecall dry-runs the recall hook for a prompt using the same query logic
// as `cortex hook recall` (internal/recall). It is read-only: tenant-scoped via
// tenantStore and never reinforces (reinforcement lives in Search, which this
// does not call).
func (s *Service) PreviewRecall(ctx context.Context, req *connect.Request[cortexv1.PreviewRecallRequest]) (*connect.Response[cortexv1.PreviewRecallResponse], error) {
	prompt := strings.TrimSpace(req.Msg.GetPrompt())
	if prompt == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("prompt must not be empty"))
	}
	if len(prompt) > previewMaxPromptBytes {
		return nil, invalidArg("prompt must be at most %d bytes", previewMaxPromptBytes)
	}
	p, err := previewParams(req.Msg)
	if err != nil {
		return nil, err
	}
	out := &cortexv1.PreviewRecallResponse{
		Limit:          int32(p.Limit),
		MaxDistance:    p.MaxDistance,
		MinChars:       int32(p.MinChars),
		MaxQueryChunks: int32(p.MaxQueryChunks),
		Note:           previewDedupNote,
	}
	if reason := recall.SkipReason(prompt, p.MinChars); reason != "" {
		out.Skipped, out.SkipReason = true, reason
		return connect.NewResponse(out), nil
	}

	// One preview at a time per tenant: each costs up to 2x32 searches + 32 embeds.
	release, err := s.gated(ctx, &s.previewGate, false, "a recall preview is already running for this account; try again when it finishes")
	if err != nil {
		return nil, err
	}
	defer release()

	ts, err := s.tenantStore(ctx)
	if err != nil {
		return nil, err
	}
	ps := &previewSearcher{s: s, ts: ts, namespace: req.Msg.GetNamespace(), vecs: map[string][]float32{}}
	res, misses := recall.Preview(ctx, ps, prompt, p)
	if len(res.Errs) > 0 {
		// Unlike the fail-open hook, a preview built from partial results would
		// mislead, so surface the failure.
		return nil, fmt.Errorf("preview search failed: %w", errors.Join(res.Errs...))
	}

	out.Queries = res.Queries
	out.TotalChunks = int32(res.Total)
	for _, m := range res.Injected {
		out.Injected = append(out.Injected, &cortexv1.RecallPreviewHit{
			Memory: m.Hit.GetMemory(), Distance: m.Hit.GetDistance(), QueryIndex: int32(m.Query),
		})
	}
	for _, nm := range misses {
		out.NearMisses = append(out.NearMisses, &cortexv1.RecallPreviewHit{
			Memory: nm.Match.Hit.GetMemory(), Distance: nm.Match.Hit.GetDistance(), QueryIndex: int32(nm.Match.Query), Reason: nm.Reason,
		})
	}
	return connect.NewResponse(out), nil
}
