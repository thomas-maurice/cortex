package rpc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/embed"
	"github.com/thomas-maurice/cortex/internal/identity"
	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/recall"
	"github.com/thomas-maurice/cortex/internal/store"
)

func ptr[T any](v T) *T { return &v }

// Bad input must be rejected before any embed/store work: each search chunk
// costs an embedding, so unbounded limits/chunk counts would be a cheap way to
// load the server. Empty store{} proves no store access happens.
func TestPreviewRecallValidation(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: false}, store: &store.Store{}}
	long := strings.Repeat("x", previewMaxPromptBytes+1)
	cases := map[string]*cortexv1.PreviewRecallRequest{
		"empty prompt":        {Prompt: ""},
		"blank prompt":        {Prompt: "   \n"},
		"prompt too large":    {Prompt: long},
		"limit zero":          {Prompt: "a valid prompt here", Limit: ptr(int32(0))},
		"limit too high":      {Prompt: "a valid prompt here", Limit: ptr(int32(previewMaxLimit + 1))},
		"negative distance":   {Prompt: "a valid prompt here", MaxDistance: ptr(float32(-0.1))},
		"distance too high":   {Prompt: "a valid prompt here", MaxDistance: ptr(float32(2.5))},
		"negative min chars":  {Prompt: "a valid prompt here", MinChars: ptr(int32(-1))},
		"negative max chars":  {Prompt: "a valid prompt here", MaxChars: ptr(int32(-1))},
		"max chars too high":  {Prompt: "a valid prompt here", MaxChars: ptr(int32(previewMaxChars + 1))},
		"min chars too high":  {Prompt: "a valid prompt here", MinChars: ptr(int32(previewMaxMinChars + 1))},
		"negative chunks":     {Prompt: "a valid prompt here", MaxQueryChunks: ptr(int32(-1))},
		"too many max chunks": {Prompt: "a valid prompt here", MaxQueryChunks: ptr(int32(previewMaxQueryChunks + 1))},
	}
	for name, req := range cases {
		_, err := svc.PreviewRecall(context.Background(), connect.NewRequest(req))
		require.Error(t, err, name)
		assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err), name)
	}
}

// Skipped prompts answer from the shared skip rules alone, with no embedder or
// store (both nil/empty here would panic if touched) — the hook never searches
// them either, and the UI must say why.
func TestPreviewRecallSkipsLikeTheHook(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: false}, store: &store.Store{}}
	for prompt, reason := range map[string]string{
		"/clear":     recall.SkipDirective,
		"short one":  recall.SkipTooShort,
		"! ls -la /": recall.SkipDirective,
	} {
		resp, err := svc.PreviewRecall(context.Background(), connect.NewRequest(&cortexv1.PreviewRecallRequest{Prompt: prompt}))
		require.NoError(t, err, prompt)
		assert.True(t, resp.Msg.GetSkipped(), prompt)
		assert.Equal(t, reason, resp.Msg.GetSkipReason(), prompt)
		assert.Empty(t, resp.Msg.GetInjected())
		assert.NotEmpty(t, resp.Msg.GetNote(), "dedup caveat is always reported")
	}

	// min_chars=0 lets the short prompt through the skip check; a nil embedder
	// would then panic, so only check the effective-settings plumbing via a
	// directive (still skipped regardless of min_chars).
	resp, err := svc.PreviewRecall(context.Background(), connect.NewRequest(&cortexv1.PreviewRecallRequest{
		Prompt: "/x", MinChars: ptr(int32(0)), Limit: ptr(int32(7)),
	}))
	require.NoError(t, err)
	assert.Equal(t, int32(7), resp.Msg.GetLimit())
	assert.Equal(t, int32(0), resp.Msg.GetMinChars())
	assert.InDelta(t, recall.DefaultMaxDistance, resp.Msg.GetMaxDistance(), 1e-6, "unset fields take the hook defaults")
	assert.Equal(t, int32(recall.DefaultMaxQueryChunks), resp.Msg.GetMaxQueryChunks())
}

func TestPreviewRecallRequiresIdentityInMTMode(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: true}, store: &store.Store{}}
	_, err := svc.PreviewRecall(context.Background(), connect.NewRequest(&cortexv1.PreviewRecallRequest{Prompt: "a valid prompt here"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeUnauthenticated, connectCode(t, err))
}

// fakeOllama answers every embed request with the same unit vector, so any
// stored memory with that vector is at distance ~0 from any query.
func fakeOllama(t *testing.T) *embed.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Input []string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&in)
		embs := make([][]float32, len(in.Input))
		for i := range embs {
			embs[i] = []float32{1, 0, 0, 0}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"embeddings": embs})
	}))
	t.Cleanup(srv.Close)
	return embed.New(srv.URL, "fake")
}

// TestPreviewRecallIntegration needs a MULTI-TENANT Weaviate (see
// internal/store/mt_integration_test.go). The preview must only ever surface the
// caller's own memories — it runs a real semantic search, so tenant scoping is
// the security invariant — and it must not reinforce what it surfaces.
func TestPreviewRecallIntegration(t *testing.T) {
	rest := os.Getenv("CORTEX_TEST_WEAVIATE_REST")
	grpc := os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" || os.Getenv("CORTEX_TEST_MULTI_TENANT") == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST, CORTEX_TEST_WEAVIATE_GRPC and CORTEX_TEST_MULTI_TENANT=1 to run this integration test")
	}
	ctx := context.Background()
	st, err := store.New(rest, grpc)
	require.NoError(t, err)
	st.SetMultiTenant(true)
	_ = st.DeleteClass(ctx)
	require.NoError(t, st.EnsureSchema(ctx))
	t.Cleanup(func() { _ = st.DeleteClass(ctx) })

	// ChunkingEnabled=false: search whole-memory vectors, so seeding needs no chunks.
	// RerankWeight>0 would make Search reinforce; PreviewRecall must not.
	svc := &Service{cfg: Config{MultiTenant: true, RerankWeight: 0.3, ReinforceTopK: 3}, store: st, embedder: fakeOllama(t)}
	alice := identity.Into(ctx, identity.Identity{UserID: "user-alice", Username: "alice", Role: identity.RoleUser})
	bob := identity.Into(ctx, identity.Identity{UserID: "user-bob", Username: "bob", Role: identity.RoleUser})

	// Three alice memories at increasing distance from the query vector (1,0,0,0).
	mk := func(text string, vec []float32) string {
		id := uuid.NewString()
		require.NoError(t, st.Tenant("user-alice").Upsert(ctx, memory.Record{
			ID: id, Text: text, Namespace: "ns1", Source: "test", CreatedAt: time.Now().UTC(),
		}, vec))
		return id
	}
	near := mk("alice recall hook near memory", []float32{1, 0, 0, 0})
	mid := mk("alice recall hook mid memory", []float32{1, 0.6, 0, 0})
	mk("alice recall hook far memory", []float32{0, 1, 0, 0})

	preview := func(c context.Context, req *cortexv1.PreviewRecallRequest) *cortexv1.PreviewRecallResponse {
		t.Helper()
		resp, err := svc.PreviewRecall(c, connect.NewRequest(req))
		require.NoError(t, err)
		return resp.Msg
	}
	idsOf := func(hs []*cortexv1.RecallPreviewHit) []string {
		var out []string
		for _, h := range hs {
			out = append(out, h.GetMemory().GetId())
		}
		return out
	}

	// Limit 1, wide cutoff: near is injected, the rest are near misses.
	got := preview(alice, &cortexv1.PreviewRecallRequest{
		Prompt: "alice recall hook near memory", Namespace: "*", Limit: ptr(int32(1)), MaxDistance: ptr(float32(2)),
	})
	assert.False(t, got.GetSkipped())
	assert.Equal(t, []string{near}, idsOf(got.GetInjected()))
	assert.Contains(t, idsOf(got.GetNearMisses()), mid)
	for _, nm := range got.GetNearMisses() {
		assert.Equal(t, recall.ReasonOverLimit, nm.GetReason(), "cutoff is wide, so the limit is the only reason")
	}
	assert.Equal(t, []string{"alice recall hook near memory"}, got.GetQueries())

	t.Run("injected block is the exact hook output and flags truncation", func(t *testing.T) {
		got := preview(alice, &cortexv1.PreviewRecallRequest{
			Prompt: "alice recall hook near memory", Namespace: "*", Limit: ptr(int32(1)), MaxDistance: ptr(float32(2)), MaxChars: ptr(int32(10)),
		})
		require.Len(t, got.GetInjected(), 1)
		h := got.GetInjected()[0]
		assert.True(t, h.GetTruncated(), "text is longer than 10 chars")
		assert.Equal(t, int32(len("alice recall hook near memory")), h.GetTextChars())
		assert.Equal(t, int32(10), got.GetMaxChars())
		assert.Equal(t, recall.FormatBlock([]*cortexv1.Hit{{Memory: h.GetMemory(), Distance: h.GetDistance()}}, 10), got.GetInjectedBlock())
		assert.Contains(t, got.GetInjectedBlock(), "alice reca… [truncated]")

		full := preview(alice, &cortexv1.PreviewRecallRequest{Prompt: "alice recall hook near memory", Namespace: "*", Limit: ptr(int32(1)), MaxDistance: ptr(float32(2))})
		assert.False(t, full.GetInjected()[0].GetTruncated(), "default 1500 cap does not cut a short memory")
		assert.Equal(t, int32(recall.DefaultMaxChars), full.GetMaxChars())
	})

	t.Run("another tenant sees nothing", func(t *testing.T) {
		got := preview(bob, &cortexv1.PreviewRecallRequest{
			Prompt: "alice recall hook near memory", Namespace: "*", Limit: ptr(int32(5)), MaxDistance: ptr(float32(2)),
		})
		assert.Empty(t, got.GetInjected())
		assert.Empty(t, got.GetNearMisses())
	})

	t.Run("does not reinforce", func(t *testing.T) {
		time.Sleep(300 * time.Millisecond) // reinforce is async; give any wrongly-fired write time to land
		rec, ok, err := st.Tenant("user-alice").Get(ctx, near)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Zero(t, rec.AccessCount, "a preview is not a recall")
	})
}

// "0 = every chunk" would let one call embed an unbounded prompt; the RPC must
// clamp it to the same cap explicit values are held to.
func TestPreviewParamsClampsAllChunks(t *testing.T) {
	p, err := previewParams(&cortexv1.PreviewRecallRequest{MaxQueryChunks: ptr(int32(0))})
	require.NoError(t, err)
	assert.Equal(t, previewMaxQueryChunks, p.MaxQueryChunks)

	p, err = previewParams(&cortexv1.PreviewRecallRequest{})
	require.NoError(t, err)
	assert.Equal(t, recall.DefaultMaxQueryChunks, p.MaxQueryChunks, "unset still takes the hook default")
}

// Preview and the Search RPC the hook calls must rank with the same store
// options; both build them via hybridSearchOpts, so config (hybrid alpha,
// living-memory rerank) and namespace resolution can't drift between them.
func TestHybridSearchOptsSharedByPreview(t *testing.T) {
	svc := &Service{cfg: Config{DefaultNamespace: "dflt", SearchAlpha: 0.4, RerankWeight: 0.3, RerankHalfLifeDays: 30}}
	o := svc.hybridSearchOpts("q text", "*", 3, 0.5)
	assert.Equal(t, store.SearchOpts{
		Namespace: "", Limit: 3, MaxDistance: 0.5, Query: "q text",
		Alpha: 0.4, RerankWeight: 0.3, RerankHalfLifeDays: 30,
	}, o, "hybrid query, config blend and rerank all propagate; * = all namespaces")
	assert.Equal(t, "dflt", svc.hybridSearchOpts("q", "", 3, 0).Namespace)
}
