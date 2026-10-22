package embed

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeEmbedding writes a well-formed /api/embed response holding the given vectors.
func writeEmbedding(t *testing.T, w http.ResponseWriter, vecs ...[]float32) {
	t.Helper()
	require.NoError(t, json.NewEncoder(w).Encode(embedResp{Embeddings: vecs}))
}

// readInputs decodes the request body's inputs.
func readInputs(t *testing.T, r *http.Request) []string {
	t.Helper()
	var req embedReq
	require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
	return req.Input
}

func TestEmbedSuccess(t *testing.T) {
	want := []float32{0.1, 0.2, 0.3}
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		assert.Equal(t, "/api/embed", r.URL.Path)
		writeEmbedding(t, w, want)
	}))
	defer srv.Close()

	got, err := New(srv.URL, "m").Embed(context.Background(), "hello")
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, int32(1), calls.Load(), "a healthy server needs exactly one call")
}

// TestEmbedRetriesTransient encodes WHY the retry exists: the embed call is on the
// synchronous recall path, so a transient 5xx must not fail the whole search when a
// prompt retry succeeds.
func TestEmbedRetriesTransient(t *testing.T) {
	want := []float32{1, 2}
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable) // 503 twice
			return
		}
		writeEmbedding(t, w, want) // succeed on the 3rd
	}))
	defer srv.Close()

	got, err := New(srv.URL, "m").Embed(context.Background(), "hello")
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, int32(3), calls.Load())
}

// TestEmbedNoRetryOn4xx guards the boundary: a 4xx is a caller/config error, so
// re-issuing the identical request is pointless and must not happen.
func TestEmbedNoRetryOn4xx(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	_, err := New(srv.URL, "m").Embed(context.Background(), "hello")
	require.Error(t, err)
	assert.Equal(t, int32(1), calls.Load(), "4xx must not be retried")
}

// TestEmbedNoRetryOnEmptyVector: an empty embedding is a deterministic outcome
// (model not pulled), not a transient blip — retrying would just repeat it.
func TestEmbedNoRetryOnEmptyVector(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeEmbedding(t, w, []float32{})
	}))
	defer srv.Close()

	_, err := New(srv.URL, "m").Embed(context.Background(), "hello")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty vector")
	assert.Equal(t, int32(1), calls.Load())
}

// TestEmbedBatchOrder: vectors must map to inputs by position — the worker pairs
// chunk i with vector i, so any reorder silently corrupts the index.
func TestEmbedBatchOrder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/embed", r.URL.Path)
		in := readInputs(t, r)
		assert.Equal(t, []string{"a", "bb", "ccc"}, in)
		vecs := make([][]float32, len(in))
		for i, s := range in {
			vecs[i] = []float32{float32(len(s))}
		}
		writeEmbedding(t, w, vecs...)
	}))
	defer srv.Close()

	got, err := New(srv.URL, "m").EmbedBatch(context.Background(), []string{"a", "bb", "ccc"})
	require.NoError(t, err)
	assert.Equal(t, [][]float32{{1}, {2}, {3}}, got)
}

// TestEmbedBatchSplitsAtCap: oversized batches become several sequential requests,
// reassembled in order.
func TestEmbedBatchSplitsAtCap(t *testing.T) {
	n := embedMaxBatch*2 + 5
	texts := make([]string, n)
	for i := range texts {
		texts[i] = strconv.Itoa(i)
	}
	var sizes []int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		in := readInputs(t, r)
		sizes = append(sizes, len(in)) // sequential requests: no race
		vecs := make([][]float32, len(in))
		for i, s := range in {
			v, err := strconv.Atoi(s)
			require.NoError(t, err)
			vecs[i] = []float32{float32(v)}
		}
		writeEmbedding(t, w, vecs...)
	}))
	defer srv.Close()

	var observed int
	c := New(srv.URL, "m")
	c.SetObserver(func(time.Duration, error) { observed++ })
	got, err := c.EmbedBatch(context.Background(), texts)
	require.NoError(t, err)
	assert.Equal(t, []int{embedMaxBatch, embedMaxBatch, 5}, sizes)
	assert.Equal(t, 3, observed, "observer fires once per HTTP request")
	require.Len(t, got, n)
	for i := range got {
		assert.Equal(t, []float32{float32(i)}, got[i], "vector %d out of order", i)
	}
}

// TestEmbedBatchCountMismatch protects chunk/vector alignment: a short response
// must fail loudly (and not be retried), never yield misaligned vectors.
func TestEmbedBatchCountMismatch(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeEmbedding(t, w, []float32{1})
	}))
	defer srv.Close()

	_, err := New(srv.URL, "m").EmbedBatch(context.Background(), []string{"a", "b"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 embeddings for 2 inputs")
	assert.Equal(t, int32(1), calls.Load())
}

// TestEmbedBatchEmptyVectorInBatch: one empty vector among good ones is still an error.
func TestEmbedBatchEmptyVectorInBatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeEmbedding(t, w, []float32{1}, []float32{})
	}))
	defer srv.Close()

	_, err := New(srv.URL, "m").EmbedBatch(context.Background(), []string{"a", "b"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty vector at index 1")
}

// TestEmbedBatchEmptyInput: nothing to embed means no request at all.
func TestEmbedBatchEmptyInput(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("no request expected for empty input")
	}))
	defer srv.Close()

	got, err := New(srv.URL, "m").EmbedBatch(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

// TestEmbedObserver: the metrics hook still sees success and failure.
func TestEmbedObserver(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeEmbedding(t, w, []float32{1})
	}))
	defer srv.Close()

	var errs []error
	c := New(srv.URL, "m")
	c.SetObserver(func(_ time.Duration, err error) { errs = append(errs, err) })
	_, err := c.Embed(context.Background(), "x")
	require.NoError(t, err)
	srv.Close()
	_, err = c.Embed(context.Background(), "x")
	require.Error(t, err)
	require.Len(t, errs, 2)
	assert.NoError(t, errs[0])
	assert.Error(t, errs[1])
}

func TestEmbedExhaustsRetries(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, err := New(srv.URL, "m").Embed(context.Background(), "hello")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "giving up after 3 attempts")
	assert.Equal(t, int32(embedMaxAttempts), calls.Load())
}

// TestEmbedContextCancelledDuringBackoff: a cancelled context must abort the retry
// loop promptly rather than sleeping out the backoff — the caller's deadline wins.
func TestEmbedContextCancelledDuringBackoff(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError) // transient → would retry
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := New(srv.URL, "m").Embed(ctx, "hello")
	require.Error(t, err)
	// The full backoff for 3 attempts is 200ms+400ms=600ms; a context-aware loop
	// returns well before that.
	assert.Less(t, time.Since(start), 500*time.Millisecond)
	assert.LessOrEqual(t, calls.Load(), int32(embedMaxAttempts))
}

func TestReachable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/tags", r.URL.Path)
		writeTags(t, w, nil)
	}))
	defer srv.Close()
	require.NoError(t, New(srv.URL, "m").Reachable(context.Background()))
}

func TestReachableDown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	require.Error(t, New(srv.URL, "m").Reachable(context.Background()))
}

// TestHasModelMatchesLatest pins the ":latest" normalisation: a model configured
// WITHOUT a tag must match how Ollama actually stores it (as ":latest").
func TestHasModelMatchesLatest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeTags(t, w, []string{"nomic-embed:latest", "other:1"})
	}))
	defer srv.Close()

	// configured tagless "nomic-embed" must match the stored "nomic-embed:latest"
	has, err := New(srv.URL, "nomic-embed").HasModel(context.Background())
	require.NoError(t, err)
	assert.True(t, has)

	missing, err := New(srv.URL, "nope").HasModel(context.Background())
	require.NoError(t, err)
	assert.False(t, missing)
}

// TestHasModelExactTagMatch: a model configured WITH an explicit tag matches only
// that exact stored name (no ":latest" suffixing).
func TestHasModelExactTagMatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeTags(t, w, []string{"qwen3-embedding:0.6b"})
	}))
	defer srv.Close()

	has, err := New(srv.URL, "qwen3-embedding:0.6b").HasModel(context.Background())
	require.NoError(t, err)
	assert.True(t, has)
}

func writeTags(t *testing.T, w http.ResponseWriter, names []string) {
	t.Helper()
	var out tagsResp
	for _, n := range names {
		out.Models = append(out.Models, struct {
			Name string `json:"name"`
		}{Name: n})
	}
	require.NoError(t, json.NewEncoder(w).Encode(out))
}

func TestModel(t *testing.T) {
	assert.Equal(t, "some-model", New("http://x", "some-model").Model())
}

func TestSleepCtxReturnsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := sleepCtx(ctx, time.Hour)
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "context canceled"))
}
