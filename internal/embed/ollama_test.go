package embed

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeEmbedding writes a well-formed /api/embeddings response.
func writeEmbedding(t *testing.T, w http.ResponseWriter, vec []float32) {
	t.Helper()
	require.NoError(t, json.NewEncoder(w).Encode(embedResp{Embedding: vec}))
}

func TestEmbedSuccess(t *testing.T) {
	want := []float32{0.1, 0.2, 0.3}
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		assert.Equal(t, "/api/embeddings", r.URL.Path)
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
		writeEmbedding(t, w, nil)
	}))
	defer srv.Close()

	_, err := New(srv.URL, "m").Embed(context.Background(), "hello")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty vector")
	assert.Equal(t, int32(1), calls.Load())
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
