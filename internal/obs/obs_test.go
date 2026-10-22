package obs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func get(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	body, err := io.ReadAll(rec.Result().Body)
	require.NoError(t, err)
	return rec.Code, string(body)
}

func okCheck(name string) Check {
	return Check{Name: name, Fn: func(context.Context) error { return nil }}
}

func failCheck(name, msg string) Check {
	return Check{Name: name, Fn: func(context.Context) error { return errors.New(msg) }}
}

// Liveness must not depend on any backend: if it did, a Weaviate outage would
// make the orchestrator restart a perfectly healthy process.
func TestHealthzIgnoresDependencies(t *testing.T) {
	mux := http.NewServeMux()
	Mount(mux, nil, []Check{failCheck("weaviate", "down")})
	code, body := get(t, mux, "/healthz")
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "ok\n", body)
}

// Readiness gates traffic: any failing dependency must flip it to 503, and an
// all-green set must be 200.
func TestReadyz(t *testing.T) {
	t.Run("200 when all checks pass", func(t *testing.T) {
		code, body := get(t, ReadyHandler(ReadyTimeout, []Check{okCheck("nats"), okCheck("weaviate"), okCheck("ollama")}), "/readyz")
		assert.Equal(t, http.StatusOK, code)
		assert.JSONEq(t, `{"nats":"ok","weaviate":"ok","ollama":"ok"}`, body)
	})

	t.Run("503 when one check fails, and nothing leaks", func(t *testing.T) {
		// The endpoint is unauthenticated: error text can carry hostnames/URLs.
		secret := `dial tcp weaviate.internal.example:8080: connection refused`
		code, body := get(t, ReadyHandler(ReadyTimeout, []Check{okCheck("nats"), failCheck("weaviate", secret), okCheck("ollama")}), "/readyz")
		assert.Equal(t, http.StatusServiceUnavailable, code)
		assert.JSONEq(t, `{"nats":"ok","weaviate":"fail","ollama":"ok"}`, body)
		assert.NotContains(t, body, "internal.example")
		assert.NotContains(t, body, "refused")
	})
}

// The ops routes must win over the "/" UI catch-all, and /metrics must be
// absent (404 from the catch-all) when no registry is supplied.
func TestMountRouting(t *testing.T) {
	catchAll := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })

	mux := http.NewServeMux()
	Mount(mux, NewRegistry(), nil)
	mux.Handle("/", catchAll)
	code, body := get(t, mux, "/metrics")
	assert.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "go_goroutines", "Go collector must be exposed")
	code, _ = get(t, mux, "/healthz")
	assert.Equal(t, http.StatusOK, code)

	off := http.NewServeMux()
	Mount(off, (*prometheus.Registry)(nil), nil)
	off.Handle("/", catchAll)
	code, _ = get(t, off, "/metrics")
	assert.Equal(t, http.StatusTeapot, code, "METRICS_ENABLED=false must leave /metrics unserved")
	code, _ = get(t, off, "/healthz")
	assert.Equal(t, http.StatusOK, code, "healthz stays on without metrics")
}
