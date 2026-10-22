package obs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// /readyz is unauthenticated: a flood must not translate into a flood of
// backend probes. Concurrent callers share one probe and the result is reused
// within the TTL.
func TestReadyzCachesAndSharesProbe(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	chk := Check{Name: "weaviate", Fn: func(context.Context) error {
		calls.Add(1)
		<-release
		return nil
	}}
	// Huge TTL: late arrivals hit the cache, early ones join the in-flight probe;
	// either way the check must run exactly once.
	h := readyHandler(ReadyTimeout, time.Hour, []Check{chk})

	var wg sync.WaitGroup
	codes := make([]int, 20)
	for i := range codes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes[i], _ = get(t, h, "/readyz")
		}()
	}
	require.Eventually(t, func() bool { return calls.Load() == 1 }, 5*time.Second, time.Millisecond)
	close(release)
	wg.Wait()
	for _, c := range codes {
		assert.Equal(t, http.StatusOK, c)
	}
	assert.EqualValues(t, 1, calls.Load(), "concurrent requests must share one probe")
	_, _ = get(t, h, "/readyz")
	assert.EqualValues(t, 1, calls.Load(), "within the TTL the cached result is served")
}

func TestReadyzReprobesAfterTTL(t *testing.T) {
	var calls atomic.Int32
	chk := Check{Name: "nats", Fn: func(context.Context) error { calls.Add(1); return nil }}
	h := readyHandler(ReadyTimeout, 10*time.Millisecond, []Check{chk})
	_, _ = get(t, h, "/readyz")
	require.EqualValues(t, 1, calls.Load())
	require.Eventually(t, func() bool {
		_, _ = get(t, h, "/readyz")
		return calls.Load() >= 2
	}, 5*time.Second, 5*time.Millisecond, "after the TTL the dependencies are probed again")
}

// A check that ignores its ctx (or panics) must not wedge /readyz: it is
// reported "fail" at the timeout and the next probe still runs.
func TestReadyzSurvivesHungAndPanickingChecks(t *testing.T) {
	var calls atomic.Int32
	block := make(chan struct{})
	defer close(block)
	hung := Check{Name: "weaviate", Fn: func(context.Context) error { calls.Add(1); <-block; return nil }}
	boom := Check{Name: "ollama", Fn: func(context.Context) error { panic("boom") }}
	h := readyHandler(50*time.Millisecond, 10*time.Millisecond, []Check{okCheck("nats"), hung, boom})

	start := time.Now()
	code, body := get(t, h, "/readyz")
	assert.Less(t, time.Since(start), 2*time.Second, "must return around the timeout, not hang")
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.JSONEq(t, `{"nats":"ok","weaviate":"fail","ollama":"fail"}`, body)

	require.Eventually(t, func() bool {
		_, _ = get(t, h, "/readyz")
		return calls.Load() >= 2
	}, 5*time.Second, 5*time.Millisecond, "a later request after the TTL must probe again, not hang")
}

// Caching must never mask an outage: a failing check yields 503, and recovery
// shows up once the cache window passes.
func TestReadyzCachedFailureIs503(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	chk := Check{Name: "ollama", Fn: func(context.Context) error {
		if fail.Load() {
			return errors.New("down")
		}
		return nil
	}}
	h := readyHandler(ReadyTimeout, 10*time.Millisecond, []Check{chk})
	for range 3 {
		code, body := get(t, h, "/readyz")
		assert.Equal(t, http.StatusServiceUnavailable, code)
		assert.JSONEq(t, `{"ollama":"fail"}`, body)
	}
	fail.Store(false)
	require.Eventually(t, func() bool {
		code, _ := get(t, h, "/readyz")
		return code == http.StatusOK
	}, 5*time.Second, 5*time.Millisecond, "recovery shows once the cache window passes")
}
