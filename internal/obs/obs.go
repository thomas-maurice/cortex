// Package obs holds the observability plumbing shared by the server and the
// worker: a dedicated Prometheus registry, the unauthenticated /healthz,
// /readyz and /metrics handlers, and the dependency probes behind /readyz.
package obs

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/thomas-maurice/cortex/internal/embed"
	"github.com/thomas-maurice/cortex/internal/store"
)

// ReadyTimeout bounds the whole /readyz probe so an orchestrator never waits on
// a hung dependency.
const ReadyTimeout = 3 * time.Second

// Check is one named readiness probe. A nil error means healthy.
type Check struct {
	Name string
	Fn   func(ctx context.Context) error
}

// NewRegistry returns a fresh registry (NOT the global default, so tests are
// deterministic) with the Go runtime and process collectors registered.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

// DependencyChecks returns the NATS / Weaviate / Ollama reachability probes —
// the same primitives Status and Doctor use (connection state, Store.Ready,
// Embedder.Reachable), minus Doctor's expensive embed probe.
func DependencyChecks(nc *nats.Conn, st *store.Store, em *embed.Client) []Check {
	return []Check{
		{Name: "nats", Fn: func(context.Context) error {
			if nc.IsConnected() {
				return nil
			}
			return nats.ErrDisconnected
		}},
		{Name: "weaviate", Fn: st.Ready},
		{Name: "ollama", Fn: em.Reachable},
	}
}

// Mount registers /healthz and /readyz (always) and /metrics (when reg != nil)
// on mux. These routes are unauthenticated by design, so they expose no error
// text, hostnames or versions.
func Mount(mux *http.ServeMux, reg *prometheus.Registry, checks []Check) {
	mux.Handle("GET /healthz", HealthHandler())
	mux.Handle("GET /readyz", ReadyHandler(ReadyTimeout, checks))
	if reg != nil {
		mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	}
}

// HealthHandler is liveness: 200 as long as the process serves HTTP. It checks
// no dependencies, so a backend outage never gets the process restarted.
func HealthHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
}

// ReadyCacheTTL is how long a /readyz result is reused. The endpoint is
// unauthenticated, so without it a request flood would fan out to the backends
// on every hit.
const ReadyCacheTTL = time.Second

// ReadyHandler runs every check concurrently under timeout and answers 200 when
// all pass, 503 otherwise. The JSON body maps check name to "ok"/"fail" only.
// The result is cached for ReadyCacheTTL and concurrent callers share one
// in-flight probe.
func ReadyHandler(timeout time.Duration, checks []Check) http.Handler {
	return readyHandler(timeout, ReadyCacheTTL, checks)
}

type readyResult struct {
	ok   bool
	body []byte
}

func readyHandler(timeout, ttl time.Duration, checks []Check) http.Handler {
	var (
		mu       sync.Mutex
		cached   *readyResult
		cachedAt time.Time
		inflight *readyFlight
	)
	probe := func() *readyResult {
		// Detached from any request: one caller's cancellation must not fail the
		// probe the others are waiting on.
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		var (
			rmu     sync.Mutex
			wg      sync.WaitGroup
			results = make(map[string]string, len(checks))
		)
		// Pessimistic: a check that has not reported (hung, ignores ctx, panicked)
		// counts as "fail".
		for _, c := range checks {
			results[c.Name] = "fail"
		}
		for _, c := range checks {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { _ = recover() }() // a panicking check stays "fail"
				if err := c.Fn(ctx); err == nil {
					rmu.Lock()
					results[c.Name] = "ok"
					rmu.Unlock()
				}
			}()
		}
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-ctx.Done(): // do not wait for checks that ignore ctx
		}

		rmu.Lock()
		snap := make(map[string]string, len(results))
		allOK := true
		for k, v := range results {
			snap[k] = v
			allOK = allOK && v == "ok"
		}
		rmu.Unlock()
		body, _ := json.Marshal(snap)
		return &readyResult{ok: allOK, body: append(body, '\n')}
	}
	get := func() *readyResult {
		mu.Lock()
		if cached != nil && time.Since(cachedAt) < ttl {
			r := cached
			mu.Unlock()
			return r
		}
		if f := inflight; f != nil {
			mu.Unlock()
			<-f.done
			return f.res
		}
		f := &readyFlight{done: make(chan struct{})}
		inflight = f
		mu.Unlock()

		defer func() {
			mu.Lock()
			if f.res != nil {
				cached, cachedAt = f.res, time.Now()
			}
			inflight = nil
			mu.Unlock()
			close(f.done)
		}()
		f.res = probe()
		return f.res
	}
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		res := get()
		w.Header().Set("Content-Type", "application/json")
		if !res.ok {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_, _ = w.Write(res.body)
	})
}

type readyFlight struct {
	done chan struct{}
	res  *readyResult
}
