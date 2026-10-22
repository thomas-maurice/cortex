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

// ReadyHandler runs every check concurrently under timeout and answers 200 when
// all pass, 503 otherwise. The JSON body maps check name to "ok"/"fail" only.
func ReadyHandler(timeout time.Duration, checks []Check) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		var (
			mu      sync.Mutex
			wg      sync.WaitGroup
			results = make(map[string]string, len(checks))
			allOK   = true
		)
		for _, c := range checks {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res := "ok"
				if err := c.Fn(ctx); err != nil {
					res = "fail"
				}
				mu.Lock()
				defer mu.Unlock()
				results[c.Name] = res
				if res != "ok" {
					allOK = false
				}
			}()
		}
		wg.Wait()

		w.Header().Set("Content-Type", "application/json")
		if !allOK {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(results)
	})
}
