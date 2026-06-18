// Command server is the Cortex Connect RPC server. It is the single owner of
// NATS (writes) and Weaviate/Ollama (reads); the MCP server and the cortex CLI
// are thin clients of it. This lets the brain be self-hosted once and reached
// from multiple machines.
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/grpcreflect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/thomas-maurice/cortex/gen/cortex/v1/cortexv1connect"
	"github.com/thomas-maurice/cortex/internal/bus"
	"github.com/thomas-maurice/cortex/internal/embed"
	"github.com/thomas-maurice/cortex/internal/rpc"
	"github.com/thomas-maurice/cortex/internal/store"
	"github.com/thomas-maurice/cortex/ui"
)

// version is the build version, injected at release time via
// -ldflags "-X main.version=...". Defaults to "dev" for un-stamped builds.
var version = "dev"

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envFloat reads a float32 env var, returning def when unset or unparseable.
func envFloat(key string, def float32) float32 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 32)
	if err != nil {
		return def
	}
	return float32(f)
}

// envInt reads an int env var, returning def when unset or unparseable.
func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var (
		listen       = env("CORTEX_LISTEN", ":8080")
		natsURL      = env("NATS_URL", "nats://localhost:4222")
		ollamaURL    = env("OLLAMA_URL", "http://localhost:11434")
		ollamaModel  = env("OLLAMA_MODEL", "qwen3-embedding:0.6b")
		weaviateREST = env("WEAVIATE_HOST", "localhost:8080")
		weaviateGRPC = env("WEAVIATE_GRPC_HOST", "localhost:50051")
		defaultNS    = env("DEFAULT_NAMESPACE", "global")
		source       = env("MEMORY_SOURCE", "cortex")
		backupDir    = env("CORTEX_BACKUP_DIR", ".")
		searchAlpha  = envFloat("SEARCH_ALPHA", 0.5) // hybrid blend: 1=pure vector, 0=pure keyword
		// "Living memory": rerankWeight>0 enables usage-aware re-ranking + hit
		// reinforcement (opt-in, like DEDUP_DISTANCE — 0 keeps the old behaviour).
		rerankWeight   = envFloat("RERANK_WEIGHT", 0)
		rerankHalfLife = envFloat("RERANK_HALFLIFE_DAYS", 30)
		reinforceTopK  = envInt("REINFORCE_TOPK", 1)
		authToken      = os.Getenv("CORTEX_AUTH_TOKEN")
		uiUser         = env("CORTEX_UI_USER", "admin")
		uiPass         = os.Getenv("CORTEX_UI_PASSWORD")
		jwtSecret      = os.Getenv("CORTEX_JWT_SECRET")
	)

	// The UI logs in for a JWT signed with this secret.
	// Precedence:
	//   1. CORTEX_JWT_SECRET (explicit)
	//   2. sha256("cortex/jwt-secret/v1:" + authToken) — stable across restarts
	//      without using the API token bytes directly as a signing key, so a
	//      leaked JWT cannot be trivially replayed as an API token.
	//   3. 32 random bytes — per-process only; UI sessions die on restart.
	if jwtSecret == "" && authToken != "" {
		h := sha256.Sum256([]byte("cortex/jwt-secret/v1:" + authToken))
		jwtSecret = hex.EncodeToString(h[:])
	}
	if jwtSecret == "" {
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			log.Error("rand.Read failed", "err", err)
			os.Exit(1)
		}
		jwtSecret = hex.EncodeToString(b)
		log.Warn("CORTEX_JWT_SECRET and CORTEX_AUTH_TOKEN are not set — using a random per-process JWT secret; UI sessions will not survive a restart")
	}
	jwtMgr := rpc.NewJWTManager(jwtSecret, 12*time.Hour)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	nc, js, err := bus.Connect(natsURL)
	if err != nil {
		log.Error("connect nats", "err", err)
		os.Exit(1)
	}
	defer nc.Close()
	if err := bus.EnsureStream(ctx, js); err != nil {
		log.Error("ensure stream", "err", err)
		os.Exit(1)
	}

	st, err := store.New(weaviateREST, weaviateGRPC)
	if err != nil {
		log.Error("store init", "err", err)
		os.Exit(1)
	}
	// The server is the query owner, so it ensures the schema it reads exists.
	// EnsureSchema is idempotent and additive (it only adds missing properties),
	// so deploying a server that knows about a new property migrates the class
	// without waiting on the worker. Retry while Weaviate finishes booting.
	if err := ensureSchemaWithRetry(ctx, log, st); err != nil {
		log.Error("ensure schema", "err", err)
		os.Exit(1)
	}

	svc := rpc.NewService(nc, js, st, embed.New(ollamaURL, ollamaModel), rpc.Config{
		DefaultNamespace:   defaultNS,
		Source:             source,
		Version:            version,
		BackupDir:          backupDir,
		SearchAlpha:        searchAlpha,
		RerankWeight:       rerankWeight,
		RerankHalfLifeDays: rerankHalfLife,
		ReinforceTopK:      reinforceTopK,
	}, log)

	// When the API token is set, accept either it (MCP/CLI) or a UI-issued JWT
	// (browser). With no token the server stays open for local dev.
	var authJWT *rpc.JWTManager
	if authToken != "" {
		authJWT = jwtMgr
	}
	auth, enabled := rpc.NewServerAuthenticator(authToken, authJWT)
	if !enabled {
		log.Warn("CORTEX_AUTH_TOKEN is not set — the server is UNAUTHENTICATED; set a token before exposing it off localhost")
	}
	if uiPass == "" {
		log.Warn("CORTEX_UI_PASSWORD is not set — the web UI login is disabled")
	}

	mux := http.NewServeMux()
	path, handler := cortexv1connect.NewMemoryServiceHandler(svc,
		connect.WithInterceptors(rpc.ServerLogInterceptor(log), rpc.ServerAuthInterceptor(auth)))
	mux.Handle(path, handler)
	// gRPC server reflection so tools like grpcurl, Bruno, and Postman can
	// introspect and call the API without a local .proto. Reflection exposes only
	// the schema (method + message names, already public in proto/); actual RPC
	// calls still pass through the auth interceptor above. Both the v1 and v1alpha
	// reflection services are mounted for broad client compatibility.
	reflector := grpcreflect.NewStaticReflector(cortexv1connect.MemoryServiceName)
	mux.Handle(grpcreflect.NewHandlerV1(reflector))
	mux.Handle(grpcreflect.NewHandlerV1Alpha(reflector))
	// Web UI: login mints a JWT, the embedded SPA is the catch-all route.
	mux.Handle("/auth/login", rpc.LoginHandler(jwtMgr, uiUser, uiPass, "admin", log))
	mux.Handle("/", ui.Handler())

	srv := &http.Server{
		Addr:              listen,
		Handler:           h2c.NewHandler(mux, &http2.Server{}),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	log.Info("cortex server listening", "addr", listen, "namespace", defaultNS, "model", ollamaModel, "auth", enabled)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("server", "err", err)
		os.Exit(1)
	}
	log.Info("shutting down")
}

// ensureSchemaWithRetry creates the schema, backing off while Weaviate finishes
// booting on a fresh stack. Gives up after ~30s.
func ensureSchemaWithRetry(ctx context.Context, log *slog.Logger, st *store.Store) error {
	const attempts = 15
	var err error
	for i := 0; i < attempts; i++ {
		if err = st.EnsureSchema(ctx); err == nil {
			return nil
		}
		log.Info("waiting for weaviate", "attempt", i+1, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return err
}
