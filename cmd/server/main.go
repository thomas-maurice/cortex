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
	"github.com/thomas-maurice/cortex/internal/identity"
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

// envBool reads a boolean env var (1/t/true/0/f/false, case-insensitive),
// returning def when unset or unparseable.
func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
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
		// chunkingEnabled selects the primary search path: chunk-based (with a
		// whole-memory fallback so an un-chunked store still works) when true, or
		// pure whole-memory search when false. Must match the worker's setting.
		chunkingEnabled = envBool("CHUNKING_ENABLED", true)
		// multiTenant gates per-user isolation (Weaviate multi-tenancy, tenant =
		// user). DEFAULT ON. Set CORTEX_MULTI_TENANT=false for legacy single-user
		// mode. Must match the worker's CORTEX_MULTI_TENANT. NOTE: an existing
		// pre-multi-tenancy store must be migrated ONCE with `cortex migrate-mt`
		// after enabling this — it cannot flip a populated class in place.
		multiTenant = envBool("CORTEX_MULTI_TENANT", true)
		authToken   = os.Getenv("CORTEX_AUTH_TOKEN")
		uiUser      = env("CORTEX_UI_USER", "admin")
		uiPass      = os.Getenv("CORTEX_UI_PASSWORD")
		// Bootstrap admin: fall back to the UI creds / auth token so an MT deployment
		// that already sets CORTEX_UI_USER/PASSWORD (or CORTEX_AUTH_TOKEN) gets a
		// working admin with zero extra config.
		bootstrapUser = env("CORTEX_BOOTSTRAP_USER", uiUser)
		bootstrapPass = env("CORTEX_BOOTSTRAP_PASSWORD", uiPass)
		bootstrapKey  = env("CORTEX_BOOTSTRAP_API_KEY", authToken)
		jwtSecret     = os.Getenv("CORTEX_JWT_SECRET")
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
	st.SetMultiTenant(multiTenant)
	// The server is the query owner, so it ensures the schema it reads exists.
	// EnsureSchema is idempotent and additive (it only adds missing properties),
	// so deploying a server that knows about a new property migrates the class
	// without waiting on the worker. Retry while Weaviate finishes booting.
	if err := ensureSchemaWithRetry(ctx, log, st); err != nil {
		log.Error("ensure schema", "err", err)
		os.Exit(1)
	}
	checkSchema(ctx, log, st)
	if multiTenant {
		if err := st.EnsureIdentitySchema(ctx); err != nil {
			log.Error("ensure identity schema", "err", err)
			os.Exit(1)
		}
		bootstrapMultiTenant(ctx, log, st, bootstrapUser, bootstrapPass, bootstrapKey)
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
		ChunkingEnabled:    chunkingEnabled,
		MultiTenant:        multiTenant,
	}, log)

	// Wire the JWT verifier whenever UI/login auth is in play: when a static API
	// token is set (single-user: token for MCP/CLI + UI JWT for the browser), OR in
	// multi-tenant mode where per-user JWT login IS the primary auth (so it must be
	// verifiable even with no static token). Without this, login would issue JWTs
	// the interceptor can't verify. Open dev (no token, MT off) leaves it nil.
	var authJWT *rpc.JWTManager
	if authToken != "" || multiTenant {
		authJWT = jwtMgr
	}
	authCfg := rpc.ServerAuthenticatorConfig{
		Token:             authToken,
		JWTMgr:            authJWT,
		MultiTenant:       multiTenant,
		Store:             st,
		BootstrapUsername: bootstrapUser,
	}
	auth, enabled := rpc.NewServerAuthenticator(authCfg)
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
	mux.Handle("/auth/login", rpc.LoginHandler(jwtMgr, uiUser, uiPass, "admin", log, multiTenant, st))
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

// bootstrapMultiTenant creates the admin user and its API key from the environment
// if they don't already exist — the migration/bootstrap path so an existing
// deployment's clients keep working. CORTEX_BOOTSTRAP_USER + CORTEX_BOOTSTRAP_PASSWORD
// create the admin account; CORTEX_BOOTSTRAP_API_KEY (the SAME raw key existing
// MCP/CLI configs already use) is registered to that admin. Idempotent: a re-run
// with the user/key already present does nothing. Best-effort — a failure is logged
// loudly but does not abort boot (the operator can fix and restart).
func bootstrapMultiTenant(ctx context.Context, log *slog.Logger, st *store.Store, user, pass, rawKey string) {
	if user == "" || pass == "" {
		log.Warn("multi-tenancy on but no bootstrap admin configured (set CORTEX_BOOTSTRAP_USER/PASSWORD, or CORTEX_UI_USER/PASSWORD) — no admin will exist until one is created, and the server will reject all requests except the legacy CORTEX_AUTH_TOKEN")
		return
	}

	u, found, err := st.GetUserByUsername(ctx, user)
	if err != nil {
		log.Error("bootstrap: look up admin failed", "err", err)
		return
	}
	if !found {
		u, err = st.CreateUser(ctx, user, pass, identity.RoleAdmin)
		if err != nil {
			log.Error("bootstrap: create admin failed", "err", err)
			return
		}
		log.Info("bootstrap admin user created", "username", user, "userId", u.ID)
	}

	if rawKey == "" {
		return
	}
	if _, ok, err := st.GetApiKeyByHash(ctx, store.HashAPIKey(rawKey)); err == nil && !ok {
		if _, err := st.AddApiKeyRaw(ctx, u.ID, "bootstrap", rawKey); err != nil {
			log.Error("bootstrap: register admin api key failed", "err", err)
			return
		}
		log.Info("bootstrap admin api key registered", "username", user)
	}
}

// checkSchema verifies the Weaviate classes are present and correctly shaped after
// EnsureSchema, logging a clear OK or loud, actionable warnings (e.g. a class
// created before the tokenization fix needs a rebuild/reindex). Advisory only —
// search keeps working via the whole-memory fallback — so it never aborts boot.
func checkSchema(ctx context.Context, log *slog.Logger, st *store.Store) {
	problems, err := st.VerifySchema(ctx)
	if err != nil {
		log.Warn("schema verification could not run", "err", err)
		return
	}
	if len(problems) == 0 {
		log.Info("weaviate schema OK", "classes", "Memory, MemoryChunk, ConversationSummary")
		return
	}
	for _, p := range problems {
		log.Warn("weaviate schema issue", "problem", p)
	}
	log.Warn("weaviate schema needs attention — search still works (whole-memory fallback), but a `cortex reindex` / class rebuild is recommended",
		"issues", len(problems))
}
