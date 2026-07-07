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

// backupStartupDelay is how soon after boot an OVERDUE periodic backup runs
// (short, not immediate, so startup settles first). A not-overdue schedule waits
// the remaining interval instead.
const backupStartupDelay = time.Minute

// initialBackupDelay computes how long to wait before the FIRST periodic backup
// after startup, given when the last on-disk backup ran (last/hasLast from
// Service.LastBackupTime). This is what makes the scheduler restart-resilient:
// if there is no prior backup, or the last one is already older than interval,
// back up soon (startupDelay) rather than waiting a full interval a frequent
// restart would never reach; otherwise resume the existing cadence by waiting
// only the remaining time.
func initialBackupDelay(last time.Time, hasLast bool, interval, startupDelay time.Duration, now time.Time) time.Duration {
	if !hasLast {
		return startupDelay
	}
	if elapsed := now.Sub(last); elapsed < interval {
		return interval - elapsed
	}
	return startupDelay
}

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

// envDuration reads a Go duration env var (e.g. "24h", "30m"), returning def
// when unset or unparseable. An empty string or "0" effectively disables a
// feature guarded by duration > 0.
func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
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
		// Full-server backup schedule. backupInterval == 0 disables the periodic
		// goroutine; backupKeep == 0 disables pruning after each backup.
		backupInterval = envDuration("BACKUP_INTERVAL", 0)
		backupKeep     = envInt("BACKUP_KEEP", 7)
	)

	// Login brute-force / DoS protection for /auth/login. Defaults are conservative
	// for an internet-exposed self-host; each knob is overridable. TrustProxy must
	// be enabled ONLY behind a trusted reverse proxy (else X-Forwarded-For spoofing
	// defeats per-IP limits).
	loginLimiterCfg := rpc.DefaultLoginLimiterConfig()
	loginLimiterCfg.PerIP = envInt("CORTEX_LOGIN_PER_IP_PER_MIN", loginLimiterCfg.PerIP)
	loginLimiterCfg.PerIPBurst = envInt("CORTEX_LOGIN_PER_IP_BURST", loginLimiterCfg.PerIPBurst)
	loginLimiterCfg.MaxFailures = envInt("CORTEX_LOGIN_MAX_FAILURES", loginLimiterCfg.MaxFailures)
	loginLimiterCfg.MaxConcurrent = envInt("CORTEX_LOGIN_MAX_CONCURRENT", loginLimiterCfg.MaxConcurrent)
	loginLimiterCfg.TrustProxy = envBool("CORTEX_TRUST_PROXY", false)
	loginLimiter := rpc.NewLoginLimiter(loginLimiterCfg)

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
		BackupKeep:         backupKeep,
		S3:                 s3ConfigFromEnv(),
	}, log)

	// Periodic full-server backup goroutine. Disabled when BACKUP_INTERVAL is
	// empty or "0". Stops cleanly on server shutdown via the signal context.
	//
	// Restart-resilient: the FIRST backup is scheduled from the newest on-disk
	// backup's age (see initialBackupDelay), not from process start. A naive
	// ticker-from-start would never fire on a server that restarts more often than
	// the interval; instead we back up shortly after boot when one is overdue,
	// then keep the cadence.
	if backupInterval > 0 {
		go func() {
			last, hasLast := svc.LastBackupTime()
			delay := initialBackupDelay(last, hasLast, backupInterval, backupStartupDelay, time.Now())
			log.Info("periodic backup scheduled", "interval", backupInterval, "first_in", delay.Round(time.Second))
			timer := time.NewTimer(delay)
			defer timer.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
					if err := svc.RunPeriodicBackup(ctx); err != nil {
						log.Error("periodic backup failed", "err", err)
					}
					timer.Reset(backupInterval)
				}
			}
		}()
	}

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
	mux.Handle("/auth/login", rpc.LoginHandler(jwtMgr, uiUser, uiPass, "admin", log, multiTenant, st, loginLimiter))
	mux.Handle("/", ui.Handler())

	// Cleartext HTTP/2 (prior knowledge) is required for gRPC clients hitting the
	// server directly without TLS; native http.Server support replaces the
	// deprecated x/net h2c handler.
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	srv := &http.Server{
		Addr:              listen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		Protocols:         protocols,
	}

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			log.Error("http server shutdown", "err", err)
		}
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
		// The bootstrap password may be PLAINTEXT or an already-hashed
		// (argon2id/bcrypt) value — e.g. when it falls back to a hashed
		// CORTEX_UI_PASSWORD. Store a hash verbatim; hash a plaintext once. Either
		// way the admin logs in with their plaintext password.
		if store.IsPasswordHash(pass) {
			u, err = st.CreateUserWithHash(ctx, user, pass, identity.RoleAdmin)
		} else {
			u, err = st.CreateUser(ctx, user, pass, identity.RoleAdmin)
		}
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

// s3ConfigFromEnv builds the offsite S3 backup target from AWS_*/CORTEX_S3_*
// environment variables. Credentials are NEVER stored — the returned config is
// held in memory by the server for the process lifetime. Enabled is false (so no
// upload is attempted) unless both credentials AND a bucket are present. No
// credential is hardcoded; every value comes solely from the environment:
//
//	AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY   credentials (BOTH required)
//	AWS_DEFAULT_REGION or AWS_REGION            region (default "us-east-1")
//	CORTEX_S3_ENDPOINT                          endpoint host[:port] (default "s3.amazonaws.com")
//	CORTEX_S3_BUCKET / CORTEX_S3_PREFIX         target bucket + key prefix
//	CORTEX_S3_USE_SSL                           TLS toggle (default true)
//	CORTEX_S3_ENABLED                           enable uploads (default true; forced off without creds+bucket)
func s3ConfigFromEnv() rpc.S3Config {
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	bucket := os.Getenv("CORTEX_S3_BUCKET")
	region := env("AWS_DEFAULT_REGION", os.Getenv("AWS_REGION"))
	if region == "" {
		region = "us-east-1"
	}
	return rpc.S3Config{
		// Only enable uploads with full credentials AND a bucket to write to;
		// enabling without them would fail every backup's offsite step.
		Enabled:   accessKey != "" && secretKey != "" && bucket != "" && envBool("CORTEX_S3_ENABLED", true),
		Endpoint:  env("CORTEX_S3_ENDPOINT", "s3.amazonaws.com"),
		Region:    region,
		Bucket:    bucket,
		Prefix:    os.Getenv("CORTEX_S3_PREFIX"),
		AccessKey: accessKey,
		SecretKey: secretKey,
		UseSsl:    envBool("CORTEX_S3_USE_SSL", true),
	}
}
