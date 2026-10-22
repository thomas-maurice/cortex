// Command worker is the indexing consumer. It pulls memory.index events from
// JetStream, embeds the text via Ollama, and writes vectors to Weaviate.
// This is the ONLY process that mutates the vector store on write.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/thomas-maurice/cortex/internal/bus"
	"github.com/thomas-maurice/cortex/internal/chunk"
	"github.com/thomas-maurice/cortex/internal/dlock"
	"github.com/thomas-maurice/cortex/internal/embed"
	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/obs"
	"github.com/thomas-maurice/cortex/internal/store"
)

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

// envBool reads a boolean env var, returning def when unset or unparseable.
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

// Per-memory-id locking. Workers scale horizontally; every read-modify-write on
// one memory (index write, link edit) or one summary runs under a lock in the
// lockBucket KV bucket. The 60s TTL only reclaims a crashed holder's lock
// (no lease refresh); the indexedSeq guard is the backstop if a holder outlives it.
const (
	lockBucket = "cortex_locks"
	lockTTL    = 60 * time.Second
	// ackWait is set explicitly on both consumers; the pool sends InProgress every
	// keepaliveInterval (5s) for a message's whole life, far inside it.
	ackWait = 30 * time.Second
	// fetchReadyGrace: /readyz fails once JetStream pulls have failed continuously
	// for this long.
	fetchReadyGrace = 30 * time.Second
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var (
		natsURL      = env("NATS_URL", "nats://localhost:4222")
		ollamaURL    = env("OLLAMA_URL", "http://localhost:11434")
		ollamaModel  = env("OLLAMA_MODEL", "qwen3-embedding:0.6b")
		weaviate     = env("WEAVIATE_HOST", "localhost:8080")
		weaviateGRPC = env("WEAVIATE_GRPC_HOST", "localhost:50051")
		// dedupDistance is the cosine-distance band within which a freshly indexed
		// memory's existing neighbours are flagged as duplicate candidates. 0
		// disables the check entirely (the default), so behaviour is unchanged
		// until opted in. The right value is model-specific (qwen3 distances are
		// compressed) — tune it against your own data.
		dedupDistance = envFloat("DEDUP_DISTANCE", 0)
		// chunkMaxTokens / chunkOverlap size the per-chunk vectors the primary search
		// runs against. 512/64 by default; sized with a cl100k proxy tokenizer (see
		// internal/chunk). Configurable so chunk granularity can be tuned per model.
		chunkMaxTokens = envInt("CHUNK_MAX_TOKENS", chunk.DefaultMaxTokens)
		chunkOverlap   = envInt("CHUNK_OVERLAP_TOKENS", chunk.DefaultOverlap)
		// chunkingEnabled gates whether the worker writes per-chunk vectors at all.
		// When false the worker only embeds + upserts the whole memory (the
		// pre-chunking behaviour); the server must be set to match. Must agree with
		// the server's CHUNKING_ENABLED.
		chunkingEnabled = envBool("CHUNKING_ENABLED", true)
		// multiTenant gates per-user isolation (Weaviate multi-tenancy). DEFAULT ON;
		// set CORTEX_MULTI_TENANT=false for legacy single-user mode. The worker
		// scopes its store writes by the payload's tenant when on. Must match the
		// server's CORTEX_MULTI_TENANT.
		multiTenant = envBool("CORTEX_MULTI_TENANT", true)
		// metricsAddr is the listen address of the ops HTTP server (/metrics,
		// /healthz, /readyz). An explicitly empty WORKER_METRICS_ADDR disables it.
		metricsAddr = metricsAddrFromEnv()
		// concurrency bounds in-flight messages per consumer in THIS process. Scale
		// further by adding worker replicas.
		concurrency = envInt("WORKER_CONCURRENCY", 4)
	)

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

	st, err := store.New(weaviate, weaviateGRPC)
	if err != nil {
		log.Error("store init", "err", err)
		os.Exit(1)
	}
	st.SetMultiTenant(multiTenant)
	// Weaviate may still be starting on a fresh stack; retry rather than
	// crash-loop on the restart policy.
	if err := ensureSchemaWithRetry(ctx, log, st); err != nil {
		log.Error("ensure schema", "err", err)
		os.Exit(1)
	}
	checkSchema(ctx, log, st)

	host, _ := os.Hostname() // ignored: owner is a debugging label only
	locker, err := dlock.New(ctx, js, lockBucket, lockTTL, fmt.Sprintf("%s/%d", host, os.Getpid()))
	if err != nil {
		log.Error("ensure lock bucket", "err", err)
		os.Exit(1)
	}

	embedder := embed.New(ollamaURL, ollamaModel)
	reg := obs.NewRegistry()
	m := newMetrics(reg)
	embedder.SetObserver(m.observeEmbed)

	// A nil chunker means chunking is disabled: indexChunks becomes a no-op and the
	// worker only writes whole-memory vectors (the server then searches those).
	var chunker *chunk.Chunker
	if chunkingEnabled {
		chunker, err = chunk.New(chunkMaxTokens, chunkOverlap)
		if err != nil {
			log.Error("init chunker", "err", err)
			os.Exit(1)
		}
	}

	log.Info("worker up", "nats", natsURL, "weaviate", weaviate, "model", ollamaModel,
		"dedup_distance", dedupDistance, "chunking_enabled", chunkingEnabled,
		"chunk_max_tokens", chunkMaxTokens, "chunk_overlap", chunkOverlap, "multi_tenant", multiTenant)

	locker.Instrument(log, m.lockInfraError, m.observeLockWait)
	deps := &workerDeps{
		log: log, m: m, js: js, locker: locker, stream: newStreamIdent(js), embedder: embedder,
		stores: func(userID string) tenantStore { return st.Tenant(userID) }, chunker: chunker,
		dedupDistance: dedupDistance, concurrency: concurrency, ackWait: ackWait,
	}
	indexPool, linkPool, err := deps.startPools(ctx)
	if err != nil {
		log.Error("start consumers", "err", err)
		os.Exit(1)
	}
	defer indexPool.wait()
	defer linkPool.wait()

	if metricsAddr != "" {
		checks := obs.DependencyChecks(nc, st, embedder)
		// A worker whose pulls have failed continuously is processing nothing.
		for name, p := range map[string]*pool{"jetstream-index": indexPool, "jetstream-link": linkPool} {
			checks = append(checks, obs.Check{Name: name, Fn: func(context.Context) error { return p.health.err(fetchReadyGrace) }})
		}
		mux := http.NewServeMux()
		obs.Mount(mux, reg, checks)
		// A failure to listen is logged but not fatal: metrics must never take the indexer down.
		if _, err := obs.Serve(ctx, log, "worker ops", metricsAddr, mux); err != nil {
			log.Error("ops http listen", "addr", metricsAddr, "err", err)
		}
	}

	<-ctx.Done()
	log.Info("shutting down")
}

// ensureConsumer creates (or updates) a durable pull consumer with the worker's
// settings. The stream must exist.
func ensureConsumer(ctx context.Context, js jetstream.JetStream, durable string, filters []string, maxDeliver int, ackWait time.Duration) (jetstream.Consumer, error) {
	return js.CreateOrUpdateConsumer(ctx, memory.StreamName, jetstream.ConsumerConfig{
		Durable:        durable,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        ackWait,
		FilterSubjects: filters,
		MaxDeliver:     maxDeliver,
	})
}

// workerDeps is everything the consumer loops need; main and the multi-replica
// tests both start pools through startPools, so the tests run the real wiring.
type workerDeps struct {
	log           *slog.Logger
	m             *metrics
	js            jetstream.JetStream
	locker        *dlock.Locker
	stream        *streamIdent
	embedder      *embed.Client
	stores        tenantStores
	chunker       *chunk.Chunker
	dedupDistance float32
	concurrency   int
	ackWait       time.Duration
}

// startPools creates the two durable consumers (the stream must already exist) and
// starts `concurrency` loops on each, every loop pulling ONE message at a time (see
// pool): bounded, and no replica can hoard a backlog. Correctness across goroutines
// and replicas rests on the per-id locks and the indexedSeq guard. The pools
// re-create a consumer (stream re-ensured first) only when that cannot replay history.
func (d *workerDeps) startPools(ctx context.Context) (index, link *pool, err error) {
	// Links ride their own durable consumer, decoupled from indexing: an edge whose
	// endpoint is still being embedded retries on this consumer's budget without
	// blocking or being blocked by the index stream.
	indexFilters := []string{memory.SubjectIndex, memory.SubjectSummary}
	linkFilters := []string{memory.SubjectLink}
	// Both consumers re-ensure the SAME stream when it is wiped; serialise that so
	// one's stream update never overlaps the other's consumer create (nats-server
	// v2.15.1 races fileStore.UpdateConfig against ConsumerStore on the stream config).
	var ensureMu sync.Mutex
	recreate := func(durable string, filters []string, maxDeliver int) func(context.Context) (jetstream.Consumer, error) {
		return func(ctx context.Context) (jetstream.Consumer, error) {
			ensureMu.Lock()
			defer ensureMu.Unlock()
			if err := bus.EnsureStream(ctx, d.js); err != nil {
				return nil, err
			}
			return ensureConsumer(ctx, d.js, durable, filters, maxDeliver, d.ackWait)
		}
	}
	idx, err := ensureConsumer(ctx, d.js, memory.ConsumerName, indexFilters, memory.MaxDeliver, d.ackWait)
	if err != nil {
		return nil, nil, fmt.Errorf("create consumer: %w", err)
	}
	lnk, err := ensureConsumer(ctx, d.js, memory.LinkConsumerName, linkFilters, memory.LinkMaxDeliver, d.ackWait)
	if err != nil {
		return nil, nil, fmt.Errorf("create link consumer: %w", err)
	}
	index, link = newPool(d.concurrency, d.log, d.m), newPool(d.concurrency, d.log, d.m)
	index.run(ctx, newConsumerRef(d.js, idx, recreate(memory.ConsumerName, indexFilters, memory.MaxDeliver)),
		func(wctx context.Context, msg jetstream.Msg) {
			handle(wctx, d.log, d.m, d.js, d.locker, d.stream, d.embedder, d.stores, d.chunker, msg, d.dedupDistance)
		})
	link.run(ctx, newConsumerRef(d.js, lnk, recreate(memory.LinkConsumerName, linkFilters, memory.LinkMaxDeliver)),
		func(wctx context.Context, msg jetstream.Msg) {
			handleLink(wctx, d.log, d.m, d.js, d.locker, d.stores, msg)
		})
	return index, link, nil
}

// envOrEmpty is like env but distinguishes "unset" (use def) from "set to empty"
// (returns ""), so an operator can switch a feature off with KEY="".
func envOrEmpty(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

// metricsAddrFromEnv is the ops listener address: WORKER_METRICS_ADDR, default
// :9090, empty disables.
func metricsAddrFromEnv() string { return envOrEmpty("WORKER_METRICS_ADDR", ":9090") }

// checkSchema verifies the Weaviate classes exist and are correctly shaped after
// EnsureSchema, logging a clear OK or loud, actionable warnings (e.g. a class
// created before the tokenization fix needs a rebuild/reindex). Advisory only, so
// it never aborts boot.
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
	log.Warn("weaviate schema needs attention — indexing/search still work, but a `cortex reindex` / class rebuild is recommended",
		"issues", len(problems))
}

// ensureSchemaWithRetry tries to create the schema, backing off while Weaviate
// finishes booting. Gives up after ~30s.
func ensureSchemaWithRetry(ctx context.Context, log *slog.Logger, st *store.Store) error {
	const attempts = 15
	var err error
	for i := range attempts {
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

// handle dispatches by subject: fact-index events and conversation-summary
// events share one durable consumer.
func handle(ctx context.Context, log *slog.Logger, m *metrics, js jetstream.JetStream, locker *dlock.Locker, stream *streamIdent, embedder *embed.Client, stores tenantStores, chunker *chunk.Chunker, msg jetstream.Msg, dedupDistance float32) {
	if msg.Subject() == memory.SubjectSummary {
		handleSummary(ctx, log, m, js, locker, stream, embedder, stores, msg)
		return
	}
	handleIndex(ctx, log, m, js, locker, stream, embedder, stores, chunker, msg, dedupDistance)
}

// deadLetterPublisher is the signature for publishing a dead-letter record.
// In production it wraps bus.PublishDead; in tests it is a controllable stub.
type deadLetterPublisher func(dl memory.DeadLetter) error

// deadLetterMalformed dead-letters a message whose payload could not be
// unmarshalled, preserving the raw bytes in the synthetic Record.Text field so
// an operator can inspect them via `cortex dead`. It Term-s the message after a
// successful publish (no more redeliveries of a fundamentally broken payload),
// or NAK-s if the dead-letter publish itself fails (so NATS redelivers and we
// get another chance to preserve the bytes rather than silently dropping them).
// A durability queue must never silently lose bytes.
func deadLetterMalformed(ctx context.Context, log *slog.Logger, m *metrics, kind string, pub deadLetterPublisher, msg jetstream.Msg, unmarshalErr error) {
	raw := msg.Data()
	dl := memory.DeadLetter{
		Record: memory.Record{
			ID:     uuid.New().String(),
			Text:   string(raw),
			Source: "malformed-payload",
		},
		Error:      fmt.Sprintf("malformed payload: %s", unmarshalErr),
		Deliveries: numDelivered(msg),
		FailedAt:   time.Now().UTC(),
	}
	if err := pub(dl); err != nil {
		log.Error("dead-letter publish failed, will retry", "err", err, "unmarshal_err", unmarshalErr)
		if nerr := msg.Nak(); nerr != nil {
			log.Warn("msg nak failed", "err", nerr)
		}
		m.count(kind, outcomeRetry)
		return
	}
	log.Error("bad payload, dead-lettered", "raw_bytes", len(raw), "err", unmarshalErr)
	m.count(kind, outcomeDead)
	if nerr := msg.Term(); nerr != nil {
		log.Warn("msg term failed", "err", nerr)
	}
}

// handleSummary embeds a conversation summary and upserts it (one per
// conversation, replaced in place). Summaries are reproducible — the agent
// re-summarises periodically — so on permanent failure we log loudly and ack
// rather than dead-letter.
func handleSummary(ctx context.Context, log *slog.Logger, m *metrics, js jetstream.JetStream, locker *dlock.Locker, stream *streamIdent, embedder *embed.Client, stores tenantStores, msg jetstream.Msg) {
	var sum memory.Summary
	if err := json.Unmarshal(msg.Data(), &sum); err != nil {
		pub := func(dl memory.DeadLetter) error { return bus.PublishDead(ctx, js, dl) }
		deadLetterMalformed(ctx, log, m, kindSummary, pub, msg, err)
		return
	}

	// Tenant comes from the NATS payload UserID stamped by the server handler —
	// NEVER from a request field. st.Tenant("") is a no-op when MT is off.
	ts := stores(sum.UserID)

	var procErr error
	var applied bool
	var embedDur, storeDur time.Duration
	embedStart := time.Now()
	vec, err := embedder.Embed(ctx, sum.Text)
	embedDur = time.Since(embedStart)
	if err != nil {
		procErr = fmt.Errorf("embed: %w", err)
	} else {
		sum.Model = embedder.Model()
		sum.Dims = len(vec)
		if sum.UpdatedAt.IsZero() {
			sum.UpdatedAt = time.Now().UTC()
		}
		storeStart := time.Now()
		applied, err = commitSummary(ctx, locker, ts, sum, vec, streamSeq(msg), stream)
		noteHoldExpiry(ctx, log, m, kindSummary, sum.ConversationID, err)
		if err != nil {
			procErr = fmt.Errorf("upsert: %w", err)
		}
		storeDur = time.Since(storeStart)
	}

	if procErr == nil && !applied {
		log.Info("stale summary skipped, a newer one is already stored", "conversation", sum.ConversationID)
		ackAs(log, m, kindSummary, msg, outcomeStale)
		return
	}
	if procErr == nil {
		log.Info("summarized", "conversation", sum.ConversationID, "dims", len(vec),
			"embed_ms", embedDur.Milliseconds(), "store_ms", storeDur.Milliseconds(),
			"total_ms", (embedDur + storeDur).Milliseconds(),
			"queue_latency_ms", time.Since(sum.UpdatedAt).Milliseconds())
		ack(log, m, kindSummary, msg)
		return
	}

	if abortForShutdown(ctx, log, msg) {
		return
	}
	deliveries := 0
	if md, err := msg.Metadata(); err == nil {
		deliveries = int(md.NumDelivered)
	}
	if deliveries >= memory.MaxDeliver {
		log.Error("gave up summarizing, dropping (agent will re-summarize)", "conversation", sum.ConversationID, "deliveries", deliveries, "err", procErr)
		if nerr := msg.Ack(); nerr != nil {
			log.Warn("msg ack failed", "err", nerr)
		}
		m.count(kindSummary, outcomeTerm)
		return
	}
	log.Warn("summarizing failed, will retry", "conversation", sum.ConversationID, "deliveries", deliveries, "err", procErr)
	if nerr := msg.NakWithDelay(2 * time.Second); nerr != nil {
		log.Warn("msg nak failed", "err", nerr)
	}
	m.count(kindSummary, outcomeRetry)
}

// ack acknowledges msg as successfully processed and counts it.
func ack(log *slog.Logger, m *metrics, kind string, msg jetstream.Msg) {
	ackAs(log, m, kind, msg, outcomeAck)
}

// ackAs acknowledges msg and counts it under outcome: ack for done work, stale for
// a message whose work is already superseded by a newer stored version.
func ackAs(log *slog.Logger, m *metrics, kind string, msg jetstream.Msg, outcome string) {
	if err := msg.Ack(); err != nil {
		log.Warn("msg ack failed", "err", err)
	}
	m.count(kind, outcome)
}

// streamSeq is the JetStream stream sequence of msg: a monotonic, cluster-wide
// version for the publish order. 0 when unavailable (tests).
func streamSeq(msg jetstream.Msg) uint64 {
	if md, err := msg.Metadata(); err == nil {
		return md.Sequence.Stream
	}
	return 0
}

// noteHoldExpiry logs and counts a critical section cut off by the lock hold
// deadline (not by worker shutdown). The error text carries the phase that was
// running (read stored / upsert / chunk index / set links ...); the message is
// retried by the normal failure path.
func noteHoldExpiry(ctx context.Context, log *slog.Logger, m *metrics, kind, id string, err error) {
	if err == nil || ctx.Err() != nil || !errors.Is(err, context.DeadlineExceeded) {
		return
	}
	log.Warn("critical section hit the lock hold deadline, will retry", "kind", kind, "id", id, "err", err)
	m.holdTimeout()
}

// abortForShutdown NAKs msg and returns true when the worker is shutting down
// (ctx cancelled). A handler that failed only because its context was cancelled
// must not be settled as a processing failure (that would count toward dead-letter
// for perfectly valid work); the message is simply handed back for redelivery.
func abortForShutdown(ctx context.Context, log *slog.Logger, msg jetstream.Msg) bool {
	if ctx.Err() == nil {
		return false
	}
	log.Info("shutting down, returning message for redelivery")
	if err := msg.Nak(); err != nil {
		log.Warn("msg nak failed", "err", err)
	}
	return true
}

// streamIdent caches the identity of the MEMORY stream (its creation time, unix
// microseconds). JetStream sequences restart at 1 when a stream is recreated, so
// the stale-write guard only compares sequences recorded under the same identity.
// Cached, refreshed after streamIdentMaxAge so a recreation is noticed; a failed
// refresh keeps the last known value (0 = unknown, which disables the identity
// check and falls back to comparing sequences).
type streamIdent struct {
	js  jetstream.JetStream
	mu  sync.Mutex
	val int64
	at  time.Time
}

const streamIdentMaxAge = 30 * time.Second

// streamIdentity is what the commit functions need from a streamIdent (an
// interface so tests can pin identities).
type streamIdentity interface {
	get(ctx context.Context) int64
	refresh(ctx context.Context)
}

// staleAfterRefresh evaluates the stale guard and, before declaring a message
// stale, refreshes the cached stream identity and re-evaluates: a stream recreated
// within the cache window would otherwise make every message look older than
// stored for up to streamIdentMaxAge. Stale is rare, so the lookup is cheap.
// It returns the verdict and the identity to stamp on a write.
func staleAfterRefresh(ctx context.Context, sid streamIdentity, storedSeq uint64, storedStream int64, seq uint64) (isStale bool, stream int64) {
	stream = sid.get(ctx)
	if !superseded(storedSeq, storedStream, seq, stream) {
		return false, stream
	}
	sid.refresh(ctx)
	stream = sid.get(ctx)
	return superseded(storedSeq, storedStream, seq, stream), stream
}

func newStreamIdent(js jetstream.JetStream) *streamIdent { return &streamIdent{js: js} }

func (s *streamIdent) get(ctx context.Context) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.val == 0 || time.Since(s.at) > streamIdentMaxAge {
		s.refreshLocked(ctx)
	}
	return s.val
}

func (s *streamIdent) refresh(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refreshLocked(ctx)
}

func (s *streamIdent) refreshLocked(ctx context.Context) {
	s.at = time.Now()
	if info, err := streamInfo(ctx, s.js); err == nil {
		s.val = info.Created.UnixMicro()
	}
}

// streamInfo returns the MEMORY stream's current info (js.Stream fetches it, so
// CachedInfo is fresh).
func streamInfo(ctx context.Context, js jetstream.JetStream) (*jetstream.StreamInfo, error) {
	st, err := js.Stream(ctx, memory.StreamName)
	if err != nil {
		return nil, err
	}
	return st.CachedInfo(), nil
}

// superseded reports whether a stored (seq, stream) is newer than the message's.
// Sequences are only comparable within one stream: if both identities are known
// and differ, the stream was recreated since the stored write, so the message is
// fresh. If either is unknown (legacy object, lookup failure) sequences are
// compared as-is.
func superseded(storedSeq uint64, storedStream int64, seq uint64, stream int64) bool {
	if storedStream != 0 && stream != 0 && storedStream != stream {
		return false
	}
	return storedSeq > seq
}

// tenantStore is everything the handlers use from a tenant-scoped store.
// *store.TenantStore implements it; tests substitute a fake, so the concurrency
// behaviour is testable without Weaviate.
type tenantStore interface {
	Get(ctx context.Context, id string) (memory.Record, bool, error)
	SetLinks(ctx context.Context, id string, links []string) error
	Upsert(ctx context.Context, rec memory.Record, vector []float32) error
	UpdateIndexFields(ctx context.Context, rec memory.Record, vector []float32) error
	ReplaceChunks(ctx context.Context, rec memory.Record, chunks []store.ChunkVec) error
	DeleteWithReason(ctx context.Context, id, reason string) error
	GetSummarySeq(ctx context.Context, conversationID string) (seq uint64, stream int64, err error)
	UpsertSummary(ctx context.Context, sum memory.Summary, vector []float32) error
	SearchMemoryVectors(ctx context.Context, vec []float32, opts store.SearchOpts) ([]memory.Hit, error)
}

// tenantStores resolves the tenant-scoped store for a payload's UserID.
type tenantStores func(userID string) tenantStore

// commitSummary upserts sum under the conversation's lock unless a summary from a
// newer queue message is already stored (then applied=false). Summaries are one
// object per conversation, so without this an older message finishing after a
// newer one (two workers, or a redelivery) would regress the summary.
func commitSummary(ctx context.Context, locker *dlock.Locker, ts tenantStore, sum memory.Summary, vec []float32, seq uint64, sid streamIdentity) (applied bool, err error) {
	hctx, release, err := locker.AcquireAll(ctx, []string{dlock.Key("sum", sum.UserID, sum.ConversationID)}, locker.Wait())
	if err != nil {
		return false, fmt.Errorf("lock: %w", err)
	}
	defer release()

	storedSeq, storedStream, err := ts.GetSummarySeq(hctx, sum.ConversationID)
	if err != nil {
		return false, err
	}
	isStale, stream := staleAfterRefresh(hctx, sid, storedSeq, storedStream, seq)
	if isStale {
		return false, nil
	}
	sum.IndexedSeq, sum.IndexedStream = seq, stream
	if err := ts.UpsertSummary(hctx, sum, vec); err != nil {
		return false, err
	}
	return true, nil
}

// handleLink applies one bidirectional edge mutation (memory.LinkMsg) to the
// store. It is the durable, retrying replacement for the old inline link-on-index
// path: links now arrive on their own subject decoupled from indexing.
//
// For an add, BOTH endpoints must already exist; if either is missing its index
// event is presumably still queued, so the message is NAK'd with a capped backoff
// and retried — up to memory.LinkMaxDeliver — rather than silently dropped. A
// remove needs no waiting: a missing endpoint has no edge to clear, so it applies
// to whichever endpoint exists and acks. The apply itself is idempotent
// (set-union / set-difference), so redelivery and duplicate publishes are no-ops.
//
// Each apply is a read-modify-write of both endpoints' linkedIds (SetLinks replaces
// the whole list), so it runs under the distributed lock of BOTH ids, taken in
// sorted order: safe across any number of workers and against the index path,
// which takes the same per-id lock. linkedIds is owned by this consumer alone.
func handleLink(ctx context.Context, log *slog.Logger, m *metrics, js jetstream.JetStream, locker *dlock.Locker, stores tenantStores, msg jetstream.Msg) {
	var lm memory.LinkMsg
	if err := json.Unmarshal(msg.Data(), &lm); err != nil {
		pub := func(dl memory.DeadLetter) error { return bus.PublishDead(ctx, js, dl) }
		deadLetterMalformed(ctx, log, m, kindLink, pub, msg, err)
		return
	}
	// Memory ids are Weaviate object ids, i.e. UUIDs. A non-UUID endpoint can never
	// exist (every store call would 422), so an ADD to one terminates now instead of
	// burning the whole LinkMaxDeliver budget. A REMOVE keeps its old meaning: the
	// missing endpoint has no edge to clear, and the edge is still removed from the
	// valid side (a dangling link to a bad id can be cleaned up).
	if lm.A == "" || lm.B == "" || lm.A == lm.B || (lm.Op != memory.LinkOpRemove && (!validID(lm.A) || !validID(lm.B))) {
		log.Warn("invalid link msg, terminating", "op", lm.Op, "a", lm.A, "b", lm.B)
		_ = msg.Term()
		m.count(kindLink, outcomeTerm)
		return
	}

	hctx, release, err := locker.AcquireAll(ctx, []string{dlock.Key("mem", lm.UserID, lm.A), dlock.Key("mem", lm.UserID, lm.B)}, locker.Wait())
	if err != nil {
		linkRetry(ctx, log, m, msg, lm, "lock failed", err)
		return
	}
	defer release()

	// Tenant comes from the NATS payload's UserID, stamped by the RPC handler.
	ts := stores(lm.UserID)

	// lookup treats a non-UUID endpoint (only reachable for removes) as absent
	// without asking the store, which would reject it.
	lookup := func(id string) (memory.Record, bool, error) {
		if !validID(id) {
			return memory.Record{}, false, nil
		}
		return ts.Get(hctx, id)
	}
	a, aOK, err := lookup(lm.A)
	if err != nil {
		linkRetry(ctx, log, m, msg, lm, "endpoint lookup failed", err)
		return
	}
	b, bOK, err := lookup(lm.B)
	if err != nil {
		linkRetry(ctx, log, m, msg, lm, "endpoint lookup failed", err)
		return
	}

	if lm.Op == memory.LinkOpAdd && (!aOK || !bOK) {
		// The out-of-order case the queue exists for: an endpoint is not indexed
		// yet. Retry until it lands or the budget is spent (a genuinely bad id).
		deliveries := numDelivered(msg)
		if deliveries >= memory.LinkMaxDeliver {
			log.Error("gave up linking, endpoint never appeared", "a", lm.A, "b", lm.B,
				"a_exists", aOK, "b_exists", bOK, "deliveries", deliveries)
			_ = msg.Term()
			m.count(kindLink, outcomeTerm)
			return
		}
		log.Info("link endpoint not indexed yet, will retry", "a", lm.A, "b", lm.B,
			"a_exists", aOK, "b_exists", bOK, "deliveries", deliveries)
		_ = msg.NakWithDelay(linkRetryDelay(deliveries))
		m.count(kindLink, outcomeRetry)
		return
	}

	newA, newB := applyEdge(lm.Op, lm.A, a.LinkedIDs, lm.B, b.LinkedIDs)
	if aOK {
		if err := ts.SetLinks(hctx, lm.A, newA); err != nil {
			linkRetry(ctx, log, m, msg, lm, "set links failed", err)
			return
		}
	}
	if bOK {
		if err := ts.SetLinks(hctx, lm.B, newB); err != nil {
			linkRetry(ctx, log, m, msg, lm, "set links failed", err)
			return
		}
	}
	log.Info("link applied", "op", lm.Op, "a", lm.A, "b", lm.B)
	ack(log, m, kindLink, msg)
}

// validID reports whether id can be a memory (Weaviate object) id.
func validID(id string) bool {
	_, err := uuid.Parse(id)
	return err == nil
}

// applyEdge returns the new link slices for endpoints a and b after applying op.
// Pure and idempotent: add is set-union, remove is set-difference, so applying
// the same edge twice yields the same result as applying it once.
func applyEdge(op memory.LinkOp, aID string, aLinks []string, bID string, bLinks []string) (newA, newB []string) {
	if op == memory.LinkOpRemove {
		return removeString(aLinks, bID), removeString(bLinks, aID)
	}
	return addUnique(aLinks, bID), addUnique(bLinks, aID)
}

// linkRetryDelay backs off proportionally to delivery count, capped at 30s, so a
// link waiting on a slow embedding backlog retries gently over minutes rather
// than hammering the store.
func linkRetryDelay(deliveries int) time.Duration {
	d := time.Duration(deliveries) * 2 * time.Second
	if d > 30*time.Second {
		return 30 * time.Second
	}
	return d
}

// linkRetry NAKs a transient link failure (store error) with backoff; on a fresh
// message deliveries is 0, giving an immediate redelivery.
func linkRetry(ctx context.Context, log *slog.Logger, m *metrics, msg jetstream.Msg, lm memory.LinkMsg, what string, err error) {
	if abortForShutdown(ctx, log, msg) {
		return
	}
	noteHoldExpiry(ctx, log, m, kindLink, lm.A+"<->"+lm.B, err)
	deliveries := numDelivered(msg)
	log.Warn("link apply failed, will retry", "reason", what, "op", lm.Op, "a", lm.A, "b", lm.B, "deliveries", deliveries, "err", err)
	_ = msg.NakWithDelay(linkRetryDelay(deliveries))
	m.count(kindLink, outcomeRetry)
}

// numDelivered reads the JetStream delivery count for msg, or 0 if unavailable.
func numDelivered(msg jetstream.Msg) int {
	if md, err := msg.Metadata(); err == nil {
		return int(md.NumDelivered)
	}
	return 0
}

// removeString returns xs without any element equal to v.
func removeString(xs []string, v string) []string {
	out := make([]string, 0, len(xs))
	for _, x := range xs {
		if x != v {
			out = append(out, x)
		}
	}
	return out
}

// addUnique appends v to xs if absent, preserving order.
func addUnique(xs []string, v string) []string {
	if slices.Contains(xs, v) {
		return xs
	}
	return append(xs, v)
}

// planSupersedes returns the distinct source ids a freshly-indexed record should
// delete: rec.Supersedes minus empties, self-references, and duplicates. Pure,
// so the skip/dedup contract is unit-testable without a live store.
func planSupersedes(recID string, supersedes []string) []string {
	seen := make(map[string]bool, len(supersedes))
	out := make([]string, 0, len(supersedes))
	for _, id := range supersedes {
		if id == "" || id == recID || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out
}

// applySupersedes deletes the memories rec replaces, AFTER rec itself has been
// durably upserted by the caller. This ordering is the safety property of
// consolidation: the merged memory is persisted before any source is removed, so
// a failure here leaves stale sources behind (the pre-merge state) rather than
// losing the merged content. Best-effort and non-fatal: a missing/already-gone
// source is logged and skipped, so a redelivery that re-runs this (sources
// already deleted) is harmless. Sources go to the Trash (reason supersedes:<id>),
// so a bad consolidation is recoverable. ts is scoped to the record's tenant so the
// deletes only affect the correct tenant's data in multi-tenant mode.
func applySupersedes(ctx context.Context, log *slog.Logger, ts tenantStore, rec memory.Record) {
	for _, id := range planSupersedes(rec.ID, rec.Supersedes) {
		if err := ts.DeleteWithReason(ctx, id, memory.TrashReasonSupersedes+rec.ID); err != nil {
			log.Warn("supersede delete failed, skipping", "id", rec.ID, "superseded", id, "err", err)
			continue
		}
		log.Info("superseded", "id", rec.ID, "deleted", id)
	}
}

// candidateLimit caps how many duplicate candidates one memory records. A handful
// is enough to flag a cluster for review; the rest of the cluster surfaces via
// each member's own candidates.
const candidateLimit = 5

// findCandidates returns ids of already-indexed memories within dedupDistance of
// rec's vector, in the same namespace — heuristic duplicate hints for later
// review. Returns nil when the check is disabled (dedupDistance<=0). Best-effort:
// a search failure is logged and treated as "no candidates", never fatal, since
// the record must still be indexed regardless. ts is scoped to the record's tenant
// so cross-tenant matches are impossible in multi-tenant mode.
func findCandidates(ctx context.Context, log *slog.Logger, ts tenantStore, rec memory.Record, vec []float32, dedupDistance float32) []string {
	if dedupDistance <= 0 {
		return nil
	}
	// Dedup wants WHOLE-memory similarity (is this a near-duplicate of an existing
	// memory?), so it searches the full-memory vectors directly, NOT the chunk index
	// the primary Search now uses.
	hits, err := ts.SearchMemoryVectors(ctx, vec, store.SearchOpts{
		Namespace:   rec.Namespace,
		Limit:       candidateLimit + 1 + len(rec.NotDuplicateOf), // headroom for self + dismissed pairs
		MaxDistance: dedupDistance,
	})
	if err != nil {
		log.Warn("dedup candidate search failed, skipping", "id", rec.ID, "err", err)
		return nil
	}
	// Pairs a reviewer already confirmed are NOT duplicates must never be
	// re-flagged. The list is bidirectional, so checking rec's own copy is enough.
	dismissed := make(map[string]bool, len(rec.NotDuplicateOf))
	for _, id := range rec.NotDuplicateOf {
		dismissed[id] = true
	}
	var out []string
	for _, h := range hits {
		if h.ID == rec.ID || h.ID == "" || dismissed[h.ID] {
			continue
		}
		out = append(out, h.ID)
		if len(out) >= candidateLimit {
			break
		}
	}
	return out
}

// embedChunks splits rec.Text into overlapping, token-bounded chunks and embeds
// them (outside the id lock: embedding is the slow part). The chunk vectors are
// the PRIMARY search target: a fact buried in a long memory is found by matching
// its focused chunk, not the whole document's averaged vector. A nil chunker means
// chunking is disabled and no chunks are produced.
func embedChunks(ctx context.Context, embedder *embed.Client, chunker *chunk.Chunker, rec memory.Record) ([]store.ChunkVec, error) {
	if chunker == nil {
		return nil, nil
	}
	chunks := chunker.Split(rec.Text)
	// One batched call (split internally at the client's cap) instead of a serial
	// round-trip per chunk; vectors come back index-aligned with chunks.
	vecs, err := embedder.EmbedBatch(ctx, chunks)
	if err != nil {
		return nil, fmt.Errorf("embed chunks: %w", err)
	}
	cvs := make([]store.ChunkVec, 0, len(chunks))
	for i, ct := range chunks {
		cvs = append(cvs, store.ChunkVec{Index: i, Text: ct, Vector: vecs[i]})
	}
	return cvs, nil
}

// commitIndex is the locked store phase of indexing one memory. Under the id's
// distributed lock it reads the stored state and
//   - skips everything if a newer queue message already landed (applied=false);
//   - CREATES a memory that does not exist yet with the full record, links
//     included (import, restore, migrate-mt and reindex-after-rebuild carry the
//     only copy of the links);
//   - UPDATES an existing memory by writing only the fields the index path owns
//     (see store.UpdateIndexFields). linkedIds, accessCount/lastAccessedAt and
//     notDuplicateOf belong to other writers and are never touched, so a queued
//     edit carrying a stale snapshot of them cannot erase a link or a
//     reinforcement made since it was published.
//
// With no chunker, ReplaceChunks(nil) purges chunks left from when chunking was
// enabled.
func commitIndex(ctx context.Context, log *slog.Logger, locker *dlock.Locker, ts tenantStore, rec memory.Record, vec []float32, cvs []store.ChunkVec, seq uint64, sid streamIdentity) (applied bool, err error) {
	// hctx is bounded inside the first lease; see Locker.AcquireAll.
	hctx, release, err := locker.AcquireAll(ctx, []string{dlock.Key("mem", rec.UserID, rec.ID)}, locker.Wait())
	if err != nil {
		return false, fmt.Errorf("lock: %w", err)
	}
	defer release()

	stored, exists, err := ts.Get(hctx, rec.ID)
	if err != nil {
		return false, fmt.Errorf("read stored: %w", err)
	}
	// Strictly greater: a redelivery of this very message (same seq) re-applies,
	// which is idempotent and lets a half-finished supersedes complete.
	// A missing memory reads as seq 0 / identity 0, which is never superseded.
	isStale, stream := staleAfterRefresh(hctx, sid, stored.IndexedSeq, stored.IndexedStream, seq)
	if isStale {
		return false, nil
	}
	rec.IndexedSeq, rec.IndexedStream = seq, stream
	if exists {
		// Dedup candidates were computed before the lock from the message's
		// snapshot of notDuplicateOf; honour dismissals made since.
		rec.DupCandidates = withoutIDs(rec.DupCandidates, stored.NotDuplicateOf)
		err = ts.UpdateIndexFields(hctx, rec, vec)
	} else {
		err = ts.Upsert(hctx, rec, vec)
	}
	if err != nil {
		// The full-memory vector is still stored (dedup + find-similar use it),
		// but it is no longer the primary search target.
		return false, fmt.Errorf("upsert: %w", err)
	}
	// Chunks are what Search matches against, so a chunk failure must retry:
	// the memory would otherwise exist but be unfindable. Idempotent on retry.
	if err := ts.ReplaceChunks(hctx, rec, cvs); err != nil {
		return false, fmt.Errorf("chunk index: %w", err)
	}
	// Requested links are not applied here: they travel on SubjectLink and are
	// applied by the link consumer, which waits for both endpoints to exist.
	// Supersedes still runs inline: now that the merged memory AND its chunks are
	// durable, delete the sources it replaces. Ordering matters: post-upsert means
	// a crash here can't lose content.
	applySupersedes(hctx, log, ts, rec)
	return true, nil
}

// withoutIDs returns xs minus any element of drop.
func withoutIDs(xs, drop []string) []string {
	if len(drop) == 0 {
		return xs
	}
	var out []string
	for _, x := range xs {
		if !slices.Contains(drop, x) {
			out = append(out, x)
		}
	}
	return out
}

func handleIndex(ctx context.Context, log *slog.Logger, m *metrics, js jetstream.JetStream, locker *dlock.Locker, stream *streamIdent, embedder *embed.Client, stores tenantStores, chunker *chunk.Chunker, msg jetstream.Msg, dedupDistance float32) {
	defer m.observeIndex(time.Now())
	var rec memory.Record
	if err := json.Unmarshal(msg.Data(), &rec); err != nil {
		// Unrecoverable: bad payload. Dead-letter it (preserving raw bytes) and
		// Term so NATS does not redeliver. If the dead-letter publish fails, NAK
		// instead — redelivery is better than silent data loss.
		pub := func(dl memory.DeadLetter) error { return bus.PublishDead(ctx, js, dl) }
		deadLetterMalformed(ctx, log, m, kindIndex, pub, msg, err)
		return
	}

	// Tenant comes from the NATS payload's UserID, stamped by the RPC handler
	// from the authenticated identity — NEVER from any request field.
	// st.Tenant("") is a no-op when multi-tenancy is off.
	ts := stores(rec.UserID)

	var procErr error
	var applied bool
	var embedDur, storeDur time.Duration
	var nChunks int
	embedStart := time.Now()
	vec, err := embedder.Embed(ctx, rec.Text)
	if err != nil {
		procErr = fmt.Errorf("embed: %w", err)
	} else {
		// The worker is authoritative on provenance: stamp the model that
		// actually produced this vector and its dimension, overriding whatever
		// (if anything) the producer put on the record.
		rec.Model = embedder.Model()
		rec.Dims = len(vec)
		// Heuristic dedup: reuse the vector we just computed to find existing
		// near neighbours in the same namespace and flag them as candidates.
		// Recomputed on every (re)index, so it always reflects current contents.
		rec.DupCandidates = findCandidates(ctx, log, ts, rec, vec, dedupDistance)
		var cvs []store.ChunkVec
		cvs, procErr = embedChunks(ctx, embedder, chunker, rec)
		if procErr == nil {
			embedDur = time.Since(embedStart)
			storeStart := time.Now()
			applied, procErr = commitIndex(ctx, log, locker, ts, rec, vec, cvs, streamSeq(msg), stream)
			noteHoldExpiry(ctx, log, m, kindIndex, rec.ID, procErr)
			nChunks = len(cvs)
			storeDur = time.Since(storeStart)
		}
	}

	if procErr == nil && !applied {
		log.Info("stale index message skipped, a newer version is already stored", "id", rec.ID, "seq", streamSeq(msg))
		ackAs(log, m, kindIndex, msg, outcomeStale)
		return
	}
	if procErr == nil {
		// queue_latency_ms is the end-to-end wait from publish (rec.CreatedAt) to
		// indexed — it includes JetStream queue time and any Ollama cold-load, so
		// it is the number to watch for "why did indexing take so long".
		log.Info("indexed", "id", rec.ID, "namespace", rec.Namespace, "dims", len(vec), "chunks", nChunks,
			"embed_ms", embedDur.Milliseconds(), "store_ms", storeDur.Milliseconds(),
			"total_ms", (embedDur + storeDur).Milliseconds(),
			"queue_latency_ms", time.Since(rec.CreatedAt).Milliseconds())
		ack(log, m, kindIndex, msg)
		return
	}

	if abortForShutdown(ctx, log, msg) {
		return
	}
	pub := func(dl memory.DeadLetter) error { return bus.PublishDead(ctx, js, dl) }
	settleIndexFailure(log, m, pub, msg, rec, procErr)
}

// settleIndexFailure handles a failed index attempt. Retry until MaxDeliver,
// then dead-letter so the record is preserved rather than silently dropped. A
// failed dead-letter publish retries instead — never lose the record.
func settleIndexFailure(log *slog.Logger, m *metrics, pub deadLetterPublisher, msg jetstream.Msg, rec memory.Record, procErr error) {
	deliveries := numDelivered(msg)
	if deliveries >= memory.MaxDeliver {
		dl := memory.DeadLetter{
			Record:     rec,
			Error:      procErr.Error(),
			Deliveries: deliveries,
			FailedAt:   time.Now().UTC(),
		}
		if err := pub(dl); err != nil {
			// Couldn't preserve it — keep retrying rather than lose it.
			log.Error("dead-letter publish failed, will retry", "id", rec.ID, "err", err)
			_ = msg.NakWithDelay(5 * time.Second)
			m.count(kindIndex, outcomeRetry)
			return
		}
		log.Error("gave up indexing, dead-lettered", "id", rec.ID, "deliveries", deliveries, "err", procErr)
		_ = msg.Ack()
		m.count(kindIndex, outcomeDead)
		return
	}

	log.Warn("indexing failed, will retry", "id", rec.ID, "deliveries", deliveries, "err", procErr)
	_ = msg.NakWithDelay(2 * time.Second)
	m.count(kindIndex, outcomeRetry)
}
