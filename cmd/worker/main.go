// Command worker is the indexing consumer. It pulls memory.index events from
// JetStream, embeds the text via Ollama, and writes vectors to Weaviate.
// This is the ONLY process that mutates the vector store on write.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/thomas-maurice/cortex/internal/bus"
	"github.com/thomas-maurice/cortex/internal/embed"
	"github.com/thomas-maurice/cortex/internal/memory"
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
	// Weaviate may still be starting on a fresh stack; retry rather than
	// crash-loop on the restart policy.
	if err := ensureSchemaWithRetry(ctx, log, st); err != nil {
		log.Error("ensure schema", "err", err)
		os.Exit(1)
	}

	embedder := embed.New(ollamaURL, ollamaModel)

	cons, err := js.CreateOrUpdateConsumer(ctx, memory.StreamName, jetstream.ConsumerConfig{
		Durable:        memory.ConsumerName,
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: []string{memory.SubjectIndex, memory.SubjectSummary},
		MaxDeliver:     memory.MaxDeliver,
	})
	if err != nil {
		log.Error("create consumer", "err", err)
		os.Exit(1)
	}

	log.Info("worker up", "nats", natsURL, "weaviate", weaviate, "model", ollamaModel, "dedup_distance", dedupDistance)

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		handle(ctx, log, js, embedder, st, msg, dedupDistance)
	})
	if err != nil {
		log.Error("consume", "err", err)
		os.Exit(1)
	}
	defer cc.Stop()

	<-ctx.Done()
	log.Info("shutting down")
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
func handle(ctx context.Context, log *slog.Logger, js jetstream.JetStream, embedder *embed.Client, st *store.Store, msg jetstream.Msg, dedupDistance float32) {
	if msg.Subject() == memory.SubjectSummary {
		handleSummary(ctx, log, embedder, st, msg)
		return
	}
	handleIndex(ctx, log, js, embedder, st, msg, dedupDistance)
}

// handleSummary embeds a conversation summary and upserts it (one per
// conversation, replaced in place). Summaries are reproducible — the agent
// re-summarises periodically — so on permanent failure we log loudly and ack
// rather than dead-letter.
func handleSummary(ctx context.Context, log *slog.Logger, embedder *embed.Client, st *store.Store, msg jetstream.Msg) {
	var sum memory.Summary
	if err := json.Unmarshal(msg.Data(), &sum); err != nil {
		log.Error("bad summary payload, terminating", "err", err)
		_ = msg.Term()
		return
	}

	var procErr error
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
		if err := st.UpsertSummary(ctx, sum, vec); err != nil {
			procErr = fmt.Errorf("upsert: %w", err)
		}
		storeDur = time.Since(storeStart)
	}

	if procErr == nil {
		log.Info("summarized", "conversation", sum.ConversationID, "dims", len(vec),
			"embed_ms", embedDur.Milliseconds(), "store_ms", storeDur.Milliseconds(),
			"total_ms", (embedDur + storeDur).Milliseconds(),
			"queue_latency_ms", time.Since(sum.UpdatedAt).Milliseconds())
		_ = msg.Ack()
		return
	}

	deliveries := 0
	if md, err := msg.Metadata(); err == nil {
		deliveries = int(md.NumDelivered)
	}
	if deliveries >= memory.MaxDeliver {
		log.Error("gave up summarizing, dropping (agent will re-summarize)", "conversation", sum.ConversationID, "deliveries", deliveries, "err", procErr)
		_ = msg.Ack()
		return
	}
	log.Warn("summarizing failed, will retry", "conversation", sum.ConversationID, "deliveries", deliveries, "err", procErr)
	_ = msg.NakWithDelay(2 * time.Second)
}

// linkMu serializes the read-modify-write that link application performs on each
// target's linkedIds. SetLinks replaces the whole list, so concurrent consumers
// linking to the same target would otherwise lose updates (the Link RPC guards
// the same hazard with its own mutex). This protects a single worker process;
// running multiple worker replicas reintroduces the cross-process race, which is
// non-fatal (it can leave a link asymmetric) but worth knowing.
var linkMu sync.Mutex

// linkTarget is one resolved entry of rec.LinkTo: the target id, its current
// link set, and whether it actually exists in the store.
type linkTarget struct {
	id     string
	links  []string // the target's current linkedIds
	exists bool
}

// planLinks computes the bidirectional link mutations for a freshly-indexed
// record. A target is skipped when it is empty, equals the record itself, was
// not found, or repeats an earlier target. It returns the record's full desired
// forward link set and, per changed target, that target's new link set (the
// reverse direction). Pure: all store IO is done by the caller, so the
// skip/dedup/bidirectional contract is unit-testable without a live Weaviate.
func planLinks(recID string, recLinks []string, targets []linkTarget) (forward []string, reverse map[string][]string) {
	forward = append([]string(nil), recLinks...)
	reverse = make(map[string][]string)
	seen := make(map[string]bool, len(targets))
	for _, t := range targets {
		if t.id == "" || t.id == recID || !t.exists || seen[t.id] {
			continue
		}
		seen[t.id] = true
		forward = addUnique(forward, t.id)
		reverse[t.id] = addUnique(t.links, recID)
	}
	return forward, reverse
}

// applyLinks bidirectionally links a freshly-indexed record to each target in
// rec.LinkTo. Best-effort: a missing/stale target is skipped and logged, never
// fatal — the record is already indexed by the time this runs.
func applyLinks(ctx context.Context, log *slog.Logger, st *store.Store, rec memory.Record) {
	if len(rec.LinkTo) == 0 {
		return
	}
	linkMu.Lock()
	defer linkMu.Unlock()

	targets := make([]linkTarget, 0, len(rec.LinkTo))
	for _, id := range rec.LinkTo {
		if id == "" || id == rec.ID {
			continue
		}
		t, found, err := st.Get(ctx, id)
		if err != nil {
			log.Warn("link target lookup failed, skipping", "id", rec.ID, "target", id, "err", err)
			continue
		}
		if !found {
			log.Warn("link target does not exist, skipping", "id", rec.ID, "target", id)
			continue
		}
		targets = append(targets, linkTarget{id: id, links: t.LinkedIDs, exists: true})
	}

	forward, reverse := planLinks(rec.ID, rec.LinkedIDs, targets)
	for id, links := range reverse {
		if err := st.SetLinks(ctx, id, links); err != nil {
			log.Warn("link target update failed, skipping", "id", rec.ID, "target", id, "err", err)
			continue
		}
		log.Info("linked", "id", rec.ID, "target", id)
	}
	// Write the record's own forward links only if any target was actually added.
	if len(forward) > len(rec.LinkedIDs) {
		if err := st.SetLinks(ctx, rec.ID, forward); err != nil {
			log.Warn("forward link update failed", "id", rec.ID, "err", err)
		}
	}
}

// addUnique appends v to xs if absent, preserving order.
func addUnique(xs []string, v string) []string {
	if slices.Contains(xs, v) {
		return xs
	}
	return append(xs, v)
}

// candidateLimit caps how many duplicate candidates one memory records. A handful
// is enough to flag a cluster for review; the rest of the cluster surfaces via
// each member's own candidates.
const candidateLimit = 5

// findCandidates returns ids of already-indexed memories within dedupDistance of
// rec's vector, in the same namespace — heuristic duplicate hints for later
// review. Returns nil when the check is disabled (dedupDistance<=0). Best-effort:
// a search failure is logged and treated as "no candidates", never fatal, since
// the record must still be indexed regardless.
func findCandidates(ctx context.Context, log *slog.Logger, st *store.Store, rec memory.Record, vec []float32, dedupDistance float32) []string {
	if dedupDistance <= 0 {
		return nil
	}
	hits, err := st.Search(ctx, vec, store.SearchOpts{
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

func handleIndex(ctx context.Context, log *slog.Logger, js jetstream.JetStream, embedder *embed.Client, st *store.Store, msg jetstream.Msg, dedupDistance float32) {
	var rec memory.Record
	if err := json.Unmarshal(msg.Data(), &rec); err != nil {
		// Unrecoverable: bad payload. Terminate so it is not redelivered.
		log.Error("bad payload, terminating", "err", err)
		_ = msg.Term()
		return
	}

	var procErr error
	var embedDur, storeDur time.Duration
	embedStart := time.Now()
	vec, err := embedder.Embed(ctx, rec.Text)
	embedDur = time.Since(embedStart)
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
		rec.DupCandidates = findCandidates(ctx, log, st, rec, vec, dedupDistance)
		storeStart := time.Now()
		if err := st.Upsert(ctx, rec, vec); err != nil {
			procErr = fmt.Errorf("upsert: %w", err)
		} else {
			// The record now exists in the store, so its requested links can be
			// applied. Best-effort and non-fatal: link problems must not fail the
			// (already successful) index.
			applyLinks(ctx, log, st, rec)
		}
		storeDur = time.Since(storeStart)
	}

	if procErr == nil {
		// queue_latency_ms is the end-to-end wait from publish (rec.CreatedAt) to
		// indexed — it includes JetStream queue time and any Ollama cold-load, so
		// it is the number to watch for "why did indexing take so long".
		log.Info("indexed", "id", rec.ID, "namespace", rec.Namespace, "dims", len(vec),
			"embed_ms", embedDur.Milliseconds(), "store_ms", storeDur.Milliseconds(),
			"total_ms", (embedDur + storeDur).Milliseconds(),
			"queue_latency_ms", time.Since(rec.CreatedAt).Milliseconds())
		_ = msg.Ack()
		return
	}

	// Failure path. Retry until MaxDeliver, then dead-letter so the record is
	// preserved rather than silently dropped.
	deliveries := 0
	if md, err := msg.Metadata(); err == nil {
		deliveries = int(md.NumDelivered)
	}
	if deliveries >= memory.MaxDeliver {
		dl := memory.DeadLetter{
			Record:     rec,
			Error:      procErr.Error(),
			Deliveries: deliveries,
			FailedAt:   time.Now().UTC(),
		}
		if err := bus.PublishDead(ctx, js, dl); err != nil {
			// Couldn't preserve it — keep retrying rather than lose it.
			log.Error("dead-letter publish failed, will retry", "id", rec.ID, "err", err)
			_ = msg.NakWithDelay(5 * time.Second)
			return
		}
		log.Error("gave up indexing, dead-lettered", "id", rec.ID, "deliveries", deliveries, "err", procErr)
		_ = msg.Ack()
		return
	}

	log.Warn("indexing failed, will retry", "id", rec.ID, "deliveries", deliveries, "err", procErr)
	_ = msg.NakWithDelay(2 * time.Second)
}
