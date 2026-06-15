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

	// Links ride their own durable consumer, decoupled from indexing: an edge
	// whose endpoint is still being embedded retries on this consumer's budget
	// without blocking or being blocked by the index stream.
	linkCons, err := js.CreateOrUpdateConsumer(ctx, memory.StreamName, jetstream.ConsumerConfig{
		Durable:        memory.LinkConsumerName,
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: []string{memory.SubjectLink},
		MaxDeliver:     memory.LinkMaxDeliver,
	})
	if err != nil {
		log.Error("create link consumer", "err", err)
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

	lcc, err := linkCons.Consume(func(msg jetstream.Msg) {
		handleLink(ctx, log, st, msg)
	})
	if err != nil {
		log.Error("consume links", "err", err)
		os.Exit(1)
	}
	defer lcc.Stop()

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

// linkMu serializes the read-modify-write the link consumer performs on each
// endpoint's linkedIds. SetLinks replaces the whole list, so concurrent applies
// touching the same endpoint would otherwise lose updates. This protects a single
// worker process; running multiple worker replicas reintroduces the cross-process
// race, which is non-fatal (it can leave a link asymmetric) but worth knowing.
var linkMu sync.Mutex

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
func handleLink(ctx context.Context, log *slog.Logger, st *store.Store, msg jetstream.Msg) {
	var lm memory.LinkMsg
	if err := json.Unmarshal(msg.Data(), &lm); err != nil {
		log.Error("bad link payload, terminating", "err", err)
		_ = msg.Term()
		return
	}
	if lm.A == "" || lm.B == "" || lm.A == lm.B {
		log.Warn("invalid link msg, terminating", "op", lm.Op, "a", lm.A, "b", lm.B)
		_ = msg.Term()
		return
	}

	linkMu.Lock()
	defer linkMu.Unlock()

	a, aOK, err := st.Get(ctx, lm.A)
	if err != nil {
		linkRetry(log, msg, lm, "endpoint lookup failed", err)
		return
	}
	b, bOK, err := st.Get(ctx, lm.B)
	if err != nil {
		linkRetry(log, msg, lm, "endpoint lookup failed", err)
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
			return
		}
		log.Info("link endpoint not indexed yet, will retry", "a", lm.A, "b", lm.B,
			"a_exists", aOK, "b_exists", bOK, "deliveries", deliveries)
		_ = msg.NakWithDelay(linkRetryDelay(deliveries))
		return
	}

	newA, newB := applyEdge(lm.Op, lm.A, a.LinkedIDs, lm.B, b.LinkedIDs)
	if aOK {
		if err := st.SetLinks(ctx, lm.A, newA); err != nil {
			linkRetry(log, msg, lm, "set links failed", err)
			return
		}
	}
	if bOK {
		if err := st.SetLinks(ctx, lm.B, newB); err != nil {
			linkRetry(log, msg, lm, "set links failed", err)
			return
		}
	}
	log.Info("link applied", "op", lm.Op, "a", lm.A, "b", lm.B)
	_ = msg.Ack()
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
func linkRetry(log *slog.Logger, msg jetstream.Msg, lm memory.LinkMsg, what string, err error) {
	deliveries := numDelivered(msg)
	log.Warn("link apply failed, will retry", "reason", what, "op", lm.Op, "a", lm.A, "b", lm.B, "deliveries", deliveries, "err", err)
	_ = msg.NakWithDelay(linkRetryDelay(deliveries))
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
// already deleted) is harmless.
func applySupersedes(ctx context.Context, log *slog.Logger, st *store.Store, rec memory.Record) {
	for _, id := range planSupersedes(rec.ID, rec.Supersedes) {
		if err := st.Delete(ctx, id); err != nil {
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
			// Requested links are no longer applied here: they travel on
			// SubjectLink and are applied by the link consumer, which waits for both
			// endpoints to exist. Supersedes still runs inline — now that the merged
			// memory is durable, delete the sources it replaces. Ordering matters:
			// post-upsert means a crash here can't lose the merged content.
			// Best-effort and non-fatal.
			applySupersedes(ctx, log, st, rec)
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
