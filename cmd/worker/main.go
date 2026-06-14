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

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var (
		natsURL      = env("NATS_URL", "nats://localhost:4222")
		ollamaURL    = env("OLLAMA_URL", "http://localhost:11434")
		ollamaModel  = env("OLLAMA_MODEL", "qwen3-embedding:0.6b")
		weaviate     = env("WEAVIATE_HOST", "localhost:8080")
		weaviateGRPC = env("WEAVIATE_GRPC_HOST", "localhost:50051")
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

	log.Info("worker up", "nats", natsURL, "weaviate", weaviate, "model", ollamaModel)

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		handle(ctx, log, js, embedder, st, msg)
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
func handle(ctx context.Context, log *slog.Logger, js jetstream.JetStream, embedder *embed.Client, st *store.Store, msg jetstream.Msg) {
	if msg.Subject() == memory.SubjectSummary {
		handleSummary(ctx, log, embedder, st, msg)
		return
	}
	handleIndex(ctx, log, js, embedder, st, msg)
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

func handleIndex(ctx context.Context, log *slog.Logger, js jetstream.JetStream, embedder *embed.Client, st *store.Store, msg jetstream.Msg) {
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
		storeStart := time.Now()
		if err := st.Upsert(ctx, rec, vec); err != nil {
			procErr = fmt.Errorf("upsert: %w", err)
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
