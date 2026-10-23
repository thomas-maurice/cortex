package bus

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// migrate converts a legacy MEMORY stream (LimitsPolicy: keeps every message
// forever; retention is immutable in JetStream) to the InterestPolicy work queue
// plus the separate dead-letter stream. It never drops unprocessed work: it only
// deletes the old stream once both durable consumers report nothing pending and
// nothing unacked, otherwise it returns ErrMigrationPending and leaves everything
// as it was (the caller retries later). A KV lock makes concurrent processes
// (server, worker replicas) safe: exactly one migrates, the rest see
// ErrMigrationPending and carry on against the still-working old stream.
//
// Zero-loss protocol: before the final idle check the legacy stream is FENCED
// (subjects narrowed to memory.dead), so index/summary/link publishes fail loudly
// rather than being acked and then deleted with the stream; likewise the new
// stream only gets its real subjects after its consumers exist (ensureQueue). The
// fence is reversible and lifted on any non-success path, and a crashed migrator's
// stale fence is lifted by the next attempt.
func migrate(ctx context.Context, js jetstream.JetStream, o options) error {
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      migrationBucket,
		Description: "Cortex one-off migration locks",
		TTL:         migrationLease, // reclaims the lock of a crashed migrator
		History:     1,
		Storage:     jetstream.FileStorage,
	})
	if err != nil {
		return fmt.Errorf("migration lock bucket: %w", err)
	}
	host, _ := os.Hostname()
	rev, err := kv.Create(ctx, migrationKey, fmt.Appendf(nil, "%s/%d", host, os.Getpid()))
	if errors.Is(err, jetstream.ErrKeyExists) {
		return fmt.Errorf("%w: another process holds the migration lock", ErrMigrationPending)
	}
	if err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	defer func() { _ = kv.Delete(context.WithoutCancel(ctx), migrationKey, jetstream.LastRevision(rev)) }()

	// Re-check under the lock: someone may have finished before we got it.
	cur, err := js.Stream(ctx, memory.StreamName)
	if err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("migrate: %w", err)
	}
	if err != nil || cur.CachedInfo().Config.Retention != jetstream.LimitsPolicy {
		return ensureQueue(ctx, js, o)
	}
	legacy := cur.CachedInfo()
	log := o.log.With("stream", memory.StreamName)

	// A previous migrator may have crashed while the fence was up (subjects
	// narrowed): lift it first so this attempt starts from a serving stream.
	if !slices.Equal(legacy.Config.Subjects, legacySubjects) {
		if err := setSubjects(ctx, js, legacy.Config, legacySubjects); err != nil {
			return fmt.Errorf("migrate: lift stale fence: %w", err)
		}
		log.Warn("lifted a stale migration fence left by an interrupted migration")
	}

	// Cheap pre-check, so a busy queue never raises the fence.
	if idle, why, err := consumersIdle(ctx, js); err != nil {
		return fmt.Errorf("migrate: %w", err)
	} else if !idle {
		log.Info("MEMORY stream migration waiting: unprocessed messages remain", "why", why)
		return fmt.Errorf("%w: %s", ErrMigrationPending, why)
	}
	if err := ensureStreamConfig(ctx, js, deadConfig(o)); err != nil {
		return err
	}

	// Fence: narrow the legacy stream to the legacy dead-letter subject. From now on
	// index/summary/link publishes FAIL (no responders) instead of being acked into
	// a stream we are about to delete; callers see the error and retry. Reversible.
	if err := setSubjects(ctx, js, legacy.Config, []string{legacyDead}); err != nil {
		return fmt.Errorf("migrate: raise fence: %w", err)
	}
	unfence := func() {
		if err := setSubjects(context.WithoutCancel(ctx), js, legacy.Config, legacySubjects); err != nil {
			log.Error("could not lift the migration fence; the next start will", "err", err)
		}
	}
	idle, why, err := waitIdle(ctx, js, o.fenceWait)
	if err != nil {
		unfence()
		return fmt.Errorf("migrate: %w", err)
	}
	if !idle {
		unfence()
		log.Info("MEMORY stream migration waiting: consumers still busy", "why", why)
		return fmt.Errorf("%w: %s", ErrMigrationPending, why)
	}

	// Nothing can arrive any more and the consumers are done: copy the dead
	// letters (the complete set), then replace the stream.
	copied, err := copyDead(ctx, js, legacy.Created.UnixNano())
	if err != nil {
		unfence()
		return fmt.Errorf("migrate: copy dead letters: %w", err)
	}
	if copied > deadMaxMsgs {
		log.Warn("legacy dead letters exceed the dead-letter stream cap: the oldest were dropped",
			"copied", copied, "kept", deadMaxMsgs)
	}
	if err := js.DeleteStream(ctx, memory.StreamName); err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
		unfence()
		return fmt.Errorf("migrate: delete legacy stream: %w", err)
	}
	if err := ensureQueue(ctx, js, o); err != nil {
		return err
	}
	log.Info("MEMORY stream migrated to interest retention",
		"legacyMessages", legacy.State.Msgs, "deadLettersCopied", copied,
		"queueMaxAge", o.queueMaxAge.String(), "deadMaxAge", o.deadMaxAge.String())
	return nil
}

// legacySubjects is the pre-retention MEMORY subject set, restored when a fence
// is lifted.
var legacySubjects = []string{"memory.>"}

func setSubjects(ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig, subjects []string) error {
	cfg.Subjects = subjects
	_, err := js.UpdateStream(ctx, cfg)
	return err
}

// waitIdle polls consumersIdle until it holds or max elapses.
func waitIdle(ctx context.Context, js jetstream.JetStream, max time.Duration) (bool, string, error) {
	deadline := time.Now().Add(max)
	for {
		idle, why, err := consumersIdle(ctx, js)
		if err != nil || idle || time.Now().After(deadline) {
			return idle, why, err
		}
		select {
		case <-ctx.Done():
			return false, why, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// consumersIdle reports whether both durable consumers have fully processed the
// stream (nothing pending, nothing unacked). A MISSING consumer counts as idle
// only if the stream holds no message for its subjects: we cannot prove history
// was processed, so we refuse to delete it.
func consumersIdle(ctx context.Context, js jetstream.JetStream) (bool, string, error) {
	st, err := js.Stream(ctx, memory.StreamName)
	if err != nil {
		return false, "", fmt.Errorf("get stream: %w", err)
	}
	for _, cfg := range ConsumerConfigs(DefaultAckWait) {
		cons, err := js.Consumer(ctx, memory.StreamName, cfg.Durable)
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			for _, subj := range cfg.FilterSubjects {
				info, err := st.Info(ctx, jetstream.WithSubjectFilter(subj))
				if err != nil {
					return false, "", fmt.Errorf("stream info: %w", err)
				}
				if n := info.State.Subjects[subj]; n > 0 {
					return false, fmt.Sprintf("consumer %s is missing but the stream holds %d %s messages", cfg.Durable, n, subj), nil
				}
			}
			continue
		}
		if err != nil {
			return false, "", fmt.Errorf("consumer %s: %w", cfg.Durable, err)
		}
		info, err := cons.Info(ctx)
		if err != nil {
			return false, "", fmt.Errorf("consumer %s info: %w", cfg.Durable, err)
		}
		if info.NumPending > 0 || info.NumAckPending > 0 {
			return false, fmt.Sprintf("consumer %s has %d pending, %d unacked", cfg.Durable, info.NumPending, info.NumAckPending), nil
		}
	}
	return true, "", nil
}

// copyDead copies every dead letter of the legacy stream into MEMORY_DEAD,
// preserving payload and headers. Each copy carries a deterministic Nats-Msg-Id
// (legacy stream identity + sequence), and ids already present in the dead stream
// are skipped, so a re-run after a crash never duplicates (this does not rely on
// JetStream's short dedup window).
func copyDead(ctx context.Context, js jetstream.JetStream, legacyCreated int64) (int, error) {
	have := map[string]bool{}
	if err := readAll(ctx, js, memory.DeadStreamName, memory.SubjectDead, func(m jetstream.Msg) error {
		if id := m.Headers().Get(nats.MsgIdHdr); id != "" {
			have[id] = true
		}
		return nil
	}); err != nil {
		return 0, err
	}
	copied := 0
	err := readAll(ctx, js, memory.StreamName, legacyDead, func(m jetstream.Msg) error {
		meta, err := m.Metadata()
		if err != nil {
			return err
		}
		id := fmt.Sprintf("migrate:%d:%d", legacyCreated, meta.Sequence.Stream)
		if have[id] {
			return nil
		}
		msg := &nats.Msg{Subject: memory.SubjectDead, Data: m.Data(), Header: nats.Header{}}
		for k, v := range m.Headers() {
			msg.Header[k] = append([]string(nil), v...)
		}
		msg.Header.Set(nats.MsgIdHdr, id)
		if _, err := js.PublishMsg(ctx, msg); err != nil {
			return err
		}
		copied++
		return nil
	})
	return copied, err
}
