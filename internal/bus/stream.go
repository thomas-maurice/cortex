package bus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// Retention defaults. See docs/DESIGN.md "Streams and retention".
const (
	// DefaultQueueMaxAge is the MEMORY work queue's safety net: a message nobody
	// acked within it is DROPPED. It must exceed any realistic worker outage.
	DefaultQueueMaxAge = 7 * 24 * time.Hour
	// DefaultDeadMaxAge is how long dead letters are kept for inspection/requeue.
	DefaultDeadMaxAge = 30 * 24 * time.Hour
	// DefaultAckWait is the consumers' ack wait when this package has to create
	// them (the worker's own settings overwrite it via CreateOrUpdate).
	DefaultAckWait = 30 * time.Second

	// Size caps on the dead-letter stream (oldest dropped first), independent of age.
	deadMaxMsgs  = 10_000
	deadMaxBytes = 512 << 20

	migrationBucket = "cortex_migrations"
	migrationKey    = "memory-stream-retention"
	migrationLease  = 2 * time.Minute
	legacyDead      = "memory.dead" // dead-letter subject inside pre-migration streams
	// queuePlaceholder is the only subject MEMORY has while it is being created:
	// the real subjects are switched on AFTER the durable consumers exist, so a
	// publish can never be acked into an interest stream with nobody interested
	// (it would be discarded silently); it fails with "no responders" instead.
	queuePlaceholder = "memory.migrating"
	// defaultFenceWait bounds how long a migration holds the publish fence waiting
	// for the consumers to finish their last messages.
	defaultFenceWait = 5 * time.Second
)

// ErrMigrationPending means the MEMORY stream still has the legacy
// (LimitsPolicy) configuration and was left untouched this time: a consumer still
// has unprocessed messages, or another process is migrating. The stream is fully
// usable; nothing is lost. Retry later (see EnsureAndMigrate).
var ErrMigrationPending = errors.New("memory stream migration pending")

// ParseAge parses a retention env var (DEAD_LETTER_MAX_AGE, QUEUE_MAX_AGE):
// empty = def, otherwise a Go duration ("0" = keep forever / no cap). Strict:
// a typo like "30d" is an error, not a silent fallback, because it decides when
// data is deleted.
func ParseAge(name, raw string, def time.Duration) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: want a Go duration like 720h (days are not supported; use hours), or 0 for no limit: %w", name, raw, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("invalid %s %q: must not be negative", name, raw)
	}
	return d, nil
}

// OptionsFromEnv builds the options from QUEUE_MAX_AGE and DEAD_LETTER_MAX_AGE.
// Server and worker must run with the same values (each applies them on startup).
func OptionsFromEnv() ([]Option, error) {
	q, err := ParseAge("QUEUE_MAX_AGE", os.Getenv("QUEUE_MAX_AGE"), DefaultQueueMaxAge)
	if err != nil {
		return nil, err
	}
	d, err := ParseAge("DEAD_LETTER_MAX_AGE", os.Getenv("DEAD_LETTER_MAX_AGE"), DefaultDeadMaxAge)
	if err != nil {
		return nil, err
	}
	return []Option{WithQueueMaxAge(q), WithDeadMaxAge(d)}, nil
}

type options struct {
	queueMaxAge time.Duration
	deadMaxAge  time.Duration
	ackWait     time.Duration
	fenceWait   time.Duration
	log         *slog.Logger
}

// Option tunes EnsureStream.
type Option func(*options)

// WithQueueMaxAge sets the MEMORY safety-net age (0 = none).
func WithQueueMaxAge(d time.Duration) Option { return func(o *options) { o.queueMaxAge = d } }

// WithDeadMaxAge sets the dead-letter retention (0 = keep, size caps still apply).
func WithDeadMaxAge(d time.Duration) Option { return func(o *options) { o.deadMaxAge = d } }

// WithAckWait sets the AckWait used when consumers have to be created.
func WithAckWait(d time.Duration) Option { return func(o *options) { o.ackWait = d } }

// WithLogger sets the logger for migration progress (default slog.Default()).
func WithLogger(l *slog.Logger) Option { return func(o *options) { o.log = l } }

// withFenceWait is for tests.
func withFenceWait(d time.Duration) Option { return func(o *options) { o.fenceWait = d } }

func newOptions(opts []Option) options {
	o := options{queueMaxAge: DefaultQueueMaxAge, deadMaxAge: DefaultDeadMaxAge, ackWait: DefaultAckWait, fenceWait: defaultFenceWait, log: slog.Default()}
	for _, f := range opts {
		f(&o)
	}
	return o
}

func queueSubjects() []string {
	return []string{memory.SubjectIndex, memory.SubjectSummary, memory.SubjectLink}
}

func queueConfig(o options) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:        memory.StreamName,
		Description: "Cortex second-brain work queue (index/summary/link); removed once consumed",
		Subjects:    queueSubjects(),
		Storage:     jetstream.FileStorage,
		// A message is removed once every consumer whose filter matches it has
		// acked it. MaxAge is only the safety net for messages nobody consumes.
		Retention: jetstream.InterestPolicy,
		MaxAge:    o.queueMaxAge,
	}
}

func deadConfig(o options) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:        memory.DeadStreamName,
		Description: "Cortex dead-lettered index messages, kept for inspection/requeue",
		Subjects:    []string{memory.SubjectDead},
		Storage:     jetstream.FileStorage,
		Retention:   jetstream.LimitsPolicy,
		MaxAge:      o.deadMaxAge,
		MaxMsgs:     deadMaxMsgs,
		MaxBytes:    deadMaxBytes,
		Discard:     jetstream.DiscardOld,
	}
}

// ConsumerConfigs are the two durable consumers: indexer (index+summary) and
// linker (link). The worker uses the same configs, so there is one definition.
func ConsumerConfigs(ackWait time.Duration) []jetstream.ConsumerConfig {
	return []jetstream.ConsumerConfig{
		{Durable: memory.ConsumerName, AckPolicy: jetstream.AckExplicitPolicy, AckWait: ackWait,
			FilterSubjects: []string{memory.SubjectIndex, memory.SubjectSummary}, MaxDeliver: memory.MaxDeliver},
		{Durable: memory.LinkConsumerName, AckPolicy: jetstream.AckExplicitPolicy, AckWait: ackWait,
			FilterSubjects: []string{memory.SubjectLink}, MaxDeliver: memory.LinkMaxDeliver},
	}
}

// EnsureStream makes the MEMORY work queue and MEMORY_DEAD dead-letter streams
// (and the two durable consumers) exist with the current configuration. Safe to
// call concurrently from the server, workers and replicas.
//
// On an InterestPolicy stream a message published while NO consumer matches it is
// discarded immediately, so the durable consumers are created here too: otherwise
// the server could publish into the void before the first worker ever started.
//
// A legacy MEMORY stream (LimitsPolicy, pre-retention) is migrated in place when
// every message has been processed; otherwise it is left as is and
// ErrMigrationPending is returned (the stream stays usable).
func EnsureStream(ctx context.Context, js jetstream.JetStream, opts ...Option) error {
	// One ensure/create at a time per process: nats-server races a stream
	// UpdateConfig against a concurrent ConsumerStore create on the same stream.
	ensureMu.Lock()
	defer ensureMu.Unlock()
	o := newOptions(opts)
	if err := ensureStreamConfig(ctx, js, deadConfig(o)); err != nil {
		return err
	}
	cur, err := js.Stream(ctx, memory.StreamName)
	switch {
	case errors.Is(err, jetstream.ErrStreamNotFound):
		// fall through to create
	case err != nil:
		return fmt.Errorf("ensure stream: %w", err)
	case cur.CachedInfo().Config.Retention == jetstream.LimitsPolicy:
		return migrate(ctx, js, o)
	}
	return ensureQueue(ctx, js, o)
}

// ensureMu serialises every stream/consumer ensure in this process (EnsureStream,
// EnsureConsumer and the background migration retry).
var ensureMu sync.Mutex

// ensureQueue creates or repairs the interest-retention MEMORY stream in the safe
// order: stream with the placeholder subject, durable consumers, then the real
// subjects. Every caller follows it, so concurrent callers cannot open a window in
// which publishes are accepted with no consumer to hold them.
func ensureQueue(ctx context.Context, js jetstream.JetStream, o options) error {
	want := queueConfig(o)
	if _, err := js.Stream(ctx, memory.StreamName); errors.Is(err, jetstream.ErrStreamNotFound) {
		boot := want
		boot.Subjects = []string{queuePlaceholder}
		if _, err := js.CreateStream(ctx, boot); err != nil && !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			return fmt.Errorf("ensure stream %s: %w", want.Name, err)
		}
	} else if err != nil {
		return fmt.Errorf("ensure stream %s: %w", want.Name, err)
	}
	if err := ensureConsumers(ctx, js, o.ackWait); err != nil {
		return err
	}
	return ensureStreamConfig(ctx, js, want)
}

// EnsureConsumer creates or updates a durable consumer, serialised with
// EnsureStream (see ensureMu). The stream must exist.
func EnsureConsumer(ctx context.Context, js jetstream.JetStream, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	ensureMu.Lock()
	defer ensureMu.Unlock()
	return js.CreateOrUpdateConsumer(ctx, memory.StreamName, cfg)
}

// ensureStreamConfig creates the stream or updates it when MaxAge/subjects
// drifted; an already-correct stream is not touched (avoids needless updates).
func ensureStreamConfig(ctx context.Context, js jetstream.JetStream, want jetstream.StreamConfig) error {
	cur, err := js.Stream(ctx, want.Name)
	if err == nil {
		c := cur.CachedInfo().Config
		if c.MaxAge == want.MaxAge && c.MaxMsgs == want.MaxMsgs && c.MaxBytes == want.MaxBytes &&
			slices.Equal(c.Subjects, want.Subjects) {
			return nil
		}
	} else if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("ensure stream %s: %w", want.Name, err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, want); err != nil {
		return fmt.Errorf("ensure stream %s: %w", want.Name, err)
	}
	return nil
}

// ensureConsumers creates the durable consumers if missing and never touches an
// existing one (the worker owns their tuning).
func ensureConsumers(ctx context.Context, js jetstream.JetStream, ackWait time.Duration) error {
	for _, cfg := range ConsumerConfigs(ackWait) {
		if _, err := js.Consumer(ctx, memory.StreamName, cfg.Durable); err == nil {
			continue
		} else if !errors.Is(err, jetstream.ErrConsumerNotFound) {
			return fmt.Errorf("ensure consumer %s: %w", cfg.Durable, err)
		}
		if _, err := js.CreateConsumer(ctx, memory.StreamName, cfg); err != nil && !errors.Is(err, jetstream.ErrConsumerExists) {
			return fmt.Errorf("ensure consumer %s: %w", cfg.Durable, err)
		}
	}
	return nil
}

// EnsureAndMigrate is EnsureStream for process startup: a pending migration is
// not fatal; a goroutine retries it every `every` until it succeeds or ctx ends
// (the worker keeps draining the old stream meanwhile, which is what lets the
// migration eventually proceed).
func EnsureAndMigrate(ctx context.Context, js jetstream.JetStream, every time.Duration, opts ...Option) error {
	err := EnsureStream(ctx, js, opts...)
	if !errors.Is(err, ErrMigrationPending) {
		return err
	}
	log := newOptions(opts).log
	log.Warn("MEMORY stream migration deferred; will retry", "every", every.String(), "reason", err.Error())
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			err := EnsureStream(ctx, js, opts...)
			if errors.Is(err, ErrMigrationPending) {
				continue
			}
			if err != nil {
				log.Error("MEMORY stream migration retry failed", "err", err)
				continue
			}
			return
		}
	}()
	return nil
}
