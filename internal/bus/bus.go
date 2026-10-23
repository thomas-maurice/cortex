// Package bus wraps NATS JetStream connection, stream creation, and publishing.
package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/thomas-maurice/cortex/internal/identity"
	"github.com/thomas-maurice/cortex/internal/memory"
)

// Connect dials NATS and returns the connection plus a JetStream context.
func Connect(url string) (*nats.Conn, jetstream.JetStream, error) {
	nc, err := nats.Connect(url, nats.Name("cortex"))
	if err != nil {
		return nil, nil, fmt.Errorf("nats connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("jetstream init: %w", err)
	}
	return nc, js, nil
}

// tenantFromContext extracts the UserID from the context identity, falling back
// to the empty string if no identity is set (single-user / flag-off mode). The
// UserID is the ONLY source of the tenant on the NATS payload — it is never read
// from the request body (security invariant).
func tenantFromContext(ctx context.Context) string {
	id, ok := identity.From(ctx)
	if !ok {
		return ""
	}
	return id.UserID
}

// PublishIndex publishes a record for async indexing and waits for the broker ack.
// The record's UserID is stamped from the request context identity before
// publishing — the worker reads it to scope its store writes to the correct tenant.
func PublishIndex(ctx context.Context, js jetstream.JetStream, rec memory.Record) error {
	rec.UserID = tenantFromContext(ctx)
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := js.Publish(ctx, memory.SubjectIndex, data); err != nil {
		return fmt.Errorf("publish index: %w", err)
	}
	return nil
}

// PublishSummary publishes a conversation summary for async indexing and waits
// for the broker ack. The summary's UserID is stamped from the request context
// identity before publishing.
func PublishSummary(ctx context.Context, js jetstream.JetStream, sum memory.Summary) error {
	sum.UserID = tenantFromContext(ctx)
	data, err := json.Marshal(sum)
	if err != nil {
		return err
	}
	if _, err := js.Publish(ctx, memory.SubjectSummary, data); err != nil {
		return fmt.Errorf("publish summary: %w", err)
	}
	return nil
}

// PublishLink publishes a single bidirectional edge mutation to SubjectLink for
// the worker's link consumer to apply idempotently. Endpoints are canonicalized
// (sorted) so the same edge always carries the same Nats-Msg-Id, letting
// JetStream collapse redundant publishes within its dedup window; the worker's
// apply is idempotent regardless, so the dedup is an optimisation, not a
// correctness requirement. The UserID is stamped from context so the link
// consumer operates within the correct tenant.
func PublishLink(ctx context.Context, js jetstream.JetStream, op memory.LinkOp, a, b string) error {
	if a > b {
		a, b = b, a
	}
	data, err := json.Marshal(memory.LinkMsg{Op: op, A: a, B: b, UserID: tenantFromContext(ctx)})
	if err != nil {
		return err
	}
	msg := &nats.Msg{Subject: memory.SubjectLink, Data: data, Header: nats.Header{}}
	msg.Header.Set("Nats-Msg-Id", fmt.Sprintf("link:%s:%s:%s", op, a, b))
	if _, err := js.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("publish link: %w", err)
	}
	return nil
}

// PublishDead records a record that exhausted its indexing retries onto the
// dead-letter stream, preserving it for later inspection or requeue. The
// record's UserID is already set (it was stamped on the original publish); this
// function preserves it so a requeue targets the same tenant.
func PublishDead(ctx context.Context, js jetstream.JetStream, dl memory.DeadLetter) error {
	data, err := json.Marshal(dl)
	if err != nil {
		return err
	}
	if _, err := js.Publish(ctx, memory.SubjectDead, data); err != nil {
		return fmt.Errorf("publish dead-letter: %w", err)
	}
	return nil
}

// readAll feeds every message stored under subject in stream (as of the start of
// the call) to fn, through an ephemeral ordered consumer (AckNone, so it never
// holds interest and never consumes anything). It reads exactly the number of
// messages the consumer reports pending rather than stopping at the first short
// batch, and errors if the stream stops delivering before that (a silently short
// read would make the migration drop dead letters).
func readAll(ctx context.Context, js jetstream.JetStream, stream, subject string, fn func(jetstream.Msg) error) error {
	cons, err := js.OrderedConsumer(ctx, stream, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{subject},
	})
	if err != nil {
		return fmt.Errorf("ordered consumer on %s: %w", stream, err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		return fmt.Errorf("ordered consumer info on %s: %w", stream, err)
	}
	remaining := int(info.NumPending)
	for remaining > 0 {
		batch, err := cons.Fetch(min(remaining, 100), jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			return fmt.Errorf("fetch from %s: %w", stream, err)
		}
		n := 0
		for msg := range batch.Messages() {
			if err := fn(msg); err != nil {
				return err
			}
			n++
		}
		if err := batch.Error(); err != nil {
			return fmt.Errorf("fetch from %s: %w", stream, err)
		}
		if n == 0 {
			return fmt.Errorf("read %s: stream stopped delivering with %d messages unread", stream, remaining)
		}
		remaining -= n
	}
	return nil
}

// FetchDead reads all dead-lettered records without consuming them, so it is
// safe to call for reporting.
func FetchDead(ctx context.Context, js jetstream.JetStream) ([]memory.DeadLetter, error) {
	var out []memory.DeadLetter
	err := readAll(ctx, js, memory.DeadStreamName, memory.SubjectDead, func(msg jetstream.Msg) error {
		var dl memory.DeadLetter
		if err := json.Unmarshal(msg.Data(), &dl); err == nil {
			out = append(out, dl)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("dead-letters: %w", err)
	}
	return out, nil
}

// DeadCount is the number of dead letters held, from stream metadata alone (cheap
// enough for every metrics scrape, unlike FetchDead).
func DeadCount(ctx context.Context, js jetstream.JetStream) (int64, error) {
	s, err := js.Stream(ctx, memory.DeadStreamName)
	if err != nil {
		return 0, fmt.Errorf("get dead stream: %w", err)
	}
	return int64(s.CachedInfo().State.Msgs), nil
}

// PurgeDead removes all dead-lettered records.
func PurgeDead(ctx context.Context, js jetstream.JetStream) error {
	s, err := js.Stream(ctx, memory.DeadStreamName)
	if err != nil {
		return fmt.Errorf("get dead stream: %w", err)
	}
	if err := s.Purge(ctx); err != nil {
		return fmt.Errorf("purge dead-letters: %w", err)
	}
	return nil
}

// QueueOldestAge is the age of the oldest message still held by the MEMORY queue
// (0 when empty). On the interest stream that is the oldest message not yet acked
// by every matching consumer, so a value approaching QUEUE_MAX_AGE warns that
// unconsumed messages are about to be dropped.
func QueueOldestAge(ctx context.Context, js jetstream.JetStream) (time.Duration, error) {
	s, err := js.Stream(ctx, memory.StreamName)
	if err != nil {
		return 0, fmt.Errorf("get stream: %w", err)
	}
	st := s.CachedInfo().State
	if st.Msgs == 0 {
		return 0, nil
	}
	return max(time.Since(st.FirstTime), 0), nil
}
