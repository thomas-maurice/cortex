package main

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// stubMsg is a minimal jetstream.Msg implementation for unit tests. It records
// whether Term or Nak was called so tests can assert the correct ack path was
// taken without a live NATS server.
type stubMsg struct {
	data    []byte
	subject string
	termed  bool
	nakked  bool
}

func (s *stubMsg) Data() []byte                               { return s.data }
func (s *stubMsg) Subject() string                            { return s.subject }
func (s *stubMsg) Headers() nats.Header                       { return nil }
func (s *stubMsg) Reply() string                              { return "" }
func (s *stubMsg) Ack() error                                 { return nil }
func (s *stubMsg) DoubleAck(_ context.Context) error          { return nil }
func (s *stubMsg) Nak() error                                 { s.nakked = true; return nil }
func (s *stubMsg) NakWithDelay(_ time.Duration) error         { s.nakked = true; return nil }
func (s *stubMsg) InProgress() error                          { return nil }
func (s *stubMsg) Term() error                                { s.termed = true; return nil }
func (s *stubMsg) TermWithReason(_ string) error              { s.termed = true; return nil }
func (s *stubMsg) Metadata() (*jetstream.MsgMetadata, error) { return nil, errors.New("stub") }

// compile-time assertion that stubMsg satisfies the interface
var _ jetstream.Msg = (*stubMsg)(nil)

// applyEdge is the correctness core of the async link path: an add must produce a
// bidirectional edge (each endpoint points at the other), a remove must clear
// both sides, and — because the message can be redelivered or double-published —
// both operations must be idempotent so a retry never duplicates or corrupts the
// link graph. Each subtest pins one of those guarantees.
func TestApplyEdge(t *testing.T) {
	t.Run("add creates a bidirectional edge", func(t *testing.T) {
		newA, newB := applyEdge(memory.LinkOpAdd, "a", nil, "b", nil)
		assert.Equal(t, []string{"b"}, newA, "a must point at b")
		assert.Equal(t, []string{"a"}, newB, "b must point back at a")
	})

	t.Run("add merges into existing links, not replace", func(t *testing.T) {
		newA, newB := applyEdge(memory.LinkOpAdd, "a", []string{"x"}, "b", []string{"y"})
		assert.Equal(t, []string{"x", "b"}, newA)
		assert.Equal(t, []string{"y", "a"}, newB)
	})

	t.Run("add is idempotent", func(t *testing.T) {
		a1, b1 := applyEdge(memory.LinkOpAdd, "a", nil, "b", nil)
		a2, b2 := applyEdge(memory.LinkOpAdd, "a", a1, "b", b1)
		assert.Equal(t, a1, a2, "re-adding the same edge must not duplicate")
		assert.Equal(t, b1, b2)
	})

	t.Run("remove clears both sides", func(t *testing.T) {
		newA, newB := applyEdge(memory.LinkOpRemove, "a", []string{"b", "x"}, "b", []string{"a", "y"})
		assert.Equal(t, []string{"x"}, newA)
		assert.Equal(t, []string{"y"}, newB)
	})

	t.Run("remove is idempotent on an already-absent edge", func(t *testing.T) {
		newA, newB := applyEdge(memory.LinkOpRemove, "a", []string{"x"}, "b", []string{"y"})
		assert.Equal(t, []string{"x"}, newA, "removing an absent edge is a no-op")
		assert.Equal(t, []string{"y"}, newB)
	})
}

// planSupersedes decides which sources a merged memory deletes. Consolidation is
// destructive, so the guarantees that matter are: never delete the memory itself
// (that would erase the merge result), never act on an empty id, and collapse
// duplicates so a repeated id can't be deleted twice. Each subtest pins one so a
// regression can't quietly turn a merge into data loss.
func TestPlanSupersedes(t *testing.T) {
	t.Run("returns the distinct sources to delete", func(t *testing.T) {
		assert.Equal(t, []string{"a", "b"}, planSupersedes("new", []string{"a", "b"}))
	})

	t.Run("self-reference is dropped so the merge result is never deleted", func(t *testing.T) {
		assert.Equal(t, []string{"a"}, planSupersedes("new", []string{"a", "new"}))
	})

	t.Run("empty ids are dropped", func(t *testing.T) {
		assert.Equal(t, []string{"a"}, planSupersedes("new", []string{"", "a", ""}))
	})

	t.Run("duplicates collapse to one delete", func(t *testing.T) {
		assert.Equal(t, []string{"a"}, planSupersedes("new", []string{"a", "a"}))
	})

	t.Run("nothing to supersede yields no deletes", func(t *testing.T) {
		assert.Empty(t, planSupersedes("new", nil))
	})
}

// TestDeadLetterMalformed pins the behavior that a malformed index/summary/link
// payload is never silently dropped. A durability queue must preserve every byte
// for operator recovery — silence here means permanent data loss with no trace.
func TestDeadLetterMalformed(t *testing.T) {
	rawPayload := []byte(`{bad json`)
	unmarshalErr := errors.New("invalid character 'b' looking for beginning of object key string")

	t.Run("malformed payload is dead-lettered and message is Term'd", func(t *testing.T) {
		// WHY: a Term after a successful dead-letter publish is the correct outcome —
		// the bytes are preserved and NATS should not redeliver a fundamentally broken
		// payload that would only fail again.
		msg := &stubMsg{data: rawPayload}
		var captured memory.DeadLetter
		pub := func(dl memory.DeadLetter) error {
			captured = dl
			return nil
		}

		deadLetterMalformed(context.Background(), slog.Default(), pub, msg, unmarshalErr)

		require.True(t, msg.termed, "message must be Term'd after a successful dead-letter publish so NATS does not redeliver it")
		assert.False(t, msg.nakked, "must not also Nak when Term succeeded")

		// The raw bytes must survive intact so the operator can inspect the original payload.
		assert.Equal(t, string(rawPayload), captured.Record.Text, "raw bytes must be preserved verbatim in the dead-letter record")
		assert.Contains(t, captured.Error, "malformed payload", "error must identify this as a malformed-payload dead-letter")
		assert.NotEmpty(t, captured.Record.ID, "dead-letter record must have a non-empty ID so the listing is non-breaking")
		assert.Equal(t, "malformed-payload", captured.Record.Source, "source field must identify the origin for operator triage")
		assert.False(t, captured.FailedAt.IsZero(), "FailedAt must be stamped so the listing shows when the failure occurred")
	})

	t.Run("dead-letter publish failure falls back to Nak so message redelivers", func(t *testing.T) {
		// WHY: if we cannot write to the dead-letter subject we must NOT Term the
		// message — that would silently drop it. NAK causes NATS to redeliver so
		// we get another attempt to preserve the bytes.
		msg := &stubMsg{data: rawPayload}
		pub := func(dl memory.DeadLetter) error {
			return errors.New("nats unavailable")
		}

		deadLetterMalformed(context.Background(), slog.Default(), pub, msg, unmarshalErr)

		require.True(t, msg.nakked, "must Nak when the dead-letter publish fails so NATS redelivers and we can retry preservation")
		assert.False(t, msg.termed, "must not Term when the dead-letter publish failed — that would permanently lose the bytes")
	})
}
