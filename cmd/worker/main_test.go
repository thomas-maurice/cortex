package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/memory"
)

func TestMain(m *testing.M) {
	// Fast pull/backoff so the replica tests do not idle on a real fetch expiry.
	fetchWait, errBackoff = 500*time.Millisecond, 100*time.Millisecond
	os.Exit(m.Run())
}

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
		msg := &fakeMsg{data: rawPayload}
		var captured memory.DeadLetter
		pub := func(dl memory.DeadLetter) error {
			captured = dl
			return nil
		}

		deadLetterMalformed(context.Background(), slog.Default(), nil, kindIndex, pub, msg, unmarshalErr)

		require.NotZero(t, msg.termed.Load(), "message must be Term'd after a successful dead-letter publish so NATS does not redeliver it")
		assert.Zero(t, msg.nakked.Load(), "must not also Nak when Term succeeded")

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
		msg := &fakeMsg{data: rawPayload}
		pub := func(dl memory.DeadLetter) error {
			return errors.New("nats unavailable")
		}

		deadLetterMalformed(context.Background(), slog.Default(), nil, kindIndex, pub, msg, unmarshalErr)

		require.NotZero(t, msg.nakked.Load(), "must Nak when the dead-letter publish fails so NATS redelivers and we can retry preservation")
		assert.Zero(t, msg.termed.Load(), "must not Term when the dead-letter publish failed — that would permanently lose the bytes")
	})
}

// The ops listener env var is WORKER_METRICS_ADDR (renamed from METRICS_ADDR,
// which must no longer have any effect); empty disables.
func TestMetricsAddrFromEnv(t *testing.T) {
	t.Setenv("WORKER_METRICS_ADDR", "")
	require.NoError(t, os.Unsetenv("WORKER_METRICS_ADDR"))
	t.Setenv("METRICS_ADDR", ":1234")
	assert.Equal(t, ":9090", metricsAddrFromEnv(), "old METRICS_ADDR is ignored")
	t.Setenv("WORKER_METRICS_ADDR", "")
	assert.Equal(t, "", metricsAddrFromEnv(), "empty disables")
	t.Setenv("WORKER_METRICS_ADDR", ":9191")
	assert.Equal(t, ":9191", metricsAddrFromEnv())
}
