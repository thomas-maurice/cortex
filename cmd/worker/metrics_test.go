package main

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/memory"
)

func outcome(m *metrics, kind, out string) float64 {
	return testutil.ToFloat64(m.messages.WithLabelValues(kind, out))
}

// The outcome counter is what an operator alerts on ("dead is rising", "retry is
// stuck high"), so each settle path must bump exactly its own label — a retry
// that is miscounted as ack would hide a failing Ollama.
func TestWorkerOutcomeCounters(t *testing.T) {
	log := slog.Default()
	okPub := func(memory.DeadLetter) error { return nil }
	failPub := func(memory.DeadLetter) error { return errors.New("nats down") }
	procErr := errors.New("embed: boom")

	t.Run("ack on success", func(t *testing.T) {
		m := newMetrics(prometheus.NewRegistry())
		ack(log, m, kindIndex, &fakeMsg{})
		assert.Equal(t, 1.0, outcome(m, kindIndex, outcomeAck))
		assert.Equal(t, 0.0, outcome(m, kindIndex, outcomeRetry))
		assert.Equal(t, 0.0, outcome(m, kindIndex, outcomeDead))
	})

	t.Run("retry while under the delivery budget", func(t *testing.T) {
		m := newMetrics(prometheus.NewRegistry())
		msg := &fakeMsg{delivered: memory.MaxDeliver - 1}
		settleIndexFailure(log, m, okPub, msg, memory.Record{ID: "x"}, procErr)
		assert.NotZero(t, msg.nakked.Load())
		assert.Equal(t, 1.0, outcome(m, kindIndex, outcomeRetry))
		assert.Equal(t, 0.0, outcome(m, kindIndex, outcomeDead))
		assert.Equal(t, 0.0, outcome(m, kindIndex, outcomeAck), "a failed attempt must never count as ack")
	})

	t.Run("dead once the budget is spent", func(t *testing.T) {
		m := newMetrics(prometheus.NewRegistry())
		msg := &fakeMsg{delivered: memory.MaxDeliver}
		settleIndexFailure(log, m, okPub, msg, memory.Record{ID: "x"}, procErr)
		assert.Equal(t, 1.0, outcome(m, kindIndex, outcomeDead))
		assert.Equal(t, 0.0, outcome(m, kindIndex, outcomeRetry))
	})

	t.Run("failed dead-letter publish counts as retry, not dead", func(t *testing.T) {
		m := newMetrics(prometheus.NewRegistry())
		msg := &fakeMsg{delivered: memory.MaxDeliver}
		settleIndexFailure(log, m, failPub, msg, memory.Record{ID: "x"}, procErr)
		assert.NotZero(t, msg.nakked.Load())
		assert.Equal(t, 1.0, outcome(m, kindIndex, outcomeRetry))
		assert.Equal(t, 0.0, outcome(m, kindIndex, outcomeDead), "nothing was dead-lettered, so dead must not move")
	})

	t.Run("malformed payload counts as dead under its own kind", func(t *testing.T) {
		m := newMetrics(prometheus.NewRegistry())
		deadLetterMalformed(context.Background(), log, m, kindLink, okPub, &fakeMsg{data: []byte("{")}, procErr)
		assert.Equal(t, 1.0, outcome(m, kindLink, outcomeDead))
		assert.Equal(t, 0.0, outcome(m, kindIndex, outcomeDead))
	})

	t.Run("invalid link is term", func(t *testing.T) {
		m := newMetrics(prometheus.NewRegistry())
		msg := &fakeMsg{data: []byte(`{"op":"add","a":"same","b":"same"}`)}
		handleLink(context.Background(), log, m, nil, nil, nil, msg)
		require.NotZero(t, msg.termed.Load())
		assert.Equal(t, 1.0, outcome(m, kindLink, outcomeTerm))
	})

	t.Run("nil metrics is a safe no-op", func(t *testing.T) {
		assert.NotPanics(t, func() { ack(log, nil, kindIndex, &fakeMsg{}) })
	})
}

// Embed failures must be visible separately from latency: a slow-but-working
// Ollama and a broken one need different responses.
func TestObserveEmbed(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	m.observeEmbed(50*time.Millisecond, nil)
	m.observeEmbed(time.Second, errors.New("boom"))
	assert.Equal(t, 1.0, testutil.ToFloat64(m.embedErrs), "only the failed call counts as an error")
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == "cortex_embed_duration_seconds" {
			assert.Equal(t, uint64(2), mf.GetMetric()[0].GetHistogram().GetSampleCount(), "both calls feed the latency histogram")
			return
		}
	}
	t.Fatal("cortex_embed_duration_seconds not registered")
}
