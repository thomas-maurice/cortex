package main

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Message kinds and outcomes for cortex_worker_messages_total. Fixed sets, so the
// label cardinality is bounded.
const (
	kindIndex   = "index"
	kindSummary = "summary"
	kindLink    = "link"

	outcomeAck     = "ack"     // processed (or acknowledged as done)
	outcomeRetry   = "retry"   // NAK'd for redelivery
	outcomeDead    = "dead"    // dead-lettered
	outcomeTerm    = "term"    // permanently dropped without a dead-letter
	outcomeStale   = "stale"   // acked and skipped: a newer version was already stored
	outcomeSkipped = "skipped" // acked and skipped: create-only message for an id that is live (other message) or trashed
)

// metrics is the worker's Prometheus instrumentation. Every method is safe on a
// nil receiver so code paths (and tests) that run without metrics need no guards.
type metrics struct {
	messages  *prometheus.CounterVec
	embedDur  prometheus.Histogram
	embedErrs prometheus.Counter
	indexDur  prometheus.Histogram

	lockInfraErrs prometheus.Counter
	lockWait      prometheus.Histogram
	fetchErrs     prometheus.Counter
	holdTimeouts  prometheus.Counter
	recreated     prometheus.Counter
}

func newMetrics(reg prometheus.Registerer) *metrics {
	f := promauto.With(reg)
	return &metrics{
		messages: f.NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_worker_messages_total",
			Help: "Queue messages settled by the worker, by kind (index|summary|link) and outcome (ack|retry|dead|term|stale).",
		}, []string{"subject_kind", "outcome"}),
		embedDur: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_embed_duration_seconds",
			Help:    "Ollama /api/embed request latency in seconds (one per request of up to 32 inputs, retries included).",
			Buckets: prometheus.DefBuckets,
		}),
		embedErrs: f.NewCounter(prometheus.CounterOpts{
			Name: "cortex_embed_errors_total",
			Help: "Ollama /api/embed requests that failed after retries.",
		}),
		indexDur: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_index_duration_seconds",
			Help:    "End-to-end handleIndex latency in seconds (embed + upsert + chunks).",
			Buckets: prometheus.DefBuckets,
		}),
		lockInfraErrs: f.NewCounter(prometheus.CounterOpts{
			Name: "cortex_lock_infra_errors_total",
			Help: "Lock-bucket (NATS KV) errors other than contention, e.g. the bucket vanished; each is repaired and retried.",
		}),
		lockWait: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_lock_wait_seconds",
			Help:    "Time spent acquiring one per-id lock (contention + infra retries).",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 15, 30, 60, 90},
		}),
		fetchErrs: f.NewCounter(prometheus.CounterOpts{
			Name: "cortex_worker_fetch_errors_total",
			Help: "Failed JetStream pulls (consumer/stream gone, NATS unreachable). Missing consumers are re-created.",
		}),
		recreated: f.NewCounter(prometheus.CounterOpts{
			Name: "cortex_worker_consumer_recreated_total",
			Help: "Durable consumers re-created after being found missing. Messages published while one was missing were discarded (interest retention).",
		}),
		holdTimeouts: f.NewCounter(prometheus.CounterOpts{
			Name: "cortex_worker_hold_timeouts_total",
			Help: "Critical sections cut off by the lock hold deadline (3/4 of the lock TTL); the message is retried.",
		}),
	}
}

func (m *metrics) lockInfraError() {
	if m != nil {
		m.lockInfraErrs.Inc()
	}
}

func (m *metrics) observeLockWait(d time.Duration) {
	if m != nil {
		m.lockWait.Observe(d.Seconds())
	}
}

func (m *metrics) fetchError() {
	if m != nil {
		m.fetchErrs.Inc()
	}
}

func (m *metrics) consumerRecreated() {
	if m != nil {
		m.recreated.Inc()
	}
}

func (m *metrics) holdTimeout() {
	if m != nil {
		m.holdTimeouts.Inc()
	}
}

func (m *metrics) count(kind, outcome string) {
	if m != nil {
		m.messages.WithLabelValues(kind, outcome).Inc()
	}
}

// observeEmbed is the embed.Client observer.
func (m *metrics) observeEmbed(d time.Duration, err error) {
	if m == nil {
		return
	}
	m.embedDur.Observe(d.Seconds())
	if err != nil {
		m.embedErrs.Inc()
	}
}

func (m *metrics) observeIndex(start time.Time) {
	if m != nil {
		m.indexDur.Observe(time.Since(start).Seconds())
	}
}
