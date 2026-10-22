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

	outcomeAck   = "ack"   // processed (or acknowledged as done)
	outcomeRetry = "retry" // NAK'd for redelivery
	outcomeDead  = "dead"  // dead-lettered
	outcomeTerm  = "term"  // permanently dropped without a dead-letter
)

// metrics is the worker's Prometheus instrumentation. Every method is safe on a
// nil receiver so code paths (and tests) that run without metrics need no guards.
type metrics struct {
	messages  *prometheus.CounterVec
	embedDur  prometheus.Histogram
	embedErrs prometheus.Counter
	indexDur  prometheus.Histogram
}

func newMetrics(reg prometheus.Registerer) *metrics {
	f := promauto.With(reg)
	return &metrics{
		messages: f.NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_worker_messages_total",
			Help: "Queue messages settled by the worker, by kind (index|summary|link) and outcome (ack|retry|dead|term).",
		}, []string{"subject_kind", "outcome"}),
		embedDur: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_embed_duration_seconds",
			Help:    "Ollama embed call latency in seconds (retries included).",
			Buckets: prometheus.DefBuckets,
		}),
		embedErrs: f.NewCounter(prometheus.CounterOpts{
			Name: "cortex_embed_errors_total",
			Help: "Ollama embed calls that failed after retries.",
		}),
		indexDur: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_index_duration_seconds",
			Help:    "End-to-end handleIndex latency in seconds (embed + upsert + chunks).",
			Buckets: prometheus.DefBuckets,
		}),
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
