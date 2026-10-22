package rpc

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// MetricsInterceptor records cortex_rpc_requests_total and
// cortex_rpc_duration_seconds per inbound unary RPC. Labels are the procedure
// (from the static Connect spec) and the outcome code — never request content,
// tenant ids or namespaces, so cardinality stays bounded. Register it outermost
// so calls rejected by the auth interceptor are counted too.
func MetricsInterceptor(reg prometheus.Registerer) connect.Interceptor {
	requests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_rpc_requests_total",
		Help: "RPC requests handled, by procedure and result code (ok or a Connect code).",
	}, []string{"procedure", "code"})
	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cortex_rpc_duration_seconds",
		Help:    "RPC handling latency in seconds, by procedure.",
		Buckets: prometheus.DefBuckets,
	}, []string{"procedure"})
	reg.MustRegister(requests, duration)

	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if req.Spec().IsClient {
				return next(ctx, req)
			}
			start := time.Now()
			resp, err := next(ctx, req)
			proc := req.Spec().Procedure
			code := "ok"
			if err != nil {
				code = connect.CodeOf(err).String()
			}
			requests.WithLabelValues(proc, code).Inc()
			duration.WithLabelValues(proc).Observe(time.Since(start).Seconds())
			return resp, err
		}
	})
}

// queueStats reads the index consumer's depth: present is false until the
// worker has created it. Shared by the IndexQueue RPC and the queue gauges so
// both report the same numbers.
func (s *Service) queueStats(ctx context.Context) (present bool, pending, inFlight int64, err error) {
	cons, err := s.js.Consumer(ctx, memory.StreamName, memory.ConsumerName)
	if err != nil {
		// No consumer yet means the worker has never started; report an empty
		// queue rather than failing the probe.
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			return false, 0, 0, nil
		}
		return false, 0, 0, err
	}
	info, err := cons.Info(ctx)
	if err != nil {
		return true, 0, 0, err
	}
	return true, int64(info.NumPending), int64(info.NumAckPending), nil
}

// deadCount counts dead-lettered messages from stream metadata alone (a subject
// filter on stream info), which is cheap enough to run on every scrape — unlike
// bus.FetchDead, which reads every record.
func (s *Service) deadCount(ctx context.Context) (int64, error) {
	st, err := s.js.Stream(ctx, memory.StreamName)
	if err != nil {
		return 0, err
	}
	info, err := st.Info(ctx, jetstream.WithSubjectFilter(memory.SubjectDead))
	if err != nil {
		return 0, err
	}
	return int64(info.State.Subjects[memory.SubjectDead]), nil
}

// queueScrapeTimeout bounds the NATS lookups done at scrape time.
const queueScrapeTimeout = 3 * time.Second

// QueueCollector exposes cortex_index_queue_pending and cortex_dead_letters,
// computed live at scrape time. A gauge whose lookup fails is omitted from that
// scrape (absent, not a misleading 0).
func (s *Service) QueueCollector() prometheus.Collector { return &queueCollector{svc: s} }

type queueCollector struct{ svc *Service }

var (
	pendingDesc = prometheus.NewDesc("cortex_index_queue_pending",
		"Messages waiting on the JetStream index consumer.", nil, nil)
	deadDesc = prometheus.NewDesc("cortex_dead_letters",
		"Dead-lettered (failed-to-index) messages currently held in the stream.", nil, nil)
)

func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- pendingDesc
	ch <- deadDesc
}

func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), queueScrapeTimeout)
	defer cancel()
	if present, pending, _, err := c.svc.queueStats(ctx); err != nil {
		c.svc.log.Warn("metrics: index queue lookup failed", "err", err)
	} else if present {
		ch <- prometheus.MustNewConstMetric(pendingDesc, prometheus.GaugeValue, float64(pending))
	}
	if n, err := c.svc.deadCount(ctx); err != nil {
		c.svc.log.Warn("metrics: dead-letter lookup failed", "err", err)
	} else {
		ch <- prometheus.MustNewConstMetric(deadDesc, prometheus.GaugeValue, float64(n))
	}
}
