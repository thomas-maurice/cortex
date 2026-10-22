package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// pool runs `size` consumer loops. Each loop pulls ONE message at a time
// (Fetch(1)), handles it, and only then pulls the next. So a process never holds
// more than `size` messages: a replica cannot hoard a backlog its peers could be
// working through (a streaming Consume callback that returns immediately would let
// it pull without bound), and every message it does hold is being actively
// processed and kept alive.
type pool struct {
	size int
	wg   sync.WaitGroup
	log  *slog.Logger
	m    *metrics // nil-safe

	// health tracks continuous fetch failure behind the worker's /readyz.
	health   *fetchHealth
	lastWarn atomic.Int64 // unix nanos of the last fetch-error Warn
	lastGone atomic.Int64 // unix nanos of the last "consumer missing" Error
}

// consumerRef holds the pool's consumer and can re-create it after a NATS wipe.
// ensure must (re)create the stream and consumer with the startup configuration.
// Recreation is only done when streamEmptyOrGone: a recreated durable consumer
// replays the whole stream (MEMORY keeps every message ever published), which would
// re-embed every memory and re-run every link op, so a consumer deleted while the
// stream still holds messages is NOT recreated automatically.
type consumerRef struct {
	js     jetstream.JetStream
	mu     sync.Mutex
	cons   jetstream.Consumer
	last   time.Time
	ensure func(ctx context.Context) (jetstream.Consumer, error)
}

func newConsumerRef(js jetstream.JetStream, c jetstream.Consumer, ensure func(ctx context.Context) (jetstream.Consumer, error)) *consumerRef {
	return &consumerRef{js: js, cons: c, ensure: ensure}
}

// streamEmptyOrGone reports whether recreating a consumer is harmless: the stream
// is gone or holds no messages (nothing to replay).
func streamEmptyOrGone(ctx context.Context, js jetstream.JetStream) bool {
	info, err := streamInfo(ctx, js)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return true
	}
	return err == nil && info.State.Msgs == 0
}

func (r *consumerRef) get() jetstream.Consumer {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cons
}

// recreate replaces bad (if still current) with a freshly ensured consumer,
// at most once per second.
func (r *consumerRef) recreate(ctx context.Context, bad jetstream.Consumer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cons != bad || time.Since(r.last) < time.Second {
		return nil
	}
	r.last = time.Now()
	c, err := r.ensure(ctx)
	if err != nil {
		return err
	}
	r.cons = c
	return nil
}

// consumerGone reports errors meaning the consumer or its stream no longer exists.
func consumerGone(err error) bool {
	return errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrConsumerDeleted) ||
		errors.Is(err, jetstream.ErrStreamNotFound) || errors.Is(err, jetstream.ErrNoStreamResponse) ||
		errors.Is(err, nats.ErrNoResponders)
}

// fetchHealth tracks continuous fetch failure for /readyz: a worker whose pulls
// have been failing for longer than maxAge is not ready (it processes nothing).
type fetchHealth struct {
	mu    sync.Mutex
	since time.Time
}

func (h *fetchHealth) ok() {
	h.mu.Lock()
	h.since = time.Time{}
	h.mu.Unlock()
}

func (h *fetchHealth) fail() {
	h.mu.Lock()
	if h.since.IsZero() {
		h.since = time.Now()
	}
	h.mu.Unlock()
}

// err is non-nil when fetches have failed continuously for more than maxAge.
func (h *fetchHealth) err(maxAge time.Duration) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.since.IsZero() && time.Since(h.since) > maxAge {
		return fmt.Errorf("jetstream fetch failing for %s", time.Since(h.since).Round(time.Second))
	}
	return nil
}

// handleGone deals with a consumer/stream that no longer exists: recreate it when
// that is harmless (stream gone or empty), otherwise leave it, shout (throttled)
// and let fetchHealth turn /readyz unready so the operator notices.
func (p *pool) handleGone(ctx context.Context, ref *consumerRef, bad jetstream.Consumer) {
	if streamEmptyOrGone(ctx, ref.js) {
		if err := ref.recreate(ctx, bad); err != nil {
			p.log.Warn("consumer re-create failed, will retry", "err", err)
		}
		return
	}
	if throttled(&p.lastGone, fetchWarnEvery) {
		p.log.Error("durable consumer is missing but the stream still holds messages; NOT recreating it " +
			"(a new consumer would replay the entire stream). Recreate it deliberately, see docs/DESIGN.md")
	}
}

// fetchFailed records one failed pull: counter, health, throttled Warn.
func (p *pool) fetchFailed(err error) {
	p.m.fetchError()
	p.health.fail()
	if throttled(&p.lastWarn, fetchWarnEvery) {
		p.log.Warn("jetstream fetch failed", "err", err)
	}
}

func newPool(size int, log *slog.Logger, m *metrics) *pool {
	return &pool{size: max(size, 1), log: log, m: m, health: &fetchHealth{}}
}

// throttled reports whether at least `every` has passed since *last (unix nanos)
// and, if so, claims the slot, so a repeating condition logs once per interval.
func throttled(last *atomic.Int64, every time.Duration) bool {
	now := time.Now().UnixNano()
	prev := last.Load()
	return time.Duration(now-prev) >= every && last.CompareAndSwap(prev, now)
}

// Tunables (vars so tests can shorten them). keepaliveInterval must stay well under
// the consumers' AckWait; shutdownGrace is how long an in-flight handler may keep
// running after the worker context is cancelled; fetchWait bounds one idle pull;
// errBackoff paces retries after a failed pull.
var (
	keepaliveInterval = 5 * time.Second
	shutdownGrace     = 10 * time.Second
	fetchWait         = 2 * time.Second
	errBackoff        = time.Second
	fetchWarnEvery    = 30 * time.Second
	// maxProcessing bounds ONE message's total processing time (WORKER_MAX_PROCESSING,
	// 0 = unbounded). The keepalive would otherwise hold a wedged message (e.g. a lock
	// set that can never be acquired) forever, starving a worker slot and never letting
	// JetStream redeliver it. Past it the handler's context is cancelled and keepalives
	// stop; the handler then settles the message as an ordinary failure (counts toward
	// MaxDeliver, then dead-letter) -- NOT as a shutdown hand-back.
	maxProcessing = 10 * time.Minute
)

// errMaxProcessing is the cancel cause of a handler context that outlived maxProcessing.
var errMaxProcessing = errors.New("message exceeded the maximum processing time")

// run starts the consumer loops; they stop when ctx is cancelled (after finishing
// their in-flight message, within the grace) and wait() returns once all have.
func (p *pool) run(ctx context.Context, ref *consumerRef, fn func(ctx context.Context, msg jetstream.Msg)) {
	for range p.size {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for ctx.Err() == nil {
				cons := ref.get()
				batch, err := cons.Fetch(1, jetstream.FetchMaxWait(fetchWait))
				if err == nil {
					for msg := range batch.Messages() {
						process(ctx, msg, fn)
					}
					err = batch.Error()
				}
				if err != nil && ctx.Err() == nil {
					p.fetchFailed(err)
					if consumerGone(err) {
						p.handleGone(ctx, ref, cons)
					}
					_ = sleepCtx(ctx, errBackoff)
					continue
				}
				p.health.ok()
			}
		}()
	}
}

// sleepCtx sleeps d, returning ctx.Err() if ctx ends first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// process runs fn on msg, keeping msg alive with InProgress for the whole time
// (embedding, lock waits and store writes can all outlast AckWait). fn gets a
// context that is NOT cancelled the moment ctx is: it is cancelled shutdownGrace
// later, so an in-flight commit can finish. A message fetched after ctx was
// cancelled is NAK'd (handed to another replica) without running. (Any
// redelivery counts one delivery; JetStream has no uncounted hand-back.)
func process(ctx context.Context, msg jetstream.Msg, fn func(ctx context.Context, msg jetstream.Msg)) {
	if ctx.Err() != nil {
		_ = msg.Nak()
		return
	}
	done := make(chan struct{})
	defer close(done)
	interval, grace := keepaliveInterval, shutdownGrace

	wctx, cancelCause := context.WithCancelCause(context.WithoutCancel(ctx))
	defer cancelCause(nil)
	cancel := func() { cancelCause(context.Canceled) }
	var limitC <-chan time.Time
	if maxProcessing > 0 {
		lt := time.NewTimer(maxProcessing)
		defer lt.Stop()
		limitC = lt.C
	}
	// One goroutine: keepalive ticks for the message's whole life, and cancellation
	// of the handler's context `grace` after the worker context ends.
	go func() {
		tick := time.NewTicker(interval)
		defer tick.Stop()
		ctxDone := ctx.Done()
		var graceC <-chan time.Time
		tickC := tick.C
		for {
			select {
			case <-done:
				return
			case <-limitC:
				// Stop the keepalive and cancel: the message is no longer ours to hold.
				limitC, tickC = nil, nil
				cancelCause(errMaxProcessing)
			case <-tickC:
				_ = msg.InProgress()
			case <-ctxDone:
				ctxDone = nil
				t := time.NewTimer(grace)
				defer t.Stop()
				graceC = t.C
			case <-graceC:
				cancel()
				graceC = nil
			}
		}
	}()
	fn(wctx, msg)
}

// wait blocks until every loop has exited.
func (p *pool) wait() { p.wg.Wait() }
