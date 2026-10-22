package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/bus"
	"github.com/thomas-maurice/cortex/internal/chunk"
	"github.com/thomas-maurice/cortex/internal/dlock"
	"github.com/thomas-maurice/cortex/internal/embed"
	"github.com/thomas-maurice/cortex/internal/identity"
	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/natstest"
	"github.com/thomas-maurice/cortex/internal/store"
)

// ---- fake store -----------------------------------------------------------

// fakeStore models Weaviate faithfully where it matters for these tests: objects
// are a set of independent fields; a PATCH/merge replaces only the fields it names,
// a PUT/Upsert replaces ALL of them (anything not supplied reverts to the supplied
// zero value), and a read-modify-write by a caller is NOT atomic. Every operation
// yields (a short random sleep) before taking effect so races actually manifest.
type fakeStore struct {
	mu     sync.Mutex
	objs   map[string]*memory.Record
	chunks map[string][]string
	// chunkTags records the tags each memory's chunks were last written with.
	chunkTags map[string][]string
	sums      map[string]*memory.Summary
	rngMu     sync.Mutex
	rng       *rand.Rand
	log       []string // audit trail of write ops, "<op> <id>"
	// deleteReasons records the trash reason of every delete, "<id> <reason>".
	deleteReasons []string
	// dupHits are returned by SearchMemoryVectors (dedup neighbours).
	dupHits []string
	// writeDelay slows every index write (Upsert/UpdateIndexFields), to model a
	// slow Weaviate; writes counts them.
	writeDelay time.Duration
	writes     atomic.Int32
}

func (f *fakeStore) resetAudit() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log = nil
}

func newFakeStore(seed uint64) *fakeStore {
	return &fakeStore{
		objs:      map[string]*memory.Record{},
		chunks:    map[string][]string{},
		chunkTags: map[string][]string{},
		sums:      map[string]*memory.Summary{},
		rng:       rand.New(rand.NewPCG(seed, 99)),
	}
}

func (f *fakeStore) yield() {
	f.rngMu.Lock()
	d := time.Duration(f.rng.IntN(3000)) * time.Microsecond
	f.rngMu.Unlock()
	time.Sleep(d)
}

func (f *fakeStore) tenant(userID string) tenantStore { return &fakeTenant{f: f, t: userID} }

// peek reads without yielding or auditing (test assertions only).
func (f *fakeStore) peek(tenant, id string) (memory.Record, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objs[tenant+"|"+id]
	if !ok {
		return memory.Record{}, false
	}
	return clone(*o), true
}

func (f *fakeStore) auditOps() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.log)
}

func clone(r memory.Record) memory.Record {
	r.Tags = slices.Clone(r.Tags)
	r.LinkedIDs = slices.Clone(r.LinkedIDs)
	r.DupCandidates = slices.Clone(r.DupCandidates)
	r.NotDuplicateOf = slices.Clone(r.NotDuplicateOf)
	return r
}

type fakeTenant struct {
	f *fakeStore
	t string
}

func (ft *fakeTenant) key(id string) string { return ft.t + "|" + id }

func (ft *fakeTenant) Get(_ context.Context, id string) (memory.Record, bool, error) {
	ft.f.yield()
	ft.f.mu.Lock()
	defer ft.f.mu.Unlock()
	o, ok := ft.f.objs[ft.key(id)]
	if !ok {
		return memory.Record{}, false, nil
	}
	return clone(*o), true, nil
}

// Upsert is a full replace: EVERY field comes from rec.
func (ft *fakeTenant) Upsert(ctx context.Context, rec memory.Record, _ []float32) error {
	ft.f.yield()
	ft.f.writes.Add(1)
	if err := sleepCtx(ctx, ft.f.writeDelay); err != nil {
		return err
	}
	ft.f.mu.Lock()
	defer ft.f.mu.Unlock()
	r := clone(rec)
	ft.f.objs[ft.key(rec.ID)] = &r
	ft.f.log = append(ft.f.log, "upsert "+rec.ID)
	return nil
}

// UpdateIndexFields is a merge of exactly the index-owned fields: keep this list in
// step with indexOwnedProps in internal/store/weaviate.go.
func (ft *fakeTenant) UpdateIndexFields(ctx context.Context, rec memory.Record, _ []float32) error {
	ft.f.yield()
	ft.f.writes.Add(1)
	if err := sleepCtx(ctx, ft.f.writeDelay); err != nil {
		return err
	}
	ft.f.mu.Lock()
	defer ft.f.mu.Unlock()
	o, ok := ft.f.objs[ft.key(rec.ID)]
	if !ok {
		return errors.New("merge: object not found")
	}
	o.Text, o.Namespace, o.Source, o.CreatedAt = rec.Text, rec.Namespace, rec.Source, rec.CreatedAt
	o.Tags, o.ConversationID = slices.Clone(rec.Tags), rec.ConversationID
	o.Model, o.Dims = rec.Model, rec.Dims
	o.DupCandidates = slices.Clone(rec.DupCandidates)
	o.IndexedSeq, o.IndexedStream = rec.IndexedSeq, rec.IndexedStream
	ft.f.log = append(ft.f.log, "merge-index "+rec.ID)
	return nil
}

func (ft *fakeTenant) mergeField(id, op string, apply func(*memory.Record)) error {
	ft.f.yield()
	ft.f.mu.Lock()
	defer ft.f.mu.Unlock()
	o, ok := ft.f.objs[ft.key(id)]
	if !ok {
		return errors.New("merge: object not found")
	}
	apply(o)
	ft.f.log = append(ft.f.log, op+" "+id)
	return nil
}

func (ft *fakeTenant) SetLinks(_ context.Context, id string, links []string) error {
	return ft.mergeField(id, "set-links", func(o *memory.Record) { o.LinkedIDs = slices.Clone(links) })
}

// Server-owned merge writers (the worker only calls them to carry a superseded memory's stats/dismissals).
func (ft *fakeTenant) Reinforce(_ context.Context, id string, count int, at time.Time) error {
	return ft.mergeField(id, "reinforce", func(o *memory.Record) { o.AccessCount = count; o.LastAccessedAt = at })
}

func (ft *fakeTenant) SetNotDuplicateOf(_ context.Context, id string, ids []string) error {
	return ft.mergeField(id, "set-notdup", func(o *memory.Record) { o.NotDuplicateOf = slices.Clone(ids) })
}

func (ft *fakeTenant) SetValidUntil(_ context.Context, id string, until time.Time) error {
	return ft.mergeField(id, "set-valid-until", func(o *memory.Record) { o.ValidUntil = until })
}

func (ft *fakeTenant) ReplaceChunks(_ context.Context, rec memory.Record, chunks []store.ChunkVec) error {
	ft.f.yield()
	ft.f.mu.Lock()
	defer ft.f.mu.Unlock()
	var texts []string
	for _, c := range chunks {
		texts = append(texts, c.Text)
	}
	ft.f.chunks[ft.key(rec.ID)] = texts
	ft.f.chunkTags[ft.key(rec.ID)] = rec.Tags
	return nil
}

func (ft *fakeTenant) DeleteWithReason(_ context.Context, id, reason string) error {
	ft.f.yield()
	ft.f.mu.Lock()
	defer ft.f.mu.Unlock()
	ft.f.deleteReasons = append(ft.f.deleteReasons, id+" "+reason)
	delete(ft.f.objs, ft.key(id))
	delete(ft.f.chunks, ft.key(id))
	return nil
}

func (ft *fakeTenant) GetSummarySeq(_ context.Context, convID string) (uint64, int64, error) {
	ft.f.yield()
	ft.f.mu.Lock()
	defer ft.f.mu.Unlock()
	s, ok := ft.f.sums[ft.key(convID)]
	if !ok {
		return 0, 0, nil
	}
	return s.IndexedSeq, s.IndexedStream, nil
}

func (ft *fakeTenant) UpsertSummary(_ context.Context, sum memory.Summary, _ []float32) error {
	ft.f.yield()
	ft.f.mu.Lock()
	defer ft.f.mu.Unlock()
	s := sum
	ft.f.sums[ft.key(sum.ConversationID)] = &s
	return nil
}

func (ft *fakeTenant) SearchMemoryVectors(context.Context, []float32, store.SearchOpts) ([]memory.Hit, error) {
	ft.f.mu.Lock()
	defer ft.f.mu.Unlock()
	var hits []memory.Hit
	for _, id := range ft.f.dupHits {
		hits = append(hits, memory.Hit{Record: memory.Record{ID: id}})
	}
	return hits, nil
}

// ---- fake ollama ----------------------------------------------------------

func fakeEmbedder(t *testing.T) *embed.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Input []string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		out := make([][]float32, len(req.Input))
		for i, in := range req.Input {
			out[i] = []float32{float32(len(in)), 1, 0, 0}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"embeddings": out})
	}))
	t.Cleanup(srv.Close)
	return embed.New(srv.URL, "fake-model")
}

// ---- fake message ---------------------------------------------------------

// fakeMsg is a thread-safe jetstream.Msg recording how it was settled.
type fakeMsg struct {
	data      []byte
	subject   string
	seq       uint64
	delivered uint64
	acked     atomic.Int32
	nakked    atomic.Int32
	termed    atomic.Int32
	progress  atomic.Int32
}

func (m *fakeMsg) Data() []byte                     { return m.data }
func (m *fakeMsg) Subject() string                  { return m.subject }
func (m *fakeMsg) Headers() nats.Header             { return nil }
func (m *fakeMsg) Reply() string                    { return "" }
func (m *fakeMsg) Ack() error                       { m.acked.Add(1); return nil }
func (m *fakeMsg) DoubleAck(context.Context) error  { m.acked.Add(1); return nil }
func (m *fakeMsg) Nak() error                       { m.nakked.Add(1); return nil }
func (m *fakeMsg) NakWithDelay(time.Duration) error { m.nakked.Add(1); return nil }
func (m *fakeMsg) InProgress() error                { m.progress.Add(1); return nil }
func (m *fakeMsg) Term() error                      { m.termed.Add(1); return nil }
func (m *fakeMsg) TermWithReason(string) error      { m.termed.Add(1); return nil }
func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: m.seq}, NumDelivered: m.delivered}, nil
}

var _ jetstream.Msg = (*fakeMsg)(nil)

func indexMsg(t *testing.T, seq uint64, rec memory.Record) *fakeMsg {
	t.Helper()
	b, err := json.Marshal(rec)
	require.NoError(t, err)
	return &fakeMsg{data: b, subject: memory.SubjectIndex, seq: seq, delivered: 1}
}

func linkMsg(t *testing.T, lm memory.LinkMsg) *fakeMsg {
	t.Helper()
	b, err := json.Marshal(lm)
	require.NoError(t, err)
	return &fakeMsg{data: b, subject: memory.SubjectLink, seq: 1, delivered: 1}
}

// ---- stack ----------------------------------------------------------------

type stack struct {
	t        *testing.T
	nc       *nats.Conn
	js       jetstream.JetStream
	locker   *dlock.Locker
	fs       *fakeStore
	embedder *embed.Client
	chunker  *chunk.Chunker
	reg      *prometheus.Registry
	m        *metrics
	stores   tenantStores
	stream   *streamIdent
	ackWait  time.Duration
}

func newStack(t *testing.T, seed uint64, lockTTL time.Duration) *stack {
	t.Helper()
	nc, js := natstest.Start(t)
	ctx := context.Background()
	require.NoError(t, bus.EnsureStream(ctx, js))
	locker, err := dlock.New(ctx, js, lockBucket, lockTTL, "test")
	require.NoError(t, err)
	ch, err := chunk.New(chunk.DefaultMaxTokens, chunk.DefaultOverlap)
	require.NoError(t, err)
	fs := newFakeStore(seed)
	reg := prometheus.NewRegistry()
	return &stack{t: t, nc: nc, js: js, locker: locker, fs: fs, embedder: fakeEmbedder(t), chunker: ch,
		reg: reg, m: newMetrics(reg), stores: fs.tenant, stream: newStreamIdent(js), ackWait: 30 * time.Second}
}

// worker is one simulated replica.
type worker struct {
	handled atomic.Int32 // messages whose handler started on this replica
	health  *fetchHealth // index pool's fetch health
	stop    func()       // cancels its context and waits for in-flight handlers
}

// startWorker runs one simulated worker replica through the SAME wiring as main
// (workerDeps.startPools: both durable consumers shared with every other replica,
// the real handlers behind the pool), with s.ackWait.
func (s *stack) startWorker(concurrency int) *worker {
	s.t.Helper()
	w := &worker{}
	ctx, cancel := context.WithCancel(context.Background())
	d := &workerDeps{
		log: slog.New(slog.NewTextHandler(io.Discard, nil)), m: s.m, js: s.js, locker: s.locker, stream: s.stream,
		embedder: s.embedder, chunker: s.chunker, concurrency: concurrency, ackWait: s.ackWait,
		stores: func(u string) tenantStore { w.handled.Add(1); return s.stores(u) },
	}
	ip, lp, err := d.startPools(ctx)
	require.NoError(s.t, err)
	w.health = ip.health
	var once sync.Once
	w.stop = func() {
		once.Do(func() {
			cancel()
			ip.wait()
			lp.wait()
		})
	}
	s.t.Cleanup(w.stop)
	return w
}

// crashedHolder takes the lock on key as another worker that then "crashes": it
// never releases, so the lock only frees when its lease (ttl) expires.
func (s *stack) crashedHolder(key string, ttl time.Duration) {
	s.t.Helper()
	dead, err := dlock.New(context.Background(), s.js, lockBucket, ttl, "dead-worker")
	require.NoError(s.t, err)
	_, err = dead.Acquire(context.Background(), key, time.Second)
	require.NoError(s.t, err)
}

// waitIdle blocks until both durable consumers have nothing pending or in flight
// (stable across two polls), i.e. the queue has fully drained.
func (s *stack) waitIdle() {
	s.t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(90 * time.Second)
	stable := 0
	for time.Now().Before(deadline) {
		idle := true
		for _, name := range []string{memory.ConsumerName, memory.LinkConsumerName} {
			c, err := s.js.Consumer(ctx, memory.StreamName, name)
			if err != nil {
				idle = false
				continue
			}
			info, err := c.Info(ctx)
			require.NoError(s.t, err)
			if info.NumPending != 0 || info.NumAckPending != 0 {
				idle = false
			}
		}
		if idle {
			stable++
			if stable >= 3 {
				return
			}
		} else {
			stable = 0
		}
		time.Sleep(100 * time.Millisecond)
	}
	s.t.Fatal("queue did not drain")
}

// assertNoLocksHeld fails if any lock key is still live in the lock bucket.
func (s *stack) assertNoLocksHeld() {
	s.t.Helper()
	kv, err := s.js.KeyValue(context.Background(), lockBucket)
	require.NoError(s.t, err)
	keys, err := kv.Keys(context.Background())
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return
	}
	require.NoError(s.t, err)
	require.Empty(s.t, keys, "locks leaked")
}

func tenantCtx(tenant string) context.Context {
	return identity.Into(context.Background(), identity.Identity{UserID: tenant})
}

// fixedStream is a streamIdentity with a pinned value.
type fixedStream int64

func (f fixedStream) get(context.Context) int64 { return int64(f) }
func (fixedStream) refresh(context.Context)     {}
