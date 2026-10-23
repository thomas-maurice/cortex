package bus

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/natstest"
)

func TestParseAge(t *testing.T) {
	d, err := ParseAge("X", "", DefaultDeadMaxAge)
	require.NoError(t, err)
	assert.Equal(t, 30*24*time.Hour, d, "unset = default")
	d, err = ParseAge("X", "48h", DefaultDeadMaxAge)
	require.NoError(t, err)
	assert.Equal(t, 48*time.Hour, d)
	d, err = ParseAge("X", "0", DefaultDeadMaxAge)
	require.NoError(t, err)
	assert.Zero(t, d, "0 = keep")
	// Retention decides when data is deleted: a typo must be an error, never the default.
	for _, bad := range []string{"30d", "abc", "-1h", "7"} {
		_, err := ParseAge("X", bad, DefaultDeadMaxAge)
		assert.Error(t, err, "%q", bad)
	}
}

func TestOptionsFromEnv(t *testing.T) {
	t.Setenv("DEAD_LETTER_MAX_AGE", "")
	t.Setenv("QUEUE_MAX_AGE", "")
	opts, err := OptionsFromEnv()
	require.NoError(t, err)
	o := newOptions(opts)
	assert.Equal(t, DefaultQueueMaxAge, o.queueMaxAge)
	assert.Equal(t, DefaultDeadMaxAge, o.deadMaxAge)

	t.Setenv("DEAD_LETTER_MAX_AGE", "5d")
	_, err = OptionsFromEnv()
	require.Error(t, err)
	t.Setenv("DEAD_LETTER_MAX_AGE", "0")
	t.Setenv("QUEUE_MAX_AGE", "bogus")
	_, err = OptionsFromEnv()
	require.Error(t, err)
}

func msgCount(t *testing.T, js jetstream.JetStream, stream string) uint64 {
	t.Helper()
	s, err := js.Stream(context.Background(), stream)
	require.NoError(t, err)
	return s.CachedInfo().State.Msgs
}

// drain pulls and acks everything currently deliverable to a durable consumer.
func drain(t *testing.T, js jetstream.JetStream, durable string) int {
	t.Helper()
	cons, err := js.Consumer(context.Background(), memory.StreamName, durable)
	require.NoError(t, err)
	batch, err := cons.Fetch(100, jetstream.FetchMaxWait(500*time.Millisecond))
	require.NoError(t, err)
	n := 0
	for m := range batch.Messages() {
		require.NoError(t, m.DoubleAck(context.Background()))
		n++
	}
	return n
}

func publish(t *testing.T, js jetstream.JetStream, subject, body string) {
	t.Helper()
	_, err := js.Publish(context.Background(), subject, []byte(body))
	require.NoError(t, err)
}

func TestEnsureStreamConfig(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := context.Background()
	require.NoError(t, EnsureStream(ctx, js, WithQueueMaxAge(48*time.Hour), WithDeadMaxAge(72*time.Hour)))

	q, err := js.Stream(ctx, memory.StreamName)
	require.NoError(t, err)
	assert.Equal(t, jetstream.InterestPolicy, q.CachedInfo().Config.Retention)
	assert.Equal(t, 48*time.Hour, q.CachedInfo().Config.MaxAge)
	assert.Equal(t, jetstream.FileStorage, q.CachedInfo().Config.Storage)
	d, err := js.Stream(ctx, memory.DeadStreamName)
	require.NoError(t, err)
	assert.Equal(t, jetstream.LimitsPolicy, d.CachedInfo().Config.Retention)
	assert.Equal(t, 72*time.Hour, d.CachedInfo().Config.MaxAge)
	assert.NotZero(t, d.CachedInfo().Config.MaxMsgs, "dead letters need a size cap besides age")

	// Changing the env on restart applies to the existing streams.
	require.NoError(t, EnsureStream(ctx, js, WithQueueMaxAge(time.Hour), WithDeadMaxAge(0)))
	q, _ = js.Stream(ctx, memory.StreamName)
	d, _ = js.Stream(ctx, memory.DeadStreamName)
	assert.Equal(t, time.Hour, q.CachedInfo().Config.MaxAge)
	assert.Zero(t, d.CachedInfo().Config.MaxAge, "DEAD_LETTER_MAX_AGE=0 keeps dead letters")
}

// Why: with InterestPolicy a message published while no consumer matches it is
// discarded at once. EnsureStream must therefore create the durable consumers, or
// a server that starts before any worker would publish into the void.
func TestPublishBeforeAnyWorkerIsRetained(t *testing.T) {
	_, js := natstest.Start(t)
	require.NoError(t, EnsureStream(context.Background(), js))
	publish(t, js, memory.SubjectIndex, "i")
	publish(t, js, memory.SubjectLink, "l")
	assert.EqualValues(t, 2, msgCount(t, js, memory.StreamName))
}

// Why: bounded disk. Each message must disappear once the consumer(s) that match
// it acked it, and each durable consumer only holds interest in ITS subjects: a
// link message must not wait for the indexer (and vice versa).
func TestInterestRetentionFilteredConsumers(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := context.Background()
	require.NoError(t, EnsureStream(ctx, js))
	publish(t, js, memory.SubjectIndex, "i")
	publish(t, js, memory.SubjectSummary, "s")
	publish(t, js, memory.SubjectLink, "l")
	require.EqualValues(t, 3, msgCount(t, js, memory.StreamName))

	// Linker acks the link: it goes away although the indexer has not run at all.
	require.Equal(t, 1, drain(t, js, memory.LinkConsumerName))
	assert.EqualValues(t, 2, msgCount(t, js, memory.StreamName), "link msg must not wait for the indexer")

	// Indexer fetches both but acks neither: still retained.
	cons, err := js.Consumer(ctx, memory.StreamName, memory.ConsumerName)
	require.NoError(t, err)
	batch, err := cons.Fetch(10, jetstream.FetchMaxWait(500*time.Millisecond))
	require.NoError(t, err)
	var held []jetstream.Msg
	for m := range batch.Messages() {
		held = append(held, m)
	}
	require.Len(t, held, 2)
	assert.EqualValues(t, 2, msgCount(t, js, memory.StreamName), "an unacked message must stay")

	// An ephemeral reader (what tests/tools use) must not hold or release interest.
	n := 0
	require.NoError(t, readAll(ctx, js, memory.StreamName, memory.SubjectIndex, func(jetstream.Msg) error { n++; return nil }))
	assert.Equal(t, 1, n)
	assert.EqualValues(t, 2, msgCount(t, js, memory.StreamName))

	for _, m := range held {
		require.NoError(t, m.DoubleAck(ctx))
	}
	assert.EqualValues(t, 0, msgCount(t, js, memory.StreamName), "everything acked: stream empty")

	// A new link must not be blocked by the (idle) indexer either.
	publish(t, js, memory.SubjectLink, "l2")
	require.Equal(t, 1, drain(t, js, memory.LinkConsumerName))
	assert.EqualValues(t, 0, msgCount(t, js, memory.StreamName))
}

// Why: a recreated durable consumer must not replay history. On the interest
// stream acked messages are gone, and a deleted consumer's owed messages go with it.
func TestRecreatedConsumerDoesNotReplay(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := context.Background()
	require.NoError(t, EnsureStream(ctx, js))
	publish(t, js, memory.SubjectIndex, "done")
	require.Equal(t, 1, drain(t, js, memory.ConsumerName))
	publish(t, js, memory.SubjectLink, "owed") // linker has not processed it

	require.NoError(t, js.DeleteConsumer(ctx, memory.StreamName, memory.ConsumerName))
	require.NoError(t, js.DeleteConsumer(ctx, memory.StreamName, memory.LinkConsumerName))
	require.NoError(t, EnsureStream(ctx, js))
	for _, name := range []string{memory.ConsumerName, memory.LinkConsumerName} {
		c, err := js.Consumer(ctx, memory.StreamName, name)
		require.NoError(t, err)
		info, err := c.Info(ctx)
		require.NoError(t, err)
		assert.Zero(t, info.NumPending, "%s must not replay", name)
	}
}

func TestDeadLettersLiveInTheirOwnStream(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := context.Background()
	require.NoError(t, EnsureStream(ctx, js))
	for _, id := range []string{"a", "b"} {
		require.NoError(t, PublishDead(ctx, js, memory.DeadLetter{Record: memory.Record{ID: id, Text: "t-" + id}, Error: "boom", Deliveries: 10}))
	}
	assert.EqualValues(t, 0, msgCount(t, js, memory.StreamName), "dead letters must not sit in the work queue")
	n, err := DeadCount(ctx, js)
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)
	dls, err := FetchDead(ctx, js)
	require.NoError(t, err)
	require.Len(t, dls, 2)
	assert.Equal(t, "t-a", dls[0].Record.Text)
	assert.Equal(t, "boom", dls[0].Error)

	// Listing does not consume.
	dls, _ = FetchDead(ctx, js)
	assert.Len(t, dls, 2)

	require.NoError(t, PurgeDead(ctx, js))
	dls, err = FetchDead(ctx, js)
	require.NoError(t, err)
	assert.Empty(t, dls)
	n, _ = DeadCount(ctx, js)
	assert.Zero(t, n)
}

// ---- migration ----

// legacyFixture builds the pre-retention layout: MEMORY with LimitsPolicy over
// memory.> holding processed messages and two dead letters on memory.dead, plus
// the two durable consumers having acked everything.
func legacyFixture(t *testing.T, js jetstream.JetStream) {
	t.Helper()
	ctx := context.Background()
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: memory.StreamName, Subjects: []string{"memory.>"}, Storage: jetstream.FileStorage})
	require.NoError(t, err)
	for _, cfg := range ConsumerConfigs(DefaultAckWait) {
		_, err := js.CreateConsumer(ctx, memory.StreamName, cfg)
		require.NoError(t, err)
	}
	publish(t, js, memory.SubjectIndex, "processed-1")
	publish(t, js, memory.SubjectSummary, "processed-2")
	publish(t, js, memory.SubjectLink, "processed-3")
	require.Equal(t, 2, drain(t, js, memory.ConsumerName))
	require.Equal(t, 1, drain(t, js, memory.LinkConsumerName))
	for _, id := range []string{"a", "b"} {
		dl := `{"record":{"id":"` + id + `","text":"t-` + id + `"},"error":"boom","deliveries":10}`
		_, err := js.PublishMsg(ctx, (&natsMsg{subject: legacyDead, data: dl, hdr: map[string]string{"X-Keep": id}}).toNats())
		require.NoError(t, err)
	}
}

func retention(t *testing.T, js jetstream.JetStream) jetstream.RetentionPolicy {
	t.Helper()
	s, err := js.Stream(context.Background(), memory.StreamName)
	require.NoError(t, err)
	return s.CachedInfo().Config.Retention
}

func subjects(t *testing.T, js jetstream.JetStream) []string {
	t.Helper()
	s, err := js.Stream(context.Background(), memory.StreamName)
	require.NoError(t, err)
	return s.CachedInfo().Config.Subjects
}

func created(t *testing.T, js jetstream.JetStream) time.Time {
	t.Helper()
	s, err := js.Stream(context.Background(), memory.StreamName)
	require.NoError(t, err)
	return s.CachedInfo().Created
}

func TestMigrationOfProcessedLegacyStream(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := context.Background()
	legacyFixture(t, js)
	require.Equal(t, jetstream.LimitsPolicy, retention(t, js))
	before := created(t, js)

	require.NoError(t, EnsureStream(ctx, js))

	assert.Equal(t, jetstream.InterestPolicy, retention(t, js))
	assert.True(t, created(t, js).After(before), "stream was recreated")
	assert.EqualValues(t, 0, msgCount(t, js, memory.StreamName), "processed history is gone")
	dls, err := FetchDead(ctx, js)
	require.NoError(t, err)
	require.Len(t, dls, 2, "dead letters survive the migration")
	assert.Equal(t, "t-a", dls[0].Record.Text)
	assert.Equal(t, 10, dls[0].Deliveries)

	// Headers are preserved on the copy.
	var hdr string
	require.NoError(t, readAll(ctx, js, memory.DeadStreamName, memory.SubjectDead, func(m jetstream.Msg) error {
		if hdr == "" {
			hdr = m.Headers().Get("X-Keep")
		}
		return nil
	}))
	assert.Equal(t, "a", hdr)

	// The new stream works end to end and the recreated consumers do not replay.
	for _, name := range []string{memory.ConsumerName, memory.LinkConsumerName} {
		c, err := js.Consumer(ctx, memory.StreamName, name)
		require.NoError(t, err)
		info, _ := c.Info(ctx)
		assert.Zero(t, info.NumPending, name)
	}
	publish(t, js, memory.SubjectIndex, "new")
	require.Equal(t, 1, drain(t, js, memory.ConsumerName))
	assert.EqualValues(t, 0, msgCount(t, js, memory.StreamName))

	// Re-running is a no-op: same stream, no duplicated dead letters.
	again := created(t, js)
	require.NoError(t, EnsureStream(ctx, js))
	assert.Equal(t, again, created(t, js))
	n, _ := DeadCount(ctx, js)
	assert.EqualValues(t, 2, n)
}

// Why: the migration must never lose unprocessed work. While either consumer has
// a pending OR a fetched-but-unacked message, the legacy stream stays untouched.
func TestMigrationWaitsForUnprocessedMessages(t *testing.T) {
	for _, tc := range []struct {
		name, subject, consumer string
		fetchOnly               bool
	}{
		{"pending index", memory.SubjectIndex, memory.ConsumerName, false},
		{"pending link", memory.SubjectLink, memory.LinkConsumerName, false},
		{"delivered but unacked", memory.SubjectIndex, memory.ConsumerName, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, js := natstest.Start(t)
			ctx := context.Background()
			legacyFixture(t, js)
			publish(t, js, tc.subject, "unprocessed")
			var held []jetstream.Msg
			if tc.fetchOnly {
				c, _ := js.Consumer(ctx, memory.StreamName, tc.consumer)
				b, err := c.Fetch(1, jetstream.FetchMaxWait(time.Second))
				require.NoError(t, err)
				for m := range b.Messages() {
					held = append(held, m)
				}
				require.Len(t, held, 1)
			}
			before := created(t, js)

			err := EnsureStream(ctx, js, withFenceWait(100*time.Millisecond))
			require.ErrorIs(t, err, ErrMigrationPending)
			assert.Equal(t, jetstream.LimitsPolicy, retention(t, js), "stream untouched")
			assert.Equal(t, legacySubjects, subjects(t, js), "no fence left behind: the stream keeps accepting messages")
			publish(t, js, tc.subject, "still-accepted") // a deferred migration must not leave publishers failing
			assert.Equal(t, before, created(t, js))
			assert.EqualValues(t, 7, msgCount(t, js, memory.StreamName), "unprocessed messages (+3 processed +2 dead) still there")

			// The worker finishes the message; the next attempt migrates.
			if tc.fetchOnly {
				require.NoError(t, held[0].DoubleAck(ctx))
				require.Equal(t, 1, drain(t, js, tc.consumer))
			} else {
				require.Equal(t, 2, drain(t, js, tc.consumer))
			}
			require.NoError(t, EnsureStream(ctx, js))
			assert.Equal(t, jetstream.InterestPolicy, retention(t, js))
			dls, _ := FetchDead(ctx, js)
			assert.Len(t, dls, 2)
		})
	}
}

func TestMigrationMissingConsumer(t *testing.T) {
	ctx := context.Background()
	t.Run("history we cannot prove processed blocks", func(t *testing.T) {
		_, js := natstest.Start(t)
		legacyFixture(t, js)
		require.NoError(t, js.DeleteConsumer(ctx, memory.StreamName, memory.LinkConsumerName))
		require.ErrorIs(t, EnsureStream(ctx, js), ErrMigrationPending)
		assert.Equal(t, jetstream.LimitsPolicy, retention(t, js))
	})
	t.Run("nothing for its subjects migrates", func(t *testing.T) {
		_, js := natstest.Start(t)
		_, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: memory.StreamName, Subjects: []string{"memory.>"}, Storage: jetstream.FileStorage})
		require.NoError(t, err)
		require.NoError(t, EnsureStream(ctx, js))
		assert.Equal(t, jetstream.InterestPolicy, retention(t, js))
	})
}

// Why: if a previous migration copied the dead letters and crashed before the
// delete, the retry must not copy them again (even beyond JetStream's dedup window).
func TestCopyDeadIsIdempotent(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := context.Background()
	legacyFixture(t, js)
	require.NoError(t, ensureStreamConfig(ctx, js, deadConfig(newOptions(nil))))
	st, _ := js.Stream(ctx, memory.StreamName)
	n, err := copyDead(ctx, js, st.CachedInfo().Created.UnixNano())
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	n, err = copyDead(ctx, js, st.CachedInfo().Created.UnixNano())
	require.NoError(t, err)
	assert.Zero(t, n)
	c, _ := DeadCount(ctx, js)
	assert.EqualValues(t, 2, c)

	require.NoError(t, EnsureStream(ctx, js))
	c, _ = DeadCount(ctx, js)
	assert.EqualValues(t, 2, c, "migration after a partial copy adds nothing")
}

// Why: server and every worker replica call EnsureStream at startup; exactly one
// may migrate and nobody may fail, duplicate dead letters or lose them.
func TestConcurrentEnsureMigratesOnce(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := context.Background()
	legacyFixture(t, js)

	const callers = 6
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func() { defer wg.Done(); errs[i] = EnsureStream(ctx, js) }()
	}
	wg.Wait()
	ok := 0
	for _, err := range errs {
		if err == nil {
			ok++
		} else {
			assert.ErrorIs(t, err, ErrMigrationPending, "a loser may only defer, never fail")
		}
	}
	assert.GreaterOrEqual(t, ok, 1, "someone must have migrated")
	require.NoError(t, EnsureStream(ctx, js))
	assert.Equal(t, jetstream.InterestPolicy, retention(t, js))
	n, _ := DeadCount(ctx, js)
	assert.EqualValues(t, 2, n, "dead letters copied exactly once")
	for _, name := range []string{memory.ConsumerName, memory.LinkConsumerName} {
		_, err := js.Consumer(ctx, memory.StreamName, name)
		assert.NoError(t, err, name)
	}
}

func TestMigrationLockHeldDefers(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := context.Background()
	legacyFixture(t, js)
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: migrationBucket, TTL: migrationLease})
	require.NoError(t, err)
	_, err = kv.Create(ctx, migrationKey, []byte("other"))
	require.NoError(t, err)

	require.ErrorIs(t, EnsureStream(ctx, js), ErrMigrationPending)
	assert.Equal(t, jetstream.LimitsPolicy, retention(t, js))

	require.NoError(t, kv.Delete(ctx, migrationKey))
	require.NoError(t, EnsureStream(ctx, js))
	assert.Equal(t, jetstream.InterestPolicy, retention(t, js))
	_, err = kv.Get(ctx, migrationKey)
	assert.ErrorIs(t, err, jetstream.ErrKeyNotFound, "lock released after migrating")
}

// Why: startup must not block or fail on a backlog; the background retry
// migrates once the worker has drained it.
func TestEnsureAndMigrateRetriesInBackground(t *testing.T) {
	_, js := natstest.Start(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	legacyFixture(t, js)
	publish(t, js, memory.SubjectIndex, "backlog")

	require.NoError(t, EnsureAndMigrate(ctx, js, 50*time.Millisecond))
	assert.Equal(t, jetstream.LimitsPolicy, retention(t, js))
	require.Equal(t, 1, drain(t, js, memory.ConsumerName))
	require.Eventually(t, func() bool {
		st, err := js.Stream(ctx, memory.StreamName) // briefly absent mid-migration
		return err == nil && st.CachedInfo().Config.Retention == jetstream.InterestPolicy
	}, 10*time.Second, 50*time.Millisecond)
	n, _ := DeadCount(ctx, js)
	assert.EqualValues(t, 2, n)
}

// natsMsg is a tiny builder so the fixture reads clearly.
type natsMsg struct {
	subject, data string
	hdr           map[string]string
}

func (m *natsMsg) toNats() *nats.Msg {
	out := &nats.Msg{Subject: m.subject, Data: []byte(m.data), Header: nats.Header{}}
	for k, v := range m.hdr {
		out.Header.Set(k, v)
	}
	return out
}

// Why: pagination must not stop at a short batch: 250 dead letters must all be
// copied, deduplicated on re-run, and listed.
func TestDeadLetterPaginationBeyondOneBatch(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := context.Background()
	legacyFixture(t, js) // 2 dead letters
	for i := range 248 {
		dl := fmt.Sprintf(`{"record":{"id":"x%d","text":"t"},"error":"boom","deliveries":10}`, i)
		_, err := js.PublishMsg(ctx, (&natsMsg{subject: legacyDead, data: dl}).toNats())
		require.NoError(t, err)
	}
	require.NoError(t, ensureStreamConfig(ctx, js, deadConfig(newOptions(nil))))
	st, _ := js.Stream(ctx, memory.StreamName)
	created := st.CachedInfo().Created.UnixNano()
	n, err := copyDead(ctx, js, created)
	require.NoError(t, err)
	assert.Equal(t, 250, n)
	n, err = copyDead(ctx, js, created)
	require.NoError(t, err)
	assert.Zero(t, n, "idempotency set must cover more than one batch")
	dls, err := FetchDead(ctx, js)
	require.NoError(t, err)
	assert.Len(t, dls, 250)
}

// A migrator that crashed with the fence up leaves the legacy stream rejecting
// index/link publishes. The next start must lift it (and, once idle, migrate).
func TestStaleFenceIsLifted(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := context.Background()
	legacyFixture(t, js)
	publish(t, js, memory.SubjectIndex, "owed")
	st, _ := js.Stream(ctx, memory.StreamName)
	require.NoError(t, setSubjects(ctx, js, st.CachedInfo().Config, []string{legacyDead}))
	_, err := js.Publish(ctx, memory.SubjectIndex, []byte("x"))
	require.Error(t, err, "fenced")

	require.ErrorIs(t, EnsureStream(ctx, js, withFenceWait(50*time.Millisecond)), ErrMigrationPending)
	assert.Equal(t, legacySubjects, subjects(t, js))
	publish(t, js, memory.SubjectIndex, "accepted-again")
	require.Equal(t, 2, drain(t, js, memory.ConsumerName))
	require.NoError(t, EnsureStream(ctx, js))
	assert.Equal(t, jetstream.InterestPolicy, retention(t, js))
}

// worker simulates a worker: it pulls from the indexer consumer by NAME every
// round (the consumer object is replaced by the migration) and records payloads.
func worker(ctx context.Context, js jetstream.JetStream, processed *sync.Map, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		cons, err := js.Consumer(ctx, memory.StreamName, memory.ConsumerName)
		if err != nil {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		batch, err := cons.Fetch(200, jetstream.FetchMaxWait(20*time.Millisecond))
		if err != nil {
			continue
		}
		for m := range batch.Messages() {
			processed.Store(string(m.Data()), true)
			_ = m.Ack()
		}
	}
}

// Why (zero acked-but-lost): while a publisher hammers memory.index, a publish
// that the broker ACKS must end up processed. During the migration (fence, delete,
// recreate) and on a fresh install (stream before consumers) publishes may FAIL,
// but never be acked and then discarded.
func TestNoAckedPublishLostDuringMigration(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		name := "migration"
		if fresh {
			name = "fresh install"
		}
		t.Run(name, func(t *testing.T) {
			_, js := natstest.Start(t)
			ctx := context.Background()
			if !fresh {
				legacyFixture(t, js)
				// Many dead letters make the copy (inside the fence window) slow, so the
				// publishers really race the delete.
				for i := range 600 {
					_, err := js.PublishMsg(ctx, (&natsMsg{subject: legacyDead, data: fmt.Sprintf(`{"record":{"id":"x%d"}}`, i)}).toNats())
					require.NoError(t, err)
				}
			}
			var processed sync.Map
			stopWorker, stopPub := make(chan struct{}), make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			go func() { defer wg.Done(); worker(ctx, js, &processed, stopWorker) }()

			var mu sync.Mutex
			var acked []string
			var failed int
			var pubWG sync.WaitGroup
			for p := range 4 {
				pubWG.Add(1)
				go func() {
					defer pubWG.Done()
					for i := 0; ; i++ {
						select {
						case <-stopPub:
							return
						default:
						}
						body := fmt.Sprintf("m-%d-%d", p, i)
						pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
						_, err := js.Publish(pctx, memory.SubjectIndex, []byte(body))
						cancel()
						mu.Lock()
						if err != nil {
							failed++
						} else {
							acked = append(acked, body)
						}
						mu.Unlock()
						time.Sleep(300 * time.Microsecond)
					}
				}()
			}

			time.Sleep(20 * time.Millisecond)
			deadline := time.Now().Add(30 * time.Second)
			for {
				err := EnsureStream(ctx, js, withFenceWait(2*time.Second))
				if err == nil {
					break
				}
				require.ErrorIs(t, err, ErrMigrationPending)
				require.True(t, time.Now().Before(deadline), "migration never completed")
				time.Sleep(5 * time.Millisecond)
			}
			time.Sleep(100 * time.Millisecond) // keep publishing on the new stream
			close(stopPub)
			pubWG.Wait()

			require.Eventually(t, func() bool {
				for _, b := range acked {
					if _, ok := processed.Load(b); !ok {
						return false
					}
				}
				return true
			}, 15*time.Second, 20*time.Millisecond, "an acked publish was lost")
			close(stopWorker)
			wg.Wait()
			assert.Equal(t, jetstream.InterestPolicy, retention(t, js))
			t.Logf("acked=%d failed(loud)=%d", len(acked), failed)
		})
	}
}
