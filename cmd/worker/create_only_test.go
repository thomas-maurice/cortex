package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// ---- create-only guard (O5) -----------------------------------------------

func createOnlyMsg(t *testing.T, version int64, rec memory.Record) *fakeMsg {
	t.Helper()
	rec.CreateOnly = true
	return indexMsg(t, version, rec)
}

func TestCreateOnlyGuard(t *testing.T) {
	newRec := func(id, text string) memory.Record {
		return memory.Record{ID: id, Text: text, Namespace: "ns", CreatedAt: time.Now()}
	}

	// WHY: an offline outbox replays Saves later; a replay must never bring back a
	// memory the user deleted (or that a merge/expiry trashed) in the meantime.
	t.Run("replay of a trashed id does not resurrect it and restore still works", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		id := uuid.NewString()
		runIndex(t, s, createOnlyMsg(t, 10, newRec(id, "original")))
		require.NoError(t, s.fs.tenant("").DeleteWithReason(context.Background(), id, "delete"))

		replay := createOnlyMsg(t, 20, newRec(id, "original"))
		runIndex(t, s, replay)

		_, live := s.fs.peek("", id)
		assert.False(t, live, "replay resurrected a trashed memory")
		_, inTrash, err := s.fs.tenant("").GetTrash(context.Background(), id)
		require.NoError(t, err)
		assert.True(t, inTrash, "trash entry must stay intact")
		assert.EqualValues(t, 1, replay.acked.Load(), "skip is an ack, not a retry")
		assert.Zero(t, replay.nakked.Load()+replay.termed.Load())
		assert.EqualValues(t, 1, outcome(s.m, kindIndex, outcomeSkipped))
		assert.Zero(t, outcome(s.m, kindIndex, outcomeStale), "distinct from stale")

		// RestoreFromTrash republishes the trash copy, which never carries CreateOnly.
		e, _, _ := s.fs.tenant("").GetTrash(context.Background(), id)
		assert.False(t, e.CreateOnly, "CreateOnly must not be persisted into the trash copy")
		runIndex(t, s, indexMsg(t, 30, e.Record))
		got, live := s.fs.peek("", id)
		require.True(t, live, "restore must still work after a skipped replay")
		assert.Equal(t, "original", got.Text)
	})

	// WHY: a replayed Save must not clobber edits made since the first copy landed.
	t.Run("replay of a live id written by another message does not overwrite", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		id := uuid.NewString()
		runIndex(t, s, createOnlyMsg(t, 10, newRec(id, "first")))
		runIndex(t, s, indexMsg(t, 15, newRec(id, "edited"))) // UpdateMemory since

		replay := createOnlyMsg(t, 20, newRec(id, "first"))
		runIndex(t, s, replay)

		got, _ := s.fs.peek("", id)
		assert.Equal(t, "edited", got.Text)
		assert.EqualValues(t, 15, got.IndexVersion)
		assert.EqualValues(t, 1, replay.acked.Load())
		assert.EqualValues(t, 1, outcome(s.m, kindIndex, outcomeSkipped))
		assert.Equal(t, []string{"edited"}, s.fs.chunks["|"+id], "chunks must not be replaced")
	})

	t.Run("legacy live record (version 0) is never the same message", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		id := uuid.NewString()
		seed(t, s, "", memory.Record{ID: id, Text: "legacy"})
		runIndex(t, s, createOnlyMsg(t, 5, newRec(id, "new")))
		got, _ := s.fs.peek("", id)
		assert.Equal(t, "legacy", got.Text)
		assert.EqualValues(t, 1, outcome(s.m, kindIndex, outcomeSkipped))
	})

	t.Run("a fresh id is created", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		id := uuid.NewString()
		runIndex(t, s, createOnlyMsg(t, 10, newRec(id, "new")))
		got, ok := s.fs.peek("", id)
		require.True(t, ok)
		assert.Equal(t, "new", got.Text)
		assert.False(t, got.CreateOnly, "CreateOnly is NATS-only and must never be persisted")
		assert.Zero(t, outcome(s.m, kindIndex, outcomeSkipped))
	})

	// WHY (load-bearing): a worker that crashed after the upsert but before trashing
	// the sources gets the SAME message redelivered; skipping it as "already present"
	// would leave the merged-away sources live forever.
	t.Run("redelivery of the same message completes an interrupted supersedes", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		id, src := uuid.NewString(), uuid.NewString()
		seed(t, s, "", memory.Record{ID: src, Text: "source"})
		// State after the crash: merged memory upserted at version 10, source still live.
		seed(t, s, "", memory.Record{ID: id, Text: "merged", IndexVersion: 10})

		rec := newRec(id, "merged")
		rec.Supersedes = []string{src}
		redelivered := createOnlyMsg(t, 10, rec)
		runIndex(t, s, redelivered)

		_, live := s.fs.peek("", src)
		assert.False(t, live, "redelivery must finish trashing the source")
		assert.Contains(t, s.fs.deleteReasons, src+" supersedes:"+id)
		assert.Zero(t, outcome(s.m, kindIndex, outcomeSkipped))
		assert.EqualValues(t, 1, redelivered.acked.Load())
		got, _ := s.fs.peek("", id)
		assert.False(t, got.CreateOnly)
	})

	// WHY: restore/edit publishers never set CreateOnly and must behave as before.
	t.Run("non-create-only messages ignore the guard", func(t *testing.T) {
		s := newStack(t, 1, time.Minute)
		live, trashed := uuid.NewString(), uuid.NewString()
		runIndex(t, s, indexMsg(t, 10, newRec(live, "v1")))
		runIndex(t, s, indexMsg(t, 10, newRec(trashed, "t")))
		require.NoError(t, s.fs.tenant("").DeleteWithReason(context.Background(), trashed, "delete"))

		runIndex(t, s, indexMsg(t, 20, newRec(live, "v2")))
		runIndex(t, s, indexMsg(t, 20, newRec(trashed, "t2")))

		got, _ := s.fs.peek("", live)
		assert.Equal(t, "v2", got.Text)
		got, ok := s.fs.peek("", trashed)
		require.True(t, ok, "a plain index message (restore) re-creates a trashed id")
		assert.Equal(t, "t2", got.Text)
		assert.Zero(t, outcome(s.m, kindIndex, outcomeSkipped))
	})
}

// WHY: if the trash lookup fails we cannot tell "trashed" from "new"; skipping
// would silently drop a legitimate offline save, so the message must be retried.
func TestCreateOnlyTrashLookupErrorRetries(t *testing.T) {
	s := newStack(t, 1, time.Minute)
	s.fs.trashErr = errors.New("weaviate unavailable")
	id := uuid.NewString()
	msg := createOnlyMsg(t, 10, memory.Record{ID: id, Text: "queued offline", Namespace: "ns", CreatedAt: time.Now()})
	runIndex(t, s, msg)

	_, live := s.fs.peek("", id)
	assert.False(t, live)
	assert.EqualValues(t, 1, msg.nakked.Load(), "a lookup error must NAK for retry")
	assert.Zero(t, msg.acked.Load()+msg.termed.Load())
	assert.Zero(t, outcome(s.m, kindIndex, outcomeSkipped))
}
