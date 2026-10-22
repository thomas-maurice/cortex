package rpc

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/bus"
	"github.com/thomas-maurice/cortex/internal/identity"
	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/natstest"
	"github.com/thomas-maurice/cortex/internal/store"
)

// Bad ids must be rejected before any store access (same contract as GetMemory).
func TestTrashRejectsBadIDs(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: false}, store: &store.Store{}}
	ctx := context.Background()
	for _, ids := range [][]string{nil, {"  "}, {"not-a-uuid"}} {
		_, err := svc.RestoreFromTrash(ctx, connect.NewRequest(&cortexv1.RestoreFromTrashRequest{Ids: ids}))
		require.Error(t, err, "restore %v", ids)
		assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err))
		_, err = svc.PurgeTrash(ctx, connect.NewRequest(&cortexv1.PurgeTrashRequest{Ids: ids}))
		require.Error(t, err, "purge %v", ids)
		assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err))
	}
}

func TestSweepTrashDisabledWhenRetentionZero(t *testing.T) {
	svc := &Service{cfg: Config{TrashRetention: 0}}
	n, err := svc.SweepTrash(context.Background())
	require.NoError(t, err)
	assert.Zero(t, n, "retention 0 keeps the trash forever and must not even touch the store")
}

// publishedLinks reads every link message on the stream.
func publishedLinks(t *testing.T, js jetstream.JetStream) []memory.LinkMsg {
	t.Helper()
	cons, err := js.OrderedConsumer(context.Background(), memory.StreamName, jetstream.OrderedConsumerConfig{FilterSubjects: []string{memory.SubjectLink}})
	require.NoError(t, err)
	batch, err := cons.Fetch(200, jetstream.FetchMaxWait(time.Second))
	require.NoError(t, err)
	var out []memory.LinkMsg
	for m := range batch.Messages() {
		var lm memory.LinkMsg
		require.NoError(t, json.Unmarshal(m.Data(), &lm))
		out = append(out, lm)
	}
	return out
}

// publishedIndex reads every index message on the stream.
func publishedIndex(t *testing.T, js jetstream.JetStream) []memory.Record {
	t.Helper()
	ctx := context.Background()
	cons, err := js.OrderedConsumer(ctx, memory.StreamName, jetstream.OrderedConsumerConfig{FilterSubjects: []string{memory.SubjectIndex}})
	require.NoError(t, err)
	batch, err := cons.Fetch(50, jetstream.FetchMaxWait(time.Second))
	require.NoError(t, err)
	var out []memory.Record
	for m := range batch.Messages() {
		var r memory.Record
		require.NoError(t, json.Unmarshal(m.Data(), &r))
		out = append(out, r)
	}
	return out
}

// TestTrashRPCIntegration needs a MULTI-TENANT Weaviate (see getmemory_test.go).
// It pins the user-visible contract: delete -> listed -> restore re-queues the
// record under the same id and empties the trash; a live id is never overwritten;
// and tenants cannot see each other's trash (identical NotFound).
func TestTrashRPCIntegration(t *testing.T) {
	rest := os.Getenv("CORTEX_TEST_WEAVIATE_REST")
	grpc := os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" || os.Getenv("CORTEX_TEST_MULTI_TENANT") == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST, CORTEX_TEST_WEAVIATE_GRPC and CORTEX_TEST_MULTI_TENANT=1 to run this integration test")
	}
	ctx := context.Background()
	st, err := store.New(rest, grpc)
	require.NoError(t, err)
	st.SetMultiTenant(true)
	_ = st.DeleteAllClasses(ctx)
	require.NoError(t, st.EnsureSchema(ctx))
	t.Cleanup(func() { _ = st.DeleteAllClasses(ctx) })

	_, js := natstest.Start(t)
	require.NoError(t, bus.EnsureStream(ctx, js))
	svc := &Service{cfg: Config{MultiTenant: true, TrashRetention: 168 * time.Hour}, store: st, js: js, log: slog.Default()}
	alice := identity.Into(ctx, identity.Identity{UserID: "user-alice", Username: "alice", Role: identity.RoleUser})
	bob := identity.Into(ctx, identity.Identity{UserID: "user-bob", Username: "bob", Role: identity.RoleUser})
	vec := []float32{1, 0, 0, 0}
	ta := st.Tenant("user-alice")

	liveNeighbour := memory.Record{ID: uuid.NewString(), Text: "neighbour", Namespace: "ns", CreatedAt: time.Now().UTC()}
	goneNeighbour := uuid.NewString()
	rec := memory.Record{
		ID: uuid.NewString(), Text: "deleted by mistake", Namespace: "ns", Tags: []string{"x"}, Source: "s",
		CreatedAt: time.Now().UTC().Add(-time.Hour), LinkedIDs: []string{liveNeighbour.ID, goneNeighbour},
	}
	require.NoError(t, ta.Upsert(ctx, liveNeighbour, vec))
	require.NoError(t, ta.Upsert(ctx, rec, vec))

	del := func(c context.Context, id string) error {
		_, err := svc.Delete(c, connect.NewRequest(&cortexv1.DeleteRequest{Id: id}))
		return err
	}
	list := func(c context.Context) *cortexv1.ListTrashResponse {
		r, err := svc.ListTrash(c, connect.NewRequest(&cortexv1.ListTrashRequest{}))
		require.NoError(t, err)
		return r.Msg
	}
	restore := func(c context.Context, ids ...string) error {
		_, err := svc.RestoreFromTrash(c, connect.NewRequest(&cortexv1.RestoreFromTrashRequest{Ids: ids}))
		return err
	}

	require.NoError(t, del(alice, rec.ID))
	l := list(alice)
	require.Len(t, l.Entries, 1)
	assert.Equal(t, rec.ID, l.Entries[0].Id)
	assert.Equal(t, "delete", l.Entries[0].Reason)
	assert.EqualValues(t, 168*3600, l.RetentionSeconds)
	assert.Empty(t, list(bob).Entries, "bob must not see alice's trash")

	t.Run("another tenant gets NotFound, same as for an unknown id", func(t *testing.T) {
		errOther := restore(bob, rec.ID)
		errUnknown := restore(alice, uuid.NewString())
		require.Error(t, errOther)
		require.Error(t, errUnknown)
		assert.Equal(t, connect.CodeNotFound, connectCode(t, errOther))
		assert.Equal(t, connectCode(t, errUnknown), connectCode(t, errOther))
		_, err := svc.PurgeTrash(bob, connect.NewRequest(&cortexv1.PurgeTrashRequest{Ids: []string{rec.ID}}))
		require.Error(t, err)
		assert.Equal(t, connect.CodeNotFound, connectCode(t, err))
		n, err := svc.PurgeTrash(bob, connect.NewRequest(&cortexv1.PurgeTrashRequest{All: true}))
		require.NoError(t, err)
		assert.Zero(t, n.Msg.Purged)
		assert.Len(t, list(alice).Entries, 1, "alice's entry untouched")
	})

	t.Run("restore re-queues under the same id and empties the trash", func(t *testing.T) {
		require.NoError(t, restore(alice, rec.ID))
		assert.Empty(t, list(alice).Entries)

		var got *memory.Record
		for _, r := range publishedIndex(t, js) {
			if r.ID == rec.ID {
				got = &r
			}
		}
		require.NotNil(t, got, "restore must publish an index message")
		assert.Equal(t, "user-alice", got.UserID, "published for alice's tenant")
		assert.Equal(t, rec.Text, got.Text)
		assert.Equal(t, rec.Namespace, got.Namespace)
		assert.Equal(t, rec.Tags, got.Tags)
		assert.True(t, rec.CreatedAt.Truncate(time.Second).Equal(got.CreatedAt.Truncate(time.Second)))
		assert.Empty(t, got.LinkedIDs, "links go through the link queue, not the index payload")

		// Only the endpoint that still exists is re-linked.
		cons, err := js.OrderedConsumer(ctx, memory.StreamName, jetstream.OrderedConsumerConfig{FilterSubjects: []string{memory.SubjectLink}})
		require.NoError(t, err)
		batch, err := cons.Fetch(10, jetstream.FetchMaxWait(time.Second))
		require.NoError(t, err)
		var links []memory.LinkMsg
		for m := range batch.Messages() {
			var lm memory.LinkMsg
			require.NoError(t, json.Unmarshal(m.Data(), &lm))
			links = append(links, lm)
		}
		require.Len(t, links, 1)
		assert.ElementsMatch(t, []string{rec.ID, liveNeighbour.ID}, []string{links[0].A, links[0].B})
	})

	t.Run("restoring an id that already exists is rejected and overwrites nothing", func(t *testing.T) {
		live := memory.Record{ID: uuid.NewString(), Text: "live version", Namespace: "ns", CreatedAt: time.Now().UTC()}
		require.NoError(t, ta.Upsert(ctx, live, vec))
		require.NoError(t, del(alice, live.ID))
		// Same id re-created (e.g. the worker re-indexed it) while the trash copy lingers.
		live.Text = "newer live version"
		require.NoError(t, ta.Upsert(ctx, live, vec))

		err := restore(alice, live.ID)
		require.Error(t, err)
		assert.Equal(t, connect.CodeAlreadyExists, connectCode(t, err))
		got, ok, err := ta.Get(ctx, live.ID)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, "newer live version", got.Text)
		assert.Len(t, list(alice).Entries, 1, "trash entry kept")

		_, err = svc.PurgeTrash(alice, connect.NewRequest(&cortexv1.PurgeTrashRequest{Ids: []string{live.ID}}))
		require.NoError(t, err)
		assert.Empty(t, list(alice).Entries)
	})

	t.Run("a failing id in a batch restores nothing", func(t *testing.T) {
		a := memory.Record{ID: uuid.NewString(), Text: "a", Namespace: "ns", CreatedAt: time.Now().UTC()}
		require.NoError(t, ta.Upsert(ctx, a, vec))
		require.NoError(t, del(alice, a.ID))
		before := len(publishedIndex(t, js))
		require.Error(t, restore(alice, a.ID, uuid.NewString()))
		assert.Equal(t, before, len(publishedIndex(t, js)), "nothing published")
		assert.Len(t, list(alice).Entries, 1)
	})

	t.Run("restoring A and B together restores the A-B edge", func(t *testing.T) {
		a := memory.Record{ID: uuid.NewString(), Text: "a", Namespace: "ns", CreatedAt: time.Now().UTC()}
		b := memory.Record{ID: uuid.NewString(), Text: "b", Namespace: "ns", CreatedAt: time.Now().UTC()}
		a.LinkedIDs, b.LinkedIDs = []string{b.ID}, []string{a.ID}
		for _, r := range []memory.Record{a, b} {
			require.NoError(t, ta.Upsert(ctx, r, vec))
			require.NoError(t, del(alice, r.ID))
		}
		require.NoError(t, restore(alice, a.ID, b.ID))

		cons, err := js.OrderedConsumer(ctx, memory.StreamName, jetstream.OrderedConsumerConfig{FilterSubjects: []string{memory.SubjectLink}})
		require.NoError(t, err)
		batch, err := cons.Fetch(20, jetstream.FetchMaxWait(time.Second))
		require.NoError(t, err)
		found := false
		for m := range batch.Messages() {
			var lm memory.LinkMsg
			require.NoError(t, json.Unmarshal(m.Data(), &lm))
			if lm.Op == memory.LinkOpAdd && ((lm.A == a.ID && lm.B == b.ID) || (lm.A == b.ID && lm.B == a.ID)) {
				found = true
			}
		}
		assert.True(t, found, "A-B link must be queued although neither is live yet")
	})

	pair := func(msgs []memory.LinkMsg, x, y string) (add, remove bool) {
		for _, lm := range msgs {
			if (lm.A == x && lm.B == y) || (lm.A == y && lm.B == x) {
				add = add || lm.Op == memory.LinkOpAdd
				remove = remove || lm.Op == memory.LinkOpRemove
			}
		}
		return
	}

	t.Run("restoring A then B in separate requests restores the edge", func(t *testing.T) {
		a := memory.Record{ID: uuid.NewString(), Text: "a2", Namespace: "ns", CreatedAt: time.Now().UTC()}
		b := memory.Record{ID: uuid.NewString(), Text: "b2", Namespace: "ns", CreatedAt: time.Now().UTC()}
		a.LinkedIDs, b.LinkedIDs = []string{b.ID}, []string{a.ID}
		for _, r := range []memory.Record{a, b} {
			require.NoError(t, ta.Upsert(ctx, r, vec))
			require.NoError(t, del(alice, r.ID))
		}
		require.NoError(t, restore(alice, a.ID))
		addNow, _ := pair(publishedLinks(t, js), a.ID, b.ID)
		assert.False(t, addNow, "B is still in the trash: no link yet (the link queue must not be left waiting for days)")

		// The worker would index A; simulate that so A is live when B is restored.
		require.NoError(t, ta.Upsert(ctx, memory.Record{ID: a.ID, Text: "a2", Namespace: "ns", CreatedAt: a.CreatedAt}, vec))
		require.NoError(t, restore(alice, b.ID))
		addLater, _ := pair(publishedLinks(t, js), a.ID, b.ID)
		assert.True(t, addLater, "restoring B re-links it to the live A")
	})

	t.Run("delete + restore keeps recall history", func(t *testing.T) {
		accessed := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)
		r := memory.Record{ID: uuid.NewString(), Text: "often recalled", Namespace: "ns", CreatedAt: time.Now().UTC(), AccessCount: 7, LastAccessedAt: accessed, Model: "m", Dims: 4}
		require.NoError(t, ta.Upsert(ctx, r, vec))
		require.NoError(t, del(alice, r.ID))
		e, ok, err := ta.GetTrash(ctx, r.ID)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, 7, e.AccessCount)
		assert.True(t, accessed.Equal(e.LastAccessedAt))
		assert.Equal(t, "m", e.Model)
		require.NoError(t, restore(alice, r.ID))
		var got *memory.Record
		for _, p := range publishedIndex(t, js) {
			if p.ID == r.ID {
				got = &p
			}
		}
		require.NotNil(t, got)
		assert.Equal(t, 7, got.AccessCount, "a restore must not reset usage history")
		assert.True(t, accessed.Equal(got.LastAccessedAt))
	})

	t.Run("delete reason is recorded and filterable", func(t *testing.T) {
		x := memory.Record{ID: uuid.NewString(), Text: "stale one", Namespace: "reasons", CreatedAt: time.Now().UTC()}
		y := memory.Record{ID: uuid.NewString(), Text: "plain one", Namespace: "reasons", CreatedAt: time.Now().UTC()}
		for _, r := range []memory.Record{x, y} {
			require.NoError(t, ta.Upsert(ctx, r, vec))
		}
		_, err := svc.Delete(alice, connect.NewRequest(&cortexv1.DeleteRequest{Id: x.ID, Reason: "stale-review"}))
		require.NoError(t, err)
		require.NoError(t, del(alice, y.ID))
		r, err := svc.ListTrash(alice, connect.NewRequest(&cortexv1.ListTrashRequest{Namespace: "reasons", Reason: "stale-review"}))
		require.NoError(t, err)
		require.Len(t, r.Msg.Entries, 1)
		assert.Equal(t, x.ID, r.Msg.Entries[0].Id)
		_, err = svc.Delete(alice, connect.NewRequest(&cortexv1.DeleteRequest{Id: uuid.NewString(), Reason: "because"}))
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err))

		t.Run("bulk restore by filter reports counts", func(t *testing.T) {
			// A third entry whose id is live again must be counted as failed, not abort the batch.
			z := memory.Record{ID: uuid.NewString(), Text: "revived", Namespace: "reasons", CreatedAt: time.Now().UTC()}
			require.NoError(t, ta.Upsert(ctx, z, vec))
			require.NoError(t, del(alice, z.ID))
			require.NoError(t, ta.Upsert(ctx, z, vec))
			res, err := svc.RestoreFromTrash(alice, connect.NewRequest(&cortexv1.RestoreFromTrashRequest{All: true, Namespace: "reasons"}))
			require.NoError(t, err)
			assert.EqualValues(t, 2, res.Msg.Queued)
			assert.EqualValues(t, 1, res.Msg.Failed)
			require.Len(t, res.Msg.Failures, 1)
			assert.Contains(t, res.Msg.Failures[0], z.ID)
			lr, err := svc.ListTrash(alice, connect.NewRequest(&cortexv1.ListTrashRequest{Namespace: "reasons"}))
			require.NoError(t, err)
			require.Len(t, lr.Msg.Entries, 1)
			assert.Equal(t, z.ID, lr.Msg.Entries[0].Id, "only the failed entry stays in the trash")
			_, err = svc.PurgeTrash(alice, connect.NewRequest(&cortexv1.PurgeTrashRequest{Ids: []string{z.ID}}))
			require.NoError(t, err)
		})
	})

	t.Run("purge and sweep strip dangling links from neighbours", func(t *testing.T) {
		n := memory.Record{ID: uuid.NewString(), Text: "neighbour", Namespace: "ns", CreatedAt: time.Now().UTC()}
		p := memory.Record{ID: uuid.NewString(), Text: "to purge", Namespace: "ns", CreatedAt: time.Now().UTC(), LinkedIDs: []string{n.ID}}
		n.LinkedIDs = []string{p.ID}
		require.NoError(t, ta.Upsert(ctx, n, vec))
		require.NoError(t, ta.Upsert(ctx, p, vec))
		require.NoError(t, del(alice, p.ID))
		_, err := svc.PurgeTrash(alice, connect.NewRequest(&cortexv1.PurgeTrashRequest{Ids: []string{p.ID}}))
		require.NoError(t, err)
		_, removed := pair(publishedLinks(t, js), n.ID, p.ID)
		assert.True(t, removed, "purge queues a link REMOVE for the live neighbour")

		// Sweep: an entry past retention is purged and its links stripped, in its own tenant.
		s2 := memory.Record{ID: uuid.NewString(), Text: "to sweep", Namespace: "ns", CreatedAt: time.Now().UTC(), LinkedIDs: []string{n.ID}}
		require.NoError(t, ta.PutTrashEntry(ctx, memory.TrashEntry{Record: s2, DeletedAt: time.Now().Add(-30 * 24 * time.Hour), Reason: "delete"}))
		cnt, err := svc.SweepTrash(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, cnt)
		_, removed = pair(publishedLinks(t, js), n.ID, s2.ID)
		assert.True(t, removed)
	})

	t.Run("purge all", func(t *testing.T) {
		r, err := svc.PurgeTrash(alice, connect.NewRequest(&cortexv1.PurgeTrashRequest{All: true}))
		require.NoError(t, err)
		assert.EqualValues(t, 1, r.Msg.Purged)
		assert.Empty(t, list(alice).Entries)
		_, err = svc.PurgeTrash(alice, connect.NewRequest(&cortexv1.PurgeTrashRequest{All: true, Ids: []string{uuid.NewString()}}))
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err))
	})
}

// The migration backup holds memories only, so the trash must be on disk, complete,
// BEFORE the classes are dropped (MigrateMT calls this between snapshot and drop).
func TestWriteTrashSidecar(t *testing.T) {
	dir := t.TempDir()
	backup := filepath.Join(dir, "cortex-backup-20260101-000000.json")
	entries := []memory.TrashEntry{
		{Record: memory.Record{ID: uuid.NewString(), Text: "one", Namespace: "a"}, DeletedAt: time.Now().UTC().Truncate(time.Second), Reason: "delete"},
		{Record: memory.Record{ID: uuid.NewString(), Text: "two", Namespace: "b"}, DeletedAt: time.Now().UTC().Truncate(time.Second), Reason: "supersedes:x"},
	}
	path, err := writeTrashSidecar(backup, entries)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(dir, "cortex-backup-20260101-000000.trash.json"), path)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var got []memory.TrashEntry
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, entries, got)

	none, err := writeTrashSidecar(backup, nil)
	require.NoError(t, err)
	assert.Empty(t, none, "no trash, no file")
}

// The reindex/migrate snapshot holds every memory's plaintext: owner-only.
func TestWriteBackupIsOwnerOnly(t *testing.T) {
	svc := &Service{cfg: Config{BackupDir: t.TempDir()}, log: slog.Default()}
	path, err := svc.writeBackup([]memory.Record{{ID: uuid.NewString(), Text: "secret"}})
	require.NoError(t, err)
	fi, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
}
