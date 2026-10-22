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
