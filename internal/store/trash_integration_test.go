package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// trashStore returns a store on a freshly rebuilt schema, MT or not depending on
// CORTEX_TEST_MULTI_TENANT (same gating as the other integration tests).
func trashStore(t *testing.T) (*Store, bool) {
	t.Helper()
	rest := os.Getenv("CORTEX_TEST_WEAVIATE_REST")
	grpc := os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST and CORTEX_TEST_WEAVIATE_GRPC to run this integration test")
	}
	mt := os.Getenv("CORTEX_TEST_MULTI_TENANT") != ""
	ctx := context.Background()
	st, err := New(rest, grpc)
	require.NoError(t, err)
	st.SetMultiTenant(mt)
	_ = st.DeleteAllClasses(ctx)
	require.NoError(t, st.EnsureSchema(ctx))
	t.Cleanup(func() { _ = st.DeleteAllClasses(ctx) })
	return st, mt
}

func mkRec(ns, text string) memory.Record {
	return memory.Record{
		ID: uuid.NewString(), Text: text, Namespace: ns, Tags: []string{"t1", "t2"}, Source: "src",
		ConversationID: "conv-1", LinkedIDs: []string{uuid.NewString()}, NotDuplicateOf: []string{uuid.NewString()},
		CreatedAt: time.Now().UTC().Add(-time.Hour).Truncate(time.Second),
	}
}

var trashVec = []float32{1, 0, 0, 0}

// A delete must park the full record in Trash (so it is restorable with its
// metadata) while it disappears from Memory and from the chunk index (so it can
// never surface in search again).
func TestTrashDeleteKeepsRecordAndRemovesLive(t *testing.T) {
	st, _ := trashStore(t)
	ctx := context.Background()
	ts := st.Tenant("tenant-a")

	rec := mkRec("ns", "important fact")
	require.NoError(t, ts.Upsert(ctx, rec, trashVec))
	require.NoError(t, ts.ReplaceChunks(ctx, rec, []ChunkVec{{Index: 0, Text: "important fact", Vector: trashVec}}))

	require.NoError(t, ts.DeleteWithReason(ctx, rec.ID, memory.TrashReasonSupersedes+"abc"))

	_, live, err := ts.Get(ctx, rec.ID)
	require.NoError(t, err)
	assert.False(t, live, "deleted memory must be gone from Memory")
	hits, err := ts.Search(ctx, trashVec, SearchOpts{Namespace: "ns", Limit: 5})
	require.NoError(t, err)
	assert.Empty(t, hits, "chunks must be gone too: a trashed memory is not searchable")

	e, ok, err := ts.GetTrash(ctx, rec.ID)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, rec.Text, e.Text)
	assert.Equal(t, rec.Namespace, e.Namespace)
	assert.Equal(t, rec.Tags, e.Tags)
	assert.Equal(t, rec.Source, e.Source)
	assert.Equal(t, rec.ConversationID, e.ConversationID)
	assert.Equal(t, rec.LinkedIDs, e.LinkedIDs)
	assert.Equal(t, rec.NotDuplicateOf, e.NotDuplicateOf)
	assert.True(t, rec.CreatedAt.Equal(e.CreatedAt))
	assert.Equal(t, "supersedes:abc", e.Reason)
	assert.WithinDuration(t, time.Now(), e.DeletedAt, time.Minute)

	list, err := ts.ListTrash(ctx, "")
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, rec.ID, list[0].ID)
	assert.Equal(t, "supersedes:abc", list[0].Reason)
	assert.Equal(t, rec.LinkedIDs, list[0].LinkedIDs)

	// Plain Delete uses the "delete" reason.
	rec2 := mkRec("ns", "second")
	require.NoError(t, ts.Upsert(ctx, rec2, trashVec))
	require.NoError(t, ts.Delete(ctx, rec2.ID))
	e2, ok, err := ts.GetTrash(ctx, rec2.ID)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, memory.TrashReasonDelete, e2.Reason)
}

// If the trash write fails the delete must NOT proceed: otherwise a "safe"
// delete could still lose data, defeating the point of the trash.
func TestTrashWriteFailureDeletesNothing(t *testing.T) {
	st, _ := trashStore(t)
	ctx := context.Background()
	ts := st.Tenant("tenant-a")
	rec := mkRec("ns", "must survive")
	require.NoError(t, ts.Upsert(ctx, rec, trashVec))
	require.NoError(t, ts.ReplaceChunks(ctx, rec, []ChunkVec{{Index: 0, Text: "must survive", Vector: trashVec}}))

	// Break the trash: drop its class so the write fails.
	require.NoError(t, st.client.Schema().ClassDeleter().WithClassName(memory.TrashClassName).Do(ctx))

	err := ts.Delete(ctx, rec.ID)
	require.Error(t, err, "delete must fail loudly when it cannot trash")

	_, live, err := ts.Get(ctx, rec.ID)
	require.NoError(t, err)
	assert.True(t, live, "memory must still exist")
	hits, err := ts.Search(ctx, trashVec, SearchOpts{Namespace: "ns", Limit: 5})
	require.NoError(t, err)
	assert.Len(t, hits, 1, "chunks must still exist")
}

// Namespace delete trashes every memory in the namespace (and only that one).
func TestTrashNamespaceDelete(t *testing.T) {
	st, _ := trashStore(t)
	ctx := context.Background()
	ts := st.Tenant("tenant-a")
	a1, a2, b1 := mkRec("alpha", "a1"), mkRec("alpha", "a2"), mkRec("beta", "b1")
	for _, r := range []memory.Record{a1, a2, b1} {
		require.NoError(t, ts.Upsert(ctx, r, trashVec))
	}
	require.NoError(t, ts.UpsertSummary(ctx, memory.Summary{ConversationID: "c", Text: "s", Namespace: "alpha", CreatedAt: time.Now(), UpdatedAt: time.Now()}, trashVec))

	mem, sum, err := ts.DeleteNamespace(ctx, "alpha")
	require.NoError(t, err)
	assert.Equal(t, 2, mem)
	assert.Equal(t, 1, sum)

	list, err := ts.ListTrash(ctx, "alpha")
	require.NoError(t, err)
	require.Len(t, list, 2)
	for _, e := range list {
		assert.Equal(t, memory.TrashReasonNamespaceDelete, e.Reason)
	}
	all, err := ts.ListTrash(ctx, "")
	require.NoError(t, err)
	assert.Len(t, all, 2, "beta was not deleted, so is not in the trash")
	_, live, err := ts.Get(ctx, b1.ID)
	require.NoError(t, err)
	assert.True(t, live)
	for _, r := range []memory.Record{a1, a2} {
		_, live, err := ts.Get(ctx, r.ID)
		require.NoError(t, err)
		assert.False(t, live)
	}
}

// Purge removes entries for good; DeleteTrash of a missing entry is a no-op.
func TestTrashPurge(t *testing.T) {
	st, _ := trashStore(t)
	ctx := context.Background()
	ts := st.Tenant("tenant-a")
	r1, r2, r3 := mkRec("n1", "1"), mkRec("n1", "2"), mkRec("n2", "3")
	for _, r := range []memory.Record{r1, r2, r3} {
		require.NoError(t, ts.Upsert(ctx, r, trashVec))
		require.NoError(t, ts.Delete(ctx, r.ID))
	}
	require.NoError(t, ts.DeleteTrash(ctx, r1.ID))
	require.NoError(t, ts.DeleteTrash(ctx, r1.ID), "already gone: no-op")
	_, ok, err := ts.GetTrash(ctx, r1.ID)
	require.NoError(t, err)
	assert.False(t, ok)

	n, err := ts.PurgeTrash(ctx, "n1")
	require.NoError(t, err)
	assert.Equal(t, 1, n, "only r2 remains in n1")
	n, err = ts.PurgeTrash(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	list, err := ts.ListTrash(ctx, "")
	require.NoError(t, err)
	assert.Empty(t, list)
}

// The retention sweep must purge only entries older than the cutoff, in every
// tenant, and leave fresh entries restorable.
func TestTrashRetentionSweep(t *testing.T) {
	st, mt := trashStore(t)
	ctx := context.Background()
	tenants := []string{"tenant-a"}
	if mt {
		tenants = append(tenants, "tenant-b")
	}
	now := time.Now()
	old := map[string]string{}
	fresh := map[string]string{}
	for _, tn := range tenants {
		ts := st.Tenant(tn)
		o, f := mkRec("ns", "old"), mkRec("ns", "fresh")
		require.NoError(t, ts.putTrash(ctx, o, memory.TrashReasonDelete, now.Add(-10*24*time.Hour)))
		require.NoError(t, ts.putTrash(ctx, f, memory.TrashReasonDelete, now.Add(-time.Hour)))
		old[tn], fresh[tn] = o.ID, f.ID
	}
	n, err := st.PurgeExpiredTrash(ctx, now.Add(-7*24*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, len(tenants), n)
	for _, tn := range tenants {
		ts := st.Tenant(tn)
		_, ok, err := ts.GetTrash(ctx, old[tn])
		require.NoError(t, err)
		assert.False(t, ok, "old entry purged in %s", tn)
		_, ok, err = ts.GetTrash(ctx, fresh[tn])
		require.NoError(t, err)
		assert.True(t, ok, "fresh entry kept in %s", tn)
	}
}

// One tenant's trash must be invisible to another: the trash holds full memory
// text, so a leak would be a data breach.
func TestTrashTenantIsolation(t *testing.T) {
	st, mt := trashStore(t)
	if !mt {
		t.Skip("tenant isolation needs CORTEX_TEST_MULTI_TENANT=1")
	}
	ctx := context.Background()
	a, b := st.Tenant("tenant-a"), st.Tenant("tenant-b")
	rec := mkRec("ns", "alice only")
	require.NoError(t, a.Upsert(ctx, rec, trashVec))
	require.NoError(t, a.Delete(ctx, rec.ID))

	list, err := b.ListTrash(ctx, "")
	require.NoError(t, err)
	assert.Empty(t, list)
	_, ok, err := b.GetTrash(ctx, rec.ID)
	require.NoError(t, err)
	assert.False(t, ok)
	n, err := b.PurgeTrash(ctx, "")
	require.NoError(t, err)
	assert.Zero(t, n)
	_, ok, err = a.GetTrash(ctx, rec.ID)
	require.NoError(t, err)
	assert.True(t, ok, "tenant-a's entry untouched by tenant-b's purge")
}

// A reindex rebuild (DeleteClass + EnsureSchema) must not take the trash with it:
// trash entries have no vector, so a dimension change is irrelevant to them.
func TestDeleteClassKeepsTrash(t *testing.T) {
	st, _ := trashStore(t)
	ctx := context.Background()
	ts := st.Tenant("tenant-a")
	rec := mkRec("ns", "keep me")
	require.NoError(t, ts.Upsert(ctx, rec, trashVec))
	require.NoError(t, ts.Delete(ctx, rec.ID))

	require.NoError(t, st.DeleteClass(ctx))
	require.NoError(t, st.EnsureSchema(ctx))

	_, ok, err := ts.GetTrash(ctx, rec.ID)
	require.NoError(t, err)
	assert.True(t, ok)
}

// An entry whose deletedAt is missing has an unknown age: the retention sweep
// must leave it alone rather than purge it.
func TestTrashSweepSkipsUndatedEntries(t *testing.T) {
	st, _ := trashStore(t)
	ctx := context.Background()
	ts := st.Tenant("tenant-a")
	undated, old := mkRec("ns", "undated"), mkRec("ns", "old")
	require.NoError(t, ts.upsertObject(ctx, memory.TrashClassName, undated.ID, map[string]interface{}{
		"memoryId": undated.ID, "text": undated.Text, "namespace": "ns", "reason": "delete",
	}, nil))
	require.NoError(t, ts.putTrash(ctx, old, memory.TrashReasonDelete, time.Now().Add(-30*24*time.Hour)))

	n, err := st.PurgeExpiredTrash(ctx, time.Now().Add(-7*24*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, 1, n, "only the dated old entry is purged")
	_, ok, err := ts.GetTrash(ctx, undated.ID)
	require.NoError(t, err)
	assert.True(t, ok, "undated entry kept")
}

// A big namespace is trashed+deleted concurrently: every record must end up in
// the trash exactly once and be gone from Memory (also across scan pages).
func TestTrashNamespaceDeleteLarge(t *testing.T) {
	st, _ := trashStore(t)
	ctx := context.Background()
	old := scanPageSize
	scanPageSize = 7
	t.Cleanup(func() { scanPageSize = old })
	ts := st.Tenant("tenant-a")
	const n = 30
	for i := 0; i < n; i++ {
		require.NoError(t, ts.Upsert(ctx, mkRec("big", "m"), trashVec))
	}
	mem, _, err := ts.DeleteNamespace(ctx, "big")
	require.NoError(t, err)
	assert.Equal(t, n, mem)
	list, err := ts.ListTrash(ctx, "big")
	require.NoError(t, err)
	assert.Len(t, list, n)
	live, err := ts.Count(ctx, "big")
	require.NoError(t, err)
	assert.Zero(t, live)
}

// If trashing fails the namespace wipe stops with an error, and nothing that was
// not trashed is deleted (here: the trash is broken, so nothing at all is).
func TestTrashNamespaceDeleteFailureDeletesNothing(t *testing.T) {
	st, _ := trashStore(t)
	ctx := context.Background()
	ts := st.Tenant("tenant-a")
	for i := 0; i < 12; i++ {
		require.NoError(t, ts.Upsert(ctx, mkRec("ns", "m"), trashVec))
	}
	require.NoError(t, st.DeleteTrashClass(ctx))
	mem, _, err := ts.DeleteNamespace(ctx, "ns")
	require.Error(t, err)
	assert.Zero(t, mem)
	live, err := ts.Count(ctx, "ns")
	require.NoError(t, err)
	assert.Equal(t, 12, live, "no memory deleted without a trash copy")
}
