package rpc

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/bus"
	"github.com/thomas-maurice/cortex/internal/dlock"
	"github.com/thomas-maurice/cortex/internal/identity"
	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/merge"
	"github.com/thomas-maurice/cortex/internal/natstest"
	"github.com/thomas-maurice/cortex/internal/store"
)

func TestMergeIntoRejectsBadArgs(t *testing.T) {
	svc := &Service{log: slog.Default()}
	id := uuid.NewString()
	for name, req := range map[string]*cortexv1.MergeIntoRequest{
		"empty keep":          {RemoveIds: []string{id}},
		"bad keep":            {KeepId: "nope", RemoveIds: []string{id}},
		"bad remove":          {KeepId: id, RemoveIds: []string{"nope"}},
		"nothing to remove":   {KeepId: id},
		"only removes itself": {KeepId: id, RemoveIds: []string{id}},
	} {
		_, err := svc.MergeInto(context.Background(), connect.NewRequest(req))
		require.Error(t, err, name)
		assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err), name)
	}
}

func linkMsgs(t *testing.T, js jetstream.JetStream) []memory.LinkMsg {
	t.Helper()
	cons, err := js.OrderedConsumer(context.Background(), memory.StreamName, jetstream.OrderedConsumerConfig{FilterSubjects: []string{memory.SubjectLink}})
	require.NoError(t, err)
	batch, err := cons.Fetch(50, jetstream.FetchMaxWait(time.Second))
	require.NoError(t, err)
	var out []memory.LinkMsg
	for m := range batch.Messages() {
		var lm memory.LinkMsg
		require.NoError(t, json.Unmarshal(m.Data(), &lm))
		out = append(out, lm)
	}
	return out
}

// Needs a MULTI-TENANT Weaviate (see getmemory_test.go). "Keep left/right" must
// not lose what the discarded side knew, and the duplicate list must not be hidden
// by candidates that no longer exist.
func TestMergeIntoAndCandidatesIntegration(t *testing.T) {
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
	svc := &Service{cfg: Config{MultiTenant: true, TrashRetention: time.Hour}, store: st, js: js, log: slog.Default()}
	alice := identity.Into(ctx, identity.Identity{UserID: "user-alice", Username: "alice", Role: identity.RoleUser})
	ta := st.Tenant("user-alice")
	vec := []float32{1, 0, 0, 0}

	now := time.Now().UTC().Truncate(time.Second)
	old := now.Add(-72 * time.Hour)
	last := now.Add(-time.Hour)
	keepID, goneID, nID, cID := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	for _, r := range []memory.Record{
		{ID: keepID, Text: "keep", Namespace: "ns", CreatedAt: now, AccessCount: 2, DupCandidates: []string{goneID}},
		{ID: goneID, Text: "loser", Namespace: "ns", CreatedAt: old, ConversationID: "conv-1", AccessCount: 5, LastAccessedAt: last,
			LinkedIDs: []string{nID}, NotDuplicateOf: []string{cID}},
		{ID: nID, Text: "neighbour", Namespace: "ns", CreatedAt: now, LinkedIDs: []string{goneID}},
		{ID: cID, Text: "dismissed counterpart", Namespace: "ns", CreatedAt: now, NotDuplicateOf: []string{goneID}},
	} {
		require.NoError(t, ta.Upsert(ctx, r, vec))
	}

	_, err = svc.MergeInto(alice, connect.NewRequest(&cortexv1.MergeIntoRequest{KeepId: keepID, RemoveIds: []string{goneID, goneID, keepID}}))
	require.NoError(t, err)

	keep, ok, err := ta.Get(ctx, keepID)
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, keep.CreatedAt.Equal(old), "createdAt = oldest")
	assert.Equal(t, "conv-1", keep.ConversationID)
	assert.Equal(t, 7, keep.AccessCount)
	assert.True(t, keep.LastAccessedAt.Equal(last))
	assert.Equal(t, []string{cID}, keep.NotDuplicateOf)
	assert.Empty(t, keep.DupCandidates, "the removed id is no longer a candidate")
	c, _, _ := ta.Get(ctx, cID)
	assert.Equal(t, []string{keepID}, c.NotDuplicateOf, "dismissal on the counterpart now names the survivor")

	_, live, _ := ta.Get(ctx, goneID)
	assert.False(t, live)
	e, ok, err := ta.GetTrash(ctx, goneID)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "merged-into:"+keepID, e.Reason)

	type edge struct {
		op   memory.LinkOp
		a, b string
	}
	var got []edge
	for _, lm := range linkMsgs(t, js) {
		assert.Equal(t, "user-alice", lm.UserID)
		got = append(got, edge{lm.Op, lm.A, lm.B})
	}
	ord := func(a, b string) (string, string) {
		if a > b {
			return b, a
		}
		return a, b
	}
	ka, kb := ord(keepID, nID)
	ga, gb := ord(goneID, nID)
	assert.ElementsMatch(t, []edge{{memory.LinkOpAdd, ka, kb}, {memory.LinkOpRemove, ga, gb}}, got, "links carried via the link queue")

	t.Run("retry after a partial merge succeeds without double counting", func(t *testing.T) {
		// A call that folded in `done` but failed before `todo` leaves done in the
		// trash with reason merged-into:<keep>. Retrying with BOTH ids must succeed.
		kID, doneID, todoID := uuid.NewString(), uuid.NewString(), uuid.NewString()
		for _, r := range []memory.Record{
			{ID: kID, Text: "k2", Namespace: "p", CreatedAt: now, AccessCount: 1},
			{ID: doneID, Text: "done", Namespace: "p", CreatedAt: now, AccessCount: 10},
			{ID: todoID, Text: "todo", Namespace: "p", CreatedAt: now, AccessCount: 100},
		} {
			require.NoError(t, ta.Upsert(ctx, r, vec))
		}
		require.NoError(t, ta.DeleteWithReason(ctx, doneID, memory.TrashReasonMergedInto+kID))
		r, err := svc.MergeInto(alice, connect.NewRequest(&cortexv1.MergeIntoRequest{KeepId: kID, RemoveIds: []string{doneID, todoID}}))
		require.NoError(t, err)
		assert.EqualValues(t, 2, r.Msg.Merged)
		k, _, _ := ta.Get(ctx, kID)
		assert.Equal(t, 101, k.AccessCount, "the already-merged id is not summed again")
		r, err = svc.MergeInto(alice, connect.NewRequest(&cortexv1.MergeIntoRequest{KeepId: kID, RemoveIds: []string{doneID, todoID}}))
		require.NoError(t, err, "a full replay is a no-op")
		assert.EqualValues(t, 2, r.Msg.Merged)
		k, _, _ = ta.Get(ctx, kID)
		assert.Equal(t, 101, k.AccessCount)
	})

	t.Run("holds the per-memory locks of keep, removed and counterparts; blocked by a held lock", func(t *testing.T) {
		kID, rID, cpID := uuid.NewString(), uuid.NewString(), uuid.NewString()
		for _, r := range []memory.Record{
			{ID: kID, Text: "k3", Namespace: "q", CreatedAt: now},
			{ID: rID, Text: "r3", Namespace: "q", CreatedAt: now, NotDuplicateOf: []string{cpID}},
			{ID: cpID, Text: "cp3", Namespace: "q", CreatedAt: now, NotDuplicateOf: []string{rID}},
		} {
			require.NoError(t, ta.Upsert(ctx, r, vec))
		}
		// Another replica (the worker) holds keep's lock: the merge must not run.
		l, err := dlock.New(ctx, js, dlock.DefaultBucket, dlock.DefaultTTL, "other-replica")
		require.NoError(t, err)
		rel, err := l.Acquire(ctx, dlock.Key("mem", "user-alice", kID), time.Second)
		require.NoError(t, err)
		short, cancel := context.WithTimeout(alice, 700*time.Millisecond)
		defer cancel()
		_, err = svc.MergeInto(short, connect.NewRequest(&cortexv1.MergeIntoRequest{KeepId: kID, RemoveIds: []string{rID}}))
		require.Error(t, err, "must not proceed without keep's lock")
		_, live, _ := ta.Get(ctx, rID)
		assert.True(t, live, "nothing trashed while locked out")
		require.NoError(t, rel())

		var locked []string
		svc.memLocks = func(c context.Context, tenant string, ids []string) (context.Context, func(), error) {
			locked = ids
			return c, func() {}, nil
		}
		defer func() { svc.memLocks = nil }()
		_, err = svc.MergeInto(alice, connect.NewRequest(&cortexv1.MergeIntoRequest{KeepId: kID, RemoveIds: []string{rID}}))
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{kID, rID, cpID}, locked, "the counterpart whose notDuplicateOf is rewritten is locked too")
	})

	t.Run("unknown id changes nothing", func(t *testing.T) {
		_, err := svc.MergeInto(alice, connect.NewRequest(&cortexv1.MergeIntoRequest{KeepId: keepID, RemoveIds: []string{nID, uuid.NewString()}}))
		require.Error(t, err)
		assert.Equal(t, connect.CodeNotFound, connectCode(t, err))
		_, live, _ := ta.Get(ctx, nID)
		assert.True(t, live, "validated up front, nothing trashed")
	})

	t.Run("dead candidates are dropped before the cap", func(t *testing.T) {
		// 3 newest flagged memories point only at ids that no longer exist; the two
		// real pairs are older. With limit 2 the stale ones must not eat the cap.
		ghost := uuid.NewString()
		for i := range 3 {
			require.NoError(t, ta.Upsert(ctx, memory.Record{ID: uuid.NewString(), Text: "stale", Namespace: "d", CreatedAt: now.Add(time.Duration(i+1) * time.Hour), DupCandidates: []string{ghost}}, vec))
		}
		for i := range 3 {
			a, b := uuid.NewString(), uuid.NewString()
			require.NoError(t, ta.Upsert(ctx, memory.Record{ID: a, Text: "a", Namespace: "d", CreatedAt: now.Add(-time.Duration(i+1) * time.Minute), DupCandidates: []string{b}}, vec))
			require.NoError(t, ta.Upsert(ctx, memory.Record{ID: b, Text: "b", Namespace: "d", CreatedAt: old}, vec))
		}
		r, err := svc.ListDuplicateCandidates(alice, connect.NewRequest(&cortexv1.ListDuplicateCandidatesRequest{Namespace: "d", Limit: 2}))
		require.NoError(t, err)
		require.Len(t, r.Msg.Groups, 2, "stale groups must not consume the limit")
		assert.True(t, r.Msg.Truncated, "a third live group exists")
		r, err = svc.ListDuplicateCandidates(alice, connect.NewRequest(&cortexv1.ListDuplicateCandidatesRequest{Namespace: "d", Limit: 10}))
		require.NoError(t, err)
		assert.Len(t, r.Msg.Groups, 3)
		assert.False(t, r.Msg.Truncated)
	})
}

// A merge that keeps losing the race against concurrent dismissal changes must
// surface as Aborted (retryable), not a raw internal error; a transient clash
// that clears is retried transparently.
func TestMergeIntoLockSetChangedMapsToAborted(t *testing.T) {
	calls := 0
	_, err := retryLockSetChanged(context.Background(), func() (int, error) {
		calls++
		return 0, merge.ErrLockSetChanged
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeAborted, connectCode(t, err))
	assert.Greater(t, calls, 1, "retried before giving up")

	calls = 0
	n, err := retryLockSetChanged(context.Background(), func() (int, error) {
		calls++
		if calls < 3 {
			return 0, merge.ErrLockSetChanged
		}
		return 2, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, n)
}

// Unbounded id lists would let one tenant pin worker slots with a single Save.
func TestSupersedesAndMergeIntoAreCapped(t *testing.T) {
	var ids []string
	for range maxSupersedes + 1 {
		ids = append(ids, uuid.NewString())
	}
	svc, msgs := edgeTestService(t)
	_, err := svc.Save(context.Background(), connect.NewRequest(&cortexv1.SaveRequest{Text: "x", Supersedes: ids}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err))
	assert.Zero(t, msgs(), "nothing queued")

	_, err = svc.MergeInto(context.Background(), connect.NewRequest(&cortexv1.MergeIntoRequest{KeepId: uuid.NewString(), RemoveIds: ids}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err))
}
