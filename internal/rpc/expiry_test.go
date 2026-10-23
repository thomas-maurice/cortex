package rpc

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/bus"
	"github.com/thomas-maurice/cortex/internal/identity"
	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/natstest"
	"github.com/thomas-maurice/cortex/internal/store"
)

// A bad expiry must be rejected before anything is published or stored, so a typo
// can never silently produce a memory that never expires (or expires instantly).
func TestResolveExpiry(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	ts := func(d time.Duration) *timestamppb.Timestamp { return timestamppb.New(now.Add(d)) }

	got, set, err := resolveExpiry(nil, "", now)
	require.NoError(t, err)
	assert.False(t, set, "no expiry requested")
	assert.True(t, got.IsZero())

	got, set, err = resolveExpiry(nil, "7d", now)
	require.NoError(t, err)
	assert.True(t, set)
	assert.Equal(t, now.Add(7*24*time.Hour), got)

	got, _, err = resolveExpiry(ts(time.Hour), "", now)
	require.NoError(t, err)
	assert.Equal(t, now.Add(time.Hour), got)

	for name, c := range map[string]struct {
		vu *timestamppb.Timestamp
		in string
	}{
		"both set":          {ts(time.Hour), "1d"},
		"past absolute":     {ts(-time.Hour), ""},
		"absolute == now":   {ts(0), ""},
		"absurd absolute":   {ts(11 * 365 * 24 * time.Hour), ""},
		"garbage relative":  {nil, "soon"},
		"negative relative": {nil, "-3d"},
		"zero relative":     {nil, "0d"},
		"absurd relative":   {nil, "4000d"},
		"unsupported weeks": {nil, "2w"},
		"invalid timestamp": {&timestamppb.Timestamp{Seconds: 1 << 60}, ""},
		"fractional days":   {nil, "1.5d"},
	} {
		_, _, err := resolveExpiry(c.vu, c.in, now)
		require.Error(t, err, name)
		assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err), name)
	}
}

func TestSaveAndUpdateRejectBadExpiryBeforeAnyStoreAccess(t *testing.T) {
	svc := &Service{cfg: Config{DefaultNamespace: "ns"}, store: &store.Store{}}
	ctx := context.Background()
	_, err := svc.Save(ctx, connect.NewRequest(&cortexv1.SaveRequest{Text: "x", ExpiresIn: "nope"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err))

	for name, req := range map[string]*cortexv1.SaveRequest{
		"never_expires plus expires_in":  {Text: "x", NeverExpires: true, ExpiresIn: "1d"},
		"never_expires plus valid_until": {Text: "x", NeverExpires: true, ValidUntil: timestamppb.New(time.Now().Add(time.Hour))},
	} {
		_, err := svc.Save(ctx, connect.NewRequest(req))
		require.Error(t, err, name)
		assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err), name)
	}

	id := uuid.NewString()
	for name, req := range map[string]*cortexv1.UpdateMemoryRequest{
		"bad expires_in":             {Id: id, Text: "x", ExpiresIn: "nope"},
		"clear plus set":             {Id: id, Text: "x", ExpiresIn: "1d", ClearValidUntil: true},
		"both absolute and relative": {Id: id, ValidUntil: timestamppb.New(time.Now().Add(time.Hour)), ExpiresIn: "1d"},
		"empty text, no expiry":      {Id: id},
		"empty text with tags":       {Id: id, ExpiresIn: "1d", ReplaceTags: true},
		"empty text with namespace":  {Id: id, ExpiresIn: "1d", Namespace: "other"},
	} {
		_, err := svc.UpdateMemory(ctx, connect.NewRequest(req))
		require.Error(t, err, name)
		assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err), name)
	}
}

// TestExpiryRPCIntegration needs a MULTI-TENANT Weaviate (see getmemory_test.go).
func TestExpiryRPCIntegration(t *testing.T) {
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
	svc := &Service{cfg: Config{MultiTenant: true, DefaultNamespace: "ns", Source: "t", TrashRetention: time.Hour, ExpiryGrace: 168 * time.Hour}, store: st, js: js, log: slog.Default()}
	alice := identity.Into(ctx, identity.Identity{UserID: "user-alice", Username: "alice", Role: identity.RoleUser})
	bob := identity.Into(ctx, identity.Identity{UserID: "user-bob", Username: "bob", Role: identity.RoleUser})
	ta := st.Tenant("user-alice")
	vec := []float32{0, 0, 1, 0}

	t.Run("save with expires_in rides the index payload", func(t *testing.T) {
		before := time.Now()
		resp, err := svc.Save(alice, connect.NewRequest(&cortexv1.SaveRequest{Text: "freeze until friday", ExpiresIn: "3d"}))
		require.NoError(t, err)
		var got *memory.Record
		for _, r := range publishedIndex(t, js) {
			if r.ID == resp.Msg.Id {
				got = &r
			}
		}
		require.NotNil(t, got)
		assert.WithinDuration(t, before.Add(72*time.Hour), got.ValidUntil, time.Minute)

		resp2, err := svc.Save(alice, connect.NewRequest(&cortexv1.SaveRequest{Text: "durable fact"}))
		require.NoError(t, err)
		for _, r := range publishedIndex(t, js) {
			if r.ID == resp2.Msg.Id {
				assert.True(t, r.ValidUntil.IsZero(), "no expiry requested -> never expires")
			}
		}
	})

	rec := memory.Record{ID: uuid.NewString(), Text: "original", Namespace: "ns", Tags: []string{"t"}, CreatedAt: time.Now().UTC().Add(-time.Hour)}
	require.NoError(t, ta.Upsert(ctx, rec, vec))
	require.NoError(t, ta.ReplaceChunks(ctx, rec, []store.ChunkVec{{Index: 0, Text: "original", Vector: vec}}))

	update := func(c context.Context, req *cortexv1.UpdateMemoryRequest) *cortexv1.UpdateMemoryResponse {
		req.Id = rec.ID
		r, err := svc.UpdateMemory(c, connect.NewRequest(req))
		require.NoError(t, err)
		return r.Msg
	}
	list := func(c context.Context, includeExpired bool) []*cortexv1.Memory {
		r, err := svc.List(c, connect.NewRequest(&cortexv1.ListRequest{Namespace: "ns", IncludeExpired: includeExpired}))
		require.NoError(t, err)
		return r.Msg.Memories
	}

	t.Run("expiry-only edit is applied directly and never re-embeds", func(t *testing.T) {
		published := len(publishedIndex(t, js))
		// Text omitted, and also the editor-style form: identical text + identical tags.
		r := update(alice, &cortexv1.UpdateMemoryRequest{ExpiresIn: "2d"})
		assert.Equal(t, "updated", r.Status)
		r = update(alice, &cortexv1.UpdateMemoryRequest{Text: "original", ReplaceTags: true, Tags: []string{"t"}, ExpiresIn: "3d"})
		assert.Equal(t, "updated", r.Status)
		assert.Len(t, publishedIndex(t, js), published, "no index message => the worker never re-embeds")

		got, _, err := ta.Get(ctx, rec.ID)
		require.NoError(t, err)
		assert.WithinDuration(t, time.Now().Add(72*time.Hour), got.ValidUntil, time.Minute)
		hits, err := ta.SearchMemoryVectors(ctx, vec, store.SearchOpts{Namespace: "ns", Limit: 3})
		require.NoError(t, err)
		require.NotEmpty(t, hits)
		assert.Less(t, hits[0].Distance, float32(0.01), "vector untouched")
	})

	t.Run("a real text edit republishes; the expiry is applied directly", func(t *testing.T) {
		r := update(alice, &cortexv1.UpdateMemoryRequest{Text: "reworded"})
		assert.Equal(t, "queued", r.Status)
		var last memory.Record
		for _, p := range publishedIndex(t, js) {
			if p.ID == rec.ID {
				last = p
			}
		}
		assert.Equal(t, "reworded", last.Text)
		assert.WithinDuration(t, time.Now().Add(72*time.Hour), last.ValidUntil, time.Minute, "an edit must not drop the expiry")

		r = update(alice, &cortexv1.UpdateMemoryRequest{Text: "reworded again", ClearValidUntil: true})
		assert.Equal(t, "queued", r.Status)
		got, _, err := ta.Get(ctx, rec.ID)
		require.NoError(t, err)
		assert.True(t, got.ValidUntil.IsZero(), "the expiry change is applied synchronously, not via the queue, even when the text is re-queued")
		r = update(alice, &cortexv1.UpdateMemoryRequest{Text: "reworded third", ExpiresIn: "5d"})
		assert.Equal(t, "queued", r.Status)
		got, _, _ = ta.Get(ctx, rec.ID)
		assert.WithinDuration(t, time.Now().Add(120*time.Hour), got.ValidUntil, time.Minute)
		update(alice, &cortexv1.UpdateMemoryRequest{ClearValidUntil: true})
	})

	t.Run("expired memories leave List unless include_expired; clearing restores them", func(t *testing.T) {
		// Expire it directly in the store (the RPC refuses a past time on purpose).
		require.NoError(t, ta.SetValidUntil(ctx, rec.ID, time.Now().Add(-time.Minute)))
		assert.Empty(t, list(alice, false), "expired memory must not be listed by default")
		got := list(alice, true)
		require.Len(t, got, 1)
		assert.NotNil(t, got[0].ValidUntil)

		r := update(alice, &cortexv1.UpdateMemoryRequest{ClearValidUntil: true})
		assert.Equal(t, "updated", r.Status)
		assert.Len(t, list(alice, false), 1, "clearing the expiry brings the memory back")
		assert.Nil(t, list(alice, false)[0].ValidUntil)
	})

	t.Run("another tenant cannot touch the expiry", func(t *testing.T) {
		_, err := svc.UpdateMemory(bob, connect.NewRequest(&cortexv1.UpdateMemoryRequest{Id: rec.ID, ExpiresIn: "1d"}))
		require.Error(t, err)
		assert.Equal(t, connect.CodeNotFound, connectCode(t, err))
	})

	t.Run("sweep moves past-grace memories only, per tenant", func(t *testing.T) {
		longGone := memory.Record{ID: uuid.NewString(), Text: "long gone", Namespace: "ns", CreatedAt: time.Now().UTC(), ValidUntil: time.Now().Add(-10 * 24 * time.Hour).UTC()}
		recent := memory.Record{ID: uuid.NewString(), Text: "just expired", Namespace: "ns", CreatedAt: time.Now().UTC(), ValidUntil: time.Now().Add(-time.Hour).UTC()}
		bobs := memory.Record{ID: uuid.NewString(), Text: "bob long gone", Namespace: "ns", CreatedAt: time.Now().UTC(), ValidUntil: time.Now().Add(-10 * 24 * time.Hour).UTC()}
		for _, r := range []memory.Record{longGone, recent} {
			require.NoError(t, ta.Upsert(ctx, r, vec))
		}
		require.NoError(t, st.Tenant("user-bob").Upsert(ctx, bobs, vec))

		n, err := svc.SweepExpired(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, n)

		trash, err := svc.ListTrash(alice, connect.NewRequest(&cortexv1.ListTrashRequest{}))
		require.NoError(t, err)
		require.Len(t, trash.Msg.Entries, 1)
		assert.Equal(t, longGone.ID, trash.Msg.Entries[0].Id)
		assert.Equal(t, "expired", trash.Msg.Entries[0].Reason)
		btrash, err := svc.ListTrash(bob, connect.NewRequest(&cortexv1.ListTrashRequest{}))
		require.NoError(t, err)
		require.Len(t, btrash.Msg.Entries, 1)
		assert.Equal(t, bobs.ID, btrash.Msg.Entries[0].Id)

		t.Run("restoring an expired-by-sweep memory clears the lapsed expiry", func(t *testing.T) {
			_, err := svc.RestoreFromTrash(alice, connect.NewRequest(&cortexv1.RestoreFromTrashRequest{Ids: []string{longGone.ID}}))
			require.NoError(t, err)
			var last memory.Record
			for _, p := range publishedIndex(t, js) {
				if p.ID == longGone.ID {
					last = p
				}
			}
			assert.Equal(t, "long gone", last.Text)
			assert.True(t, last.ValidUntil.IsZero(), "else it would be hidden again and re-swept at once")
		})
	})

	t.Run("restore policy: only 'expired' trash entries lose a lapsed expiry", func(t *testing.T) {
		lapsed := time.Now().Add(-48 * time.Hour).UTC().Truncate(time.Second)
		rec := memory.Record{ID: uuid.NewString(), Text: "explicitly deleted, expiry lapsed", Namespace: "ns", CreatedAt: time.Now().UTC(), ValidUntil: lapsed}
		require.NoError(t, ta.Upsert(ctx, rec, vec))
		_, err := svc.Delete(alice, connect.NewRequest(&cortexv1.DeleteRequest{Id: rec.ID}))
		require.NoError(t, err)
		tr, err := svc.ListTrash(alice, connect.NewRequest(&cortexv1.ListTrashRequest{}))
		require.NoError(t, err)
		var shown *cortexv1.TrashEntry
		for _, e := range tr.Msg.Entries {
			if e.Id == rec.ID {
				shown = e
			}
		}
		require.NotNil(t, shown)
		assert.NotNil(t, shown.ValidUntil, "the trash entry exposes its expiry")
		rr, err := svc.RestoreFromTrash(alice, connect.NewRequest(&cortexv1.RestoreFromTrashRequest{Ids: []string{rec.ID}}))
		require.NoError(t, err)
		assert.EqualValues(t, 1, rr.Msg.ExpiredQueued, "the user is told it comes back already expired (hidden)")
		var last memory.Record
		for _, p := range publishedIndex(t, js) {
			if p.ID == rec.ID {
				last = p
			}
		}
		assert.True(t, lapsed.Equal(last.ValidUntil), "an explicit delete keeps validUntil; the sweep re-trashes it as expired")
	})

	t.Run("RestoreMemories keeps past expiries and reports them", func(t *testing.T) {
		past := timestamppb.New(time.Now().Add(-time.Hour))
		before := len(publishedIndex(t, js))
		r, err := svc.RestoreMemories(alice, connect.NewRequest(&cortexv1.RestoreMemoriesRequest{Memories: []*cortexv1.Memory{
			{Id: uuid.NewString(), Text: "already expired", ValidUntil: past},
			{Id: uuid.NewString(), Text: "fine"},
		}}))
		require.NoError(t, err, "a restore must not reject a past valid_until")
		assert.EqualValues(t, 2, r.Msg.Queued)
		assert.EqualValues(t, 1, r.Msg.ExpiredQueued)
		pub := publishedIndex(t, js)
		assert.Len(t, pub, before+2)
		found := false
		for _, p := range pub {
			if p.Text == "already expired" {
				found = !p.ValidUntil.IsZero()
			}
		}
		assert.True(t, found, "the lapsed expiry is carried as-is")
	})

	t.Run("Status counts live memories and reports expired separately", func(t *testing.T) {
		require.NoError(t, ta.Upsert(ctx, memory.Record{ID: uuid.NewString(), Text: "stale", Namespace: "stat", CreatedAt: time.Now().UTC(), ValidUntil: time.Now().Add(-time.Hour).UTC()}, vec))
		live, expired, err := ta.CountLiveExpired(ctx)
		require.NoError(t, err)
		assert.Positive(t, expired)
		all, err := ta.Count(ctx, "")
		require.NoError(t, err)
		assert.Equal(t, all, live+expired)
		ns, err := svc.ListNamespaces(alice, connect.NewRequest(&cortexv1.ListNamespacesRequest{}))
		require.NoError(t, err)
		for _, n := range ns.Msg.Namespaces {
			if n.Name == "stat" {
				assert.EqualValues(t, 0, n.MemoryCount)
				assert.EqualValues(t, 1, n.ExpiredCount)
			}
		}
	})
}
