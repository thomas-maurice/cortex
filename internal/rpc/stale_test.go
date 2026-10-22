package rpc

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/identity"
	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/store"
)

// Request validation runs before any store access.
func TestDeleteManyAndStaleValidation(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: false}, store: &store.Store{}}
	ctx := context.Background()

	tooMany := make([]string, maxDeleteManyIDs+1)
	for i := range tooMany {
		tooMany[i] = uuid.NewString()
	}
	for name, ids := range map[string][]string{"empty": nil, "blank": {" "}, "bad uuid": {"nope"}, "over cap": tooMany} {
		_, err := svc.DeleteMany(ctx, connect.NewRequest(&cortexv1.DeleteManyRequest{Ids: ids}))
		require.Error(t, err, name)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), name)
	}

	// not-recalled-since without a threshold would silently mean "everything".
	_, err := svc.List(ctx, connect.NewRequest(&cortexv1.ListRequest{Stale: cortexv1.StaleMode_STALE_MODE_NOT_RECALLED_SINCE}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

// TestStaleAndDeleteManyIntegration needs a MULTI-TENANT Weaviate (see
// trash_test.go). It pins: stale listing via the RPC (default 30d guard, total,
// pagination), and DeleteMany semantics: everything lands in the trash, partial
// failures are per-id, another tenant's ids are "not found" and untouched.
func TestStaleAndDeleteManyIntegration(t *testing.T) {
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

	svc := &Service{cfg: Config{MultiTenant: true, TrashRetention: time.Hour}, store: st, log: slog.Default()}
	alice := identity.Into(ctx, identity.Identity{UserID: "user-alice", Username: "alice", Role: identity.RoleUser})
	bob := identity.Into(ctx, identity.Identity{UserID: "user-bob", Username: "bob", Role: identity.RoleUser})
	ta, tb := st.Tenant("user-alice"), st.Tenant("user-bob")
	vec := []float32{1, 0, 0, 0}
	now := time.Now().UTC()
	put := func(ts *store.TenantStore, age time.Duration, count int, last time.Duration) string {
		r := memory.Record{ID: uuid.NewString(), Text: "x", Namespace: "ns", CreatedAt: now.Add(-age), AccessCount: count}
		if last > 0 {
			r.LastAccessedAt = now.Add(-last)
		}
		require.NoError(t, ts.Upsert(ctx, r, vec))
		return r.ID
	}
	d := 24 * time.Hour
	never := put(ta, 100*d, 0, 0)
	tooNew := put(ta, 5*d, 0, 0)
	longAgo := put(ta, 200*d, 3, 120*d)
	recent := put(ta, 200*d, 3, 2*d)
	bobsOld := put(tb, 300*d, 0, 0)

	list := func(c context.Context, req *cortexv1.ListRequest) *cortexv1.ListResponse {
		req.Namespace = "*"
		r, err := svc.List(c, connect.NewRequest(req))
		require.NoError(t, err)
		return r.Msg
	}
	idsOf := func(r *cortexv1.ListResponse) []string {
		var out []string
		for _, m := range r.Memories {
			out = append(out, m.Id)
		}
		return out
	}

	t.Run("stale listing", func(t *testing.T) {
		r := list(alice, &cortexv1.ListRequest{Stale: cortexv1.StaleMode_STALE_MODE_NEVER_RECALLED})
		assert.Equal(t, []string{never}, idsOf(r), "default 30d min-age excludes the 5-day-old one; recalled ones are not 'never'")
		assert.EqualValues(t, 1, r.Total)

		zero := int32(0)
		r = list(alice, &cortexv1.ListRequest{Stale: cortexv1.StaleMode_STALE_MODE_NEVER_RECALLED, MinAgeDays: &zero})
		assert.ElementsMatch(t, []string{never, tooNew}, idsOf(r), "explicit min_age_days=0 disables the guard")

		r = list(alice, &cortexv1.ListRequest{Stale: cortexv1.StaleMode_STALE_MODE_NOT_RECALLED_SINCE, StaleDays: 90, Limit: 1})
		assert.EqualValues(t, 2, r.Total, "total is pre-pagination")
		assert.Equal(t, []string{longAgo}, idsOf(r), "oldest reference first: longAgo last recalled 120d ago, never-recalled was created 100d ago")
		r = list(alice, &cortexv1.ListRequest{Stale: cortexv1.StaleMode_STALE_MODE_NOT_RECALLED_SINCE, StaleDays: 90, Limit: 1, Offset: 1})
		assert.Equal(t, []string{never}, idsOf(r))
		assert.NotContains(t, idsOf(list(alice, &cortexv1.ListRequest{Stale: cortexv1.StaleMode_STALE_MODE_NOT_RECALLED_SINCE, StaleDays: 90})), recent)

		assert.Equal(t, []string{bobsOld}, idsOf(list(bob, &cortexv1.ListRequest{Stale: cortexv1.StaleMode_STALE_MODE_NEVER_RECALLED})), "tenant isolation")
	})

	t.Run("DeleteMany", func(t *testing.T) {
		missing := uuid.NewString()
		resp, err := svc.DeleteMany(alice, connect.NewRequest(&cortexv1.DeleteManyRequest{
			Ids: []string{never, " " + tooNew + " ", missing, bobsOld, never},
		}))
		require.NoError(t, err)
		res := map[string]*cortexv1.DeleteManyResult{}
		for _, r := range resp.Msg.Results {
			res[r.Id] = r
		}
		require.Len(t, resp.Msg.Results, 4, "duplicate input id collapsed")
		assert.EqualValues(t, 2, resp.Msg.Deleted)
		assert.True(t, res[never].Deleted)
		assert.True(t, res[tooNew].Deleted)
		assert.False(t, res[missing].Deleted)
		assert.Equal(t, "not found", res[missing].Error)
		assert.Equal(t, res[missing].Error, res[bobsOld].Error, "another tenant's id is indistinguishable from an unknown one")

		for _, id := range []string{never, tooNew} {
			_, live, err := ta.Get(ctx, id)
			require.NoError(t, err)
			assert.False(t, live)
			_, inTrash, err := ta.GetTrash(ctx, id)
			require.NoError(t, err)
			assert.True(t, inTrash, "deleted via the normal path => restorable")
		}
		_, live, err := tb.Get(ctx, bobsOld)
		require.NoError(t, err)
		assert.True(t, live, "bob's memory must be untouched")
		_, inTrash, err := tb.GetTrash(ctx, bobsOld)
		require.NoError(t, err)
		assert.False(t, inTrash)
		_, live, err = ta.Get(ctx, recent)
		require.NoError(t, err)
		assert.True(t, live)
	})
}

// Backend errors can carry URLs/hosts; DeleteMany must report only a generic
// "failed" per id (detail goes to the server log).
func TestDeleteManyDoesNotLeakBackendErrors(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom at http://secret-backend.internal:9999", http.StatusInternalServerError)
	}))
	t.Cleanup(backend.Close)
	st, err := store.New(strings.TrimPrefix(backend.URL, "http://"), "127.0.0.1:1")
	require.NoError(t, err)
	svc := &Service{cfg: Config{MultiTenant: false}, store: st, log: slog.Default()}
	id := uuid.NewString()
	resp, err := svc.DeleteMany(context.Background(), connect.NewRequest(&cortexv1.DeleteManyRequest{Ids: []string{id}}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Results, 1)
	assert.False(t, resp.Msg.Results[0].Deleted)
	assert.Equal(t, "failed", resp.Msg.Results[0].Error)
	assert.NotContains(t, resp.Msg.Results[0].Error, "secret-backend")
}
