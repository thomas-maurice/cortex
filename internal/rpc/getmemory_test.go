package rpc

import (
	"context"
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

func connectCode(t *testing.T, err error) connect.Code {
	t.Helper()
	var ce *connect.Error
	require.ErrorAs(t, err, &ce)
	return ce.Code()
}

// Malformed ids must be rejected before any store access: an empty id is a
// caller bug, and a non-UUID would otherwise reach Weaviate and come back as an
// opaque 5xx instead of a clear InvalidArgument.
func TestGetMemoryRejectsBadIDs(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: false}, store: &store.Store{}}
	for _, id := range []string{"", "   ", "not-a-uuid"} {
		_, err := svc.GetMemory(context.Background(), connect.NewRequest(&cortexv1.GetMemoryRequest{Id: id}))
		require.Error(t, err, "id %q", id)
		assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err), "id %q", id)
	}
}

// TestGetMemoryIntegration needs a MULTI-TENANT Weaviate (see the run recipe in
// internal/store/mt_integration_test.go). Tenant isolation is the security
// invariant: knowing another tenant's memory id must reveal nothing, not even
// that it exists.
func TestGetMemoryIntegration(t *testing.T) {
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

	svc := &Service{cfg: Config{MultiTenant: true}, store: st}
	alice := identity.Into(ctx, identity.Identity{UserID: "user-alice", Username: "alice", Role: identity.RoleUser})
	bob := identity.Into(ctx, identity.Identity{UserID: "user-bob", Username: "bob", Role: identity.RoleUser})

	rec := memory.Record{
		ID: uuid.NewString(), Text: "Alice's secret", Namespace: "ns1", Tags: []string{"t1"},
		Source: "test", LinkedIDs: []string{uuid.NewString()}, CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, st.Tenant("user-alice").Upsert(ctx, rec, []float32{1, 0, 0, 0}))
	get := func(c context.Context, id string) (*connect.Response[cortexv1.GetMemoryResponse], error) {
		return svc.GetMemory(c, connect.NewRequest(&cortexv1.GetMemoryRequest{Id: id}))
	}

	t.Run("own memory comes back with full fields", func(t *testing.T) {
		resp, err := get(alice, rec.ID)
		require.NoError(t, err)
		m := resp.Msg.GetMemory()
		assert.Equal(t, rec.ID, m.GetId())
		assert.Equal(t, rec.Text, m.GetText())
		assert.Equal(t, "ns1", m.GetNamespace())
		assert.Equal(t, rec.Tags, m.GetTags())
		assert.Equal(t, rec.LinkedIDs, m.GetLinkedIds())
	})
	t.Run("unknown id is NotFound", func(t *testing.T) {
		_, err := get(alice, uuid.NewString())
		require.Error(t, err)
		assert.Equal(t, connect.CodeNotFound, connectCode(t, err))
	})
	t.Run("another tenant's id is NotFound", func(t *testing.T) {
		_, err := get(bob, rec.ID)
		require.Error(t, err)
		assert.Equal(t, connect.CodeNotFound, connectCode(t, err))
	})
}
