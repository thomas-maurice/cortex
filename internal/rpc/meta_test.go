package rpc

import (
	"context"
	"os"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/identity"
	"github.com/thomas-maurice/cortex/internal/migrate"
	"github.com/thomas-maurice/cortex/internal/natstest"
	"github.com/thomas-maurice/cortex/internal/store"
)

// ListMeta exposes lock owners and worker ids: a non-admin must be refused before
// anything is read (a nil store would panic if the gate were after the read).
func TestListMetaDeniesNonAdmin(t *testing.T) {
	s := &Service{}
	req := connect.NewRequest(&cortexv1.ListMetaRequest{})
	_, err := s.ListMeta(identity.Into(context.Background(), identity.Identity{UserID: "u", Role: identity.RoleUser}), req)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	_, err = s.ListMeta(context.Background(), req)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

// Needs the MT Weaviate recipe (see getmemory_test.go). The admin sees BOTH the
// durable and the coordination side, so an operator can tell where a fact lives.
func TestListMetaIntegration(t *testing.T) {
	rest, grpc := os.Getenv("CORTEX_TEST_WEAVIATE_REST"), os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" || os.Getenv("CORTEX_TEST_MULTI_TENANT") == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST, CORTEX_TEST_WEAVIATE_GRPC and CORTEX_TEST_MULTI_TENANT=1 to run this integration test")
	}
	ctx := context.Background()
	st, err := store.New(rest, grpc)
	require.NoError(t, err)
	st.SetMultiTenant(true)
	_ = st.DeleteAllClasses(ctx)
	require.NoError(t, st.DeleteMetaClass(ctx))
	require.NoError(t, st.EnsureSchema(ctx))
	t.Cleanup(func() { _ = st.DeleteAllClasses(ctx); _ = st.DeleteMetaClass(ctx) })
	require.NoError(t, st.PutMeta(ctx, "schema_version", []byte("1")))

	_, js := natstest.Start(t)
	kv, err := migrate.OpenMeta(ctx, js)
	require.NoError(t, err)
	_, err = kv.Put(ctx, "migrate_lock", []byte("server x/1"))
	require.NoError(t, err)

	svc := &Service{cfg: Config{MultiTenant: true}, store: st}
	svc.SetMetaKV(kv)
	req := connect.NewRequest(&cortexv1.ListMetaRequest{})

	admin := identity.Into(ctx, identity.Identity{UserID: "a", Username: "admin", Role: identity.RoleAdmin})
	res, err := svc.ListMeta(admin, req)
	require.NoError(t, err)
	require.Len(t, res.Msg.Durable, 1)
	assert.Equal(t, "schema_version", res.Msg.Durable[0].Key)
	assert.Equal(t, "1", res.Msg.Durable[0].Value)
	assert.NotNil(t, res.Msg.Durable[0].UpdatedAt)
	require.Len(t, res.Msg.Coordination, 1)
	assert.Equal(t, "migrate_lock", res.Msg.Coordination[0].Key)
	assert.Equal(t, "server x/1", res.Msg.Coordination[0].Value)
	assert.NotZero(t, res.Msg.Coordination[0].Revision)
	assert.NotNil(t, res.Msg.Coordination[0].CreatedAt)

	user := identity.Into(ctx, identity.Identity{UserID: "u", Username: "bob", Role: identity.RoleUser})
	_, err = svc.ListMeta(user, req)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}
