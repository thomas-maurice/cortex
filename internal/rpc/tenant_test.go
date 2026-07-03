package rpc

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/identity"
	"github.com/thomas-maurice/cortex/internal/store"
)

// ---- tenantStore unit tests ----
//
// tenantStore is the single point where a request's tenant is derived from the
// context. In multi-tenant mode a missing identity MUST be a hard error, not a
// fallback: the auth interceptor is supposed to reject unauthenticated requests
// first, so an identity-less context reaching a handler means that guarantee
// broke — and falling back to a shared tenant would turn an auth bug into a
// cross-tenant data leak.

func TestTenantStoreFailsLoudWithoutIdentityInMTMode(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: true}, store: &store.Store{}}
	_, err := svc.tenantStore(context.Background())
	require.Error(t, err)
	var ce *connect.Error
	require.True(t, errors.As(err, &ce))
	assert.Equal(t, connect.CodeUnauthenticated, ce.Code())
}

func TestTenantStoreFallsBackToBootstrapInSingleUserMode(t *testing.T) {
	// Single-user/dev mode has no auth, so no identity is normal — requests
	// must keep working against the bootstrap tenant.
	svc := &Service{cfg: Config{MultiTenant: false}, store: &store.Store{}}
	ts, err := svc.tenantStore(context.Background())
	require.NoError(t, err)
	require.NotNil(t, ts)
}

func TestTenantStoreAcceptsAuthenticatedIdentityInMTMode(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: true}, store: &store.Store{}}
	ctx := identity.Into(context.Background(), identity.Identity{UserID: "u-1", Username: "alice", Role: identity.RoleUser})
	ts, err := svc.tenantStore(ctx)
	require.NoError(t, err)
	require.NotNil(t, ts)
}
