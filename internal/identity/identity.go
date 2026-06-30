// Package identity carries the authenticated caller's identity on the request
// context, mirroring the project's logger-in-context convention. The auth
// interceptor resolves a JWT, API key, or the legacy shared token into an
// Identity and stores it on the context; RPC handlers, the store, and the NATS
// payload then read the tenant from there — NEVER from the request body. That is
// the load-bearing multi-tenancy security invariant: a client cannot name another
// user's tenant because the tenant is derived from authentication alone.
package identity

import "context"

// Role values stored on a User record and carried in the UI JWT.
const (
	RoleAdmin = "admin"
	RoleUser  = "user"
)

// BootstrapTenant is the reserved tenant id for the bootstrap-admin / single-user
// identity: the tenant an existing pre-multi-tenancy store migrates INTO, and the
// identity the legacy CORTEX_AUTH_TOKEN and dev-open mode resolve to. Keeping it a
// stable, reserved constant means migration (P4) and single-user mode always
// target the same tenant.
const BootstrapTenant = "cortex-bootstrap"

// Identity is the authenticated caller. UserID doubles as the Weaviate tenant
// name for that user's memories.
type Identity struct {
	UserID   string
	Username string
	Role     string
}

// IsAdmin reports whether the identity carries the admin role.
func (i Identity) IsAdmin() bool { return i.Role == RoleAdmin }

type ctxKey struct{}

// Into returns a copy of ctx carrying id.
func Into(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// From returns the identity stored on ctx and whether one was present. A false
// second return means the request was not authenticated into an identity — a
// tenant-scoped operation must refuse rather than guess a tenant.
func From(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(ctxKey{}).(Identity)
	return id, ok
}
