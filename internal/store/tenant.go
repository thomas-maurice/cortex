package store

import (
	"errors"
	"strings"

	"github.com/weaviate/weaviate-go-client/v5/weaviate/fault"
)

// TenantStore is a tenant-scoped view of the memory classes (Memory, MemoryChunk,
// ConversationSummary). Every Weaviate Data/Search/Batch builder it creates carries
// .WithTenant(t) when multi-tenancy is enabled, so there is exactly one call site
// per method and it is impossible to forget the tenant.
//
// When multi-tenancy is OFF, t is the empty string and no .WithTenant call is
// issued — the class is queried without a tenant, which is the correct non-MT
// behaviour.
//
// Obtain a TenantStore from Store.Tenant(userID). A query with no tenant against
// an MT-enabled class will error from Weaviate — that is the desired fail-loud.
type TenantStore struct {
	s *Store
	t string // empty when MT is off
}

// Tenant returns a TenantStore scoped to userID. When multi-tenancy is disabled on
// s, t is the empty string and the returned handle issues no .WithTenant calls —
// behaviour is identical to the pre-MT code path.
func (s *Store) Tenant(userID string) *TenantStore {
	t := ""
	if s.multiTenant {
		t = userID
	}
	return &TenantStore{s: s, t: t}
}

// isTenantNotFound reports whether err is Weaviate's 422 "tenant not found"
// response. AutoTenantCreation only triggers on writes; reads and deletes against
// a tenant that was never written to return this error. The correct semantics for
// a lazy-created (auto) tenant is: reads are empty, deletes are no-ops.
//
// Only the exact 422 + "tenant not found" combination is matched so that genuine
// query errors (auth failures, schema problems, network errors) still surface loud.
func isTenantNotFound(err error) bool {
	var wErr *fault.WeaviateClientError
	if errors.As(err, &wErr) {
		return wErr.StatusCode == 422 && strings.Contains(wErr.Msg, "tenant not found")
	}
	// Fall back to string matching for wrapped errors from batch/gRPC paths that
	// may not carry the typed error.
	return strings.Contains(err.Error(), "tenant not found")
}
