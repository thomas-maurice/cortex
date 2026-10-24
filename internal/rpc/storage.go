package rpc

import (
	"context"
	"fmt"
	"time"

	"connectrpc.com/connect"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/identity"
	"github.com/thomas-maurice/cortex/internal/memory"
)

// embedProbeTimeout bounds the embedder probe so a hung Ollama cannot hang the page.
const embedProbeTimeout = 5 * time.Second

// GetStorageInfo is the read-only System -> Storage view. Admins (and everyone in
// non-MT mode) get the full picture; a non-admin in MT mode gets the same response
// shape reduced to their own tenant by scopeToTenant, which runs LAST so no field
// added above can bypass it.
func (s *Service) GetStorageInfo(ctx context.Context, _ *connect.Request[cortexv1.GetStorageInfoRequest]) (*connect.Response[cortexv1.GetStorageInfoResponse], error) {
	id, haveID := identity.From(ctx)
	scoped := false
	if s.cfg.MultiTenant {
		if !haveID {
			return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("no authenticated identity on request"))
		}
		scoped = !id.IsAdmin()
	}

	onlyTenant := ""
	if scoped {
		onlyTenant = id.UserID
	}
	info, err := s.store.StorageInfo(ctx, onlyTenant)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("read storage info: %w", err))
	}

	usernames := map[string]string{}
	if scoped {
		// Only the caller's own username is ever shown.
		usernames[id.UserID] = id.Username
	} else if s.cfg.MultiTenant {
		// Best effort: a failed lookup only means ids are shown without usernames.
		if users, uerr := s.store.ListUsers(ctx); uerr == nil {
			for _, u := range users {
				usernames[u.ID] = u.Username
			}
		}
	}
	out := storageToProto(info, usernames)
	out.EmbedModel = s.embedder.Model()
	out.Maintenance = s.inMaintenance(ctx)
	if s.migrations != nil {
		if st, merr := s.migrations.Status(ctx); merr == nil {
			out.SchemaVersion = int32(st.SchemaVersion)
		}
	}
	pctx, cancel := context.WithTimeout(ctx, embedProbeTimeout)
	defer cancel()
	if vec, eerr := s.embedder.Embed(pctx, "ping"); eerr == nil {
		out.EmbedDims = int32(len(vec))
	}

	if scoped {
		scopeToTenant(out, id.UserID)
	}
	return connect.NewResponse(out), nil
}

// scopeToTenant reduces a full storage response, in place, to what one tenant may
// see: their own shard per multi-tenant class (counts, status and queue recomputed
// from it), no orphans, no nodes, no global identity/metadata classes. Vector settings,
// physical names, the model and the schema version are deployment facts, not tenant
// data, and are kept. It is the single place that does this so a new field cannot
// leak by being forgotten elsewhere; TestScopeToTenant walks every string field.
func scopeToTenant(resp *cortexv1.GetStorageInfoResponse, tenant string) {
	resp.Scoped = true
	resp.OrphanClasses = nil
	resp.Nodes = nil
	kept := resp.Classes[:0]
	for _, c := range resp.Classes {
		if c.Logical == memory.UserClassName || c.Logical == memory.ApiKeyClassName || c.Logical == memory.MetaClassName {
			continue
		}
		// A class that is not multi-tenant has no per-tenant figure to show: its
		// totals are global, so they are withheld rather than leaked.
		c.ObjectCount, c.IndexingStatus, c.VectorQueueLength = -1, "", 0
		var mine []*cortexv1.StorageTenant
		for _, t := range c.Tenants {
			if t.Tenant == tenant {
				mine = append(mine, t)
			}
		}
		c.Tenants = mine
		c.TenantCount = int32(len(mine))
		if c.MultiTenant {
			c.ObjectCount = 0 // no shard for this tenant yet: nothing stored
			if len(mine) == 1 {
				c.ObjectCount, c.IndexingStatus, c.VectorQueueLength = mine[0].ObjectCount, mine[0].IndexingStatus, mine[0].VectorQueueLength
			}
		}
		kept = append(kept, c)
	}
	resp.Classes = kept
}
