package rpc

import (
	"context"
	"errors"
	"sync"

	"connectrpc.com/connect"

	"github.com/thomas-maurice/cortex/internal/identity"
)

// tenantGate is a single-flight guard: at most one holder per key. Heavy
// "fetch everything" RPCs (backup, reindex, restore) hold the whole tenant in
// memory, so letting one tenant run N of them concurrently would let that tenant
// OOM the server for everyone. The zero value is ready to use.
//
// State is per process: with several server replicas the limit is per replica.
type tenantGate struct {
	mu   sync.Mutex
	held map[string]struct{}
}

// TryAcquire takes the slot for key. ok is false when it is already held. The
// returned release func is idempotent and must be called when the work ends.
func (g *tenantGate) TryAcquire(key string) (release func(), ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, busy := g.held[key]; busy {
		return nil, false
	}
	if g.held == nil {
		g.held = make(map[string]struct{})
	}
	g.held[key] = struct{}{}
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			delete(g.held, key)
			g.mu.Unlock()
		})
	}, true
}

// adminGateKey is the single server-wide key for the admin-only heavy RPCs.
const adminGateKey = "server"

// gateKey derives the gate key from the AUTHENTICATED identity only (never from
// request fields). Without an identity (single-tenant/dev mode) a constant is used.
func gateKey(ctx context.Context) string {
	if id, ok := identity.From(ctx); ok {
		return id.UserID
	}
	return ""
}

// acquire takes g for key or returns ResourceExhausted with msg.
func acquire(g *tenantGate, key, msg string) (func(), error) {
	release, ok := g.TryAcquire(key)
	if !ok {
		return nil, connect.NewError(connect.CodeResourceExhausted, errors.New(msg))
	}
	return release, nil
}
