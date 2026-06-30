// Package rpc implements the Cortex Connect RPC server and a client helper. The
// server owns all NATS/Weaviate/Ollama access; the MCP server and CLI are thin
// clients of it.
package rpc

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"

	"github.com/thomas-maurice/cortex/internal/identity"
	"github.com/thomas-maurice/cortex/internal/store"
)

// authHeader is where the bearer token travels. Kept as a constant so the
// client and server agree, and so a future scheme (OIDC, per-client API keys)
// has one place to evolve.
const authHeader = "Authorization"

// debounceWindow is how long apiKeyAuth suppresses redundant lastUsedAt writes.
// A missed update is cosmetic (it's a display field) so a generous window keeps
// hot paths clean. Operators can tune this by rebuilding; we don't expose it as
// an env var because it's a performance knob, not a security one.
const debounceWindow = 10 * time.Minute

// Authenticator decides whether an inbound request may proceed and resolves the
// caller's identity. The identity is placed on the context by ServerAuthInterceptor
// so RPC handlers can read the tenant without touching the request body.
type Authenticator interface {
	// Authenticate verifies the request headers and returns the resolved identity.
	// A non-nil error means the request is unauthenticated and must be rejected
	// with CodeUnauthenticated. The identity is zero-valued on error.
	Authenticate(ctx context.Context, h http.Header) (identity.Identity, error)
}

// NewAuthenticator returns an Authenticator for the given shared token, plus
// whether auth is actually enforced. An empty token yields an open server
// (every request allowed) — convenient for local dev, dangerous in the open;
// callers should warn loudly when enabled is false.
func NewAuthenticator(token string) (Authenticator, bool) {
	if token == "" {
		return openAuth{bootstrap: bootstrapIdentity("")}, false
	}
	return tokenAuth{token: token, bootstrap: bootstrapIdentity("")}, true
}

// openAuth allows every request. Used when no token is configured.
// It returns the bootstrap admin identity so single-user dev mode has a stable
// tenant anchor (identity.BootstrapTenant) even without any credential.
type openAuth struct{ bootstrap identity.Identity }

func (o openAuth) Authenticate(context.Context, http.Header) (identity.Identity, error) {
	return o.bootstrap, nil
}

// tokenAuth checks a single shared bearer token in constant time. On a match it
// returns the pre-resolved bootstrap identity (the caller is the bootstrap admin,
// because the only credential is the single shared token).
type tokenAuth struct {
	token     string
	bootstrap identity.Identity
}

func (t tokenAuth) Authenticate(_ context.Context, h http.Header) (identity.Identity, error) {
	raw := h.Get(authHeader)
	got, ok := strings.CutPrefix(raw, "Bearer ")
	if !ok || subtle.ConstantTimeCompare([]byte(strings.TrimSpace(got)), []byte(t.token)) != 1 {
		return identity.Identity{}, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid or missing bearer token"))
	}
	return t.bootstrap, nil
}

// jwtAuth accepts a valid UI-issued JWT in the bearer header. It shares the
// header with tokenAuth; a static token simply fails to parse as a JWT and a
// JWT fails the constant-time token compare, so a multiAuth can try both.
// The resolved identity is read directly from the JWT claims.
type jwtAuth struct {
	mgr       *JWTManager
	bootstrap identity.Identity // fallback for old JWTs with no UserID claim
}

func (j jwtAuth) Authenticate(_ context.Context, h http.Header) (identity.Identity, error) {
	raw := h.Get(authHeader)
	got, ok := strings.CutPrefix(raw, "Bearer ")
	if !ok {
		return identity.Identity{}, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid or missing bearer token"))
	}
	claims, err := j.mgr.Parse(strings.TrimSpace(got))
	if err != nil {
		return identity.Identity{}, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid or expired token"))
	}
	id := identity.Identity{
		UserID:   claims.UserID,
		Username: claims.Username,
		Role:     claims.Role,
	}
	// Backward compat: tokens issued before P2 have no UserID claim. Fall back to
	// the bootstrap identity so an existing session keeps working after upgrade.
	if id.UserID == "" {
		id.UserID = j.bootstrap.UserID
	}
	return id, nil
}

// apiKeyLookup is the subset of store.Store that apiKeyAuth needs. Expressed as
// an interface so unit tests can substitute a fake without a real Weaviate.
type apiKeyLookup interface {
	GetApiKeyByHash(ctx context.Context, keyHash string) (store.ApiKey, bool, error)
	GetUserByID(ctx context.Context, id string) (store.User, bool, error)
	TouchApiKeyLastUsed(ctx context.Context, id string, at time.Time) error
}

// apiKeyAuth authenticates MCP / CLI clients that present a raw API key as the
// bearer token. On a match it resolves the owning user and returns their
// identity. lastUsedAt is updated asynchronously and debounced so a busy client
// does not hammer Weaviate with merge writes.
type apiKeyAuth struct {
	store apiKeyLookup

	// debounce tracks the last time each api key id was touched (by key id, not
	// hash, to avoid keeping the secret in memory any longer than needed).
	mu         sync.Mutex
	lastTouched map[string]time.Time
}

func newAPIKeyAuth(st apiKeyLookup) *apiKeyAuth {
	return &apiKeyAuth{
		store:       st,
		lastTouched: make(map[string]time.Time),
	}
}

func (a *apiKeyAuth) Authenticate(ctx context.Context, h http.Header) (identity.Identity, error) {
	raw := h.Get(authHeader)
	bearer, ok := strings.CutPrefix(raw, "Bearer ")
	if !ok || strings.TrimSpace(bearer) == "" {
		return identity.Identity{}, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid or missing bearer token"))
	}
	bearer = strings.TrimSpace(bearer)
	keyHash := store.HashAPIKey(bearer)

	key, found, err := a.store.GetApiKeyByHash(ctx, keyHash)
	if err != nil {
		return identity.Identity{}, connect.NewError(connect.CodeUnauthenticated, errors.New("api key lookup failed"))
	}
	if !found {
		return identity.Identity{}, connect.NewError(connect.CodeUnauthenticated, errors.New("unknown api key"))
	}

	u, found, err := a.store.GetUserByID(ctx, key.UserID)
	if err != nil || !found {
		return identity.Identity{}, connect.NewError(connect.CodeUnauthenticated, errors.New("api key owner not found"))
	}

	a.touchDebounced(key.ID)

	return identity.Identity{
		UserID:   u.ID,
		Username: u.Username,
		Role:     u.Role,
	}, nil
}

// touchDebounced schedules a lastUsedAt write if the key hasn't been touched
// within the debounce window. The write is fire-and-forget in a goroutine —
// never synchronous per request. A missed write is cosmetic.
func (a *apiKeyAuth) touchDebounced(keyID string) {
	now := time.Now()
	a.mu.Lock()
	last := a.lastTouched[keyID]
	if now.Sub(last) < debounceWindow {
		a.mu.Unlock()
		return
	}
	a.lastTouched[keyID] = now
	a.mu.Unlock()

	go func() {
		// context.Background: the request context may be cancelled by the time
		// the goroutine runs. A missed write is harmless; we don't retry.
		_ = a.store.TouchApiKeyLastUsed(context.Background(), keyID, now)
	}()
}

// multiAuth passes a request if ANY of its authenticators accepts it. It lets
// the static API token (MCP/CLI) and a UI JWT (browser) coexist on one server.
// The identity from the FIRST accepting authenticator is returned.
type multiAuth struct{ auths []Authenticator }

func (m multiAuth) Authenticate(ctx context.Context, h http.Header) (identity.Identity, error) {
	var lastErr error = connect.NewError(connect.CodeUnauthenticated, errors.New("unauthenticated"))
	for _, a := range m.auths {
		if id, e := a.Authenticate(ctx, h); e == nil {
			return id, nil
		} else {
			lastErr = e
		}
	}
	return identity.Identity{}, lastErr
}

// ServerAuthenticatorConfig carries the options for NewServerAuthenticator.
type ServerAuthenticatorConfig struct {
	// Token is the legacy CORTEX_AUTH_TOKEN shared bearer. Empty = no token check.
	Token string
	// JWTMgr is the JWT manager for UI-issued tokens. Nil = no JWT check.
	JWTMgr *JWTManager
	// MultiTenant enables the API-key lookup path via the provided store.
	MultiTenant bool
	// Store is used for API-key authentication when MultiTenant is true.
	Store apiKeyLookup
	// BootstrapUsername is the configured CORTEX_UI_USER / CORTEX_BOOTSTRAP_USER
	// used to derive the bootstrap admin identity for legacy-token + open paths.
	BootstrapUsername string
}

// NewServerAuthenticator builds the server-side authenticator from a config
// struct. With an empty token the server is open (dev). Otherwise it accepts
// the static token and, if a JWT manager is supplied, UI-issued JWTs as well.
// When MultiTenant is true, an additional apiKeyAuth is added.
//
// The bootstrap identity (legacy token + open mode) is derived from
// BootstrapUsername so the legacy CORTEX_AUTH_TOKEN resolves to the bootstrap
// admin's tenant rather than an anonymous one.
func NewServerAuthenticator(cfg ServerAuthenticatorConfig) (Authenticator, bool) {
	bootstrap := bootstrapIdentity(cfg.BootstrapUsername)
	var auths []Authenticator
	if cfg.Token != "" {
		auths = append(auths, tokenAuth{token: cfg.Token, bootstrap: bootstrap})
	}
	if cfg.JWTMgr != nil {
		auths = append(auths, jwtAuth{mgr: cfg.JWTMgr, bootstrap: bootstrap})
	}
	if cfg.MultiTenant && cfg.Store != nil {
		auths = append(auths, newAPIKeyAuth(cfg.Store))
	}
	if len(auths) == 0 {
		return openAuth{bootstrap: bootstrap}, false
	}
	return multiAuth{auths: auths}, true
}

// ServerAuthInterceptor enforces the Authenticator on inbound handler calls and
// places the resolved identity on the context so RPC handlers can read the
// tenant without touching the request body. This is the load-bearing security
// boundary: identity flows from auth → context, never from the request payload.
func ServerAuthInterceptor(a Authenticator) connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if req.Spec().IsClient {
				return next(ctx, req)
			}
			id, err := a.Authenticate(ctx, req.Header())
			if err != nil {
				return nil, err
			}
			return next(identity.Into(ctx, id), req)
		}
	})
}

// clientAuthInterceptor attaches the bearer token to outbound client requests.
func clientAuthInterceptor(token string) connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if req.Spec().IsClient && token != "" {
				req.Header().Set(authHeader, "Bearer "+token)
			}
			return next(ctx, req)
		}
	})
}
