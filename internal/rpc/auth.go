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

	"connectrpc.com/connect"
)

// authHeader is where the bearer token travels. Kept as a constant so the
// client and server agree, and so a future scheme (OIDC, per-client API keys)
// has one place to evolve.
const authHeader = "Authorization"

// Authenticator decides whether an inbound request may proceed. It is given the
// request headers so future schemes (API keys in X-Api-Key, OIDC bearer JWTs)
// can plug in without changing the interceptor wiring.
type Authenticator interface {
	Authenticate(ctx context.Context, h http.Header) error
}

// NewAuthenticator returns an Authenticator for the given shared token, plus
// whether auth is actually enforced. An empty token yields an open server
// (every request allowed) — convenient for local dev, dangerous in the open;
// callers should warn loudly when enabled is false.
func NewAuthenticator(token string) (Authenticator, bool) {
	if token == "" {
		return openAuth{}, false
	}
	return tokenAuth{token: token}, true
}

// openAuth allows every request. Used when no token is configured.
type openAuth struct{}

func (openAuth) Authenticate(context.Context, http.Header) error { return nil }

// tokenAuth checks a single shared bearer token in constant time.
type tokenAuth struct{ token string }

func (t tokenAuth) Authenticate(_ context.Context, h http.Header) error {
	raw := h.Get(authHeader)
	got, ok := strings.CutPrefix(raw, "Bearer ")
	if !ok || subtle.ConstantTimeCompare([]byte(strings.TrimSpace(got)), []byte(t.token)) != 1 {
		return connect.NewError(connect.CodeUnauthenticated, errors.New("invalid or missing bearer token"))
	}
	return nil
}

// jwtAuth accepts a valid UI-issued JWT in the bearer header. It shares the
// header with tokenAuth; a static token simply fails to parse as a JWT and a
// JWT fails the constant-time token compare, so a multiAuth can try both.
type jwtAuth struct{ mgr *JWTManager }

func (j jwtAuth) Authenticate(_ context.Context, h http.Header) error {
	raw := h.Get(authHeader)
	got, ok := strings.CutPrefix(raw, "Bearer ")
	if !ok {
		return connect.NewError(connect.CodeUnauthenticated, errors.New("invalid or missing bearer token"))
	}
	if _, err := j.mgr.Parse(strings.TrimSpace(got)); err != nil {
		return connect.NewError(connect.CodeUnauthenticated, errors.New("invalid or expired token"))
	}
	return nil
}

// multiAuth passes a request if ANY of its authenticators accepts it. It lets
// the static API token (MCP/CLI) and a UI JWT (browser) coexist on one server.
type multiAuth struct{ auths []Authenticator }

func (m multiAuth) Authenticate(ctx context.Context, h http.Header) error {
	err := error(connect.NewError(connect.CodeUnauthenticated, errors.New("unauthenticated")))
	for _, a := range m.auths {
		if e := a.Authenticate(ctx, h); e == nil {
			return nil
		} else {
			err = e
		}
	}
	return err
}

// NewServerAuthenticator builds the server-side authenticator. With an empty
// token the server is open (dev). Otherwise it accepts the static token and, if
// a JWT manager is supplied, UI-issued JWTs as well.
func NewServerAuthenticator(token string, jwtMgr *JWTManager) (Authenticator, bool) {
	var auths []Authenticator
	if token != "" {
		auths = append(auths, tokenAuth{token: token})
	}
	if jwtMgr != nil {
		auths = append(auths, jwtAuth{mgr: jwtMgr})
	}
	if len(auths) == 0 {
		return openAuth{}, false
	}
	return multiAuth{auths: auths}, true
}

// ServerAuthInterceptor enforces the Authenticator on inbound handler calls.
func ServerAuthInterceptor(a Authenticator) connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if req.Spec().IsClient {
				return next(ctx, req)
			}
			if err := a.Authenticate(ctx, req.Header()); err != nil {
				return nil, err
			}
			return next(ctx, req)
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
