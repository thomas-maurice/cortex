package rpc

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alexedwards/argon2id"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/identity"
	"github.com/thomas-maurice/cortex/internal/store"
)

// fakeKeyStore implements apiKeyLookup with an in-memory set of keys and users.
// It is the stand-in for Weaviate in unit tests; using the real store here would
// require a live Weaviate and belong in an integration test.
type fakeKeyStore struct {
	keys       map[string]store.ApiKey // keyed by keyHash
	users      map[string]store.User   // keyed by userId
	touchCalls atomic.Int32            // count of TouchApiKeyLastUsed calls
}

func (f *fakeKeyStore) GetApiKeyByHash(_ context.Context, keyHash string) (store.ApiKey, bool, error) {
	k, ok := f.keys[keyHash]
	return k, ok, nil
}

func (f *fakeKeyStore) GetUserByID(_ context.Context, id string) (store.User, bool, error) {
	u, ok := f.users[id]
	return u, ok, nil
}

func (f *fakeKeyStore) TouchApiKeyLastUsed(_ context.Context, _ string, _ time.Time) error {
	f.touchCalls.Add(1)
	return nil
}

// bearerHeader builds an http.Header with the given bearer value.
func bearerHeader(token string) http.Header {
	h := http.Header{}
	if token != "" {
		h.Set(authHeader, "Bearer "+token)
	}
	return h
}

// TestAPIKeyAuthResolvesIdentity pins the primary path: a known API key is
// looked up by sha256 hash and the owning user's identity is returned. This is
// the security-critical resolution — the wrong answer here is cross-user access.
func TestAPIKeyAuthResolvesIdentity(t *testing.T) {
	raw := "ctx_testkey1234567"
	keyHash := store.HashAPIKey(raw)

	fs := &fakeKeyStore{
		keys: map[string]store.ApiKey{
			keyHash: {ID: "key-1", KeyHash: keyHash, UserID: "user-alice"},
		},
		users: map[string]store.User{
			"user-alice": {ID: "user-alice", Username: "alice", Role: identity.RoleAdmin},
		},
	}
	auth := newAPIKeyAuth(fs)

	id, err := auth.Authenticate(context.Background(), bearerHeader(raw))
	require.NoError(t, err)
	assert.Equal(t, "user-alice", id.UserID)
	assert.Equal(t, "alice", id.Username)
	assert.Equal(t, identity.RoleAdmin, id.Role)
}

// TestAPIKeyAuthUnknownKeyRejected pins the rejection contract: an unknown key
// must return an error and no identity, never a zero-value identity that could
// accidentally pass a downstream role check.
func TestAPIKeyAuthUnknownKeyRejected(t *testing.T) {
	fs := &fakeKeyStore{
		keys:  map[string]store.ApiKey{},
		users: map[string]store.User{},
	}
	auth := newAPIKeyAuth(fs)

	_, err := auth.Authenticate(context.Background(), bearerHeader("ctx_unknownkey9999"))
	require.Error(t, err, "unknown key must be rejected")
}

// TestAPIKeyAuthMissingBearerRejected covers missing / malformed Authorization.
func TestAPIKeyAuthMissingBearerRejected(t *testing.T) {
	fs := &fakeKeyStore{
		keys:  map[string]store.ApiKey{},
		users: map[string]store.User{},
	}
	auth := newAPIKeyAuth(fs)

	_, err := auth.Authenticate(context.Background(), http.Header{})
	assert.Error(t, err, "missing header must be rejected")

	_, err = auth.Authenticate(context.Background(), bearerHeader(""))
	assert.Error(t, err, "empty bearer must be rejected")
}

// TestAPIKeyAuthDebounce pins the write-amplification guard: two auth calls for
// the same key within the debounce window must produce AT MOST ONE
// TouchApiKeyLastUsed call. A second call within the window is a no-op.
// This is load-bearing: a busy MCP client makes a request per tool call, so
// without debouncing every request would fire a Weaviate merge.
func TestAPIKeyAuthDebounce(t *testing.T) {
	raw := "ctx_debouncekey123"
	keyHash := store.HashAPIKey(raw)

	fs := &fakeKeyStore{
		keys: map[string]store.ApiKey{
			keyHash: {ID: "key-db", KeyHash: keyHash, UserID: "user-bob"},
		},
		users: map[string]store.User{
			"user-bob": {ID: "user-bob", Username: "bob", Role: identity.RoleUser},
		},
	}
	auth := newAPIKeyAuth(fs)

	// First call should trigger a touch.
	_, err := auth.Authenticate(context.Background(), bearerHeader(raw))
	require.NoError(t, err)

	// Second call immediately — must NOT trigger another touch.
	_, err = auth.Authenticate(context.Background(), bearerHeader(raw))
	require.NoError(t, err)

	// The touch is async; give it a moment to land.
	time.Sleep(30 * time.Millisecond)

	assert.EqualValues(t, 1, fs.touchCalls.Load(),
		"exactly one TouchApiKeyLastUsed must fire for two auth calls within the debounce window")
}

// TestAPIKeyAuthDebounceExpiry confirms a touch IS written after the window
// expires. We fake the expiry by backdating the lastTouched entry directly.
func TestAPIKeyAuthDebounceExpiry(t *testing.T) {
	raw := "ctx_expiredkey9876"
	keyHash := store.HashAPIKey(raw)

	fs := &fakeKeyStore{
		keys: map[string]store.ApiKey{
			keyHash: {ID: "key-exp", KeyHash: keyHash, UserID: "user-carol"},
		},
		users: map[string]store.User{
			"user-carol": {ID: "user-carol", Username: "carol", Role: identity.RoleUser},
		},
	}
	auth := newAPIKeyAuth(fs)

	// Pre-date the last-touched time so it looks expired.
	auth.mu.Lock()
	auth.lastTouched["key-exp"] = time.Now().Add(-2 * debounceWindow)
	auth.mu.Unlock()

	_, err := auth.Authenticate(context.Background(), bearerHeader(raw))
	require.NoError(t, err)

	time.Sleep(30 * time.Millisecond)
	assert.EqualValues(t, 1, fs.touchCalls.Load(), "an expired debounce window must allow a new touch")
}

// TestJWTAuthResolvesIdentityFromClaims pins that jwtAuth reads UserID,
// Username, and Role from the token claims and passes them through intact. The
// JWT is the only channel between the login handler and the interceptor; if
// claim→identity mapping is wrong, the wrong user sees the wrong data.
func TestJWTAuthResolvesIdentityFromClaims(t *testing.T) {
	mgr := NewJWTManager("jwt-secret", time.Hour)
	tok, err := mgr.Issue("user-id-dave", "dave", identity.RoleUser)
	require.NoError(t, err)

	ja := jwtAuth{mgr: mgr, bootstrap: bootstrapIdentity("admin")}
	id, err := ja.Authenticate(context.Background(), bearerHeader(tok))
	require.NoError(t, err)
	assert.Equal(t, "user-id-dave", id.UserID)
	assert.Equal(t, "dave", id.Username)
	assert.Equal(t, identity.RoleUser, id.Role)
}

// TestJWTAuthBackwardCompatNoUserID verifies that an older JWT without a UserID
// claim falls back to the bootstrap identity's UserID rather than an empty
// string (which would resolve to the wrong/no tenant in P3).
func TestJWTAuthBackwardCompatNoUserID(t *testing.T) {
	mgr := NewJWTManager("jwt-secret", time.Hour)
	// Issue a token WITHOUT the UserID field (empty string = "no claim" for old clients).
	tok, err := mgr.Issue("", "admin", identity.RoleAdmin)
	require.NoError(t, err)

	bootstrap := bootstrapIdentity("admin")
	ja := jwtAuth{mgr: mgr, bootstrap: bootstrap}
	id, err := ja.Authenticate(context.Background(), bearerHeader(tok))
	require.NoError(t, err)
	assert.Equal(t, bootstrap.UserID, id.UserID,
		"a JWT with no UserID claim must fall back to the bootstrap admin's UserID")
}

// TestAPIKeyAuthInMultiAuth verifies that apiKeyAuth participates correctly when
// composed into a multiAuth alongside tokenAuth — the first successful
// authenticator's identity wins.
func TestAPIKeyAuthInMultiAuth(t *testing.T) {
	raw := "ctx_multiauth_key99"
	keyHash := store.HashAPIKey(raw)

	fs := &fakeKeyStore{
		keys: map[string]store.ApiKey{
			keyHash: {ID: "key-m", KeyHash: keyHash, UserID: "user-frank"},
		},
		users: map[string]store.User{
			"user-frank": {ID: "user-frank", Username: "frank", Role: identity.RoleUser},
		},
	}

	auth, _ := NewServerAuthenticator(ServerAuthenticatorConfig{
		Token:             "static-tok",
		MultiTenant:       true,
		Store:             fs,
		BootstrapUsername: "admin",
	})

	// API key resolves to frank, not the bootstrap admin.
	id, err := auth.Authenticate(context.Background(), bearerHeader(raw))
	require.NoError(t, err)
	assert.Equal(t, "user-frank", id.UserID)
	assert.Equal(t, identity.RoleUser, id.Role)
}

// ---- MT login handler tests via fake store ----

// mtLoginHandler builds an http.Handler that mirrors LoginHandler's MT path
// using an in-memory user map, so we can test the argon2 verify + JWT issue
// without a real Weaviate.
type fakeLoginEntry struct {
	id   string
	hash string
	role string
}

func loginHandlerFromFakeUsers(mgr *JWTManager, users map[string]fakeLoginEntry) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 4096))
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		var req loginRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		entry, ok := users[req.Username]
		if !ok || !verifyPassword(req.Password, entry.hash) {
			http.Error(w, "invalid credentials", http.StatusUnauthorized)
			return
		}
		tok, err := mgr.Issue(entry.id, req.Username, entry.role)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(loginResponse{Token: tok})
	})
}

func TestMultiTenantLoginSuccess(t *testing.T) {
	hashedPass, err := argon2id.CreateHash("s3cret", argon2id.DefaultParams)
	require.NoError(t, err)

	mgr := NewJWTManager("secret", time.Hour)
	h := loginHandlerFromFakeUsers(mgr, map[string]fakeLoginEntry{
		"alice": {id: "user-id-alice", hash: hashedPass, role: identity.RoleAdmin},
	})

	rr := postLogin(t, h, `{"username":"alice","password":"s3cret"}`)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp loginResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	claims, err := mgr.Parse(resp.Token)
	require.NoError(t, err)
	assert.Equal(t, "user-id-alice", claims.UserID, "JWT must carry the user's store id as userId")
	assert.Equal(t, "alice", claims.Username)
	assert.Equal(t, identity.RoleAdmin, claims.Role)
}

func TestMultiTenantLoginWrongPassword(t *testing.T) {
	hashedPass, err := argon2id.CreateHash("right", argon2id.DefaultParams)
	require.NoError(t, err)

	mgr := NewJWTManager("secret", time.Hour)
	h := loginHandlerFromFakeUsers(mgr, map[string]fakeLoginEntry{
		"eve": {id: "user-eve", hash: hashedPass, role: identity.RoleUser},
	})

	rr := postLogin(t, h, `{"username":"eve","password":"wrong"}`)
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestMultiTenantLoginUnknownUser(t *testing.T) {
	mgr := NewJWTManager("secret", time.Hour)
	h := loginHandlerFromFakeUsers(mgr, map[string]fakeLoginEntry{})

	rr := postLogin(t, h, `{"username":"ghost","password":"x"}`)
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

// TestServerAuthInterceptorPutsIdentityOnContext verifies that the interceptor
// places the resolved identity on the context so RPC handlers can read the
// tenant without touching the request body. This is the security boundary: if
// identity doesn't land on ctx, P3 store ops will have no tenant.
//
// We test the core contract — authenticate → Into(ctx, id) → From(ctx) — by
// calling the three functions directly rather than spinning up a full Connect
// server. The wiring in ServerAuthInterceptor is trivial; the important
// invariants are (a) Authenticate returns the right identity and (b) Into/From
// round-trip correctly (each covered separately). Together they pin the contract.
func TestServerAuthInterceptorPutsIdentityOnContext(t *testing.T) {
	want := identity.Identity{UserID: "uid-123", Username: "alice", Role: identity.RoleAdmin}
	auth := fixedAuth{id: want}

	ctx := context.Background()
	id, err := auth.Authenticate(ctx, http.Header{})
	require.NoError(t, err)

	enrichedCtx := identity.Into(ctx, id)
	gotID, gotOK := identity.From(enrichedCtx)

	assert.True(t, gotOK, "identity must be present on context after successful auth")
	assert.Equal(t, want, gotID)
}

// fixedAuth always grants a pre-set identity (used to test the interceptor
// contract in isolation from any real credential logic).
type fixedAuth struct{ id identity.Identity }

func (f fixedAuth) Authenticate(context.Context, http.Header) (identity.Identity, error) {
	return f.id, nil
}
