package rpc

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/identity"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestJWTRoundTrip(t *testing.T) {
	m := NewJWTManager("secret", time.Hour)
	tok, err := m.Issue("user-id-123", "alice", "admin")
	require.NoError(t, err)

	claims, err := m.Parse(tok)
	require.NoError(t, err)
	assert.Equal(t, "user-id-123", claims.UserID)
	assert.Equal(t, "alice", claims.Username)
	assert.Equal(t, "admin", claims.Role)
}

func TestJWTRejectsWrongSecret(t *testing.T) {
	tok, err := NewJWTManager("secret-a", time.Hour).Issue("uid", "alice", "admin")
	require.NoError(t, err)
	// A token signed with a different secret must not validate — this is the
	// guarantee that a UI session can't be forged without the server secret.
	_, err = NewJWTManager("secret-b", time.Hour).Parse(tok)
	assert.Error(t, err)
}

func TestJWTRejectsExpired(t *testing.T) {
	m := NewJWTManager("secret", -time.Minute) // already expired
	tok, err := m.Issue("uid", "alice", "admin")
	require.NoError(t, err)
	_, err = m.Parse(tok)
	assert.Error(t, err)
}

func postLogin(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/auth/login", strings.NewReader(body))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestLoginHandlerSuccess(t *testing.T) {
	mgr := NewJWTManager("secret", time.Hour)
	h := LoginHandler(mgr, "admin", "hunter2", "admin", testLogger(), false, nil)

	rr := postLogin(t, h, `{"username":"admin","password":"hunter2"}`)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp struct {
		Token string `json:"token"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	claims, err := mgr.Parse(resp.Token)
	require.NoError(t, err)
	assert.Equal(t, "admin", claims.Username)
	assert.NotEmpty(t, claims.UserID, "single-user login must include a userId claim")
}

func TestLoginHandlerRejectsBadCredentials(t *testing.T) {
	h := LoginHandler(NewJWTManager("secret", time.Hour), "admin", "hunter2", "admin", testLogger(), false, nil)
	rr := postLogin(t, h, `{"username":"admin","password":"wrong"}`)
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestLoginHandlerDisabledWhenNoPassword(t *testing.T) {
	// No configured password means the UI login must be refused outright, never
	// fall through to issuing a token.
	h := LoginHandler(NewJWTManager("secret", time.Hour), "admin", "", "admin", testLogger(), false, nil)
	rr := postLogin(t, h, `{"username":"admin","password":""}`)
	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
}

func TestServerAuthenticatorAcceptsTokenOrJWT(t *testing.T) {
	mgr := NewJWTManager("secret", time.Hour)
	auth, enabled := NewServerAuthenticator(ServerAuthenticatorConfig{
		Token:             "static-tok",
		JWTMgr:            mgr,
		BootstrapUsername: "admin",
	})
	require.True(t, enabled)

	jwtTok, err := mgr.Issue("user-id-alice", "alice", "admin")
	require.NoError(t, err)

	cases := map[string]struct {
		bearer      string
		wantOK      bool
		wantUserID  string
		wantRole    string
	}{
		"static token": {"static-tok", true, "", identity.RoleAdmin},   // bootstrap id from username
		"valid jwt":    {jwtTok, true, "user-id-alice", identity.RoleAdmin},
		"garbage":      {"nonsense", false, "", ""},
		"empty":        {"", false, "", ""},
		"wrong token":  {"static-toj", false, "", ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := http.Header{}
			if tc.bearer != "" {
				h.Set(authHeader, "Bearer "+tc.bearer)
			}
			id, err := auth.Authenticate(t.Context(), h)
			if tc.wantOK {
				require.NoError(t, err)
				if tc.wantUserID != "" {
					assert.Equal(t, tc.wantUserID, id.UserID)
				}
				assert.Equal(t, tc.wantRole, id.Role)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestServerAuthenticatorOpenWhenNoTokenOrJWT(t *testing.T) {
	auth, enabled := NewServerAuthenticator(ServerAuthenticatorConfig{})
	assert.False(t, enabled)
	id, err := auth.Authenticate(t.Context(), http.Header{})
	assert.NoError(t, err)
	assert.Equal(t, identity.RoleAdmin, id.Role, "open auth must return bootstrap admin")
}

// TestLegacyTokenResolvesToBootstrapAdmin pins the security contract: the legacy
// CORTEX_AUTH_TOKEN resolves to the bootstrap admin identity, not an anonymous one.
// The BootstrapUsername maps to a deterministic tenant id that P3 uses as the
// legacy data tenant.
func TestLegacyTokenResolvesToBootstrapAdmin(t *testing.T) {
	auth, _ := NewServerAuthenticator(ServerAuthenticatorConfig{
		Token:             "legacy-token",
		BootstrapUsername: "admin",
	})
	h := http.Header{}
	h.Set(authHeader, "Bearer legacy-token")
	id, err := auth.Authenticate(t.Context(), h)
	require.NoError(t, err)
	assert.Equal(t, identity.RoleAdmin, id.Role)
	assert.Equal(t, "admin", id.Username)
	assert.NotEmpty(t, id.UserID, "bootstrap admin must have a non-empty UserID")
}
