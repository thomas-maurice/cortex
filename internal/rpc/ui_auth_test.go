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
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestJWTRoundTrip(t *testing.T) {
	m := NewJWTManager("secret", time.Hour)
	tok, err := m.Issue("alice", "admin")
	require.NoError(t, err)

	claims, err := m.Parse(tok)
	require.NoError(t, err)
	assert.Equal(t, "alice", claims.Username)
	assert.Equal(t, "admin", claims.Role)
}

func TestJWTRejectsWrongSecret(t *testing.T) {
	tok, err := NewJWTManager("secret-a", time.Hour).Issue("alice", "admin")
	require.NoError(t, err)
	// A token signed with a different secret must not validate — this is the
	// guarantee that a UI session can't be forged without the server secret.
	_, err = NewJWTManager("secret-b", time.Hour).Parse(tok)
	assert.Error(t, err)
}

func TestJWTRejectsExpired(t *testing.T) {
	m := NewJWTManager("secret", -time.Minute) // already expired
	tok, err := m.Issue("alice", "admin")
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
	h := LoginHandler(mgr, "admin", "hunter2", "admin", testLogger())

	rr := postLogin(t, h, `{"username":"admin","password":"hunter2"}`)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp struct {
		Token string `json:"token"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	claims, err := mgr.Parse(resp.Token)
	require.NoError(t, err)
	assert.Equal(t, "admin", claims.Username)
}

func TestLoginHandlerRejectsBadCredentials(t *testing.T) {
	h := LoginHandler(NewJWTManager("secret", time.Hour), "admin", "hunter2", "admin", testLogger())
	rr := postLogin(t, h, `{"username":"admin","password":"wrong"}`)
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestLoginHandlerDisabledWhenNoPassword(t *testing.T) {
	// No configured password means the UI login must be refused outright, never
	// fall through to issuing a token.
	h := LoginHandler(NewJWTManager("secret", time.Hour), "admin", "", "admin", testLogger())
	rr := postLogin(t, h, `{"username":"admin","password":""}`)
	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
}

func TestServerAuthenticatorAcceptsTokenOrJWT(t *testing.T) {
	mgr := NewJWTManager("secret", time.Hour)
	auth, enabled := NewServerAuthenticator("static-tok", mgr)
	require.True(t, enabled)

	jwtTok, err := mgr.Issue("alice", "admin")
	require.NoError(t, err)

	cases := map[string]struct {
		bearer string
		wantOK bool
	}{
		"static token":  {"static-tok", true},
		"valid jwt":     {jwtTok, true},
		"garbage":       {"nonsense", false},
		"empty":         {"", false},
		"wrong token":   {"static-toj", false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := http.Header{}
			if tc.bearer != "" {
				h.Set(authHeader, "Bearer "+tc.bearer)
			}
			err := auth.Authenticate(t.Context(), h)
			if tc.wantOK {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestServerAuthenticatorOpenWhenNoTokenOrJWT(t *testing.T) {
	auth, enabled := NewServerAuthenticator("", nil)
	assert.False(t, enabled)
	assert.NoError(t, auth.Authenticate(t.Context(), http.Header{}))
}
