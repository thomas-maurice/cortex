package rpc

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/identity"
)

// Auth is the only trust boundary in the server: a wrong/missing token must be
// rejected and a correct one accepted. These cases pin that contract so a
// refactor of the header parsing can't silently open the server.
func TestTokenAuth(t *testing.T) {
	a, enabled := NewAuthenticator("s3cret")
	require.True(t, enabled, "a non-empty token must enforce auth")

	hdr := func(v string) http.Header {
		h := http.Header{}
		if v != "" {
			h.Set("Authorization", v)
		}
		return h
	}

	id, err := a.Authenticate(context.Background(), hdr("Bearer s3cret"))
	assert.NoError(t, err, "correct token accepted")
	assert.Equal(t, identity.RoleAdmin, id.Role, "token auth resolves bootstrap admin role")

	_, err = a.Authenticate(context.Background(), hdr("Bearer wrong"))
	assert.Error(t, err, "wrong token rejected")
	_, err = a.Authenticate(context.Background(), hdr("s3cret"))
	assert.Error(t, err, "missing Bearer prefix rejected")
	_, err = a.Authenticate(context.Background(), hdr(""))
	assert.Error(t, err, "missing header rejected")
}

// An empty configured token disables enforcement (local-dev convenience). This
// is intentional but dangerous, so it is pinned: callers rely on `enabled` to
// warn, and on every request being allowed through with the bootstrap identity.
func TestOpenAuth(t *testing.T) {
	a, enabled := NewAuthenticator("")
	assert.False(t, enabled, "an empty token must report auth disabled")
	id, err := a.Authenticate(context.Background(), http.Header{})
	assert.NoError(t, err, "open auth allows unauthenticated requests")
	assert.Equal(t, identity.RoleAdmin, id.Role, "open auth returns bootstrap admin identity")
	assert.NotEmpty(t, id.UserID, "open auth must produce a non-empty UserID (bootstrap tenant)")
}
