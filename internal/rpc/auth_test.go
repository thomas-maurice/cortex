package rpc

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	assert.NoError(t, a.Authenticate(context.Background(), hdr("Bearer s3cret")), "correct token accepted")
	assert.Error(t, a.Authenticate(context.Background(), hdr("Bearer wrong")), "wrong token rejected")
	assert.Error(t, a.Authenticate(context.Background(), hdr("s3cret")), "missing Bearer prefix rejected")
	assert.Error(t, a.Authenticate(context.Background(), hdr("")), "missing header rejected")
}

// An empty configured token disables enforcement (local-dev convenience). This
// is intentional but dangerous, so it is pinned: callers rely on `enabled` to
// warn, and on every request being allowed through.
func TestOpenAuth(t *testing.T) {
	a, enabled := NewAuthenticator("")
	assert.False(t, enabled, "an empty token must report auth disabled")
	assert.NoError(t, a.Authenticate(context.Background(), http.Header{}), "open auth allows unauthenticated requests")
}
