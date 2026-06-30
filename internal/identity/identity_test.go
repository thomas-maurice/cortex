package identity

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The context round-trip is the seam every tenant-scoped op depends on: what the
// auth interceptor puts in must come back out intact, and an unauthenticated
// context must report absence (so callers refuse rather than guess a tenant).
func TestIdentityContextRoundTrip(t *testing.T) {
	got, ok := From(context.Background())
	assert.False(t, ok, "an empty context must report no identity")
	assert.Equal(t, Identity{}, got)

	want := Identity{UserID: "u-123", Username: "alice", Role: RoleAdmin}
	ctx := Into(context.Background(), want)
	got, ok = From(ctx)
	assert.True(t, ok)
	assert.Equal(t, want, got)
	assert.True(t, got.IsAdmin())
}

func TestIsAdmin(t *testing.T) {
	assert.True(t, Identity{Role: RoleAdmin}.IsAdmin())
	assert.False(t, Identity{Role: RoleUser}.IsAdmin())
	assert.False(t, Identity{}.IsAdmin())
}
