package store

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A bare 422 may only become "not found" on positive confirmation that the
// tenant is absent. A present tenant (e.g. inactive/offloaded) or a failed
// lookup must keep the original error loud, or callers like the link worker
// would silently drop work.
func TestTenantAbsent(t *testing.T) {
	ctx := context.Background()

	absent, err := tenantAbsent(ctx, func(context.Context) (bool, error) { return false, nil })
	require.NoError(t, err)
	assert.True(t, absent, "tenant does not exist -> absent")

	absent, err = tenantAbsent(ctx, func(context.Context) (bool, error) { return true, nil })
	require.NoError(t, err)
	assert.False(t, absent, "tenant exists (any activity status) -> not absent")

	boom := errors.New("boom")
	absent, err = tenantAbsent(ctx, func(context.Context) (bool, error) { return false, boom })
	require.ErrorIs(t, err, boom)
	assert.False(t, absent, "failed check must never read as absent")
}
