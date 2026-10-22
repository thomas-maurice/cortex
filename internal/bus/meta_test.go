package bus_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/bus"
	"github.com/thomas-maurice/cortex/internal/natstest"
)

// The epoch is set ONCE: a restart (or second replica) must read the original
// instant, never move it forward (which would hide real usage history from the
// "tracked since" notice and the unused-reference computation).
func TestEnsureUsageTrackingSinceSetOnce(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := t.Context()
	first := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	got, err := bus.EnsureUsageTrackingSince(ctx, js, first)
	require.NoError(t, err)
	assert.True(t, first.Equal(got))

	got, err = bus.EnsureUsageTrackingSince(ctx, js, first.Add(48*time.Hour))
	require.NoError(t, err)
	assert.True(t, first.Equal(got), "existing value must win")
}
