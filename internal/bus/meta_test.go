package bus_test

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/bus"
	"github.com/thomas-maurice/cortex/internal/natstest"
)

// The legacy epoch is what seeds CortexMeta; reading it must be side-effect free
// and report "absent" (not an error) on a fresh NATS, else a wiped NATS would
// block server start.
func TestLegacyUsageTrackingSince(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := t.Context()
	_, found, err := bus.LegacyUsageTrackingSince(ctx, js)
	require.NoError(t, err)
	assert.False(t, found, "no bucket")

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: bus.MetaBucket})
	require.NoError(t, err)
	_, found, err = bus.LegacyUsageTrackingSince(ctx, js)
	require.NoError(t, err)
	assert.False(t, found, "bucket without key")

	want := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	_, err = kv.Put(ctx, "usage_tracking_since", []byte(want.Format(time.RFC3339)))
	require.NoError(t, err)
	got, found, err := bus.LegacyUsageTrackingSince(ctx, js)
	require.NoError(t, err)
	assert.True(t, found)
	assert.True(t, want.Equal(got))
}
