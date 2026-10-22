package memory

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The accepted forms are what the MCP description and CLI help promise; anything
// else (zero, negative, absurd, unknown units) must be rejected, never guessed.
func TestParseExpiresIn(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"7d":    7 * 24 * time.Hour,
		"36h":   36 * time.Hour,
		"1d":    24 * time.Hour,
		" 14d ": 14 * 24 * time.Hour,
		"90m":   90 * time.Minute,
		"3650d": 3650 * 24 * time.Hour,
		"1h30m": 90 * time.Minute,
	} {
		got, err := ParseExpiresIn(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
	for _, bad := range []string{"", "  ", "d", "7", "abc", "0d", "0h", "-1d", "-36h", "3651d", "99999999999999d", "2w", "1.5d", "7 d"} {
		_, err := ParseExpiresIn(bad)
		assert.Error(t, err, "%q must be rejected", bad)
	}
}

func TestRecordExpired(t *testing.T) {
	now := time.Now()
	assert.False(t, Record{}.Expired(now), "no expiry never expires")
	assert.False(t, Record{ValidUntil: now.Add(time.Minute)}.Expired(now))
	assert.True(t, Record{ValidUntil: now.Add(-time.Minute)}.Expired(now))
	assert.True(t, Record{ValidUntil: now}.Expired(now), "valid_until is the first instant it is no longer true")
}
