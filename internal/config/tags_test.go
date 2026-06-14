package config

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeTags(t *testing.T) {
	// Dedup must preserve base order first, then append only the novel extras —
	// so the auto save-tags never duplicate a tag the caller already supplied.
	got := MergeTags([]string{"a", "b"}, []string{"b", "c", "", "a"})
	assert.Equal(t, []string{"a", "b", "c"}, got)

	// Empty strings are dropped from both sides rather than stored as blank tags.
	assert.Equal(t, []string{"x"}, MergeTags([]string{"", "x"}, []string{""}))

	// Nil inputs are safe and yield an empty (non-nil) slice.
	assert.Empty(t, MergeTags(nil, nil))
}

func TestAutoTagsHostnameOptIn(t *testing.T) {
	// hostnameTag is opt-in: when false, only the static tags come back so an
	// unconfigured host never leaks its hostname into saved memories.
	assert.Equal(t, []string{"personal"}, AutoTags([]string{"personal"}, false))

	// When enabled, the host tag is appended with the documented prefix.
	host, err := os.Hostname()
	require.NoError(t, err)
	require.NotEmpty(t, host)
	got := AutoTags([]string{"work"}, true)
	require.Len(t, got, 2)
	assert.Equal(t, "work", got[0])
	assert.Equal(t, HostnameTagPrefix+host, got[1])
	assert.True(t, strings.HasPrefix(got[1], "host:"))
}
