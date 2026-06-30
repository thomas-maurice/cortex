package store

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A minted key must be unique, prefixed, and hash deterministically — the prefix
// is what the UI shows, and the hash is the only thing stored, so both must be
// stable and the raw key unguessable from what's persisted.
func TestMintAPIKey(t *testing.T) {
	raw1, hash1, prefix1, err := MintAPIKey()
	require.NoError(t, err)
	raw2, _, _, err := MintAPIKey()
	require.NoError(t, err)

	assert.NotEqual(t, raw1, raw2, "each minted key must be unique")
	assert.True(t, strings.HasPrefix(raw1, "ctx_"), "keys carry a recognizable prefix")
	assert.Equal(t, raw1[:apiKeyPrefixLen], prefix1, "stored prefix is the raw key's leading chars")
	assert.Equal(t, HashAPIKey(raw1), hash1, "the stored hash is the sha256 of the raw key")
	assert.NotContains(t, hash1, raw1[apiKeyPrefixLen:], "the secret tail must not be recoverable from the hash")
}

// HashAPIKey must be deterministic (same key → same hash, for O(1) lookup) and
// collision-free across different keys.
func TestHashAPIKeyDeterministic(t *testing.T) {
	assert.Equal(t, HashAPIKey("ctx_abc"), HashAPIKey("ctx_abc"))
	assert.NotEqual(t, HashAPIKey("ctx_abc"), HashAPIKey("ctx_abd"))
	assert.Len(t, HashAPIKey("ctx_abc"), 64, "sha256 hex is 64 chars")
}
