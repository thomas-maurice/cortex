package store

import (
	"strings"
	"testing"

	"github.com/alexedwards/argon2id"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Password hashing must use Parallelism=1 (predictable single-core cost, small
// DoS blast radius) over a 64 MiB, 2-iteration argon2id — and the produced hash
// must ENCODE those params, so verification cost is portable across machines and
// independent of the library's NumCPU-based default.
func TestPasswordParams(t *testing.T) {
	p := PasswordParams()
	assert.EqualValues(t, 1, p.Parallelism, "p must be pinned to 1, not the library's NumCPU default")
	assert.EqualValues(t, 2, p.Iterations)
	assert.EqualValues(t, 64*1024, p.Memory, "64 MiB memory cost keeps it GPU-hostile")

	h, err := argon2id.CreateHash("pw", p)
	require.NoError(t, err)
	assert.Contains(t, h, "m=65536,t=2,p=1", "the hash must encode the pinned cost parameters")
}

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

// IsPasswordHash must recognise the same hash formats the login handler's
// verifyPassword accepts (argon2id + bcrypt), and must treat a plaintext password
// as NOT a hash — otherwise the bootstrap would store a plaintext verbatim and
// login could never match it.
func TestIsPasswordHash(t *testing.T) {
	assert.True(t, IsPasswordHash("$argon2id$v=19$m=65536,t=1,p=10$abc$def"))
	assert.True(t, IsPasswordHash("$2a$10$abcdefghijklmnopqrstuv"))
	assert.True(t, IsPasswordHash("$2b$12$abc"))
	assert.True(t, IsPasswordHash("$2y$10$abc"))
	assert.False(t, IsPasswordHash("just-a-plaintext-password"))
	assert.False(t, IsPasswordHash("admin123"))
	assert.False(t, IsPasswordHash(""))
}

// HashAPIKey must be deterministic (same key → same hash, for O(1) lookup) and
// collision-free across different keys.
func TestHashAPIKeyDeterministic(t *testing.T) {
	assert.Equal(t, HashAPIKey("ctx_abc"), HashAPIKey("ctx_abc"))
	assert.NotEqual(t, HashAPIKey("ctx_abc"), HashAPIKey("ctx_abd"))
	assert.Len(t, HashAPIKey("ctx_abc"), 64, "sha256 hex is 64 chars")
}
