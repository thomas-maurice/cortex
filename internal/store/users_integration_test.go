package store

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/identity"
)

// TestUserAndApiKeyCRUD exercises the identity registry end-to-end against a real
// Weaviate (env-gated like the other integration tests). It pins the contracts the
// auth layer depends on: a user round-trips by username AND id, a duplicate
// username is rejected, role/password updates stick, an API key is looked up by
// its hash, keys list per-owner, and deleting a user cascades its keys (no orphan
// credential survives an account deletion).
func TestUserAndApiKeyCRUD(t *testing.T) {
	rest := os.Getenv("CORTEX_TEST_WEAVIATE_REST")
	grpc := os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST and CORTEX_TEST_WEAVIATE_GRPC to run this integration test")
	}

	ctx := context.Background()
	st, err := New(rest, grpc)
	require.NoError(t, err)
	_ = st.DeleteClass(ctx)
	require.NoError(t, st.EnsureIdentitySchema(ctx))
	t.Cleanup(func() {
		_ = st.client.Schema().ClassDeleter().WithClassName("User").Do(ctx)
		_ = st.client.Schema().ClassDeleter().WithClassName("ApiKey").Do(ctx)
	})

	t.Run("create + lookup user; duplicate rejected", func(t *testing.T) {
		u, err := st.CreateUser(ctx, "alice", "s3cret-pw", identity.RoleAdmin)
		require.NoError(t, err)
		assert.Equal(t, "alice", u.Username)
		assert.Equal(t, identity.RoleAdmin, u.Role)
		assert.NotEmpty(t, u.PasswordHash)
		assert.NotEqual(t, "s3cret-pw", u.PasswordHash, "password must be hashed, never stored plaintext")

		byName, ok, err := st.GetUserByUsername(ctx, "alice")
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, u.ID, byName.ID)

		byID, ok, err := st.GetUserByID(ctx, u.ID)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, "alice", byID.Username)

		_, err = st.CreateUser(ctx, "alice", "another", identity.RoleUser)
		assert.ErrorIs(t, err, ErrUserExists, "a duplicate username must be rejected")
	})

	t.Run("CreateUserWithHash stores the hash verbatim (no double-hash)", func(t *testing.T) {
		// A realistic argon2id PHC hash (as CORTEX_UI_PASSWORD would hold).
		const phc = "$argon2id$v=19$m=65536,t=1,p=10$c29tZXNhbHQ$c29tZWhhc2hvdXRwdXR2YWx1ZQ"
		u, err := st.CreateUserWithHash(ctx, "hashed-admin", phc, identity.RoleAdmin)
		require.NoError(t, err)
		got, ok, err := st.GetUserByID(ctx, u.ID)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, phc, got.PasswordHash, "a pre-hashed password must be stored verbatim, never re-hashed")
		require.NoError(t, st.DeleteUser(ctx, u.ID))
	})

	t.Run("update role + password", func(t *testing.T) {
		u, _, err := st.GetUserByUsername(ctx, "alice")
		require.NoError(t, err)
		require.NoError(t, st.UpdateUserRole(ctx, u.ID, identity.RoleUser))
		require.NoError(t, st.SetUserPassword(ctx, u.ID, "new-pw"))

		got, ok, err := st.GetUserByID(ctx, u.ID)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, identity.RoleUser, got.Role)
		assert.NotEqual(t, u.PasswordHash, got.PasswordHash, "password hash must change")
	})

	t.Run("api key: create, lookup by hash, list per user", func(t *testing.T) {
		u, _, _ := st.GetUserByUsername(ctx, "alice")
		raw, key, err := st.CreateApiKey(ctx, u.ID, "laptop")
		require.NoError(t, err)
		assert.NotEmpty(t, raw)
		assert.Equal(t, u.ID, key.UserID)

		got, ok, err := st.GetApiKeyByHash(ctx, HashAPIKey(raw))
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, u.ID, got.UserID)
		assert.Equal(t, "laptop", got.Label)

		keys, err := st.ListApiKeysForUser(ctx, u.ID)
		require.NoError(t, err)
		assert.Len(t, keys, 1)

		_, ok, err = st.GetApiKeyByHash(ctx, HashAPIKey("ctx_nonexistent"))
		require.NoError(t, err)
		assert.False(t, ok, "an unknown key must not resolve")
	})

	t.Run("AddApiKeyRaw is idempotent (bootstrap key)", func(t *testing.T) {
		u, _, _ := st.GetUserByUsername(ctx, "alice")
		_, err := st.AddApiKeyRaw(ctx, u.ID, "bootstrap", "ctx_fixed_boot_key")
		require.NoError(t, err)
		_, err = st.AddApiKeyRaw(ctx, u.ID, "bootstrap", "ctx_fixed_boot_key")
		require.NoError(t, err, "re-adding the same raw key must upsert, not error or duplicate")
		got, ok, err := st.GetApiKeyByHash(ctx, HashAPIKey("ctx_fixed_boot_key"))
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, u.ID, got.UserID)
	})

	t.Run("DeleteUser cascades its api keys", func(t *testing.T) {
		u, _, _ := st.GetUserByUsername(ctx, "alice")
		require.NoError(t, st.DeleteUser(ctx, u.ID))

		_, ok, err := st.GetUserByID(ctx, u.ID)
		require.NoError(t, err)
		assert.False(t, ok, "user gone")

		keys, err := st.ListApiKeysForUser(ctx, u.ID)
		require.NoError(t, err)
		assert.Empty(t, keys, "deleting a user must delete its api keys — no orphan credentials")
	})
}
