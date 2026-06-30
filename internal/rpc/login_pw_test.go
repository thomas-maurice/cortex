package rpc

import (
	"net/http"
	"testing"
	"time"

	"github.com/alexedwards/argon2id"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

func TestVerifyPasswordPlaintext(t *testing.T) {
	assert.True(t, verifyPassword("hunter2", "hunter2"))
	assert.False(t, verifyPassword("wrong", "hunter2"))
}

func TestVerifyPasswordArgon2id(t *testing.T) {
	hash, err := argon2id.CreateHash("hunter2", argon2id.DefaultParams)
	require.NoError(t, err)
	require.True(t, len(hash) > 0 && hash[0] == '$')

	assert.True(t, verifyPassword("hunter2", hash), "correct password must verify against the argon2id hash")
	assert.False(t, verifyPassword("wrong", hash))
	// The hash itself must not be accepted as the plaintext password.
	assert.False(t, verifyPassword(hash, hash))
}

func TestVerifyPasswordBcrypt(t *testing.T) {
	hash, err := bcrypt.GenerateFromPassword([]byte("hunter2"), bcrypt.MinCost)
	require.NoError(t, err)

	assert.True(t, verifyPassword("hunter2", string(hash)))
	assert.False(t, verifyPassword("wrong", string(hash)))
}

func TestLoginHandlerAcceptsArgon2Hash(t *testing.T) {
	hash, err := argon2id.CreateHash("s3cret", argon2id.DefaultParams)
	require.NoError(t, err)
	h := LoginHandler(NewJWTManager("secret", time.Hour), "admin", hash, "admin", testLogger(), false, nil)

	ok := postLogin(t, h, `{"username":"admin","password":"s3cret"}`)
	assert.Equal(t, http.StatusOK, ok.Code, "correct password should authenticate against the configured hash")

	bad := postLogin(t, h, `{"username":"admin","password":"nope"}`)
	assert.Equal(t, http.StatusUnauthorized, bad.Code)
}
