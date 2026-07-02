package rpc

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The per-IP limiter must let a burst through and then refuse further attempts —
// this is what caps a single-source credential-guessing flood.
func TestLoginLimiterPerIP(t *testing.T) {
	lim := NewLoginLimiter(LoginLimiterConfig{PerIP: 1, PerIPBurst: 3, MaxConcurrent: 1})
	for i := 0; i < 3; i++ {
		assert.True(t, lim.allowIP("10.0.0.1"), "the first PerIPBurst attempts must be allowed")
	}
	assert.False(t, lim.allowIP("10.0.0.1"), "attempts past the burst must be refused")
	// A different IP has its own budget — one attacker can't lock everyone out.
	assert.True(t, lim.allowIP("10.0.0.2"))
}

// Per-username lockout must trip after MaxFailures, clear when the window
// expires, and reset immediately on a successful login. Uses an injected clock so
// the expiry is deterministic (no sleeping).
func TestLoginLimiterUsernameLockout(t *testing.T) {
	lim := NewLoginLimiter(LoginLimiterConfig{MaxFailures: 3, LockoutFor: time.Minute, MaxConcurrent: 1, PerIP: 1000, PerIPBurst: 1000})
	now := time.Unix(1_000, 0)
	lim.now = func() time.Time { return now }

	for i := 0; i < 3; i++ {
		assert.False(t, lim.lockedOut("bob"), "must not lock before MaxFailures failures")
		lim.recordFailure("bob")
	}
	assert.True(t, lim.lockedOut("bob"), "must lock once MaxFailures consecutive failures are reached")

	now = now.Add(2 * time.Minute) // past LockoutFor
	assert.False(t, lim.lockedOut("bob"), "lock must clear after the window expires")

	// A single post-expiry failure must NOT immediately re-lock (counter reset).
	lim.recordFailure("bob")
	assert.False(t, lim.lockedOut("bob"))

	// A success wipes failure state entirely.
	lim.recordFailure("bob")
	lim.recordFailure("bob")
	lim.recordSuccess("bob")
	assert.False(t, lim.lockedOut("bob"), "a successful login must reset the failure counter")
}

// The concurrency cap must hand out exactly MaxConcurrent slots and refuse the
// next until one is released — this is the bound on how many 64 MiB argon2 hashes
// can run at once, i.e. the DoS ceiling.
func TestLoginLimiterConcurrencySlots(t *testing.T) {
	lim := NewLoginLimiter(LoginLimiterConfig{MaxConcurrent: 2, PerIPBurst: 1})
	assert.True(t, lim.tryAcquire())
	assert.True(t, lim.tryAcquire())
	assert.False(t, lim.tryAcquire(), "no slot may be handed out beyond MaxConcurrent")
	lim.release()
	assert.True(t, lim.tryAcquire(), "releasing a slot must free capacity")
}

// clientIP must default to the connection address, and only trust
// X-Forwarded-For when explicitly told to — otherwise a client could spoof the
// header to escape per-IP limits.
func TestClientIP(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/auth/login", nil)
	req.RemoteAddr = "203.0.113.9:5555"
	req.Header.Set("X-Forwarded-For", "1.2.3.4, 5.6.7.8")

	assert.Equal(t, "203.0.113.9", clientIP(req, false), "without TrustProxy, use the real connection IP, not XFF")
	assert.Equal(t, "5.6.7.8", clientIP(req, true), "with TrustProxy, use the right-most XFF entry (added by the trusted proxy)")

	// No XFF present → fall back to RemoteAddr even with TrustProxy on.
	bare := httptest.NewRequest(http.MethodPost, "/auth/login", nil)
	bare.RemoteAddr = "198.51.100.7:9"
	assert.Equal(t, "198.51.100.7", clientIP(bare, true))
}

// End-to-end through the handler (single-user path, no store needed): after
// MaxFailures bad passwords the account is locked and further attempts get 429
// WITHOUT reaching the password check — the brute-force defense.
func TestLoginHandlerLocksOutAfterFailures(t *testing.T) {
	lim := NewLoginLimiter(LoginLimiterConfig{MaxFailures: 3, LockoutFor: time.Minute, MaxConcurrent: 4, PerIP: 1000, PerIPBurst: 1000})
	h := LoginHandler(NewJWTManager("secret", time.Hour), "admin", "hunter2", "admin", testLogger(), false, nil, lim)

	for i := 0; i < 3; i++ {
		rr := postLogin(t, h, `{"username":"admin","password":"wrong"}`)
		require.Equal(t, http.StatusUnauthorized, rr.Code, "bad password returns 401 until the lockout trips")
	}
	locked := postLogin(t, h, `{"username":"admin","password":"wrong"}`)
	assert.Equal(t, http.StatusTooManyRequests, locked.Code, "once locked, attempts are refused with 429")

	// Even the CORRECT password is refused while locked — the lock gates before
	// the credential check.
	stillLocked := postLogin(t, h, `{"username":"admin","password":"hunter2"}`)
	assert.Equal(t, http.StatusTooManyRequests, stillLocked.Code)
}

// The per-IP rate limit must cut a flood off with 429 regardless of whether the
// credentials are valid — it gates before the password hash runs.
func TestLoginHandlerPerIPRateLimit(t *testing.T) {
	lim := NewLoginLimiter(LoginLimiterConfig{PerIP: 1, PerIPBurst: 2, MaxFailures: 1000, MaxConcurrent: 4})
	h := LoginHandler(NewJWTManager("secret", time.Hour), "admin", "hunter2", "admin", testLogger(), false, nil, lim)

	require.Equal(t, http.StatusUnauthorized, postLogin(t, h, `{"username":"admin","password":"x"}`).Code)
	require.Equal(t, http.StatusUnauthorized, postLogin(t, h, `{"username":"admin","password":"x"}`).Code)
	assert.Equal(t, http.StatusTooManyRequests, postLogin(t, h, `{"username":"admin","password":"x"}`).Code,
		"the third attempt from one IP exceeds PerIPBurst and is rate-limited")
}

// When every hashing slot is busy the handler must shed load with 503 rather than
// queue another memory-heavy hash — the DoS backstop.
func TestLoginHandlerShedsLoadWhenSaturated(t *testing.T) {
	lim := NewLoginLimiter(LoginLimiterConfig{PerIP: 1000, PerIPBurst: 1000, MaxFailures: 1000, MaxConcurrent: 1})
	require.True(t, lim.tryAcquire(), "occupy the only slot to simulate saturation")
	h := LoginHandler(NewJWTManager("secret", time.Hour), "admin", "hunter2", "admin", testLogger(), false, nil, lim)

	rr := postLogin(t, h, `{"username":"admin","password":"hunter2"}`)
	assert.Equal(t, http.StatusServiceUnavailable, rr.Code, "a saturated server returns 503, not a queued hash")
}

// The multi-tenant not-found path must verify against a REAL argon2 hash so that
// login latency does not reveal whether a username exists (enumeration oracle).
// We assert the sentinel is a genuine argon2id hash and rejects any guess.
func TestDummyPasswordHashClosesEnumerationOracle(t *testing.T) {
	assert.True(t, strings.HasPrefix(dummyPasswordHash, "$argon2id$"),
		"the not-found path must run a genuine argon2 verification, not a cheap string compare")
	// The sentinel matching its own hash is harmless — the not-found branch returns
	// 401 regardless of the hash result; the hash exists only to equalize timing.
	// What matters is that a real attacker guess does not verify.
	assert.False(t, verifyPassword("hunter2", dummyPasswordHash), "an attacker guess must not verify against the sentinel hash")
	assert.False(t, verifyPassword("", dummyPasswordHash))
}
