package rpc

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// LoginLimiterConfig tunes brute-force and DoS protection on the login endpoint.
// Every check it drives runs BEFORE the (expensive) argon2 password verification,
// so a throttled request never spends a hash — this defends against credential
// guessing AND the CPU/RAM exhaustion a login flood would otherwise cause (each
// argon2 verify costs ~64 MiB).
type LoginLimiterConfig struct {
	// PerIP is the sustained attempts/minute allowed from a single client IP.
	PerIP int
	// PerIPBurst is the momentary burst of attempts allowed above PerIP.
	PerIPBurst int
	// MaxFailures is the number of consecutive failed attempts against one
	// username before it is locked out for LockoutFor. A success clears the count.
	MaxFailures int
	// LockoutFor is how long a username stays locked after MaxFailures failures.
	LockoutFor time.Duration
	// MaxConcurrent caps how many password verifications run at once. It bounds
	// the CPU/RAM a login flood can consume regardless of the per-IP limit.
	MaxConcurrent int
	// TrustProxy makes the limiter derive the client IP from the right-most
	// X-Forwarded-For entry (the address our trusted reverse proxy observed)
	// instead of RemoteAddr. Enable ONLY when a trusted proxy sits in front —
	// otherwise a client can spoof the header to dodge per-IP limits.
	TrustProxy bool
}

// DefaultLoginLimiterConfig are conservative defaults for a small self-hosted
// instance exposed to the internet: 10 attempts/IP/min, lockout after 5 failed
// attempts per username, and at most 4 concurrent password hashes.
func DefaultLoginLimiterConfig() LoginLimiterConfig {
	return LoginLimiterConfig{
		PerIP:         10,
		PerIPBurst:    10,
		MaxFailures:   5,
		LockoutFor:    15 * time.Minute,
		MaxConcurrent: 4,
	}
}

// LoginLimiter enforces a LoginLimiterConfig. The zero value is not usable; build
// one with NewLoginLimiter. It is safe for concurrent use.
type LoginLimiter struct {
	cfg LoginLimiterConfig
	sem chan struct{}
	now func() time.Time // injectable clock for tests; defaults to time.Now

	mu    sync.Mutex
	ips   map[string]*rate.Limiter
	users map[string]*userFailure
}

// userFailure tracks consecutive login failures for one username and, once the
// threshold is crossed, the time until which the account is locked.
type userFailure struct {
	count       int
	lockedUntil time.Time
}

func NewLoginLimiter(cfg LoginLimiterConfig) *LoginLimiter {
	if cfg.MaxConcurrent < 1 {
		cfg.MaxConcurrent = 1
	}
	if cfg.PerIPBurst < 1 {
		cfg.PerIPBurst = 1
	}
	return &LoginLimiter{
		cfg:   cfg,
		sem:   make(chan struct{}, cfg.MaxConcurrent),
		now:   time.Now,
		ips:   make(map[string]*rate.Limiter),
		users: make(map[string]*userFailure),
	}
}

// allowIP reports whether an attempt from ip is within the per-IP rate budget,
// consuming one token when it is.
func (l *LoginLimiter) allowIP(ip string) bool {
	l.mu.Lock()
	lim, ok := l.ips[ip]
	if !ok {
		// PerIP is expressed per minute; rate.Limit is per second.
		lim = rate.NewLimiter(rate.Limit(float64(l.cfg.PerIP)/60.0), l.cfg.PerIPBurst)
		l.ips[ip] = lim
	}
	l.mu.Unlock()
	return lim.Allow()
}

// lockedOut reports whether username is currently locked due to repeated
// failures. Call it BEFORE running the password hash.
func (l *LoginLimiter) lockedOut(username string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	f, ok := l.users[username]
	if !ok {
		return false
	}
	return l.now().Before(f.lockedUntil)
}

// recordFailure increments username's failure counter and locks the account once
// it reaches MaxFailures. An expired lock resets the counter before counting.
func (l *LoginLimiter) recordFailure(username string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	f := l.users[username]
	if f == nil {
		f = &userFailure{}
		l.users[username] = f
	}
	if !f.lockedUntil.IsZero() && !l.now().Before(f.lockedUntil) {
		f.count = 0
		f.lockedUntil = time.Time{}
	}
	f.count++
	if f.count >= l.cfg.MaxFailures {
		f.lockedUntil = l.now().Add(l.cfg.LockoutFor)
	}
}

// recordSuccess clears all failure state for username on a successful login.
func (l *LoginLimiter) recordSuccess(username string) {
	l.mu.Lock()
	delete(l.users, username)
	l.mu.Unlock()
}

// tryAcquire reserves one of the MaxConcurrent hashing slots without blocking. It
// returns false if every slot is busy, in which case the caller should shed load
// (503) rather than pile another 64 MiB hash onto a saturated server. Pair a true
// return with exactly one release.
func (l *LoginLimiter) tryAcquire() bool {
	select {
	case l.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

// release returns a hashing slot taken by tryAcquire.
func (l *LoginLimiter) release() { <-l.sem }

// clientIP extracts the client address used for per-IP limiting. With TrustProxy
// it uses the right-most X-Forwarded-For entry — the address appended by the
// trusted proxy, which a client cannot forge past that hop; otherwise it uses the
// connection's RemoteAddr.
func clientIP(r *http.Request, trustProxy bool) string {
	if trustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			if last := strings.TrimSpace(parts[len(parts)-1]); last != "" {
				return last
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
