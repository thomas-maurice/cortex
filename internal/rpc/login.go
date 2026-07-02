package rpc

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/alexedwards/argon2id"
	"golang.org/x/crypto/bcrypt"

	"github.com/thomas-maurice/cortex/internal/identity"
	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/store"
)

// verifyPassword checks a submitted password against the configured value. The
// configured value may be a password HASH — detected by its PHC/crypt prefix —
// so the plaintext secret need not live in the environment/compose file:
//
//	$argon2id$...   argon2id (e.g. `vaultwarden hash`, or any PHC argon2 string)
//	$2a$/$2b$/$2y$  bcrypt
//
// Anything else is treated as a plaintext password and compared in constant time
// (backward compatible).
func verifyPassword(submitted, configured string) bool {
	switch {
	case strings.HasPrefix(configured, "$argon2"):
		ok, err := argon2id.ComparePasswordAndHash(submitted, configured)
		return err == nil && ok
	case strings.HasPrefix(configured, "$2a$"), strings.HasPrefix(configured, "$2b$"), strings.HasPrefix(configured, "$2y$"):
		return bcrypt.CompareHashAndPassword([]byte(configured), []byte(submitted)) == nil
	default:
		return subtle.ConstantTimeCompare([]byte(submitted), []byte(configured)) == 1
	}
}

// dummyPasswordHash is a real argon2id hash (with the store's password cost
// parameters) that the multi-tenant login path verifies against when the
// submitted username does not exist. Running a genuine hash on the not-found
// path keeps login latency independent of whether a username is valid, closing
// the enumeration oracle that a short-circuited "user missing → fast 401" would
// otherwise open. Built once at startup; a failure here is fatal by design.
var dummyPasswordHash = func() string {
	h, err := argon2id.CreateHash("cortex-nonexistent-user-sentinel", store.PasswordParams())
	if err != nil {
		panic("rpc: build dummy password hash: " + err.Error())
	}
	return h
}()

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	Token string `json:"token"`
}

// LoginHandler authenticates the single configured UI identity and returns a
// JWT. It is the only credential-checking endpoint; the Connect API trusts the
// JWT it mints (see jwtAuth). When user or pass is empty the UI login is
// disabled — the API is still usable by MCP/CLI via the static bearer token.
//
// When multiTenant is true and st is non-nil the handler looks the user up in
// the store and verifies their stored password hash — the real multi-user auth
// path. The JWT then carries userId + role + username.
// When multiTenant is false it falls back to the single configured
// CORTEX_UI_USER/PASSWORD, keeping single-user behaviour identical.
//
// lim, when non-nil, applies brute-force/DoS protection BEFORE any password hash
// runs: a per-IP rate limit, a per-username lockout, and a cap on concurrent
// hashes. A nil lim disables throttling (used by unit tests).
func LoginHandler(mgr *JWTManager, user, pass, role string, log *slog.Logger, multiTenant bool, st *store.Store, lim *LoginLimiter) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		remote := r.RemoteAddr
		if lim != nil {
			remote = clientIP(r, lim.cfg.TrustProxy)
			if !lim.allowIP(remote) {
				log.Warn("login rate-limited", "remote", remote)
				http.Error(w, "too many requests", http.StatusTooManyRequests)
				return
			}
		}

		var req loginRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		if lim != nil && lim.lockedOut(req.Username) {
			log.Warn("login locked out", "username", req.Username, "remote", remote)
			http.Error(w, "too many failed attempts, try again later", http.StatusTooManyRequests)
			return
		}

		// The password hash is the only expensive, memory-heavy step; run it under
		// a concurrency slot so a flood can't exhaust CPU/RAM. Shed load with 503
		// when every slot is busy rather than queueing another 64 MiB hash.
		if lim != nil {
			if !lim.tryAcquire() {
				http.Error(w, "server busy", http.StatusServiceUnavailable)
				return
			}
		}
		id, ok, disabled, srvErr := checkCredentials(r.Context(), req, multiTenant, st, user, pass, role, log)
		if lim != nil {
			lim.release()
		}

		switch {
		case srvErr != nil:
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		case disabled:
			http.Error(w, "web UI login is disabled (set CORTEX_UI_PASSWORD)", http.StatusServiceUnavailable)
			return
		case !ok:
			if lim != nil {
				lim.recordFailure(req.Username)
			}
			log.Warn("login failed", "username", req.Username, "remote", remote)
			http.Error(w, "invalid credentials", http.StatusUnauthorized)
			return
		}

		if lim != nil {
			lim.recordSuccess(req.Username)
		}
		token, err := mgr.Issue(id.UserID, id.Username, id.Role)
		if err != nil {
			log.Error("issue token", "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(loginResponse{Token: token})
	})
}

// checkCredentials performs the credential check for both auth modes — the
// expensive argon2 step — and returns the identity to mint a token for on
// success. disabled is true only in single-user mode with no password
// configured (UI login off). It ALWAYS runs a password hash, even for a missing
// multi-tenant user (against dummyPasswordHash), so response time does not reveal
// whether a username exists.
func checkCredentials(ctx context.Context, req loginRequest, multiTenant bool, st *store.Store, cfgUser, cfgPass, cfgRole string, log *slog.Logger) (id identity.Identity, ok, disabled bool, srvErr error) {
	if multiTenant && st != nil {
		u, found, err := st.GetUserByUsername(ctx, req.Username)
		if err != nil {
			log.Error("mt login: lookup user", "username", req.Username, "err", err)
			return identity.Identity{}, false, false, err
		}
		hash := dummyPasswordHash
		if found {
			hash = u.PasswordHash
		}
		pwOK := verifyPassword(req.Password, hash)
		if !found || !pwOK {
			return identity.Identity{}, false, false, nil
		}
		return identity.Identity{UserID: u.ID, Username: u.Username, Role: u.Role}, true, false, nil
	}

	// Single-user path (flag OFF).
	if cfgUser == "" || cfgPass == "" {
		return identity.Identity{}, false, true, nil
	}
	userOK := subtle.ConstantTimeCompare([]byte(req.Username), []byte(cfgUser)) == 1
	passOK := verifyPassword(req.Password, cfgPass)
	if !userOK || !passOK {
		return identity.Identity{}, false, false, nil
	}
	// Include the deterministic user-id for the bootstrap admin so the jwtAuth
	// interceptor resolves the same identity whether the flag is on or off.
	return identity.Identity{UserID: memory.UserID(cfgUser), Username: cfgUser, Role: cfgRole}, true, false, nil
}

// bootstrapIdentity returns the sentinel Identity for the bootstrap-admin /
// single-user (legacy token + dev-open) mode. username may be empty for the
// pure dev-open case; the UserID is the stable BootstrapTenant constant so
// P3's tenant resolution always has a valid anchor.
func bootstrapIdentity(username string) identity.Identity {
	userID := identity.BootstrapTenant
	if username != "" {
		userID = memory.UserID(username)
	}
	return identity.Identity{
		UserID:   userID,
		Username: username,
		Role:     identity.RoleAdmin,
	}
}
