package rpc

import (
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
// CORTEX_UI_USER/PASSWORD, keeping single-user behaviour byte-for-byte identical.
func LoginHandler(mgr *JWTManager, user, pass, role string, log *slog.Logger, multiTenant bool, st *store.Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req loginRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		if multiTenant && st != nil {
			// Multi-tenant path: look the user up in the identity store and verify
			// their argon2id hash. The JWT carries userId + username + role.
			u, found, err := st.GetUserByUsername(r.Context(), req.Username)
			if err != nil {
				log.Error("mt login: lookup user", "username", req.Username, "err", err)
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
			if !found || !verifyPassword(req.Password, u.PasswordHash) {
				log.Warn("mt login failed", "username", req.Username, "remote", r.RemoteAddr)
				http.Error(w, "invalid credentials", http.StatusUnauthorized)
				return
			}
			token, err := mgr.Issue(u.ID, u.Username, u.Role)
			if err != nil {
				log.Error("issue mt token", "err", err)
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(loginResponse{Token: token})
			return
		}

		// Single-user path (flag OFF): behaves exactly as before.
		if user == "" || pass == "" {
			http.Error(w, "web UI login is disabled (set CORTEX_UI_PASSWORD)", http.StatusServiceUnavailable)
			return
		}
		userOK := subtle.ConstantTimeCompare([]byte(req.Username), []byte(user)) == 1
		passOK := verifyPassword(req.Password, pass)
		if !userOK || !passOK {
			log.Warn("ui login failed", "username", req.Username, "remote", r.RemoteAddr)
			http.Error(w, "invalid credentials", http.StatusUnauthorized)
			return
		}
		// Single-user JWT: include the deterministic user-id for the bootstrap
		// admin so the jwtAuth interceptor resolves the same identity whether the
		// flag is on or off. memory.UserID is a deterministic hash of the username.
		token, err := mgr.Issue(memory.UserID(user), user, role)
		if err != nil {
			log.Error("issue token", "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(loginResponse{Token: token})
	})
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
