package rpc

import (
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/alexedwards/argon2id"
	"golang.org/x/crypto/bcrypt"
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
// The single user/pass check is the deliberate #1 (single-user) implementation;
// swapping it for a user store is where #2 (multi-user) plugs in, with the JWT
// role claim already carried through.
func LoginHandler(mgr *JWTManager, user, pass, role string, log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if user == "" || pass == "" {
			http.Error(w, "web UI login is disabled (set CORTEX_UI_PASSWORD)", http.StatusServiceUnavailable)
			return
		}
		var req loginRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		userOK := subtle.ConstantTimeCompare([]byte(req.Username), []byte(user)) == 1
		passOK := verifyPassword(req.Password, pass)
		if !userOK || !passOK {
			log.Warn("ui login failed", "username", req.Username, "remote", r.RemoteAddr)
			http.Error(w, "invalid credentials", http.StatusUnauthorized)
			return
		}
		token, err := mgr.Issue(user, role)
		if err != nil {
			log.Error("issue token", "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(loginResponse{Token: token})
	})
}
