package rpc

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Claims is the JWT payload issued to the web UI. UserID is the tenant id (=
// Weaviate tenant name) set by the multi-tenancy login path; in single-user
// mode it mirrors the bootstrap tenant. Username and Role were here first and
// stay so the UI and existing sessions keep working without a token refresh.
type Claims struct {
	UserID   string `json:"userId"`
	Username string `json:"username"`
	Role     string `json:"role"`
	jwt.RegisteredClaims
}

// JWTManager issues and validates the HS256 tokens the UI logs in for. The MCP
// server and CLI keep using the static bearer token; this is only the browser
// auth path.
type JWTManager struct {
	secret []byte
	ttl    time.Duration
}

// NewJWTManager returns a manager signing with secret for the given token TTL.
func NewJWTManager(secret string, ttl time.Duration) *JWTManager {
	return &JWTManager{secret: []byte(secret), ttl: ttl}
}

// Issue mints a signed token for the given identity. userID is the tenant id
// (empty string is fine for backward-compat callers; the interceptor falls back
// to the bootstrap identity when the claim is absent).
func (m *JWTManager) Issue(userID, username, role string) (string, error) {
	now := time.Now()
	claims := &Claims{
		UserID:   userID,
		Username: username,
		Role:     role,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   username,
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(m.ttl)),
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(m.secret)
}

// Parse validates a token's signature and expiry and returns its claims.
func (m *JWTManager) Parse(token string) (*Claims, error) {
	parsed, err := jwt.ParseWithClaims(token, &Claims{}, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method %q", t.Header["alg"])
		}
		return m.secret, nil
	})
	if err != nil {
		return nil, err
	}
	claims, ok := parsed.Claims.(*Claims)
	if !ok || !parsed.Valid {
		return nil, errors.New("invalid token")
	}
	return claims, nil
}
