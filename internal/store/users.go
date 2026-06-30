package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/alexedwards/argon2id"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/thomas-maurice/cortex/internal/identity"
	"github.com/thomas-maurice/cortex/internal/memory"
)

// ErrUserExists is returned by CreateUser when the username is already taken.
var ErrUserExists = errors.New("user already exists")

// User is an account in the multi-tenancy registry. PasswordHash is an argon2id
// PHC string (passwords are low-frequency, so a slow hash is right). ID doubles as
// the Weaviate tenant name for the user's memories.
type User struct {
	ID           string
	Username     string
	PasswordHash string
	Role         string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// ApiKey is a credential a user hands to the MCP server / CLI. Only KeyHash (a
// fast sha256 of the raw key — keys are high-entropy and hit every request) and
// Prefix (a non-secret identifier for the UI) are stored; the raw key is shown
// once at creation and never again.
type ApiKey struct {
	ID         string
	KeyHash    string
	UserID     string
	Label      string
	Prefix     string
	CreatedAt  time.Time
	LastUsedAt time.Time
}

// EnsureIdentitySchema creates the User and ApiKey registry classes if absent. It
// is called ONLY when multi-tenancy is enabled (the server gates it on
// CORTEX_MULTI_TENANT), so a single-user store stays schema-identical to before.
// Both classes are non-multi-tenant (they ARE the tenant directory) and never
// embedded (vectorizer none).
func (s *Store) EnsureIdentitySchema(ctx context.Context) error {
	if err := s.ensureClass(ctx, userClass(), userProperties()); err != nil {
		return err
	}
	return s.ensureClass(ctx, apiKeyClass(), apiKeyProperties())
}

func userClass() *models.Class {
	return &models.Class{
		Class:       memory.UserClassName,
		Description: "A user account in the Cortex multi-tenancy registry",
		Vectorizer:  "none",
		Properties:  userProperties(),
	}
}

func userProperties() []*models.Property {
	return []*models.Property{
		// username and role are exact-match keys → field tokenization (see memoryProperties).
		{Name: "username", DataType: []string{"text"}, Tokenization: "field"},
		{Name: "passwordHash", DataType: []string{"text"}},
		{Name: "role", DataType: []string{"text"}, Tokenization: "field"},
		{Name: "createdAt", DataType: []string{"date"}},
		{Name: "updatedAt", DataType: []string{"date"}},
	}
}

func apiKeyClass() *models.Class {
	return &models.Class{
		Class:       memory.ApiKeyClassName,
		Description: "An API key credential owned by a user",
		Vectorizer:  "none",
		Properties:  apiKeyProperties(),
	}
}

func apiKeyProperties() []*models.Property {
	return []*models.Property{
		// keyHash and userId are exact-match lookups; prefix is filtered too.
		{Name: "keyHash", DataType: []string{"text"}, Tokenization: "field"},
		{Name: "userId", DataType: []string{"text"}, Tokenization: "field"},
		{Name: "label", DataType: []string{"text"}},
		{Name: "prefix", DataType: []string{"text"}, Tokenization: "field"},
		{Name: "createdAt", DataType: []string{"date"}},
		{Name: "lastUsedAt", DataType: []string{"date"}},
	}
}

// ---- Users ----

// IsPasswordHash reports whether s is ALREADY a password hash (argon2id PHC or
// bcrypt), matching the detection in the login handler's verifyPassword. The
// bootstrap uses it to decide whether to store a value directly or hash a
// plaintext — so a pre-hashed CORTEX_UI_PASSWORD / CORTEX_BOOTSTRAP_PASSWORD is
// stored as-is rather than hashed a second time (which would make login fail).
func IsPasswordHash(s string) bool {
	return strings.HasPrefix(s, "$argon2") ||
		strings.HasPrefix(s, "$2a$") || strings.HasPrefix(s, "$2b$") || strings.HasPrefix(s, "$2y$")
}

// CreateUser creates a user with an argon2id-hashed password (the password is
// PLAINTEXT and hashed here). It rejects a duplicate username (ErrUserExists) —
// the deterministic object id makes the collision detectable. role defaults to
// "user" when empty.
func (s *Store) CreateUser(ctx context.Context, username, password, role string) (User, error) {
	if password == "" {
		return User{}, errors.New("password must not be empty")
	}
	hash, err := argon2id.CreateHash(password, argon2id.DefaultParams)
	if err != nil {
		return User{}, fmt.Errorf("hash password: %w", err)
	}
	return s.createUser(ctx, username, hash, role)
}

// CreateUserWithHash creates a user whose password is ALREADY hashed (a PHC
// argon2id/bcrypt string). It stores the hash verbatim — used by the bootstrap so
// a pre-hashed admin password works (login then verifies the original plaintext
// against it). Use IsPasswordHash to choose between this and CreateUser.
func (s *Store) CreateUserWithHash(ctx context.Context, username, passwordHash, role string) (User, error) {
	if passwordHash == "" {
		return User{}, errors.New("password hash must not be empty")
	}
	return s.createUser(ctx, username, passwordHash, role)
}

// createUser writes a user record with an already-prepared password hash.
func (s *Store) createUser(ctx context.Context, username, passwordHash, role string) (User, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return User{}, errors.New("username must not be empty")
	}
	if role == "" {
		role = identity.RoleUser
	}
	id := memory.UserID(username)
	exists, err := s.client.Data().Checker().WithClassName(memory.UserClassName).WithID(id).Do(ctx)
	if err != nil {
		return User{}, fmt.Errorf("check user: %w", err)
	}
	if exists {
		return User{}, ErrUserExists
	}
	now := time.Now().UTC()
	u := User{ID: id, Username: username, PasswordHash: passwordHash, Role: role, CreatedAt: now, UpdatedAt: now}
	props := map[string]interface{}{
		"username":     u.Username,
		"passwordHash": u.PasswordHash,
		"role":         u.Role,
		"createdAt":    now.Format(time.RFC3339),
		"updatedAt":    now.Format(time.RFC3339),
	}
	if _, err := s.client.Data().Creator().
		WithClassName(memory.UserClassName).WithID(id).WithProperties(props).Do(ctx); err != nil {
		return User{}, fmt.Errorf("create user: %w", err)
	}
	return u, nil
}

// GetUserByUsername resolves a user by username. found is false if absent. It is
// the login lookup; deterministic ids make it an O(1) get, not a search.
func (s *Store) GetUserByUsername(ctx context.Context, username string) (User, bool, error) {
	return s.GetUserByID(ctx, memory.UserID(strings.TrimSpace(username)))
}

// GetUserByID fetches a user by id (= tenant name). found is false if absent.
func (s *Store) GetUserByID(ctx context.Context, id string) (User, bool, error) {
	exists, err := s.client.Data().Checker().WithClassName(memory.UserClassName).WithID(id).Do(ctx)
	if err != nil {
		return User{}, false, fmt.Errorf("check user: %w", err)
	}
	if !exists {
		return User{}, false, nil
	}
	objs, err := s.client.Data().ObjectsGetter().WithClassName(memory.UserClassName).WithID(id).Do(ctx)
	if err != nil {
		return User{}, false, fmt.Errorf("get user: %w", err)
	}
	if len(objs) == 0 {
		return User{}, false, nil
	}
	return userFromREST(id, objs[0].Properties), true, nil
}

// ListUsers returns all users (admin view). Passwords are included as hashes only.
func (s *Store) ListUsers(ctx context.Context) ([]User, error) {
	res, err := s.client.Experimental().Search().
		WithCollection(memory.UserClassName).
		WithProperties("username", "passwordHash", "role", "createdAt", "updatedAt").
		WithMetadata(&graphql.Metadata{ID: true}).
		WithLimit(allCount).
		Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	out := make([]User, 0, len(res))
	for _, r := range res {
		out = append(out, userFromSearch(r))
	}
	return out, nil
}

// UpdateUserRole sets a user's role via a merge (PATCH).
func (s *Store) UpdateUserRole(ctx context.Context, id, role string) error {
	return s.mergeObject(ctx, memory.UserClassName, id, map[string]interface{}{
		"role":      role,
		"updatedAt": time.Now().UTC().Format(time.RFC3339),
	})
}

// SetUserPassword re-hashes and stores a new password via a merge (PATCH).
func (s *Store) SetUserPassword(ctx context.Context, id, password string) error {
	if password == "" {
		return errors.New("password must not be empty")
	}
	hash, err := argon2id.CreateHash(password, argon2id.DefaultParams)
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}
	return s.mergeObject(ctx, memory.UserClassName, id, map[string]interface{}{
		"passwordHash": hash,
		"updatedAt":    time.Now().UTC().Format(time.RFC3339),
	})
}

// DeleteUser removes a user and cascades to ALL of their API keys, so no orphaned
// credential survives. The user's MEMORY tenant is dropped separately (P3, by the
// caller that has the tenant-aware store) — DeleteUser only owns the registry.
func (s *Store) DeleteUser(ctx context.Context, id string) error {
	if _, err := s.client.Batch().ObjectsBatchDeleter().
		WithClassName(memory.ApiKeyClassName).
		WithWhere(filters.Where().WithPath([]string{"userId"}).WithOperator(filters.Equal).WithValueText(id)).
		Do(ctx); err != nil {
		return fmt.Errorf("delete user's api keys: %w", err)
	}
	if err := s.client.Data().Deleter().WithClassName(memory.UserClassName).WithID(id).Do(ctx); err != nil {
		return fmt.Errorf("delete user: %w", err)
	}
	return nil
}

// ---- API keys ----

// HashAPIKey is the fast, exact-match hash used to store and look up an API key.
// Keys are high-entropy (we mint them), so a single sha256 is safe and O(1) — and
// it MUST stay fast because it runs on every authenticated request. (Passwords use
// argon2; keys do not.)
func HashAPIKey(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// apiKeyPrefixLen is how many leading chars of the raw key are stored as the
// non-secret prefix shown in the UI (enough to disambiguate, far too few to brute).
const apiKeyPrefixLen = 12

// MintAPIKey generates a new raw key and returns it with its hash and prefix. The
// raw key is "ctx_" + URL-safe base64 of 24 random bytes (~32 chars of entropy).
func MintAPIKey() (raw, keyHash, prefix string, err error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", "", "", fmt.Errorf("generate api key: %w", err)
	}
	raw = "ctx_" + base64.RawURLEncoding.EncodeToString(b)
	return raw, HashAPIKey(raw), raw[:apiKeyPrefixLen], nil
}

// CreateApiKey mints a NEW key for userId and stores it, returning the raw key
// (shown to the user exactly once) and the stored record (without the secret).
func (s *Store) CreateApiKey(ctx context.Context, userID, label string) (raw string, key ApiKey, err error) {
	raw, keyHash, prefix, err := MintAPIKey()
	if err != nil {
		return "", ApiKey{}, err
	}
	key, err = s.putApiKey(ctx, userID, label, keyHash, prefix)
	return raw, key, err
}

// AddApiKeyRaw stores a SPECIFIC raw key (the env bootstrap key, so existing MCP/CLI
// configs keep working). Idempotent: re-adding the same raw key upserts the same
// object (deterministic id from the hash).
func (s *Store) AddApiKeyRaw(ctx context.Context, userID, label, raw string) (ApiKey, error) {
	keyHash := HashAPIKey(raw)
	prefix := raw
	if len(prefix) > apiKeyPrefixLen {
		prefix = prefix[:apiKeyPrefixLen]
	}
	return s.putApiKey(ctx, userID, label, keyHash, prefix)
}

func (s *Store) putApiKey(ctx context.Context, userID, label, keyHash, prefix string) (ApiKey, error) {
	now := time.Now().UTC()
	id := memory.ApiKeyID(keyHash)
	k := ApiKey{ID: id, KeyHash: keyHash, UserID: userID, Label: label, Prefix: prefix, CreatedAt: now}
	props := map[string]interface{}{
		"keyHash":   keyHash,
		"userId":    userID,
		"label":     label,
		"prefix":    prefix,
		"createdAt": now.Format(time.RFC3339),
	}
	// create-or-replace (idempotent for the bootstrap key applied every boot).
	if _, err := s.client.Data().Creator().
		WithClassName(memory.ApiKeyClassName).WithID(id).WithProperties(props).Do(ctx); err != nil {
		if uerr := s.client.Data().Updater().
			WithClassName(memory.ApiKeyClassName).WithID(id).WithProperties(props).Do(ctx); uerr != nil {
			return ApiKey{}, fmt.Errorf("store api key (create failed: %v): %w", err, uerr)
		}
	}
	return k, nil
}

// GetApiKeyByHash resolves a key by its hash (the per-request auth lookup). found
// is false if absent.
func (s *Store) GetApiKeyByHash(ctx context.Context, keyHash string) (ApiKey, bool, error) {
	res, err := s.client.Experimental().Search().
		WithCollection(memory.ApiKeyClassName).
		WithProperties("keyHash", "userId", "label", "prefix", "createdAt", "lastUsedAt").
		WithMetadata(&graphql.Metadata{ID: true}).
		WithLimit(1).
		WithWhere(filters.Where().WithPath([]string{"keyHash"}).WithOperator(filters.Equal).WithValueText(keyHash)).
		Do(ctx)
	if err != nil {
		return ApiKey{}, false, fmt.Errorf("lookup api key: %w", err)
	}
	if len(res) == 0 {
		return ApiKey{}, false, nil
	}
	return apiKeyFromSearch(res[0]), true, nil
}

// ListApiKeysForUser returns the keys owned by userID (never the secret).
func (s *Store) ListApiKeysForUser(ctx context.Context, userID string) ([]ApiKey, error) {
	res, err := s.client.Experimental().Search().
		WithCollection(memory.ApiKeyClassName).
		WithProperties("keyHash", "userId", "label", "prefix", "createdAt", "lastUsedAt").
		WithMetadata(&graphql.Metadata{ID: true}).
		WithLimit(allCount).
		WithWhere(filters.Where().WithPath([]string{"userId"}).WithOperator(filters.Equal).WithValueText(userID)).
		Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("list api keys: %w", err)
	}
	out := make([]ApiKey, 0, len(res))
	for _, r := range res {
		out = append(out, apiKeyFromSearch(r))
	}
	return out, nil
}

// DeleteApiKey removes a key by its object id. Ownership (the key belongs to the
// caller) is enforced by the RPC layer before calling this.
func (s *Store) DeleteApiKey(ctx context.Context, id string) error {
	if err := s.client.Data().Deleter().WithClassName(memory.ApiKeyClassName).WithID(id).Do(ctx); err != nil {
		return fmt.Errorf("delete api key: %w", err)
	}
	return nil
}

// TouchApiKeyLastUsed stamps lastUsedAt via a merge (PATCH). The auth layer calls
// this DEBOUNCED (not every request) — see the api-key authenticator.
func (s *Store) TouchApiKeyLastUsed(ctx context.Context, id string, at time.Time) error {
	return s.mergeObject(ctx, memory.ApiKeyClassName, id, map[string]interface{}{
		"lastUsedAt": at.UTC().Format(time.RFC3339),
	})
}

// DropTenant removes a user's memory tenant from all three MT classes (Memory,
// MemoryChunk, ConversationSummary). It is called by the P5 DeleteUser RPC after
// cascading the api-key deletion. A missing tenant (never written to) is silently
// ignored so the delete is idempotent. Only meaningful when multi-tenancy is on;
// callers must check s.multiTenant before calling (the RPC layer does this).
func (s *Store) DropTenant(ctx context.Context, userID string) error {
	for _, class := range []string{memory.ClassName, memory.ChunkClassName, memory.SummaryClassName} {
		if err := s.client.Schema().TenantsDeleter().
			WithClassName(class).WithTenants(userID).Do(ctx); err != nil {
			// 404 / tenant-not-found means the user never had any data — no-op.
			if strings.Contains(err.Error(), "tenant not found") ||
				strings.Contains(err.Error(), "404") ||
				strings.Contains(err.Error(), "not found") {
				continue
			}
			return fmt.Errorf("drop tenant %s from %s: %w", userID, class, err)
		}
	}
	return nil
}

// mergeObject applies a partial property update (PATCH) leaving other properties
// (and any vector) untouched — the same primitive SetLinks/Reinforce use.
func (s *Store) mergeObject(ctx context.Context, className, id string, props map[string]interface{}) error {
	if err := s.client.Data().Updater().
		WithMerge().WithClassName(className).WithID(id).WithProperties(props).Do(ctx); err != nil {
		return fmt.Errorf("update %s/%s: %w", className, id, err)
	}
	return nil
}

// ---- decoders ----

func userFromREST(id string, raw models.PropertySchema) User {
	p, _ := raw.(map[string]interface{})
	u := User{
		ID:           id,
		Username:     restString(p, "username"),
		PasswordHash: restString(p, "passwordHash"),
		Role:         restString(p, "role"),
	}
	u.CreatedAt, _ = time.Parse(time.RFC3339, restString(p, "createdAt"))
	u.UpdatedAt, _ = time.Parse(time.RFC3339, restString(p, "updatedAt"))
	return u
}

func userFromSearch(r graphql.SearchResult) User {
	p := r.Properties
	u := User{
		ID:           r.ID,
		Username:     propString(p, "username"),
		PasswordHash: propString(p, "passwordHash"),
		Role:         propString(p, "role"),
	}
	u.CreatedAt, _ = time.Parse(time.RFC3339, propString(p, "createdAt"))
	u.UpdatedAt, _ = time.Parse(time.RFC3339, propString(p, "updatedAt"))
	return u
}

func apiKeyFromSearch(r graphql.SearchResult) ApiKey {
	p := r.Properties
	k := ApiKey{
		ID:      r.ID,
		KeyHash: propString(p, "keyHash"),
		UserID:  propString(p, "userId"),
		Label:   propString(p, "label"),
		Prefix:  propString(p, "prefix"),
	}
	k.CreatedAt, _ = time.Parse(time.RFC3339, propString(p, "createdAt"))
	if la := propString(p, "lastUsedAt"); la != "" {
		k.LastUsedAt, _ = time.Parse(time.RFC3339, la)
	}
	return k
}
