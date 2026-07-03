package store

// settings.go stores system-level configuration (currently S3 backup target
// config) in Weaviate as a single, non-MT, non-vectorised class. The S3 config
// lives in one object with a fixed deterministic UUID derived from its key name;
// there is no collection or iteration — it is a one-row config table.
//
// The secret_key field is stored server-side only; GetS3Config never returns it
// to callers — that is enforced in the RPC layer (see internal/rpc/s3.go).

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/weaviate/weaviate/entities/models"
)

const settingsClassName = "CortexSettings"

// s3ConfigID is the fixed, deterministic Weaviate object id for the S3 config.
// Computed once from a stable namespace + key so it survives server restarts.
var s3ConfigID = uuid.NewSHA1(uuid.NameSpaceURL, []byte("cortex/settings:s3config")).String()

// StoredS3Config is the S3 backup target configuration persisted in Weaviate.
// SecretKey is stored verbatim (the class is server-internal and never scanned
// by the user-facing search path). MergeS3Secret enforces the write-only
// invariant: a SetS3Config with an empty SecretKey preserves the existing one.
type StoredS3Config struct {
	Enabled   bool   `json:"enabled"`
	Endpoint  string `json:"endpoint"`
	Region    string `json:"region"`
	Bucket    string `json:"bucket"`
	Prefix    string `json:"prefix"`
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"` // stored server-side only; never returned by GetS3Config RPC
	UseSsl    bool   `json:"useSsl"`
}

// MergeS3Secret implements the "empty new key = keep existing" invariant for
// the S3 credential. An empty newKey signals "no change"; a non-empty newKey
// replaces the existing secret. Exported for unit testing without a Weaviate
// connection — the caller (SetS3Config) uses it to apply the rule before
// persisting the merged config.
func MergeS3Secret(existing, newKey string) string {
	if newKey == "" {
		return existing
	}
	return newKey
}

func settingsClass() *models.Class {
	return &models.Class{
		Class:      settingsClassName,
		Vectorizer: "none",
		Properties: settingsProperties(),
	}
}

func settingsProperties() []*models.Property {
	// A single JSON blob property avoids schema churn when new config fields
	// are added — we bump the JSON struct, not the Weaviate class.
	return []*models.Property{
		{Name: "data", DataType: []string{"text"}},
	}
}

// EnsureSettingsSchema creates the CortexSettings class if absent. Called from
// EnsureSchema so it is always provisioned on boot, regardless of MT mode.
func (s *Store) EnsureSettingsSchema(ctx context.Context) error {
	return s.ensureClass(ctx, settingsClass(), settingsProperties())
}

// GetS3Config reads the stored S3 configuration.
// Returns (zero, false, nil) when no config has been saved yet.
func (s *Store) GetS3Config(ctx context.Context) (StoredS3Config, bool, error) {
	exists, err := s.client.Data().Checker().
		WithClassName(settingsClassName).WithID(s3ConfigID).Do(ctx)
	if err != nil {
		return StoredS3Config{}, false, fmt.Errorf("check s3 config: %w", err)
	}
	if !exists {
		return StoredS3Config{}, false, nil
	}
	objs, err := s.client.Data().ObjectsGetter().
		WithClassName(settingsClassName).WithID(s3ConfigID).Do(ctx)
	if err != nil {
		return StoredS3Config{}, false, fmt.Errorf("get s3 config: %w", err)
	}
	if len(objs) == 0 {
		return StoredS3Config{}, false, nil
	}
	p, _ := objs[0].Properties.(map[string]interface{})
	raw := restString(p, "data")
	var cfg StoredS3Config
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return StoredS3Config{}, false, fmt.Errorf("decode s3 config: %w", err)
	}
	return cfg, true, nil
}

// SetS3Config stores the S3 configuration. If cfg.SecretKey == "" the existing
// stored secret is preserved (MergeS3Secret rule). The whole object is replaced
// on each call (idempotent, single-row table).
func (s *Store) SetS3Config(ctx context.Context, cfg StoredS3Config) error {
	// Apply the keep-existing rule before touching Weaviate.
	existing, _, err := s.GetS3Config(ctx)
	if err != nil {
		return fmt.Errorf("load existing s3 config for merge: %w", err)
	}
	cfg.SecretKey = MergeS3Secret(existing.SecretKey, cfg.SecretKey)

	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("encode s3 config: %w", err)
	}
	props := map[string]interface{}{"data": string(data)}

	// create-or-replace: attempt a create; on failure fall through to a replace.
	if _, err := s.client.Data().Creator().
		WithClassName(settingsClassName).WithID(s3ConfigID).WithProperties(props).Do(ctx); err != nil {
		if uerr := s.client.Data().Updater().
			WithClassName(settingsClassName).WithID(s3ConfigID).WithProperties(props).Do(ctx); uerr != nil {
			return fmt.Errorf("store s3 config (create: %v): %w", err, uerr)
		}
	}
	return nil
}
