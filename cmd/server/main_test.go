package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestS3ConfigFromEnv pins the env-var -> S3-config mapping used to seed a
// default backup target. The rules that matter (and could silently regress):
// both credentials are required, uploads only enable when a bucket exists, and
// region falls back AWS_DEFAULT_REGION -> AWS_REGION -> "us-east-1".
func TestS3ConfigFromEnv(t *testing.T) {
	// Clear every var the function reads so each subtest starts from a known
	// blank environment regardless of the host's real AWS_* vars.
	clearEnv := func(t *testing.T) {
		for _, k := range []string{
			"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION",
			"AWS_REGION", "CORTEX_S3_ENDPOINT", "CORTEX_S3_BUCKET",
			"CORTEX_S3_PREFIX", "CORTEX_S3_USE_SSL", "CORTEX_S3_ENABLED",
		} {
			t.Setenv(k, "")
		}
	}

	t.Run("no credentials is a no-op", func(t *testing.T) {
		clearEnv(t)
		_, ok := s3ConfigFromEnv()
		assert.False(t, ok, "must not seed without credentials")
	})

	t.Run("one credential alone is not enough", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("AWS_ACCESS_KEY_ID", "AKIA")
		_, ok := s3ConfigFromEnv()
		assert.False(t, ok, "both access and secret key are required")
	})

	t.Run("credentials plus bucket seed an enabled config with defaults", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("AWS_ACCESS_KEY_ID", "AKIA")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
		t.Setenv("CORTEX_S3_BUCKET", "cortex-backups")
		cfg, ok := s3ConfigFromEnv()
		require.True(t, ok)
		assert.True(t, cfg.Enabled, "bucket present + default enabled")
		assert.Equal(t, "s3.amazonaws.com", cfg.Endpoint)
		assert.Equal(t, "us-east-1", cfg.Region)
		assert.Equal(t, "cortex-backups", cfg.Bucket)
		assert.Equal(t, "AKIA", cfg.AccessKey)
		assert.Equal(t, "secret", cfg.SecretKey)
		assert.True(t, cfg.UseSsl)
	})

	t.Run("no bucket forces uploads off even with credentials", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("AWS_ACCESS_KEY_ID", "AKIA")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
		cfg, ok := s3ConfigFromEnv()
		require.True(t, ok)
		assert.False(t, cfg.Enabled, "nowhere to upload -> not enabled")
	})

	t.Run("region falls back to AWS_REGION and overrides apply", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("AWS_ACCESS_KEY_ID", "AKIA")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
		t.Setenv("AWS_REGION", "eu-west-3")
		t.Setenv("CORTEX_S3_ENDPOINT", "minio.lan:9000")
		t.Setenv("CORTEX_S3_BUCKET", "b")
		t.Setenv("CORTEX_S3_USE_SSL", "false")
		t.Setenv("CORTEX_S3_ENABLED", "false")
		cfg, ok := s3ConfigFromEnv()
		require.True(t, ok)
		assert.Equal(t, "eu-west-3", cfg.Region)
		assert.Equal(t, "minio.lan:9000", cfg.Endpoint)
		assert.False(t, cfg.UseSsl)
		assert.False(t, cfg.Enabled, "explicitly disabled despite a bucket")
	})
}
