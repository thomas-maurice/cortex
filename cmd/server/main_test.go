package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestInitialBackupDelay pins the restart-resilient scheduling rule: the first
// periodic backup after boot must be scheduled from the newest on-disk backup's
// age, NOT from process start. Without this, a server that restarts more often
// than the interval would keep resetting a ticker that never fires.
func TestInitialBackupDelay(t *testing.T) {
	const interval = 24 * time.Hour
	const startup = time.Minute
	now := time.Unix(1_000_000, 0)

	t.Run("no prior backup -> back up soon", func(t *testing.T) {
		assert.Equal(t, startup, initialBackupDelay(time.Time{}, false, interval, startup, now))
	})

	t.Run("overdue backup -> back up soon", func(t *testing.T) {
		last := now.Add(-25 * time.Hour) // older than interval
		assert.Equal(t, startup, initialBackupDelay(last, true, interval, startup, now))
	})

	t.Run("exactly at interval -> overdue, back up soon", func(t *testing.T) {
		last := now.Add(-interval)
		assert.Equal(t, startup, initialBackupDelay(last, true, interval, startup, now))
	})

	t.Run("recent backup -> wait only the remaining interval", func(t *testing.T) {
		last := now.Add(-6 * time.Hour) // 18h remaining
		assert.Equal(t, 18*time.Hour, initialBackupDelay(last, true, interval, startup, now))
	})
}

// TestS3ConfigFromEnv pins the env-var -> S3-config mapping. Credentials are held
// in memory only (never stored), so the mapping is the whole contract. Rules that
// matter and could silently regress: uploads only enable with BOTH credentials
// AND a bucket, CORTEX_S3_ENABLED can force them off, and region falls back
// AWS_DEFAULT_REGION -> AWS_REGION -> "us-east-1".
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

	t.Run("no credentials is disabled", func(t *testing.T) {
		clearEnv(t)
		assert.False(t, s3ConfigFromEnv().Enabled, "must not enable without credentials")
	})

	t.Run("one credential alone is not enough", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("AWS_ACCESS_KEY_ID", "AKIA")
		assert.False(t, s3ConfigFromEnv().Enabled, "both access and secret key are required")
	})

	t.Run("credentials plus bucket enable an upload config with defaults", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("AWS_ACCESS_KEY_ID", "AKIA")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
		t.Setenv("CORTEX_S3_BUCKET", "cortex-backups")
		cfg := s3ConfigFromEnv()
		assert.True(t, cfg.Enabled, "creds + bucket present + default enabled")
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
		assert.False(t, s3ConfigFromEnv().Enabled, "nowhere to upload -> not enabled")
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
		cfg := s3ConfigFromEnv()
		assert.Equal(t, "eu-west-3", cfg.Region)
		assert.Equal(t, "minio.lan:9000", cfg.Endpoint)
		assert.False(t, cfg.UseSsl)
		assert.False(t, cfg.Enabled, "explicitly disabled despite a bucket")
	})
}
