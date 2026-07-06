package rpc

// s3_test.go covers the S3 upload helper. S3 is configured purely from the
// server environment (see cmd/server's s3ConfigFromEnv) and there are no S3-config
// RPCs, so the only server-side behavior worth pinning is that an upload failure
// degrades to a soft result string and never panics or aborts a backup.

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// A failed upload must still return a non-empty "s3 ..." result string (never
// panic, never report success) — a broken offsite target has to degrade to a
// logged warning, never take down a local backup.
func TestUploadToS3ReportsFailureSoftly(t *testing.T) {
	cfg := S3Config{
		Enabled:   true,
		Endpoint:  "127.0.0.1:1", // nothing listening / file missing anyway
		Bucket:    "b",
		AccessKey: "k",
		SecretKey: "s",
	}
	res := uploadToS3(context.Background(), cfg, "/nonexistent/cortex-full-backup-x.json")
	assert.NotEmpty(t, res, "a failed upload must still return a result string")
	assert.True(t, strings.HasPrefix(res, "s3 "),
		"a failure must be reported as an s3 error description, got %q", res)
	assert.False(t, strings.HasPrefix(res, "uploaded"), "a failure must not report success")
}

// The zero value must be inert: disabled, so runFullBackup skips the offsite step
// entirely when S3 is not configured via the environment.
func TestS3ConfigZeroValueDisabled(t *testing.T) {
	assert.False(t, S3Config{}.Enabled)
}
