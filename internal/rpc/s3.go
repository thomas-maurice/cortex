package rpc

// s3.go implements the S3-config RPCs (GetS3Config, SetS3Config, TestS3) and
// the server-internal S3 upload helper used by runFullBackup.
//
// Security invariants:
//   - secret_key is WRITE-ONLY: SetS3Config accepts it; GetS3Config never returns
//     it (secret_set bool signals whether one is stored). An empty secret_key on
//     SetS3Config means "keep the existing one" (MergeS3Secret rule).
//   - All three RPCs are admin-gated — S3 creds are server-wide and must not
//     leak to non-admin users.
//   - S3 errors from uploadToS3 are NEVER propagated as RPC errors; they appear
//     in BackupAllResponse.s3_result and are logged as warnings so a local backup
//     is never lost due to an offsite upload failure.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"connectrpc.com/connect"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/store"
)

const (
	s3UploadTimeout = 5 * time.Minute
	s3ProbeTimeout  = 60 * time.Second
)

// GetS3Config returns the stored S3 backup target configuration. The
// secret_key field is NEVER included in the response; secret_set indicates
// whether a secret is currently stored.
func (s *Service) GetS3Config(ctx context.Context, _ *connect.Request[cortexv1.GetS3ConfigRequest]) (*connect.Response[cortexv1.GetS3ConfigResponse], error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	cfg, ok, err := s.store.GetS3Config(ctx)
	if err != nil {
		return nil, fmt.Errorf("get s3 config: %w", err)
	}
	if !ok {
		// Not configured yet — return an empty struct so the client can render defaults.
		return connect.NewResponse(&cortexv1.GetS3ConfigResponse{
			Config: &cortexv1.S3Config{},
		}), nil
	}
	return connect.NewResponse(&cortexv1.GetS3ConfigResponse{
		Config: &cortexv1.S3Config{
			Enabled:   cfg.Enabled,
			Endpoint:  cfg.Endpoint,
			Region:    cfg.Region,
			Bucket:    cfg.Bucket,
			Prefix:    cfg.Prefix,
			AccessKey: cfg.AccessKey,
			// SecretKey is intentionally absent — it is write-only.
			UseSsl: cfg.UseSsl,
		},
		SecretSet: cfg.SecretKey != "",
	}), nil
}

// SetS3Config stores the S3 backup target configuration. An empty
// secret_key means "no change" — the existing stored secret is preserved
// (MergeS3Secret rule, enforced in store.SetS3Config).
func (s *Service) SetS3Config(ctx context.Context, req *connect.Request[cortexv1.SetS3ConfigRequest]) (*connect.Response[cortexv1.SetS3ConfigResponse], error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	c := req.Msg.GetConfig()
	if c == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("config must not be nil"))
	}
	cfg := store.StoredS3Config{
		Enabled:   c.GetEnabled(),
		Endpoint:  c.GetEndpoint(),
		Region:    c.GetRegion(),
		Bucket:    c.GetBucket(),
		Prefix:    c.GetPrefix(),
		AccessKey: c.GetAccessKey(),
		SecretKey: c.GetSecretKey(), // empty = keep existing (handled in store.SetS3Config)
		UseSsl:    c.GetUseSsl(),
	}
	if err := s.store.SetS3Config(ctx, cfg); err != nil {
		return nil, fmt.Errorf("set s3 config: %w", err)
	}
	return connect.NewResponse(&cortexv1.SetS3ConfigResponse{}), nil
}

// TestS3 probes the stored S3 configuration by writing and deleting a small
// test object. Returns ok=true when the round-trip succeeds. Never returns an
// RPC error for S3-side failures — the ok/message pair encodes the outcome.
func (s *Service) TestS3(ctx context.Context, _ *connect.Request[cortexv1.TestS3Request]) (*connect.Response[cortexv1.TestS3Response], error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	cfg, ok, err := s.store.GetS3Config(ctx)
	if err != nil {
		return nil, fmt.Errorf("get s3 config: %w", err)
	}
	if !ok || !cfg.Enabled {
		return connect.NewResponse(&cortexv1.TestS3Response{
			Ok: false, Message: "S3 not configured or not enabled",
		}), nil
	}
	probeCtx, cancel := context.WithTimeout(ctx, s3ProbeTimeout)
	defer cancel()
	isOk, msg := testS3Connection(probeCtx, cfg)
	return connect.NewResponse(&cortexv1.TestS3Response{Ok: isOk, Message: msg}), nil
}

// testS3Connection is a pure-function probe: write a small object and delete
// it. Separated from the handler so it is callable from runFullBackup's test
// helper and unit-testable. Returns (true, description) on success.
func testS3Connection(ctx context.Context, cfg store.StoredS3Config) (bool, string) {
	client, err := buildS3Client(cfg)
	if err != nil {
		return false, fmt.Sprintf("build client: %v", err)
	}
	probeKey := cfg.Prefix + ".cortex-s3-probe"
	content := []byte("cortex-s3-probe")
	if _, err := client.PutObject(ctx, cfg.Bucket, probeKey,
		bytes.NewReader(content), int64(len(content)),
		minio.PutObjectOptions{ContentType: "text/plain"}); err != nil {
		return false, fmt.Sprintf("probe write failed: %v", err)
	}
	if err := client.RemoveObject(ctx, cfg.Bucket, probeKey,
		minio.RemoveObjectOptions{}); err != nil {
		return false, fmt.Sprintf("probe cleanup failed: %v", err)
	}
	return true, fmt.Sprintf("S3 connection ok (s3://%s/%s)", cfg.Bucket, probeKey)
}

// buildS3Client constructs a minio client from the stored S3 configuration.
func buildS3Client(cfg store.StoredS3Config) (*minio.Client, error) {
	creds := credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, "")
	return minio.New(cfg.Endpoint, &minio.Options{
		Creds:  creds,
		Secure: cfg.UseSsl,
		Region: cfg.Region,
	})
}

// uploadToS3 uploads the file at filePath to the configured S3 bucket.
// It returns a short result string for BackupAllResponse.s3_result:
//   - "uploaded s3://bucket/key" on success
//   - an error description on failure
//
// It never returns an error value — S3 failures are soft and must never abort
// a successfully written local backup. Callers log the result string as
// appropriate (warn on failure, info on success).
func uploadToS3(ctx context.Context, cfg store.StoredS3Config, filePath string) string {
	client, err := buildS3Client(cfg)
	if err != nil {
		return fmt.Sprintf("s3 client error: %v", err)
	}
	key := cfg.Prefix + filepath.Base(filePath)
	if _, err := client.FPutObject(ctx, cfg.Bucket, key, filePath,
		minio.PutObjectOptions{ContentType: "application/json"}); err != nil {
		return fmt.Sprintf("s3 upload error: %v", err)
	}
	return fmt.Sprintf("uploaded s3://%s/%s", cfg.Bucket, key)
}
