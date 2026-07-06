package rpc

// s3.go holds the S3 offsite-backup configuration and the server-internal upload
// helper used by runFullBackup.
//
// Security model: S3 is configured SOLELY from the server environment (see
// cmd/server's s3ConfigFromEnv). The config — including the secret key — is held
// in memory for the process lifetime and is NEVER persisted to Weaviate, exposed
// over RPC, or included in a backup. There are deliberately no S3-config RPCs.
//
// S3 errors from uploadToS3 are NEVER propagated as RPC errors; they appear in
// BackupAllResponse.s3_result and are logged as warnings so a local backup is
// never lost due to an offsite upload failure.

import (
	"context"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"sort"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	s3UploadTimeout = 5 * time.Minute
	s3ListTimeout   = 30 * time.Second
)

// s3Object is a full-backup object found in the offsite bucket.
type s3Object struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// listS3Backups lists full-backup objects under the configured bucket/prefix,
// newest first. Only object keys whose base name matches the cortex-full-backup
// pattern are returned (reindex snapshots and unrelated objects are ignored).
func listS3Backups(ctx context.Context, cfg S3Config) ([]s3Object, error) {
	client, err := buildS3Client(cfg)
	if err != nil {
		return nil, fmt.Errorf("build s3 client: %w", err)
	}
	var out []s3Object
	for obj := range client.ListObjects(ctx, cfg.Bucket, minio.ListObjectsOptions{
		Prefix:    cfg.Prefix,
		Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("list s3 objects: %w", obj.Err)
		}
		if !backupFullPattern.MatchString(path.Base(obj.Key)) {
			continue
		}
		out = append(out, s3Object{Key: obj.Key, Size: obj.Size, LastModified: obj.LastModified})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LastModified.After(out[j].LastModified) })
	return out, nil
}

// downloadFromS3 fetches the full content of an object by key.
func downloadFromS3(ctx context.Context, cfg S3Config, key string) ([]byte, error) {
	client, err := buildS3Client(cfg)
	if err != nil {
		return nil, fmt.Errorf("build s3 client: %w", err)
	}
	obj, err := client.GetObject(ctx, cfg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get s3 object %q: %w", key, err)
	}
	defer func() { _ = obj.Close() }()
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("read s3 object %q: %w", key, err)
	}
	return data, nil
}

// S3Config is the offsite backup target, built from environment variables at
// startup. The zero value (Enabled=false) means "no offsite upload". Works with
// AWS S3 and any S3-compatible endpoint (MinIO, Garage, R2, ...).
type S3Config struct {
	Enabled   bool
	Endpoint  string
	Region    string
	Bucket    string
	Prefix    string
	AccessKey string
	SecretKey string
	UseSsl    bool
}

// buildS3Client constructs a minio client from the S3 configuration.
func buildS3Client(cfg S3Config) (*minio.Client, error) {
	creds := credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, "")
	return minio.New(cfg.Endpoint, &minio.Options{
		Creds:  creds,
		Secure: cfg.UseSsl,
		Region: cfg.Region,
	})
}

// uploadToS3 uploads the file at filePath to the configured S3 bucket. It
// returns a short result string for BackupAllResponse.s3_result:
//   - "uploaded s3://bucket/key" on success
//   - an error description on failure
//
// It never returns an error value — S3 failures are soft and must never abort a
// successfully written local backup. Callers log the result string (warn on
// failure, info on success).
func uploadToS3(ctx context.Context, cfg S3Config, filePath string) string {
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
