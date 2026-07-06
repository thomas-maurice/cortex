package rpc

// backup_test.go covers the full-server backup/restore handlers (BackupAll,
// ListBackups, RestoreAll) and their supporting functions.
//
// Tests are intentionally narrow: they use a nil store (or a temp-dir
// filesystem) so they run without Weaviate or NATS. The things verified here:
//   - Admin gate: a non-admin identity is rejected on all three handlers.
//   - Filename validation: traversal, separators, wrong prefix, valid cases.
//   - Prune logic: only cortex-full-backup-*.json files are deleted; reindex
//     snapshots (cortex-backup-*.json) and all other files survive.
//   - Envelope round-trip: marshal → unmarshal preserves tenants/users/records;
//     restore rejects a wrong version without touching the store.
//   - ListBackups: files returned newest-first.

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/store"
)

// ---- Admin-gating tests ----

// TestBackupAllAdminGate verifies BackupAll rejects a non-admin with
// PermissionDenied. The store is nil — requireAdmin fires before any store op.
func TestBackupAllAdminGate(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: true}}
	_, err := svc.BackupAll(userCtx("uid-bob", "bob"), connect.NewRequest(&cortexv1.BackupAllRequest{}))
	require.Error(t, err)
	var ce *connect.Error
	require.True(t, errors.As(err, &ce))
	assert.Equal(t, connect.CodePermissionDenied, ce.Code(),
		"non-admin must receive PermissionDenied for BackupAll — the admin gate is the first line of defense")
}

// TestListBackupsAdminGate verifies ListBackups rejects a non-admin.
func TestListBackupsAdminGate(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: true}}
	_, err := svc.ListBackups(userCtx("uid-bob", "bob"), connect.NewRequest(&cortexv1.ListBackupsRequest{}))
	require.Error(t, err)
	var ce *connect.Error
	require.True(t, errors.As(err, &ce))
	assert.Equal(t, connect.CodePermissionDenied, ce.Code(),
		"non-admin must receive PermissionDenied for ListBackups")
}

// TestRestoreAllAdminGate verifies RestoreAll rejects a non-admin.
// The valid-looking filename means the only thing between a non-admin and
// disaster-recovery data is this gate.
func TestRestoreAllAdminGate(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: true}}
	_, err := svc.RestoreAll(userCtx("uid-bob", "bob"),
		connect.NewRequest(&cortexv1.RestoreAllRequest{Name: "cortex-full-backup-20240101-120000.json"}))
	require.Error(t, err)
	var ce *connect.Error
	require.True(t, errors.As(err, &ce))
	assert.Equal(t, connect.CodePermissionDenied, ce.Code(),
		"non-admin must receive PermissionDenied for RestoreAll")
}

// ---- Filename validation tests ----

// TestValidateBackupFilename is a table-driven test for all the path-traversal
// and pattern checks that gate RestoreAll. A bypass here lets an attacker read
// arbitrary server-side files by naming a directory-traversal sequence.
func TestValidateBackupFilename(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"empty string", "", true},
		{"unix path separator", "foo/cortex-full-backup-20240101-120000.json", true},
		{"windows path separator", "foo\\cortex-full-backup-20240101-120000.json", true},
		{"dot-dot traversal", "../etc/passwd", true},
		{"dot-dot embedded", "cortex-full-backup-..20240101-120000.json", true},
		{"reindex snapshot wrong prefix", "cortex-backup-20240101-120000.json", true},
		{"wrong prefix entirely", "backup-20240101-120000.json", true},
		{"valid date format", "cortex-full-backup-20240101-120000.json", false},
		{"valid date another", "cortex-full-backup-20260703-235959.json", false},
		{"letters in date", "cortex-full-backup-20240101-12abcd.json", true},
		{"extra extension", "cortex-full-backup-20240101-120000.json.gz", true},
		{"missing json extension", "cortex-full-backup-20240101-120000", true},
		{"short date", "cortex-full-backup-2024010-120000.json", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateBackupFilename(tc.input)
			if tc.wantErr {
				assert.Error(t, err, "input %q should be rejected", tc.input)
			} else {
				assert.NoError(t, err, "input %q should be accepted", tc.input)
			}
		})
	}
}

// ---- Prune logic tests ----

// TestPruneBackupsKeepsNewestN verifies that pruneBackups:
//   - Keeps the newest N cortex-full-backup-*.json files.
//   - Deletes older cortex-full-backup-*.json files beyond the window.
//   - NEVER deletes reindex snapshots (cortex-backup-*.json).
//   - NEVER deletes unrelated files.
//
// Pinning the reindex-snapshot safety property is critical: the backup dir is
// shared between full-server backups and reindex safety snapshots; a prune that
// erases a reindex snapshot could make a class-rebuild non-recoverable.
func TestPruneBackupsKeepsNewestN(t *testing.T) {
	dir := t.TempDir()

	// Five full-server backup files; each gets a distinct mtime so sort is deterministic.
	fullNames := []string{
		"cortex-full-backup-20240101-120000.json", // oldest (i=0)
		"cortex-full-backup-20240102-120000.json",
		"cortex-full-backup-20240103-120000.json",
		"cortex-full-backup-20240104-120000.json",
		"cortex-full-backup-20240105-120000.json", // newest (i=4)
	}
	// A reindex safety snapshot and an unrelated file that must survive.
	reindexName := "cortex-backup-20240101-120000.json"
	otherName := "notes.txt"

	base := time.Now()
	for i, name := range fullNames {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, []byte("{}"), 0o644))
		mt := base.Add(time.Duration(i) * time.Minute)
		require.NoError(t, os.Chtimes(p, mt, mt))
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, reindexName), []byte("{}"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, otherName), []byte("data"), 0o644))

	// Prune to keep the 3 newest.
	require.NoError(t, pruneBackups(dir, 3))

	// The 3 newest full-backup files must still exist.
	for _, name := range fullNames[2:] { // indices 2, 3, 4 are the 3 newest
		_, err := os.Stat(filepath.Join(dir, name))
		assert.NoError(t, err, "newest backup %q must survive prune", name)
	}
	// The 2 oldest full-backup files must be deleted.
	for _, name := range fullNames[:2] {
		_, err := os.Stat(filepath.Join(dir, name))
		assert.True(t, os.IsNotExist(err),
			"oldest backup %q must be deleted after prune", name)
	}
	// Reindex snapshot must NEVER be deleted — it is the class-rebuild safety net.
	_, err := os.Stat(filepath.Join(dir, reindexName))
	assert.NoError(t, err, "reindex snapshot cortex-backup-*.json must never be pruned")
	// Unrelated file must be untouched.
	_, err = os.Stat(filepath.Join(dir, otherName))
	assert.NoError(t, err, "unrelated files must never be pruned")
}

// TestPruneBackupsNoOpWhenKeepZero verifies that keep <= 0 is a no-op.
func TestPruneBackupsNoOpWhenKeepZero(t *testing.T) {
	dir := t.TempDir()
	name := "cortex-full-backup-20240101-120000.json"
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("{}"), 0o644))

	require.NoError(t, pruneBackups(dir, 0))
	_, err := os.Stat(filepath.Join(dir, name))
	assert.NoError(t, err, "keep=0 must not delete any files")
}

// ---- Envelope round-trip tests ----

// TestBackupEnvelopeRoundTrip verifies the JSON envelope preserves all fields
// that matter for disaster recovery: tenant attribution, memory/summary records,
// user records with their password hashes (verbatim), and API key records.
func TestBackupEnvelopeRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	orig := backupEnvelope{
		Version:       backupVersion,
		CreatedAt:     now,
		ServerVersion: "0.0.13",
		MultiTenant:   true,
		Users: []store.User{
			{
				ID:           "uid-alice",
				Username:     "alice",
				PasswordHash: "$argon2id$v=19$m=65536,t=2,p=1$fakesalt$fakehash",
				Role:         "admin",
				CreatedAt:    now,
			},
		},
		ApiKeys: []store.ApiKey{
			{
				ID:      "key-1",
				KeyHash: "abc123deadbeef",
				UserID:  "uid-alice",
				Label:   "laptop",
				Prefix:  "ctx_abc123",
			},
		},
		Tenants: []tenantBackup{
			{
				Tenant: "uid-alice",
				Memories: []memory.Record{
					{ID: "mem-1", Text: "hello world", Namespace: "test"},
				},
				Summaries: []memory.Summary{
					{ConversationID: "conv-1", Text: "summary of session"},
				},
			},
		},
	}

	data, err := json.Marshal(orig)
	require.NoError(t, err)

	var got backupEnvelope
	require.NoError(t, json.Unmarshal(data, &got))

	assert.Equal(t, backupVersion, got.Version,
		"version must survive round-trip — RestoreAll checks it on import")
	assert.True(t, got.MultiTenant, "multi-tenant flag must be preserved")

	require.Len(t, got.Users, 1)
	assert.Equal(t, "alice", got.Users[0].Username)
	assert.Equal(t, "$argon2id$v=19$m=65536,t=2,p=1$fakesalt$fakehash",
		got.Users[0].PasswordHash,
		"password hash must survive verbatim — it is never re-hashed on restore")
	assert.Equal(t, "admin", got.Users[0].Role)

	require.Len(t, got.ApiKeys, 1)
	assert.Equal(t, "abc123deadbeef", got.ApiKeys[0].KeyHash,
		"API key hash must survive verbatim — it is the lookup key on auth")
	assert.Equal(t, "uid-alice", got.ApiKeys[0].UserID)

	require.Len(t, got.Tenants, 1)
	assert.Equal(t, "uid-alice", got.Tenants[0].Tenant)
	require.Len(t, got.Tenants[0].Memories, 1)
	assert.Equal(t, "mem-1", got.Tenants[0].Memories[0].ID)
	require.Len(t, got.Tenants[0].Summaries, 1)
	assert.Equal(t, "conv-1", got.Tenants[0].Summaries[0].ConversationID)
}

// TestRestoreAllRejectsWrongVersion verifies that RestoreAll refuses an envelope
// with an unrecognised version field. Without this check, a future incompatible
// format could be silently imported and corrupt the store.
func TestRestoreAllRejectsWrongVersion(t *testing.T) {
	dir := t.TempDir()
	name := "cortex-full-backup-20240101-120000.json"
	env := backupEnvelope{
		Version: 999, // unrecognised version
		Tenants: []tenantBackup{},
	}
	data, err := json.Marshal(env)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0o644))

	svc := &Service{cfg: Config{BackupDir: dir, MultiTenant: false}}
	_, err = svc.RestoreAll(adminCtx(), connect.NewRequest(&cortexv1.RestoreAllRequest{Name: name}))
	require.Error(t, err)
	var ce *connect.Error
	require.True(t, errors.As(err, &ce))
	assert.Equal(t, connect.CodeInvalidArgument, ce.Code(),
		"wrong version must be rejected with InvalidArgument before any store mutation")
}

// ---- ListBackups ordering test ----

// TestListBackupsNewestFirst verifies ListBackups sorts by mtime descending.
// Newest-first matters: operators listing backups for a restore typically want
// the most recent one at the top.
func TestListBackupsNewestFirst(t *testing.T) {
	dir := t.TempDir()
	names := []string{
		"cortex-full-backup-20240101-120000.json",
		"cortex-full-backup-20240102-120000.json",
		"cortex-full-backup-20240103-120000.json",
	}
	base := time.Now()
	for i, name := range names {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, []byte("{}"), 0o644))
		mt := base.Add(time.Duration(i) * time.Minute)
		require.NoError(t, os.Chtimes(p, mt, mt))
	}
	// A reindex snapshot must not appear in ListBackups results.
	reindexName := "cortex-backup-20240101-120000.json"
	require.NoError(t, os.WriteFile(filepath.Join(dir, reindexName), []byte("{}"), 0o644))

	svc := &Service{cfg: Config{BackupDir: dir, MultiTenant: false}}
	resp, err := svc.ListBackups(adminCtx(), connect.NewRequest(&cortexv1.ListBackupsRequest{}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Backups, 3, "reindex snapshot must not appear in ListBackups")

	// Verify newest-first order.
	assert.Equal(t, "cortex-full-backup-20240103-120000.json", resp.Msg.Backups[0].Name,
		"newest backup must be first")
	assert.Equal(t, "cortex-full-backup-20240101-120000.json", resp.Msg.Backups[2].Name,
		"oldest backup must be last")

	// Each entry must carry a size and a created_at.
	for _, b := range resp.Msg.Backups {
		assert.NotNil(t, b.CreatedAt, "created_at must be set")
		assert.NotZero(t, b.SizeBytes, "size_bytes must be non-zero")
	}
}

// TestLastBackupTime pins the input to the restart-resilient scheduler: it must
// return the NEWEST full-backup file's mtime (ignoring reindex snapshots), and
// report ok=false when there is no backup / no dir. A wrong answer here makes the
// scheduler either back up too eagerly or never at all across restarts.
func TestLastBackupTime(t *testing.T) {
	t.Run("returns newest full-backup mtime, ignoring reindex snapshots", func(t *testing.T) {
		dir := t.TempDir()
		base := time.Now().Truncate(time.Second)
		names := []string{
			"cortex-full-backup-20240101-120000.json",
			"cortex-full-backup-20240103-120000.json", // newest by mtime below
			"cortex-full-backup-20240102-120000.json",
		}
		for i, name := range names {
			p := filepath.Join(dir, name)
			require.NoError(t, os.WriteFile(p, []byte("{}"), 0o644))
			mt := base.Add(time.Duration(i) * time.Minute)
			require.NoError(t, os.Chtimes(p, mt, mt))
		}
		// A reindex snapshot, even if newest, must NOT be considered a full backup.
		reindex := filepath.Join(dir, "cortex-backup-20240104-120000.json")
		require.NoError(t, os.WriteFile(reindex, []byte("{}"), 0o644))
		future := base.Add(time.Hour)
		require.NoError(t, os.Chtimes(reindex, future, future))

		svc := &Service{cfg: Config{BackupDir: dir}}
		got, ok := svc.LastBackupTime()
		require.True(t, ok)
		// The newest full backup is the 3rd file written (i=2 → base+2m).
		assert.WithinDuration(t, base.Add(2*time.Minute), got, time.Second,
			"must return the newest full-backup mtime, not the reindex snapshot's")
	})

	t.Run("false when no backups exist", func(t *testing.T) {
		svc := &Service{cfg: Config{BackupDir: t.TempDir()}}
		_, ok := svc.LastBackupTime()
		assert.False(t, ok)
	})

	t.Run("false when dir is absent", func(t *testing.T) {
		svc := &Service{cfg: Config{BackupDir: filepath.Join(t.TempDir(), "nope")}}
		_, ok := svc.LastBackupTime()
		assert.False(t, ok)
	})
}

// TestListBackupsEmptyDirReturnsEmpty verifies ListBackups returns an empty
// response (not an error) when the backup directory does not yet exist.
func TestListBackupsEmptyDirReturnsEmpty(t *testing.T) {
	svc := &Service{cfg: Config{BackupDir: filepath.Join(t.TempDir(), "nonexistent"), MultiTenant: false}}
	resp, err := svc.ListBackups(adminCtx(), connect.NewRequest(&cortexv1.ListBackupsRequest{}))
	require.NoError(t, err)
	assert.Empty(t, resp.Msg.Backups)
}

// TestRestoreAllInvalidFilenameReturnsInvalidArgument verifies that RestoreAll
// returns InvalidArgument (not NotFound or internal error) for filenames that
// fail validation — important so the client knows the error is in the request.
func TestRestoreAllInvalidFilenameReturnsInvalidArgument(t *testing.T) {
	ctx := adminCtx()
	svc := &Service{cfg: Config{BackupDir: t.TempDir(), MultiTenant: false}}

	for _, badName := range []string{
		"",
		"../etc/passwd",
		"cortex-backup-20240101-120000.json",
		"/abs/path.json",
	} {
		_, err := svc.RestoreAll(ctx, connect.NewRequest(&cortexv1.RestoreAllRequest{Name: badName}))
		require.Error(t, err, "bad name %q must be rejected", badName)
		var ce *connect.Error
		require.True(t, errors.As(err, &ce), "error must be connect.Error for %q", badName)
		assert.Equal(t, connect.CodeInvalidArgument, ce.Code(),
			"invalid filename must return InvalidArgument, not NotFound or PermissionDenied")
	}
}

// TestRestoreAllSourceSelection pins the three-way source rule: exactly one of
// name/data/s3_key must be set, and an s3_key restore requires S3 to be
// configured — both caught before any store mutation.
func TestRestoreAllSourceSelection(t *testing.T) {
	ctx := adminCtx()

	t.Run("zero or multiple sources -> InvalidArgument", func(t *testing.T) {
		svc := &Service{cfg: Config{BackupDir: t.TempDir()}}
		reqs := []*cortexv1.RestoreAllRequest{
			{}, // none
			{Name: "cortex-full-backup-20240101-120000.json", S3Key: "k"},
			{Name: "cortex-full-backup-20240101-120000.json", Data: []byte("{}")},
			{Data: []byte("{}"), S3Key: "k"},
		}
		for _, r := range reqs {
			_, err := svc.RestoreAll(ctx, connect.NewRequest(r))
			require.Error(t, err)
			var ce *connect.Error
			require.True(t, errors.As(err, &ce))
			assert.Equal(t, connect.CodeInvalidArgument, ce.Code(),
				"exactly one of name/data/s3_key must be required")
		}
	})

	t.Run("s3_key without S3 configured -> FailedPrecondition", func(t *testing.T) {
		svc := &Service{cfg: Config{S3: S3Config{Enabled: false}}}
		_, err := svc.RestoreAll(ctx, connect.NewRequest(&cortexv1.RestoreAllRequest{S3Key: "cortex/backup.json"}))
		require.Error(t, err)
		var ce *connect.Error
		require.True(t, errors.As(err, &ce))
		assert.Equal(t, connect.CodeFailedPrecondition, ce.Code(),
			"restoring from S3 without S3 configured must be a clear precondition failure")
	})
}

// TestListS3BackupsGates verifies ListS3Backups is admin-gated and returns a
// clear precondition error (not a crash) when S3 is not configured.
func TestListS3BackupsGates(t *testing.T) {
	t.Run("non-admin rejected", func(t *testing.T) {
		svc := &Service{cfg: Config{MultiTenant: true}}
		_, err := svc.ListS3Backups(userCtx("uid-bob", "bob"), connect.NewRequest(&cortexv1.ListS3BackupsRequest{}))
		require.Error(t, err)
		var ce *connect.Error
		require.True(t, errors.As(err, &ce))
		assert.Equal(t, connect.CodePermissionDenied, ce.Code())
	})
	t.Run("S3 not configured -> FailedPrecondition", func(t *testing.T) {
		svc := &Service{cfg: Config{S3: S3Config{Enabled: false}}}
		_, err := svc.ListS3Backups(adminCtx(), connect.NewRequest(&cortexv1.ListS3BackupsRequest{}))
		require.Error(t, err)
		var ce *connect.Error
		require.True(t, errors.As(err, &ce))
		assert.Equal(t, connect.CodeFailedPrecondition, ce.Code())
	})
}

// ---- DownloadBackup and DeleteBackup tests ----

// TestDownloadBackupAdminGate verifies DownloadBackup rejects a non-admin.
func TestDownloadBackupAdminGate(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: true}}
	_, err := svc.DownloadBackup(userCtx("uid-bob", "bob"),
		connect.NewRequest(&cortexv1.DownloadBackupRequest{Name: "cortex-full-backup-20240101-120000.json"}))
	require.Error(t, err)
	var ce *connect.Error
	require.True(t, errors.As(err, &ce))
	assert.Equal(t, connect.CodePermissionDenied, ce.Code(),
		"DownloadBackup must be admin-gated")
}

// TestDeleteBackupAdminGate verifies DeleteBackup rejects a non-admin.
func TestDeleteBackupAdminGate(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: true}}
	_, err := svc.DeleteBackup(userCtx("uid-bob", "bob"),
		connect.NewRequest(&cortexv1.DeleteBackupRequest{Name: "cortex-full-backup-20240101-120000.json"}))
	require.Error(t, err)
	var ce *connect.Error
	require.True(t, errors.As(err, &ce))
	assert.Equal(t, connect.CodePermissionDenied, ce.Code(),
		"DeleteBackup must be admin-gated")
}

// TestDownloadBackupReturnsFileContents verifies that DownloadBackup reads the
// correct file and returns its contents.
func TestDownloadBackupReturnsFileContents(t *testing.T) {
	dir := t.TempDir()
	name := "cortex-full-backup-20240101-120000.json"
	content := []byte(`{"version":1,"tenants":[]}`)
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), content, 0o644))

	svc := &Service{cfg: Config{BackupDir: dir}}
	resp, err := svc.DownloadBackup(adminCtx(),
		connect.NewRequest(&cortexv1.DownloadBackupRequest{Name: name}))
	require.NoError(t, err)
	assert.Equal(t, name, resp.Msg.Name)
	assert.Equal(t, content, resp.Msg.Data,
		"DownloadBackup must return file contents verbatim")
}

// TestDownloadBackupNotFound verifies CodeNotFound when the file is absent.
func TestDownloadBackupNotFound(t *testing.T) {
	svc := &Service{cfg: Config{BackupDir: t.TempDir()}}
	_, err := svc.DownloadBackup(adminCtx(),
		connect.NewRequest(&cortexv1.DownloadBackupRequest{Name: "cortex-full-backup-20240101-120000.json"}))
	require.Error(t, err)
	var ce *connect.Error
	require.True(t, errors.As(err, &ce))
	assert.Equal(t, connect.CodeNotFound, ce.Code(),
		"missing backup file must return CodeNotFound")
}

// TestDeleteBackupRemovesFile verifies DeleteBackup removes the named file.
func TestDeleteBackupRemovesFile(t *testing.T) {
	dir := t.TempDir()
	name := "cortex-full-backup-20240101-120000.json"
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("{}"), 0o644))

	svc := &Service{cfg: Config{BackupDir: dir}}
	_, err := svc.DeleteBackup(adminCtx(),
		connect.NewRequest(&cortexv1.DeleteBackupRequest{Name: name}))
	require.NoError(t, err)

	_, statErr := os.Stat(filepath.Join(dir, name))
	assert.True(t, os.IsNotExist(statErr),
		"DeleteBackup must remove the named file from BackupDir")
}

// TestDeleteBackupNotFound verifies CodeNotFound when the file does not exist.
func TestDeleteBackupNotFound(t *testing.T) {
	svc := &Service{cfg: Config{BackupDir: t.TempDir()}}
	_, err := svc.DeleteBackup(adminCtx(),
		connect.NewRequest(&cortexv1.DeleteBackupRequest{Name: "cortex-full-backup-20240101-120000.json"}))
	require.Error(t, err)
	var ce *connect.Error
	require.True(t, errors.As(err, &ce))
	assert.Equal(t, connect.CodeNotFound, ce.Code(),
		"deleting a non-existent backup must return CodeNotFound")
}

// TestDeleteBackupInvalidFilename verifies CodeInvalidArgument for bad filenames.
func TestDeleteBackupInvalidFilename(t *testing.T) {
	svc := &Service{cfg: Config{BackupDir: t.TempDir()}}
	_, err := svc.DeleteBackup(adminCtx(),
		connect.NewRequest(&cortexv1.DeleteBackupRequest{Name: "../etc/passwd"}))
	require.Error(t, err)
	var ce *connect.Error
	require.True(t, errors.As(err, &ce))
	assert.Equal(t, connect.CodeInvalidArgument, ce.Code())
}

// ---- BackupSelf / RestoreSelf gate tests ----

// TestBackupSelfNotAdminGated verifies BackupSelf does NOT require admin.
// With MultiTenant=true and no identity on ctx, the first error comes from
// tenantStore (CodeUnauthenticated), NOT from requireAdmin (CodePermissionDenied).
// If the handler called requireAdmin, we would see PermissionDenied; we do not.
func TestBackupSelfNotAdminGated(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: true}}
	// Use a context with no identity — tenantStore fires before any store call.
	_, err := svc.BackupSelf(context.Background(),
		connect.NewRequest(&cortexv1.BackupSelfRequest{}))
	require.Error(t, err)
	var ce *connect.Error
	require.True(t, errors.As(err, &ce))
	assert.Equal(t, connect.CodeUnauthenticated, ce.Code(),
		"BackupSelf is not admin-gated: no-identity error must be Unauthenticated, not PermissionDenied")
}

// TestRestoreSelfNotAdminGated verifies RestoreSelf does NOT require admin.
// Same technique as TestBackupSelfNotAdminGated: no-identity in MT mode yields
// CodeUnauthenticated from tenantStore, not CodePermissionDenied from requireAdmin.
func TestRestoreSelfNotAdminGated(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: true}}
	env := backupEnvelope{Version: backupVersion, Tenants: []tenantBackup{}}
	data, err := json.Marshal(env)
	require.NoError(t, err)

	_, err = svc.RestoreSelf(context.Background(),
		connect.NewRequest(&cortexv1.RestoreSelfRequest{Data: data}))
	require.Error(t, err)
	var ce *connect.Error
	require.True(t, errors.As(err, &ce))
	assert.Equal(t, connect.CodeUnauthenticated, ce.Code(),
		"RestoreSelf is not admin-gated: no-identity error must be Unauthenticated, not PermissionDenied")
}

// TestRestoreSelfRejectsTooLarge verifies the 100 MiB cap is enforced before
// any parsing or authentication. The size check happens before tenantStore so
// this test does NOT need a valid envelope.
func TestRestoreSelfRejectsTooLarge(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: false}}
	oversized := make([]byte, maxRestoreSelfBytes+1)
	_, err := svc.RestoreSelf(context.Background(),
		connect.NewRequest(&cortexv1.RestoreSelfRequest{Data: oversized}))
	require.Error(t, err)
	var ce *connect.Error
	require.True(t, errors.As(err, &ce))
	assert.Equal(t, connect.CodeInvalidArgument, ce.Code(),
		"oversized RestoreSelf payload must be rejected with CodeInvalidArgument before any auth or parsing")
}

// TestRestoreSelfRejectsWrongVersion mirrors TestRestoreAllRejectsWrongVersion
// but for the self-service path. RestoreSelf parses/validates the version
// BEFORE calling tenantStore, so this test works without a live Weaviate
// connection (a nil store never gets called).
func TestRestoreSelfRejectsWrongVersion(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: true}}
	env := backupEnvelope{Version: 999, Tenants: []tenantBackup{}}
	data, err := json.Marshal(env)
	require.NoError(t, err)

	// context.Background() has no identity — but version check fires first
	// (before tenantStore) so there is no store access and no panic.
	_, err = svc.RestoreSelf(context.Background(),
		connect.NewRequest(&cortexv1.RestoreSelfRequest{Data: data}))
	require.Error(t, err)
	var ce *connect.Error
	require.True(t, errors.As(err, &ce))
	assert.Equal(t, connect.CodeInvalidArgument, ce.Code(),
		"RestoreSelf must reject an unrecognised backup version before any auth or store call")
}

// ---- RestoreAll name-XOR-data validation ----

// TestRestoreAllNameXORData verifies the "exactly one of name/data" validation.
// The checks happen after requireAdmin but before file I/O or Weaviate access,
// so the nil-store service pattern is safe for these cases.
func TestRestoreAllNameXORData(t *testing.T) {
	svc := &Service{cfg: Config{BackupDir: t.TempDir(), MultiTenant: false}}

	t.Run("both empty → InvalidArgument", func(t *testing.T) {
		_, err := svc.RestoreAll(adminCtx(),
			connect.NewRequest(&cortexv1.RestoreAllRequest{})) // name="" data=nil
		require.Error(t, err)
		var ce *connect.Error
		require.True(t, errors.As(err, &ce))
		assert.Equal(t, connect.CodeInvalidArgument, ce.Code(),
			"both name and data empty must return InvalidArgument")
	})

	t.Run("both set → InvalidArgument", func(t *testing.T) {
		_, err := svc.RestoreAll(adminCtx(), connect.NewRequest(&cortexv1.RestoreAllRequest{
			Name: "cortex-full-backup-20240101-120000.json",
			Data: []byte("{}"),
		}))
		require.Error(t, err)
		var ce *connect.Error
		require.True(t, errors.As(err, &ce))
		assert.Equal(t, connect.CodeInvalidArgument, ce.Code(),
			"providing both name and data must return InvalidArgument")
	})

	t.Run("data path: wrong version → InvalidArgument (not file error)", func(t *testing.T) {
		env := backupEnvelope{Version: 999, Tenants: []tenantBackup{}}
		data, merr := json.Marshal(env)
		require.NoError(t, merr)
		_, err := svc.RestoreAll(adminCtx(),
			connect.NewRequest(&cortexv1.RestoreAllRequest{Data: data}))
		require.Error(t, err)
		var ce *connect.Error
		require.True(t, errors.As(err, &ce))
		assert.Equal(t, connect.CodeInvalidArgument, ce.Code(),
			"data path must validate version the same as the name path")
	})
}

// Ensure the no-op context satisfies the compiler — context is required by the
// handler signatures but unused in the nil-store admin-gate tests above.
var _ context.Context = context.Background()
