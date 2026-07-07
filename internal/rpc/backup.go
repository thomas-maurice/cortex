package rpc

// backup.go implements the backup/restore RPCs:
//
// Admin-gated (requireAdmin first):
//   - BackupAll — full-server snapshot to BackupDir; optional S3 offsite upload.
//   - ListBackups — lists cortex-full-backup-*.json files in BackupDir, newest first.
//   - RestoreAll — reads a named file OR raw bytes, validates format, recreates
//     users/keys (MT mode), re-queues memories and summaries onto NATS.
//   - DownloadBackup — returns the raw bytes of a named backup file.
//   - DeleteBackup — removes a named backup file (CodeNotFound when absent).
//
// Self-service (any authenticated user; no admin gate):
//   - BackupSelf — returns the caller's own tenant data as bytes (no disk write).
//   - RestoreSelf — re-queues uploaded bytes into the CALLER's own tenant, always
//     (envelope tenant attribution is ignored — a user must never write into
//     another tenant).
//
// Vectors are NOT included in any backup; the worker re-embeds on restore.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/bus"
	"github.com/thomas-maurice/cortex/internal/identity"
	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/store"
)

// backupVersion is the envelope format version. RestoreAll/RestoreSelf rejects
// envelopes with a different version so an incompatible format is never
// silently imported.
const backupVersion = 1

// maxRestoreSelfBytes is the maximum accepted byte size for RestoreSelf uploads.
// 100 MiB — a personal tenant backup is never legitimately this large.
const maxRestoreSelfBytes = 100 * 1024 * 1024

// maxBackupDownloadBytes is the maximum file size DownloadBackup will serve.
// 1 GiB — full-server backups grow with the tenant count but stay well under
// this in realistic deployments; exceeding it signals an unusual situation
// better handled out-of-band than via the RPC channel.
const maxBackupDownloadBytes = int64(1 << 30) // 1 GiB

// backupFullPattern matches the bare filename of a full-server backup file.
// Reindex safety snapshots use cortex-backup-*.json and must never be pruned.
var backupFullPattern = regexp.MustCompile(`^cortex-full-backup-\d{8}-\d{6}\.json$`)

// backupEnvelope is the versioned container written to disk by BackupAll.
type backupEnvelope struct {
	Version       int       `json:"version"`
	CreatedAt     time.Time `json:"createdAt"`
	ServerVersion string    `json:"serverVersion"`
	MultiTenant   bool      `json:"multiTenant"`
	// Users and ApiKeys are populated only in MT mode. Hashes are already one-way;
	// they are stored verbatim here for disaster recovery only and are NEVER sent
	// over the RPC wire — they live solely in the server-side file.
	Users   []store.User   `json:"users,omitempty"`
	ApiKeys []store.ApiKey `json:"apiKeys,omitempty"`
	// Tenants holds the per-tenant data. In non-MT mode there is exactly one entry
	// with Tenant == "" (the pseudo-tenant for a single-user store).
	Tenants []tenantBackup `json:"tenants"`
}

// tenantBackup holds all memories and summaries for one tenant.
type tenantBackup struct {
	Tenant    string           `json:"tenant"`
	Memories  []memory.Record  `json:"memories"`
	Summaries []memory.Summary `json:"summaries"`
}

// BackupAll is the admin-gated RPC handler. It delegates to runFullBackup and
// then prunes old backups.
func (s *Service) BackupAll(ctx context.Context, _ *connect.Request[cortexv1.BackupAllRequest]) (*connect.Response[cortexv1.BackupAllResponse], error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	resp, err := s.runFullBackup(ctx)
	if err != nil {
		return nil, fmt.Errorf("backup: %w", err)
	}
	if err := s.pruneFullBackups(); err != nil {
		s.log.Warn("backup: prune failed", "err", err)
	}
	return connect.NewResponse(resp), nil
}

// ListBackups returns the cortex-full-backup-*.json files in BackupDir, newest
// first, with size and mtime. Non-matching files (e.g. reindex snapshots) are
// excluded.
func (s *Service) ListBackups(ctx context.Context, _ *connect.Request[cortexv1.ListBackupsRequest]) (*connect.Response[cortexv1.ListBackupsResponse], error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	dir := s.backupDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return connect.NewResponse(&cortexv1.ListBackupsResponse{}), nil
		}
		return nil, fmt.Errorf("list backups: %w", err)
	}
	type entry struct {
		name  string
		mtime time.Time
		size  int64
	}
	var files []entry
	for _, e := range entries {
		if e.IsDir() || !backupFullPattern.MatchString(e.Name()) {
			continue
		}
		fi, ferr := e.Info()
		if ferr != nil {
			continue // skip unreadable entries
		}
		files = append(files, entry{name: e.Name(), mtime: fi.ModTime(), size: fi.Size()})
	}
	// Newest first.
	sort.Slice(files, func(i, j int) bool {
		return files[i].mtime.After(files[j].mtime)
	})
	out := &cortexv1.ListBackupsResponse{Backups: make([]*cortexv1.BackupFile, 0, len(files))}
	for _, f := range files {
		out.Backups = append(out.Backups, &cortexv1.BackupFile{
			Name:      f.name,
			SizeBytes: f.size,
			CreatedAt: timestamppb.New(f.mtime),
		})
	}
	return connect.NewResponse(out), nil
}

// ListS3Backups lists full-backup objects in the configured offsite (S3)
// bucket/prefix, newest first. Returns FailedPrecondition when S3 is not
// configured/enabled on the server. Admin-only. This is the read-back path that
// makes offsite backups usable — S3 config is server-env only, so the server
// (which holds the credentials) does the listing.
func (s *Service) ListS3Backups(ctx context.Context, _ *connect.Request[cortexv1.ListS3BackupsRequest]) (*connect.Response[cortexv1.ListS3BackupsResponse], error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	if !s.cfg.S3.Enabled {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("offsite S3 backup is not configured (set CORTEX_S3_*/AWS_* on the server)"))
	}
	listCtx, cancel := context.WithTimeout(ctx, s3ListTimeout)
	defer cancel()
	objs, err := listS3Backups(listCtx, s.cfg.S3)
	if err != nil {
		return nil, fmt.Errorf("list s3 backups: %w", err)
	}
	out := &cortexv1.ListS3BackupsResponse{Backups: make([]*cortexv1.BackupFile, 0, len(objs))}
	for _, o := range objs {
		out.Backups = append(out.Backups, &cortexv1.BackupFile{
			Name:      o.Key,
			SizeBytes: o.Size,
			CreatedAt: timestamppb.New(o.LastModified),
		})
	}
	return connect.NewResponse(out), nil
}

// btoi is 1 when b is true, else 0 — used to count how many of a set of
// mutually-exclusive request options are provided.
func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// LastBackupTime returns the modification time of the newest full-backup file in
// BackupDir. ok is false when no backup exists yet (or the dir is absent). The
// server's periodic scheduler uses this to stay restart-resilient: it decides
// whether a backup is overdue from what is actually on disk, not from process
// start time — so a server that restarts more often than BACKUP_INTERVAL still
// gets backed up once the newest file ages past the interval.
func (s *Service) LastBackupTime() (time.Time, bool) {
	entries, err := os.ReadDir(s.backupDir())
	if err != nil {
		return time.Time{}, false
	}
	var newest time.Time
	found := false
	for _, e := range entries {
		if e.IsDir() || !backupFullPattern.MatchString(e.Name()) {
			continue
		}
		fi, ferr := e.Info()
		if ferr != nil {
			continue
		}
		if mt := fi.ModTime(); !found || mt.After(newest) {
			newest = mt
			found = true
		}
	}
	return newest, found
}

// RestoreAll is the admin-gated RPC handler. It accepts EITHER a named backup
// file (name field) OR raw bytes (data field) — exactly one must be set
// (CodeInvalidArgument when both or neither are provided). It validates the
// format version, recreates missing users/keys (in MT mode), and re-queues
// every memory and summary onto the NATS index queue per tenant. Upsert-by-id
// makes re-runs safe.
func (s *Service) RestoreAll(ctx context.Context, req *connect.Request[cortexv1.RestoreAllRequest]) (*connect.Response[cortexv1.RestoreAllResponse], error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}

	// Exactly one source of name/data/s3_key must be set.
	name := req.Msg.GetName()
	inData := req.Msg.GetData()
	s3Key := req.Msg.GetS3Key()
	hasName := name != ""
	hasData := len(inData) > 0
	hasS3 := s3Key != ""
	if btoi(hasName)+btoi(hasData)+btoi(hasS3) != 1 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("exactly one of name, data, or s3_key must be set"))
	}

	var data []byte
	switch {
	case hasName:
		if err := validateBackupFilename(name); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		path := filepath.Join(s.backupDir(), name)
		var err error
		data, err = os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, connect.NewError(connect.CodeNotFound,
					fmt.Errorf("backup file %q not found", name))
			}
			return nil, fmt.Errorf("read backup: %w", err)
		}
	case hasS3:
		if !s.cfg.S3.Enabled {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				errors.New("offsite S3 backup is not configured (set CORTEX_S3_*/AWS_* on the server)"))
		}
		dlCtx, cancel := context.WithTimeout(ctx, s3UploadTimeout)
		d, err := downloadFromS3(dlCtx, s.cfg.S3, s3Key)
		cancel()
		if err != nil {
			return nil, connect.NewError(connect.CodeNotFound,
				fmt.Errorf("download s3 backup %q: %w", s3Key, err))
		}
		data = d
	default: // hasData
		data = inData
	}
	var env backupEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid backup format: %w", err))
	}
	if env.Version != backupVersion {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("unsupported backup version %d (expected %d); refusing restore", env.Version, backupVersion))
	}

	resp := &cortexv1.RestoreAllResponse{}

	// Recreate missing users and API keys — only in MT mode and only when the
	// backup has user records (single-user backups omit both).
	if s.cfg.MultiTenant && len(env.Users) > 0 {
		for _, u := range env.Users {
			// CreateUserWithHash stores the hash verbatim (no re-hash). It returns
			// ErrUserExists when the username already exists — skip those.
			if _, cerr := s.store.CreateUserWithHash(ctx, u.Username, u.PasswordHash, u.Role); cerr != nil {
				if errors.Is(cerr, store.ErrUserExists) {
					resp.UsersSkipped++
					continue
				}
				return nil, fmt.Errorf("restore user %q: %w", u.Username, cerr)
			}
			resp.UsersCreated++
		}
		for _, k := range env.ApiKeys {
			created, cerr := s.store.AddApiKeyFromBackup(ctx, k)
			if cerr != nil {
				return nil, fmt.Errorf("restore api key %q: %w", k.ID, cerr)
			}
			if created {
				resp.ApiKeysCreated++
			} else {
				resp.ApiKeysSkipped++
			}
		}
	}

	// Re-queue memories and summaries per tenant. PublishIndex/PublishSummary
	// stamp the tenant from context, so we wrap a per-tenant identity for each
	// tenant's records. In non-MT mode the single pseudo-tenant "" uses the
	// plain ctx (the worker ignores UserID when MT is off).
	for _, tb := range env.Tenants {
		var tenantCtx context.Context
		if s.cfg.MultiTenant && tb.Tenant != "" {
			tenantCtx = identity.Into(ctx, identity.Identity{UserID: tb.Tenant})
		} else {
			tenantCtx = ctx
		}
		for _, r := range tb.Memories {
			if strings.TrimSpace(r.Text) == "" {
				continue // worker cannot embed an empty record
			}
			if err := bus.PublishIndex(tenantCtx, s.js, r); err != nil {
				return nil, fmt.Errorf("re-queue memory %s: %w", r.ID, err)
			}
			resp.MemoriesQueued++
		}
		for _, sum := range tb.Summaries {
			if strings.TrimSpace(sum.Text) == "" {
				continue
			}
			if err := bus.PublishSummary(tenantCtx, s.js, sum); err != nil {
				return nil, fmt.Errorf("re-queue summary %s: %w", sum.ConversationID, err)
			}
			resp.SummariesQueued++
		}
	}
	resp.Message = fmt.Sprintf(
		"restore complete: %d memories and %d summaries queued; "+
			"%d users created, %d users skipped; %d keys created, %d keys skipped",
		resp.MemoriesQueued, resp.SummariesQueued,
		resp.UsersCreated, resp.UsersSkipped,
		resp.ApiKeysCreated, resp.ApiKeysSkipped,
	)
	s.log.Info("restore complete", "memories_queued", resp.MemoriesQueued,
		"summaries_queued", resp.SummariesQueued,
		"users_created", resp.UsersCreated, "users_skipped", resp.UsersSkipped,
		"keys_created", resp.ApiKeysCreated, "keys_skipped", resp.ApiKeysSkipped)
	return connect.NewResponse(resp), nil
}

// BackupSelf returns the caller's own tenant data as a JSON bytes payload (no
// disk write). It is NOT admin-gated — any authenticated user may export their
// own data. In non-MT mode the single pseudo-tenant "" is backed up.
func (s *Service) BackupSelf(ctx context.Context, _ *connect.Request[cortexv1.BackupSelfRequest]) (*connect.Response[cortexv1.BackupSelfResponse], error) {
	ts, err := s.tenantStore(ctx)
	if err != nil {
		return nil, err
	}

	// Derive the tenant label for the envelope. In non-MT mode the caller may
	// have no identity (bootstrap tenant); the label is then "".
	callerID, _ := identity.From(ctx)
	tenant := callerID.UserID

	recs, err := ts.List(ctx, store.ListOpts{Limit: allLimit})
	if err != nil {
		return nil, fmt.Errorf("list memories: %w", err)
	}
	sums, err := ts.ListSummaries(ctx, store.SummaryListOpts{Limit: allLimit})
	if err != nil {
		return nil, fmt.Errorf("list summaries: %w", err)
	}

	env := backupEnvelope{
		Version:       backupVersion,
		CreatedAt:     time.Now().UTC(),
		ServerVersion: s.cfg.Version,
		MultiTenant:   s.cfg.MultiTenant,
		Tenants: []tenantBackup{{
			Tenant:    tenant,
			Memories:  recs,
			Summaries: sums,
		}},
	}
	data, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("marshal self-backup: %w", err)
	}
	name := fmt.Sprintf("cortex-self-backup-%s.json", time.Now().UTC().Format("20060102-150405"))
	return connect.NewResponse(&cortexv1.BackupSelfResponse{
		Name:      name,
		Data:      data,
		Memories:  int32(len(recs)),
		Summaries: int32(len(sums)),
	}), nil
}

// RestoreSelf parses an uploaded backup payload and re-queues every memory and
// summary into the CALLER's own tenant. It is NOT admin-gated — any
// authenticated user may restore their own data.
//
// Security invariant: the tenant attribution stored in the envelope's
// tenantBackup entries is intentionally IGNORED. All records are published
// using the caller's own context, so bus.PublishIndex/PublishSummary stamp the
// caller's UserID. A user can never write into another tenant's store.
func (s *Service) RestoreSelf(ctx context.Context, req *connect.Request[cortexv1.RestoreSelfRequest]) (*connect.Response[cortexv1.RestoreSelfResponse], error) {
	data := req.Msg.GetData()
	if len(data) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("data must not be empty"))
	}
	if len(data) > maxRestoreSelfBytes {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("data too large (%d bytes; limit is %d)", len(data), maxRestoreSelfBytes))
	}

	// Parse and validate format version before authentication: the version
	// number is not sensitive, and returning InvalidArgument (bad format)
	// rather than Unauthenticated (bad token) gives the client a clearer
	// signal when the upload is structurally wrong. Also makes the version
	// check testable without a live Weaviate connection.
	var env backupEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("invalid backup format: %w", err))
	}
	if env.Version != backupVersion {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("unsupported backup version %d (expected %d)", env.Version, backupVersion))
	}

	// Authenticate the caller (MT: must have identity; non-MT: bootstrap tenant).
	// We do not call requireAdmin — RestoreSelf is self-service.
	if _, err := s.tenantStore(ctx); err != nil {
		return nil, err
	}

	resp := &cortexv1.RestoreSelfResponse{}
	for _, tb := range env.Tenants {
		// Always publish with caller's ctx — NEVER with a per-tenant wrapped ctx.
		// This is the tenant-scoping invariant for RestoreSelf.
		for _, r := range tb.Memories {
			if strings.TrimSpace(r.Text) == "" {
				continue
			}
			if err := bus.PublishIndex(ctx, s.js, r); err != nil {
				return nil, fmt.Errorf("re-queue memory %s: %w", r.ID, err)
			}
			resp.MemoriesQueued++
		}
		for _, sum := range tb.Summaries {
			if strings.TrimSpace(sum.Text) == "" {
				continue
			}
			if err := bus.PublishSummary(ctx, s.js, sum); err != nil {
				return nil, fmt.Errorf("re-queue summary %s: %w", sum.ConversationID, err)
			}
			resp.SummariesQueued++
		}
	}
	resp.Message = fmt.Sprintf(
		"restore complete: %d memories and %d summaries queued into your tenant",
		resp.MemoriesQueued, resp.SummariesQueued)
	return connect.NewResponse(resp), nil
}

// DownloadBackup returns the raw bytes of a named backup file. Admin-gated.
func (s *Service) DownloadBackup(ctx context.Context, req *connect.Request[cortexv1.DownloadBackupRequest]) (*connect.Response[cortexv1.DownloadBackupResponse], error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	name := req.Msg.GetName()
	if err := validateBackupFilename(name); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	path := filepath.Join(s.backupDir(), name)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, connect.NewError(connect.CodeNotFound,
				fmt.Errorf("backup file %q not found", name))
		}
		return nil, fmt.Errorf("stat backup: %w", err)
	}
	if info.Size() > maxBackupDownloadBytes {
		return nil, connect.NewError(connect.CodeResourceExhausted,
			fmt.Errorf("backup file %q size %d exceeds %d byte download limit", name, info.Size(), maxBackupDownloadBytes))
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read backup: %w", err)
	}
	return connect.NewResponse(&cortexv1.DownloadBackupResponse{Name: name, Data: data}), nil
}

// DeleteBackup removes a named backup file from BackupDir. Admin-gated.
// Returns CodeNotFound when the file does not exist.
func (s *Service) DeleteBackup(ctx context.Context, req *connect.Request[cortexv1.DeleteBackupRequest]) (*connect.Response[cortexv1.DeleteBackupResponse], error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	name := req.Msg.GetName()
	if err := validateBackupFilename(name); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	path := filepath.Join(s.backupDir(), name)
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return nil, connect.NewError(connect.CodeNotFound,
				fmt.Errorf("backup file %q not found", name))
		}
		return nil, fmt.Errorf("delete backup: %w", err)
	}
	return connect.NewResponse(&cortexv1.DeleteBackupResponse{}), nil
}

// validateBackupFilename rejects any name containing path separators, "..", or
// not matching the cortex-full-backup-YYYYMMDD-HHMMSS.json pattern.
// This ensures RestoreAll/DownloadBackup/DeleteBackup never touch files outside
// the configured backup directory.
func validateBackupFilename(name string) error {
	if name == "" {
		return errors.New("filename must not be empty")
	}
	if strings.ContainsAny(name, "/\\") || strings.Contains(name, "..") {
		return errors.New("filename must not contain path separators or \"..\"")
	}
	if !backupFullPattern.MatchString(name) {
		return fmt.Errorf("filename %q does not match cortex-full-backup-YYYYMMDD-HHMMSS.json", name)
	}
	return nil
}

// runFullBackup is the identity-free backup function used by both BackupAll
// (RPC, after requireAdmin) and RunPeriodicBackup (scheduled goroutine).
// It writes the versioned envelope to BackupDir and returns the proto response.
func (s *Service) runFullBackup(ctx context.Context) (*cortexv1.BackupAllResponse, error) {
	env := backupEnvelope{
		Version:       backupVersion,
		CreatedAt:     time.Now().UTC(),
		ServerVersion: s.cfg.Version,
		MultiTenant:   s.cfg.MultiTenant,
	}

	var tenants []string
	if s.cfg.MultiTenant {
		// Collect the full user registry (hashes included — server-side file only).
		users, err := s.store.ListUsers(ctx)
		if err != nil {
			return nil, fmt.Errorf("list users: %w", err)
		}
		env.Users = users
		for _, u := range users {
			keys, err := s.store.ListApiKeysForUser(ctx, u.ID)
			if err != nil {
				return nil, fmt.Errorf("list api keys for %s: %w", u.ID, err)
			}
			env.ApiKeys = append(env.ApiKeys, keys...)
		}
		// Union tenants from Memory + ConversationSummary so orphaned-summary
		// tenants are captured even if they have no memories.
		tenants, err = s.store.ListAllTenants(ctx)
		if err != nil {
			return nil, fmt.Errorf("list tenants: %w", err)
		}
	} else {
		// Single-user mode: one pseudo-tenant "". The store ignores the name when
		// MT is off and issues no WithTenant on Weaviate calls.
		tenants = []string{""}
	}

	totalMemories := 0
	totalSummaries := 0
	for _, tenant := range tenants {
		ts := s.store.Tenant(tenant)
		recs, err := ts.List(ctx, store.ListOpts{Limit: allLimit})
		if err != nil {
			return nil, fmt.Errorf("list memories for tenant %q: %w", tenant, err)
		}
		sums, err := ts.ListSummaries(ctx, store.SummaryListOpts{Limit: allLimit})
		if err != nil {
			return nil, fmt.Errorf("list summaries for tenant %q: %w", tenant, err)
		}
		env.Tenants = append(env.Tenants, tenantBackup{
			Tenant:    tenant,
			Memories:  recs,
			Summaries: sums,
		})
		totalMemories += len(recs)
		totalSummaries += len(sums)
	}

	// Write to disk.
	dir := s.backupDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("backup dir: %w", err)
	}
	fname := fmt.Sprintf("cortex-full-backup-%s.json", time.Now().UTC().Format("20060102-150405"))
	fpath := filepath.Join(dir, fname)
	data, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal backup: %w", err)
	}
	if err := os.WriteFile(fpath, data, 0o644); err != nil {
		return nil, fmt.Errorf("write backup: %w", err)
	}
	s.log.Info("wrote full backup",
		"path", fpath,
		"tenants", len(tenants),
		"memories", totalMemories,
		"summaries", totalSummaries,
		"users", len(env.Users),
		"api_keys", len(env.ApiKeys),
	)

	// S3 offsite upload: best-effort, never fails the backup. The target comes
	// from the server environment (s.cfg.S3), held in memory — no stored config.
	s3Result := ""
	if s.cfg.S3.Enabled {
		uploadCtx, cancel := context.WithTimeout(ctx, s3UploadTimeout)
		s3Result = uploadToS3(uploadCtx, s.cfg.S3, fpath)
		cancel()
		if strings.HasPrefix(s3Result, "uploaded") {
			s.log.Info("full backup uploaded to S3", "result", s3Result)
		} else {
			s.log.Warn("full backup S3 upload failed (local backup still valid)",
				"result", s3Result)
		}
	}

	return &cortexv1.BackupAllResponse{
		Path:      fpath,
		Tenants:   int32(len(tenants)),
		Memories:  int32(totalMemories),
		Summaries: int32(totalSummaries),
		Users:     int32(len(env.Users)),
		ApiKeys:   int32(len(env.ApiKeys)),
		S3Result:  s3Result,
	}, nil
}

// RunPeriodicBackup is called by the server's scheduled goroutine: it runs the
// backup and then prunes. It does NOT call requireAdmin — the goroutine is
// server-internal and has no caller identity. Log errors but do not crash.
func (s *Service) RunPeriodicBackup(ctx context.Context) error {
	resp, err := s.runFullBackup(ctx)
	if err != nil {
		return err
	}
	s.log.Info("periodic backup complete",
		"path", resp.Path,
		"tenants", resp.Tenants,
		"memories", resp.Memories,
		"summaries", resp.Summaries,
	)
	if err := s.pruneFullBackups(); err != nil {
		s.log.Warn("periodic backup: prune failed", "err", err)
	}
	return nil
}

// pruneFullBackups delegates to pruneBackups using the service's configured
// BackupDir and BackupKeep.
func (s *Service) pruneFullBackups() error {
	return pruneBackups(s.backupDir(), s.cfg.BackupKeep)
}

// pruneBackups keeps the newest keep cortex-full-backup-*.json files in dir
// and deletes older ones. It NEVER touches other files — in particular it never
// deletes reindex safety snapshots (cortex-backup-*.json). Passing keep <= 0
// is a no-op.
func pruneBackups(dir string, keep int) error {
	if keep <= 0 {
		return nil // pruning disabled
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("prune: readdir %s: %w", dir, err)
	}
	type fentry struct {
		path  string
		mtime time.Time
	}
	var files []fentry
	for _, e := range entries {
		if e.IsDir() || !backupFullPattern.MatchString(e.Name()) {
			continue
		}
		fi, ferr := e.Info()
		if ferr != nil {
			continue
		}
		files = append(files, fentry{path: filepath.Join(dir, e.Name()), mtime: fi.ModTime()})
	}
	if len(files) <= keep {
		return nil
	}
	// Sort newest first; delete all beyond the keep window.
	sort.Slice(files, func(i, j int) bool {
		return files[i].mtime.After(files[j].mtime)
	})
	for _, f := range files[keep:] {
		if rerr := os.Remove(f.path); rerr != nil && !os.IsNotExist(rerr) {
			return fmt.Errorf("prune: remove %s: %w", f.path, rerr)
		}
	}
	return nil
}

// backupDir returns the configured backup directory, defaulting to "." when
// not set (mirroring the reindex writeBackup convention).
func (s *Service) backupDir() string {
	if s.cfg.BackupDir == "" {
		return "."
	}
	return s.cfg.BackupDir
}
