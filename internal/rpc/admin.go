package rpc

// admin.go implements the P5 (user management) and P6 (API key management)
// RPC handlers on Service. All P5 handlers are admin-only and gated behind
// the CORTEX_MULTI_TENANT flag; P6 handlers are scoped to the authenticated
// caller's UserID and are also flag-gated.
//
// Security invariants enforced here:
//   - Identity is read from context (identity.From(ctx)) — never from req.Msg.
//   - Admin gate: every P5 handler calls requireAdmin(ctx) first.
//   - Cross-user scope: every P6 delete checks that the key's userId == caller.
//   - Flag-off: every handler returns CodeFailedPrecondition when MT is disabled.

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/identity"
	"github.com/thomas-maurice/cortex/internal/store"
)

// requireAdmin returns a CodePermissionDenied error unless the identity on ctx
// carries the admin role. It is the single admin-gate used by all P5 handlers.
func requireAdmin(ctx context.Context) error {
	id, ok := identity.From(ctx)
	if !ok || !id.IsAdmin() {
		return connect.NewError(connect.CodePermissionDenied, errors.New("admin role required"))
	}
	return nil
}

// requireMT returns a CodeFailedPrecondition error when multi-tenancy is off.
// Flag-off ⇒ these RPCs are not meaningful and must not silently touch anything.
func (s *Service) requireMT() error {
	if !s.cfg.MultiTenant {
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("multi-tenancy disabled (CORTEX_MULTI_TENANT=false)"))
	}
	return nil
}

// ---- P5: User management ----

func (s *Service) ListUsers(ctx context.Context, _ *connect.Request[cortexv1.ListUsersRequest]) (*connect.Response[cortexv1.ListUsersResponse], error) {
	if err := s.requireMT(); err != nil {
		return nil, err
	}
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	users, err := s.store.ListUsers(ctx)
	if err != nil {
		return nil, err
	}
	out := &cortexv1.ListUsersResponse{Users: make([]*cortexv1.UserInfo, 0, len(users))}
	for _, u := range users {
		out.Users = append(out.Users, userToProto(u))
	}
	return connect.NewResponse(out), nil
}

func (s *Service) CreateUser(ctx context.Context, req *connect.Request[cortexv1.CreateUserRequest]) (*connect.Response[cortexv1.CreateUserResponse], error) {
	if err := s.requireMT(); err != nil {
		return nil, err
	}
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	username := strings.TrimSpace(req.Msg.GetUsername())
	password := req.Msg.GetPassword()
	role := strings.TrimSpace(req.Msg.GetRole())
	if username == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("username must not be empty"))
	}
	if password == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("password must not be empty"))
	}
	if role != "" && role != identity.RoleAdmin && role != identity.RoleUser {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("role must be %q or %q", identity.RoleAdmin, identity.RoleUser))
	}
	u, err := s.store.CreateUser(ctx, username, password, role)
	if err != nil {
		if errors.Is(err, store.ErrUserExists) {
			return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf("user %q already exists", username))
		}
		return nil, err
	}
	return connect.NewResponse(&cortexv1.CreateUserResponse{User: userToProto(u)}), nil
}

func (s *Service) DeleteUser(ctx context.Context, req *connect.Request[cortexv1.DeleteUserRequest]) (*connect.Response[cortexv1.DeleteUserResponse], error) {
	if err := s.requireMT(); err != nil {
		return nil, err
	}
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	username := strings.TrimSpace(req.Msg.GetUsername())
	if username == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("username must not be empty"))
	}
	target, found, err := s.store.GetUserByUsername(ctx, username)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("user %q not found", username))
	}

	// Safety: refuse to delete the bootstrap admin.
	if target.ID == identity.BootstrapTenant {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			errors.New("cannot delete the bootstrap admin user"))
	}

	// Safety: refuse to delete the last remaining admin.
	if target.Role == identity.RoleAdmin {
		allUsers, err := s.store.ListUsers(ctx)
		if err != nil {
			return nil, err
		}
		adminCount := 0
		for _, u := range allUsers {
			if u.Role == identity.RoleAdmin {
				adminCount++
			}
		}
		if adminCount <= 1 {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				errors.New("cannot delete the last admin user — promote another user first"))
		}
	}

	// Cascade 1: api keys — done inside store.DeleteUser.
	// Cascade 2: memory tenant — drop the tenant from the three MT classes.
	if s.cfg.MultiTenant {
		if err := s.store.DropTenant(ctx, target.ID); err != nil {
			s.log.Warn("delete user: drop tenant failed (data may remain)", "user", target.ID, "err", err)
			// Don't block the delete — the user record and keys are authoritative;
			// orphaned tenant data is inert because no credential can reach it.
		}
	}

	if err := s.store.DeleteUser(ctx, target.ID); err != nil {
		return nil, err
	}
	return connect.NewResponse(&cortexv1.DeleteUserResponse{Status: "deleted"}), nil
}

func (s *Service) SetUserRole(ctx context.Context, req *connect.Request[cortexv1.SetUserRoleRequest]) (*connect.Response[cortexv1.SetUserRoleResponse], error) {
	if err := s.requireMT(); err != nil {
		return nil, err
	}
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	username := strings.TrimSpace(req.Msg.GetUsername())
	role := strings.TrimSpace(req.Msg.GetRole())
	if username == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("username must not be empty"))
	}
	if role != identity.RoleAdmin && role != identity.RoleUser {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("role must be %q or %q", identity.RoleAdmin, identity.RoleUser))
	}

	target, found, err := s.store.GetUserByUsername(ctx, username)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("user %q not found", username))
	}

	// Safety: refuse to demote the last admin.
	if target.Role == identity.RoleAdmin && role == identity.RoleUser {
		allUsers, err := s.store.ListUsers(ctx)
		if err != nil {
			return nil, err
		}
		adminCount := 0
		for _, u := range allUsers {
			if u.Role == identity.RoleAdmin {
				adminCount++
			}
		}
		if adminCount <= 1 {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				errors.New("cannot demote the last admin — promote another user first"))
		}
	}

	if err := s.store.UpdateUserRole(ctx, target.ID, role); err != nil {
		return nil, err
	}
	target.Role = role
	return connect.NewResponse(&cortexv1.SetUserRoleResponse{User: userToProto(target)}), nil
}

func (s *Service) ResetUserPassword(ctx context.Context, req *connect.Request[cortexv1.ResetUserPasswordRequest]) (*connect.Response[cortexv1.ResetUserPasswordResponse], error) {
	if err := s.requireMT(); err != nil {
		return nil, err
	}
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	username := strings.TrimSpace(req.Msg.GetUsername())
	newPassword := req.Msg.GetNewPassword()
	if username == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("username must not be empty"))
	}
	if newPassword == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("new_password must not be empty"))
	}
	target, found, err := s.store.GetUserByUsername(ctx, username)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("user %q not found", username))
	}
	if err := s.store.SetUserPassword(ctx, target.ID, newPassword); err != nil {
		return nil, err
	}
	return connect.NewResponse(&cortexv1.ResetUserPasswordResponse{Status: "updated"}), nil
}

// ---- P6: API key management ----

func (s *Service) CreateApiKey(ctx context.Context, req *connect.Request[cortexv1.CreateApiKeyRequest]) (*connect.Response[cortexv1.CreateApiKeyResponse], error) {
	if err := s.requireMT(); err != nil {
		return nil, err
	}
	id, ok := identity.From(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("not authenticated"))
	}
	label := strings.TrimSpace(req.Msg.GetLabel())
	raw, key, err := s.store.CreateApiKey(ctx, id.UserID, label)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&cortexv1.CreateApiKeyResponse{
		RawKey: raw,
		Key:    apiKeyToProto(key),
	}), nil
}

func (s *Service) ListApiKeys(ctx context.Context, _ *connect.Request[cortexv1.ListApiKeysRequest]) (*connect.Response[cortexv1.ListApiKeysResponse], error) {
	if err := s.requireMT(); err != nil {
		return nil, err
	}
	id, ok := identity.From(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("not authenticated"))
	}
	keys, err := s.store.ListApiKeysForUser(ctx, id.UserID)
	if err != nil {
		return nil, err
	}
	out := &cortexv1.ListApiKeysResponse{Keys: make([]*cortexv1.ApiKeyInfo, 0, len(keys))}
	for _, k := range keys {
		out.Keys = append(out.Keys, apiKeyToProto(k))
	}
	return connect.NewResponse(out), nil
}

func (s *Service) DeleteApiKey(ctx context.Context, req *connect.Request[cortexv1.DeleteApiKeyRequest]) (*connect.Response[cortexv1.DeleteApiKeyResponse], error) {
	if err := s.requireMT(); err != nil {
		return nil, err
	}
	callerID, ok := identity.From(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("not authenticated"))
	}
	keyID := strings.TrimSpace(req.Msg.GetId())
	if keyID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id must not be empty"))
	}

	// Ownership check: retrieve the key by id and verify it belongs to the caller.
	// We look it up by listing the caller's keys (ListApiKeysForUser already scopes
	// to the caller's userId, so a key not in that list is either absent or owned
	// by someone else — both cases return NotFound without revealing existence).
	keys, err := s.store.ListApiKeysForUser(ctx, callerID.UserID)
	if err != nil {
		return nil, err
	}
	var found bool
	for _, k := range keys {
		if k.ID == keyID {
			found = true
			break
		}
	}
	if !found {
		// Return NotFound regardless of whether the key exists under another user
		// — do not reveal cross-user existence.
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("api key %q not found", keyID))
	}

	if err := s.store.DeleteApiKey(ctx, keyID); err != nil {
		return nil, err
	}
	return connect.NewResponse(&cortexv1.DeleteApiKeyResponse{Status: "deleted"}), nil
}

// ---- proto helpers ----

func userToProto(u store.User) *cortexv1.UserInfo {
	p := &cortexv1.UserInfo{
		Id:       u.ID,
		Username: u.Username,
		Role:     u.Role,
	}
	if !u.CreatedAt.IsZero() {
		p.CreatedAt = timestamppb.New(u.CreatedAt)
	}
	if !u.UpdatedAt.IsZero() {
		p.UpdatedAt = timestamppb.New(u.UpdatedAt)
	}
	return p
}

func apiKeyToProto(k store.ApiKey) *cortexv1.ApiKeyInfo {
	p := &cortexv1.ApiKeyInfo{
		Id:     k.ID,
		Label:  k.Label,
		Prefix: k.Prefix,
	}
	if !k.CreatedAt.IsZero() {
		p.CreatedAt = timestamppb.New(k.CreatedAt)
	}
	if !k.LastUsedAt.IsZero() {
		p.LastUsedAt = timestamppb.New(k.LastUsedAt)
	}
	return p
}
