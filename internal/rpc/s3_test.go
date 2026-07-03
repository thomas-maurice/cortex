package rpc

// s3_test.go covers the S3-config RPC handlers and the pure-function helpers.

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/store"
)

// ---- Admin-gate tests ----

// TestGetS3ConfigAdminGate verifies GetS3Config rejects a non-admin.
func TestGetS3ConfigAdminGate(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: true}}
	_, err := svc.GetS3Config(userCtx("uid-bob", "bob"),
		connect.NewRequest(&cortexv1.GetS3ConfigRequest{}))
	require.Error(t, err)
	var ce *connect.Error
	require.True(t, errors.As(err, &ce))
	assert.Equal(t, connect.CodePermissionDenied, ce.Code(),
		"GetS3Config must be admin-gated — S3 credentials are server-wide secrets")
}

// TestSetS3ConfigAdminGate verifies SetS3Config rejects a non-admin.
func TestSetS3ConfigAdminGate(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: true}}
	_, err := svc.SetS3Config(userCtx("uid-bob", "bob"),
		connect.NewRequest(&cortexv1.SetS3ConfigRequest{
			Config: &cortexv1.S3Config{Endpoint: "s3.example.com"},
		}))
	require.Error(t, err)
	var ce *connect.Error
	require.True(t, errors.As(err, &ce))
	assert.Equal(t, connect.CodePermissionDenied, ce.Code(),
		"SetS3Config must be admin-gated")
}

// TestTestS3AdminGate verifies TestS3 rejects a non-admin.
func TestTestS3AdminGate(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: true}}
	_, err := svc.TestS3(userCtx("uid-bob", "bob"),
		connect.NewRequest(&cortexv1.TestS3Request{}))
	require.Error(t, err)
	var ce *connect.Error
	require.True(t, errors.As(err, &ce))
	assert.Equal(t, connect.CodePermissionDenied, ce.Code(),
		"TestS3 must be admin-gated")
}

// ---- MergeS3Secret pure-function tests ----

// TestMergeS3SecretKeepsExisting verifies the write-only credential invariant:
// an empty new secret must never overwrite an existing one. This is the core
// rule that makes secret_key a write-only field in the API — a GetS3Config
// followed by SetS3Config (without re-supplying the secret) must preserve the
// original secret, not blank it.
func TestMergeS3SecretKeepsExisting(t *testing.T) {
	tests := []struct {
		name     string
		existing string
		newKey   string
		want     string
	}{
		{
			name:     "keep when new is empty",
			existing: "oldsecret",
			newKey:   "",
			want:     "oldsecret",
		},
		{
			name:     "replace when new is provided",
			existing: "oldsecret",
			newKey:   "newsecret",
			want:     "newsecret",
		},
		{
			name:     "no-op when both are empty",
			existing: "",
			newKey:   "",
			want:     "",
		},
		{
			name:     "set from blank store when new is provided",
			existing: "",
			newKey:   "firstsecret",
			want:     "firstsecret",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := store.MergeS3Secret(tc.existing, tc.newKey)
			assert.Equal(t, tc.want, got,
				"MergeS3Secret must enforce the write-only credential invariant: "+
					"empty newKey must never overwrite an existing secret")
		})
	}
}

// ---- SetS3Config nil-config guard ----

// TestSetS3ConfigRejectsNilConfig verifies that SetS3Config returns
// CodeInvalidArgument when the config field is absent. The admin gate fires
// first, so this test uses an admin context — without one the gate would return
// PermissionDenied before we reach the nil check.
//
// Note: with a nil store the handler panics after the nil check (when it calls
// s.store.SetS3Config). We intercept that with a recover so the nil-config
// check is exercised without a Weaviate connection.
func TestSetS3ConfigRejectsNilConfig(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: false}}
	ctx := adminCtx()

	var code connect.Code
	func() {
		defer func() { recover() }() //nolint:errcheck
		_, err := svc.SetS3Config(ctx,
			connect.NewRequest(&cortexv1.SetS3ConfigRequest{Config: nil}))
		if err != nil {
			var ce *connect.Error
			if errors.As(err, &ce) {
				code = ce.Code()
			}
		}
	}()
	// If code was set before the panic the nil check fired correctly.
	// If code is zero (unset), err was nil which should not happen.
	assert.Equal(t, connect.CodeInvalidArgument, code,
		"nil S3Config must return InvalidArgument before any store call")
}

// Ensure context import is used.
var _ = context.Background
