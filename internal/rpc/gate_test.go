package rpc

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/identity"
)

func TestTenantGate(t *testing.T) {
	var g tenantGate
	rel, ok := g.TryAcquire("a")
	require.True(t, ok)

	_, ok = g.TryAcquire("a")
	assert.False(t, ok, "same tenant must not run a second heavy op concurrently")

	relB, ok := g.TryAcquire("b")
	assert.True(t, ok, "another tenant must not be blocked by a's operation")
	relB()

	rel()
	rel() // idempotent: a double release must not free a later holder's slot
	rel2, ok := g.TryAcquire("a")
	require.True(t, ok, "slot is reusable after release")
	_, ok = g.TryAcquire("a")
	assert.False(t, ok, "stale double release must not have freed the new holder")
	rel2()
}

func gateTestCtx(id string) context.Context {
	return identity.Into(context.Background(), identity.Identity{UserID: id, Role: identity.RoleAdmin})
}

// holdFirst blocks the first call inside the gate (via the afterGate seam) so a
// second call can be issued while it is provably in flight, then aborts it.
// The seam also keeps these tests free of Weaviate/NATS.
func holdFirst(t *testing.T, svc *Service, call func(ctx context.Context) error) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	svc.afterGate = func() error {
		select {
		case <-entered: // already signalled: later calls pass straight through
			return errors.New("abort")
		default:
		}
		close(entered)
		<-release
		return errors.New("abort")
	}
	done := make(chan error, 1)
	go func() { done <- call(gateTestCtx("u1")) }()
	<-entered

	err := call(gateTestCtx("u1"))
	require.Error(t, err)
	assert.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err), "same tenant, concurrent call")

	// A different tenant gets past the gate (reaches the seam and aborts there).
	err = call(gateTestCtx("u2"))
	require.Error(t, err)
	assert.Equal(t, "abort", err.Error(), "other tenant must not be blocked")

	close(release)
	assert.EqualError(t, <-done, "abort")
	// Released: the same tenant is admitted again.
	err = call(gateTestCtx("u1"))
	assert.EqualError(t, err, "abort")
}

func TestHeavyRPCsSingleFlightPerTenant(t *testing.T) {
	tests := map[string]func(s *Service) func(context.Context) error{
		"BackupSelf": func(s *Service) func(context.Context) error {
			return func(ctx context.Context) error {
				_, err := s.BackupSelf(ctx, connect.NewRequest(&cortexv1.BackupSelfRequest{}))
				return err
			}
		},
		"Reindex": func(s *Service) func(context.Context) error {
			return func(ctx context.Context) error {
				_, err := s.Reindex(ctx, connect.NewRequest(&cortexv1.ReindexRequest{}))
				return err
			}
		},
		"DeleteNamespace": func(s *Service) func(context.Context) error {
			return func(ctx context.Context) error {
				_, err := s.DeleteNamespace(ctx, connect.NewRequest(&cortexv1.DeleteNamespaceRequest{Namespace: "ns"}))
				return err
			}
		},
		"RestoreTrashAll": func(s *Service) func(context.Context) error {
			return func(ctx context.Context) error {
				_, err := s.RestoreFromTrash(ctx, connect.NewRequest(&cortexv1.RestoreFromTrashRequest{All: true}))
				return err
			}
		},
		"PurgeTrashAll": func(s *Service) func(context.Context) error {
			return func(ctx context.Context) error {
				_, err := s.PurgeTrash(ctx, connect.NewRequest(&cortexv1.PurgeTrashRequest{All: true}))
				return err
			}
		},
		"RestoreSelf": func(s *Service) func(context.Context) error {
			return func(ctx context.Context) error {
				_, err := s.RestoreSelf(ctx, connect.NewRequest(&cortexv1.RestoreSelfRequest{Data: []byte("{}")}))
				return err
			}
		},
		"RestoreAll": func(s *Service) func(context.Context) error {
			return func(ctx context.Context) error {
				_, err := s.RestoreAll(ctx, connect.NewRequest(&cortexv1.RestoreAllRequest{Name: "x"}))
				return err
			}
		},
	}
	for name, mk := range tests {
		t.Run(name, func(t *testing.T) {
			svc := &Service{cfg: Config{MultiTenant: true}}
			call := mk(svc)
			if name == "RestoreAll" {
				// Admin ops are server-wide: a different admin IS blocked.
				entered, release := make(chan struct{}), make(chan struct{})
				svc.afterGate = func() error { close(entered); <-release; return errors.New("abort") }
				done := make(chan error, 1)
				go func() { done <- call(gateTestCtx("admin1")) }()
				<-entered
				err := call(gateTestCtx("admin2"))
				assert.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
				close(release)
				assert.EqualError(t, <-done, "abort")
				return
			}
			holdFirst(t, svc, call)
		})
	}
}
