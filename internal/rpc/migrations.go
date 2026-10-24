package rpc

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/gen/cortex/v1/cortexv1connect"
	"github.com/thomas-maurice/cortex/internal/migrate"
)

// MigrationReporter is what ListMigrations needs from the migration runner.
type MigrationReporter interface {
	Status(ctx context.Context) (*migrate.Status, error)
}

// SetMigrations wires the schema-migration reporter behind ListMigrations. Call it
// before serving.
func (s *Service) SetMigrations(r MigrationReporter) { s.migrations = r }

// SetMaintenance wires the maintenance-mode check used to skip best-effort writes
// (search reinforcement) while a schema migration copies data.
func (s *Service) SetMaintenance(active func(context.Context) bool) { s.maintenance = active }

func (s *Service) inMaintenance(ctx context.Context) bool {
	return s.maintenance != nil && s.maintenance(ctx)
}

// ListMigrations is the read-only `cortex migrate status`. Admin only: it exposes
// the deployment's internals, not tenant data.
func (s *Service) ListMigrations(ctx context.Context, _ *connect.Request[cortexv1.ListMigrationsRequest]) (*connect.Response[cortexv1.ListMigrationsResponse], error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	if s.migrations == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("schema migrations are not enabled on this server"))
	}
	st, err := s.migrations.Status(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("read migration status: %w", err))
	}
	out := &cortexv1.ListMigrationsResponse{
		SchemaVersion: int32(st.SchemaVersion),
		Latest:        int32(st.Latest),
		LockHolder:    st.LockHolder,
	}
	for _, m := range st.Migrations {
		sm := &cortexv1.SchemaMigration{Id: int32(m.ID), Name: m.Name, Description: m.Description, Applied: m.Applied, Assumed: m.Assumed}
		if m.Applied {
			sm.AppliedAt = timestamppb.New(m.AppliedAt)
			sm.DurationMs = m.Duration.Milliseconds()
		}
		out.Migrations = append(out.Migrations, sm)
	}
	if st.Maintenance != nil {
		out.Maintenance, out.MaintenanceReason = true, st.Maintenance.Reason
	}
	return connect.NewResponse(out), nil
}

// MaintenanceMode classifies every MemoryService RPC for schema-migration
// maintenance mode (see internal/migrate). While a class is being copied:
//
//   - blocked RPCs write Weaviate directly, so a write could land in the class
//     being replaced after it was copied and be lost: they answer Unavailable.
//   - queue-only RPCs only publish to NATS; the worker is paused and the queue
//     buffers them, so they are accepted (refusing a Save would lose a memory).
//   - everything else reads (or touches only users/keys/files) and keeps working.
//
// TestEveryRPCIsClassifiedForMaintenance fails when a new RPC is added without
// being listed in exactly one of the first two sets or the read set.
var (
	maintenanceBlocked = setOf(
		cortexv1connect.MemoryServiceUpdateMemoryProcedure, // SetValidUntil writes directly
		cortexv1connect.MemoryServiceDeleteProcedure,
		cortexv1connect.MemoryServiceDeleteManyProcedure,
		cortexv1connect.MemoryServiceRecordUseProcedure,
		cortexv1connect.MemoryServiceReindexProcedure,
		cortexv1connect.MemoryServiceDismissDuplicateProcedure,
		cortexv1connect.MemoryServiceMergeIntoProcedure,
		cortexv1connect.MemoryServiceRenameNamespaceProcedure,
		cortexv1connect.MemoryServiceDeleteNamespaceProcedure,
		cortexv1connect.MemoryServiceRenameTagProcedure,
		cortexv1connect.MemoryServiceDeleteTagProcedure,
		cortexv1connect.MemoryServiceRestoreFromTrashProcedure,
		cortexv1connect.MemoryServicePurgeTrashProcedure,
		cortexv1connect.MemoryServiceMigrateMTProcedure,
		cortexv1connect.MemoryServiceRestoreAllProcedure,
		cortexv1connect.MemoryServiceDeleteUserProcedure,
	)
	maintenanceQueueOnly = setOf(
		cortexv1connect.MemoryServiceSaveProcedure,
		cortexv1connect.MemoryServiceSummarizeSessionProcedure,
		cortexv1connect.MemoryServiceLinkProcedure,
		cortexv1connect.MemoryServiceUnlinkProcedure,
		cortexv1connect.MemoryServiceRestoreMemoriesProcedure,
		cortexv1connect.MemoryServiceRestoreSelfProcedure,
	)
)

func setOf(items ...string) map[string]bool {
	m := make(map[string]bool, len(items))
	for _, i := range items {
		m[i] = true
	}
	return m
}

// MaintenanceInterceptor answers Unavailable to blocked RPCs while active reports
// maintenance mode. Reads and queue-only writes pass through.
func MaintenanceInterceptor(active func(context.Context) bool) connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if !req.Spec().IsClient && maintenanceBlocked[req.Spec().Procedure] && active(ctx) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("maintenance, retry shortly: a schema migration is copying data"))
			}
			return next(ctx, req)
		}
	})
}
