package rpc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/gen/cortex/v1/cortexv1connect"
	"github.com/thomas-maurice/cortex/internal/identity"
	"github.com/thomas-maurice/cortex/internal/migrate"
)

// maintenanceReads are the RPCs that keep working during maintenance: they read, or
// touch only users / API keys / backup files. A NEW RPC must be added here, to
// maintenanceBlocked, or to maintenanceQueueOnly: a Weaviate-writing RPC left
// out would silently write into a class that is being replaced.
var maintenanceReads = setOf(
	cortexv1connect.MemoryServiceSearchProcedure,
	cortexv1connect.MemoryServiceSearchSimilarProcedure,
	cortexv1connect.MemoryServicePreviewRecallProcedure,
	cortexv1connect.MemoryServiceListProcedure,
	cortexv1connect.MemoryServiceGetMemoryProcedure,
	cortexv1connect.MemoryServiceStatusProcedure,
	cortexv1connect.MemoryServiceGetVersionProcedure,
	cortexv1connect.MemoryServiceDoctorProcedure,
	cortexv1connect.MemoryServiceDeadProcedure,
	cortexv1connect.MemoryServiceIndexQueueProcedure,
	cortexv1connect.MemoryServicePullModelProcedure,
	cortexv1connect.MemoryServiceRecallSessionProcedure,
	cortexv1connect.MemoryServiceListSummariesProcedure,
	cortexv1connect.MemoryServiceListDuplicateCandidatesProcedure,
	cortexv1connect.MemoryServiceConsolidateProcedure,
	cortexv1connect.MemoryServiceListNamespacesProcedure,
	cortexv1connect.MemoryServiceListTagsProcedure,
	cortexv1connect.MemoryServiceListTrashProcedure,
	cortexv1connect.MemoryServiceListMigrationsProcedure,
	cortexv1connect.MemoryServiceGetStorageInfoProcedure,
	cortexv1connect.MemoryServiceBackupSelfProcedure,
	cortexv1connect.MemoryServiceBackupAllProcedure,
	cortexv1connect.MemoryServiceListBackupsProcedure,
	cortexv1connect.MemoryServiceListS3BackupsProcedure,
	cortexv1connect.MemoryServiceDownloadBackupProcedure,
	cortexv1connect.MemoryServiceDeleteBackupProcedure,
	cortexv1connect.MemoryServiceListUsersProcedure,
	cortexv1connect.MemoryServiceCreateUserProcedure,
	cortexv1connect.MemoryServiceSetUserRoleProcedure,
	cortexv1connect.MemoryServiceResetUserPasswordProcedure,
	cortexv1connect.MemoryServiceCreateApiKeyProcedure,
	cortexv1connect.MemoryServiceListApiKeysProcedure,
	cortexv1connect.MemoryServiceDeleteApiKeyProcedure,
	cortexv1connect.MemoryServiceAdminCreateApiKeyProcedure,
	cortexv1connect.MemoryServiceAdminListApiKeysProcedure,
	cortexv1connect.MemoryServiceAdminDeleteApiKeyProcedure,
)

func TestEveryRPCIsClassifiedForMaintenance(t *testing.T) {
	methods := cortexv1.File_cortex_v1_cortex_proto.Services().ByName("MemoryService").Methods()
	seen := map[string]bool{}
	for i := 0; i < methods.Len(); i++ {
		proc := "/cortex.v1.MemoryService/" + string(methods.Get(i).Name())
		seen[proc] = true
		n := 0
		for _, set := range []map[string]bool{maintenanceBlocked, maintenanceQueueOnly, maintenanceReads} {
			if set[proc] {
				n++
			}
		}
		assert.Equal(t, 1, n, "%s must be classified in exactly one maintenance set", proc)
	}
	for _, set := range []map[string]bool{maintenanceBlocked, maintenanceQueueOnly, maintenanceReads} {
		for proc := range set {
			assert.True(t, seen[proc], "%s is classified but is not an RPC", proc)
		}
	}
}

type probeHandler struct {
	cortexv1connect.UnimplementedMemoryServiceHandler
}

// While a migration copies data, direct Weaviate writers must be refused (a write
// into the class being replaced would be lost) but reads and queue-only writes
// (NATS buffers them for the paused worker) must keep working.
func TestMaintenanceInterceptor(t *testing.T) {
	active := false
	_, h := cortexv1connect.NewMemoryServiceHandler(probeHandler{},
		connect.WithInterceptors(MaintenanceInterceptor(func(context.Context) bool { return active })))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c := cortexv1connect.NewMemoryServiceClient(http.DefaultClient, srv.URL)
	ctx := context.Background()

	code := func(err error) connect.Code { return connect.CodeOf(err) }
	del := func() error {
		_, err := c.Delete(ctx, connect.NewRequest(&cortexv1.DeleteRequest{Id: "x"}))
		return err
	}
	save := func() error {
		_, err := c.Save(ctx, connect.NewRequest(&cortexv1.SaveRequest{Text: "x"}))
		return err
	}
	list := func() error {
		_, err := c.List(ctx, connect.NewRequest(&cortexv1.ListRequest{}))
		return err
	}

	// Off: everything reaches the handler (which answers Unimplemented).
	assert.Equal(t, connect.CodeUnimplemented, code(del()))

	active = true
	err := del()
	assert.Equal(t, connect.CodeUnavailable, code(err), "writes are refused while maintenance is on")
	assert.Contains(t, err.Error(), "maintenance")
	assert.Equal(t, connect.CodeUnimplemented, code(save()), "queue-only writes pass: NATS buffers them")
	assert.Equal(t, connect.CodeUnimplemented, code(list()), "reads pass")

	active = false
	assert.Equal(t, connect.CodeUnimplemented, code(del()), "and writes resume afterwards")
}

type fakeReporter struct{ st *migrate.Status }

func (f fakeReporter) Status(context.Context) (*migrate.Status, error) { return f.st, nil }

func TestListMigrationsIsAdminOnly(t *testing.T) {
	s := &Service{}
	s.SetMigrations(fakeReporter{st: &migrate.Status{
		SchemaVersion: 1, Latest: 2,
		Migrations: []migrate.MigrationStatus{
			{Migration: migrate.Migration{ID: 1, Name: "one"}, Applied: true},
			{Migration: migrate.Migration{ID: 2, Name: "two"}},
		},
		Maintenance: &migrate.MaintenanceState{Reason: "copying"},
	}})
	req := connect.NewRequest(&cortexv1.ListMigrationsRequest{})

	_, err := s.ListMigrations(identity.Into(context.Background(), identity.Identity{UserID: "u", Role: identity.RoleUser}), req)
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	res, err := s.ListMigrations(identity.Into(context.Background(), identity.Identity{UserID: "a", Role: identity.RoleAdmin}), req)
	require.NoError(t, err)
	assert.EqualValues(t, 1, res.Msg.SchemaVersion)
	assert.EqualValues(t, 2, res.Msg.Latest)
	require.Len(t, res.Msg.Migrations, 2)
	assert.True(t, res.Msg.Migrations[0].Applied)
	assert.False(t, res.Msg.Migrations[1].Applied)
	assert.True(t, res.Msg.Maintenance)
	assert.Equal(t, "copying", res.Msg.MaintenanceReason)
}
