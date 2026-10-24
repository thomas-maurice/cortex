package main

import (
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
)

// migrateCmd implements `cortex migrate`. Schema migrations run automatically
// when the server starts (see docs/DESIGN.md "Schema migrations"); this command
// only reports them. (Not to be confused with `migrate-mt`, the one-shot
// single-tenant to multi-tenant conversion.)
func migrateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Weaviate schema migrations (run automatically at server start)",
	}
	cmd.AddCommand(&cobra.Command{
		Use:   "status",
		Short: "List schema migrations and whether each has been applied (admin only)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			resp, err := client().ListMigrations(cmd.Context(), connect.NewRequest(&cortexv1.ListMigrationsRequest{}))
			if err != nil {
				return err
			}
			fmt.Print(formatMigrations(resp.Msg))
			return nil
		},
	})
	return cmd
}

func formatMigrations(m *cortexv1.ListMigrationsResponse) string {
	var b strings.Builder
	state := "up to date"
	if m.GetSchemaVersion() < m.GetLatest() {
		state = fmt.Sprintf("%d pending", m.GetLatest()-m.GetSchemaVersion())
	}
	fmt.Fprintf(&b, "schema version %d of %d (%s)\n", m.GetSchemaVersion(), m.GetLatest(), state)
	for _, mg := range m.GetMigrations() {
		if mg.GetApplied() && mg.GetAssumed() {
			fmt.Fprintf(&b, "  %3d  assumed   %s  (fresh install, not run)  %s\n", mg.GetId(), mg.GetAppliedAt().AsTime().Format(time.RFC3339), mg.GetName())
		} else if mg.GetApplied() {
			fmt.Fprintf(&b, "  %3d  applied   %s  took %s  %s\n", mg.GetId(), mg.GetAppliedAt().AsTime().Format(time.RFC3339),
				time.Duration(mg.GetDurationMs())*time.Millisecond, mg.GetName())
		} else {
			fmt.Fprintf(&b, "  %3d  PENDING   %s\n", mg.GetId(), mg.GetName())
		}
	}
	if m.GetLockHolder() != "" {
		fmt.Fprintf(&b, "migration running on: %s\n", m.GetLockHolder())
	}
	if m.GetMaintenance() {
		fmt.Fprintf(&b, "MAINTENANCE MODE: writes paused (%s)\n", m.GetMaintenanceReason())
	}
	return b.String()
}
