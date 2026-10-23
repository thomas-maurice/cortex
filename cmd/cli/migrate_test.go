package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
)

// An operator reads this to decide whether the upgrade is stuck: pending vs
// applied, and whether writers are currently paused, must be unmistakable.
func TestFormatMigrations(t *testing.T) {
	out := formatMigrations(&cortexv1.ListMigrationsResponse{
		SchemaVersion: 1, Latest: 2,
		Migrations: []*cortexv1.SchemaMigration{
			{Id: 1, Name: "tags-field-tokenization", Applied: true, AppliedAt: timestamppb.New(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)), DurationMs: 1500},
			{Id: 2, Name: "next"},
		},
		Maintenance: true, MaintenanceReason: "copying Memory", LockHolder: "server a/1",
	})
	assert.Contains(t, out, "schema version 1 of 2 (1 pending)")
	assert.Contains(t, out, "applied   2026-01-02T03:04:05Z  took 1.5s  tags-field-tokenization")
	assert.Contains(t, out, "PENDING   next")
	assert.Contains(t, out, "MAINTENANCE MODE: writes paused (copying Memory)")
	assert.Contains(t, out, "migration running on: server a/1")

	assert.Contains(t, formatMigrations(&cortexv1.ListMigrationsResponse{SchemaVersion: 1, Latest: 1}), "up to date")
}
