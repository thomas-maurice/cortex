package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// export reads the BackupSelf payload (uncapped) instead of the capped List RPC,
// so it must (a) honour -n filtering client-side, (b) emit the exact shape
// `cortex import` reads, and (c) keep newest-first order.
func TestExportRecordsFromBackup(t *testing.T) {
	t0 := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	env := map[string]any{
		"version": 1,
		"tenants": []any{map[string]any{
			"tenant": "u1",
			"memories": []memory.Record{
				{ID: "old", Text: "a", Namespace: "alpha", Tags: []string{"x"}, CreatedAt: t0, Dims: 4, LinkedIDs: []string{"new"}, NotDuplicateOf: []string{"z"}},
				{ID: "new", Text: "b", Namespace: "alpha", CreatedAt: t0.Add(time.Hour)},
				{ID: "other", Text: "c", Namespace: "beta", CreatedAt: t0.Add(2 * time.Hour)},
			},
		}},
	}
	data, err := json.Marshal(env)
	require.NoError(t, err)

	all, err := exportRecordsFromBackup(data, "*")
	require.NoError(t, err)
	require.Len(t, all, 3)
	assert.Equal(t, []string{"other", "new", "old"}, []string{all[0].ID, all[1].ID, all[2].ID}, "newest first")

	alpha, err := exportRecordsFromBackup(data, "alpha")
	require.NoError(t, err)
	require.Len(t, alpha, 2, "namespace filter must be exact")
	assert.Equal(t, "new", alpha[0].ID)

	old := alpha[1]
	assert.Equal(t, exportRecord{
		ID: "old", Text: "a", Namespace: "alpha", Tags: []string{"x"},
		CreatedAt: "2026-01-02T03:04:05Z", Dims: 4, LinkedIDs: []string{"new"}, NotDuplicateOf: []string{"z"},
	}, old)

	// Round-trips through the JSON that `cortex import` parses.
	out, err := json.Marshal(alpha)
	require.NoError(t, err)
	var back []exportRecord
	require.NoError(t, json.Unmarshal(out, &back))
	assert.Equal(t, alpha, back)

	none, err := exportRecordsFromBackup(data, "nope")
	require.NoError(t, err)
	assert.NotNil(t, none, "empty result must marshal as [] not null")
	assert.Empty(t, none)

	_, err = exportRecordsFromBackup([]byte("not json"), "*")
	assert.Error(t, err)
}
