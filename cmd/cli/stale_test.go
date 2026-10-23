package main

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
)

// The server works in whole days, so "90d"/"12w"/hours must all land on the
// right day count, and sub-day durations must round UP (never silently become
// "0 = disabled", which would flag brand-new memories).
func TestParseAgeDays(t *testing.T) {
	for in, want := range map[string]int32{"90d": 90, "12w": 84, "2160h": 90, "25h": 2, "1h": 1, "0": 0, " 30d ": 30} {
		got, err := parseAgeDays(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
	for _, bad := range []string{"", "d", "-5d", "abc", "1.5d", "99999999d", "-1h"} {
		_, err := parseAgeDays(bad)
		assert.Error(t, err, bad)
	}
}

func TestApplyStaleFlags(t *testing.T) {
	req := &cortexv1.ListRequest{}
	require.NoError(t, applyStaleFlags(req, "", ""))
	assert.Equal(t, cortexv1.StaleMode_STALE_MODE_NEVER_RECALLED, req.Stale)
	assert.Nil(t, req.MinAgeDays, "unset min-age must leave the server default (30d) in charge")

	req = &cortexv1.ListRequest{}
	require.NoError(t, applyStaleFlags(req, "90d", "0"))
	assert.Equal(t, cortexv1.StaleMode_STALE_MODE_NOT_RECALLED_SINCE, req.Stale)
	assert.EqualValues(t, 90, req.StaleDays)
	require.NotNil(t, req.MinAgeDays, "explicit --min-age 0 must be sent (disables the guard), not mistaken for unset")
	assert.EqualValues(t, 0, *req.MinAgeDays)

	assert.Error(t, applyStaleFlags(&cortexv1.ListRequest{}, "0d", ""))
	assert.Error(t, applyStaleFlags(&cortexv1.ListRequest{}, "x", ""))
}

type fakeDeleter struct {
	single []string
	many   [][]string
	resp   *cortexv1.DeleteManyResponse
	err    error
}

func (f *fakeDeleter) Delete(_ context.Context, r *connect.Request[cortexv1.DeleteRequest]) (*connect.Response[cortexv1.DeleteResponse], error) {
	f.single = append(f.single, r.Msg.GetId())
	return connect.NewResponse(&cortexv1.DeleteResponse{}), f.err
}

func (f *fakeDeleter) DeleteMany(_ context.Context, r *connect.Request[cortexv1.DeleteManyRequest]) (*connect.Response[cortexv1.DeleteManyResponse], error) {
	f.many = append(f.many, r.Msg.GetIds())
	return connect.NewResponse(f.resp), f.err
}

func TestRunDeleteSingleUsesDelete(t *testing.T) {
	f := &fakeDeleter{}
	var buf bytes.Buffer
	require.NoError(t, runDelete(context.Background(), f, &buf, []string{" a "}))
	assert.Equal(t, []string{"a"}, f.single)
	assert.Empty(t, f.many, "one id keeps the plain Delete path")
}

// Several ids go in ONE DeleteMany call, and a partial failure must surface as a
// non-zero exit (error) so scripts notice, while still printing per-id outcomes.
func TestRunDeleteManyReportsPartialFailure(t *testing.T) {
	f := &fakeDeleter{resp: &cortexv1.DeleteManyResponse{Deleted: 1, Results: []*cortexv1.DeleteManyResult{
		{Id: "a", Deleted: true}, {Id: "b", Error: "not found"},
	}}}
	var buf bytes.Buffer
	err := runDelete(context.Background(), f, &buf, []string{"a", "b"})
	require.Error(t, err)
	assert.Equal(t, [][]string{{"a", "b"}}, f.many)
	assert.Empty(t, f.single)
	assert.Contains(t, buf.String(), "deleted a")
	assert.Contains(t, buf.String(), "FAILED  b: not found")

	f.resp = &cortexv1.DeleteManyResponse{Deleted: 2, Results: []*cortexv1.DeleteManyResult{{Id: "a", Deleted: true}, {Id: "b", Deleted: true}}}
	require.NoError(t, runDelete(context.Background(), f, &buf, []string{"a", "b"}))

	assert.Error(t, runDelete(context.Background(), f, &buf, []string{" ", ""}))
	f.err = errors.New("boom")
	assert.Error(t, runDelete(context.Background(), f, &buf, []string{"a", "b"}))
}

// Usage is only tracked since the server's epoch: a window longer than that must
// be flagged, or "nothing stale" would be mistaken for "everything is used".
func TestTrackingNotice(t *testing.T) {
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	young := trackingNotice(now.Add(-10*24*time.Hour), true, 90, now)
	assert.Contains(t, young, "usage tracked since 2026-05-2")
	assert.Contains(t, young, "limited")
	old := trackingNotice(now.Add(-200*24*time.Hour), true, 90, now)
	assert.Contains(t, old, "usage tracked since")
	assert.NotContains(t, old, "limited")
	assert.Contains(t, trackingNotice(time.Time{}, false, 30, now), "unknown")

	assert.EqualValues(t, 90, staleWindowDays(&cortexv1.ListRequest{Stale: cortexv1.StaleMode_STALE_MODE_NOT_RECALLED_SINCE, StaleDays: 90}))
	assert.EqualValues(t, 30, staleWindowDays(&cortexv1.ListRequest{Stale: cortexv1.StaleMode_STALE_MODE_NEVER_RECALLED}))
}

// --no-expiry on save and --expires are contradictory; rejected client-side
// before any request is made.
func TestSaveRejectsExpiresWithNoExpiry(t *testing.T) {
	cmd := saveCmd()
	cmd.SetArgs([]string{"--expires", "7d", "--no-expiry", "text"})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	err := cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")
	assert.NotNil(t, saveCmd().Flags().Lookup("no-expiry"))
}
