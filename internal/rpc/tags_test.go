package rpc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/dlock"
	"github.com/thomas-maurice/cortex/internal/natstest"
)

// TestTagRPCValidation: input is rejected before any store access (svc.store is
// nil here, so reaching it would panic). The reserved `preference` tag needs an
// explicit confirm, since silently renaming/deleting it would break the
// cortex-prefs SessionStart hook for every future session.
func TestTagRPCValidation(t *testing.T) {
	svc := &Service{cfg: Config{MultiTenant: true}}
	ctx := gateTestCtx("u1")
	rename := func(from, to string, confirm bool) error {
		_, err := svc.RenameTag(ctx, connect.NewRequest(&cortexv1.RenameTagRequest{From: from, To: to, ConfirmPreference: confirm}))
		return err
	}
	del := func(tag string, confirm bool) error {
		_, err := svc.DeleteTag(ctx, connect.NewRequest(&cortexv1.DeleteTagRequest{Tag: tag, ConfirmPreference: confirm}))
		return err
	}

	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(rename("", "x", false)))
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(rename("a", "  ", false)), "whitespace-only is empty")
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(rename("a", strings.Repeat("x", maxTagLen+1), false)))
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(del(" ", false)))

	// from == to (after trimming) is a successful no-op.
	resp, err := svc.RenameTag(ctx, connect.NewRequest(&cortexv1.RenameTagRequest{From: " a ", To: "a"}))
	require.NoError(t, err)
	assert.Zero(t, resp.Msg.GetMemoriesUpdated())

	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(rename("preference", "prefs", false)))
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(rename("prefs", "Preference", false)), "merging INTO it changes the hook too, case-insensitively")
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(del("preference", false)))
	assert.Contains(t, del("preference", false).Error(), "preference")
}

func TestTagRPCsSingleFlightPerTenant(t *testing.T) {
	// Each rename/delete scans the whole tenant, so one in flight per tenant.
	calls := map[string]func(s *Service) func(context.Context) error{
		"RenameTag": func(s *Service) func(context.Context) error {
			return func(ctx context.Context) error {
				_, err := s.RenameTag(ctx, connect.NewRequest(&cortexv1.RenameTagRequest{From: "a", To: "b"}))
				return err
			}
		},
		"DeleteTag": func(s *Service) func(context.Context) error {
			return func(ctx context.Context) error {
				_, err := s.DeleteTag(ctx, connect.NewRequest(&cortexv1.DeleteTagRequest{Tag: "a"}))
				return err
			}
		},
	}
	for name, mk := range calls {
		t.Run(name, func(t *testing.T) {
			svc := &Service{cfg: Config{MultiTenant: true}}
			holdFirst(t, svc, mk(svc))
		})
	}
}

// TestMemoryLockerSharesWorkerKey: the tag rewrite must contend on the exact KV
// key the worker's index commit holds ("mem" + tenant + id in cortex_locks),
// otherwise it would not exclude a concurrent index write at all.
func TestMemoryLockerSharesWorkerKey(t *testing.T) {
	_, js := natstest.Start(t)
	ctx := gateTestCtx("tenant-1")
	svc := &Service{js: js, cfg: Config{MultiTenant: true}}
	lock, err := svc.memoryLocker(ctx)
	require.NoError(t, err)

	// "The worker": a separate Locker on the same bucket holds the memory.
	worker, err := dlock.New(ctx, js, dlock.DefaultBucket, dlock.DefaultTTL, "worker")
	require.NoError(t, err)
	hctx, releaseWorker, err := worker.AcquireAll(ctx, []string{dlock.Key("mem", "tenant-1", "mem-1")}, time.Second)
	require.NoError(t, err)
	_ = hctx

	got := make(chan func(), 1)
	go func() {
		_, rel, err := lock(ctx, "mem-1")
		if err == nil {
			got <- rel
		}
	}()
	select {
	case <-got:
		t.Fatal("tag lock acquired while the worker held the same memory")
	case <-time.After(400 * time.Millisecond):
	}
	// A different memory is not blocked.
	_, relOther, err := lock(ctx, "mem-2")
	require.NoError(t, err)
	relOther()

	releaseWorker()
	select {
	case rel := <-got:
		rel()
	case <-time.After(5 * time.Second):
		t.Fatal("tag lock never acquired after the worker released")
	}
}

// TestPartialErrReportsProgress: a failed bulk tag op is partially applied; the
// error must say how many memories were already changed and keep the cause/code.
func TestPartialErrReportsProgress(t *testing.T) {
	cause := connect.NewError(connect.CodeUnavailable, errors.New("weaviate down"))
	err := partialErr("rename tag", 7, cause)
	assert.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "after updating 7 memories")
	assert.Contains(t, err.Error(), "weaviate down")
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(partialErr("x", 0, errors.New("boom"))))
}
