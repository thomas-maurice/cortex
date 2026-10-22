package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/thomas-maurice/cortex/internal/bus"
	"github.com/thomas-maurice/cortex/internal/embed"
	"github.com/thomas-maurice/cortex/internal/identity"
	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/store"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
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
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(del("", false)))

	// from == to (the new name is trimmed; from is matched exactly) is a successful no-op.
	resp, err := svc.RenameTag(ctx, connect.NewRequest(&cortexv1.RenameTagRequest{From: "a", To: " a "}))
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

func lastIndexMsg(t *testing.T, js jetstream.JetStream) memory.Record {
	t.Helper()
	ctx := context.Background()
	stream, err := js.Stream(ctx, memory.StreamName)
	require.NoError(t, err)
	m, err := stream.GetLastMsgForSubject(ctx, memory.SubjectIndex)
	require.NoError(t, err)
	var rec memory.Record
	require.NoError(t, json.Unmarshal(m.Data, &rec))
	return rec
}

// Tags are canonicalised at write time (trim, drop empty, dedup) so look-alikes
// like "k8s " can never exist: rename/delete match tags exactly.
func TestSaveNormalizesTags(t *testing.T) {
	_, js := natstest.Start(t)
	require.NoError(t, bus.EnsureStream(context.Background(), js))
	svc := &Service{js: js, cfg: Config{DefaultNamespace: "d"}}
	_, err := svc.Save(context.Background(), connect.NewRequest(&cortexv1.SaveRequest{Text: "x", Tags: []string{" k8s ", "k8s", "", "  ", "a"}}))
	require.NoError(t, err)
	rec := lastIndexMsg(t, js)
	assert.Equal(t, []string{"k8s", "a"}, rec.Tags)
	assert.False(t, rec.KeepTags, "a save carries a genuine tag set")
}

// A rename/delete must not be undone by queued snapshots: while a reindex or
// restore is publishing this tenant's memories it is refused outright.
func TestTagChangeRejectedDuringReindexOrRestore(t *testing.T) {
	for name, hold := range map[string]func(s *Service) func(){
		"reindex": func(s *Service) func() { r, _ := s.reindexGate.TryAcquire("u1"); return r },
		"restore": func(s *Service) func() { r, _ := s.restoreGate.TryAcquire("u1"); return r },
		"admin":   func(s *Service) func() { r, _ := s.adminGate.TryAcquire(adminGateKey); return r },
	} {
		t.Run(name, func(t *testing.T) {
			svc := &Service{cfg: Config{MultiTenant: true}}
			rel := hold(svc)
			_, err := svc.RenameTag(gateTestCtx("u1"), connect.NewRequest(&cortexv1.RenameTagRequest{From: "a", To: "b"}))
			assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
			assert.Contains(t, err.Error(), "reindex or restore")
			_, err = svc.DeleteTag(gateTestCtx("u1"), connect.NewRequest(&cortexv1.DeleteTagRequest{Tag: "a"}))
			assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
			rel()
		})
	}
	// Another tenant's reindex does not block this one.
	svc := &Service{cfg: Config{MultiTenant: true}}
	rel, _ := svc.reindexGate.TryAcquire("other")
	defer rel()
	assert.NoError(t, svc.rejectDuringBulkRewrite(gateTestCtx("u1")))
}

// TestStaleSnapshotProducersSetKeepTags: an edit that does not replace tags and a
// reindex both republish the STORED tags; the worker must keep whatever is stored
// at commit time (tag rename may have landed since), so these producers must set
// keepTags. replace_tags edits carry an explicit set and must not.
func TestStaleSnapshotProducersSetKeepTags(t *testing.T) {
	rest, grpc := os.Getenv("CORTEX_TEST_WEAVIATE_REST"), os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST and CORTEX_TEST_WEAVIATE_GRPC to run this integration test")
	}
	mt := os.Getenv("CORTEX_TEST_MULTI_TENANT") != ""
	ctx := context.Background()
	st, err := store.New(rest, grpc)
	require.NoError(t, err)
	st.SetMultiTenant(mt)
	_ = st.DeleteClass(ctx)
	require.NoError(t, st.EnsureSchema(ctx))
	t.Cleanup(func() { _ = st.DeleteClass(ctx) })

	_, js := natstest.Start(t)
	require.NoError(t, bus.EnsureStream(ctx, js))
	ollama := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"embeddings":[[1,0,0,0]]}`))
	}))
	t.Cleanup(ollama.Close)
	svc := &Service{js: js, store: st, log: slog.Default(), embedder: embed.New(ollama.URL, "m"), cfg: Config{MultiTenant: mt, BackupDir: t.TempDir()}}
	uctx := ctx
	tenant := ""
	if mt {
		tenant = "user-keep"
		uctx = identity.Into(ctx, identity.Identity{UserID: tenant, Role: identity.RoleUser})
	}
	rec := memory.Record{ID: uuid.NewString(), Text: "t", Namespace: "n", Tags: []string{"old"}, CreatedAt: time.Now().UTC()}
	require.NoError(t, st.Tenant(tenant).Upsert(ctx, rec, []float32{1, 0, 0, 0}))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, ok, err := st.Tenant(tenant).Get(ctx, rec.ID)
		require.NoError(c, err)
		assert.True(c, ok)
	}, 10*time.Second, 200*time.Millisecond)

	_, err = svc.UpdateMemory(uctx, connect.NewRequest(&cortexv1.UpdateMemoryRequest{Id: rec.ID, Text: "edited"}))
	require.NoError(t, err)
	got := lastIndexMsg(t, js)
	assert.True(t, got.KeepTags, "text-only edit must not carry tags as an instruction")

	_, err = svc.UpdateMemory(uctx, connect.NewRequest(&cortexv1.UpdateMemoryRequest{Id: rec.ID, Text: "edited", ReplaceTags: true, Tags: []string{" x ", "x"}}))
	require.NoError(t, err)
	got = lastIndexMsg(t, js)
	assert.False(t, got.KeepTags)
	assert.Equal(t, []string{"x"}, got.Tags)

	_, err = svc.Reindex(uctx, connect.NewRequest(&cortexv1.ReindexRequest{Namespace: "*"}))
	require.NoError(t, err)
	assert.True(t, lastIndexMsg(t, js).KeepTags, "a reindex snapshot must not undo a later tag rename")
}
