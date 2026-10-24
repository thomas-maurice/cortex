package outbox

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/gen/cortex/v1/cortexv1connect"
)

type fakeSender struct {
	mu   sync.Mutex
	sent []string
	fail map[string]error // by request id; "summary:<conv>" fails a summary
}

func (f *fakeSender) SummarizeSession(_ context.Context, r *connect.Request[cortexv1.SummarizeSessionRequest]) (*connect.Response[cortexv1.SummarizeSessionResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fail["summary:"+r.Msg.GetConversationId()]; err != nil {
		return nil, err
	}
	f.sent = append(f.sent, "summary:"+r.Msg.GetConversationId()+":"+r.Msg.GetText())
	return connect.NewResponse(&cortexv1.SummarizeSessionResponse{ConversationId: r.Msg.GetConversationId()}), nil
}

func (f *fakeSender) Save(_ context.Context, r *connect.Request[cortexv1.SaveRequest]) (*connect.Response[cortexv1.SaveResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fail[r.Msg.GetId()]; err != nil {
		return nil, err
	}
	f.sent = append(f.sent, r.Msg.GetId())
	return connect.NewResponse(&cortexv1.SaveResponse{Id: r.Msg.GetId()}), nil
}

var errDown = connect.NewError(connect.CodeUnavailable, errors.New("down"))

// queue stores a SaveRequest and returns its id. at orders the enqueue time.
func queue(t *testing.T, path, id, conv string, at int64, mut func(*cortexv1.SaveRequest)) {
	t.Helper()
	req := &cortexv1.SaveRequest{Id: &id, Text: "t-" + id, ConversationId: conv}
	if mut != nil {
		mut(req)
	}
	raw, err := proto.Marshal(req)
	require.NoError(t, err)
	require.NoError(t, Enqueue(path, Entry{ID: id, Request: raw, ConversationID: conv, EnqueuedAt: at}))
}

func queuedIDs(t *testing.T, path string) []string {
	t.Helper()
	all, err := ListAll(path)
	require.NoError(t, err)
	var out []string
	for _, e := range all {
		out = append(out, e.ID)
	}
	return out
}

// An entry may only leave the outbox once the server accepted it: a failed send
// that deleted it would lose the memory. A per-entry rejection must not hold up
// the entries behind it.
func TestDrainDeletesOnlyOnSuccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "o.db")
	queue(t, path, "a", "c", 1, nil)
	queue(t, path, "b", "c", 2, nil)
	f := &fakeSender{fail: map[string]error{"a": connect.NewError(connect.CodeInvalidArgument, errors.New("rejected"))}}

	res, err := Drain(context.Background(), path, f, nil, time.Second)
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, f.sent, "a failure must not stop later entries")
	assert.Equal(t, []string{"a"}, queuedIDs(t, path), "failed entry stays queued")
	assert.Equal(t, OutcomeFailed, res[0].Outcome)
	assert.Equal(t, OutcomeSent, res[1].Outcome)
}

// A corrupt value cannot be decoded, so it can never be sent; it must stay put
// (visible, droppable) and not block the healthy entries after it.
func TestDrainSkipsCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "o.db")
	queue(t, path, "ok", "c", 2, nil)
	putRaw(t, path, "bad", []byte("not json"))
	f := &fakeSender{}

	res, err := Drain(context.Background(), path, f, nil, time.Second)
	require.NoError(t, err)
	assert.Equal(t, []string{"ok"}, f.sent)
	assert.ElementsMatch(t, []string{"bad"}, queuedIDs(t, path))
	var corrupt int
	for _, r := range res {
		if r.Outcome == OutcomeCorrupt {
			corrupt++
		}
	}
	assert.Equal(t, 1, corrupt)
}

// A queued time-bound fact whose expiry passed must be dropped unsent: the
// server would reject a non-future valid_until, leaving a permanent stuck entry.
func TestDrainDropsExpired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "o.db")
	queue(t, path, "old", "c", 1, func(r *cortexv1.SaveRequest) {
		r.ValidUntil = timestamppb.New(time.Now().Add(30 * time.Second)) // inside the 1m margin
	})
	f := &fakeSender{}

	res, err := Drain(context.Background(), path, f, nil, time.Second)
	require.NoError(t, err)
	assert.Empty(t, f.sent)
	assert.Empty(t, queuedIDs(t, path))
	assert.Equal(t, OutcomeExpired, res[0].Outcome)
}

// The link consumer gives up if a link target never lands, so a queued target
// (even another session's) goes first; a chain sends deepest first.
func TestDrainSendsDependenciesFirstAcrossConversations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "o.db")
	queue(t, path, "w", "B", 1, nil)
	queue(t, path, "x", "B", 2, func(r *cortexv1.SaveRequest) { r.LinkTo = []string{"w"} })
	queue(t, path, "y", "A", 3, func(r *cortexv1.SaveRequest) { r.Supersedes = []string{"x"} })
	f := &fakeSender{}

	conv := "A"
	res, err := Drain(context.Background(), path, f, &conv, time.Second)
	require.NoError(t, err)
	assert.Equal(t, []string{"w", "x", "y"}, f.sent)
	assert.Empty(t, queuedIDs(t, path))
	assert.Len(t, res, 1, "only the session's own entries start a send")
}

// If the dependency cannot be sent, the dependent must stay queued behind it.
func TestSendWithDepsKeepsDependentWhenDependencyFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "o.db")
	queue(t, path, "x", "B", 1, nil)
	queue(t, path, "y", "A", 2, func(r *cortexv1.SaveRequest) { r.LinkTo = []string{"x"} })
	f := &fakeSender{fail: map[string]error{"x": errDown}}

	e, _, err := Get(path, "y")
	require.NoError(t, err)
	require.Error(t, SendWithDeps(context.Background(), path, f, e, nil))
	assert.Empty(t, f.sent)
	assert.ElementsMatch(t, []string{"x", "y"}, queuedIDs(t, path))
}

// A cycle (X links Y, Y links X) must terminate and send both.
func TestSendWithDepsCycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "o.db")
	queue(t, path, "x", "c", 1, func(r *cortexv1.SaveRequest) { r.LinkTo = []string{"y"} })
	queue(t, path, "y", "c", 2, func(r *cortexv1.SaveRequest) { r.LinkTo = []string{"x"} })
	f := &fakeSender{}
	e, _, _ := Get(path, "x")
	require.NoError(t, SendWithDeps(context.Background(), path, f, e, nil))
	assert.ElementsMatch(t, []string{"x", "y"}, f.sent)
}

func TestIsTransient(t *testing.T) {
	for code, want := range map[connect.Code]bool{
		connect.CodeUnavailable: true, connect.CodeDeadlineExceeded: true,
		connect.CodeInternal: true, connect.CodeUnknown: true, // R1
		connect.CodeInvalidArgument: false, connect.CodeUnauthenticated: false,
		connect.CodePermissionDenied: false, connect.CodeNotFound: false,
		connect.CodeAlreadyExists: false, connect.CodeResourceExhausted: false,
	} {
		assert.Equal(t, want, IsTransient(connect.NewError(code, errors.New("x"))), code.String())
	}
	assert.True(t, IsTransient(context.DeadlineExceeded))
	assert.False(t, IsTransient(errors.New("plain")))
	assert.False(t, IsTransient(nil))
}

// The real "server is down" error must classify as transient (resolves R1
// empirically: it is what a closed port actually produces).
func TestIsTransientRealConnectionRefused(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	c := cortexv1connect.NewMemoryServiceClient(http.DefaultClient, url)
	_, err := c.Save(context.Background(), connect.NewRequest(&cortexv1.SaveRequest{Text: "x"}))
	require.Error(t, err)
	t.Logf("code=%v err=%v", connect.CodeOf(err), err)
	assert.True(t, IsTransient(err))
}

func putRaw(t *testing.T, path, id string, val []byte) {
	t.Helper()
	db, err := open(path)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}
		return b.Put([]byte(id), val)
	}))
	require.NoError(t, db.Close())
}

// A transient failure means the server is unreachable: every later send would
// wait out its own timeout too (minutes for a large backlog, during which no
// new drain can start), so the drain stops and leaves the rest queued.
func TestDrainStopsWhenServerUnreachable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "o.db")
	queue(t, path, "a", "c", 1, nil)
	queue(t, path, "b", "c", 2, nil)
	f := &fakeSender{fail: map[string]error{"a": errDown}}

	res, err := Drain(context.Background(), path, f, nil, time.Second)
	require.NoError(t, err)
	assert.Empty(t, f.sent, "no send attempted after the server proved unreachable")
	assert.Equal(t, []string{"a", "b"}, queuedIDs(t, path))
	require.Len(t, res, 1)
	assert.Equal(t, OutcomeFailed, res[0].Outcome)
}

func putSummary(t *testing.T, path, conv, text string, at int64) {
	t.Helper()
	raw, err := proto.Marshal(&cortexv1.SummarizeSessionRequest{ConversationId: conv, Text: text})
	require.NoError(t, err)
	require.NoError(t, PutSummary(path, SummaryEntry{ConversationID: conv, Request: raw, EnqueuedAt: at, Preview: text}))
}

// One slot per conversation, latest wins: the final summary supersedes earlier
// offline attempts, and survives reopening the db (an MCP restart).
func TestSummarySlotLatestWinsAndPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "o.db")
	putSummary(t, path, "c", "first", 1)
	putSummary(t, path, "c", "second", 2)
	putSummary(t, path, "d", "other", 3)

	got, err := ListSummaries(path) // fresh open per call == a restarted process
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "second", got[0].Preview)
}

// Drain sends summaries only after the conversation's memory entries, honours
// the conversation filter, and clears the slot.
func TestDrainSendsSummaryAfterSaves(t *testing.T) {
	path := filepath.Join(t.TempDir(), "o.db")
	queue(t, path, "m1", "c", 1, nil)
	putSummary(t, path, "c", "sum c", 2)
	putSummary(t, path, "d", "sum d", 3)
	f := &fakeSender{}

	conv := "c"
	res, err := Drain(context.Background(), path, f, &conv, time.Second)
	require.NoError(t, err)
	assert.Equal(t, []string{"m1", "summary:c:sum c"}, f.sent)
	assert.True(t, res[len(res)-1].Summary)
	left, err := ListSummaries(path)
	require.NoError(t, err)
	require.Len(t, left, 1, "other conversation's slot untouched")
	assert.Equal(t, "d", left[0].ConversationID)
}

// A failed summary send must keep the slot (the summary is not lost); a
// transient save failure means the server is down, so no summary is attempted.
func TestDrainSummaryKeptOnFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "o.db")
	putSummary(t, path, "c", "sum", 1)
	f := &fakeSender{fail: map[string]error{"summary:c": errDown}}
	res, err := Drain(context.Background(), path, f, nil, time.Second)
	require.NoError(t, err)
	assert.Equal(t, OutcomeFailed, res[0].Outcome)
	left, _ := ListSummaries(path)
	assert.Len(t, left, 1)

	queue(t, path, "m1", "c", 1, nil)
	f = &fakeSender{fail: map[string]error{"m1": errDown}}
	_, err = Drain(context.Background(), path, f, nil, time.Second)
	require.NoError(t, err)
	assert.Empty(t, f.sent, "summary must not be sent after a transient save failure")
}

// If a newer summary replaces the slot while the old one is in flight, the
// drain's delete must not remove the newer one.
func TestDeleteSummaryOnlyIfUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "o.db")
	putSummary(t, path, "c", "new", 5)
	_, found, err := DeleteSummary(path, "c", 4)
	require.NoError(t, err)
	assert.False(t, found)
	_, found, err = DeleteSummary(path, "c", 5)
	require.NoError(t, err)
	assert.True(t, found)
}
