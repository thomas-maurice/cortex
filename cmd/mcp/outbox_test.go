package main

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/gen/cortex/v1/cortexv1connect"
	"github.com/thomas-maurice/cortex/internal/outbox"
)

// saveClient records Save requests and fails them with err (when set).
type saveClient struct {
	cortexv1connect.MemoryServiceClient
	mu   sync.Mutex
	reqs []*cortexv1.SaveRequest
	err  error
	// failID fails only the Save with this id.
	failID string
	block  bool
}

func (c *saveClient) Save(ctx context.Context, r *connect.Request[cortexv1.SaveRequest]) (*connect.Response[cortexv1.SaveResponse], error) {
	if c.block {
		<-ctx.Done()
		return nil, connect.NewError(connect.CodeDeadlineExceeded, ctx.Err())
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reqs = append(c.reqs, proto.Clone(r.Msg).(*cortexv1.SaveRequest))
	if c.err != nil || (c.failID != "" && c.failID == r.Msg.GetId()) {
		return nil, c.err
	}
	return connect.NewResponse(&cortexv1.SaveResponse{Id: r.Msg.GetId(), Status: "queued"}), nil
}

func (c *saveClient) ids() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, r := range c.reqs {
		out = append(out, r.GetId())
	}
	return out
}

func newOutboxDeps(t *testing.T, c cortexv1connect.MemoryServiceClient) *deps {
	t.Helper()
	return &deps{client: c, seen: map[string]bool{}, conversationID: "sess", outboxPath: filepath.Join(t.TempDir(), "outbox.db")}
}

func queued(t *testing.T, d *deps) []outbox.Entry {
	t.Helper()
	es, err := outbox.ListAll(d.outboxPath)
	require.NoError(t, err)
	return es
}

// Online: the minted id is sent, nothing is queued, and created_at is NOT set
// (an online save must take the server clock).
func TestSaveOnlineSendsMintedIDAndNoCreatedAt(t *testing.T) {
	c := &saveClient{}
	d := newOutboxDeps(t, c)
	_, out, err := d.save(context.Background(), nil, SaveIn{Text: "hello"})
	require.NoError(t, err)
	require.Len(t, c.reqs, 1)
	assert.Equal(t, c.reqs[0].GetId(), out.ID)
	assert.NotEmpty(t, out.ID)
	assert.Nil(t, c.reqs[0].GetCreatedAt())
	assert.Empty(t, queued(t, d))
}

// Server down: Claude gets a stable id back and the save is durably stored, with
// the relative expiry made absolute (it must not restart at replay time) and
// created_at = enqueue time.
func TestSaveOfflineQueuesWithStableID(t *testing.T) {
	for _, err := range []error{
		connect.NewError(connect.CodeUnavailable, errors.New("refused")),
		connect.NewError(connect.CodeInternal, errors.New("nats down")), // R1
	} {
		c := &saveClient{err: err}
		d := newOutboxDeps(t, c)
		before := time.Now()
		_, out, serr := d.save(context.Background(), nil, SaveIn{Text: "hello", ExpiresIn: "2d", LinkTo: []string{"zzz"}})
		require.NoError(t, serr)
		assert.Equal(t, "queued (offline)", out.Status)

		es := queued(t, d)
		require.Len(t, es, 1)
		assert.Equal(t, out.ID, es[0].ID, "the id returned to Claude is the id replayed")
		assert.Equal(t, "sess", es[0].ConversationID)
		req := &cortexv1.SaveRequest{}
		require.NoError(t, proto.Unmarshal(es[0].Request, req))
		assert.Equal(t, out.ID, req.GetId())
		assert.Empty(t, req.GetExpiresIn(), "expires_in must be absolute once queued")
		assert.WithinDuration(t, before.Add(48*time.Hour), req.GetValidUntil().AsTime(), 5*time.Second)
		assert.WithinDuration(t, before, req.GetCreatedAt().AsTime(), 5*time.Second)
	}
}

// A rejected save (bad input, auth) will never succeed on replay: return the
// error, queue nothing.
func TestSavePermanentErrorNotQueued(t *testing.T) {
	c := &saveClient{err: connect.NewError(connect.CodeInvalidArgument, errors.New("bad"))}
	d := newOutboxDeps(t, c)
	_, _, err := d.save(context.Background(), nil, SaveIn{Text: "hello"})
	require.Error(t, err)
	assert.Empty(t, queued(t, d))
}

func TestSaveOfflineFullOutboxSaysNotStored(t *testing.T) {
	old := outbox.MaxEntries
	outbox.MaxEntries = 0
	t.Cleanup(func() { outbox.MaxEntries = old })
	d := newOutboxDeps(t, &saveClient{err: connect.NewError(connect.CodeUnavailable, errors.New("x"))})
	_, _, err := d.save(context.Background(), nil, SaveIn{Text: "hello"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NOT stored")
}

// A call that times out must still tell Claude "queued (offline)" with the id:
// withTimeout used to replace every result with a bare timeout error, hiding that
// the memory was in fact stored locally.
func TestSaveTimeoutKeepsQueuedResult(t *testing.T) {
	d := newOutboxDeps(t, &saveClient{block: true})
	_, out, err := withTimeout(50*time.Millisecond, d.save)(context.Background(), nil, SaveIn{Text: "hello"})
	require.NoError(t, err)
	assert.Equal(t, "queued (offline)", out.Status)
	require.Len(t, queued(t, d), 1)
	assert.Equal(t, out.ID, queued(t, d)[0].ID)
}

func TestWithTimeoutStillFailsWithoutResult(t *testing.T) {
	h := func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, struct{}, error) {
		<-ctx.Done()
		return nil, struct{}{}, nil
	}
	_, _, err := withTimeout(20*time.Millisecond, h)(context.Background(), nil, struct{}{})
	require.Error(t, err)
}

func queueEntry(t *testing.T, d *deps, id, conv string, at int64) {
	t.Helper()
	req := &cortexv1.SaveRequest{Id: &id, Text: id}
	raw, err := proto.Marshal(req)
	require.NoError(t, err)
	require.NoError(t, outbox.Enqueue(d.outboxPath, outbox.Entry{ID: id, Request: raw, ConversationID: conv, EnqueuedAt: at}))
}

// A link target still queued (by ANOTHER session) must reach the server before
// the save that links to it, or the link consumer exhausts its retries.
func TestSaveSendsQueuedDependencyFirst(t *testing.T) {
	for _, field := range []string{"link", "supersede"} {
		c := &saveClient{}
		d := newOutboxDeps(t, c)
		queueEntry(t, d, "target", "other-session", 1)
		in := SaveIn{Text: "new"}
		if field == "link" {
			in.LinkTo = []string{"target"}
		} else {
			in.Supersedes = []string{"target"}
		}
		_, out, err := d.save(context.Background(), nil, in)
		require.NoError(t, err)
		assert.Equal(t, []string{"target", out.ID}, c.ids(), field)
		assert.Empty(t, queued(t, d), "delivered dependency leaves the outbox")
	}
}

// If the dependency cannot be delivered, the new save goes behind it in the
// outbox instead of racing ahead of its target.
func TestSaveQueuedBehindFailingDependency(t *testing.T) {
	c := &saveClient{err: connect.NewError(connect.CodeUnavailable, errors.New("x")), failID: "target"}
	d := newOutboxDeps(t, c)
	queueEntry(t, d, "target", "other-session", 1)
	_, out, err := d.save(context.Background(), nil, SaveIn{Text: "new", LinkTo: []string{"target"}})
	require.NoError(t, err)
	assert.Equal(t, "queued (offline)", out.Status)
	assert.Equal(t, []string{"target"}, c.ids(), "the dependent save must not be sent")
	assert.Len(t, queued(t, d), 2)
}

// Draining after a successful RPC replays this session's entries only.
func TestKickDrainSendsOwnSessionOnly(t *testing.T) {
	c := &saveClient{}
	d := newOutboxDeps(t, c)
	queueEntry(t, d, "mine", "sess", 1)
	queueEntry(t, d, "theirs", "other", 2)
	d.kickDrain()
	d.drainWG.Wait()
	assert.Equal(t, []string{"mine"}, c.ids())
	left := queued(t, d)
	require.Len(t, left, 1)
	assert.Equal(t, "theirs", left[0].ID)
}

// sumClient records SummarizeSession calls and fails them with err (when set).
type sumClient struct {
	cortexv1connect.MemoryServiceClient
	mu    sync.Mutex
	texts []string
	err   error
}

func (c *sumClient) SummarizeSession(_ context.Context, r *connect.Request[cortexv1.SummarizeSessionRequest]) (*connect.Response[cortexv1.SummarizeSessionResponse], error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	c.texts = append(c.texts, r.Msg.GetText())
	return connect.NewResponse(&cortexv1.SummarizeSessionResponse{ConversationId: r.Msg.GetConversationId(), Status: "ok"}), nil
}

func (c *sumClient) Save(context.Context, *connect.Request[cortexv1.SaveRequest]) (*connect.Response[cortexv1.SaveResponse], error) {
	return connect.NewResponse(&cortexv1.SaveResponse{}), nil
}

func slotTexts(t *testing.T, d *deps) []string {
	t.Helper()
	sl, err := outbox.ListSummaries(d.outboxPath)
	require.NoError(t, err)
	var out []string
	for _, s := range sl {
		req := &cortexv1.SummarizeSessionRequest{}
		require.NoError(t, proto.Unmarshal(s.Request, req))
		out = append(out, req.GetText())
	}
	return out
}

var errUnavailable = connect.NewError(connect.CodeUnavailable, errors.New("down"))

// The end-of-session summary is the one thing a down server must not lose; the
// second offline call replaces the first (one slot per conversation), and the
// slot is on disk so a restarted MCP (new deps, same file) still sees it.
func TestSummarizeOfflineStoresLatestOnly(t *testing.T) {
	c := &sumClient{err: errUnavailable}
	d := newOutboxDeps(t, c)
	res, out, err := d.summarize(context.Background(), nil, SummarizeIn{Text: "first"})
	require.NoError(t, err, "an offline summary is not an error")
	assert.Equal(t, queuedStatus, out.Status)
	assert.Contains(t, resultText(t, res), "summary stored offline; will sync")
	_, _, err = d.summarize(context.Background(), nil, SummarizeIn{Text: "second"})
	require.NoError(t, err)

	restarted := &deps{client: c, seen: map[string]bool{}, conversationID: "sess", outboxPath: d.outboxPath}
	assert.Equal(t, []string{"second"}, slotTexts(t, restarted))
}

// A permanent rejection will never succeed on replay: return it, store nothing.
func TestSummarizePermanentErrorNotStored(t *testing.T) {
	d := newOutboxDeps(t, &sumClient{err: connect.NewError(connect.CodeInvalidArgument, errors.New("bad"))})
	_, _, err := d.summarize(context.Background(), nil, SummarizeIn{Text: "x"})
	require.Error(t, err)
	assert.Empty(t, slotTexts(t, d))
}

// A stale offline slot must not later overwrite a newer summary that reached
// the server: a successful summarize clears it.
func TestSummarizeSuccessClearsStaleSlot(t *testing.T) {
	c := &sumClient{err: errUnavailable}
	d := newOutboxDeps(t, c)
	_, _, err := d.summarize(context.Background(), nil, SummarizeIn{Text: "old"})
	require.NoError(t, err)
	require.Equal(t, []string{"old"}, slotTexts(t, d))

	c.err = nil
	_, _, err = d.summarize(context.Background(), nil, SummarizeIn{Text: "new"})
	require.NoError(t, err)
	assert.Empty(t, slotTexts(t, d))
	d.kickDrain()
	d.drainWG.Wait()
	assert.Equal(t, []string{"new"}, c.texts, "the stale summary must never be sent")
}

// The background drain delivers the slot once the server is back.
func TestKickDrainSendsSummarySlot(t *testing.T) {
	c := &sumClient{}
	d := newOutboxDeps(t, c)
	raw, err := proto.Marshal(&cortexv1.SummarizeSessionRequest{ConversationId: "sess", Text: "offline one"})
	require.NoError(t, err)
	require.NoError(t, outbox.PutSummary(d.outboxPath, outbox.SummaryEntry{ConversationID: "sess", Request: raw}))
	d.kickDrain()
	d.drainWG.Wait()
	assert.Equal(t, []string{"offline one"}, c.texts)
	assert.Empty(t, slotTexts(t, d))
}
