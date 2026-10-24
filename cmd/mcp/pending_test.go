package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/gen/cortex/v1/cortexv1connect"
	"github.com/thomas-maurice/cortex/internal/outbox"
)

// readClient fakes the read/delete RPCs; err (when set) fails all of them.
type readClient struct {
	cortexv1connect.MemoryServiceClient
	err        error
	deleteCall int
}

func (c *readClient) Search(context.Context, *connect.Request[cortexv1.SearchRequest]) (*connect.Response[cortexv1.SearchResponse], error) {
	if c.err != nil {
		return nil, c.err
	}
	return connect.NewResponse(&cortexv1.SearchResponse{}), nil
}

func (c *readClient) RecallSession(context.Context, *connect.Request[cortexv1.RecallSessionRequest]) (*connect.Response[cortexv1.RecallSessionResponse], error) {
	if c.err != nil {
		return nil, c.err
	}
	return connect.NewResponse(&cortexv1.RecallSessionResponse{}), nil
}

func (c *readClient) GetMemory(context.Context, *connect.Request[cortexv1.GetMemoryRequest]) (*connect.Response[cortexv1.GetMemoryResponse], error) {
	return nil, c.err
}

func (c *readClient) Delete(context.Context, *connect.Request[cortexv1.DeleteRequest]) (*connect.Response[cortexv1.DeleteResponse], error) {
	c.deleteCall++
	return nil, c.err
}

var errDown = connect.NewError(connect.CodeUnavailable, errors.New("connection refused"))

func seedQueued(t *testing.T, d *deps, id, conv, text string) {
	t.Helper()
	raw, err := proto.Marshal(&cortexv1.SaveRequest{Id: &id, Text: text, Namespace: "ns"})
	require.NoError(t, err)
	require.NoError(t, outbox.Enqueue(d.outboxPath, outbox.Entry{
		ID: id, Request: raw, ConversationID: conv, Preview: outbox.PreviewOf(text), EnqueuedAt: time.Now().UnixMicro(),
	}))
}

// The model must be told a queued save is not searchable and how it syncs.
func TestSaveOfflineMessageWarnsNotSearchable(t *testing.T) {
	d := newOutboxDeps(t, &saveClient{err: errDown})
	seedQueued(t, d, "other-1", "someone-else", "old")
	res, out, err := d.save(context.Background(), nil, SaveIn{Text: "hello"})
	require.NoError(t, err)
	msg := resultText(t, res)
	assert.Contains(t, msg, "local offline outbox")
	assert.Contains(t, msg, "id="+out.ID)
	assert.Contains(t, msg, "NOT searchable or visible to other sessions until synced")
	assert.Contains(t, msg, "cortex outbox flush")
	assert.Contains(t, msg, "Pending: 2 (1 this session)")
	assert.Equal(t, 2, out.PendingOffline)
}

// Search success: footer + field only when something is pending.
func TestSearchPendingFooter(t *testing.T) {
	d := newOutboxDeps(t, &readClient{})
	res, out, err := d.search(context.Background(), nil, SearchIn{Query: "q"})
	require.NoError(t, err)
	assert.NotContains(t, resultText(t, res), "queued offline")
	assert.Zero(t, out.PendingOffline)

	seedQueued(t, d, "a", "sess", "mine")
	seedQueued(t, d, "b", "other", "theirs")
	res, out, err = d.search(context.Background(), nil, SearchIn{Query: "q"})
	require.NoError(t, err)
	assert.Contains(t, resultText(t, res), "Note: 2 saves are queued offline and not searchable yet (1 from this session).")
	assert.Equal(t, 2, out.PendingOffline)

	rres, rout, err := d.recall(context.Background(), nil, RecallIn{Query: "q"})
	require.NoError(t, err)
	assert.Contains(t, resultText(t, rres), "2 saves are queued offline and not searchable yet")
	assert.Equal(t, 2, rout.PendingOffline)
}

// An unreachable server must never look like an empty result, and the session's
// queued saves are listed (capped).
func TestSearchUnreachableIsErrorListingQueued(t *testing.T) {
	d := newOutboxDeps(t, &readClient{err: errDown})
	for i := 0; i < maxListedPending+3; i++ {
		seedQueued(t, d, fmt.Sprintf("id-%02d", i), "sess", fmt.Sprintf("preview %02d", i))
	}
	seedQueued(t, d, "x", "other", "foreign")

	for name, call := range map[string]func() error{
		"search": func() error { _, _, err := d.search(context.Background(), nil, SearchIn{Query: "q"}); return err },
		"recall": func() error { _, _, err := d.recall(context.Background(), nil, RecallIn{Query: "q"}); return err },
		"get":    func() error { _, _, err := d.get(context.Background(), nil, GetIn{ID: "nope"}); return err },
	} {
		err := call()
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "unreachable", name)
		assert.Contains(t, err.Error(), "no results ≠ nothing stored", name)
		assert.Contains(t, err.Error(), "id-00 preview 00", name)
		assert.Contains(t, err.Error(), "+3 more", name)
		assert.NotContains(t, err.Error(), "preview 12", name)
		assert.Contains(t, err.Error(), "1 more queued by other sessions", name)
	}
}

// A permanent error is passed through, not relabelled "unreachable".
func TestSearchPermanentErrorUntouched(t *testing.T) {
	perm := connect.NewError(connect.CodePermissionDenied, errors.New("nope"))
	d := newOutboxDeps(t, &readClient{err: perm})
	_, _, err := d.search(context.Background(), nil, SearchIn{Query: "q"})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "unreachable")
}

// Reading a broken outbox must not fail the tool call.
func TestPendingUnreadableOutboxOmitsHint(t *testing.T) {
	d := newOutboxDeps(t, &readClient{})
	d.outboxPath = t.TempDir() // a directory: bbolt cannot open it
	res, _, err := d.search(context.Background(), nil, SearchIn{Query: "q"})
	require.NoError(t, err)
	assert.NotContains(t, resultText(t, res), "queued offline")
}

// get works on a queued id, for NotFound and for an unreachable server.
func TestGetQueuedID(t *testing.T) {
	for name, cerr := range map[string]error{
		"notfound":    connect.NewError(connect.CodeNotFound, errors.New("missing")),
		"unreachable": errDown,
	} {
		d := newOutboxDeps(t, &readClient{err: cerr})
		seedQueued(t, d, "q1", "sess", "queued body")
		res, hit, err := d.get(context.Background(), nil, GetIn{ID: "q1"})
		require.NoError(t, err, name)
		assert.Equal(t, "queued (offline)", hit.Status, name)
		assert.Equal(t, "queued body", hit.Text, name)
		assert.Contains(t, resultText(t, res), "queued (offline)", name)
	}
}

// delete of a queued id removes it from the outbox even when the server is down.
func TestDeleteQueuedID(t *testing.T) {
	c := &readClient{err: errDown}
	d := newOutboxDeps(t, c)
	seedQueued(t, d, "q1", "sess", "x")
	seedQueued(t, d, "q2", "sess", "y")
	res, _, err := d.del(context.Background(), nil, DeleteIn{ID: "q1"})
	require.NoError(t, err)
	assert.Contains(t, resultText(t, res), "removed from offline outbox")
	left := queued(t, d)
	require.Len(t, left, 1)
	assert.Equal(t, "q2", left[0].ID)

	// Unknown id + unreachable server: still an error.
	_, _, err = d.del(context.Background(), nil, DeleteIn{ID: "nope"})
	require.Error(t, err)
}

// Startup instructions mention leftovers iff there are any.
func TestStartupPendingNotice(t *testing.T) {
	d := newOutboxDeps(t, &readClient{})
	assert.Empty(t, startupPendingNotice(d.outboxPath, time.Now()))
	assert.Empty(t, startupPendingNotice("", time.Now()))

	seedQueued(t, d, "old", "earlier", "x")
	n := startupPendingNotice(d.outboxPath, time.Now().Add(3*time.Hour))
	assert.Contains(t, n, "1 memory from an earlier session is queued")
	assert.Contains(t, n, "3 h")
	assert.Contains(t, n, "cortex outbox flush")

	// A pending session summary is leftover state too; the model must hear about it.
	err := outbox.PutSummary(d.outboxPath, outbox.SummaryEntry{ConversationID: "earlier", Request: []byte("x")})
	require.NoError(t, err)
	n = startupPendingNotice(d.outboxPath, time.Now().Add(3*time.Hour))
	assert.Contains(t, n, "1 memory and 1 session summary from earlier sessions are queued")

	// Summary only.
	d2 := newOutboxDeps(t, &readClient{})
	err = outbox.PutSummary(d2.outboxPath, outbox.SummaryEntry{ConversationID: "earlier", Request: []byte("x")})
	require.NoError(t, err)
	assert.Contains(t, startupPendingNotice(d2.outboxPath, time.Now()), "1 session summary from an earlier session is queued")
}
