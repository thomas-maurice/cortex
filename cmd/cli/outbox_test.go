package main

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/outbox"
)

type fakeSender struct {
	fail map[string]error // by save id; "summary:<conv>" fails a summary
	sums []string         // "<conv>:<text>" of summaries sent
}

func (f *fakeSender) SummarizeSession(_ context.Context, r *connect.Request[cortexv1.SummarizeSessionRequest]) (*connect.Response[cortexv1.SummarizeSessionResponse], error) {
	if err := f.fail["summary:"+r.Msg.GetConversationId()]; err != nil {
		return nil, err
	}
	f.sums = append(f.sums, r.Msg.GetConversationId()+":"+r.Msg.GetText())
	return connect.NewResponse(&cortexv1.SummarizeSessionResponse{}), nil
}

func seedSummary(t *testing.T, path, conv, text string) {
	t.Helper()
	raw, err := proto.Marshal(&cortexv1.SummarizeSessionRequest{ConversationId: conv, Text: text})
	require.NoError(t, err)
	require.NoError(t, outbox.PutSummary(path, outbox.SummaryEntry{ConversationID: conv, Request: raw, Preview: text}))
}

func (f *fakeSender) Save(_ context.Context, r *connect.Request[cortexv1.SaveRequest]) (*connect.Response[cortexv1.SaveResponse], error) {
	if err := f.fail[r.Msg.GetId()]; err != nil {
		return nil, err
	}
	return connect.NewResponse(&cortexv1.SaveResponse{Id: r.Msg.GetId()}), nil
}

func seed(t *testing.T, path, id, conv string, at int64, mut func(*cortexv1.SaveRequest)) {
	t.Helper()
	req := &cortexv1.SaveRequest{Id: &id, Text: "text-" + id, ConversationId: conv}
	if mut != nil {
		mut(req)
	}
	raw, err := proto.Marshal(req)
	require.NoError(t, err)
	require.NoError(t, outbox.Enqueue(path, outbox.Entry{ID: id, Request: raw, ConversationID: conv, EnqueuedAt: at, Preview: req.GetText()}))
}

func ids(t *testing.T, path string) []string {
	t.Helper()
	all, err := outbox.ListAll(path)
	require.NoError(t, err)
	var out []string
	for _, e := range all {
		out = append(out, e.ID)
	}
	return out
}

func newDB(t *testing.T) string { return filepath.Join(t.TempDir(), "outbox.db") }

func TestOutboxStatus(t *testing.T) {
	p := newDB(t)
	var out strings.Builder
	require.NoError(t, outboxStatus(&out, p, time.Now()))
	assert.Equal(t, "nothing pending\n", out.String(), "missing db is empty, not an error")

	now := time.Now()
	seed(t, p, "a1", "A", now.Add(-2*time.Hour).UnixMicro(), nil)
	seed(t, p, "b1", "B", now.Add(-time.Hour).UnixMicro(), nil)
	seed(t, p, "a2", "A", now.UnixMicro(), nil)
	out.Reset()
	require.NoError(t, outboxStatus(&out, p, now))
	s := out.String()
	assert.Contains(t, s, "3 pending, oldest 2h0m0s ago")
	assert.Contains(t, s, "A: 2")
	assert.Contains(t, s, "B: 1")
	assert.Contains(t, s, "a1  text-a1")
}

func TestOutboxDropUnknownDoesNotAbortRest(t *testing.T) {
	p := newDB(t)
	seed(t, p, "x", "A", 1, nil)
	seed(t, p, "y", "A", 2, nil)
	seed(t, p, "z", "A", 3, nil)
	var out strings.Builder
	// An unknown id is an error (exit 1, so scripts notice the typo), but the
	// other ids are still dropped.
	err := outboxDrop(&out, p, []string{"x", "nope", "z"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nope: not queued")
	assert.Contains(t, out.String(), "dropped x text-x")
	assert.Contains(t, out.String(), "not queued: nope")
	assert.Contains(t, out.String(), "dropped z text-z", "ids after the unknown one must still be dropped")
	assert.Equal(t, []string{"y"}, ids(t, p))
}

func TestOutboxPurge(t *testing.T) {
	setup := func() string {
		p := newDB(t)
		seed(t, p, "a1", "A", 1, nil)
		seed(t, p, "a2", "A", 2, nil)
		seed(t, p, "b1", "B", 3, nil)
		return p
	}
	t.Run("no confirmation deletes nothing", func(t *testing.T) {
		for _, in := range []string{"no\n", "y\n", ""} {
			p := setup()
			var out strings.Builder
			err := outboxPurge(strings.NewReader(in), &out, io.Discard, p, nil, false)
			assert.EqualError(t, err, "aborted")
			assert.Len(t, ids(t, p), 3, "input %q must not delete", in)
		}
	})
	t.Run("conversation scoped", func(t *testing.T) {
		p := setup()
		conv := "A"
		var out strings.Builder
		require.NoError(t, outboxPurge(strings.NewReader("yes\n"), &out, io.Discard, p, &conv, false))
		assert.Equal(t, []string{"b1"}, ids(t, p), "other conversations must survive")
		assert.Contains(t, out.String(), "purged 2 saves")
	})
	t.Run("all with --yes", func(t *testing.T) {
		p := setup()
		var out strings.Builder
		require.NoError(t, outboxPurge(strings.NewReader(""), &out, io.Discard, p, nil, true))
		assert.Empty(t, ids(t, p))
	})
}

func TestOutboxFlush(t *testing.T) {
	p := newDB(t)
	seed(t, p, "ok", "A", 1, nil)
	seed(t, p, "bad", "B", 2, nil)
	seed(t, p, "old", "A", 3, func(r *cortexv1.SaveRequest) { r.ValidUntil = timestamppb.New(time.Now().Add(-time.Hour)) })
	c := &fakeSender{fail: map[string]error{"bad": errors.New("boom")}}
	var out strings.Builder
	err := outboxFlush(context.Background(), &out, p, c)
	require.Error(t, err, "non-zero exit while anything remains")
	assert.Contains(t, out.String(), "sent 1, expired 1, failed 1, remaining 1")
	assert.Contains(t, out.String(), "bad: boom")
	assert.Equal(t, []string{"bad"}, ids(t, p), "failed entry stays queued across conversations")

	out.Reset()
	require.NoError(t, outboxFlush(context.Background(), &out, p, &fakeSender{}))
	assert.Contains(t, out.String(), "sent 1, expired 0, failed 0, remaining 0")
}

func TestOutboxFlushStopsWhenServerUnreachable(t *testing.T) {
	p := newDB(t)
	seed(t, p, "a", "A", 1, nil)
	seed(t, p, "b", "A", 2, nil)
	c := &fakeSender{fail: map[string]error{"a": connect.NewError(connect.CodeUnavailable, errors.New("down"))}}
	var out strings.Builder
	require.Error(t, outboxFlush(context.Background(), &out, p, c))
	assert.Contains(t, out.String(), "remaining 2", "remaining comes from the db, not from results")
	assert.Contains(t, out.String(), "server unreachable")
}

// A summary slot is invisible work: status must show it, or the user thinks
// nothing is pending and never flushes.
func TestOutboxStatusShowsSummaries(t *testing.T) {
	p := newDB(t)
	seedSummary(t, p, "A", "final summary of A")
	var out strings.Builder
	require.NoError(t, outboxStatus(&out, p, time.Now()))
	assert.NotContains(t, out.String(), "nothing pending")
	assert.Contains(t, out.String(), "A  final summary of A")
}

// Flush delivers the summary AFTER that conversation's saves (so it never lands
// before the facts it digests) and clears the slot.
func TestOutboxFlushSendsSummary(t *testing.T) {
	p := newDB(t)
	seed(t, p, "m1", "A", 1, nil)
	seedSummary(t, p, "A", "sum A")
	c := &fakeSender{}
	var out strings.Builder
	require.NoError(t, outboxFlush(context.Background(), &out, p, c))
	assert.Equal(t, []string{"A:sum A"}, c.sums)
	assert.Contains(t, out.String(), "sent 2, expired 0, failed 0, remaining 0")
	left, err := outbox.ListSummaries(p)
	require.NoError(t, err)
	assert.Empty(t, left)
}

// A summary that cannot be sent stays queued and makes flush exit non-zero.
func TestOutboxFlushFailedSummaryRemains(t *testing.T) {
	p := newDB(t)
	seedSummary(t, p, "A", "sum A")
	c := &fakeSender{fail: map[string]error{"summary:A": errors.New("boom")}}
	var out strings.Builder
	require.Error(t, outboxFlush(context.Background(), &out, p, c))
	assert.Contains(t, out.String(), "remaining 1")
	assert.Contains(t, out.String(), "summary A: boom")
}

// Purge must clear summary slots too (else "purge" leaves data the user asked
// gone), honouring --conversation; drop never touches them.
func TestOutboxPurgeAndDropSummaries(t *testing.T) {
	p := newDB(t)
	seed(t, p, "a1", "A", 1, nil)
	seedSummary(t, p, "A", "sum A")
	seedSummary(t, p, "B", "sum B")

	require.Error(t, outboxDrop(io.Discard, p, []string{"a1", "A"}), "A is a conversation, not a queued id")
	sums, err := outbox.ListSummaries(p)
	require.NoError(t, err)
	assert.Len(t, sums, 2, "drop must not delete slots, even by conversation id")

	conv := "A"
	var out strings.Builder
	require.NoError(t, outboxPurge(strings.NewReader("yes\n"), &out, io.Discard, p, &conv, false))
	sums, err = outbox.ListSummaries(p)
	require.NoError(t, err)
	require.Len(t, sums, 1)
	assert.Equal(t, "B", sums[0].ConversationID, "--conversation purge keeps other conversations' slots")
	assert.Contains(t, out.String(), "sum A")

	require.NoError(t, outboxPurge(strings.NewReader(""), io.Discard, io.Discard, p, nil, true))
	sums, err = outbox.ListSummaries(p)
	require.NoError(t, err)
	assert.Empty(t, sums)
}
