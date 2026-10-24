package rpc

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/bus"
	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/natstest"
)

// edgeTestService starts an embedded JetStream with the MEMORY stream and returns
// a Service plus a func reporting how many messages were published so far.
func edgeTestService(t *testing.T) (*Service, func() uint64) {
	t.Helper()
	_, js := natstest.Start(t)
	ctx := context.Background()
	require.NoError(t, bus.EnsureStream(ctx, js))
	svc := &Service{cfg: Config{DefaultNamespace: "ns"}, js: js, log: slog.Default()}
	return svc, func() uint64 {
		st, err := js.Stream(ctx, memory.StreamName)
		require.NoError(t, err)
		info, err := st.Info(ctx)
		require.NoError(t, err)
		return info.State.Msgs
	}
}

// A malformed id can never resolve in Weaviate, so accepting it would report
// success while the worker silently drops the edge.
func TestLinkUnlinkRejectNonUUID(t *testing.T) {
	svc, msgs := edgeTestService(t)
	ctx := context.Background()
	good := uuid.NewString()
	cases := []struct{ name, id, target string }{
		{"bad id", "nope", good},
		{"bad target", good, "nope"},
	}
	for _, c := range cases {
		_, err := svc.Link(ctx, connect.NewRequest(&cortexv1.LinkRequest{Id: c.id, TargetId: c.target}))
		require.Error(t, err, "link %s", c.name)
		assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err), "link %s", c.name)
		_, err = svc.Unlink(ctx, connect.NewRequest(&cortexv1.UnlinkRequest{Id: c.id, TargetId: c.target}))
		require.Error(t, err, "unlink %s", c.name)
		assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err), "unlink %s", c.name)
	}
	assert.Zero(t, msgs(), "nothing may be published for invalid ids")
}

func TestLinkAcceptsUUIDs(t *testing.T) {
	svc, msgs := edgeTestService(t)
	ctx := context.Background()
	a, b := uuid.NewString(), uuid.NewString()
	_, err := svc.Link(ctx, connect.NewRequest(&cortexv1.LinkRequest{Id: a, TargetId: b}))
	require.NoError(t, err)
	_, err = svc.Unlink(ctx, connect.NewRequest(&cortexv1.UnlinkRequest{Id: a, TargetId: b}))
	require.NoError(t, err)
	assert.Equal(t, uint64(2), msgs())
}

// One bad link_to must fail the whole save: the memory is not queued, so the
// caller can fix the request instead of losing a link after a "queued" reply.
func TestSaveRejectsBadLinkTo(t *testing.T) {
	svc, msgs := edgeTestService(t)
	_, err := svc.Save(context.Background(), connect.NewRequest(&cortexv1.SaveRequest{
		Text:   "hello",
		LinkTo: []string{uuid.NewString(), "bogus", uuid.NewString()},
	}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err))
	assert.Contains(t, err.Error(), "link_to[1]")
	assert.Zero(t, msgs(), "no index message and no link messages may be queued")
}

func TestSaveRejectsBadSupersedes(t *testing.T) {
	svc, msgs := edgeTestService(t)
	_, err := svc.Save(context.Background(), connect.NewRequest(&cortexv1.SaveRequest{
		Text:       "hello",
		Supersedes: []string{"bogus"},
	}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err))
	assert.Contains(t, err.Error(), "supersedes[0]")
	assert.Zero(t, msgs())
}

func TestSaveAcceptsValidIDs(t *testing.T) {
	svc, msgs := edgeTestService(t)
	// Empty entries stay skipped; valid UUIDs queue 1 index + 2 link messages.
	resp, err := svc.Save(context.Background(), connect.NewRequest(&cortexv1.SaveRequest{
		Text:       "hello",
		LinkTo:     []string{uuid.NewString(), " ", uuid.NewString()},
		Supersedes: []string{uuid.NewString()},
	}))
	require.NoError(t, err)
	assert.NotEmpty(t, resp.Msg.GetId())
	assert.Equal(t, uint64(3), msgs())
}

// DismissDuplicate must reject malformed ids up front (InvalidArgument) rather
// than letting them reach Weaviate as an opaque backend error.
func TestLinkEndpointsRejectNonUUID(t *testing.T) {
	svc := &Service{}
	good := uuid.NewString()
	for _, c := range [][2]string{{"nope", good}, {good, "nope"}} {
		_, _, err := svc.linkEndpoints(context.Background(), nil, c[0], c[1])
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err))
	}
}

// What was validated must be exactly what is queued: trimmed, empties dropped.
func TestSaveQueuesTrimmedSupersedes(t *testing.T) {
	svc, _ := edgeTestService(t)
	ctx := context.Background()
	id := uuid.NewString()
	_, err := svc.Save(ctx, connect.NewRequest(&cortexv1.SaveRequest{
		Text:       "hello",
		Supersedes: []string{"  " + id + " ", " "},
	}))
	require.NoError(t, err)

	st, err := svc.js.Stream(ctx, memory.StreamName)
	require.NoError(t, err)
	raw, err := st.GetLastMsgForSubject(ctx, memory.SubjectIndex)
	require.NoError(t, err)
	var rec memory.Record
	require.NoError(t, json.Unmarshal(raw.Data, &rec))
	assert.Equal(t, []string{id}, rec.Supersedes)
}

// never_expires must travel on the index payload: the worker is the one that
// decides whether to inherit a source expiry.
func TestSaveQueuesNeverExpires(t *testing.T) {
	svc, _ := edgeTestService(t)
	ctx := context.Background()
	_, err := svc.Save(ctx, connect.NewRequest(&cortexv1.SaveRequest{Text: "permanent", Supersedes: []string{uuid.NewString()}, NeverExpires: true}))
	require.NoError(t, err)
	st, err := svc.js.Stream(ctx, memory.StreamName)
	require.NoError(t, err)
	raw, err := st.GetLastMsgForSubject(ctx, memory.SubjectIndex)
	require.NoError(t, err)
	var rec memory.Record
	require.NoError(t, json.Unmarshal(raw.Data, &rec))
	assert.True(t, rec.NeverExpires)
}

// uuid.Parse accepts several spellings of one id; they must all be stored in the
// canonical form, otherwise the same memory could be referenced by ids that
// never compare equal (and a self-link would slip past the check).
func spellings(u uuid.UUID) []string {
	c := u.String()
	return []string{"{" + c + "}", "urn:uuid:" + c, strings.ToUpper(c), strings.ReplaceAll(c, "-", "")}
}

func TestNonCanonicalIDsAreNormalized(t *testing.T) {
	svc, msgs := edgeTestService(t)
	ctx := context.Background()
	u := uuid.New()
	canon := u.String()

	for _, sp := range spellings(u) {
		_, err := svc.Link(ctx, connect.NewRequest(&cortexv1.LinkRequest{Id: sp, TargetId: uuid.NewString()}))
		require.NoError(t, err, sp)
	}
	st, err := svc.js.Stream(ctx, memory.StreamName)
	require.NoError(t, err)
	for seq := uint64(1); seq <= msgs(); seq++ {
		raw, err := st.GetMsg(ctx, seq)
		require.NoError(t, err)
		var lm memory.LinkMsg
		require.NoError(t, json.Unmarshal(raw.Data, &lm))
		assert.Contains(t, []string{lm.A, lm.B}, canon)
	}

	// Self-link is detected across spellings.
	_, err = svc.Link(ctx, connect.NewRequest(&cortexv1.LinkRequest{Id: canon, TargetId: strings.ToUpper(canon)}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err))

	// Save queues canonical supersedes and link targets.
	before := msgs()
	_, err = svc.Save(ctx, connect.NewRequest(&cortexv1.SaveRequest{
		Text: "x", Supersedes: []string{"{" + canon + "}"}, LinkTo: []string{strings.ToUpper(canon)},
	}))
	require.NoError(t, err)
	assert.Equal(t, before+2, msgs())
	raw, err := st.GetLastMsgForSubject(ctx, memory.SubjectIndex)
	require.NoError(t, err)
	var rec memory.Record
	require.NoError(t, json.Unmarshal(raw.Data, &rec))
	assert.Equal(t, []string{canon}, rec.Supersedes)
	raw, err = st.GetLastMsgForSubject(ctx, memory.SubjectLink)
	require.NoError(t, err)
	var lm memory.LinkMsg
	require.NoError(t, json.Unmarshal(raw.Data, &lm))
	assert.Contains(t, []string{lm.A, lm.B}, canon)
}

func TestRestoreMemoriesRejectsBadIDs(t *testing.T) {
	good := uuid.NewString()
	for name, m := range map[string]*cortexv1.Memory{
		"id":               {Id: "bad", Text: "t"},
		"linked_ids":       {Id: good, Text: "t", LinkedIds: []string{good, "bad"}},
		"dup_candidates":   {Id: good, Text: "t", DupCandidates: []string{"bad"}},
		"not_duplicate_of": {Id: good, Text: "t", NotDuplicateOf: []string{"bad"}},
	} {
		svc, msgs := edgeTestService(t)
		// A valid record first: it must not be queued when a later one is bad.
		_, err := svc.RestoreMemories(context.Background(), connect.NewRequest(&cortexv1.RestoreMemoriesRequest{
			Memories: []*cortexv1.Memory{{Id: uuid.NewString(), Text: "ok"}, m},
		}))
		require.Error(t, err, name)
		assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err), name)
		assert.Contains(t, err.Error(), "memories[1]."+name, name)
		assert.Zero(t, msgs(), name)
	}
}

func TestRestoreMemoriesNormalizes(t *testing.T) {
	svc, _ := edgeTestService(t)
	ctx := context.Background()
	u := uuid.New()
	_, err := svc.RestoreMemories(ctx, connect.NewRequest(&cortexv1.RestoreMemoriesRequest{
		Memories: []*cortexv1.Memory{{Id: strings.ToUpper(u.String()), Text: "t", LinkedIds: []string{"{" + u.String() + "}"}}},
	}))
	require.NoError(t, err)
	st, err := svc.js.Stream(ctx, memory.StreamName)
	require.NoError(t, err)
	raw, err := st.GetLastMsgForSubject(ctx, memory.SubjectIndex)
	require.NoError(t, err)
	var rec memory.Record
	require.NoError(t, json.Unmarshal(raw.Data, &rec))
	assert.Equal(t, u.String(), rec.ID)
	assert.Equal(t, []string{u.String()}, rec.LinkedIDs)
}

// Consistency with GetMemory: these must fail fast with InvalidArgument instead of
// reaching Weaviate. A zero Service proves no store access happens first.
func TestIDRPCsRejectNonUUID(t *testing.T) {
	svc := &Service{}
	ctx := context.Background()
	_, err := svc.UpdateMemory(ctx, connect.NewRequest(&cortexv1.UpdateMemoryRequest{Id: "bad", Text: "t"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err))
	_, err = svc.Delete(ctx, connect.NewRequest(&cortexv1.DeleteRequest{Id: "bad"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err))
	_, err = svc.SearchSimilar(ctx, connect.NewRequest(&cortexv1.SearchSimilarRequest{Id: "bad"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err))
}

func TestBadIDEchoIsTruncated(t *testing.T) {
	_, err := normalizeMemoryID("id", strings.Repeat("x", 500))
	require.Error(t, err)
	assert.Less(t, len(err.Error()), 150)
	assert.Contains(t, err.Error(), "…")
}

// lastIndexed decodes the most recent index payload published by Save.
func lastIndexed(t *testing.T, svc *Service) memory.Record {
	t.Helper()
	st, err := svc.js.Stream(context.Background(), memory.StreamName)
	require.NoError(t, err)
	raw, err := st.GetLastMsgForSubject(context.Background(), memory.SubjectIndex)
	require.NoError(t, err)
	var rec memory.Record
	require.NoError(t, json.Unmarshal(raw.Data, &rec))
	return rec
}

// The offline outbox picks the id before the server is reachable; honouring it
// makes that stable id the Weaviate object id, so a replay is idempotent.
func TestSaveHonoursClientID(t *testing.T) {
	svc, msgs := edgeTestService(t)
	id := uuid.New()
	resp, err := svc.Save(context.Background(), connect.NewRequest(&cortexv1.SaveRequest{
		Text: "hello", Id: proto.String(strings.ToUpper(id.String())),
	}))
	require.NoError(t, err)
	assert.Equal(t, id.String(), resp.Msg.GetId(), "canonical form returned")
	assert.Equal(t, uint64(1), msgs())
	rec := lastIndexed(t, svc)
	assert.Equal(t, id.String(), rec.ID)
	assert.True(t, rec.CreateOnly, "Save must never overwrite an existing id")
}

// Arbitrary strings must never become Weaviate object ids.
func TestSaveRejectsMalformedClientID(t *testing.T) {
	for _, bad := range []string{"not-a-uuid", "../etc"} {
		svc, msgs := edgeTestService(t)
		_, err := svc.Save(context.Background(), connect.NewRequest(&cortexv1.SaveRequest{Text: "hello", Id: proto.String(bad)}))
		require.Error(t, err, bad)
		assert.Equal(t, connect.CodeInvalidArgument, connectCode(t, err), bad)
		assert.Zero(t, msgs(), "nothing published for %q", bad)
	}
}

// Existing clients (CLI) send no id; the server must keep minting one.
func TestSaveMintsIDWhenAbsent(t *testing.T) {
	svc, msgs := edgeTestService(t)
	resp, err := svc.Save(context.Background(), connect.NewRequest(&cortexv1.SaveRequest{Text: "hello"}))
	require.NoError(t, err)
	_, perr := uuid.Parse(resp.Msg.GetId())
	require.NoError(t, perr)
	assert.Equal(t, uint64(1), msgs())
	assert.Equal(t, resp.Msg.GetId(), lastIndexed(t, svc).ID)
}

func TestSaveCreatedAtClamp(t *testing.T) {
	ctx := context.Background()
	save := func(t *testing.T, ca *timestamppb.Timestamp) (memory.Record, time.Time, time.Time) {
		svc, _ := edgeTestService(t)
		before := time.Now().UTC()
		_, err := svc.Save(ctx, connect.NewRequest(&cortexv1.SaveRequest{Text: "hello", CreatedAt: ca}))
		require.NoError(t, err)
		return lastIndexed(t, svc), before, time.Now().UTC()
	}

	// A memory queued for days must not rank as fresh.
	t.Run("past is kept", func(t *testing.T) {
		past := time.Now().Add(-72 * time.Hour).UTC().Truncate(time.Microsecond)
		rec, _, _ := save(t, timestamppb.New(past))
		assert.True(t, rec.CreatedAt.Equal(past), "got %s want %s", rec.CreatedAt, past)
	})
	// A fast client clock must not make a memory newer than everything else.
	t.Run("future is clamped to now", func(t *testing.T) {
		rec, before, after := save(t, timestamppb.New(time.Now().Add(48*time.Hour)))
		assert.False(t, rec.CreatedAt.Before(before) || rec.CreatedAt.After(after), "got %s", rec.CreatedAt)
	})
	t.Run("unset is now", func(t *testing.T) {
		rec, before, after := save(t, nil)
		assert.False(t, rec.CreatedAt.Before(before) || rec.CreatedAt.After(after), "got %s", rec.CreatedAt)
	})
}
