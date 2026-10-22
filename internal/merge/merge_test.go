package merge

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/memory"
)

type fakeStore struct {
	recs map[string]memory.Record
}

func (f *fakeStore) Get(_ context.Context, id string) (memory.Record, bool, error) {
	r, ok := f.recs[id]
	return r, ok, nil
}
func (f *fakeStore) Reinforce(_ context.Context, id string, c int, at time.Time) error {
	r := f.recs[id]
	r.AccessCount, r.LastAccessedAt = c, at
	f.recs[id] = r
	return nil
}
func (f *fakeStore) SetNotDuplicateOf(_ context.Context, id string, ids []string) error {
	r := f.recs[id]
	r.NotDuplicateOf = ids
	f.recs[id] = r
	return nil
}

func TestProvenance(t *testing.T) {
	t0 := time.Unix(1000, 0)
	keep := memory.Record{CreatedAt: t0.Add(time.Hour)}
	srcs := []memory.Record{
		{CreatedAt: t0.Add(30 * time.Minute), ConversationID: "mid"},
		{CreatedAt: t0, ConversationID: "old"},
		{}, // zero createdAt must not win
	}
	created, conv := Provenance(keep, srcs)
	assert.True(t, created.Equal(t0), "oldest capture time wins")
	assert.Equal(t, "old", conv, "oldest source's conversation when keep has none")

	keep.ConversationID = "mine"
	_, conv = Provenance(keep, srcs)
	assert.Equal(t, "mine", conv, "the survivor's own conversation is never replaced")

	_, conv = Provenance(memory.Record{}, []memory.Record{{ConversationID: "shared"}, {ConversationID: "shared"}})
	assert.Equal(t, "shared", conv)
}

// A dismissal ("these two are not duplicates") is the user's decision; folding a
// memory away must move it to the survivor on both sides of the pair.
func TestCarryDismissalsAndStats(t *testing.T) {
	const keep, a, b, c = "k", "a", "b", "c"
	st := &fakeStore{recs: map[string]memory.Record{
		keep: {ID: keep, AccessCount: 1, NotDuplicateOf: []string{a}},
		a:    {ID: a, AccessCount: 2, LastAccessedAt: time.Unix(50, 0), NotDuplicateOf: []string{c, b, "gone"}},
		b:    {ID: b, AccessCount: 3, LastAccessedAt: time.Unix(90, 0)},
		c:    {ID: c, NotDuplicateOf: []string{a}},
	}}
	noPub := func(memory.LinkOp, string, string) error { return nil }
	for _, src := range []string{a, b} {
		require.NoError(t, Carry(context.Background(), st, keep, src, []string{a, b}, noPub, []string{keep, a, b, c, "gone"}))
	}
	assert.Equal(t, []string{c}, st.recs[keep].NotDuplicateOf, "sources stripped, live counterparts inherited, dead ones ignored")
	assert.Equal(t, []string{keep}, st.recs[c].NotDuplicateOf)
	assert.Equal(t, 6, st.recs[keep].AccessCount)
	assert.True(t, st.recs[keep].LastAccessedAt.Equal(time.Unix(90, 0)))
}

func TestCarryMissingKeepFails(t *testing.T) {
	st := &fakeStore{recs: map[string]memory.Record{}}
	assert.Error(t, Carry(context.Background(), st, "k", "a", []string{"a"}, nil, nil), "never delete sources into a merge target that is not there")
}

// A counterpart whose lock the caller does not hold must not be rewritten.
func TestCarryRefusesUnlockedCounterpart(t *testing.T) {
	st := &fakeStore{recs: map[string]memory.Record{
		"k": {ID: "k"}, "a": {ID: "a", NotDuplicateOf: []string{"c"}}, "c": {ID: "c", NotDuplicateOf: []string{"a"}},
	}}
	err := Carry(context.Background(), st, "k", "a", []string{"a"}, func(memory.LinkOp, string, string) error { return nil }, []string{"k", "a"})
	assert.ErrorIs(t, err, ErrLockSetChanged)
	assert.Equal(t, []string{"a"}, st.recs["c"].NotDuplicateOf, "nothing written outside the held locks")

	ids, err := LockIDs(context.Background(), st, "k", []string{"a"})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"k", "a", "c"}, ids)
}

// The lock set is held for a whole merge while a worker slot waits for it, so it
// must be bounded and deduplicated (not quadratic) rather than caller-sized.
func TestLockIDsBoundsAndDedups(t *testing.T) {
	ctx := context.Background()
	st := &fakeStore{recs: map[string]memory.Record{}}

	var many []string
	for i := range MaxSources + 1 {
		many = append(many, fmt.Sprintf("s%d", i))
	}
	_, err := LockIDs(ctx, st, "k", many)
	assert.ErrorIs(t, err, ErrTooLarge, "too many sources")

	// Two sources sharing the same 150 counterparts: deduped to 150 (+ k + 2).
	var shared []string
	for i := range 150 {
		shared = append(shared, fmt.Sprintf("c%d", i))
	}
	st.recs["a"] = memory.Record{ID: "a", NotDuplicateOf: shared}
	st.recs["b"] = memory.Record{ID: "b", NotDuplicateOf: shared}
	ids, err := LockIDs(ctx, st, "k", []string{"a", "b"})
	require.NoError(t, err)
	assert.Len(t, ids, 153)

	// Counterparts beyond the lock-set cap fail loudly.
	var big []string
	for i := range MaxLockSet + 5 {
		big = append(big, fmt.Sprintf("x%d", i))
	}
	st.recs["a"] = memory.Record{ID: "a", NotDuplicateOf: big}
	_, err = LockIDs(ctx, st, "k", []string{"a"})
	assert.ErrorIs(t, err, ErrTooLarge)
}

// A merge of only time-bound facts must expire with the LAST of them; any
// permanent source (or an unknown one) keeps the merged memory permanent, and a
// keep that already has its own expiry is never rewritten.
func TestInheritedExpiry(t *testing.T) {
	soon := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Second)
	later := soon.Add(48 * time.Hour)
	exp := func(u time.Time) memory.Record { return memory.Record{ValidUntil: u} }

	assert.Equal(t, later, InheritedExpiry(time.Time{}, []memory.Record{exp(soon), exp(later)}, 2))
	assert.True(t, InheritedExpiry(time.Time{}, []memory.Record{exp(soon), {}}, 2).IsZero(), "a permanent source keeps the merge permanent")
	assert.True(t, InheritedExpiry(soon, []memory.Record{exp(later)}, 1).IsZero(), "keep's own expiry wins")
	assert.True(t, InheritedExpiry(time.Time{}, []memory.Record{exp(soon)}, 2).IsZero(), "a missing source is unknown, not inherited")
	assert.True(t, InheritedExpiry(time.Time{}, nil, 0).IsZero())
}
