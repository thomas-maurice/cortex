package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// planLinks is the correctness core of model-driven linking: links must be
// bidirectional (the record points at each target AND each target points back),
// and bad inputs the model can realistically send — a self-reference, a
// duplicate, an empty id, or a stale/missing target — must be silently dropped
// rather than producing a dangling or one-sided edge. Each subtest pins one of
// those guarantees so a regression can't quietly corrupt the link graph.
func TestPlanLinks(t *testing.T) {
	t.Run("bidirectional link to an existing target", func(t *testing.T) {
		forward, reverse := planLinks("new", nil, []linkTarget{
			{id: "a", links: nil, exists: true},
		})
		assert.Equal(t, []string{"a"}, forward, "record must point at the target")
		assert.Equal(t, []string{"new"}, reverse["a"], "target must point back at the record")
	})

	t.Run("merges into the target's existing links, not replace", func(t *testing.T) {
		_, reverse := planLinks("new", nil, []linkTarget{
			{id: "a", links: []string{"old1", "old2"}, exists: true},
		})
		assert.Equal(t, []string{"old1", "old2", "new"}, reverse["a"], "existing links of the target must be preserved")
	})

	t.Run("missing target is skipped entirely", func(t *testing.T) {
		forward, reverse := planLinks("new", nil, []linkTarget{
			{id: "ghost", exists: false},
		})
		assert.Empty(t, forward, "a non-existent target must not appear in forward links")
		assert.NotContains(t, reverse, "ghost", "a non-existent target gets no reverse update")
	})

	t.Run("self-reference is dropped", func(t *testing.T) {
		forward, reverse := planLinks("new", nil, []linkTarget{
			{id: "new", exists: true},
		})
		assert.Empty(t, forward)
		assert.Empty(t, reverse)
	})

	t.Run("duplicate targets are applied once", func(t *testing.T) {
		forward, reverse := planLinks("new", nil, []linkTarget{
			{id: "a", exists: true},
			{id: "a", exists: true},
		})
		assert.Equal(t, []string{"a"}, forward)
		assert.Len(t, reverse, 1)
	})

	t.Run("existing forward links are kept when adding new ones", func(t *testing.T) {
		forward, _ := planLinks("new", []string{"existing"}, []linkTarget{
			{id: "a", exists: true},
		})
		assert.Equal(t, []string{"existing", "a"}, forward)
	})

	t.Run("no valid targets leaves forward links unchanged", func(t *testing.T) {
		forward, reverse := planLinks("new", []string{"existing"}, nil)
		assert.Equal(t, []string{"existing"}, forward)
		assert.Empty(t, reverse)
	})
}

// planSupersedes decides which sources a merged memory deletes. Consolidation is
// destructive, so the guarantees that matter are: never delete the memory itself
// (that would erase the merge result), never act on an empty id, and collapse
// duplicates so a repeated id can't be deleted twice. Each subtest pins one so a
// regression can't quietly turn a merge into data loss.
func TestPlanSupersedes(t *testing.T) {
	t.Run("returns the distinct sources to delete", func(t *testing.T) {
		assert.Equal(t, []string{"a", "b"}, planSupersedes("new", []string{"a", "b"}))
	})

	t.Run("self-reference is dropped so the merge result is never deleted", func(t *testing.T) {
		assert.Equal(t, []string{"a"}, planSupersedes("new", []string{"a", "new"}))
	})

	t.Run("empty ids are dropped", func(t *testing.T) {
		assert.Equal(t, []string{"a"}, planSupersedes("new", []string{"", "a", ""}))
	})

	t.Run("duplicates collapse to one delete", func(t *testing.T) {
		assert.Equal(t, []string{"a"}, planSupersedes("new", []string{"a", "a"}))
	})

	t.Run("nothing to supersede yields no deletes", func(t *testing.T) {
		assert.Empty(t, planSupersedes("new", nil))
	})
}
