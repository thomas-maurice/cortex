package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// applyEdge is the correctness core of the async link path: an add must produce a
// bidirectional edge (each endpoint points at the other), a remove must clear
// both sides, and — because the message can be redelivered or double-published —
// both operations must be idempotent so a retry never duplicates or corrupts the
// link graph. Each subtest pins one of those guarantees.
func TestApplyEdge(t *testing.T) {
	t.Run("add creates a bidirectional edge", func(t *testing.T) {
		newA, newB := applyEdge(memory.LinkOpAdd, "a", nil, "b", nil)
		assert.Equal(t, []string{"b"}, newA, "a must point at b")
		assert.Equal(t, []string{"a"}, newB, "b must point back at a")
	})

	t.Run("add merges into existing links, not replace", func(t *testing.T) {
		newA, newB := applyEdge(memory.LinkOpAdd, "a", []string{"x"}, "b", []string{"y"})
		assert.Equal(t, []string{"x", "b"}, newA)
		assert.Equal(t, []string{"y", "a"}, newB)
	})

	t.Run("add is idempotent", func(t *testing.T) {
		a1, b1 := applyEdge(memory.LinkOpAdd, "a", nil, "b", nil)
		a2, b2 := applyEdge(memory.LinkOpAdd, "a", a1, "b", b1)
		assert.Equal(t, a1, a2, "re-adding the same edge must not duplicate")
		assert.Equal(t, b1, b2)
	})

	t.Run("remove clears both sides", func(t *testing.T) {
		newA, newB := applyEdge(memory.LinkOpRemove, "a", []string{"b", "x"}, "b", []string{"a", "y"})
		assert.Equal(t, []string{"x"}, newA)
		assert.Equal(t, []string{"y"}, newB)
	})

	t.Run("remove is idempotent on an already-absent edge", func(t *testing.T) {
		newA, newB := applyEdge(memory.LinkOpRemove, "a", []string{"x"}, "b", []string{"y"})
		assert.Equal(t, []string{"x"}, newA, "removing an absent edge is a no-op")
		assert.Equal(t, []string{"y"}, newB)
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
