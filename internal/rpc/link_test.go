package rpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddUniqueNoDuplicates(t *testing.T) {
	// Linking the same pair twice must not duplicate the id — the graph treats
	// linked_ids as a set.
	links := addUnique([]string{"a", "b"}, "b")
	assert.Equal(t, []string{"a", "b"}, links)

	links = addUnique([]string{"a"}, "b")
	assert.Equal(t, []string{"a", "b"}, links)
}

func TestRemoveString(t *testing.T) {
	assert.Equal(t, []string{"a", "c"}, removeString([]string{"a", "b", "c"}, "b"))
	assert.Equal(t, []string{"a"}, removeString([]string{"a"}, "x")) // absent: unchanged
	assert.Empty(t, removeString([]string{"b"}, "b"))
}
