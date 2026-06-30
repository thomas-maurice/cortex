package chunk

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A text within the token budget must stay a single chunk — chunking exists to
// break up LONG memories, not to fragment short ones (which would just multiply
// near-identical vectors and dilute, not improve, retrieval).
func TestSplitShortTextIsOneChunk(t *testing.T) {
	c, err := New(512, 64)
	require.NoError(t, err)
	got := c.Split("Halcyon Station orbits Vesper Prime. It has three fusion rings.")
	require.Len(t, got, 1)
	assert.Equal(t, "Halcyon Station orbits Vesper Prime. It has three fusion rings.", got[0])
}

// The load-bearing invariant: every chunk of a long text must be at or under the
// token budget (so each embeds as a focused vector), there must be MORE than one
// chunk, and consecutive chunks must overlap (a fact on a boundary survives).
func TestSplitLongTextRespectsBudgetAndOverlaps(t *testing.T) {
	c, err := New(64, 16) // small budget so a few paragraphs force many chunks
	require.NoError(t, err)

	// ~40 distinct sentences so packing must split repeatedly.
	var sb strings.Builder
	for range 40 {
		sb.WriteString("Sentence number ")
		sb.WriteString(strings.Repeat("alpha ", 3))
		sb.WriteString("about subsystem index here. ")
	}
	got := c.Split(sb.String())

	require.Greater(t, len(got), 1, "a long text must split into multiple chunks")
	for i, ch := range got {
		assert.LessOrEqualf(t, c.Count(ch), 64, "chunk %d exceeds the token budget", i)
		assert.NotEmpty(t, strings.TrimSpace(ch))
	}

	// Overlap: the last sentence of chunk i should reappear at the start of i+1.
	overlaps := 0
	for i := 0; i+1 < len(got); i++ {
		lastSentence := lastSentenceOf(got[i])
		if lastSentence != "" && strings.HasPrefix(got[i+1], lastSentence) {
			overlaps++
		}
	}
	assert.Greater(t, overlaps, 0, "consecutive chunks must share overlapping context")
}

// A single sentence longer than the budget has no sentence boundary to pack on,
// so it must still be hard-split into in-budget windows rather than emitted whole.
func TestSplitHardSplitsAnOversizedSentence(t *testing.T) {
	c, err := New(32, 8)
	require.NoError(t, err)
	oneLongSentence := strings.TrimSpace(strings.Repeat("lumarite ", 200)) // no '.' anywhere
	got := c.Split(oneLongSentence)
	require.Greater(t, len(got), 1)
	for i, ch := range got {
		assert.LessOrEqualf(t, c.Count(ch), 32, "hard-split chunk %d exceeds budget", i)
	}
}

func TestSplitEmpty(t *testing.T) {
	c, err := New(0, 0) // defaults
	require.NoError(t, err)
	assert.Nil(t, c.Split("   "))
}

func lastSentenceOf(s string) string {
	parts := splitSentences(s)
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}
