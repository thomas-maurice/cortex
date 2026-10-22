package chunk

import (
	"fmt"
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

// TestLocate pins the contract the search excerpt relies on: a chunk (whose
// whitespace Split normalises) is found in the source text with rune offsets, so
// the UI can highlight the matched span.
func TestLocate(t *testing.T) {
	t.Run("whitespace-normalised chunk maps back to the original span", func(t *testing.T) {
		text := "Intro line.\n\nFirst fact.   Second fact.\n\nOutro."
		s, e, ok := Locate(text, "First fact. Second fact.", 0)
		require.True(t, ok)
		assert.Equal(t, "First fact.   Second fact.", string([]rune(text)[s:e]))
	})
	t.Run("offsets are in runes, not bytes", func(t *testing.T) {
		text := "héllo wörld ☃ target text"
		s, e, ok := Locate(text, "target text", 0)
		require.True(t, ok)
		assert.Equal(t, "target text", string([]rune(text)[s:e]))
	})
	t.Run("not found", func(t *testing.T) {
		_, _, ok := Locate("abc def", "xyz", 0)
		assert.False(t, ok)
		_, _, ok = Locate("abc", "   ", 0)
		assert.False(t, ok)
	})
	t.Run("every Split chunk of a real document is locatable", func(t *testing.T) {
		c, err := New(40, 10)
		require.NoError(t, err)
		var sb strings.Builder
		for i := 0; i < 50; i++ {
			fmt.Fprintf(&sb, "Sentence number %d says something.\n\n", i)
		}
		text := sb.String()
		for i, ch := range c.Split(text) {
			s, e, ok := Locate(text, ch, i)
			require.True(t, ok, "chunk %d", i)
			assert.Equal(t, strings.Fields(ch), strings.Fields(string([]rune(text)[s:e])))
		}
	})
}

// Repeated boilerplate must not pull the highlight to the first occurrence: the
// chunk index picks the occurrence nearest where that chunk should sit.
func TestLocateDisambiguatesRepeatsByChunkIndex(t *testing.T) {
	rep := strings.TrimSpace(strings.Repeat("This block repeats verbatim. ", 8))
	text := rep + "\n\n" + strings.Repeat("Filler words here now. ", 10) + "\n\n" + rep
	runes := []rune(text)
	s0, _, ok := Locate(text, rep, 0)
	require.True(t, ok)
	assert.Equal(t, 0, s0, "chunk 0 is the first occurrence")
	s2, e2, ok := Locate(text, rep, 2)
	require.True(t, ok)
	assert.Equal(t, len(runes)-len([]rune(rep)), s2, "chunk 2 is the second occurrence")
	assert.Equal(t, rep, string(runes[s2:e2]))
}

// Offsets count code points: an astral-plane emoji is 2 UTF-16 units and 4 bytes
// but ONE rune.
func TestLocateAstralRunes(t *testing.T) {
	text := "😀😀 x 😀 target 😀"
	s, e, ok := Locate(text, "target", 0)
	require.True(t, ok)
	assert.Equal(t, 7, s)
	assert.Equal(t, 13, e)
	assert.Equal(t, "target", string([]rune(text)[s:e]))
}
