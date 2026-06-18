package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// hit builds a search hit for the rerank tests. dist is the relevance distance
// (lower = closer), accessed is the last-access time (zero = never), count the
// access count.
func hit(id string, dist float32, accessed time.Time, count int) memory.Hit {
	return memory.Hit{
		Record:   memory.Record{ID: id, AccessCount: count, LastAccessedAt: accessed},
		Distance: dist,
	}
}

func ids(hits []memory.Hit) []string {
	out := make([]string, len(hits))
	for i, h := range hits {
		out[i] = h.ID
	}
	return out
}

// The whole point of "living memory": between two EQUALLY relevant memories, the
// one that has actually been used recently must rank first. If this ordering ever
// silently flips, reinforcement stops paying off and the feature is dead — so the
// assertion is on the order, the business-meaningful outcome, not on a score.
func TestRerankReinforcedBeatsStaleAtEqualRelevance(t *testing.T) {
	now := time.Date(2026, 6, 18, 0, 0, 0, 0, time.UTC)
	stale := hit("stale", 0.2, now.AddDate(0, 0, -365), 0) // same relevance, untouched for a year
	fresh := hit("fresh", 0.2, now, 5)                     // same relevance, used now, 5 times
	got := rerankHits([]memory.Hit{stale, fresh}, 0.3, 30, now)
	assert.Equal(t, []string{"fresh", "stale"}, ids(got), "recently-used memory must outrank an equally-relevant stale one")
}

// Relevance must still dominate at a moderate weight: a clearly closer match
// outranks a distant one even when the distant one is maximally reinforced and
// the close one has never been touched. This guards against the rerank turning
// into a popularity contest that buries the best answer.
func TestRerankRelevanceDominatesAtModerateWeight(t *testing.T) {
	now := time.Date(2026, 6, 18, 0, 0, 0, 0, time.UTC)
	close := hit("close", 0.1, time.Time{}, 0) // much closer, no usage signal at all
	far := hit("far", 0.6, now, 50)            // distant, heavily used right now
	got := rerankHits([]memory.Hit{far, close}, 0.3, 30, now)
	assert.Equal(t, []string{"close", "far"}, ids(got), "a much closer match must win over a distant but popular one at weight 0.3")
}

// The weight is the knob that trades relevance for usage. The SAME two memories
// that keep relevance order at a low weight must flip to usage order at a high
// weight — proving the parameter actually controls the blend (a test that passed
// at every weight would not be testing the blend at all).
func TestRerankWeightControlsTheBlend(t *testing.T) {
	now := time.Date(2026, 6, 18, 0, 0, 0, 0, time.UTC)
	close := hit("close", 0.1, time.Time{}, 0)
	far := hit("far", 0.6, now, 50)

	low := rerankHits([]memory.Hit{far, close}, 0.2, 30, now)
	assert.Equal(t, "close", ids(low)[0], "at low weight relevance leads")

	high := rerankHits([]memory.Hit{far, close}, 0.8, 30, now)
	assert.Equal(t, "far", ids(high)[0], "at high weight usage leads")
}

// Disabling the feature (weight 0) must be a true no-op: order is preserved
// exactly, so RERANK_WEIGHT=0 deployments behave identically to before the
// feature existed. Distances are never mutated regardless of weight.
func TestRerankDisabledIsNoOp(t *testing.T) {
	now := time.Date(2026, 6, 18, 0, 0, 0, 0, time.UTC)
	in := []memory.Hit{hit("a", 0.5, now, 9), hit("b", 0.1, time.Time{}, 0)}
	got := rerankHits(in, 0, 30, now)
	assert.Equal(t, []string{"a", "b"}, ids(got), "weight 0 must not reorder")
	assert.Equal(t, float32(0.5), got[0].Distance, "rerank must never mutate a hit's relevance distance")
}

// recencyScore falls back to CreatedAt when a memory was never reinforced, and
// is exactly 0 only when neither timestamp exists — so a brand-new memory still
// carries a recency signal rather than being treated as ancient.
func TestRecencyScoreFallbackAndZero(t *testing.T) {
	now := time.Date(2026, 6, 18, 0, 0, 0, 0, time.UTC)

	created := memory.Hit{Record: memory.Record{CreatedAt: now}}
	assert.InDelta(t, 1.0, recencyScore(created, 30, now), 1e-9, "never-accessed but just-created memory has full recency via createdAt fallback")

	oneHalfLife := memory.Hit{Record: memory.Record{LastAccessedAt: now.AddDate(0, 0, -30)}}
	assert.InDelta(t, 0.5, recencyScore(oneHalfLife, 30, now), 1e-6, "one half-life ago decays to 0.5")

	none := memory.Hit{Record: memory.Record{}}
	assert.Equal(t, 0.0, recencyScore(none, 30, now), "no timestamps at all means no usage signal")

	future := memory.Hit{Record: memory.Record{LastAccessedAt: now.AddDate(0, 0, 1)}}
	assert.InDelta(t, 1.0, recencyScore(future, 30, now), 1e-9, "clock-skewed future access is clamped to full recency, never >1")
}

// memoryProperties and memoryProps must agree on the living-memory fields, or a
// reindex/search would read a property the schema never created (or vice versa).
func TestLivingMemoryPropsPinned(t *testing.T) {
	schema := memoryProperties()
	have := map[string]bool{}
	for _, p := range schema {
		have[p.Name] = true
	}
	for _, name := range []string{"accessCount", "lastAccessedAt"} {
		require.True(t, have[name], "schema must declare %q", name)
		assert.Contains(t, memoryProps, name, "memoryProps must fetch %q so reindex carries it", name)
	}
}
