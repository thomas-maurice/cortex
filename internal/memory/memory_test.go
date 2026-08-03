package memory

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// These are GOLDEN-VALUE tests, not behaviour tests. The four ID functions are
// pure uuidv5 derivations whose outputs are baked into stored data: a Weaviate
// object id (which decides overwrite-vs-duplicate) and — for UserID — the tenant
// name that isolates a user's entire memory set. If a derivation ever silently
// changes (a tweaked prefix, a different namespace UUID, a formatting change),
// every existing record becomes unreachable under the new id. These frozen values
// make that failure loud instead of silent.
//
// The admin UserID in particular is the single live tenant on the production
// deployment; a prior incident traced "all memories vanished" to querying the
// wrong tenant. Do NOT "fix" a failing assertion here by updating the expected
// value — a mismatch means the derivation changed and existing data would be
// orphaned. Change these only alongside a deliberate, migrated id scheme.

func TestSummaryIDGolden(t *testing.T) {
	assert.Equal(t, "1f4f59f3-31e8-5393-839d-c0fb281d3cf2", SummaryID("conv-123"))
	assert.Equal(t, "25c2c5d3-ff40-595f-9510-b5ab94866817", SummaryID(""))
}

func TestChunkIDGolden(t *testing.T) {
	assert.Equal(t, "e001da3a-116d-5054-b5f6-716bb397827e", ChunkID("mem-1", 0))
	assert.Equal(t, "f1adebdd-aa34-5d38-bda8-1011bf035820", ChunkID("mem-1", 7))
}

func TestUserIDGolden(t *testing.T) {
	// This exact UUID is the live admin tenant in production — see the package doc.
	assert.Equal(t, "e232b9c8-9379-5078-9c62-e859285af71a", UserID("admin"))
	assert.Equal(t, "c3c75a89-f8c9-5e1d-89ea-8ae0ab457da9", UserID("bob"))
}

func TestApiKeyIDGolden(t *testing.T) {
	assert.Equal(t, "bc248c77-7da9-5b5a-9d9c-a38857aae3df", ApiKeyID("deadbeef"))
}

// TestIDsAreDistinctPerNamespace guards the WHY behind the per-function string
// prefixes ("cortex/summary:", "cortex/chunk:", ...): they keep the four id
// spaces disjoint so the same raw key can never collide across kinds — e.g. a
// user named "x" and a summary for conversation "x" must not share an object id.
func TestIDsAreDistinctPerNamespace(t *testing.T) {
	const key = "x"
	ids := []string{SummaryID(key), UserID(key), ApiKeyID(key), ChunkID(key, 0)}
	seen := map[string]bool{}
	for _, id := range ids {
		assert.False(t, seen[id], "id %q collides across kinds for key %q", id, key)
		seen[id] = true
	}
}

// TestChunkIDVariesByIndex pins that each chunk of a memory gets its own id, which
// is what lets a re-index overwrite chunks in place per index slot.
func TestChunkIDVariesByIndex(t *testing.T) {
	assert.NotEqual(t, ChunkID("mem", 0), ChunkID("mem", 1))
	assert.Equal(t, ChunkID("mem", 3), ChunkID("mem", 3), "must be deterministic")
}
