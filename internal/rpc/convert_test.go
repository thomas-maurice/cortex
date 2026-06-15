package rpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// protoToRecord is the import half of dump/restore, so it must faithfully carry
// everything a restore needs to reconstruct a memory: id, namespace, tags,
// source, createdAt, conversationId, and BOTH relationship sets (linkedIds,
// notDuplicateOf). It must NOT carry dupCandidates — those are a heuristic the
// worker recomputes on (re)index, and importing them would resurrect stale flags.
func TestProtoToRecordRoundTrip(t *testing.T) {
	created := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	orig := recordToProto(memRecordForTest(created))

	got := protoToRecord(orig)

	assert.Equal(t, "id-1", got.ID)
	assert.Equal(t, "host mega-fucker runs cortex prod", got.Text)
	assert.Equal(t, "homelab", got.Namespace)
	assert.Equal(t, []string{"infra", "prod"}, got.Tags)
	assert.Equal(t, "cortex", got.Source)
	assert.True(t, created.Equal(got.CreatedAt), "createdAt must survive the round-trip")
	assert.Equal(t, "sess-9", got.ConversationID)
	assert.Equal(t, []string{"link-a"}, got.LinkedIDs, "links must be preserved so the graph survives a restore")
	assert.Equal(t, []string{"notdup-b"}, got.NotDuplicateOf, "dismissed-duplicate decisions must be preserved")
	assert.Empty(t, got.DupCandidates, "dup candidates must be dropped — the worker recomputes them")
}

func memRecordForTest(created time.Time) memory.Record {
	return memory.Record{
		ID:             "id-1",
		Text:           "host mega-fucker runs cortex prod",
		Namespace:      "homelab",
		Tags:           []string{"infra", "prod"},
		Source:         "cortex",
		CreatedAt:      created,
		ConversationID: "sess-9",
		LinkedIDs:      []string{"link-a"},
		DupCandidates:  []string{"dup-x"}, // present on the source, must NOT survive import
		NotDuplicateOf: []string{"notdup-b"},
	}
}
