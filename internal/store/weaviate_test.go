package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

// The vectorization invariant is load-bearing: if the Memory class ever gains a
// vectorizer module, Weaviate would start embedding metadata (id, tags,
// namespace) and a vector search could match on those instead of memory text.
// This pins Vectorizer to "none" so any such change fails loudly in CI rather
// than silently corrupting search relevance.
func TestMemoryClassVectorizerNone(t *testing.T) {
	for _, class := range []*models.Class{memoryClass(), summaryClass()} {
		assert.Equal(t, "none", class.Vectorizer, "%s must not auto-vectorize; vectors are supplied by the worker from text only", class.Class)
		assert.Empty(t, class.ModuleConfig, "no vector module may be configured on %s", class.Class)

		names := make([]string, 0, len(class.Properties))
		for _, p := range class.Properties {
			names = append(names, p.Name)
			assert.Empty(t, p.ModuleConfig, "property %q on %s must carry no vectorizer module config", p.Name, class.Class)
		}
		assert.Contains(t, names, "text", "%s must have a text property (the only embedded field)", class.Class)
	}
}

// resultToRecord decodes the gRPC search result's loosely-typed property map.
// The test pins the contract that ID/distance come from result metadata (not
// properties), that a text[] tags property survives the *pb.ListValue
// round-trip, and that an int dims arrives as int64 — a regression here would
// silently return memories with empty IDs (breaking delete) or dropped tags.
func TestResultToRecord(t *testing.T) {
	r := graphql.SearchResult{
		ID: "abc-123",
		Properties: map[string]any{
			"text":           "Thomas prefers Go",
			"namespace":      "global",
			"source":         "claude-code",
			"model":          "qwen3-embedding:0.6b",
			"dims":           int64(1024),
			"createdAt":      "2026-06-11T12:00:00Z",
			"conversationId": "sess-42",
			"tags": &pb.ListValue{Kind: &pb.ListValue_TextValues{
				TextValues: &pb.TextValues{Values: []string{"pref", "lang"}},
			}},
		},
		Metadata: graphql.MetadataResult{Distance: 0.25},
	}

	rec := resultToRecord(r)
	assert.Equal(t, "abc-123", rec.ID, "ID must come from result metadata, not the property map")
	assert.Equal(t, "Thomas prefers Go", rec.Text)
	assert.Equal(t, "global", rec.Namespace)
	assert.Equal(t, []string{"pref", "lang"}, rec.Tags)
	assert.Equal(t, 1024, rec.Dims)
	assert.Equal(t, "sess-42", rec.ConversationID)
	assert.Equal(t, 2026, rec.CreatedAt.Year())
}

// A result with missing/empty properties must decode to a zero-valued record
// without panicking, so a partial object never crashes a query.
func TestResultToRecordEmpty(t *testing.T) {
	rec := resultToRecord(graphql.SearchResult{})
	assert.Empty(t, rec.ID)
	assert.Empty(t, rec.Tags)
	assert.Zero(t, rec.Dims)
	assert.True(t, rec.CreatedAt.IsZero())
}
