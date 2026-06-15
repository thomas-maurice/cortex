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
// hybridDistance maps Weaviate's relativeScoreFusion score (higher = better)
// onto Cortex's distance metric (lower = better) so hybrid hits sort and filter
// against the same maxDistance cutoff as vector hits. The contract: a perfect
// score (1) is distance 0, a zero score is distance 1, and the result is never
// negative (scores can marginally exceed 1).
func TestHybridDistance(t *testing.T) {
	assert.InDelta(t, 0.0, hybridDistance(1.0), 1e-6, "top score maps to distance 0")
	assert.InDelta(t, 1.0, hybridDistance(0.0), 1e-6, "zero score maps to distance 1")
	assert.InDelta(t, 0.25, hybridDistance(0.75), 1e-6, "monotonic: higher score -> lower distance")
	assert.Equal(t, float32(0), hybridDistance(1.5), "never negative even if score exceeds 1")
}

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
			"dupCandidates": &pb.ListValue{Kind: &pb.ListValue_TextValues{
				TextValues: &pb.TextValues{Values: []string{"dup-1", "dup-2"}},
			}},
			"notDuplicateOf": &pb.ListValue{Kind: &pb.ListValue_TextValues{
				TextValues: &pb.TextValues{Values: []string{"keep-1"}},
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
	// dupCandidates must decode like any other text[] property — the review tool
	// and graph UI depend on it surviving the round-trip from Weaviate.
	assert.Equal(t, []string{"dup-1", "dup-2"}, rec.DupCandidates)
	// notDuplicateOf must survive the round-trip too: it is what the worker reads
	// (threaded through reindex) to avoid re-flagging a dismissed pair.
	assert.Equal(t, []string{"keep-1"}, rec.NotDuplicateOf)
}

// buildWhere encodes the tag-filter semantics that callers depend on:
// includeTags is "must have ALL" (ContainsAll) while anyTags is "must have at
// least ONE" (ContainsAny). The two are distinct operators on the same `tags`
// property, and when both are present they must be ANDed — a regression that
// swapped the operators or dropped the AND would silently widen or narrow every
// tag-filtered query.
func TestBuildWhereTagOperators(t *testing.T) {
	t.Run("includeTags only -> ContainsAll", func(t *testing.T) {
		f := buildWhere("", "", []string{"go", "rust"}, nil).Build()
		assert.Equal(t, "ContainsAll", f.Operator)
		assert.Equal(t, []string{"tags"}, f.Path)
		assert.Equal(t, []string{"go", "rust"}, f.ValueTextArray)
	})

	t.Run("anyTags only -> ContainsAny", func(t *testing.T) {
		f := buildWhere("", "", nil, []string{"go", "rust"}).Build()
		assert.Equal(t, "ContainsAny", f.Operator)
		assert.Equal(t, []string{"tags"}, f.Path)
		assert.Equal(t, []string{"go", "rust"}, f.ValueTextArray)
	})

	t.Run("both -> AND of ContainsAll and ContainsAny", func(t *testing.T) {
		f := buildWhere("", "", []string{"go"}, []string{"rust", "zig"}).Build()
		assert.Equal(t, "And", f.Operator)
		ops := make(map[string][]string)
		for _, o := range f.Operands {
			ops[o.Operator] = o.ValueTextArray
		}
		assert.Equal(t, []string{"go"}, ops["ContainsAll"], "includeTags must stay ContainsAll")
		assert.Equal(t, []string{"rust", "zig"}, ops["ContainsAny"], "anyTags must be ContainsAny")
	})

	t.Run("no constraints -> nil", func(t *testing.T) {
		assert.Nil(t, buildWhere("", "", nil, nil))
	})
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
