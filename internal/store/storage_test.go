package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func shard(class, name string, count, queue int64, status string) *models.NodeShardStatus {
	return &models.NodeShardStatus{Class: class, Name: name, ObjectCount: count, VectorQueueLength: queue, VectorIndexingStatus: status}
}

// Operators read these numbers to decide whether a migration/crash left work
// behind, so totals must add up across shards and nodes.
func TestAggregateShardsSumsAcrossShardsAndNodes(t *testing.T) {
	nodes := []*models.NodeStatus{
		{Name: "n1", Shards: []*models.NodeShardStatus{
			shard("Memory_v2", "alice", 10, 1, "READY"),
			shard("Memory_v2", "bob", 5, 0, "READY"),
		}},
		{Name: "n2", Shards: []*models.NodeShardStatus{
			shard("Memory_v2", "carol", 7, 2, "READY"),
			shard("MemoryChunk_v1", "alice", 40, 0, "READY"),
		}},
	}
	got := aggregateShards(nodes)
	mem := got["Memory_v2"]
	require.NotNil(t, mem)
	assert.EqualValues(t, 22, mem.count)
	assert.EqualValues(t, 3, mem.queue)
	assert.EqualValues(t, 10, mem.byShard["alice"].count)
	assert.EqualValues(t, 7, mem.byShard["carol"].count)
	assert.EqualValues(t, 40, got["MemoryChunk_v1"].count)
}

// A shard replicated on two nodes is the same data: counting it twice would
// double the figure an operator trusts.
func TestAggregateShardsDoesNotDoubleCountReplicas(t *testing.T) {
	nodes := []*models.NodeStatus{
		{Shards: []*models.NodeShardStatus{shard("Memory_v1", "alice", 10, 0, "READY")}},
		{Shards: []*models.NodeShardStatus{shard("Memory_v1", "alice", 9, 0, "READY")}},
	}
	got := aggregateShards(nodes)["Memory_v1"]
	assert.EqualValues(t, 10, got.count)
	assert.EqualValues(t, 10, got.byShard["alice"].count)
}

// One shard still INDEXING means the class is not fully searchable, whatever the
// other shards say.
func TestWorstIndexingStatus(t *testing.T) {
	nodes := []*models.NodeStatus{{Shards: []*models.NodeShardStatus{
		shard("Memory_v1", "a", 1, 0, "READY"),
		shard("Memory_v1", "b", 1, 0, "INDEXING"),
		shard("Memory_v1", "c", 1, 0, "READONLY"),
	}}}
	got := aggregateShards(nodes)["Memory_v1"]
	assert.Equal(t, "INDEXING", got.status)
	assert.Equal(t, "READY", got.byShard["a"].status, "per-tenant status is not polluted by siblings")
	assert.Equal(t, "READONLY", worseStatus("READY", "READONLY"))
	assert.Equal(t, "READONLY", worseStatus("READONLY", "READY"))
	assert.Equal(t, "READY", worseStatus("", "READY"))
}

// After an interrupted migration Memory_v1 lingers: it must be flagged, while the
// live target and legacy plain known classes must never be.
func TestFindOrphans(t *testing.T) {
	known := []string{"Memory", "Trash"}
	targets := map[string]string{"Memory": "Memory_v2"}

	got := findOrphans([]string{"Memory_v1", "Memory_v2", "Trash", "Stray"}, targets, known)
	assert.Equal(t, []string{"Memory_v1", "Stray"}, got)

	assert.Empty(t, findOrphans([]string{"Memory_v2"}, targets, known), "live alias target is never an orphan")
	assert.Empty(t, findOrphans([]string{"Memory"}, map[string]string{}, known), "legacy plain class is live")
}

func TestVectorSettings(t *testing.T) {
	c := &models.Class{VectorIndexType: "hnsw", Vectorizer: "none",
		VectorIndexConfig: map[string]any{"distance": "cosine"}}
	typ, dist, vz := vectorSettings(c)
	assert.Equal(t, "hnsw", typ)
	assert.Equal(t, "cosine", dist)
	assert.Equal(t, "none", vz)

	dyn := &models.Class{VectorIndexType: "dynamic",
		VectorIndexConfig: map[string]any{"hnsw": map[string]any{"distance": "dot"}}}
	_, dist, _ = vectorSettings(dyn)
	assert.Equal(t, "dot", dist)

	named := &models.Class{VectorConfig: map[string]models.VectorConfig{
		"default": {VectorIndexType: "flat", VectorIndexConfig: map[string]any{"distance": "l2-squared"}},
	}}
	typ, dist, _ = vectorSettings(named)
	assert.Equal(t, "flat", typ)
	assert.Equal(t, "l2-squared", dist)
}

func TestCountFromAggregate(t *testing.T) {
	data := map[string]models.JSONObject{"Aggregate": map[string]any{
		"Memory_v2": []any{map[string]any{"meta": map[string]any{"count": float64(7)}}},
	}}
	assert.EqualValues(t, 7, countFromAggregate(data, "Memory_v2"))
	assert.Equal(t, CountUnknown, countFromAggregate(data, "Other"), "missing class is unknown, not 0")
	assert.Equal(t, CountUnknown, countFromAggregate(nil, "Memory_v2"))
}
