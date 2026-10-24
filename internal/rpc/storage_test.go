package rpc

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/store"
)

func fullStorageResponse() *cortexv1.GetStorageInfoResponse {
	info := &store.StorageInfo{
		Orphans: []string{"Memory_v1", "Secret_orphan"},
		Nodes:   []store.NodeStorage{{Name: "node-zeta", Version: "1.38.0", ShardCount: 5}},
		Classes: []store.ClassStorage{
			{
				Logical: "Memory", Physical: "Memory_v2", Version: 2, Aliased: true, MultiTenant: true,
				ObjectCount: 60, TenantCount: 3, VectorIndexType: "hnsw", Distance: "cosine", StoredDims: 1024,
				IndexingStatus: "INDEXING", VectorQueueLength: 9,
				Tenants: []store.TenantStorage{
					{Tenant: "tid-alice", Activity: "HOT", ObjectCount: 10, IndexingStatus: "READY", QueueLength: 0},
					{Tenant: "tid-bob", Activity: "HOT", ObjectCount: 20, IndexingStatus: "INDEXING", QueueLength: 9},
					{Tenant: "tid-carol", Activity: "COLD", ObjectCount: 30, IndexingStatus: "READY"},
				},
			},
			{Logical: "User", Physical: "User_v1", Aliased: true, ObjectCount: 3},
			{Logical: "ApiKey", Physical: "ApiKey_v1", Aliased: true, ObjectCount: 4},
			{Logical: "CortexMeta", Physical: "CortexMeta", ObjectCount: 7},
		},
	}
	resp := storageToProto(info, map[string]string{"tid-alice": "alice", "tid-bob": "bobby-tables", "tid-carol": "carol"})
	resp.EmbedModel, resp.SchemaVersion = "qwen3", 2
	return resp
}

// collectStrings returns every string value reachable from m, including repeated
// and nested messages, so a field added to the proto later is covered without
// editing the test.
func collectStrings(m protoreflect.Message) []string {
	var out []string
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		visit := func(v protoreflect.Value) {
			switch {
			case fd.Kind() == protoreflect.StringKind:
				out = append(out, v.String())
			case fd.Kind() == protoreflect.MessageKind:
				out = append(out, collectStrings(v.Message())...)
			}
		}
		if fd.IsList() {
			for i := 0; i < v.List().Len(); i++ {
				visit(v.List().Get(i))
			}
		} else {
			visit(v)
		}
		return true
	})
	return out
}

// The security invariant: a non-admin must learn nothing about other tenants
// (ids, usernames) nor about deployment internals. Walking EVERY string field
// means a field added later and forgotten in scopeToTenant fails here.
func TestScopeToTenantLeaksNothingAboutOthers(t *testing.T) {
	resp := fullStorageResponse()
	scopeToTenant(resp, "tid-alice")

	assert.True(t, resp.Scoped)
	all := strings.Join(collectStrings(resp.ProtoReflect()), "\x00")
	for _, secret := range []string{"tid-bob", "bobby-tables", "tid-carol", "carol", "node-zeta", "Secret_orphan", "Memory_v1", "User", "ApiKey", "CortexMeta"} {
		assert.NotContains(t, all, secret)
	}
	assert.Contains(t, all, "alice", "own username is kept")
	assert.Empty(t, resp.Nodes)
	assert.Empty(t, resp.OrphanClasses)

	require.Len(t, resp.Classes, 1, "global identity and metadata classes are omitted")
	mem := resp.Classes[0]
	assert.Equal(t, "Memory_v2", mem.Physical, "deployment facts are kept")
	assert.EqualValues(t, 1024, mem.StoredDims)
	require.Len(t, mem.Tenants, 1)
	assert.EqualValues(t, 1, mem.TenantCount)
	assert.EqualValues(t, 10, mem.ObjectCount, "count is the caller's shard only, not 60")
	assert.Equal(t, "READY", mem.IndexingStatus, "status is the caller's shard, not the INDEXING neighbour")
	assert.EqualValues(t, 0, mem.VectorQueueLength, "queue is the caller's shard, not the neighbour's 9")
	assert.Equal(t, "qwen3", resp.EmbedModel)
}

// A caller who has never written has no shard: report empty, not the global count.
func TestScopeToTenantWithoutShard(t *testing.T) {
	resp := fullStorageResponse()
	scopeToTenant(resp, "tid-nobody")
	require.Len(t, resp.Classes, 1)
	assert.EqualValues(t, 0, resp.Classes[0].ObjectCount)
	assert.EqualValues(t, 0, resp.Classes[0].TenantCount)
	assert.Empty(t, resp.Classes[0].Tenants)
}

// Admins get the unscoped response untouched (the handler only calls
// scopeToTenant for non-admins in MT mode; the full mapping keeps everything).
func TestStorageToProtoFullKeepsEverything(t *testing.T) {
	resp := fullStorageResponse()
	assert.False(t, resp.Scoped)
	assert.Len(t, resp.Classes, 4)
	assert.Len(t, resp.Nodes, 1)
	assert.Len(t, resp.OrphanClasses, 2)
	assert.Len(t, resp.Classes[0].Tenants, 3)
	assert.Equal(t, "bobby-tables", resp.Classes[0].Tenants[1].Username)
	assert.True(t, proto.Equal(resp, fullStorageResponse()))
}
