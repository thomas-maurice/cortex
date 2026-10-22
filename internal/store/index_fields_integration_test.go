package store

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// integrationStore connects to a throwaway Weaviate (env-gated like the other
// integration tests; CORTEX_TEST_MULTI_TENANT selects the tenancy mode) with a
// clean schema, returning the tenant name to use ("" when MT is off).
func integrationStore(t *testing.T) (*Store, string) {
	t.Helper()
	rest := os.Getenv("CORTEX_TEST_WEAVIATE_REST")
	grpc := os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST and CORTEX_TEST_WEAVIATE_GRPC to run this integration test")
	}
	mt := os.Getenv("CORTEX_TEST_MULTI_TENANT") != ""
	st, err := New(rest, grpc)
	require.NoError(t, err)
	st.SetMultiTenant(mt)
	ctx := context.Background()
	_ = st.DeleteAllClasses(ctx)
	t.Cleanup(func() { _ = st.DeleteAllClasses(ctx) })
	if mt {
		return st, "idx-fields-tenant"
	}
	return st, ""
}

// TestUpdateIndexFieldsMerge pins the field-ownership contract of the index write
// path on a real Weaviate: a merge-with-vector rewrites text/vector/seq and leaves
// the fields other writers own (links, reinforcement, not-duplicate decisions)
// untouched, and merges of DISJOINT fields from concurrent writers never clobber
// each other (the property the lock-free server-side merges rely on).
func TestUpdateIndexFieldsMerge(t *testing.T) {
	st, tenant := integrationStore(t)
	ctx := context.Background()
	require.NoError(t, st.EnsureSchema(ctx))
	ts := st.Tenant(tenant)

	id := uuid.NewString()
	created := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, ts.Upsert(ctx, memory.Record{
		ID: id, Text: "before", Namespace: "ns", Tags: []string{"a"}, Source: "s", CreatedAt: created,
		Model: "m", Dims: 4, LinkedIDs: []string{"l1", "l2"}, AccessCount: 4, LastAccessedAt: created, UseCount: 7, LastUsedAt: created,
		NotDuplicateOf: []string{"nd1"}, IndexedSeq: 1,
	}, []float32{1, 0, 0, 0}))

	require.NoError(t, ts.UpdateIndexFields(ctx, memory.Record{
		ID: id, Text: "after", Namespace: "ns2", Tags: []string{"b"}, Source: "s", CreatedAt: created,
		Model: "m2", Dims: 4, DupCandidates: []string{"d1"}, IndexedSeq: 42, IndexedStream: 1760000000999999,
		// A stale snapshot on the payload: must be ignored.
		LinkedIDs: []string{"STALE"}, AccessCount: 0, UseCount: 0, NotDuplicateOf: []string{"STALE"},
	}, []float32{0, 1, 0, 0}))

	got, ok, err := ts.Get(ctx, id)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "after", got.Text)
	assert.Equal(t, "ns2", got.Namespace)
	assert.Equal(t, []string{"b"}, got.Tags)
	assert.Equal(t, "m2", got.Model)
	assert.Equal(t, []string{"d1"}, got.DupCandidates)
	assert.EqualValues(t, 42, got.IndexedSeq, "indexedSeq must round-trip")
	assert.EqualValues(t, 1760000000999999, got.IndexedStream, "indexedStream must round-trip exactly")
	assert.Equal(t, []string{"l1", "l2"}, got.LinkedIDs, "links belong to the link consumer")
	assert.Equal(t, 4, got.AccessCount, "reinforcement belongs to the server")
	assert.True(t, got.LastAccessedAt.Equal(created))
	assert.Equal(t, 7, got.UseCount, "usage tracking belongs to the server (RecordUse)")
	assert.True(t, got.LastUsedAt.Equal(created))
	assert.Equal(t, []string{"nd1"}, got.NotDuplicateOf)

	hits, err := ts.SearchMemoryVectors(ctx, []float32{0, 1, 0, 0}, SearchOpts{Limit: 3})
	require.NoError(t, err)
	require.NotEmpty(t, hits)
	assert.Equal(t, id, hits[0].ID)
	assert.Less(t, hits[0].Distance, float32(0.01), "the merge must have replaced the vector")

	t.Run("disjoint-field merges from concurrent writers do not clobber each other", func(t *testing.T) {
		const n = 30
		var wg sync.WaitGroup
		errs := make(chan error, 3*n)
		wg.Go(func() {
			for i := 1; i <= n; i++ {
				errs <- ts.SetLinks(ctx, id, []string{fmt.Sprintf("l-%d", i)})
			}
		})
		wg.Go(func() {
			for i := 1; i <= n; i++ {
				errs <- ts.Reinforce(ctx, id, 100+i, created)
			}
		})
		wg.Go(func() {
			for i := 1; i <= n; i++ {
				errs <- ts.UpdateIndexFields(ctx, memory.Record{ID: id, Text: fmt.Sprintf("t-%d", i), Namespace: "ns2",
					CreatedAt: created, IndexedSeq: uint64(100 + i)}, []float32{0, 1, 0, 0})
			}
		})
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		got, _, err := ts.Get(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, []string{fmt.Sprintf("l-%d", n)}, got.LinkedIDs)
		assert.Equal(t, 100+n, got.AccessCount)
		assert.Equal(t, fmt.Sprintf("t-%d", n), got.Text)
		assert.EqualValues(t, 100+n, got.IndexedSeq)
	})
}

// TestIndexedSeqAddedToExistingClass proves an upgrade needs no rebuild: classes
// created before indexedSeq existed gain the property via EnsureSchema (on MT
// classes too), objects written before it read as seq 0, and the property then
// round-trips for both memories and summaries.
func TestIndexedSeqAddedToExistingClass(t *testing.T) {
	st, tenant := integrationStore(t)
	ctx := context.Background()

	// Create the OLD-shape classes (no indexedSeq).
	for _, c := range []*models.Class{memoryClass(st.multiTenant), summaryClass(st.multiTenant)} {
		var old []*models.Property
		for _, p := range c.Properties {
			if p.Name != "indexedSeq" {
				old = append(old, p)
			}
		}
		c.Properties = old
		require.NoError(t, st.client.Schema().ClassCreator().WithClass(c).Do(ctx))
	}
	if st.multiTenant {
		for _, cn := range []string{memory.ClassName, memory.SummaryClassName} {
			require.NoError(t, st.client.Schema().TenantsCreator().WithClassName(cn).
				WithTenants(models.Tenant{Name: tenant}).Do(ctx))
		}
	}
	// A pre-upgrade memory, written without the property.
	id := uuid.NewString()
	creator := st.client.Data().Creator().WithClassName(memory.ClassName).WithID(id).
		WithProperties(map[string]interface{}{"text": "legacy", "namespace": "ns"}).WithVector([]float32{1, 0, 0, 0})
	if tenant != "" {
		creator = creator.WithTenant(tenant)
	}
	_, err := creator.Do(ctx)
	require.NoError(t, err)

	require.NoError(t, st.EnsureSchema(ctx), "EnsureSchema must add the missing property additively")
	problems, err := st.VerifySchema(ctx)
	require.NoError(t, err)
	for _, p := range problems {
		assert.NotContains(t, p, "indexedSeq")
	}

	ts := st.Tenant(tenant)
	got, ok, err := ts.Get(ctx, id)
	require.NoError(t, err)
	require.True(t, ok)
	assert.EqualValues(t, 0, got.IndexedSeq, "pre-existing objects must read as seq 0")

	require.NoError(t, ts.UpdateIndexFields(ctx, memory.Record{ID: id, Text: "legacy2", Namespace: "ns", CreatedAt: time.Now(), IndexedSeq: 9},
		[]float32{1, 0, 0, 0}))
	got, _, err = ts.Get(ctx, id)
	require.NoError(t, err)
	assert.EqualValues(t, 9, got.IndexedSeq)

	// Summaries.
	noSeq, noStream, err := ts.GetSummarySeq(ctx, "conv-none")
	require.NoError(t, err)
	assert.Zero(t, noSeq)
	assert.Zero(t, noStream, "a missing summary reads as 0,0")
	require.NoError(t, ts.UpsertSummary(ctx, memory.Summary{ConversationID: "conv-1", Text: "s", Namespace: "ns",
		CreatedAt: time.Now(), UpdatedAt: time.Now(), IndexedSeq: 7, IndexedStream: 1760000000123456}, []float32{1, 0, 0, 0}))
	seq, stream, err := ts.GetSummarySeq(ctx, "conv-1")
	require.NoError(t, err)
	assert.EqualValues(t, 7, seq)
	assert.EqualValues(t, 1760000000123456, stream, "stream identity (unix micros, > 2^53/1000) must round-trip exactly")
}
