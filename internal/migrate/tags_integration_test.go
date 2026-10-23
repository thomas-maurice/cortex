package migrate

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/thomas-maurice/cortex/internal/memory"
	"github.com/thomas-maurice/cortex/internal/natstest"
	"github.com/thomas-maurice/cortex/internal/store"
)

func testStore(t *testing.T, mt bool) *store.Store {
	t.Helper()
	rest, grpc := os.Getenv("CORTEX_TEST_WEAVIATE_REST"), os.Getenv("CORTEX_TEST_WEAVIATE_GRPC")
	if rest == "" || grpc == "" {
		t.Skip("set CORTEX_TEST_WEAVIATE_REST and CORTEX_TEST_WEAVIATE_GRPC to run this integration test")
	}
	st, err := store.New(rest, grpc)
	require.NoError(t, err)
	st.SetMultiTenant(mt)
	ctx := context.Background()
	require.NoError(t, st.DeleteAllClasses(ctx))
	t.Cleanup(func() { _ = st.DeleteAllClasses(ctx) })
	return st
}

func testMT() bool { return os.Getenv("CORTEX_TEST_MULTI_TENANT") != "" }

func tenants(mt bool) []string {
	if mt {
		return []string{"mig-a", "mig-b"}
	}
	return []string{""}
}

// installLegacy creates the pre-migration world: PLAIN Memory / MemoryChunk / Trash
// classes whose tags use the default word tokenization.
func installLegacy(t *testing.T, st *store.Store) {
	t.Helper()
	ctx := context.Background()
	for _, name := range []string{memory.ClassName, memory.ChunkClassName, memory.TrashClassName} {
		c := st.ClassDefinition(name)
		for _, p := range c.Properties {
			if p.Name == "tags" {
				p.Tokenization = ""
			}
		}
		require.NoError(t, st.Client().Schema().ClassCreator().WithClass(c).Do(ctx))
	}
	require.NoError(t, st.EnsureSchema(ctx)) // what a new binary does at boot on a legacy store
}

func tagsTokenization(t *testing.T, st *store.Store, class string) string {
	t.Helper()
	c, err := st.Client().Schema().ClassGetter().WithClassName(class).Do(ctx0)
	require.NoError(t, err)
	for _, p := range c.Properties {
		if p.Name == "tags" {
			return p.Tokenization
		}
	}
	t.Fatalf("class %s has no tags property", class)
	return ""
}

var ctx0 = context.Background()

func newRunner(t *testing.T, st *store.Store) *Runner {
	t.Helper()
	_, js := natstest.Start(t)
	kv, err := OpenMeta(ctx0, js)
	require.NoError(t, err)
	maint := NewMaintenance(kv, "test", quiet())
	maint.CacheFor = 0
	env := &Env{Store: st, Log: quiet(), Maint: maint, DrainTimeout: -1}
	r := NewRunner(kv, env, "test")
	r.Poll = 10 * time.Millisecond
	return r
}

type seeded struct {
	recs    []memory.Record
	vectors map[string][]float32
}

func seed(t *testing.T, st *store.Store, tenant string) seeded {
	t.Helper()
	r := rand.New(rand.NewPCG(3, 4))
	out := seeded{vectors: map[string][]float32{}}
	for i, tags := range [][]string{{"foo"}, {"Foo"}, {"foo-bar", "x"}, {"bar"}, {"doomed"}} {
		rec := memory.Record{
			ID: uuid.NewString(), Text: fmt.Sprintf("memory %d", i), Namespace: "ns", Tags: tags, Source: "s",
			CreatedAt: time.Now().UTC().Add(-time.Hour).Truncate(time.Second), Model: "m", Dims: 8,
		}
		vec := make([]float32, 8)
		for j := range vec {
			vec[j] = float32(r.NormFloat64())
		}
		ts := st.Tenant(tenant)
		require.NoError(t, ts.Upsert(ctx0, rec, vec))
		require.NoError(t, ts.ReplaceChunks(ctx0, rec, []store.ChunkVec{{Index: 0, Text: rec.Text, Vector: vec}}))
		out.recs = append(out.recs, rec)
		out.vectors[rec.ID] = vec
	}
	// One memory goes to Trash (which also carries tags).
	require.NoError(t, st.Tenant(tenant).Delete(ctx0, out.recs[4].ID))
	out.recs = out.recs[:4]
	return out
}

func idsWithTag(t *testing.T, ts *store.TenantStore, tag string) []string {
	t.Helper()
	recs, err := ts.List(ctx0, store.ListOpts{Namespace: "ns", AnyTags: []string{tag}, Limit: 100})
	require.NoError(t, err)
	var ids []string
	for _, r := range recs {
		ids = append(ids, r.ID)
	}
	slices.Sort(ids)
	return ids
}

func chunkIDsWithTag(t *testing.T, ts *store.TenantStore, q []float32, tag string) []string {
	t.Helper()
	hits, err := ts.Search(ctx0, q, store.SearchOpts{Namespace: "ns", AnyTags: []string{tag}, Limit: 20})
	require.NoError(t, err)
	var ids []string
	for _, h := range hits {
		ids = append(ids, h.ID)
	}
	slices.Sort(ids)
	return ids
}

// Migration #1 end to end on a legacy install: the tags filter goes from fuzzy to
// exact on memories AND chunks, nothing is lost or re-embedded, writers were
// actually paused while it ran, and it is recorded so it never runs twice.
func TestMigration1MakesTagFilterExact(t *testing.T) {
	mt := testMT()
	st := testStore(t, mt)
	installLegacy(t, st)
	ts := st.Tenant(tenants(mt)[0])
	data := map[string]seeded{}
	for _, tn := range tenants(mt) {
		data[tn] = seed(t, st, tn)
	}
	d := data[tenants(mt)[0]]
	byTag := func(tags ...string) []string {
		var out []string
		for _, r := range d.recs {
			if slices.ContainsFunc(r.Tags, func(x string) bool { return slices.Contains(tags, x) }) {
				out = append(out, r.ID)
			}
		}
		slices.Sort(out)
		return out
	}
	q := d.vectors[d.recs[0].ID]

	// BEFORE: the bug. "foo" also matches "Foo" and "foo-bar".
	require.Equal(t, byTag("foo", "Foo", "foo-bar"), idsWithTag(t, ts, "foo"))
	require.Equal(t, byTag("foo", "Foo", "foo-bar"), chunkIDsWithTag(t, ts, q, "foo"))
	require.Equal(t, "word", tagsTokenization(t, st, memory.ClassName))
	hitsBefore, err := ts.Search(ctx0, q, store.SearchOpts{Namespace: "ns", Limit: 10})
	require.NoError(t, err)

	r := newRunner(t, st)
	var sawMaintenance atomic.Bool
	pctx, stop := context.WithCancel(ctx0)
	defer stop()
	go func() {
		for pctx.Err() == nil {
			if r.Env.Maint.Active(pctx) {
				sawMaintenance.Store(true)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	require.NoError(t, r.Run(ctx0))
	stop()

	assert.True(t, sawMaintenance.Load(), "writers must have been paused while the data was copied")
	assert.False(t, r.Env.Maint.Active(ctx0), "and released afterwards")

	for _, c := range []string{memory.ClassName, memory.ChunkClassName, memory.TrashClassName} {
		assert.Equal(t, "field", tagsTokenization(t, st, c), "%s tags tokenization", c)
		phys, err := st.PhysicalClass(ctx0, c)
		require.NoError(t, err)
		assert.Equal(t, c+"_v1", phys)
	}

	// AFTER: exact and case-sensitive, on memories and chunks, in every tenant.
	for _, tn := range tenants(mt) {
		ts := st.Tenant(tn)
		d := data[tn]
		want := func(tag string) []string {
			var out []string
			for _, r := range d.recs {
				if slices.Contains(r.Tags, tag) {
					out = append(out, r.ID)
				}
			}
			slices.Sort(out)
			return out
		}
		q := d.vectors[d.recs[0].ID]
		for _, tag := range []string{"foo", "Foo", "foo-bar", "bar"} {
			assert.Equal(t, want(tag), idsWithTag(t, ts, tag), "memory filter %q tenant %q", tag, tn)
			assert.Equal(t, want(tag), chunkIDsWithTag(t, ts, q, tag), "chunk filter %q tenant %q", tag, tn)
		}
		recs, err := ts.List(ctx0, store.ListOpts{Namespace: "ns", Limit: 100})
		require.NoError(t, err)
		assert.Len(t, recs, len(d.recs), "no memory lost")
		for _, got := range recs {
			for _, exp := range d.recs {
				if exp.ID == got.ID {
					assert.ElementsMatch(t, exp.Tags, got.Tags, "tag values are unchanged")
				}
			}
		}
		tr, err := ts.ListTrash(ctx0, "ns")
		require.NoError(t, err)
		require.Len(t, tr, 1, "trash survives")
		assert.Equal(t, []string{"doomed"}, tr[0].Tags)
	}

	hitsAfter, err := ts.Search(ctx0, q, store.SearchOpts{Namespace: "ns", Limit: 10})
	require.NoError(t, err)
	require.Len(t, hitsAfter, len(hitsBefore))
	for i := range hitsBefore {
		assert.Equal(t, hitsBefore[i].ID, hitsAfter[i].ID, "search ranking is unchanged: vectors were copied, not recomputed")
		assert.InDelta(t, hitsBefore[i].Distance, hitsAfter[i].Distance, 1e-6)
	}

	v, _, err := SchemaVersion(ctx0, r.KV)
	require.NoError(t, err)
	assert.Equal(t, 1, v)
	stt, err := r.Status(ctx0)
	require.NoError(t, err)
	assert.True(t, stt.Migrations[0].Applied)

	// Second boot: nothing to do, no maintenance, classes untouched.
	sawMaintenance.Store(false)
	require.NoError(t, r.Run(ctx0))
	assert.False(t, r.Env.Maint.Active(ctx0))
	phys, _ := st.PhysicalClass(ctx0, memory.ClassName)
	assert.Equal(t, "Memory_v1", phys)
}

// The migration itself must be idempotent: if the process dies after the swap but
// before the version is recorded, the next boot runs Up again and it must be a no-op.
func TestMigration1UpIsIdempotent(t *testing.T) {
	mt := testMT()
	st := testStore(t, mt)
	installLegacy(t, st)
	d := seed(t, st, tenants(mt)[0])
	r := newRunner(t, st)

	require.NoError(t, tagsFieldTokenization.Up(ctx0, r.Env))
	require.NoError(t, tagsFieldTokenization.Up(ctx0, r.Env), "re-running after a completed swap")
	for _, c := range []string{memory.ClassName, memory.ChunkClassName, memory.TrashClassName} {
		phys, err := st.PhysicalClass(ctx0, c)
		require.NoError(t, err)
		assert.Equal(t, c+"_v1", phys, "no needless second rebuild of %s", c)
	}
	recs, err := st.Tenant(tenants(mt)[0]).List(ctx0, store.ListOpts{Namespace: "ns", Limit: 100})
	require.NoError(t, err)
	assert.Len(t, recs, len(d.recs))
}

// A fresh install already has field-tokenized tags (and aliases): migration #1 must
// recognise that and not rebuild or pause anything.
func TestMigration1OnFreshInstallDoesNothing(t *testing.T) {
	st := testStore(t, testMT())
	r := newRunner(t, st)
	var sawMaintenance atomic.Bool
	pctx, stop := context.WithCancel(ctx0)
	defer stop()
	go func() {
		for pctx.Err() == nil {
			if r.Env.Maint.Active(pctx) {
				sawMaintenance.Store(true)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	require.NoError(t, r.Run(ctx0))
	stop()
	assert.False(t, sawMaintenance.Load(), "an install that is already current needs no write pause")
	v, _, err := SchemaVersion(ctx0, r.KV)
	require.NoError(t, err)
	assert.Equal(t, 1, v)
	for _, c := range store.MemoryClasses() {
		phys, err := st.PhysicalClass(ctx0, c)
		require.NoError(t, err)
		assert.Equal(t, c+"_v1", phys)
	}
}

// A store whose classes disagree with CORTEX_MULTI_TENANT is repaired by `cortex
// migrate-mt`, which needs a RUNNING server: the migration must not abort the boot.
func TestMigration1SkipsTenancyMismatchInsteadOfBlockingBoot(t *testing.T) {
	st := testStore(t, true) // server says MT ...
	// ... but the legacy classes are single-tenant.
	for _, name := range []string{memory.ClassName, memory.ChunkClassName, memory.TrashClassName} {
		c := st.ClassDefinition(name)
		c.MultiTenancyConfig = &models.MultiTenancyConfig{}
		for _, p := range c.Properties {
			if p.Name == "tags" {
				p.Tokenization = ""
			}
		}
		require.NoError(t, st.Client().Schema().ClassCreator().WithClass(c).Do(ctx0))
	}
	r := newRunner(t, st)
	require.NoError(t, r.Run(ctx0))
	phys, err := st.PhysicalClass(ctx0, memory.ClassName)
	require.NoError(t, err)
	assert.Equal(t, memory.ClassName, phys, "left as a plain class for migrate-mt to rebuild")
}
