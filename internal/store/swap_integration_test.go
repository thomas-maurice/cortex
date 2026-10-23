package store

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// aliasTargetsOf is a test shorthand.
func aliasTargetsOf(t *testing.T, st *Store) map[string]string {
	t.Helper()
	m, err := st.aliasTargets(context.Background())
	require.NoError(t, err)
	return m
}

func classNamesOf(t *testing.T, st *Store) []string {
	t.Helper()
	n, err := st.allClassNames(context.Background())
	require.NoError(t, err)
	return n
}

// tenantsFor returns the tenants a test should populate: two in MT mode, else the
// single "" tenant.
func tenantsFor(st *Store) []string {
	if st.multiTenant {
		return []string{"swap-a", "swap-b"}
	}
	return []string{""}
}

// legacyMemoryClass is the Memory class as shipped before alias-backed classes and
// field-tokenized tags: a PLAIN class named Memory whose tags use the default word
// tokenization.
func legacyMemoryClass(mt bool) *models.Class {
	c := memoryClass(mt)
	for _, p := range c.Properties {
		if p.Name == "tags" {
			p.Tokenization = ""
		}
	}
	return c
}

func randVec(r *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = float32(r.NormFloat64())
	}
	return v
}

// seedLegacyMemories creates the legacy plain Memory class and fills every tenant
// with memories whose tags differ only in case / hyphenation.
func seedLegacyMemories(t *testing.T, st *Store) map[string][]memory.Record {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, st.DeleteAllClasses(ctx))
	require.NoError(t, st.client.Schema().ClassCreator().WithClass(legacyMemoryClass(st.multiTenant)).Do(ctx))
	// A real legacy install is then booted by the new code: EnsureSchema must leave
	// the plain Memory class alone and create the others aliased.
	require.NoError(t, st.EnsureSchema(ctx))
	require.NotContains(t, aliasTargetsOf(t, st), memory.ClassName)
	r := rand.New(rand.NewPCG(1, 2))
	out := map[string][]memory.Record{}
	tagSets := [][]string{{"foo"}, {"Foo"}, {"foo-bar", "x"}, {"bar"}, {}}
	for _, tn := range tenantsFor(st) {
		ts := st.Tenant(tn)
		for i, tags := range tagSets {
			rec := memory.Record{
				ID: uuid.NewString(), Text: fmt.Sprintf("memory %d of %q", i, tn), Namespace: "ns", Tags: tags,
				Source: "s", CreatedAt: time.Now().UTC().Add(-time.Hour).Truncate(time.Second), Model: "m", Dims: 8,
				LinkedIDs: []string{uuid.NewString()}, AccessCount: i, UseCount: i + 1,
				LastUsedAt: time.Now().UTC().Truncate(time.Second),
			}
			require.NoError(t, ts.Upsert(ctx, rec, randVec(r, 8)))
			out[tn] = append(out[tn], rec)
		}
	}
	return out
}

func idsOfRecs(rs []memory.Record) []string {
	ids := make([]string, 0, len(rs))
	for _, r := range rs {
		ids = append(ids, r.ID)
	}
	slices.Sort(ids)
	return ids
}

// A fresh install must end up alias-based from day one, and every schema/tenant
// WRITE path must keep working through that indirection (the failure modes are
// silent: ClassDeleter on an alias deletes nothing, TenantsDeleter says "not found").
func TestFreshInstallIsAliasBased(t *testing.T) {
	st, _ := trashStore(t)
	ctx := context.Background()

	targets := aliasTargetsOf(t, st)
	for _, name := range MemoryClasses() {
		assert.Equal(t, name+"_v1", targets[name], "%s must be an alias of %s_v1", name, name)
	}
	names := classNamesOf(t, st)
	assert.NotContains(t, names, memory.ClassName, "no plain Memory class may coexist with the alias")

	require.NoError(t, st.EnsureSchema(ctx), "EnsureSchema is idempotent on an alias-based install")
	assert.Equal(t, targets, aliasTargetsOf(t, st))
	assert.ElementsMatch(t, names, classNamesOf(t, st), "a second EnsureSchema creates nothing")

	problems, err := st.VerifySchema(ctx)
	require.NoError(t, err)
	assert.Empty(t, problems, "a fresh alias-based schema (field-tokenized tags) is healthy")

	phys, err := st.PhysicalClass(ctx, memory.ClassName)
	require.NoError(t, err)
	assert.Equal(t, "Memory_v1", phys)

	t.Run("EnsureSchema adds a missing property to the physical class", func(t *testing.T) {
		require.NoError(t, st.client.Schema().ClassDeleter().WithClassName("Memory_v1").Do(ctx))
		require.NoError(t, st.client.Alias().AliasDeleter().WithAliasName(memory.ClassName).Do(ctx))
		old := legacyMemoryClass(st.multiTenant)
		old.Class = "Memory_v1"
		var props []*models.Property
		for _, p := range old.Properties {
			if p.Name != "validUntil" {
				props = append(props, p)
			}
		}
		old.Properties = props
		require.NoError(t, st.client.Schema().ClassCreator().WithClass(old).Do(ctx))
		require.NoError(t, st.createAlias(ctx, memory.ClassName, "Memory_v1"))
		require.NoError(t, st.EnsureSchema(ctx))
		got, err := st.client.Schema().ClassGetter().WithClassName("Memory_v1").Do(ctx)
		require.NoError(t, err)
		var names []string
		for _, p := range got.Properties {
			names = append(names, p.Name)
		}
		assert.Contains(t, names, "validUntil")
	})

	t.Run("DeleteClass removes alias and physical class", func(t *testing.T) {
		require.NoError(t, st.DeleteClass(ctx))
		assert.NotContains(t, aliasTargetsOf(t, st), memory.ClassName)
		assert.NotContains(t, classNamesOf(t, st), "Memory_v1", "ClassDeleter on the alias would have left the data behind")
		require.NoError(t, st.EnsureSchema(ctx))
		assert.Equal(t, "Memory_v1", aliasTargetsOf(t, st)[memory.ClassName])
	})

	t.Run("DropTenant reaches the physical classes", func(t *testing.T) {
		if !st.multiTenant {
			t.Skip("tenants only exist in multi-tenant mode")
		}
		ts := st.Tenant("drop-me")
		require.NoError(t, ts.Upsert(ctx, memory.Record{ID: uuid.NewString(), Text: "x", Namespace: "n"}, []float32{1, 0, 0, 0}))
		ok, err := st.client.Schema().TenantsExists().WithClassName(memory.ClassName).WithTenant("drop-me").Do(ctx)
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, st.DropTenant(ctx, "drop-me"))
		ok, err = st.client.Schema().TenantsExists().WithClassName(memory.ClassName).WithTenant("drop-me").Do(ctx)
		require.NoError(t, err)
		assert.False(t, ok, "tenant must be gone (a swallowed 'class not found' would leave it)")
	})
}

// Every property type must survive a copy. The gRPC client returns arrays as
// *pb.ListValue; an unconverted one is silently dropped by the insert, so this is
// checked per property against an independent REST read of source and destination.
func TestCopyClassWithVectorsRoundTripsEveryPropertyType(t *testing.T) {
	st, _ := trashStore(t)
	ctx := context.Background()
	old := copyPageSize
	copyPageSize = 3 // force several cursor pages
	t.Cleanup(func() { copyPageSize = old })

	mk := func(name string) *models.Class {
		c := &models.Class{Class: name, Vectorizer: "none", Properties: []*models.Property{
			{Name: "s", DataType: []string{"text"}},
			{Name: "ss", DataType: []string{"text[]"}},
			{Name: "i", DataType: []string{"int"}},
			{Name: "is", DataType: []string{"int[]"}},
			{Name: "n", DataType: []string{"number"}},
			{Name: "ns", DataType: []string{"number[]"}},
			{Name: "b", DataType: []string{"boolean"}},
			{Name: "bs", DataType: []string{"boolean[]"}},
			{Name: "d", DataType: []string{"date"}},
			{Name: "ds", DataType: []string{"date[]"}},
		}}
		if st.multiTenant {
			c.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true, AutoTenantCreation: true}
		}
		return c
	}
	for _, n := range []string{"CopySrc", "CopyDst"} {
		_ = st.client.Schema().ClassDeleter().WithClassName(n).Do(ctx)
		require.NoError(t, st.client.Schema().ClassCreator().WithClass(mk(n)).Do(ctx))
		n := n
		t.Cleanup(func() { _ = st.client.Schema().ClassDeleter().WithClassName(n).Do(ctx) })
	}

	r := rand.New(rand.NewPCG(7, 7))
	type obj struct {
		id, tenant string
		vec        []float32
	}
	var objs []obj
	for _, tn := range tenantsFor(st) {
		batch := []*models.Object{}
		for i := range 8 { // 8 > page size 3, and not a multiple of it
			o := obj{id: uuid.NewString(), tenant: tn, vec: randVec(r, 16)}
			props := map[string]any{
				"s": fmt.Sprintf("text %d", i), "ss": []string{"Foo", "foo-bar", "ünï"},
				"i": 1 << 40, "is": []int64{-1, 0, 1 << 41}, "n": 3.14159, "ns": []float64{0.5, -2.25},
				"b": true, "bs": []bool{true, false},
				"d": "2025-01-02T03:04:05Z", "ds": []string{"2024-12-31T23:59:59Z", "2025-06-01T00:00:00Z"},
			}
			if i == 1 {
				props = map[string]any{"s": "sparse"} // absent props must stay absent
			}
			if i == 2 {
				props["ss"], props["is"] = []string{}, []int64{} // empty arrays stay arrays
			}
			batch = append(batch, &models.Object{Class: "CopySrc", ID: toUUID(o.id), Tenant: tn, Vector: o.vec, Properties: props})
			objs = append(objs, o)
		}
		require.NoError(t, st.batchInsert(ctx, batch))
	}

	counts, err := st.CopyClassWithVectors(ctx, "CopySrc", "CopyDst", nil)
	require.NoError(t, err)
	for _, tn := range tenantsFor(st) {
		assert.Equal(t, 8, counts[tn], "per-tenant count for %q", tn)
	}

	get := func(class string, o obj) *models.Object {
		g := st.client.Data().ObjectsGetter().WithClassName(class).WithID(o.id).WithVector()
		if o.tenant != "" {
			g = g.WithTenant(o.tenant)
		}
		res, err := g.Do(ctx)
		require.NoError(t, err)
		require.Len(t, res, 1, "%s/%s", class, o.id)
		return res[0]
	}
	for _, o := range objs {
		src, dst := get("CopySrc", o), get("CopyDst", o)
		assert.Equal(t, src.Properties, dst.Properties, "every property of %s must survive", o.id)
		assert.Equal(t, o.vec, []float32(dst.Vector), "vector must be bit-identical, not re-embedded")
	}
	// Spot-check the values themselves so a drop on BOTH sides cannot pass.
	full := get("CopyDst", objs[0]).Properties.(map[string]any)
	assert.Equal(t, []any{"Foo", "foo-bar", "ünï"}, full["ss"])
	assert.Equal(t, []any{float64(-1), float64(0), float64(1 << 41)}, full["is"])
	assert.Equal(t, []any{true, false}, full["bs"])
	assert.Len(t, full["ds"], 2)
	assert.Equal(t, float64(1<<40), full["i"])
	sparse := get("CopyDst", objs[1]).Properties.(map[string]any)
	assert.NotContains(t, sparse, "ss")
	empty := get("CopyDst", objs[2]).Properties.(map[string]any)
	assert.Equal(t, []any{}, empty["ss"])

	t.Run("transform rewrites properties", func(t *testing.T) {
		_ = st.client.Schema().ClassDeleter().WithClassName("CopyDst").Do(ctx)
		require.NoError(t, st.client.Schema().ClassCreator().WithClass(mk("CopyDst")).Do(ctx))
		_, err := st.CopyClassWithVectors(ctx, "CopySrc", "CopyDst", func(p map[string]any) map[string]any {
			p["s"] = "rewritten"
			return p
		})
		require.NoError(t, err)
		assert.Equal(t, "rewritten", get("CopyDst", objs[0]).Properties.(map[string]any)["s"])
	})

	t.Run("a destination missing an object fails verification", func(t *testing.T) {
		del := st.client.Data().Deleter().WithClassName("CopyDst").WithID(objs[0].id)
		if objs[0].tenant != "" {
			del = del.WithTenant(objs[0].tenant)
		}
		require.NoError(t, del.Do(ctx))
		require.Error(t, st.verifyTenantCopy(ctx, "CopySrc", "CopyDst", objs[0].tenant))
	})
}

func TestCopyRefusesUncarriableTypes(t *testing.T) {
	st, _ := trashStore(t)
	ctx := context.Background()
	for _, n := range []string{"GeoSrc", "GeoDst"} {
		_ = st.client.Schema().ClassDeleter().WithClassName(n).Do(ctx)
		require.NoError(t, st.client.Schema().ClassCreator().WithClass(&models.Class{Class: n, Vectorizer: "none",
			Properties: []*models.Property{{Name: "g", DataType: []string{"geoCoordinates"}}}}).Do(ctx))
		n := n
		t.Cleanup(func() { _ = st.client.Schema().ClassDeleter().WithClassName(n).Do(ctx) })
	}
	_, err := st.CopyClassWithVectors(ctx, "GeoSrc", "GeoDst", nil)
	require.Error(t, err, "silently dropping a property type would be data loss")
}

// The headline behaviour: a plain, word-tokenized class becomes an alias-backed,
// field-tokenized one with the data, ids and vectors intact; then a second swap
// repoints the alias; re-running either is a no-op.
func TestSwapToVersion(t *testing.T) {
	st, _ := trashStore(t)
	ctx := context.Background()
	want := st.ClassDefinition(memory.ClassName)
	tagFilter := func(ts *TenantStore, tag string) []string {
		recs, err := ts.List(ctx, ListOpts{Namespace: "ns", AnyTags: []string{tag}, Limit: 100})
		require.NoError(t, err)
		return idsOfRecs(recs)
	}

	seeded := seedLegacyMemories(t, st)
	first := tenantsFor(st)[0]
	ts := st.Tenant(first)
	byTags := func(recs []memory.Record, tags ...string) []string {
		var out []string
		for _, r := range recs {
			if slices.ContainsFunc(r.Tags, func(x string) bool { return slices.Contains(tags, x) }) {
				out = append(out, r.ID)
			}
		}
		slices.Sort(out)
		return out
	}
	// Legacy behaviour (the bug): "foo" also matches "Foo" and "foo-bar".
	assert.Equal(t, byTags(seeded[first], "foo", "Foo", "foo-bar"), tagFilter(ts, "foo"))

	q := randVec(rand.New(rand.NewPCG(1, 2)), 8) // the seed vector of memory 0
	before, err := ts.SearchMemoryVectors(ctx, q, SearchOpts{Namespace: "ns", Limit: 10})
	require.NoError(t, err)
	require.NotEmpty(t, before)

	var entered, exited int
	hooks := SwapHooks{
		Enter: func(context.Context) error { entered++; return nil },
		Exit:  func(context.Context) { exited++ },
	}
	require.NoError(t, st.SwapToVersion(ctx, memory.ClassName, want, hooks))
	assert.Equal(t, 1, entered)
	assert.Equal(t, 1, exited, "writers are released after the cutover")

	assert.Equal(t, "Memory_v1", aliasTargetsOf(t, st)[memory.ClassName])
	assert.NotContains(t, classNamesOf(t, st), memory.ClassName, "the plain class is gone")

	t.Run("data path works through the alias with identical data", func(t *testing.T) {
		for _, tn := range tenantsFor(st) {
			recs, err := st.Tenant(tn).List(ctx, ListOpts{Namespace: "ns", Limit: 100})
			require.NoError(t, err)
			assert.Equal(t, idsOfRecs(seeded[tn]), idsOfRecs(recs), "tenant %q", tn)
			for _, got := range recs {
				for _, exp := range seeded[tn] {
					if exp.ID == got.ID {
						assert.ElementsMatch(t, exp.Tags, got.Tags, "tags unchanged")
						assert.Equal(t, exp.LinkedIDs, got.LinkedIDs)
						assert.Equal(t, exp.UseCount, got.UseCount)
						assert.WithinDuration(t, exp.LastUsedAt, got.LastUsedAt, time.Second)
					}
				}
			}
		}
		after, err := ts.SearchMemoryVectors(ctx, q, SearchOpts{Namespace: "ns", Limit: 10})
		require.NoError(t, err)
		require.Len(t, after, len(before))
		for i := range before {
			assert.Equal(t, before[i].ID, after[i].ID, "ranking unchanged")
			assert.InDelta(t, before[i].Distance, after[i].Distance, 1e-6, "vectors identical => same distances")
		}
		// Writes keep working through the alias.
		rec := memory.Record{ID: uuid.NewString(), Text: "post-swap", Namespace: "ns", Tags: []string{"new"}}
		require.NoError(t, ts.Upsert(ctx, rec, q))
		_, ok, err := ts.Get(ctx, rec.ID)
		require.NoError(t, err)
		assert.True(t, ok)
		require.NoError(t, ts.Delete(ctx, rec.ID))
	})

	t.Run("the tag filter is now exact", func(t *testing.T) {
		assert.Equal(t, byTags(seeded[first], "foo"), tagFilter(ts, "foo"), "only the exact, case-sensitive tag matches")
		assert.Equal(t, byTags(seeded[first], "Foo"), tagFilter(ts, "Foo"))
		assert.Equal(t, byTags(seeded[first], "foo-bar"), tagFilter(ts, "foo-bar"))
	})

	t.Run("re-running is a no-op", func(t *testing.T) {
		entered = 0
		require.NoError(t, st.SwapToVersion(ctx, memory.ClassName, want, hooks))
		assert.Equal(t, 0, entered, "an up-to-date class must not enter maintenance")
		assert.Equal(t, "Memory_v1", aliasTargetsOf(t, st)[memory.ClassName])
	})

	t.Run("second swap repoints the alias and drops the old class", func(t *testing.T) {
		want2 := st.ClassDefinition(memory.ClassName)
		want2.Properties = append(want2.Properties, &models.Property{Name: "extra", DataType: []string{"text"}})
		require.NoError(t, st.SwapToVersion(ctx, memory.ClassName, want2, hooks))
		assert.Equal(t, "Memory_v2", aliasTargetsOf(t, st)[memory.ClassName])
		names := classNamesOf(t, st)
		assert.Contains(t, names, "Memory_v2")
		assert.NotContains(t, names, "Memory_v1", "the old version must be dropped by its PHYSICAL name")
		for _, tn := range tenantsFor(st) {
			recs, err := st.Tenant(tn).List(ctx, ListOpts{Namespace: "ns", Limit: 100})
			require.NoError(t, err)
			assert.Equal(t, idsOfRecs(seeded[tn]), idsOfRecs(recs))
		}
		// EnsureSchema (which only knows the stock definition) still works on top.
		require.NoError(t, st.EnsureSchema(ctx))
	})

	t.Run("Exit runs when the copy fails", func(t *testing.T) {
		exited = 0
		bad := st.ClassDefinition(memory.ClassName)
		bad.Properties = append(bad.Properties, &models.Property{Name: "extra2", DataType: []string{"text"}})
		if st.multiTenant {
			bad.MultiTenancyConfig = nil // makes the copy refuse: tenancy differs
		} else {
			bad.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
		}
		require.Error(t, st.SwapToVersion(ctx, memory.ClassName, bad, hooks))
		assert.Equal(t, 1, exited, "a failed swap must never leave writers paused")
		assert.Equal(t, "Memory_v2", aliasTargetsOf(t, st)[memory.ClassName], "the live class is untouched")
		_ = st.client.Schema().ClassDeleter().WithClassName("Memory_v3").Do(ctx)
	})
}

// A crash can stop SwapToVersion between "delete the plain class" and "create the
// alias" (queries fail in that window). Re-running must finish the job, and must
// do it WITHOUT copying again (the plain class, the source, is already gone).
func TestSwapRecoversFromCrashAfterPlainClassDeleted(t *testing.T) {
	st, _ := trashStore(t)
	ctx := context.Background()
	want := st.ClassDefinition(memory.ClassName)
	seeded := seedLegacyMemories(t, st)

	// Reproduce the exact post-crash state by hand.
	next := want
	next.Class = "Memory_v1"
	require.NoError(t, st.client.Schema().ClassCreator().WithClass(next).Do(ctx))
	_, err := st.CopyClassWithVectors(ctx, memory.ClassName, "Memory_v1", nil)
	require.NoError(t, err)
	require.NoError(t, st.client.Schema().ClassDeleter().WithClassName(memory.ClassName).Do(ctx))
	require.NotContains(t, aliasTargetsOf(t, st), memory.ClassName)

	entered := false
	require.NoError(t, st.SwapToVersion(ctx, memory.ClassName, st.ClassDefinition(memory.ClassName),
		SwapHooks{Enter: func(context.Context) error { entered = true; return nil }}))
	assert.False(t, entered, "recovery only creates the alias: no copy, no maintenance")
	assert.Equal(t, "Memory_v1", aliasTargetsOf(t, st)[memory.ClassName])
	for _, tn := range tenantsFor(st) {
		recs, err := st.Tenant(tn).List(ctx, ListOpts{Namespace: "ns", Limit: 100})
		require.NoError(t, err)
		assert.Equal(t, idsOfRecs(seeded[tn]), idsOfRecs(recs))
	}
}

// A crash mid-copy leaves a partial target. Re-running must reuse it and complete.
func TestSwapResumesPartialCopy(t *testing.T) {
	st, _ := trashStore(t)
	ctx := context.Background()
	seeded := seedLegacyMemories(t, st)
	want := st.ClassDefinition(memory.ClassName)

	next := st.ClassDefinition(memory.ClassName)
	next.Class = "Memory_v1"
	require.NoError(t, st.client.Schema().ClassCreator().WithClass(next).Do(ctx))
	_, err := st.CopyClassWithVectors(ctx, memory.ClassName, "Memory_v1", nil)
	require.NoError(t, err)
	// Lose one object from the partial copy.
	tn := tenantsFor(st)[0]
	victim := seeded[tn][0].ID
	del := st.client.Data().Deleter().WithClassName("Memory_v1").WithID(victim)
	if tn != "" {
		del = del.WithTenant(tn)
	}
	require.NoError(t, del.Do(ctx))

	require.NoError(t, st.SwapToVersion(ctx, memory.ClassName, want, SwapHooks{}))
	recs, err := st.Tenant(tn).List(ctx, ListOpts{Namespace: "ns", Limit: 100})
	require.NoError(t, err)
	assert.Equal(t, idsOfRecs(seeded[tn]), idsOfRecs(recs), "the lost object is copied again")
}

func toUUID(s string) strfmt.UUID { return strfmt.UUID(s) }

// A crash mid-copy leaves a partial target. If the source then loses an object, a
// reused target would keep it as an extra, verification would fail, and boot would
// fail on every restart. The leftover must be rebuilt from scratch.
func TestSwapRebuildsLeftoverTargetSoDeletedObjectsDoNotWedgeBoot(t *testing.T) {
	st, _ := trashStore(t)
	ctx := context.Background()
	seeded := seedLegacyMemories(t, st)
	want := st.ClassDefinition(memory.ClassName)

	next := st.ClassDefinition(memory.ClassName)
	next.Class = "Memory_v1"
	require.NoError(t, st.client.Schema().ClassCreator().WithClass(next).Do(ctx))
	_, err := st.CopyClassWithVectors(ctx, memory.ClassName, "Memory_v1", nil) // the "first attempt"
	require.NoError(t, err)

	// The user deletes a memory from the (still live) source before the retry.
	tn := tenantsFor(st)[0]
	victim := seeded[tn][0].ID
	del := st.client.Data().Deleter().WithClassName(memory.ClassName).WithID(victim)
	if tn != "" {
		del = del.WithTenant(tn)
	}
	require.NoError(t, del.Do(ctx))

	require.NoError(t, st.SwapToVersion(ctx, memory.ClassName, want, SwapHooks{}), "resume must not fail verification")
	recs, err := st.Tenant(tn).List(ctx, ListOpts{Namespace: "ns", Limit: 100})
	require.NoError(t, err)
	assert.NotContains(t, idsOfRecs(recs), victim, "the deleted object must be absent from the new class")
	assert.Len(t, recs, len(seeded[tn])-1)
}

// A plain class that already has the wanted shape is left alone: no copy, no write
// pause (PhysicalClass handles plain classes).
func TestSwapSkipsPlainClassThatAlreadySatisfies(t *testing.T) {
	st, _ := trashStore(t)
	ctx := context.Background()
	require.NoError(t, st.DeleteAllClasses(ctx))
	require.NoError(t, st.client.Schema().ClassCreator().WithClass(st.ClassDefinition(memory.ClassName)).Do(ctx))
	entered := false
	require.NoError(t, st.SwapToVersion(ctx, memory.ClassName, st.ClassDefinition(memory.ClassName),
		SwapHooks{Enter: func(context.Context) error { entered = true; return nil }}))
	assert.False(t, entered)
	assert.NotContains(t, aliasTargetsOf(t, st), memory.ClassName)
}

// An Enter that cannot stop writers must abort the swap before anything is copied,
// and Exit must still run so the flag it set is cleared.
func TestSwapAbortsWhenEnterFails(t *testing.T) {
	st, _ := trashStore(t)
	ctx := context.Background()
	seedLegacyMemories(t, st)
	exited := 0
	err := st.SwapToVersion(ctx, memory.ClassName, st.ClassDefinition(memory.ClassName), SwapHooks{
		Enter: func(context.Context) error { return assert.AnError },
		Exit:  func(context.Context) { exited++ },
	})
	require.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, 1, exited)
	assert.NotContains(t, aliasTargetsOf(t, st), memory.ClassName, "the live class is untouched")
}

// An inactive tenant cannot be read; the error must name it and say what to do.
func TestCopyNamesInactiveTenant(t *testing.T) {
	st, _ := trashStore(t)
	if !st.multiTenant {
		t.Skip("tenants only exist in multi-tenant mode")
	}
	ctx := context.Background()
	seedLegacyMemories(t, st)
	require.NoError(t, st.client.Schema().TenantsUpdater().WithClassName(memory.ClassName).
		WithTenants(models.Tenant{Name: "swap-b", ActivityStatus: "COLD"}).Do(ctx))
	err := st.SwapToVersion(ctx, memory.ClassName, st.ClassDefinition(memory.ClassName), SwapHooks{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "swap-b")
	assert.Contains(t, err.Error(), "activate")
	assert.NotContains(t, aliasTargetsOf(t, st), memory.ClassName, "the live class keeps serving")
}
