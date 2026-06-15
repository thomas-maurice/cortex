package rpc

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// fakeGetter resolves neighbour ids from an in-memory map. A missing id reports
// found=false; an id listed in fails reports an error.
func fakeGetter(recs map[string]memory.Record, fails map[string]bool) func(string) (memory.Record, bool, error) {
	return func(id string) (memory.Record, bool, error) {
		if fails[id] {
			return memory.Record{}, false, errors.New("boom")
		}
		r, ok := recs[id]
		return r, ok, nil
	}
}

func hit(id string, dups, links []string) memory.Hit {
	return memory.Hit{Record: memory.Record{ID: id, DupCandidates: dups, LinkedIDs: links}}
}

// assembleCluster decides exactly which memories a Consolidate call hands the LLM
// to merge, and the merge's supersede manifest is derived from it — so its
// contract is load-bearing for both completeness (don't miss related memories)
// and safety (don't surface an id the merge can then delete without having seen
// its content). Each subtest pins one guarantee.
func TestAssembleCluster(t *testing.T) {
	t.Run("seeds come first, in order, before any neighbour", func(t *testing.T) {
		seeds := []memory.Hit{hit("s1", []string{"n1"}, nil), hit("s2", nil, nil)}
		recs := map[string]memory.Record{"n1": {ID: "n1"}}
		cluster := assembleCluster(seeds, 10, fakeGetter(recs, nil))
		ids := clusterIDs(cluster)
		assert.Equal(t, []string{"s1", "s2", "n1"}, ids, "seeds precede expanded neighbours")
	})

	t.Run("expands duplicate candidates AND links of seeds", func(t *testing.T) {
		seeds := []memory.Hit{hit("s1", []string{"dup"}, []string{"lnk"})}
		recs := map[string]memory.Record{"dup": {ID: "dup"}, "lnk": {ID: "lnk"}}
		cluster := assembleCluster(seeds, 10, fakeGetter(recs, nil))
		assert.ElementsMatch(t, []string{"s1", "dup", "lnk"}, clusterIDs(cluster))
	})

	t.Run("duplicate candidates expand before links", func(t *testing.T) {
		seeds := []memory.Hit{hit("s1", []string{"dup"}, []string{"lnk"})}
		recs := map[string]memory.Record{"dup": {ID: "dup"}, "lnk": {ID: "lnk"}}
		cluster := assembleCluster(seeds, 10, fakeGetter(recs, nil))
		assert.Equal(t, []string{"s1", "dup", "lnk"}, clusterIDs(cluster), "dup candidates are the point of consolidation, surface them first")
	})

	t.Run("a neighbour that is already a seed is not duplicated", func(t *testing.T) {
		seeds := []memory.Hit{hit("s1", []string{"s2"}, nil), hit("s2", nil, nil)}
		cluster := assembleCluster(seeds, 10, fakeGetter(nil, nil))
		assert.Equal(t, []string{"s1", "s2"}, clusterIDs(cluster))
	})

	t.Run("a neighbour referenced by two seeds appears once", func(t *testing.T) {
		seeds := []memory.Hit{hit("s1", []string{"n"}, nil), hit("s2", []string{"n"}, nil)}
		recs := map[string]memory.Record{"n": {ID: "n"}}
		cluster := assembleCluster(seeds, 10, fakeGetter(recs, nil))
		assert.Equal(t, []string{"s1", "s2", "n"}, clusterIDs(cluster))
	})

	t.Run("a vanished neighbour (deleted since flagging) is skipped, not surfaced", func(t *testing.T) {
		seeds := []memory.Hit{hit("s1", []string{"ghost"}, nil)}
		cluster := assembleCluster(seeds, 10, fakeGetter(nil, nil)) // empty map => ghost not found
		assert.Equal(t, []string{"s1"}, clusterIDs(cluster))
	})

	t.Run("a neighbour whose lookup errors is skipped, never fatal", func(t *testing.T) {
		seeds := []memory.Hit{hit("s1", []string{"boom"}, nil)}
		cluster := assembleCluster(seeds, 10, fakeGetter(nil, map[string]bool{"boom": true}))
		assert.Equal(t, []string{"s1"}, clusterIDs(cluster))
	})

	t.Run("limit caps the total cluster size", func(t *testing.T) {
		seeds := []memory.Hit{hit("s1", []string{"n1", "n2", "n3"}, nil)}
		recs := map[string]memory.Record{"n1": {ID: "n1"}, "n2": {ID: "n2"}, "n3": {ID: "n3"}}
		cluster := assembleCluster(seeds, 2, fakeGetter(recs, nil))
		require.Len(t, cluster, 2)
		assert.Equal(t, []string{"s1", "n1"}, clusterIDs(cluster), "seed plus one neighbour, then the cap stops expansion")
	})

	t.Run("limit smaller than the seed count truncates seeds", func(t *testing.T) {
		seeds := []memory.Hit{hit("s1", nil, nil), hit("s2", nil, nil), hit("s3", nil, nil)}
		cluster := assembleCluster(seeds, 2, fakeGetter(nil, nil))
		assert.Equal(t, []string{"s1", "s2"}, clusterIDs(cluster))
	})

	t.Run("zero limit yields nothing", func(t *testing.T) {
		seeds := []memory.Hit{hit("s1", nil, nil)}
		assert.Empty(t, assembleCluster(seeds, 0, fakeGetter(nil, nil)))
	})
}

// tagFilter re-applies the store's tag scope to neighbours fetched by id during
// expansion. The behaviour that must be unambiguous is the EMPTY case: no tags
// means "do not filter" (admit everything), NOT "only untagged memories" — a
// regression there would silently shrink every unscoped consolidation. The
// include/any/exclude semantics must also match the store so a tag-scoped
// consolidation can't pull an out-of-scope memory into the supersede set.
func TestTagFilter(t *testing.T) {
	t.Run("no tags admits everything, including untagged and tagged", func(t *testing.T) {
		keep := tagFilter(nil, nil, nil)
		assert.True(t, keep(nil), "untagged memory is admitted when no filter is set")
		assert.True(t, keep([]string{"anything"}), "tagged memory is admitted when no filter is set")
	})

	t.Run("include requires ALL listed tags", func(t *testing.T) {
		keep := tagFilter([]string{"a", "b"}, nil, nil)
		assert.True(t, keep([]string{"a", "b", "c"}))
		assert.False(t, keep([]string{"a"}), "missing one required tag excludes it")
		assert.False(t, keep(nil), "untagged is excluded once a required tag is set")
	})

	t.Run("anyOf requires at least one listed tag", func(t *testing.T) {
		keep := tagFilter(nil, []string{"a", "b"}, nil)
		assert.True(t, keep([]string{"b"}))
		assert.False(t, keep([]string{"c"}))
	})

	t.Run("exclude drops a memory carrying any listed tag", func(t *testing.T) {
		keep := tagFilter(nil, nil, []string{"secret"})
		assert.False(t, keep([]string{"ok", "secret"}))
		assert.True(t, keep([]string{"ok"}))
	})

	t.Run("filters combine (include AND not-exclude)", func(t *testing.T) {
		keep := tagFilter([]string{"proj"}, nil, []string{"archived"})
		assert.True(t, keep([]string{"proj"}))
		assert.False(t, keep([]string{"proj", "archived"}), "exclude wins over include")
	})
}

func clusterIDs(recs []memory.Record) []string {
	ids := make([]string, 0, len(recs))
	for _, r := range recs {
		ids = append(ids, r.ID)
	}
	return ids
}
