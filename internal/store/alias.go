package store

// Alias model. Every logical class (Memory, MemoryChunk, ConversationSummary,
// Trash) is an ALIAS pointing at a versioned PHYSICAL class "<Name>_v<N>". That is
// what lets a schema change that Weaviate forbids in place (e.g. a property's
// tokenization) be applied by building <Name>_v<N+1>, copying the data, and
// atomically repointing the alias (see SwapToVersion and internal/migrate).
//
// Rules for code in this package (they are easy to get wrong, the failures are
// silent or confusing):
//
//   - DATA path (search, create/get/PATCH/PUT/delete, batch, filters, cursor
//     scans, tenant-scoped everything) uses the LOGICAL name; Weaviate resolves it.
//   - Schema READS (ClassGetter, ClassExistenceChecker, TenantsGetter,
//     TenantsExists) also work through the alias, but the result reports the
//     PHYSICAL name in .Class: never compare a returned class name to a constant.
//   - Schema/tenant WRITES (PropertyCreator, ClassCreator, TenantsCreator/
//     Updater/Deleter, ClassDeleter) need the PHYSICAL name. Through an alias
//     the first fails with "class not found" and ClassDeleter silently deletes
//     NOTHING. Always go through PhysicalClass first.
//   - A plain (legacy, pre-alias) class and an alias cannot share a name; an
//     install created before aliases keeps plain classes until migration #1
//     converts them. PhysicalClass returns the name unchanged for those.

import (
	"context"
	"fmt"
	"regexp"
	"strconv"

	"github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/alias"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// Client exposes the underlying Weaviate client to the migration framework
// (internal/migrate), which needs raw schema access. Normal code must go
// through Store methods.
func (s *Store) Client() *weaviate.Client { return s.client }

// MultiTenant reports whether the store was configured for multi-tenancy.
func (s *Store) MultiTenant() bool { return s.multiTenant }

// MemoryClasses lists the logical names of the four memory classes, in the order
// EnsureSchema creates them.
func MemoryClasses() []string {
	return []string{memory.ClassName, memory.ChunkClassName, memory.SummaryClassName, memory.TrashClassName}
}

// ClassDefinition returns the authoritative current definition of one of the
// memory classes (Class = logical name), or nil for an unknown name. Migrations
// derive the schema of the next physical version from it.
func (s *Store) ClassDefinition(logical string) *models.Class {
	switch logical {
	case memory.ClassName:
		return memoryClass(s.multiTenant)
	case memory.ChunkClassName:
		return chunkClass(s.multiTenant)
	case memory.SummaryClassName:
		return summaryClass(s.multiTenant)
	case memory.TrashClassName:
		return trashClass(s.multiTenant)
	}
	return nil
}

// physicalName is the name of version n of a logical class.
func physicalName(logical string, n int) string { return fmt.Sprintf("%s_v%d", logical, n) }

// versionOf parses a physical class name of the form <logical>_v<N>.
func versionOf(logical, class string) (int, bool) {
	re := regexp.MustCompile("^" + regexp.QuoteMeta(logical) + `_v(\d+)$`)
	m := re.FindStringSubmatch(class)
	if m == nil {
		return 0, false
	}
	n, err := strconv.Atoi(m[1])
	return n, err == nil
}

// aliasTargets returns alias name -> physical class for every alias.
func (s *Store) aliasTargets(ctx context.Context) (map[string]string, error) {
	list, err := s.client.Alias().Getter().Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("list aliases: %w", err)
	}
	out := make(map[string]string, len(list))
	for _, a := range list {
		out[a.Alias] = a.Class
	}
	return out, nil
}

// allClassNames returns every PHYSICAL class name in the schema.
func (s *Store) allClassNames(ctx context.Context) ([]string, error) {
	sch, err := s.client.Schema().Getter().Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("get schema: %w", err)
	}
	names := make([]string, 0, len(sch.Classes))
	for _, c := range sch.Classes {
		names = append(names, c.Class)
	}
	return names, nil
}

// PhysicalClass resolves logical to the physical class backing it: the alias
// target when logical is an alias, else logical itself (a plain class, or absent).
// Use it before any schema/tenant WRITE.
func (s *Store) PhysicalClass(ctx context.Context, logical string) (string, error) {
	targets, err := s.aliasTargets(ctx)
	if err != nil {
		return "", err
	}
	if p, ok := targets[logical]; ok {
		return p, nil
	}
	return logical, nil
}

// createAliased creates a fresh install's class as <Name>_v1 plus the alias
// <Name>. If another process (a concurrent boot) wins the race, the loser
// re-checks and treats an existing result as success.
func (s *Store) createAliased(ctx context.Context, class *models.Class) error {
	logical := class.Class
	phys := *class
	phys.Class = physicalName(logical, 1)
	if err := s.client.Schema().ClassCreator().WithClass(&phys).Do(ctx); err != nil {
		if ok, cerr := s.client.Schema().ClassExistenceChecker().WithClassName(phys.Class).Do(ctx); cerr != nil || !ok {
			return fmt.Errorf("create class %s: %w", phys.Class, err)
		}
	}
	if err := s.client.Alias().AliasCreator().WithAlias(&alias.Alias{Alias: logical, Class: phys.Class}).Do(ctx); err != nil {
		targets, terr := s.aliasTargets(ctx)
		if terr != nil || targets[logical] == "" {
			return fmt.Errorf("create alias %s -> %s: %w", logical, phys.Class, err)
		}
	}
	return nil
}

// dropLogical removes everything backing one logical class: the alias, a plain
// class of that name, and every versioned physical class. Absent parts are fine.
func (s *Store) dropLogical(ctx context.Context, logical string) error {
	targets, err := s.aliasTargets(ctx)
	if err != nil {
		return err
	}
	if _, ok := targets[logical]; ok {
		if err := s.client.Alias().AliasDeleter().WithAliasName(logical).Do(ctx); err != nil {
			return fmt.Errorf("delete alias %s: %w", logical, err)
		}
	}
	names, err := s.allClassNames(ctx)
	if err != nil {
		return err
	}
	for _, n := range names {
		if _, isVersion := versionOf(logical, n); !isVersion && n != logical {
			continue
		}
		if err := s.client.Schema().ClassDeleter().WithClassName(n).Do(ctx); err != nil {
			return fmt.Errorf("delete class %s: %w", n, err)
		}
	}
	return nil
}

// createAlias points a new alias at an existing physical class.
func (s *Store) createAlias(ctx context.Context, logical, physical string) error {
	if err := s.client.Alias().AliasCreator().WithAlias(&alias.Alias{Alias: logical, Class: physical}).Do(ctx); err != nil {
		return fmt.Errorf("create alias %s -> %s: %w", logical, physical, err)
	}
	return nil
}
