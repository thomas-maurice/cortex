package store

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sort"

	"github.com/weaviate/weaviate-go-client/v5/weaviate/alias"
	"github.com/weaviate/weaviate/entities/models"
)

// SwapHooks lets the caller (internal/migrate) bracket the write-sensitive window
// of SwapToVersion. Enter is called before the data copy starts (it must stop
// writers: set maintenance mode, let in-flight writes drain); Exit after the
// cutover, and also on any failure once Enter was called, including Enter itself
// failing (so a flag it set is cleared); it must be idempotent. Enter MUST return an
// error rather than let the copy proceed when writers cannot be stopped. All fields
// are optional.
type SwapHooks struct {
	Enter func(ctx context.Context) error
	Exit  func(ctx context.Context)
	Log   *slog.Logger
}

// SwapToVersion moves the logical class `logical` onto a new physical class
// built from `want` (want.Class is ignored; the new name is derived), without
// re-embedding: <Logical>_v<N+1> is created, every object is copied with its
// vector (CopyClassWithVectors), the copy is verified, the alias is cut over to
// it, and the old physical class is dropped.
//
//   - First time (logical is still a plain, pre-alias class): the plain class must
//     be deleted before an alias of that name can exist, so there is a ~20ms
//     window with neither (queries fail); writers are stopped by the hooks.
//   - Later (logical is an alias): the cutover is one atomic AliasUpdater call.
//
// It is idempotent and resumable at every step: if the current physical class
// already satisfies `want` it only cleans up leftovers; a half-copied target is
// reused (the copy is idempotent by id); and a crash between "delete the plain
// class" and "create the alias" is completed by pointing the alias at the highest
// <Logical>_v<N> (it was verified before the plain class was deleted).
func (s *Store) SwapToVersion(ctx context.Context, logical string, want *models.Class, h SwapHooks) (err error) {
	log := h.Log
	if log == nil {
		log = slog.Default()
	}
	log = log.With("class", logical)

	targets, err := s.aliasTargets(ctx)
	if err != nil {
		return err
	}
	names, err := s.allClassNames(ctx)
	if err != nil {
		return err
	}
	aliasTarget, hasAlias := targets[logical]
	plain := slices.Contains(names, logical)

	switch {
	case hasAlias:
		cur, err := s.client.Schema().ClassGetter().WithClassName(aliasTarget).Do(ctx)
		if err != nil {
			return fmt.Errorf("get class %s: %w", aliasTarget, err)
		}
		if schemaSatisfies(cur, want) {
			return s.dropStaleVersions(ctx, logical, aliasTarget, log)
		}
		curN, ok := versionOf(logical, aliasTarget)
		if !ok {
			return fmt.Errorf("alias %s points at %s, which is not a %s_v<N> class", logical, aliasTarget, logical)
		}
		next := physicalName(logical, curN+1)
		return s.swap(ctx, logical, cur, next, want, h, log, func() error {
			return s.client.Alias().AliasUpdater().WithAlias(&alias.Alias{Alias: logical, Class: next}).Do(ctx)
		})

	case plain:
		cur, err := s.client.Schema().ClassGetter().WithClassName(logical).Do(ctx)
		if err != nil {
			return fmt.Errorf("get class %s: %w", logical, err)
		}
		if schemaSatisfies(cur, want) {
			// Nothing to change and PhysicalClass handles plain classes, so skip the
			// copy (and the write pause) rather than rebuild only to rename.
			log.Info("plain class already has the wanted schema; leaving it as is")
			return nil
		}
		next := physicalName(logical, 1)
		return s.swap(ctx, logical, cur, next, want, h, log, func() error {
			// An alias cannot share a name with a class: drop the plain class first.
			if err := s.client.Schema().ClassDeleter().WithClassName(logical).Do(ctx); err != nil {
				return fmt.Errorf("delete plain class %s: %w", logical, err)
			}
			return s.createAlias(ctx, logical, next)
		})
	}

	// Neither an alias nor a plain class: a crash after the plain class was deleted.
	best, bestN := "", 0
	for _, n := range names {
		if v, ok := versionOf(logical, n); ok && v > bestN {
			best, bestN = n, v
		}
	}
	if best == "" {
		return fmt.Errorf("class %s does not exist (EnsureSchema must run first)", logical)
	}
	cur, err := s.client.Schema().ClassGetter().WithClassName(best).Do(ctx)
	if err != nil {
		return fmt.Errorf("get class %s: %w", best, err)
	}
	if !schemaSatisfies(cur, want) {
		return fmt.Errorf("recovering %s: %s exists without an alias but does not have the wanted schema; manual attention needed", logical, best)
	}
	log.Warn("recovering an interrupted cutover: creating the missing alias", "target", best)
	if err := s.createAlias(ctx, logical, best); err != nil {
		return err
	}
	return s.dropStaleVersions(ctx, logical, best, log)
}

// swap is the create -> copy -> verify -> cutover -> drop sequence shared by the
// first-time and later cases; cutover is the case-specific alias switch.
func (s *Store) swap(ctx context.Context, logical string, cur *models.Class, next string, want *models.Class, h SwapHooks, log *slog.Logger, cutover func() error) (err error) {
	// A leftover target from an interrupted attempt is ALWAYS dropped and rebuilt:
	// `cur` still exists and is the source of truth, and a reused target would keep
	// objects deleted from the source since (verification would then fail forever).
	// Copying is cheap (~8k objects/s). The only case where the target is the sole
	// copy (the plain class already deleted) is handled in SwapToVersion and never
	// reaches here.
	exists, err := s.client.Schema().ClassExistenceChecker().WithClassName(next).Do(ctx)
	if err != nil {
		return fmt.Errorf("check class %s: %w", next, err)
	}
	if exists {
		log.Warn("dropping a leftover target class from an interrupted attempt", "target", next)
		if err := s.client.Schema().ClassDeleter().WithClassName(next).Do(ctx); err != nil {
			return fmt.Errorf("delete class %s: %w", next, err)
		}
	}
	nc := *want
	nc.Class = next
	// Keep the vector index settings (distance metric!) of the class being
	// replaced, so search results do not change.
	nc.VectorIndexType = cur.VectorIndexType
	nc.VectorIndexConfig = cur.VectorIndexConfig
	if err := s.client.Schema().ClassCreator().WithClass(&nc).Do(ctx); err != nil {
		return fmt.Errorf("create class %s: %w", next, err)
	}

	entered := h.Enter != nil
	exit := func() {
		if entered && h.Exit != nil {
			h.Exit(context.WithoutCancel(ctx))
		}
		entered = false
	}
	defer exit()
	if h.Enter != nil {
		if err := h.Enter(ctx); err != nil {
			return fmt.Errorf("enter maintenance (nothing was copied, the live class is untouched): %w", err)
		}
	}

	log.Info("copying data", "from", cur.Class, "to", next)
	counts, err := s.CopyClassWithVectors(ctx, logical, next, nil)
	if err != nil {
		return err
	}
	total := 0
	for _, n := range counts {
		total += n
	}
	log.Info("copy verified", "to", next, "tenants", len(counts), "objects", total)

	// A tenant created after the copy listed them would be lost at the cutover.
	// Writers are stopped, so this should not happen; check anyway, just before it.
	if isMT := cur.MultiTenancyConfig != nil && cur.MultiTenancyConfig.Enabled; isMT {
		tl, err := s.client.Schema().TenantsGetter().WithClassName(logical).Do(ctx)
		if err != nil {
			return fmt.Errorf("re-list tenants of %s before cutover: %w", logical, err)
		}
		for _, t := range tl {
			if _, copied := counts[t.Name]; !copied {
				return fmt.Errorf("tenant %q appeared in %s during the copy; aborting before cutover (the live class is untouched, a restart retries)", t.Name, logical)
			}
		}
		if len(tl) != len(counts) {
			return fmt.Errorf("tenant set of %s changed during the copy (%d listed, %d copied); aborting before cutover", logical, len(tl), len(counts))
		}
	}

	if err := cutover(); err != nil {
		return fmt.Errorf("cutover %s -> %s: %w", logical, next, err)
	}
	exit()

	return s.dropStaleVersions(ctx, logical, next, log)
}

// dropStaleVersions deletes every <logical>_v<N> physical class except keep. The old
// class MUST be deleted by its physical name (ClassDeleter on an alias is a silent
// no-op). Safe to call only while the alias points at keep.
func (s *Store) dropStaleVersions(ctx context.Context, logical, keep string, log *slog.Logger) error {
	targets, err := s.aliasTargets(ctx)
	if err != nil {
		return err
	}
	if targets[logical] != keep {
		return fmt.Errorf("refusing to drop old versions of %s: alias points at %q, expected %q", logical, targets[logical], keep)
	}
	names, err := s.allClassNames(ctx)
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, n := range names {
		if _, ok := versionOf(logical, n); !ok || n == keep {
			continue
		}
		log.Info("dropping old physical class", "class", n)
		if err := s.client.Schema().ClassDeleter().WithClassName(n).Do(ctx); err != nil {
			return fmt.Errorf("delete old class %s: %w", n, err)
		}
	}
	return nil
}

// effectiveTokenization is a text property's tokenization with Weaviate's default
// ("word") filled in, so "" and "word" compare equal.
func effectiveTokenization(p *models.Property) string {
	if p.Tokenization != "" {
		return p.Tokenization
	}
	if len(p.DataType) == 1 && (p.DataType[0] == "text" || p.DataType[0] == "text[]") {
		return "word"
	}
	return ""
}

// schemaSatisfies reports whether cur already has the shape want asks for: same
// multi-tenancy, and every wanted property present with the same data type and
// tokenization. Extra properties on cur are ignored.
func schemaSatisfies(cur, want *models.Class) bool {
	curMT := cur.MultiTenancyConfig != nil && cur.MultiTenancyConfig.Enabled
	wantMT := want.MultiTenancyConfig != nil && want.MultiTenancyConfig.Enabled
	if curMT != wantMT {
		return false
	}
	have := make(map[string]*models.Property, len(cur.Properties))
	for _, p := range cur.Properties {
		have[p.Name] = p
	}
	for _, w := range want.Properties {
		p, ok := have[w.Name]
		if !ok || !slices.Equal(p.DataType, w.DataType) || effectiveTokenization(p) != effectiveTokenization(w) {
			return false
		}
	}
	return true
}
