package migrate

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// tagsFieldTokenization makes `tags` use "field" tokenization on every class that
// carries it. With the default "word" tokenization a tag filter was fuzzy:
// ContainsAny ["foo"] also matched "Foo" and "foo-bar". Tokenization is immutable
// on an existing property, so each class is rebuilt (Env.SwapToVersion). Tag values
// are untouched and nothing is re-embedded.
var tagsFieldTokenization = Migration{
	ID:   1,
	Name: "tags-field-tokenization",
	Description: "Rebuild Memory, MemoryChunk and Trash so the tags filter matches exact, " +
		"case-sensitive tags (no re-embedding); classes become alias-backed (<Name>_v<N>)",
	Up: func(ctx context.Context, env *Env) error {
		for _, name := range []string{memory.ClassName, memory.ChunkClassName, memory.TrashClassName} {
			want := env.Store.ClassDefinition(name)
			if want == nil {
				return fmt.Errorf("no definition for class %s", name)
			}
			if skip, err := mtMismatch(ctx, env, name, want); err != nil {
				return err
			} else if skip {
				continue
			}
			if err := env.SwapToVersion(ctx, name, want); err != nil {
				return fmt.Errorf("class %s: %w", name, err)
			}
		}
		return nil
	},
}

// mtMismatch reports whether the live class's tenancy differs from what this
// server is configured for. That state is already flagged by VerifySchema and is
// only fixed by `cortex migrate-mt`, which drops and recreates the classes with
// the current definitions (so they come out right anyway); failing here would
// refuse to boot and make migrate-mt unreachable. A missing class is an error.
func mtMismatch(ctx context.Context, env *Env, name string, want *models.Class) (bool, error) {
	exists, err := env.Store.ClassExists(ctx, name)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, fmt.Errorf("class %s does not exist (EnsureSchema should have created it)", name)
	}
	isMT, err := env.Store.IsClassMultiTenant(ctx, name)
	if err != nil {
		return false, err
	}
	wantMT := want.MultiTenancyConfig != nil && want.MultiTenancyConfig.Enabled
	if isMT != wantMT {
		env.Log.Warn("class tenancy differs from CORTEX_MULTI_TENANT; skipping its rebuild (run `cortex migrate-mt`, which recreates it with the current schema)",
			"class", name, "class_multi_tenant", isMT, "configured_multi_tenant", wantMT)
		return true, nil
	}
	return false, nil
}
