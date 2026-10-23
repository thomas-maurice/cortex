package migrate

import (
	"context"
	"log/slog"
	"time"

	"github.com/weaviate/weaviate/entities/models"

	"github.com/thomas-maurice/cortex/internal/store"
)

// Env is what a Migration's Up gets: the store, a logger, and the reusable
// helpers. Prefer these over raw Weaviate calls: they encode the alias rules.
type Env struct {
	Store *store.Store
	Log   *slog.Logger
	Maint *Maintenance
	// DrainTimeout bounds the wait for workers to go idle after the maintenance
	// flag is set (0 = DefaultDrainTimeout, negative = do not wait). On timeout the
	// migration aborts without copying.
	DrainTimeout time.Duration
}

// PhysicalClass resolves an alias to its physical class; a plain class is
// returned unchanged.
func (e *Env) PhysicalClass(ctx context.Context, name string) (string, error) {
	return e.Store.PhysicalClass(ctx, name)
}

// CopyClassWithVectors copies every object, with its vector, from -> to (a
// physical class), per tenant, verifying the result. See store.CopyClassWithVectors.
func (e *Env) CopyClassWithVectors(ctx context.Context, from, to string, transform func(map[string]any) map[string]any) (map[string]int, error) {
	return e.Store.CopyClassWithVectors(ctx, from, to, transform)
}

// SwapToVersion rebuilds the logical class `alias` with newSchema and cuts over,
// stopping writers (maintenance mode) for the copy + cutover only. Idempotent and
// resumable; see store.SwapToVersion.
func (e *Env) SwapToVersion(ctx context.Context, alias string, newSchema *models.Class) error {
	return e.Store.SwapToVersion(ctx, alias, newSchema, store.SwapHooks{
		Log: e.Log,
		Enter: func(ctx context.Context) error {
			if err := e.Maint.Enable(ctx, "schema migration: copying "+alias); err != nil {
				return err
			}
			// Writers mid-flight must land BEFORE the copy: wait for every live worker
			// to report held with nothing in flight. Never copy on a timeout.
			if e.DrainTimeout >= 0 {
				// Server-side readers of the flag are cached/polled: let them see it.
				if err := e.Maint.Settle(ctx); err != nil {
					return err
				}
			}
			return e.Maint.WaitWorkersIdle(ctx, e.DrainTimeout)
		},
		Exit: func(ctx context.Context) { e.Maint.Disable(ctx) },
	})
}
