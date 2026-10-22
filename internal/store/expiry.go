package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// sweepBatch is how many expired memories one sweep round pulls per tenant.
// A var so tests can shrink it. Each round's survivors are trashed (and so leave
// the result set), so the loop drains an arbitrarily large backlog without a cursor.
var sweepBatch = 500

// listExpiredBefore returns up to limit memories whose validUntil is strictly
// before cutoff. A plain `<` filter excludes memories with no validUntil, so
// never-expiring memories can never be selected.
func (ts *TenantStore) listExpiredBefore(ctx context.Context, cutoff time.Time, limit int) ([]memory.Record, error) {
	query := ts.s.client.Experimental().Search().
		WithCollection(memory.ClassName).
		WithProperties(memoryProps...).
		WithMetadata(&graphql.Metadata{ID: true}).
		WithWhere(filters.Where().
			WithPath([]string{"validUntil"}).
			WithOperator(filters.LessThan).
			WithValueDate(cutoff)).
		WithSort(graphql.Sort{Path: []string{"validUntil"}, Order: graphql.Asc}).
		WithLimit(limit)
	if ts.t != "" {
		query = query.WithTenant(ts.t)
	}
	res, err := query.Do(ctx)
	if err != nil {
		if isTenantNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list expired: %w", err)
	}
	recs := make([]memory.Record, 0, len(res))
	for _, r := range res {
		recs = append(recs, resultToRecord(r))
	}
	return recs, nil
}

// TrashExpiredBefore moves every memory of this tenant whose validUntil is before
// cutoff to Trash with reason "expired" (via DeleteWithReason: trash-first, chunks
// cascaded) and returns how many were moved. Each candidate is re-read right
// before it is trashed and skipped if its expiry was extended in the meantime, so
// a user who rescues a memory while a sweep is running does not lose it (the
// remaining window is a single read-to-delete gap, and the trash makes even that
// recoverable).
func (ts *TenantStore) TrashExpiredBefore(ctx context.Context, cutoff time.Time) (int, error) {
	moved := 0
	// skip holds ids that failed (or were extended) this run. They stay in the
	// result set, so each round fetches sweepBatch PLUS len(skip) rows and ignores
	// the skipped ones: persistent failures can never fill the window and starve
	// the healthy memories behind them.
	skip := map[string]bool{}
	var errs []error
	for {
		limit := sweepBatch + len(skip)
		batch, err := ts.listExpiredBefore(ctx, cutoff, limit)
		if err != nil {
			return moved, errors.Join(append(errs, err)...)
		}
		fresh := 0
		for _, rec := range batch {
			if skip[rec.ID] {
				continue
			}
			fresh++
			cur, found, err := ts.Get(ctx, rec.ID)
			if err != nil {
				errs = append(errs, fmt.Errorf("re-read %s: %w", rec.ID, err))
				skip[rec.ID] = true
				continue
			}
			if !found {
				continue // deleted by someone else: it has left the result set
			}
			if cur.ValidUntil.IsZero() || !cur.ValidUntil.Before(cutoff) {
				slog.Info("expiry sweep: skipping memory whose expiry was extended", "id", rec.ID, "tenant", ts.t)
				skip[rec.ID] = true
				continue
			}
			if err := sweepTrash(ctx, ts, rec.ID); err != nil {
				errs = append(errs, fmt.Errorf("trash %s: %w", rec.ID, err))
				skip[rec.ID] = true
				continue
			}
			moved++
		}
		// A short window means we saw every match; a window with nothing new means
		// only skipped rows remain. Otherwise each round trashes something or grows
		// skip, so the loop terminates.
		if fresh == 0 || len(batch) < limit {
			return moved, errors.Join(errs...)
		}
	}
}

// sweepTrash moves one expired memory to Trash. A var so tests can inject failures.
var sweepTrash = func(ctx context.Context, ts *TenantStore, id string) error {
	return ts.DeleteWithReason(ctx, id, memory.TrashReasonExpired)
}

// SweepExpired is the expiry sweep: across EVERY tenant it moves memories that
// expired before cutoff to Trash, and returns the total moved. A failure on one
// tenant does not stop the others; the errors are joined.
func (s *Store) SweepExpired(ctx context.Context, cutoff time.Time) (int, error) {
	tenants := []string{""}
	if s.multiTenant {
		var err error
		if tenants, err = s.ListTenants(ctx, memory.ClassName); err != nil {
			return 0, err
		}
	}
	total := 0
	var errs []error
	for _, t := range tenants {
		n, err := s.Tenant(t).TrashExpiredBefore(ctx, cutoff)
		total += n
		if err != nil {
			errs = append(errs, fmt.Errorf("tenant %q: %w", t, err))
		}
	}
	return total, errors.Join(errs...)
}
