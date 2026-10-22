package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// isStale feeds bulk deletion, so its edge cases must be conservative and pinned.
func TestIsStale(t *testing.T) {
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	ago := func(d int) time.Time { return now.Add(-time.Duration(d) * day) }
	rec := func(created time.Time, count int, last time.Time) memory.Record {
		return memory.Record{CreatedAt: created, AccessCount: count, LastAccessedAt: last}
	}
	never := StaleOpts{MinAge: 30 * day}
	since := StaleOpts{MinAge: 30 * day, IdleFor: 90 * day}
	neverNoGuard := StaleOpts{}
	sinceNoGuard := StaleOpts{IdleFor: 90 * day}

	tests := []struct {
		name string
		r    memory.Record
		o    StaleOpts
		want bool
	}{
		{"never: old never-recalled", rec(ago(100), 0, time.Time{}), never, true},
		{"never: too new for the guard", rec(ago(5), 0, time.Time{}), never, false},
		{"never: exactly at min-age boundary is stale", rec(ago(30), 0, time.Time{}), never, true},
		{"never: recalled long ago is not never-recalled", rec(ago(300), 2, ago(200)), never, false},
		{"never: count>0 but no lastAccessedAt is not never-recalled", rec(ago(300), 3, time.Time{}), never, false},
		{"never: missing createdAt + min-age>0 -> unknown age, not stale", rec(time.Time{}, 0, time.Time{}), never, false},
		{"never: missing createdAt + min-age 0 -> stale", rec(time.Time{}, 0, time.Time{}), neverNoGuard, true},
		{"since: last recall older than N", rec(ago(300), 2, ago(120)), since, true},
		{"since: last recall newer than N", rec(ago(300), 2, ago(10)), since, false},
		{"since: never recalled, created older than N", rec(ago(100), 0, time.Time{}), since, true},
		{"since: never recalled, created newer than N", rec(ago(60), 0, time.Time{}), since, false},
		{"since: count>0 without lastAccessedAt falls back to createdAt (old)", rec(ago(200), 4, time.Time{}), since, true},
		{"since: count>0 without lastAccessedAt falls back to createdAt (young)", rec(ago(60), 4, time.Time{}), since, false},
		{"since: min-age guard applies even when last recall is old", rec(ago(20), 1, ago(200)), since, false},
		{"since: missing createdAt + min-age>0 -> not stale", rec(time.Time{}, 0, time.Time{}), since, false},
		{"since: missing createdAt + min-age 0, no recall -> no reference, not stale", rec(time.Time{}, 0, time.Time{}), sinceNoGuard, false},
		{"since: missing createdAt + min-age 0 but old recall -> stale", rec(time.Time{}, 1, ago(120)), sinceNoGuard, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isStale(tc.r, tc.o, now))
		})
	}
}
