package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// isStale feeds bulk deletion, so its edge cases must be conservative and pinned.
// Staleness is based on ACTUAL USE (useCount/lastUsedAt); a never-used memory's
// reference time is max(createdAt, trackingSince) because usage before tracking
// began was invisible.
func TestIsStale(t *testing.T) {
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	ago := func(d int) time.Time { return now.Add(-time.Duration(d) * day) }
	rec := func(created time.Time, count int, last time.Time) memory.Record {
		return memory.Record{CreatedAt: created, UseCount: count, LastUsedAt: last}
	}
	never := StaleOpts{MinAge: 30 * day}
	since := StaleOpts{MinAge: 30 * day, IdleFor: 90 * day}
	neverNoGuard := StaleOpts{}
	sinceNoGuard := StaleOpts{IdleFor: 90 * day}
	withTracking := func(o StaleOpts, d int) StaleOpts { o.TrackingSince = ago(d); return o }

	tests := []struct {
		name string
		r    memory.Record
		o    StaleOpts
		want bool
	}{
		{"never: old never-used", rec(ago(100), 0, time.Time{}), never, true},
		{"never: too new for the guard", rec(ago(5), 0, time.Time{}), never, false},
		{"never: exactly at min-age boundary is stale", rec(ago(30), 0, time.Time{}), never, true},
		{"never: used long ago is not never-used", rec(ago(300), 2, ago(200)), never, false},
		{"never: count>0 but no lastUsedAt is not never-used", rec(ago(300), 3, time.Time{}), never, false},
		{"never: missing createdAt + min-age>0 -> unknown age, not stale", rec(time.Time{}, 0, time.Time{}), never, false},
		{"never: missing createdAt + min-age 0 -> stale", rec(time.Time{}, 0, time.Time{}), neverNoGuard, true},
		{"since: last use older than N", rec(ago(300), 2, ago(120)), since, true},
		{"since: last use newer than N", rec(ago(300), 2, ago(10)), since, false},
		{"since: never used, created older than N", rec(ago(100), 0, time.Time{}), since, true},
		{"since: never used, created newer than N", rec(ago(60), 0, time.Time{}), since, false},
		{"since: count>0 without lastUsedAt falls back to createdAt (old)", rec(ago(200), 4, time.Time{}), since, true},
		{"since: count>0 without lastUsedAt falls back to createdAt (young)", rec(ago(60), 4, time.Time{}), since, false},
		{"since: min-age guard applies even when last use is old", rec(ago(20), 1, ago(200)), since, false},
		{"since: missing createdAt + min-age>0 -> not stale", rec(time.Time{}, 0, time.Time{}), since, false},
		{"since: missing createdAt + min-age 0, no use, no epoch -> no reference, not stale", rec(time.Time{}, 0, time.Time{}), sinceNoGuard, false},
		{"since: missing createdAt + min-age 0 but old use -> still not stale (age unknown, when in doubt do not flag)", rec(time.Time{}, 1, ago(120)), sinceNoGuard, false},

		// Tracking epoch: usage before it was invisible.
		{"epoch: never-used old memory is NOT stale when tracking began 5d ago (min-age)", rec(ago(300), 0, time.Time{}), withTracking(never, 5), false},
		{"epoch: ... but is stale once tracking is 40d old", rec(ago(300), 0, time.Time{}), withTracking(never, 40), true},
		{"epoch: since-mode reference is max(created, epoch): 60d of tracking < 90d window", rec(ago(300), 0, time.Time{}), withTracking(since, 60), false},
		{"epoch: since-mode: 100d of tracking > 90d window", rec(ago(300), 0, time.Time{}), withTracking(since, 100), true},
		{"epoch: memory created after the epoch uses its own createdAt", rec(ago(100), 0, time.Time{}), withTracking(since, 400), true},
		{"epoch: a real lastUsedAt ignores the epoch", rec(ago(300), 1, ago(120)), withTracking(since, 5), true},
		{"epoch: a used memory's min-age guard starts at createdAt, not the epoch", rec(ago(300), 1, ago(200)), withTracking(since, 5), true},
		{"epoch: missing createdAt + epoch + since-mode, no guard -> the epoch must not stand in for an unknown age", rec(time.Time{}, 0, time.Time{}), withTracking(sinceNoGuard, 120), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isStale(tc.r, tc.o, now))
		})
	}
}
