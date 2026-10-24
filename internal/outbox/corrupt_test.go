package outbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// A value that no longer decodes must not make the whole outbox unreadable
// (the drain would stall every queued memory behind it), and `drop` must be
// able to remove it (R5: stuck entries are cleared by hand).
func TestCorruptEntryIsListedAndDeletable(t *testing.T) {
	p := dbPath(t)
	require.NoError(t, Enqueue(p, Entry{ID: "good", Preview: "ok"}))

	db, err := open(p)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucketName)).Put([]byte("bad"), []byte("{not json"))
	}))
	require.NoError(t, db.Close())

	all, err := ListAll(p)
	require.NoError(t, err)
	require.Len(t, all, 2)
	byID := map[string]Entry{}
	for _, e := range all {
		byID[e.ID] = e
	}
	assert.False(t, byID["good"].Corrupt)
	assert.True(t, byID["bad"].Corrupt)

	got, found, err := Get(p, "bad")
	require.NoError(t, err)
	assert.True(t, found)
	assert.True(t, got.Corrupt)

	_, found, err = Delete(p, "bad")
	require.NoError(t, err)
	assert.True(t, found)
	_, found, err = Get(p, "bad")
	require.NoError(t, err)
	assert.False(t, found)
}

// Equal enqueue times must still list in a stable order (drain order).
func TestEqualEnqueueTimeTieBreaksByID(t *testing.T) {
	p := dbPath(t)
	require.NoError(t, Enqueue(p, Entry{ID: "b", EnqueuedAt: 5}))
	require.NoError(t, Enqueue(p, Entry{ID: "a", EnqueuedAt: 5}))
	all, err := ListAll(p)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, ids(all))
}

// A corrupt entry has no enqueue time; it must not make the outbox look
// decades old in the startup notice.
func TestStatsOldestIgnoresCorrupt(t *testing.T) {
	p := dbPath(t)
	require.NoError(t, Enqueue(p, Entry{ID: "good", EnqueuedAt: 42}))
	db, err := open(p)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucketName)).Put([]byte("bad"), []byte("{not json"))
	}))
	require.NoError(t, db.Close())

	st, err := ReadStats(p)
	require.NoError(t, err)
	assert.Equal(t, 2, st.Total)
	assert.EqualValues(t, 42, st.OldestEnqueuedAt)
}
