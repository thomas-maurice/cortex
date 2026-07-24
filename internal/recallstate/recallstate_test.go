package recallstate

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// Dedup exists so the same memory is not re-delivered to a session's context on
// every prompt/search — repeated full texts inflate token use for nothing. Both
// the hook and the MCP server key by session id, so the state must round-trip
// across processes (open→txn→close each time; nothing holds the database).
func TestRoundTrip(t *testing.T) {
	db := filepath.Join(t.TempDir(), "sub", "cortex.db") // dir auto-created

	s := Load(db, "sess-1")
	assert.False(t, s.Seen("m1"))
	s.Add("m1", "m2")

	s2 := Load(db, "sess-1")
	assert.True(t, s2.Seen("m1"))
	assert.True(t, s2.Seen("m2"))
	assert.False(t, s2.Seen("m3"))

	// Another conversation's bucket shares nothing.
	assert.False(t, Load(db, "sess-2").Seen("m1"))

	// No session id / no path → state disabled, Add is a memory-only no-op.
	Load(db, "").Add("m9")
	assert.False(t, Load(db, "").Seen("m9"))
	Load("", "sess-1").Add("m9")
	assert.False(t, Load(db, "sess-1").Seen("m9"))
}

// backdate rewrites a conversation's lastSeen stamp.
func backdate(t *testing.T, db, convo string, age time.Duration) {
	t.Helper()
	bdb, err := bolt.Open(db, 0o600, nil)
	require.NoError(t, err)
	stale := make([]byte, 8)
	binary.BigEndian.PutUint64(stale, uint64(time.Now().Add(-age).Unix()))
	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(convo)).Put([]byte(lastSeenKey), stale)
	}))
	require.NoError(t, bdb.Close())
}

// Conversation buckets idle past PruneAge are deleted on the next write; the
// database lives in a cache dir and must not grow with every session forever.
func TestAutoPrune(t *testing.T) {
	db := filepath.Join(t.TempDir(), "cortex.db")

	Load(db, "old-sess").Add("m1")
	backdate(t, db, "old-sess", PruneAge+time.Hour)

	// A write from any other session prunes it; the live session survives.
	Load(db, "new-sess").Add("m2")
	assert.False(t, Load(db, "old-sess").Seen("m1"), "stale conversation bucket pruned")
	assert.True(t, Load(db, "new-sess").Seen("m2"))
}

// The `cortex state` surface: ls/show/clear/purge over the shared database, so
// the user can see and reset what dedup has recorded without touching bbolt.
func TestInspectionAndMaintenance(t *testing.T) {
	db := filepath.Join(t.TempDir(), "cortex.db")

	// Missing db: empty list / empty purge, no error (nothing was ever recorded).
	convos, err := Conversations(db)
	require.NoError(t, err)
	assert.Empty(t, convos)
	purged, err := Purge(db, time.Hour)
	require.NoError(t, err)
	assert.Empty(t, purged)

	Load(db, "sess-a").Add("m1", "m2")
	Load(db, "sess-b").Add("m3")

	convos, err = Conversations(db)
	require.NoError(t, err)
	require.Len(t, convos, 2)
	byID := map[string]Conversation{}
	for _, c := range convos {
		byID[c.ID] = c
		assert.False(t, c.LastSeen.IsZero())
	}
	assert.Equal(t, 2, byID["sess-a"].Recalled)
	assert.Equal(t, 1, byID["sess-b"].Recalled)

	rec, err := Recalled(db, "sess-a")
	require.NoError(t, err)
	require.Len(t, rec, 2)
	assert.False(t, rec[0].DeliveredAt.IsZero())

	_, err = Recalled(db, "nope")
	assert.Error(t, err, "unknown conversation is an error, not silence")

	// clear: memories become injectable again.
	require.NoError(t, Clear(db, "sess-a"))
	assert.False(t, Load(db, "sess-a").Seen("m1"))
	assert.Error(t, Clear(db, "sess-a"), "clearing twice reports the absence")

	// purge with a custom threshold takes only the idle conversation.
	Load(db, "sess-idle").Add("m4")
	backdate(t, db, "sess-idle", 2*time.Hour)
	purged, err = Purge(db, time.Hour)
	require.NoError(t, err)
	assert.Equal(t, []string{"sess-idle"}, purged)
	assert.True(t, Load(db, "sess-b").Seen("m3"), "active conversation survives purge")
}

// Session ids come from external JSON; as bucket names they are plain bytes, so
// even a path-shaped id must stay contained in the one database file.
func TestPathShapedSessionIDIsSafe(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "cortex.db")
	Load(db, "../../etc/passwd").Add("m1")
	assert.True(t, Load(db, "../../etc/passwd").Seen("m1"))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "nothing outside the database file")
	assert.Equal(t, "cortex.db", entries[0].Name())
}
