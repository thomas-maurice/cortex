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

// Conversation buckets idle past pruneAge are deleted on the next write; the
// database lives in a cache dir and must not grow with every session forever.
func TestPrune(t *testing.T) {
	db := filepath.Join(t.TempDir(), "cortex.db")

	Load(db, "old-sess").Add("m1")

	// Backdate old-sess's lastSeen beyond pruneAge.
	bdb, err := bolt.Open(db, 0o600, nil)
	require.NoError(t, err)
	stale := make([]byte, 8)
	binary.BigEndian.PutUint64(stale, uint64(time.Now().Add(-72*time.Hour).Unix()))
	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("old-sess")).Put([]byte(lastSeenKey), stale)
	}))
	require.NoError(t, bdb.Close())

	// A write from any other session prunes it; the live session survives.
	Load(db, "new-sess").Add("m2")
	assert.False(t, Load(db, "old-sess").Seen("m1"), "stale conversation bucket pruned")
	assert.True(t, Load(db, "new-sess").Seen("m2"))
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
