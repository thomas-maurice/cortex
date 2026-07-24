// Package recallstate tracks which memories have already been placed into a
// Claude session's context, so the same memory is never delivered in full twice
// — that would only inflate token use. It is shared by the two delivery paths:
// the `cortex hook recall` UserPromptSubmit hook (injects on every prompt) and
// the MCP server's cortex_memory_search (model-initiated searches). Both key
// their state by the Claude session id.
//
// Storage is a single bbolt database (default ~/.cache/cortex/cortex.db) with
// one root bucket per conversation id, so future per-conversation state can
// live alongside as sibling keys/sub-buckets:
//
//	<conversation-id>/          root bucket
//	  recalled/                 sub-bucket: memory id → unix time delivered
//	  lastSeen                  key: unix time of last touch (drives pruning)
//
// Multi-process access: bbolt takes an EXCLUSIVE flock for the lifetime of an
// open handle, so the database is never held open — every operation is
// open → txn → close, with a short lock timeout. Concurrent MCP servers and
// hook runs therefore interleave instead of deadlocking. The state is a
// best-effort optimization: a lock timeout or any other error degrades to an
// empty state / skipped write (worst case: one repeated injection), and the
// whole file lives in a cache directory, i.e. is disposable by contract —
// conversation buckets untouched for pruneAge are deleted.
package recallstate

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	// PruneAge is how long a conversation bucket may go untouched before the
	// automatic prune (piggybacked on writes) deletes it. A week: long enough
	// for resumed sessions and future per-conversation state, short enough that
	// the cache does not grow forever. `cortex state purge` overrides per call.
	PruneAge = 7 * 24 * time.Hour

	// openTimeout bounds the wait for the file lock. Holders are open→txn→close,
	// so waits are milliseconds; hitting this means degrading to empty state
	// rather than delaying a prompt.
	openTimeout = 500 * time.Millisecond

	// openAttempts/openRetryDelay: bbolt's Timeout already polls the flock, so
	// these outer retries parry the remaining races (concurrent first-time
	// creation, transient fs errors) — a couple of extra shots inside ~1s,
	// still far under the hook's 5s budget.
	openAttempts   = 3
	openRetryDelay = 250 * time.Millisecond

	recalledBucket = "recalled"
	lastSeenKey    = "lastSeen"
)

// DefaultPath is ~/.cache/cortex/cortex.db (deliberately XDG-style on every
// platform so the path is predictable across the user's machines), or "" (state
// disabled) when the home directory is unresolvable.
func DefaultPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".cache", "cortex", "cortex.db")
}

// State is the set of memory ids already delivered to one session's context,
// snapshotted at Load time. It holds no open database handle.
type State struct {
	path    string
	session string
	ids     map[string]bool
}

// open opens the database with retries, creating its directory if absent.
func open(path string) (*bolt.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	var db *bolt.DB
	var err error
	for attempt := 1; ; attempt++ {
		db, err = bolt.Open(path, 0o600, &bolt.Options{Timeout: openTimeout})
		if err == nil || attempt == openAttempts {
			return db, err
		}
		time.Sleep(openRetryDelay)
	}
}

// Load snapshots the delivered-memory set for sessionID (missing database or
// bucket, lock timeout, or empty path/session = empty state).
func Load(path, sessionID string) *State {
	s := &State{path: path, session: sessionID, ids: map[string]bool{}}
	if path == "" || sessionID == "" {
		s.path = ""
		return s
	}
	db, err := open(path)
	if err != nil {
		return s
	}
	defer func() { _ = db.Close() }()
	_ = db.View(func(tx *bolt.Tx) error {
		convo := tx.Bucket([]byte(sessionID))
		if convo == nil {
			return nil
		}
		if rec := convo.Bucket([]byte(recalledBucket)); rec != nil {
			return rec.ForEach(func(k, _ []byte) error {
				s.ids[string(k)] = true
				return nil
			})
		}
		return nil
	})
	return s
}

// Seen reports whether id was already delivered to this session.
func (s *State) Seen(id string) bool { return s.ids[id] }

// Add records ids as delivered, touches the conversation's lastSeen stamp, and
// prunes conversations idle past pruneAge. Best-effort: failures are ignored.
func (s *State) Add(ids ...string) {
	for _, id := range ids {
		s.ids[id] = true
	}
	if s.path == "" || len(ids) == 0 {
		return
	}
	db, err := open(s.path)
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()
	now := make([]byte, 8)
	binary.BigEndian.PutUint64(now, uint64(time.Now().Unix()))
	_ = db.Update(func(tx *bolt.Tx) error {
		convo, err := tx.CreateBucketIfNotExists([]byte(s.session))
		if err != nil {
			return err
		}
		rec, err := convo.CreateBucketIfNotExists([]byte(recalledBucket))
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := rec.Put([]byte(id), now); err != nil {
				return err
			}
		}
		if err := convo.Put([]byte(lastSeenKey), now); err != nil {
			return err
		}
		_, err = pruneTx(tx, s.session, PruneAge)
		return err
	})
}

// pruneTx deletes conversation buckets whose lastSeen is older than olderThan
// (or missing), except keep, returning the deleted conversation ids. Runs
// inside a write transaction — the database is small and this keeps pruning
// free of extra locking rounds.
func pruneTx(tx *bolt.Tx, keep string, olderThan time.Duration) ([]string, error) {
	cutoff := uint64(time.Now().Add(-olderThan).Unix())
	var stale []string
	c := tx.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if v != nil || string(k) == keep {
			continue // not a bucket, or the live conversation
		}
		last := tx.Bucket(k).Get([]byte(lastSeenKey))
		if len(last) != 8 || binary.BigEndian.Uint64(last) < cutoff {
			stale = append(stale, string(k))
		}
	}
	for _, k := range stale {
		if err := tx.DeleteBucket([]byte(k)); err != nil {
			return nil, err
		}
	}
	return stale, nil
}

// ---- inspection / maintenance (the `cortex state` commands) ----
//
// Unlike Load/Add, these are user-facing operations: errors are returned, not
// swallowed.

// Conversation summarizes one conversation bucket.
type Conversation struct {
	ID       string
	LastSeen time.Time
	Recalled int
}

// Conversations lists all conversation buckets, most recently seen first. A
// missing database is an empty list, not an error.
func Conversations(path string) ([]Conversation, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, nil
	}
	db, err := open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	var out []Conversation
	err = db.View(func(tx *bolt.Tx) error {
		c := tx.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if v != nil {
				continue
			}
			convo := tx.Bucket(k)
			info := Conversation{ID: string(k)}
			if last := convo.Get([]byte(lastSeenKey)); len(last) == 8 {
				info.LastSeen = time.Unix(int64(binary.BigEndian.Uint64(last)), 0)
			}
			if rec := convo.Bucket([]byte(recalledBucket)); rec != nil {
				info.Recalled = rec.Stats().KeyN
			}
			out = append(out, info)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LastSeen.After(out[j].LastSeen) })
	return out, nil
}

// Recalled returns a conversation's delivered memory ids with delivery times,
// newest first.
func Recalled(path, sessionID string) ([]RecalledMemory, error) {
	db, err := open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	var out []RecalledMemory
	err = db.View(func(tx *bolt.Tx) error {
		convo := tx.Bucket([]byte(sessionID))
		if convo == nil {
			return fmt.Errorf("no state for conversation %q", sessionID)
		}
		rec := convo.Bucket([]byte(recalledBucket))
		if rec == nil {
			return nil
		}
		return rec.ForEach(func(k, v []byte) error {
			m := RecalledMemory{ID: string(k)}
			if len(v) == 8 {
				m.DeliveredAt = time.Unix(int64(binary.BigEndian.Uint64(v)), 0)
			}
			out = append(out, m)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DeliveredAt.After(out[j].DeliveredAt) })
	return out, nil
}

// RecalledMemory is one delivered-memory record inside a conversation.
type RecalledMemory struct {
	ID          string
	DeliveredAt time.Time
}

// Clear deletes one conversation's bucket.
func Clear(path, sessionID string) error {
	db, err := open(path)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	return db.Update(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte(sessionID)) == nil {
			return fmt.Errorf("no state for conversation %q", sessionID)
		}
		return tx.DeleteBucket([]byte(sessionID))
	})
}

// Purge deletes every conversation idle for longer than olderThan and returns
// the purged conversation ids. A missing database purges nothing.
func Purge(path string, olderThan time.Duration) ([]string, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, nil
	}
	db, err := open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	var purged []string
	err = db.Update(func(tx *bolt.Tx) error {
		purged, err = pruneTx(tx, "", olderThan)
		return err
	})
	return purged, err
}
