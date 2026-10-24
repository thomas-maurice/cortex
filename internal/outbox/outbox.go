// Package outbox persists Save requests that could not reach the server, so the
// MCP client can replay them later. Pure storage: no RPC, classification or
// draining lives here.
//
// Storage is a bbolt database (default ~/.cache/cortex/outbox.db) with one root
// bucket, "outbox", mapping the client-minted memory UUID to a JSON Entry. It is
// deliberately NOT recallstate's cortex.db: recallstate.pruneTx deletes every
// root bucket lacking a fresh lastSeen stamp, which would silently drop queued
// saves.
//
// Concurrency discipline is copied from recallstate: the database is never held
// open — every operation is open (500ms lock timeout, 3 attempts) → txn →
// close, so concurrent MCP servers and CLI runs interleave. Unlike recallstate,
// Enqueue/Delete errors are RETURNED: a lost write here is a lost memory.
package outbox

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"time"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"
)

const (
	openTimeout    = 500 * time.Millisecond
	openAttempts   = 3
	openRetryDelay = 250 * time.Millisecond

	bucketName = "outbox"

	// previewRunes bounds Entry.Preview.
	previewRunes = 120
)

// DefaultMaxEntries is the default cap on queued entries.
const DefaultMaxEntries = 1000

// MaxEntries is the cap on queued entries. At the cap Enqueue of a NEW id
// returns ErrFull (nothing is evicted or dropped silently).
var MaxEntries = DefaultMaxEntries

// ErrFull is returned by Enqueue when the outbox holds MaxEntries entries and
// the id is not already queued.
var ErrFull = errors.New("outbox is full")

// DefaultPath is ~/.cache/cortex/outbox.db (same directory as recallstate's
// cortex.db, different file), or "" when the home directory is unresolvable.
func DefaultPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".cache", "cortex", "outbox.db")
}

// Entry is one queued save. ID is the client-minted memory UUID: the same value
// as SaveRequest.id and the final object id, so re-enqueueing replaces.
type Entry struct {
	ID             string `json:"id"`
	Request        []byte `json:"request"` // proto.Marshal(SaveRequest), replayed verbatim
	ConversationID string `json:"conversation_id,omitempty"`
	EnqueuedAt     int64  `json:"enqueued_at"` // unix microseconds; set by Enqueue when zero
	Preview        string `json:"preview,omitempty"`
	// Corrupt is set (never stored) when the stored value does not decode: the
	// entry is still listed by id so it can be seen and dropped, but not sent.
	Corrupt bool `json:"-"`
}

// Stats summarizes the outbox. OldestEnqueuedAt is 0 when empty.
type Stats struct {
	Total            int
	OldestEnqueuedAt int64
	ByConversation   map[string]int
}

// PreviewOf returns the first ~120 runes of text, for Entry.Preview.
func PreviewOf(text string) string {
	if utf8.RuneCountInString(text) <= previewRunes {
		return text
	}
	return string([]rune(text)[:previewRunes])
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

// exists reports whether the db file is present; read paths treat a missing
// file as empty without creating it.
func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// Enqueue stores e, replacing any entry with the same ID. A new ID at the cap
// yields ErrFull.
func Enqueue(path string, e Entry) error {
	if e.ID == "" {
		return errors.New("outbox: entry id is required")
	}
	if e.EnqueuedAt == 0 {
		e.EnqueuedAt = time.Now().UnixMicro()
	}
	val, err := json.Marshal(e)
	if err != nil {
		return err
	}
	db, err := open(path)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}
		if b.Get([]byte(e.ID)) == nil && b.Stats().KeyN >= MaxEntries {
			return ErrFull
		}
		return b.Put([]byte(e.ID), val)
	})
}

// ListAll returns every entry, oldest first. A missing db is empty.
func ListAll(path string) ([]Entry, error) {
	if !exists(path) {
		return nil, nil
	}
	db, err := open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	var out []Entry
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			var e Entry
			if err := json.Unmarshal(v, &e); err != nil {
				// One bad value must not hide (or block the drain of) the rest.
				e = Entry{ID: string(k), Corrupt: true}
			}
			out = append(out, e)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	// bbolt iterates by key (a UUID); callers want enqueue order.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].EnqueuedAt != out[j].EnqueuedAt {
			return out[i].EnqueuedAt < out[j].EnqueuedAt
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

// ListByConversation returns the entries of one conversation, oldest first.
func ListByConversation(path, convID string) ([]Entry, error) {
	all, err := ListAll(path)
	if err != nil {
		return nil, err
	}
	var out []Entry
	for _, e := range all {
		if e.ConversationID == convID {
			out = append(out, e)
		}
	}
	return out, nil
}

// Get returns one entry by id.
func Get(path, id string) (Entry, bool, error) {
	var e Entry
	if !exists(path) {
		return e, false, nil
	}
	db, err := open(path)
	if err != nil {
		return e, false, err
	}
	defer func() { _ = db.Close() }()
	found := false
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return nil
		}
		v := b.Get([]byte(id))
		if v == nil {
			return nil
		}
		found = true
		if json.Unmarshal(v, &e) != nil {
			e = Entry{ID: id, Corrupt: true}
		}
		return nil
	})
	return e, found, err
}

// Delete removes id and returns the removed entry. A missing id (or db) is
// (zero, false, nil).
func Delete(path, id string) (Entry, bool, error) {
	var e Entry
	if !exists(path) {
		return e, false, nil
	}
	db, err := open(path)
	if err != nil {
		return e, false, err
	}
	defer func() { _ = db.Close() }()
	found := false
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return nil
		}
		v := b.Get([]byte(id))
		if v == nil {
			return nil
		}
		if json.Unmarshal(v, &e) != nil {
			e = Entry{ID: id, Corrupt: true} // still deletable: drop must clear a stuck entry
		}
		found = true
		return b.Delete([]byte(id))
	})
	if err != nil {
		return Entry{}, false, err
	}
	return e, found, nil
}

// ReadStats reports totals, the oldest enqueue time and per-conversation counts.
func ReadStats(path string) (Stats, error) {
	st := Stats{ByConversation: map[string]int{}}
	all, err := ListAll(path)
	if err != nil {
		return st, err
	}
	for _, e := range all {
		st.Total++
		st.ByConversation[e.ConversationID]++
		// all is oldest first; a corrupt entry has no enqueue time and must not
		// pass for the oldest.
		if st.OldestEnqueuedAt == 0 && !e.Corrupt {
			st.OldestEnqueuedAt = e.EnqueuedAt
		}
	}
	return st, nil
}
