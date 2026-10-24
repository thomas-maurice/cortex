package outbox

import (
	"encoding/json"
	"errors"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"
)

// summaryBucket holds the offline session-summary slots: at most ONE per
// conversation id, latest wins. Same database file as the saves (so the same
// recallstate-prune caveat applies), but a separate bucket: slots are keyed by
// conversation id, not a memory UUID, and are not counted against MaxEntries.
const summaryBucket = "outbox_summary"

var errConvRequired = errors.New("outbox: summary conversation id is required")

func sortSummaries(s []SummaryEntry) {
	sort.SliceStable(s, func(i, j int) bool {
		if s[i].EnqueuedAt != s[j].EnqueuedAt {
			return s[i].EnqueuedAt < s[j].EnqueuedAt
		}
		return s[i].ConversationID < s[j].ConversationID
	})
}

// SummaryEntry is a pending SummarizeSession request for one conversation.
type SummaryEntry struct {
	ConversationID string `json:"conversation_id"`
	Request        []byte `json:"request"`     // proto.Marshal(SummarizeSessionRequest), replayed verbatim
	EnqueuedAt     int64  `json:"enqueued_at"` // unix microseconds; set by PutSummary when zero
	Preview        string `json:"preview,omitempty"`
	// Corrupt is set (never stored) when the stored value does not decode: listed
	// so it can be seen and purged, never sent.
	Corrupt bool `json:"-"`
}

// PutSummary stores e, replacing any slot for the same conversation.
func PutSummary(path string, e SummaryEntry) error {
	if e.ConversationID == "" {
		return errConvRequired
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
		b, err := tx.CreateBucketIfNotExists([]byte(summaryBucket))
		if err != nil {
			return err
		}
		return b.Put([]byte(e.ConversationID), val)
	})
}

// ListSummaries returns every slot, oldest first. A missing db is empty.
func ListSummaries(path string) ([]SummaryEntry, error) {
	if !exists(path) {
		return nil, nil
	}
	db, err := open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	var out []SummaryEntry
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(summaryBucket))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			var e SummaryEntry
			if json.Unmarshal(v, &e) != nil {
				e = SummaryEntry{ConversationID: string(k), Corrupt: true}
			}
			out = append(out, e)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sortSummaries(out)
	return out, nil
}

// DeleteSummary removes conv's slot and returns it. A missing slot (or db) is
// (zero, false, nil). When onlyAt != 0 the slot is removed only if it is still
// the one enqueued at that time, so a drain that just sent a summary cannot
// delete a newer one written meanwhile.
func DeleteSummary(path, conv string, onlyAt int64) (SummaryEntry, bool, error) {
	var e SummaryEntry
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
		b := tx.Bucket([]byte(summaryBucket))
		if b == nil {
			return nil
		}
		v := b.Get([]byte(conv))
		if v == nil {
			return nil
		}
		if json.Unmarshal(v, &e) != nil {
			e = SummaryEntry{ConversationID: conv, Corrupt: true}
		}
		if onlyAt != 0 && e.EnqueuedAt != onlyAt {
			e = SummaryEntry{}
			return nil
		}
		found = true
		return b.Delete([]byte(conv))
	})
	if err != nil {
		return SummaryEntry{}, false, err
	}
	return e, found, nil
}
