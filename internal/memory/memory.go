// Package memory defines the core data model shared across the MCP server
// and the indexing worker.
package memory

import (
	"time"

	"github.com/google/uuid"
)

// Weaviate class + NATS stream/subject identifiers.
const (
	ClassName        = "Memory"
	SummaryClassName = "ConversationSummary"
	StreamName       = "MEMORY"
	SubjectIndex     = "memory.index"
	SubjectSummary   = "memory.summary"
	SubjectDead      = "memory.dead"
	SubjectAll       = "memory.>"
	ConsumerName     = "indexer"

	// MaxDeliver is how many times the worker attempts to index a message
	// before giving up and dead-lettering it to SubjectDead. Both subjects live
	// in StreamName (covered by SubjectAll), so dead letters persist for review.
	MaxDeliver = 10
)

// SummaryID derives the deterministic Weaviate object ID for a conversation's
// summary. Because the ID is a pure function of the conversation ID, re-saving a
// summary for the same conversation overwrites the previous one — giving exactly
// one, ever-current summary per conversation without any dedup logic.
func SummaryID(conversationID string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("cortex/summary:"+conversationID)).String()
}

// Record is one stored memory. It doubles as the NATS index payload and the
// Weaviate object body.
//
// Model and Dims are provenance: which embedding model vectorised this record
// and the resulting vector dimension. They are authoritative only after the
// worker indexes the record (the producer does not embed), so they are empty on
// a freshly published save event and stamped by the worker before Upsert.
type Record struct {
	ID        string    `json:"id"`
	Text      string    `json:"text"`
	Namespace string    `json:"namespace"`
	Tags      []string  `json:"tags"`
	Source    string    `json:"source"`
	CreatedAt time.Time `json:"createdAt"`
	Model     string    `json:"model,omitempty"`
	Dims      int       `json:"dims,omitempty"`

	// ConversationID ties a memory back to the client session that created it
	// (e.g. the Claude Code session ID). Optional provenance; metadata only,
	// never embedded.
	ConversationID string `json:"conversationId,omitempty"`

	// LinkedIDs are ids of other memories explicitly linked to this one
	// (bidirectional, set via the Link RPC). Metadata only, never embedded. It
	// threads through the worker upsert so reindex/redelivery preserve links.
	LinkedIDs []string `json:"linkedIds,omitempty"`
}

// Hit is a search result: a record plus its vector distance to the query.
type Hit struct {
	Record
	Distance float32 `json:"distance"`
}

// Summary is a single, ever-current digest of one conversation. It is unique per
// ConversationID (see SummaryID) and updated in place as the conversation
// evolves. Like Record, only its Text is embedded; everything else is metadata.
// It is the entry point for "do you remember the session where we…" recall: a
// vector match on the summary yields a ConversationID, which then fans out to
// the individual facts that carry the same conversationId.
type Summary struct {
	ConversationID string    `json:"conversationId"`
	Text           string    `json:"text"`
	Namespace      string    `json:"namespace"`
	Source         string    `json:"source"`
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
	Model          string    `json:"model,omitempty"`
	Dims           int       `json:"dims,omitempty"`
}

// SummaryHit is a summary plus its vector distance to the query.
type SummaryHit struct {
	Summary
	Distance float32 `json:"distance"`
}

// DeadLetter is a record the worker could not index after MaxDeliver attempts.
// It is published to SubjectDead so the data is preserved (not silently dropped)
// and can be inspected or requeued from the CLI.
type DeadLetter struct {
	Record     Record    `json:"record"`
	Error      string    `json:"error"`
	Deliveries int       `json:"deliveries"`
	FailedAt   time.Time `json:"failedAt"`
}
