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
	SubjectLink      = "memory.link"
	SubjectDead      = "memory.dead"
	SubjectAll       = "memory.>"
	ConsumerName     = "indexer"
	LinkConsumerName = "linker"

	// MaxDeliver is how many times the worker attempts to index a message
	// before giving up and dead-lettering it to SubjectDead. Both subjects live
	// in StreamName (covered by SubjectAll), so dead letters persist for review.
	MaxDeliver = 10

	// LinkMaxDeliver is the link consumer's retry budget. It is much larger than
	// MaxDeliver because a link's endpoint may simply be a record whose index
	// event is still queued: the link must out-wait the embedding backlog before
	// it concludes the endpoint genuinely does not exist. With the capped backoff
	// in the worker this spans many minutes.
	LinkMaxDeliver = 50
)

// LinkOp is the mutation a LinkMsg requests on an edge.
type LinkOp string

const (
	LinkOpAdd    LinkOp = "add"
	LinkOpRemove LinkOp = "remove"
)

// LinkMsg is a single bidirectional edge mutation published to SubjectLink and
// applied idempotently by the worker's link consumer. A and B are canonicalized
// (sorted) by the publisher so {A,B} and {B,A} are the same edge, giving a stable
// Nats-Msg-Id for broker-level dedup. The apply is idempotent: add is a set-union
// on both endpoints' link lists, remove is a set-difference, so redelivery and
// double-publish are no-ops. For add, if either endpoint is not yet indexed (its
// index event may still be queued) the worker NAKs for retry until both land or
// LinkMaxDeliver is exhausted — the out-of-order case this queue exists for.
type LinkMsg struct {
	Op LinkOp `json:"op"`
	A  string `json:"a"`
	B  string `json:"b"`
}

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

	// Supersedes carries a one-shot instruction (NOT persisted state): ids of
	// existing memories this record replaces, e.g. the sources gathered by a
	// Consolidate call and merged into this memory. It rides the NATS index
	// payload from the Save RPC to the worker, which — only AFTER this
	// record is durably upserted — deletes each superseded source, then discards
	// the list. Deleting post-upsert is the safety property: a crash mid-way can
	// leave stale sources behind (the pre-merge state) but never loses the merged
	// content. Never written as a Weaviate property, so it is empty on
	// reindex/redelivery.
	Supersedes []string `json:"supersedes,omitempty"`

	// DupCandidates are ids of existing memories the worker found to be near
	// duplicates of this one at index time (vector distance within
	// DEDUP_DISTANCE). It is a heuristic hint for review, NOT a confirmed
	// relationship like LinkedIDs: the worker never deletes or merges on its
	// own — a human or the agent adjudicates via the review tool / UI. Stored as
	// a Weaviate property and recomputed whenever the record is re-indexed.
	// One-directional (this record -> the older memories it resembles).
	DupCandidates []string `json:"dupCandidates,omitempty"`

	// NotDuplicateOf are ids of memories a reviewer has explicitly confirmed are
	// NOT duplicates of this one. The worker excludes these when recomputing
	// DupCandidates, so a dismissed pair is never re-flagged. Bidirectional (set
	// on both records, like LinkedIDs) so neither side re-flags the other, and
	// persisted as a Weaviate property so it survives reindex/redelivery.
	NotDuplicateOf []string `json:"notDuplicateOf,omitempty"`

	// AccessCount is the "living memory" usage signal: how many times this record
	// has surfaced as a top search hit and been reinforced. It only accrues when
	// re-ranking is enabled server-side (RERANK_WEIGHT>0). Stored as a Weaviate
	// property and carried on the index payload so it survives reindex (the worker
	// re-stamps Model/Dims but leaves this untouched). Metadata only, never embedded.
	AccessCount int `json:"accessCount,omitempty"`

	// LastAccessedAt is when this record was last reinforced by a search hit.
	// Together with AccessCount it feeds the decay/reinforcement re-rank: recency
	// is measured from here (falling back to CreatedAt when never accessed).
	// Stored as a Weaviate property, carried on the index payload like AccessCount.
	LastAccessedAt time.Time `json:"lastAccessedAt"`
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
