// Package memory defines the core data model shared across the MCP server
// and the indexing worker.
package memory

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Weaviate class + NATS stream/subject identifiers.
const (
	ClassName        = "Memory"
	ChunkClassName   = "MemoryChunk"
	SummaryClassName = "ConversationSummary"
	// TrashClassName holds soft-deleted memories (see store/trash.go). Same tenancy
	// as Memory, no vector.
	TrashClassName = "Trash"
	// UserClassName / ApiKeyClassName are the multi-tenancy identity registry —
	// global (NON-multi-tenant) collections that hold the user accounts and API
	// keys. They exist only when CORTEX_MULTI_TENANT is on, and are read/written by
	// the SERVER only (never the worker or a client).
	UserClassName   = "User"
	ApiKeyClassName = "ApiKey"
	// MetaClassName is the durable store-level metadata class (schema version,
	// applied migrations, usage-tracking epoch). Always single-tenant, no vectors,
	// plain (not alias-backed); it survives DeleteClass/DeleteAllClasses because it
	// describes the store, not the data. See store/meta.go.
	MetaClassName  = "CortexMeta"
	StreamName     = "MEMORY"
	SubjectIndex   = "memory.index"
	SubjectSummary = "memory.summary"
	SubjectLink    = "memory.link"
	// DeadStreamName holds dead letters in their OWN stream (age/size bounded),
	// separate from the interest-retained work queue StreamName. SubjectDead lives
	// OUTSIDE memory.> so the two streams' subjects never overlap (JetStream forbids
	// that); the legacy "memory.dead" subject only exists in pre-migration streams.
	DeadStreamName   = "MEMORY_DEAD"
	SubjectDead      = "deadletter.memory"
	ConsumerName     = "indexer"
	LinkConsumerName = "linker"

	// MaxDeliver is how many times the worker attempts to index a message
	// before giving up and dead-lettering it to SubjectDead (stream DeadStreamName),
	// so dead letters persist for review.
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
//
// UserID is the tenant the edge endpoints belong to. It is stamped by the RPC
// handler from the request context identity and consumed by the worker's link
// handler to scope its store lookups/writes to the correct tenant. It is NOT a
// Weaviate property — the tenant is the boundary, not a field.
type LinkMsg struct {
	Op     LinkOp `json:"op"`
	A      string `json:"a"`
	B      string `json:"b"`
	UserID string `json:"userId,omitempty"`
}

// SummaryID derives the deterministic Weaviate object ID for a conversation's
// summary. Because the ID is a pure function of the conversation ID, re-saving a
// summary for the same conversation overwrites the previous one — giving exactly
// one, ever-current summary per conversation without any dedup logic.
func SummaryID(conversationID string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("cortex/summary:"+conversationID)).String()
}

// ChunkID derives the deterministic Weaviate object ID for chunk `index` of the
// memory `memoryID`. Because it is a pure function of (memoryID, index),
// re-indexing a memory overwrites its existing chunks in place (idempotent), and
// stale chunks from a now-shorter text are exactly those whose index is past the
// new chunk count — which the store deletes after re-upserting.
func ChunkID(memoryID string, index int) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, fmt.Appendf(nil, "cortex/chunk:%s:%d", memoryID, index)).String()
}

// UserID derives the deterministic Weaviate object id for a user from their
// username. It doubles as the Weaviate TENANT name for that user's memories.
// Deterministic so a username maps to a stable tenant and a duplicate username
// collides on object id (detectable on create) rather than silently forking a
// second account/tenant. There is no rename-username feature, so the "renaming a
// username orphans its tenant" downside cannot occur.
func UserID(username string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("cortex/user:"+username)).String()
}

// ApiKeyID derives the deterministic object id for an API key from its hash, so
// re-adding the SAME raw key (e.g. the env bootstrap key applied on every boot) is
// an idempotent upsert rather than a duplicate row.
func ApiKeyID(keyHash string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("cortex/apikey:"+keyHash)).String()
}

// Record is one stored memory. It doubles as the NATS index payload and the
// Weaviate object body.
//
// Model and Dims are provenance: which embedding model vectorised this record
// and the resulting vector dimension. They are authoritative only after the
// worker indexes the record (the producer does not embed), so they are empty on
// a freshly published save event and stamped by the worker before Upsert.
//
// UserID is stamped by the RPC handler from the request context identity onto the
// NATS payload only — it is NOT written as a Weaviate property (the tenant is
// the isolation boundary, not a field). The worker reads it to scope its store
// writes to the correct tenant.
type Record struct {
	ID        string    `json:"id"`
	Text      string    `json:"text"`
	Namespace string    `json:"namespace"`
	Tags      []string  `json:"tags"`
	Source    string    `json:"source"`
	CreatedAt time.Time `json:"createdAt"`
	Model     string    `json:"model,omitempty"`
	Dims      int       `json:"dims,omitempty"`
	// KeepTags (NATS payload only, never persisted) means "this message does not
	// intend to change the tags": for an EXISTING memory the worker keeps the
	// STORED tags (memory and chunks) instead of writing the message's snapshot.
	// Without it an edit/reindex queued before a tag rename/delete would restore
	// the old tags (tags are written by the index path AND by tag rename; see
	// docs/DESIGN.md). Producers that carry a genuine point-in-time tag set
	// (Save, replace_tags edits, import/restore, dead-letter retry of those) leave
	// it false.
	KeepTags bool `json:"keepTags,omitempty"`
	// NeverExpires (NATS payload only, never persisted) is the Save caller's
	// explicit "no expiry": the worker then skips the supersedes expiry inheritance
	// (merge.InheritedExpiry) so a permanent merge of time-bound sources stays
	// permanent. Old messages decode to false (inheritance as before).
	NeverExpires bool `json:"neverExpires,omitempty"`

	// UserID is the tenant this record belongs to (NATS payload only; not a Weaviate property).
	UserID string `json:"userId,omitempty"`

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

	// UseCount / LastUsedAt are the ACTUAL-USE signal behind stale review: how many
	// times (and when last) the memory was delivered with full text into an agent's
	// context (RecordUse). Independent of AccessCount/LastAccessedAt (ranking
	// reinforcement, RERANK_WEIGHT-gated). Owned by the server's RecordUse merge:
	// the index path creates a missing memory with whatever the payload carries
	// (import/restore keep history) but NEVER writes these on an existing one.
	UseCount   int       `json:"useCount,omitempty"`
	LastUsedAt time.Time `json:"lastUsedAt"`

	// IndexedSeq is the JetStream stream sequence of the index message that last
	// wrote this object (the worker's stale-write guard). It is set by the worker
	// from message metadata under the id lock and persisted as a Weaviate
	// property; whatever a producer puts here on the payload is ignored.
	IndexedSeq uint64 `json:"indexedSeq,omitempty"`
	// IndexedStream identifies the stream IndexedSeq belongs to (stream creation
	// time, unix microseconds; 0 = unknown/legacy). Sequences from a recreated
	// stream restart at 1, so the guard only compares seqs within one stream.
	IndexedStream int64 `json:"indexedStream,omitempty"`

	// ValidUntil is when this memory stops being true ("deploy freeze until
	// Friday"). The zero time means it never expires. Expired memories are hidden
	// from Search/List/recall by default and, once expired for longer than the
	// server's EXPIRY_GRACE, swept into Trash with reason "expired". Part of the
	// memory's content: index-owned (written by the worker's index path, or by the
	// metadata-only expiry edit), never embedded.
	ValidUntil time.Time `json:"validUntil,omitzero"`
}

// Expired reports whether the memory has a validity limit that has passed.
func (r Record) Expired(now time.Time) bool {
	return !r.ValidUntil.IsZero() && !r.ValidUntil.After(now)
}

// Hit is a search result: a record plus its vector distance to the query.
type Hit struct {
	Record
	Distance float32 `json:"distance"`
	// Match is the winning chunk for hits found via the chunk index; nil for
	// whole-memory-vector hits.
	Match *Match `json:"match,omitempty"`
}

// Match is the best-matching chunk of a memory. Start/End are character (rune)
// offsets of the chunk within the memory text, [Start, End); both 0 when the
// chunk text could not be located (End == 0).
type Match struct {
	ChunkIndex int    `json:"chunkIndex"`
	Text       string `json:"text"`
	Start      int    `json:"start"`
	End        int    `json:"end"`
}

// Summary is a single, ever-current digest of one conversation. It is unique per
// ConversationID (see SummaryID) and updated in place as the conversation
// evolves. Like Record, only its Text is embedded; everything else is metadata.
// It is the entry point for "do you remember the session where we…" recall: a
// vector match on the summary yields a ConversationID, which then fans out to
// the individual facts that carry the same conversationId.
//
// UserID is stamped by the RPC handler from the request context identity onto the
// NATS payload only — it is NOT written as a Weaviate property.
type Summary struct {
	ConversationID string    `json:"conversationId"`
	Text           string    `json:"text"`
	Namespace      string    `json:"namespace"`
	Source         string    `json:"source"`
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
	Model          string    `json:"model,omitempty"`
	Dims           int       `json:"dims,omitempty"`
	// UserID is the tenant this summary belongs to (NATS payload only; not a Weaviate property).
	UserID string `json:"userId,omitempty"`
	// IndexedSeq: stream sequence of the message that last wrote this summary
	// (stale-write guard, see Record.IndexedSeq). Ignored on the payload.
	IndexedSeq    uint64 `json:"indexedSeq,omitempty"`
	IndexedStream int64  `json:"indexedStream,omitempty"`
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

// Trash reasons recorded on a TrashEntry (the supersedes reason is suffixed with
// the superseding memory's id).
const (
	TrashReasonDelete          = "delete"
	TrashReasonSupersedes      = "supersedes:"
	TrashReasonMergedInto      = "merged-into:"
	TrashReasonNamespaceDelete = "namespace-delete"
	// Reasons a caller may pass to Delete (so mass deletions are filterable).
	TrashReasonStaleReview     = "stale-review"
	TrashReasonDuplicateReview = "duplicate-review"
	// TrashReasonExpired marks a memory moved to Trash by the expiry sweep.
	TrashReasonExpired = "expired"
)

// NormalizeDeleteReason maps a caller-supplied delete reason to one of the allowed
// values ("" = "delete"); ok is false for anything outside the bounded set.
func NormalizeDeleteReason(r string) (string, bool) {
	switch r {
	case "", TrashReasonDelete:
		return TrashReasonDelete, true
	case TrashReasonStaleReview, TrashReasonDuplicateReview:
		return r, true
	}
	return "", false
}

// TrashReasonMatches reports whether an entry's reason satisfies a filter: empty
// matches all, otherwise exact or a family prefix ("supersedes" matches
// "supersedes:<id>").
func TrashReasonMatches(reason, filter string) bool {
	return filter == "" || reason == filter || strings.HasPrefix(reason, filter+":")
}

// TrashEntry is a soft-deleted Record plus when and why it was deleted.
type TrashEntry struct {
	Record
	DeletedAt time.Time `json:"deletedAt"`
	Reason    string    `json:"reason"`
}
