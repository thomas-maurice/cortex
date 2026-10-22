# Cortex — Design

Cortex is a self-hosted **second brain** for Claude (and any MCP client). v1
scope: **memory** — save facts, recall them by meaning across sessions. The
architecture is built to grow (documents, blobs, extraction, temporal graph)
without rework.

## Architecture (v1)

```
                      ┌─────────────────────────────────────────────┐
                      │                 Cortex                        │
                      │                                               │
  Claude ──stdio──►   │  cortex-mcp  ──memory_save──► NATS JetStream  │
  (MCP client)        │      │                        (memory.index)  │
                      │      │                              │         │
                      │      │                       cortex-worker    │
                      │      │                     (durable consumer) │
                      │      │                        │         │     │
                      │      │                   Ollama embed   │     │
                      │      │                  (qwen3-embed)   │     │
                      │      │                              ▼         │
                      │      └──memory_search──► Ollama embed query   │
                      │                                 │             │
                      │                          Weaviate (nearVector)│
                      │                          vectorizer: none ◄───┘ (worker writes)
                      └─────────────────────────────────────────────┘
```

### The transport decision ("Option B")

`memory_save` and `memory_search` take **different paths on purpose**:

- **Writes → NATS JetStream (async, durable).** The MCP server publishes and
  returns immediately. The worker drains the stream, embeds, and writes to
  Weaviate. This absorbs bursts and survives the embedder/DB being down — that
  is exactly what a durable queue is for.
- **Search → direct to Ollama + Weaviate (synchronous).** A tool call blocks
  until it has results, so putting a queue in front of a read buffers nothing.
  The MCP server embeds the query and does a `nearVector` lookup itself.

The alternative ("pure NATS", search via request/reply) was considered. It gives
a single owner of the vector store but adds a hop and more code for no functional
win on a single-user local setup. We chose Option B: **writes through NATS, reads
direct.** Trade-off accepted: the MCP server and the worker both hold a Weaviate
+ Ollama client.

## Components

| Component | Process | Talks to | Role |
|-----------|---------|----------|------|
| `cmd/mcp` | host binary (Claude execs it) | NATS (publish), Ollama, Weaviate | exposes tools |
| `cmd/worker` | container | NATS (consume), Ollama, Weaviate | embed + write |
| `internal/memory` | lib | — | shared `Record`/`Hit`, names |
| `internal/bus` | lib | NATS | connect, stream, publish |
| `internal/embed` | lib | Ollama | `Embed(text) → []float32` |
| `internal/store` | lib | Weaviate | schema, upsert, search, delete |

## Data model

**Auto-schema is disabled** (`AUTOSCHEMA_ENABLED=false` on Weaviate: compose, TrueNAS
compose, CI, `scripts/wv-test.sh`). The schema is declared in code only
(`memoryProperties` / `chunkProperties` / `summaryProperties` / `trashProperties` / `userProperties` /
`apiKeyProperties`, applied by `EnsureSchema` / `EnsureIdentitySchema`, additive via
`ensureProps`). Any new property a write path sets MUST be added to those lists, or
the write fails. Why: reindex-with-rebuild and `migrate-mt` drop and recreate classes;
with auto-schema on, a worker write landing in that window silently created `Memory`
with an inferred, non-MT, word-tokenized schema and the server's recreate then failed
"already exists". With it off the write errors and the worker's NAK/retry (2s delay,
`MaxDeliver` 10 for index, so a window of ~20s) applies it after the schema is
back; a rebuild that keeps the class missing for longer dead-letters those writes
(requeue with `cortex dead --requeue`) — still far better than a silently wrong
schema. Pinned by
`internal/store/autoschema_integration_test.go`.

Weaviate class `Memory`, `vectorizer: none`:

| Property | Type | Notes |
|----------|------|-------|
| `text` | text | the memory content (stored verbatim in v1) |
| `namespace` | text | scope, e.g. project name or `global` |
| `tags` | text[] | free-form labels |
| `source` | text | origin, e.g. `claude-code` |
| `createdAt` | date | RFC3339; kept for future temporal reasoning |
| `conversationId` | text | client session that created the memory (provenance) |
| `linkedIds` | text[] | ids of memories explicitly linked to this one (bidirectional) |

Object **ID** = a UUID minted by the MCP server at save time. The worker upserts
with that ID, so redelivery/retry is **idempotent** (overwrite, never dup).

**Memory links.** `linkedIds` holds explicit, user/model-created relationships
between memories (distinct from tag or vector similarity). They persist as a
Weaviate property and thread through `memory.Record` → worker upsert, so reindex /
redelivery preserve them. Links are **bidirectional** (both memories list each
other).

Linking is its own **async, retrying NATS operation**, decoupled from indexing.
The `Link`/`Unlink` RPCs (and the `linkTo` field of `memory_save`) publish a
single edge mutation to the `memory.link` subject — `{op: add|remove, a, b}` with
endpoints canonicalized (sorted) and a stable `Nats-Msg-Id` for broker-level
dedup. The worker's `linker` consumer applies it idempotently (add = set-union,
remove = set-difference, written via a Weaviate **merge**/PATCH so it never
re-embeds). This exists because indexing is much slower than a write: an edge can
reference a memory whose index event is still queued. For an `add` where either
endpoint is missing, the consumer **NAKs and retries** (capped backoff, up to
`LinkMaxDeliver: 50`) until both land — out-of-order links are no longer silently
dropped, the failure mode of the old inline link-on-index path. The
read-modify-write is serialised by the per-memory lock described in
[Horizontal scaling](#horizontal-scaling-worker-replicas). See
[WEB_UI.md](WEB_UI.md) for how the graph UI and the MCP tools drive links.

NATS stream `MEMORY`, subjects `memory.>`, file storage. Subject `memory.index`
carries save events and `memory.summary` carries conversation summaries (both on
the `indexer` consumer); `memory.link` carries edge mutations (the `linker`
consumer); `memory.dead` carries dead-letters (same stream). The `indexer` is a
durable consumer, explicit ack, `MaxDeliver: 10`, nak-with-delay (2s) on
transient embed/write failures, term on unparseable payloads. A message that
still fails on its 10th delivery is published to `memory.dead` (with the error
and delivery count) and acked — preserved for inspection/requeue via the CLI,
never silently dropped.

### Trash (soft delete)

Class `Trash` (declared in code like the others, `trashProperties`; field-tokenized
`memoryId`/`namespace`; vectorizer none, no vector; MT with auto-tenant creation iff
`Memory` is). One object per deleted memory, keyed by the memory's own id, holding
`memoryId, text, namespace, tags, source, conversationId, createdAt, linkedIds,
notDuplicateOf` plus `deletedAt` and `reason` (`delete`, `supersedes:<id>`,
`namespace-delete`). `Memory` carries NO deleted flag, so normal queries are untouched.
`TenantStore.DeleteWithReason` (used by `Delete`, the worker's `applySupersedes`, and
`DeleteNamespace`) writes the trash entry FIRST and aborts without deleting anything if
that write fails; chunk cascade is unchanged. `RestoreFromTrash` validates every id
(in trash, not live: NotFound / AlreadyExists, all-or-nothing), republishes the record via
`bus.PublishIndex` (links via `PublishLink` for still-existing endpoints), then drops the
trash entry. `PurgeTrash` is permanent. The server sweeps entries older than
`TRASH_RETENTION` hourly. Not covered: conversation summaries (permanently deleted by a
namespace delete), and backups (trash is excluded). `DeleteClass` (reindex rebuild)
drops only Memory/MemoryChunk/ConversationSummary and leaves the trash alone (no
vectors, dimension irrelevant); `migrate-mt` snapshots the non-MT trash, drops all four
classes (`DeleteAllClasses`), recreates them MT and re-writes the trash entries into the
calling admin's tenant (`PutTrashEntry`, deletedAt/reason preserved; count reported as
`trash_migrated`). The snapshotted trash is first saved to `<backup-name>.trash.json`
beside the migration backup (the backup itself holds memories only); a post-drop write
failure points at it, and `cortex import <file>` re-creates the entries as live memories. `DropTenant` (DeleteUser) deliberately wipes the user's trash with
their data. The sweep skips entries with no valid `deletedAt` (logged). Namespace delete
trashes+deletes memories with 8 workers, stopping at the first error. `TRASH_RETENTION`
is parsed strictly (Go duration, e.g. `720h`; `0` = forever; invalid = startup failure).

## Memory expiry (`validUntil`)

Time-bound facts carry an optional `validUntil` (Weaviate `date`, declared in
`memoryProperties`, added to existing classes by `ensureProps`; also on `Trash`, so a
trash round-trip keeps it). It is part of the memory's content, hence index-owned (see
the field-ownership table); `memory.Record.ValidUntil` zero = never expires.

**Semantics.** `Search`, `List`, `SearchSimilar`, `Consolidate` and recall (the hook
goes through `Search`) exclude expired memories unless `include_expired` is set. The
store options default to excluding (`SearchOpts/ListOpts.IncludeExpired` zero value),
so a new caller is expiry-safe by default.

- **Null filtering.** The filter is `NOT(validUntil <= now)`, not `validUntil > now OR
  validUntil IS NULL`: `IsNull` filtering needs the class created with
  `indexNullState` (not the case for existing deployments, and immutable), whereas NOT
  is a complement over all objects, so memories with no value (never set, or written
  before the property existed) are kept without a backfill and without a "far-future
  sentinel". Pinned by `TestExpiryFilterKeepsPreExistingMemories`.
- **Chunks.** Chunk search resolves every chunk hit to its parent (`ts.Get`) anyway, so
  expiry is checked on the parent there; `validUntil` is NOT copied onto
  `MemoryChunk`. Copying would make an expiry-only edit rewrite every chunk (and a
  missed chunk would resurrect an expired memory); the parent is the single source of
  truth. Cost: expired parents can occupy slots in the `limit x chunkPoolFactor`
  candidate pool (generous; the chunkless fallback uses the server-side filter).
- **Dedup ignores expiry.** The worker's candidate search sets `IncludeExpired`: an
  expired near-duplicate is still a duplicate, and flagging it lets a review merge or
  drop the stale copy.
- **Where expired memories still show.** `ListDuplicateCandidates` deliberately does
  NOT filter expired memories (consistent with dedup ignoring expiry: a stale copy must
  stay reviewable so it can be merged or dropped). `RecallSession` facts (a session's
  memories) DO exclude expired ones. `Consolidate` excludes them for seeds AND
  link/dup-candidate neighbours (fetched by id, so `skipExpired` re-applies the check)
  unless `include_expired`, so an expired memory can't be merged and superseded by
  accident.
- **Expiry-only edits never re-embed.** `UpdateMemory` with an expiry change and
  text/tags/namespace absent or identical to the stored values is applied by
  `TenantStore.SetValidUntil`: a single-property Weaviate merge (zero time = JSON null =
  clear). It takes no distributed lock, deliberately: a merge is atomic per property and
  a scalar replace has no read-modify-write to protect (unlike tag-array edits), so it
  cannot clobber a concurrent link/reinforcement/tag merge (integration-tested). Known
  limit, shared by every index-owned field: an index message already queued before the
  edit carries the old expiry and overwrites it when it lands.
  Any other edit republishes the record carrying the (possibly new) expiry.
- **Sweep.** Hourly in `cmd/server` (beside the trash sweep): `Store.SweepExpired` per
  tenant selects `validUntil < now - EXPIRY_GRACE` (server-side `<`, which excludes null),
  re-reads each candidate right before trashing (an extension made meanwhile wins) and
  uses `DeleteWithReason(..., "expired")` (trash-first, chunk cascade). Listed
  oldest-expiry first in batches of 500 so a backlog drains; ids that fail (or were
  extended) are skipped on later rounds and the window widens by their count, so a
  persistently failing batch cannot starve the healthy memories behind it.
  Counts are logged. Restoring from the trash drops a lapsed expiry.
- **Validation** (server): `valid_until` must be in the future; `expires_in` accepts Go
  durations plus whole `d` days; both must be > 0 and <= 10 years; both at once is
  rejected.

## Tools exposed to Claude

| Tool | Path | Args | Returns |
|------|------|------|---------|
| `memory_save` | NATS publish | `text`, `namespace?`, `tags?` | `{id, status:"queued"}` |
| `memory_search` | Ollama + Weaviate | `query`, `namespace?` (`*` = all), `limit?` | `{hits:[{id,text,namespace,tags,distance}]}` |
| `memory_delete` | Weaviate (soft: copies to `Trash` first) | `id` | `{status:"deleted"}` |
| `trash_restore` | NATS publish (`memory.index`) | `id` | `{status:"queued"}` |
| `memory_link` | NATS publish (`memory.link`) | `id`, `targetId` | `{status:"queued"}` (applied async once both ends indexed) |
| `memory_unlink` | NATS publish (`memory.link`) | `id`, `targetId` | `{status:"queued"}` |

(Plus `session_summarize` / `recall_session` for conversation summaries.) All
tool calls go through the Connect RPC server; the MCP server holds no NATS/
Weaviate/Ollama connection of its own.

Namespace resolution: empty → server default; `*` on search → no filter
(cross-namespace). The server default is detected at launch — `DEFAULT_NAMESPACE`
env if set, else the git origin remote URL of the working directory, else the
directory basename, else `global` — so each project scopes its own memories
automatically without per-call bookkeeping.

## Configuration (env)

| Var | Default | Used by |
|-----|---------|---------|
| `NATS_URL` | `nats://localhost:4222` | both |
| `OLLAMA_URL` | `http://localhost:11434` | both |
| `OLLAMA_MODEL` | `qwen3-embedding:0.6b` | both |
| `WEAVIATE_HOST` | `localhost:8080` | both |
| `SEARCH_ALPHA` | `0.5` | server (hybrid blend: 1=pure vector, 0=pure keyword/BM25) |
| `RERANK_WEIGHT` | `0` (off) | server ("living memory": >0 re-ranks survivors by usage and reinforces hits; value = usage share vs relevance) |
| `RERANK_HALFLIFE_DAYS` | `30` | server (recency half-life for the usage term; only when `RERANK_WEIGHT`>0) |
| `REINFORCE_TOPK` | `1` | server (how many top hits a search reinforces; only when `RERANK_WEIGHT`>0) |
| `DEFAULT_NAMESPACE` | auto (git remote / dir basename) | mcp |
| `MEMORY_SOURCE` | `claude-code` | mcp |
| `CORTEX_AUTH_TOKEN` | _(empty = open)_ | server/clients (static bearer token) |
| `CORTEX_UI_USER` | `admin` | server (web UI login) |
| `CORTEX_UI_PASSWORD` | _(empty = UI login disabled)_ | server |
| `CORTEX_JWT_SECRET` | derived from `CORTEX_AUTH_TOKEN`, else random | server (signs UI JWTs) |
| `TRASH_RETENTION` | `168h` (`0` = forever) | server (how long soft-deleted memories stay restorable; hourly sweep purges older entries across all tenants) |
| `EXPIRY_GRACE` | `168h` (`0` = immediately) | server (how long an expired memory stays hidden-but-extendable before the hourly sweep moves it to the trash; strict Go duration) |
| `CORTEX_MAX_REQUEST_BYTES` | `268435456` (256 MiB) | server (max RPC request body; raise for very large restores) |

## Living memory — decay + reinforcement (opt-in)

A flat vector store ranks a fact the same forever, whether it is recalled daily or
never. "Living memory" makes recall *usage-aware*, modelled on spaced repetition:

- **Reinforcement (write side).** Every memory carries `accessCount` +
  `lastAccessedAt`. When a search returns hits, the server asynchronously bumps
  the top `REINFORCE_TOPK` (default 1) — fire-and-forget, off the search path,
  best-effort (a failed bump only weakens a ranking signal, never loses data),
  and serialised so concurrent searches can't lose an increment. Only the
  **agent's recalls count**: the `SearchRequest.no_reinforce` flag suppresses the
  bump, and the web UI (browse/explore) and the CLI (`cortex search`, unless
  `--reinforce`) set it, so human browsing doesn't inflate the usage signal. The
  MCP client leaves it false. `accessCount`/`lastAccessedAt` are exposed
  read-only in the UI (Memories list + graph node panel).
- **Decay + re-rank (read side).** When `RERANK_WEIGHT`>0, `Search` over-fetches a
  wider candidate pool, applies the relevance cutoff **first** (so `maxDistance`
  semantics and each hit's `Distance` are untouched), then re-orders the
  survivors by `score = (1-w)·relevance + w·usage`, where
  `usage = min(1, recency + 0.1·ln(1+accessCount))` and
  `recency = 2^(-ageDays / RERANK_HALFLIFE_DAYS)` measured from `lastAccessedAt`
  (falling back to `createdAt`). Frequently/recently used facts float up; stale
  ones sink — without ever corrupting the relevance distance clients filter on.

It is **off by default** (`RERANK_WEIGHT=0`, like `DEDUP_DISTANCE`): zero
re-ordering and zero reinforcement writes, behaviour identical to before. The
`accessCount`/`lastAccessedAt` properties are additive (no rebuild) and survive
`reindex` (republished on the index payload) and dump/restore.

## Deliberately deferred (extension points, not missing pieces)

- **LLM extraction + dedup (mem0-style ADD/UPDATE/DELETE/NOOP).** Hook point: the
  worker, before `Upsert`, would search for near-duplicates and a chat model
  would decide the op. Needs a chat model in Ollama.
- **Temporal validity windows (Zep-style).** `createdAt` already present; add
  `validFrom/validTo` + supersede-on-update.
- **SeaweedFS / S3 blobs.** For ingesting documents/files/images: store the raw
  artifact in SeaweedFS, keep vector+metadata+pointer in Weaviate. Skipped in v1
  because memories are short text.
- **Reflection / decay.** Scheduled job to consolidate and age out memories.
- **AuthZ.** Everything is localhost-bound in v1; exposing it needs NATS auth,
  Weaviate keys, and per-namespace authorization.

## Horizontal scaling (worker replicas)

The worker is safe to run as N replicas (`docker compose up -d --scale worker=3`,
or `deploy.replicas: 3`; `WORKER_METRICS_ADDR` is per-container so ports do not clash).
All replicas share the durable `indexer` and `linker` consumers, so JetStream
spreads messages across them; each process also handles `WORKER_CONCURRENCY`
(default 4) messages per consumer at once. Messages for one memory can therefore
be processed concurrently and out of order, and three mechanisms keep that correct:

1. **Per-id distributed lock.** A JetStream KV bucket (`cortex_locks`, 60s
   TTL, file storage so held locks survive a NATS restart) holds one key per
   `tenant + memory id` (summaries: `tenant + conversation id`; keys are always
   `<kind>.<sha256-hex of tenant NUL id>`, so distinct pairs never collide and ids
   never need sanitising). Acquire is
   `kv.Create` (fails if present); release deletes only at the revision we
   created, so a holder whose lock expired cannot delete the next holder's. The
   index write and the link apply both run under it; a link takes BOTH endpoint
   locks in sorted order (no deadlock). Embedding happens before the lock, so it is
   held only for the store round-trips. The critical section's context deadline is
   3/4 of the TTL measured from the FIRST lease taken (the earliest to expire); if
   waiting for a second key would exceed that budget, everything is released and
   acquisition restarts, so a section can never outlive a lease it depends on. The
   TTL only reclaims a crashed holder's lock (no lease refresh).
   Lock-infrastructure errors (bucket or stream gone after a NATS wipe, NATS briefly
   unreachable) are repaired (bucket re-created) and waited out exactly like
   contention, so they never fail a message.
2. **Contention does not consume the delivery budget.** A message that finds its
   id locked polls (jittered) for up to TTL+30s, i.e. past a crashed holder's
   lease, then proceeds. It is never NAKed for contention (every NAK costs one of
   the `MaxDeliver` deliveries) and never dead-lettered or terminated for it. The
   message is kept alive with `InProgress` every 5s for its WHOLE life (lock wait,
   embedding, store writes), and the consumers set `AckWait` explicitly (30s).
   Each process runs `WORKER_CONCURRENCY` loops per consumer, each pulling ONE
   message at a time (`Fetch(1)`), so a replica holds at most that many messages
   and never hoards a backlog its peers could process. Trade-off, head-of-line
   blocking: a wedged hot id (crashed holder, up to the 60s lease) occupies
   the loops of every replica that pulls a message for it until the lease expires;
   other ids still flow on the remaining loops. On shutdown nothing new is pulled,
   in-flight handlers get a 10s grace to finish their commit, and a handler that
   fails only because the worker is stopping hands its message back instead of
   counting a processing failure (redelivery still costs one delivery; JetStream
   has no uncounted hand-back).
3. **Version guard.** Each index/summary message is stamped with its JetStream
   stream sequence, persisted as `indexedSeq` on the Memory / ConversationSummary
   object, together with `indexedStream` (the stream's creation time, unix
   microseconds). Under the lock, a message whose sequence is lower than the stored
   one is acked and skipped (metric outcome `stale`), so edit v1 finishing after
   v2 cannot overwrite it. Sequences restart at 1 if the MEMORY stream is
   recreated, so they are only compared when the stored and current stream
   identities match; if they differ the message is fresh and applies. Objects
   written before these properties (no identity) compare sequences as-is: their
   stream cannot be known, and assuming "same stream" keeps the guard. Reindex,
   restore, import and dead-letter requeue publish new messages, so they get
   higher sequences and still apply. Missing properties read as 0. A redelivery of
   the same sequence re-applies (idempotent). The worker caches the stream identity
   and refreshes it every 30s, and always refreshes it synchronously before
   skipping a message as stale, so a recreated stream is noticed immediately.
   Caveat: a legacy object (`indexedStream` unset) has no identity, so after a
   stream recreation a low-sequence message can be skipped as stale for it until
   a message with a higher sequence rewrites it (which stamps the identity), or it
   is deleted and re-imported.
   Upgrade: a `cortex_locks` bucket left by a pre-release build with memory
   storage must be deleted once (`nats kv del cortex_locks`); JetStream cannot
   change a bucket's storage type.

### Field ownership

Writing the whole object from a queued message would overwrite fields other
writers own with the message's stale snapshot. So the index path **creates** a
memory that does not exist (full record, links included: import/restore/migrate
carry the only copy) but only **merges** (Weaviate PATCH, vector included) the
fields it owns into an existing one. Weaviate merges per property, so merges of
disjoint fields from different processes cannot clobber each other (pinned by an
integration test), which is why server-side merges need no KV lock.

| Field | Owner / writer | Index path on an existing memory |
|-------|----------------|----------------------------------|
| `text`, vector, `namespace`, `tags`, `source`, `createdAt`, `conversationId`, `model`, `dims` | index (worker) | writes |
| `dupCandidates` | index (recomputed; server `DismissDuplicate` may prune) | writes, minus ids in the stored `notDuplicateOf` |
| `indexedSeq` | index (worker) | writes |
| `validUntil` (expiry) | index (worker) **and** server `UpdateMemory` expiry-only change (`SetValidUntil`, single-property merge) | writes (an index message without an expiry CLEARS it) |
| chunks (`MemoryChunk`) | index (worker) | replaced under the lock |
| `linkedIds` | link consumer (`SetLinks`, under both ids' locks) | never writes |
| `accessCount`, `lastAccessedAt` | server `Reinforce` (merge) | never writes |
| `notDuplicateOf` | server `DismissDuplicate` (merge) | never writes |

Known limits: `namespace` is also merged by namespace rename (an edit queued
before a rename can restore the old name); and with several SERVER replicas,
concurrent reinforcements of one memory can lose increments (the server
read-modify-writes `accessCount` without the lock; it is a ranking signal only).

### Missing consumer or stream

The `MEMORY` stream has no retention limit, so every message ever published
stays in it. A durable consumer recreated with the default delivery policy
therefore replays the ENTIRE history: every memory re-embedded (embedding runs
before the lock and stale check), every link op re-run, every summary. The worker
handles the two cases differently:

- **Stream gone or empty** (NATS wiped): nothing to replay, so the pool re-ensures
  the stream and the consumer with the startup configuration and carries on.
- **Consumer gone while the stream holds messages**: NOT recreated. The worker logs
  an Error (throttled), counts `cortex_worker_fetch_errors_total`, and its
  `/readyz` goes unready after 30s so the operator notices. Recreate the consumer
  deliberately with the worker's settings (explicit ack, AckWait 30s, MaxDeliver
  10 for `indexer` / 50 for `linker`, the same filter subjects) and an explicit
  start: new messages only, or `DeliverByStartSequence` at the first sequence that
  still needs processing. The worker then resumes without a restart.

## Multi-tenancy trust model and resource guards

Tenants are isolated for **data** (every store call is scoped to the authenticated
user's Weaviate tenant). They are **not** isolated for resources: there is no
per-tenant storage quota and no per-tenant rate limit yet, so an authenticated
tenant can grow its store arbitrarily. Because "fetch everything" paths are cursor
paged (no row cap), these guards bound what one tenant can do to the shared server:

- **Single-flight per tenant** (`tenantGate`, `internal/rpc/gate.go`): at most one
  concurrent `BackupSelf`, one `Reindex` and one `RestoreSelf` per tenant (separate
  gates per operation). A second concurrent call gets `resource_exhausted`. The key is
  the authenticated `UserID` from the request context, never a request field;
  single-tenant mode uses a constant key.
- **One admin bulk operation server-wide**: `BackupAll`, `RestoreAll` and `MigrateMT`
  share one gate (nothing serialised them before). The scheduled periodic backup is
  not gated.
- **Request size limit**: `connect.WithReadMaxBytes` from `CORTEX_MAX_REQUEST_BYTES`
  (default 256 MiB). `/auth/login` keeps its own much smaller `MaxBytesReader`.

The gates live in process memory: with several server replicas the limits apply
**per replica**, not cluster-wide. Cross-tenant fairness (quotas, rate limits) is
deferred.

## Failure behavior

- Ollama down → `memory_save` still succeeds (queued); worker retries (up to 10×,
  2s apart) until it's back. `memory_search` fails fast with a clear error.
- Weaviate down → same: writes queue and retry; searches fail fast.
- Persistent failure (e.g. wrong/missing model) → after 10 attempts the record is
  dead-lettered to `memory.dead`, not dropped. Inspect with `cortex dead`; fix the
  cause and `cortex dead --requeue` to re-index, or `--purge` to discard.
- Bad payload on the stream → terminated (not redelivered forever).
- Worker crash → durable consumer resumes from last ack on restart.
