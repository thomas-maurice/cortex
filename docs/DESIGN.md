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

## Tools exposed to Claude

| Tool | Path | Args | Returns |
|------|------|------|---------|
| `memory_save` | NATS publish | `text`, `namespace?`, `tags?` | `{id, status:"queued"}` |
| `memory_search` | Ollama + Weaviate | `query`, `namespace?` (`*` = all), `limit?` | `{hits:[{id,text,namespace,tags,distance}]}` |
| `memory_delete` | Weaviate | `id` | `{status:"deleted"}` |
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
