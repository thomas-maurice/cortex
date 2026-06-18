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
dropped, the failure mode of the old inline link-on-index path. A single worker
serializes the read-modify-write with a mutex to avoid lost updates. See
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

## Failure behavior

- Ollama down → `memory_save` still succeeds (queued); worker retries (up to 10×,
  2s apart) until it's back. `memory_search` fails fast with a clear error.
- Weaviate down → same: writes queue and retry; searches fail fast.
- Persistent failure (e.g. wrong/missing model) → after 10 attempts the record is
  dead-lettered to `memory.dead`, not dropped. Inspect with `cortex dead`; fix the
  cause and `cortex dead --requeue` to re-index, or `--purge` to discard.
- Bad payload on the stream → terminated (not redelivered forever).
- Worker crash → durable consumer resumes from last ack on restart.
