# Second-Brain / Agent Memory — Research Notes

Compiled 2026-06-11. Sources listed at the bottom. This is the background that
informed the Cortex design in `DESIGN.md`.

## 1. What a "second brain for an LLM" actually is

Two distinct meanings collapse under the term:

1. **Document second brain** — your notes/files (often an Obsidian vault) made
   queryable by the agent. The agent reads/writes Markdown and uses your link
   structure as a knowledge graph. Good for "work with my existing notes."
2. **Agent memory layer** — a service the agent calls via tools (`save`,
   `search`) to persist facts *across sessions*. This is what we are building:
   a self-hosted memory layer behind MCP that any MCP client (Claude, Cursor,
   ChatGPT) can use to store context once and recall it by meaning.

The interesting systems add **active memory management** on top of plain RAG:
scheduled reflection, dedup/consolidation, and recency/decay — the agent
maintains an evolving model of your knowledge rather than a flat bag of chunks.

## 2. The reference systems (and what to steal)

| System | Model | Key idea | Benchmark (LongMemEval) |
|--------|-------|----------|--------------------------|
| **Mem0** | vector-first, graph optional | LLM **extraction** distills each turn into compact facts; then ADD/UPDATE/DELETE/NOOP against existing memories | ~49% |
| **Zep / Graphiti** | temporal knowledge graph | time is first-class; facts have validity windows → answers "what was true last Tuesday" | ~63.8% |
| **Letta (MemGPT)** | OS-style paging | tiered memory, agent decides what to page in/out | — |
| **Cognee** | ECL pipeline | extract-cognify-load into a graph | — |

Takeaways for us:
- **Extraction is the highest-leverage feature** but it needs a chat LLM and a
  compare-against-existing step. *Deferred for v1* — in our setup Claude itself
  is the extractor (it decides what's worth saving), so we store verbatim.
- **Temporal reasoning** (Zep's edge) matters once you have updates/corrections.
  We keep `createdAt` now so we can add validity windows later without a
  migration.
- The ADD/UPDATE/DELETE/NOOP loop is the natural v2: before writing, search for
  near-duplicates and decide whether to merge.

## 3. Embeddings: local via Ollama

- `nomic-embed-text` is the default local text embedder: 768-dim, strong
  quality/size ratio, runs on CPU. Pull with `ollama pull nomic-embed-text`.
- Ollama exposes `POST /api/embeddings {"model","prompt"}` → `{"embedding":[…]}`
  (single) and a newer `/api/embed` (batch). We use the stable single endpoint.
- **Design choice:** we embed *ourselves* in the worker and store vectors into
  Weaviate with `vectorizer: none`, rather than letting Weaviate's
  `text2vec-ollama` module call Ollama. This keeps embedding logic in our Go
  code (one place, swappable model) and keeps Weaviate a dumb vector store.

## 4. Vector store: Weaviate

- Local single-node via Docker; anonymous access; `DEFAULT_VECTORIZER_MODULE=none`.
- Go client `v5`: builder pattern.
  - schema: `client.Schema().ClassCreator().WithClass(&models.Class{Vectorizer:"none", …})`
  - write: `client.Data().Creator().WithClassName().WithID().WithProperties().WithVector().Do()`
    — using our own ID makes re-indexing idempotent (overwrite).
  - search: `client.GraphQL().Get().WithNearVector(NearVectorArgBuilder().WithVector(v)).WithWhere(filter).WithLimit(n)`
  - `_additional { id distance }` returns the match id and cosine distance.
- A `where` filter on `namespace` scopes search; omit it to go cross-namespace.

## 5. Transport: NATS JetStream

- New `jetstream` package (`jetstream.New(nc)`), not the legacy `nc.JetStream()`.
- Stream: `js.CreateOrUpdateStream(ctx, StreamConfig{Subjects:["memory.>"], Storage:File})`.
- Durable consumer: `js.CreateOrUpdateConsumer(ctx, stream, ConsumerConfig{Durable, AckPolicy:Explicit, FilterSubject, MaxDeliver})`.
- Push-style delivery via `consumer.Consume(func(msg){...})`; ack/nak/term per msg.
- **Why JetStream for writes:** durability + burst absorption. A flood of
  `memory_save` calls queues on disk and drains at Ollama's pace; nothing is lost
  if the embedder or Weaviate is momentarily down (redelivery up to MaxDeliver).
- **Why NOT for search:** a search is a synchronous read — the caller blocks for
  the answer, so a queue buffers nothing. We query Weaviate directly instead
  (see DESIGN.md, "Option B").

## 6. MCP server: official Go SDK

- `github.com/modelcontextprotocol/go-sdk` (v1.x, maintained with Google).
- `server := mcp.NewServer(&mcp.Implementation{Name,Version}, nil)`
- `mcp.AddTool(server, &mcp.Tool{Name,Description}, handler)` — input/output JSON
  schemas are **inferred from Go structs**; `jsonschema:"…"` tags become
  property descriptions the model sees.
- handler signature: `func(ctx, *mcp.CallToolRequest, In) (*mcp.CallToolResult, Out, error)`.
- `server.Run(ctx, &mcp.StdioTransport{})` — stdio is what Claude Code execs.

## 7. Security notes (taken seriously, not paranoia)

- 2025–26 saw real MCP CVEs incl. an RCE in `mcp-remote` (CVE-2025-6514) and
  audits finding command-injection in a large fraction of community MCP servers.
- Our exposure is low by construction: the server takes typed structured args
  (no shell), runs locally, and talks only to localhost NATS/Weaviate/Ollama.
  No network auth surface in v1 because everything is bound to localhost.
- If this is ever exposed beyond localhost: add NATS auth, Weaviate API keys,
  and namespace authorization. Tracked as a follow-up in DESIGN.md.

## Sources

- [MindStudio — AI Second Brain with Claude Code + Obsidian](https://www.mindstudio.ai/blog/ai-second-brain-claude-code-obsidian-architecture)
- [Obsidian + Claude Code persistent memory](https://pasqualepillitteri.it/en/news/962/obsidian-claude-code-second-brain-persistent-memory)
- [Vectorize — Mem0 vs Zep (Graphiti)](https://vectorize.io/articles/mem0-vs-zep)
- [Particula — Mem0 vs Zep vs Letta vs Cognee](https://particula.tech/blog/agent-memory-frameworks-tested-mem0-zep-letta-cognee-2026)
- [Dev.to — AI Agent Memory in 2026](https://dev.to/agdex_ai/ai-agent-memory-in-2026-mem0-vs-zep-vs-letta-vs-cognee-a-practical-guide-cfa)
- [Weaviate docs — Ollama embeddings](https://docs.weaviate.io/weaviate/model-providers/ollama/embeddings)
- [Weaviate blog — Local RAG with Ollama + Weaviate](https://weaviate.io/blog/local-rag-with-ollama-and-weaviate)
- [Ollama — nomic-embed-text](https://ollama.com/library/nomic-embed-text)
- [NATS docs — JetStream consumers](https://docs.nats.io/nats-concepts/jetstream/consumers)
- [nats.go jetstream README](https://github.com/nats-io/nats.go/blob/main/jetstream/README.md)
- [modelcontextprotocol/go-sdk](https://github.com/modelcontextprotocol/go-sdk)
