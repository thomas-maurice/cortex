# Embedding model evaluation (Ollama)

The embedding model is the single biggest quality lever in Cortex: it decides
how well "search by meaning" actually works. This doc evaluates the realistic
Ollama options and explains how to switch.

Cortex embeds **in the worker** (`internal/embed`) via Ollama's `/api/embeddings`
and stores raw vectors in Weaviate (`vectorizer: none`). So "the model" is just
the `OLLAMA_MODEL` env var — swapping it is a one-line change **plus a re-index**
(see the gotcha at the bottom).

## The contenders (2026)

| Model | Dims | Context | Size | Score | Best for |
|-------|-----:|--------:|-----:|-------|----------|
| **nomic-embed-text** | 768 | 8192¹ | 274 MB | 62.3 MTEB-en | Leanest — small, fast, English short notes |
| **qwen3-embedding:0.6b** | 1024 | 32K | 639 MB | 64.3 MTEB-multi | **CPU default** — best quality-per-MB; multilingual + long context |
| mxbai-embed-large | 1024 | 512 | 670 MB | 64.7 MTEB-en | English-only, short chunks, accuracy-first |
| embeddinggemma | 768 | 2K | 622 MB | 68.8 code / 61.2 multi | Code-heavy notes, on-device |
| bge-m3 | 1024 (+sparse) | 8192 | 1.2 GB | n/a (hybrid) | 100+ languages, long docs, hybrid search |
| snowflake-arctic-embed2 | 1024 | 8192 | 1.2 GB | 55.6 BEIR | Multilingual, high throughput |
| qwen3-embedding:4b / 8b | 2560 / 4096 | 40K | 2.5 / 4.7 GB | 69.5 / 70.6 multi | Top quality — **needs a GPU** |
| all-minilm | 384 | 256 | 46 MB | n/a | Prototyping / edge only |

¹ Native 8192, but Ollama's card caps at 2048 unless you pass `num_ctx`.

Scores are not perfectly comparable (different MTEB tracks: English vs
multilingual vs BEIR retrieval), so treat them as a rough tier, not a ranking.

## Recommendation for Cortex

**`qwen3-embedding:0.6b` is the default.** It is the standout middle option:
higher score than nomic, **multilingual** (matters — you write French), 32K
context, and still sub-1GB (639 MB, 1024-dim) so it stays CPU-viable on a Mac.
It is what ships and what is running now.

**Drop to `nomic-embed-text` if you want a leaner footprint.** For a second brain
of short English notes on a constrained machine, 274 MB / 768-dim is the frugal
trade and effectively indistinguishable on short text. Switching down is the same
re-index dance as switching up (it changes vector dimensions).

**Go to `bge-m3` only when you start ingesting long, multilingual documents** —
8K context and hybrid dense+sparse retrieval are wasted on one-line memories but
shine on PDFs/articles. It costs 1.2 GB and is slower.

**Avoid for this use case:** `mxbai-embed-large` (512-token context is too short
once a memory is a paragraph, and English-only), the big `qwen3` 4b/8b variants
(need a GPU), and `all-minilm` (quality too low for real recall).

Decision tree:

```
Multilingual notes, better recall, still CPU     → qwen3-embedding:0.6b (default)
Short English notes, leanest footprint           → nomic-embed-text
Long documents, many languages, hybrid search    → bge-m3
Code snippets dominate                           → embeddinggemma
Have a GPU and want max quality                  → qwen3-embedding:4b/8b
```

## How to switch models

1. Pull the new model into the running Ollama container:
   ```bash
   docker compose exec ollama ollama pull qwen3-embedding:0.6b
   ```
2. Point both processes at it. Set `OLLAMA_MODEL` in **two places**:
   - `docker-compose.yml` → `worker.environment.OLLAMA_MODEL`
   - `.mcp.json` → `mcpServers.cortex.env.OLLAMA_MODEL`
3. **Re-index (mandatory — see gotcha).** Then `make build && docker compose up -d worker`.

## ⚠️ Gotcha: dimensions are fixed per Weaviate class

Different models emit different-size vectors (768 vs 1024 vs 2560 …). A Weaviate
class locks its vector dimension on first write. You **cannot** mix models in one
class — search math breaks or Weaviate rejects the insert.

So when you change models you must re-index. For a POC the simplest path is:

```bash
make nuke          # wipes all volumes (you lose stored memories)
make bootstrap     # fresh stack
docker compose exec ollama ollama pull <new-model>
# update OLLAMA_MODEL in compose + .mcp.json, then re-save your memories
```

A non-destructive migration (read all memories, re-embed with the new model,
write to a new class, swap) is a worthwhile future feature but is not built yet.
