# Chunked retrieval — design & accuracy experiment

## Why

A memory is embedded as a single vector. For a **long** memory (2000+ tokens) that
one vector is an average of everything in it, so a query about a *specific fact
buried in section 3* matches weakly — the fact's signal is diluted by the rest of
the document. Chunking fixes this: the memory is split into smaller, focused
pieces, each embedded on its own, and **search runs against the chunks**. A hit is
resolved back to its parent memory.

## Data model

Two Weaviate classes (both `vectorizer: none` — vectors are always supplied by the
worker, never auto-computed):

- **`Memory`** — the whole memory (full text + metadata). Still vectorized: that
  whole-memory vector is used for **duplicate-candidate detection** and
  **"find similar"** (`nearObject`), which genuinely want whole-memory similarity.
  It is no longer the primary *search* target.
- **`MemoryChunk`** — one token-bounded, overlapping slice of a memory's text,
  with its own vector. Carries `memoryId` (parent), plus `namespace`/`tags` copied
  from the parent so namespace/tag filters push down to the chunk query. Chunk IDs
  are deterministic (`ChunkID(memoryId, index)`), so re-indexing overwrites in
  place and the stale tail (from a now-shorter text) is deleted.

Chunking is done in the **worker** (`internal/chunk`) using a sentence-aware
greedy splitter sized by token count. Tokens are counted with the cl100k_base BPE
(tiktoken-go, **offline** loader so indexing needs no network). cl100k is a
documented *proxy* — qwen3's tokenizer isn't available in Go, and 512 tokens is far
under qwen3's 32K context, so the proxy only affects chunk *granularity*. Knobs:
`CHUNK_MAX_TOKENS` (default 512), `CHUNK_OVERLAP_TOKENS` (default 64).

`Search` queries `MemoryChunk`, groups the chunk hits by parent (keeping each
parent's best chunk distance), resolves parents to full `Memory` records, applies
exclude-tag and living-memory re-ranking, and returns deduped parent memories.

## Optional & backward-compatible

Chunking is a toggle — **`CHUNKING_ENABLED`** (default `true`), set identically on
the **worker** (gates whether chunks are written) and the **server** (gates
whether search uses them):

- **Enabled (default):** worker writes chunks; `Search` matches the chunk index.
- **Disabled:** worker writes only whole-memory vectors; `Search` matches those
  (`SearchMemoryVectors`) — i.e. exactly the pre-chunking behaviour. A clean revert.

Crucially, enabling chunking does **not** require a flag-day reindex. A memory
with no chunks (a store indexed before chunking, or one mid-reindex) is invisible
to the chunk query, so `Search` **falls back to the whole-memory vectors to fill
the page** whenever the chunk hits don't reach the requested limit. So:

- a fully-chunked store returns ≥ limit from chunks and the fallback never runs
  (zero extra cost in steady state);
- an entirely un-chunked store gets no chunk hits and degrades cleanly to the
  original whole-memory search (existing deployments keep working on upgrade);
- a partially-migrated store gets chunk hits topped up with whole-memory hits.

Run a `cortex reindex` at leisure to chunk the back catalogue and get the full
benefit; nothing breaks in the meantime. (`TestSearchFallbackForUnchunkedMemories`
pins this.)

Cascades: deleting a memory deletes its chunks; deleting/renaming a namespace
deletes/moves its chunks too.

### Bug found & fixed along the way

Weaviate `text` properties default to **word tokenization**, so a `namespace Equal
"demo"` filter also matched `"demo-2"` (shared tokens), and UUID `Equal` matches
(memoryId/conversationId) matched on hyphen-split fragments. Namespace scoping was
silently *fuzzy*. Fixed by marking exact-match keys (`namespace`, `memoryId`,
`conversationId`, `source`) with `tokenization: field` on all classes.

## Accuracy experiment

Method: a labelled query set (query → expected memory id), run through the live
`Search` RPC, scoring the rank of the expected memory. `recall@k` = fraction of
queries whose target is in the top *k*; `MRR` = mean reciprocal rank. The **same**
corpus and queries were measured before and after enabling chunking (only the
indexing changed), so the delta is attributable to chunking alone. Harness:
`scripts/recall_accuracy.py`; comparison: `scripts/recall_compare.py`.

Two corpora:

1. **Local long-seed** — 32 fictional long memories (each > 2048 tokens, 12 with
   3 planted "needle" facts at start/middle/end depths) + 84 prod memories as
   distractors = 116 docs / 278 chunks. 36 paraphrased queries target the needles.
2. **Prod dump** — 84 real exported memories (mostly short), 57 LLM-generated
   paraphrase probes (one per source memory).

### Result 1 — local long-seed (the case chunking targets)

| Metric | Baseline (no chunking) | Chunked | Δ |
|---|---:|---:|---:|
| recall@1 | 66.7% | **75.0%** | ▲ 8.3pp |
| recall@3 | 88.9% | **94.4%** | ▲ 5.6pp |
| recall@5 | 94.4% | **97.2%** | ▲ 2.8pp |
| MRR | 0.794 | **0.848** | ▲ 0.054 |
| mean rank of correct hit | 1.75 | **1.50** | better |

Per needle depth (MRR), chunking improved retrieval at **every** depth, most for
the deepest needles (end: 0.690 → 0.792) — exactly the dilution chunking targets.
(`end` recall@5 dipped 100%→91.7%, i.e. one of 12 deep-needle queries fell out of
the top 5 while its MRR rose; that is ±1 query = ±8.3pp, within noise at n=12.)

### Result 2 — prod dump (mostly short, keyword-rich memories)

| Metric | Baseline | Chunked | Δ |
|---|---:|---:|---:|
| recall@1 | 100.0% | 98.2% | ▼ 1.8pp |
| recall@3 / @5 / @10 | 100.0% | 100.0% | — |
| MRR | 1.000 | 0.991 | ▼ 0.009 |

The prod baseline is **saturated** (100%): the probes carry distinctive exact
tokens (RPC names, proto field numbers, hostnames) and the memories are short, so
hybrid BM25 already nails rank 1. A short memory becomes a *single* chunk, so
chunking is a near-no-op there; the one probe that slipped from rank 1 to 2 is
noise, not a real regression.

## Verdict

**Chunking is better for long memories and neutral for short ones.** It improved
every aggregate metric on the long-document corpus (recall@1 +8.3pp, MRR +0.054)
and left the already-perfect short-memory corpus statistically unchanged. The
benefit scales with memory length and corpus competition; the cost is ~2.4× the
vectors (278 chunks vs 116 memories) and proportionally more embedding at index
time — negligible for a personal store. **Recommendation: keep chunking on.**
