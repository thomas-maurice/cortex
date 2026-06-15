---
name: consolidate-memories
description: |
    Memory consolidation pass for the current repository. Gathers every source of
    knowledge about the repo — file auto-memory (MEMORY.md), existing Cortex
    memories, project docs, and the repo itself — reconciles it against current
    reality, splits it into discrete atomic memories, saves them to Cortex, and
    links related ones. Use when the user says "consolidate memories", "rebuild
    my cortex memories", "save what you know about this repo", or wants to refresh
    the persistent memory for a project.
---

# Consolidate repo knowledge into Cortex memories

Goal: take everything you know about the **current repository** and turn it into a
clean, current, deduplicated, linked set of Cortex memories. Cortex is the only
write path — do NOT create or edit file-based `*.md` memories (per global Rule 16).

## 1. Gather knowledge

Read every source of existing knowledge about this repo:

- **File auto-memory** — `MEMORY.md` and any `*.md` files in the harness memory
  directory loaded at session start.
- **Existing Cortex memories** — run `cortex_memory_search` with several *broad*
  queries covering the repo's main components, architecture, decisions, and known
  issues. Search the default namespace (auto-derived from the git origin remote).
  Pass `namespace: "*"` only if you suspect relevant cross-scope notes.
- **Project docs** — `README`, `CLAUDE.md`, `docs/`, ADRs, top-level design notes.
- **The repo itself** — module/package layout, key entrypoints, and `git log` for
  recent direction. Read enough to know what is actually true *now*.

## 2. Reconcile against reality

For every fact, decision, constraint, plan, or piece of context: verify it against
the current code/state. Discard anything stale or superseded. Where the file
auto-memory and reality disagree, **trust reality** and note the correction.

## 3. Split into discrete memories

Produce a set of atomic memories (aim for up to ~12, but let the material decide).
Each must be:

- **Self-contained** — one to a few sentences with who/what/why, not a keyword.
- **Semantically grouped** — by topic (architecture, a subsystem, a decision, an
  external dependency, an ongoing initiative), not chronologically.
- **Single-idea** — don't fragment one idea across many notes; don't blend
  unrelated ideas into one.

Prefer Markdown in the body (headings, bullets, code fences) — the Cortex UI
renders it.

## 4. Dedup against Cortex

Before saving each memory, confirm it isn't already stored (use the searches from
step 1). If an existing memory is close but outdated, save the corrected version
and prefer updating over creating a near-duplicate.

## 5. Save and link

- Save each with `cortex_memory_save` using the **default namespace** and good
  free-form `tags`.
- **Link related memories.** When two memories reference the same subsystem,
  decision, or initiative, capture the relationship:
  - Save foundational memories first so later ones can `linkTo` their IDs.
  - Pass prior memories' IDs (from your step-1 searches, or returned by earlier
    saves in this batch) via the `linkTo` parameter.
  - Use `[[name]]`-style references inside the text where they aid a future reader.

## 6. Report

List what you saved, what you updated/superseded, and what links you created. Be
explicit about anything you discarded as stale, so the user can sanity-check the
reconciliation.
