---
name: cortex-dev
description: |
    How to work on THIS repository (Cortex — the self-hosted second-brain memory
    server: Go + NATS + Weaviate + Ollama, with an embedded Vue UI and Connect
    RPC). Covers the golden rules (never commit/push without asking; rebuild +
    restart the dev stack when a feature is done), how to spin up / build / test
    the dev stack, the architecture in one screen, the full `cortex` CLI surface
    so you don't re-explore it each time, and how to seed the dev stack with fake
    memories. Read this BEFORE starting work on the cortex repo. Keep it (and the
    README/docs) updated when you learn something that changes how work is done.
---

# Working on Cortex

Cortex is a self-hosted "second brain" memory store for Claude. One Connect-RPC
**server** owns all backend access; the **MCP server** and the **`cortex` CLI**
are thin clients of it; a **worker** is the only process that writes vectors.

## 0. Golden rules (do not violate)

1. **Never `git commit` or `git push` unless Thomas explicitly says so.** Do the
   work in the working tree, report what changed, then ask "want me to commit?".
   If on `master`, branch first when he does ask. This is a hard rule.
2. **When a feature/change is done, rebuild + restart the local dev stack** so
   Thomas can see it: `make up` (rebuilds the image, then `docker compose up -d`).
   The UI is server-embedded, so UI changes only appear after the image rebuild.
   This is the *development* stack — it does **not** deploy to prod (TrueNAS).
3. **Push back on weak designs.** Thomas wants a real second opinion, not
   agreement. Surface better options; flag when his (or your earlier) approach is
   wrong. Match existing conventions even if you'd choose differently (Rule 11).

## 1. The dev stack (docker compose)

`docker-compose.yml` runs five services. Host ports are deliberately offset to
avoid colliding with other local services:

| Service | Host port | Notes |
|---------|-----------|-------|
| server | **8088** → 8080 | Connect RPC + embedded UI. **Auth is OFF in dev** (no `CORTEX_AUTH_TOKEN`), so the CLI/curl need no token. UI login defaults to `admin` / `admin`. |
| weaviate | 8081 (REST), 50051 (gRPC) | The live store — **has real data**. |
| nats | 4223 (client), 8223 (mon) | JetStream; the index queue. |
| ollama | 11434 | Embeddings. Model `qwen3-embedding:0.6b` (1024-dim). |
| worker | — | NATS → Ollama → Weaviate. The only writer. |

Common commands (run from the repo root):

```bash
make up        # rebuild image + docker compose up -d   ← the "feature done" step
make down      # stop the stack (keeps volumes/data)
make nuke      # stop AND delete all data volumes (destructive)
make logs      # tail worker + server logs
make model     # pull the embedding model into the running ollama container
docker compose ps
```

Quick health / smoke checks against the running server:

```bash
curl -s http://localhost:8088/cortex.v1.MemoryService/Status      -d '{}' -H 'content-type: application/json'
curl -s http://localhost:8088/cortex.v1.MemoryService/IndexQueue  -d '{}' -H 'content-type: application/json'
```

UI: open <http://localhost:8088/>.

## 2. Building & code generation

```bash
go build ./...     # fast compile check of all Go
make build         # builds ui/dist + all 4 binaries into ./bin
                   #   (cortex-server, cortex-mcp, cortex-worker, cortex)
make image         # build the docker image only (no restart)
```

After editing the proto (`proto/cortex/v1/cortex.proto`):

```bash
make proto         # = buf generate            → regenerate Go (gen/)
make proto-ui      # = buf generate --template buf.gen.ui.yaml → regenerate UI TS client
```

Both `make proto` and `make proto-ui` must be run **from the repo root** (the
`buf.gen.*.yaml` files live there; `make proto-ui` needs `ui/node_modules`, i.e.
`cd ui && npm install` once). New RPCs land in both
`gen/cortex/v1/cortexv1connect/` (Go) and `ui/src/gen/cortex/v1/` (TS).

UI only: `cd ui && npm run build` (prod build) or `npm run dev` (hot reload on
:5173, proxying RPC to :8080 — point that at a running `go run ./cmd/server`).

## 3. Tests

```bash
go test ./...      # unit tests; fast, no external services
```

Some store tests are **integration tests against a real Weaviate**, gated on env
so `go test ./...` skips them in CI:

```bash
# Spin up a THROWAWAY Weaviate (never point these at the dev stack — the tests
# call DeleteClass, which WIPES all memories):
docker run -d --name wv-test -p 8085:8080 -p 50055:50051 \
  -e AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
  -e PERSISTENCE_DATA_PATH=/var/lib/weaviate \
  -e DEFAULT_VECTORIZER_MODULE=none -e ENABLE_MODULES="" \
  cr.weaviate.io/semitechnologies/weaviate:1.38.0
CORTEX_TEST_WEAVIATE_REST=localhost:8085 CORTEX_TEST_WEAVIATE_GRPC=localhost:50055 \
  go test ./internal/store -v
docker rm -f wv-test
```

Test conventions: testify (`require` for setup, `assert` for checks). Tests
should encode *why* the behaviour matters, not just what it does (Rule 9).

## 4. Architecture in one screen

- **Server owns everything.** `cmd/server` (impl in `internal/rpc`) is the single
  owner of NATS + Weaviate + Ollama. The MCP server (`cmd/mcp`) and CLI
  (`cmd/cli`) hold no backend connections — they are Connect RPC clients.
- **Writes are async & durable.** `Save`/`UpdateMemory`/`import` publish onto a
  NATS JetStream queue; the **worker** (`cmd/worker`, the only writer) embeds the
  text via Ollama and upserts into Weaviate. Failures retry, then dead-letter.
- **Only `text` is embedded.** Every other field (namespace, tags, source,
  conversationId, links…) is filterable metadata, never part of the vector. This
  is a load-bearing invariant (pinned by `TestMemoryClassVectorizerNone`).
- **Metadata-only edits skip the worker.** Changing a field that isn't embedded
  (links, namespace rename, reinforcement, dedup decisions) is a direct Weaviate
  **merge/PATCH** from the server — no re-embed, no NATS round-trip. Re-embedding
  is only for `text` changes.
- **Weaviate access is gRPC, not GraphQL** (`client.Experimental().Search()`).
  Do not reintroduce `client.GraphQL().Get()`.
- **Idempotent ids.** Object id = a UUID; the worker upserts by id, so
  redelivery/retry/reindex overwrites rather than duplicates.
- Two Weaviate classes: `Memory` and `ConversationSummary` (one per
  conversation, upserted). A namespace conceptually spans **both** — anything that
  renames/deletes a namespace must touch both classes or it orphans summaries.

Package map: `internal/rpc` (service + auth/JWT/login + client), `internal/store`
(Weaviate schema/queries), `internal/bus` (NATS), `internal/embed` (Ollama),
`internal/memory` (shared model + stream/class names), `internal/config` (viper).
UI in `ui/src/views/*.vue` wired through `ui/src/router/index.js` + the navbar in
`ui/src/App.vue`; RPC client in `ui/src/utils/connect.js`.

## 5. The `cortex` CLI (so you don't re-explore it)

`./bin/cortex` (after `make build`) is a thin RPC client — handy for inspection
and scripting without going through Claude. Global flags: `--server`
(`CORTEX_SERVER_URL`), `--token` (`CORTEX_AUTH_TOKEN`), `--namespace-default`,
`--source`, `--conversation`, `--config`.

> ⚠️ **ALWAYS pass `--server http://localhost:8088` for the dev stack.** The
> default config at `~/.config/cortex/cortex.yaml` points the bare `cortex`
> command at Thomas's **production** server (`https://cortex.lil.maurice.fr`) with
> his real token. Config precedence is **flag > env var > config file > default**,
> so an explicit `--server` overrides the config file and is the only thing that
> guarantees you hit the local stack. A bare `cortex import …` / `save` / `delete`
> / `reindex` would run against PROD. Verify with `cortex --server
> http://localhost:8088 config show` (the `server:` line must read localhost). Dev
> needs no token (auth off); the prod token from the config is harmlessly ignored.

| Command | What it does |
|---------|--------------|
| `save "<text>" [-n ns] [-t tag]… [-L <id>]… [-S <id>]…` | Queue a memory. `-L/--link-to` links to existing ids (applied after indexing); `-S/--supersedes` deletes the ids it replaces once indexed. |
| `edit <id> "<text>" [-n ns] [-t tag]… [--replace-tags]` | Replace text (re-embeds), keeping id/links/history. Tags kept unless `-t` given; namespace kept unless `-n`. |
| `list [-n '*'] [-l N] [-t tag] [-T anyTag] [-x excludeTag]` | List newest-first with tag/namespace filters. |
| `search "<q>" [-n '*'] [-l N] [-d 0.6] [--autocut N] [-t/-T/-x tag] [--reinforce]` | Hybrid (BM25+vector) semantic search. `-d` = distance cutoff. CLI does NOT reinforce by default. |
| `delete <id>` | Delete one memory by id. |
| `export [-n '*'] [-o file]` | Dump memories (text+metadata, no vectors) to JSON (stdout by default). |
| `import <file> [--batch N]` | Restore a dump via the NATS ingest queue (worker re-embeds). Preserves ids/links. **This is how you load seed data** — point `--server` at the target. |
| `reindex [-n '*'] [--yes]` | Server snapshots then republishes all memories for re-embed. `--yes` allows a destructive class rebuild on an embedding-dimension change. |
| `dead [--requeue \| --purge]` | List dead-lettered (failed-to-index) memories; requeue or purge them. |
| `status` | Server health + store size (nats/weaviate/ollama/model/count). |
| `doctor` | Per-check diagnostics. |
| `summarize "<text>" --conversation <id> [-n ns]` | Save/update a conversation summary (one per conversation). |
| `summaries [-n '*'] [-l N]` | List conversation summaries, newest-updated first. |
| `recall "<q>" [-n '*'] [--fact-limit N] [-d D]` | Recall the best-matching past session: its summary + facts. |
| `candidates [-n '*'] [-l N]` | List memories flagged as likely duplicates. `candidates dismiss <id> <target-id>` marks a pair NOT duplicates. |
| `consolidate "<topic>" [-n '*'] [-l N] [-d D] [-t/-T/-x tag]` | Print the cluster of memories about a topic + manifest (read-only gather; the LLM merges). No tag flag = whole cluster, not "untagged only". |
| `config init [--force]` / `config show` | Scaffold / print the effective `cortex.yaml`. |
| `hash-password` | Print an argon2id hash for `CORTEX_UI_PASSWORD`. |

There is **no** namespace-management CLI command — rename/delete a namespace is a
UI feature (the **Namespaces** view), backed by the `ListNamespaces` /
`RenameNamespace` / `DeleteNamespace` RPCs.

## 6. Seeding the dev stack with fake memories

`testdata/seed-memories.json` is a ready-made set of fake `demo-*` memories
(with symmetric links) in the `cortex export`/`import` format. Load it:

```bash
make build
./bin/cortex --server http://localhost:8088 import testdata/seed-memories.json
```

The `--server http://localhost:8088` is **mandatory** here — without it `import`
hits PROD (see the warning in §5). Watch the **Indexing** tab (auto-refreshes) or
`cortex --server http://localhost:8088 status` as the count climbs. To reset:
delete the `demo-*` namespaces from the **Namespaces** UI view.
Format details and editing rules (UUID ids, symmetric `linkedIds`, no vectors)
are in `testdata/README.md`.

## 7. Keep this skill (and the docs) current

When you learn something about this repo that **changes how work is done** — a new
build/test step, a new RPC or UI view, a changed port, a gotcha, a convention —
update this file in the same change. Also update the user-facing docs when the
change is user-visible:

- `README.md` — features, CLI table, web-UI views, project layout, quickstart.
- `docs/WEB_UI.md` — UI views table, routes, file map, dev workflow.
- `docs/DESIGN.md` / `docs/EMBEDDING_MODELS.md` — architecture/model decisions.

Prefer correcting the existing section over appending. Don't let this skill drift
from reality — a stale skill is worse than none, because it's trusted.
