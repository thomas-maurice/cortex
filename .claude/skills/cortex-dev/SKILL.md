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

1. **Never `git commit` or `git push` unless Thomas explicitly says so — and
   never ask/offer to commit either (he said the answer is always no).** Do the
   work in the working tree, report what changed, stop. He triggers commits
   himself. If on `master`, branch first when he does ask. This is a hard rule.
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
| server | **8088** → 8080 | Connect RPC + embedded UI. **Auth is ON in dev** (multi-tenant default): RPC calls need a bearer token (JWT from `/auth/login`, or an API key). Compose defaults the UI login to `admin` / `admin`, but the persisted bootstrap admin's password may differ — as of 2026-07-03 `admin`/`admin` is rejected on the local stack (see §1b). |
| weaviate | 8081 (REST), 50051 (gRPC) | The live store — **has real data**. |
| nats | 4223 (client), 8223 (mon) | JetStream; the index queue. |
| ollama | 11434 | Embeddings via batched `/api/embed` (Ollama >= 0.3.4; `embed.Client.EmbedBatch`, 32/request). Model `qwen3-embedding:0.6b` (1024-dim). |
| worker | — | NATS → Ollama → Weaviate. The only writer process type, safe as N replicas (`docker compose up -d --scale worker=N`, see §4). Ops HTTP on `:9090` inside the container (`WORKER_METRICS_ADDR`; not published). |

Common commands (run from the repo root):

```bash
make up        # rebuild image + docker compose up -d   ← the "feature done" step
make down      # stop the stack (keeps volumes/data)
make nuke      # stop AND delete all data volumes (destructive)
make logs      # tail worker + server logs
make model     # pull the embedding model into the running ollama container
docker compose ps
```

The server also serves unauthenticated `GET /healthz`, `/readyz` (503 if NATS/Weaviate/Ollama
is down; body is ok/fail per check only) on the API port. `/metrics` (Prometheus) is NEVER on the API port: it is on a
dedicated listener `API_METRICS_ADDR` (default `:9090`, empty disables; also serves
`/healthz` `/readyz`; not published in dev compose). `/readyz` is cached ~1s — `curl http://localhost:8088/readyz` needs no token. The compose server
healthcheck runs `cortex-server healthcheck` (distroless image: no wget/curl).

Quick health / smoke checks against the running server (auth is on, so mint a
JWT first — an `unauthenticated` reply still proves the server is up):

```bash
TOKEN=$(curl -s http://localhost:8088/auth/login \
  -d '{"username":"admin","password":"<dev admin password>"}' \
  -H 'content-type: application/json' | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])')
curl -s http://localhost:8088/cortex.v1.MemoryService/Status     -d '{}' -H 'content-type: application/json' -H "Authorization: Bearer $TOKEN"
curl -s http://localhost:8088/cortex.v1.MemoryService/IndexQueue -d '{}' -H 'content-type: application/json' -H "Authorization: Bearer $TOKEN"
```

### 1b. Dev-stack auth gotcha (2026-07-03)

`.env` at the repo root sets only `CORTEX_JWT_SECRET` + rerank tuning — no UI
creds and no `CORTEX_AUTH_TOKEN` — so compose defaults the login to
`admin`/`admin`. In practice `admin`/`admin` is REJECTED on the persisted dev
store (the bootstrap admin's password was changed via the users UI, or predates
the argon2/security-hardening commits). Ask Thomas for the dev admin password,
or break-glass with `cortex users reset-password` using an admin API key.

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
`cd ui && npm ci` once). New RPCs land in both
`gen/cortex/v1/cortexv1connect/` (Go) and `ui/src/gen/cortex/v1/cortex_pb.ts` (TS;
protobuf-es v2 emits messages AND the service descriptor from `protoc-gen-es` — no
`protoc-gen-connect-es`/`*_connect.ts`; the client is `createClient` in `ui/src/utils/connect.js`).
UI gotcha: v2 Timestamps are plain objects — use `timestampDate`/`timestampFromDate` from
`@bufbuild/protobuf/wkt`, not `.toDate()`. Type-check TS/gen with `cd ui && npx tsc --noEmit -p .`.

UI only: `cd ui && npm run build` (prod build) or `npm run dev` (hot reload on
:5173, proxying RPC to :8080 — point that at a running `go run ./cmd/server`).

## 3. Tests

```bash
go test ./...      # unit tests; no external services (worker/dlock tests start an
                   # embedded NATS+JetStream via internal/natstest; ~30s with -race)
go test -race ./cmd/worker ./internal/dlock   # run these with -race when touching locking
golangci-lint run  # CI runs this (v2 config in .golangci.yml, errcheck included);
                   # go vet alone is NOT enough — run it before declaring Go work done
```

Some store tests are **integration tests against a real Weaviate**, gated on env
so `go test ./...` skips them in CI:

Use ONE throwaway Weaviate **per run** — never share one between concurrent runs
or agents. Every integration test uses the global class names `Memory` /
`MemoryChunk` / `ConversationSummary` and calls `DeleteClass`+`EnsureSchema` in
setup, so two test processes on one Weaviate delete each other's classes mid-test
(shows up as a bare 10s "did not become queryable" timeout or 422/500). Also never
point them at the dev stack. Run the non-MT and MT (`CORTEX_TEST_MULTI_TENANT=1`)
suites sequentially or on separate containers, and use `-p 1` when running the
store and rpc integration packages together.

```bash
scripts/wv-test.sh run ./internal/store                                 # non-MT
CORTEX_TEST_MULTI_TENANT=1 scripts/wv-test.sh run -p 1 ./internal/store ./internal/rpc   # MT (rpc integration test is MT-only)
# manual: eval "$(scripts/wv-test.sh start)"; go test ...; scripts/wv-test.sh stop <name>
```

The script starts a uniquely named container on random host ports
(`-p 127.0.0.1::8080 -p 127.0.0.1::50051`, resolved via `docker port`), exports
`CORTEX_TEST_WEAVIATE_REST/GRPC` for the `go test` it runs, and always removes the
container (trap). If `DOCKER_HOST` points at a missing socket, use
`docker --context desktop-linux ...` (or `export DOCKER_CONTEXT=desktop-linux`).

Test conventions: testify (`require` for setup, `assert` for checks). Tests
should encode *why* the behaviour matters, not just what it does (Rule 9).

## 4. Architecture in one screen

- **Server owns everything.** `cmd/server` (impl in `internal/rpc`) is the single
  owner of NATS + Weaviate + Ollama. The MCP server (`cmd/mcp`) and CLI
  (`cmd/cli`) hold no backend connections — they are Connect RPC clients.
- **Writes are async & durable.** `Save`/`UpdateMemory`/`import` publish onto a
  NATS JetStream queue; the **worker** (`cmd/worker`, the only writer; N replicas OK) embeds the
  text via Ollama and upserts into Weaviate. Failures retry, then dead-letter.
- **Only `text` is embedded.** Every other field (namespace, tags, source,
  conversationId, links…) is filterable metadata, never part of the vector. This
  is a load-bearing invariant (pinned by `TestMemoryClassVectorizerNone`).
- **Chunked retrieval (optional, default on).** The worker splits each memory into
  overlapping, token-bounded chunks (`internal/chunk`, 512/64 tok defaults via
  cl100k proxy) and indexes them as the `MemoryChunk` class — the PRIMARY search
  target. `store.Search` queries chunks, groups by parent `memoryId` (best chunk
  distance), resolves to full `Memory` records, and **falls back to whole-memory
  vectors for memories that have no chunks** (so an un-chunked / mid-reindex store
  still works — no flag-day reindex). The whole-memory vector is still used by dedup
  (`SearchMemoryVectors`) and find-similar (`SearchByID`). Toggle: `CHUNKING_ENABLED`
  (default true), set the SAME on worker (writes chunks) and server (searches them);
  false = pre-chunking whole-memory search. Delete / namespace-rename / -delete
  cascade to chunks. See docs/CHUNKING.md.
- **Weaviate runs with `AUTOSCHEMA_ENABLED=false`** (compose, TrueNAS compose, CI,
  `scripts/wv-test.sh`). Schema lives only in code: a new property written by ANY path
  must be declared in `memoryProperties`/`chunkProperties`/`summaryProperties`/
  `userProperties`/`apiKeyProperties` (ensureProps adds it to existing classes), else
  the write fails. Why: a worker write during a reindex/migrate-mt class drop would
  otherwise auto-create a wrong-shaped class. See `autoschema_integration_test.go`.
- **Exact-match keys use `tokenization: field`.** `namespace`, `memoryId`,
  `conversationId`, `source` are field-tokenized — Weaviate's default *word*
  tokenization makes `Equal` fuzzy (`"demo"` matches `"demo-2"`; UUIDs match on
  hyphen fragments). Changing tokenization needs a class REBUILD (ensureProps only
  adds missing props), so a prod schema change to these requires a reindex/rebuild.
- **Metadata-only edits skip the worker.** Changing a field that isn't embedded
  (links, namespace rename, reinforcement, dedup decisions) is a direct Weaviate
  **merge/PATCH** from the server — no re-embed, no NATS round-trip. Re-embedding
  is only for `text` changes.
- **Weaviate access is gRPC, not GraphQL** (`client.Experimental().Search()`).
  Do not reintroduce `client.GraphQL().Get()`.
- **The worker is replica-safe (do not reintroduce single-worker assumptions).**
  Per-memory-id distributed lock in the NATS KV bucket `cortex_locks`
  (`internal/dlock`; 60s TTL, file storage, create-to-acquire, delete-at-our-revision
  to release; bucket re-ensured on infra errors; link applies lock BOTH ids in sorted
  order; the hold deadline is anchored at the FIRST lease). Index messages carry their
  JetStream stream sequence + stream identity → persisted as `indexedSeq` /
  `indexedStream` (Memory + ConversationSummary); under the lock a lower seq (same
  stream) is acked as outcome `stale`. Lock contention waits in-handler (never NAK,
  which would burn `MaxDeliver`). The worker pulls with `Fetch(1)` loops
  (`cmd/worker/pool.go`, `WORKER_CONCURRENCY` per consumer; never a streaming Consume,
  which hoards) and `InProgress`-keepalives each message for its whole life. **Field ownership:**
  index CREATES a missing memory in full (import/restore links kept) but only
  MERGES its own fields (`TenantStore.UpdateIndexFields`) into an existing one; it
  must never write `linkedIds` (link consumer), `accessCount`/`lastAccessedAt`
  (Reinforce) or `notDuplicateOf` (DismissDuplicate). Any new server-side merge
  field must be added to the ownership table in docs/DESIGN.md. Tests: embedded
  NATS via `internal/natstest`; `cmd/worker` has a multi-replica convergence sim.
- **Idempotent ids.** Object id = a UUID; the worker upserts by id, so
  redelivery/retry/reindex overwrites rather than duplicates.
- Two Weaviate classes: `Memory` and `ConversationSummary` (one per
  conversation, upserted). A namespace conceptually spans **both** — anything that
  renames/deletes a namespace must touch both classes or it orphans summaries.

- **Observability** lives in `internal/obs` (shared registry, `/healthz` `/readyz` `/metrics`
  handlers, `obs.Serve` for the dedicated metrics listeners, dependency probes reusing `Store.Ready` / `Embedder.Reachable`). Server RPC metrics are
  a Connect interceptor + a scrape-time queue collector in `internal/rpc/metrics.go`; worker metrics
  are `cmd/worker/metrics.go` (passed explicitly, nil-safe; embed latency via `embed.Client.SetObserver`).
  Never put user input, tenant ids or namespaces in metric labels. See README "Observability".

**Recall preview** (`PreviewRecall` RPC, `internal/rpc/recall.go`, UI `/recall-preview` = `RecallPreviewView.vue`): dry-runs the hook for a prompt — injected hits + near misses (reason `over_max_distance`/`over_limit`) + skip reason. Injected set = the exact hook search (limit L, cutoff D); near misses come from a second no-cutoff search (limit L+5), so their distances can differ slightly (hybrid fusion is normalised over the fetched pool). Optional request fields unset = hook defaults. Never reinforces; session dedup is NOT simulated (client-local). Integration test needs the MT recipe in §3 (uses a fake Ollama httptest server).

Package map: `internal/recall` (hook query logic, shared by hook + server), `internal/rpc` (service + auth/JWT/login + client), `internal/store`
(Weaviate schema/queries), `internal/bus` (NATS), `internal/embed` (Ollama),
`internal/memory` (shared model + stream/class names), `internal/config` (viper).
UI in `ui/src/views/*.vue` wired through `ui/src/router/index.js` + the navbar in
`ui/src/App.vue`; RPC client in `ui/src/utils/connect.js`.

## 4b. Multi-tenancy (DEFAULT ON) + migration (`cortex migrate-mt`)

`CORTEX_MULTI_TENANT` is **on by default** (set `=false` on server AND worker for
legacy single-user). Tenant = user; the bootstrap admin is created from
`CORTEX_BOOTSTRAP_USER/PASSWORD`, falling back to `CORTEX_UI_USER/PASSWORD`, and
`CORTEX_AUTH_TOKEN` is registered as its API key + maps to the admin identity. MT
requires auth, so a deployment with no admin and no token is locked out (fail loud).

An EXISTING pre-MT store can't flip in place — Weaviate cannot enable MT on a
populated class. `cortex migrate-mt` (admin-only) rebuilds it:

1. Server snapshots all memories + summaries to a backup JSON file (text+metadata,
   no vectors; same format as `cortex export`). **Chunks are NOT in the backup**;
   they regenerate when the worker processes the re-import queue.
2. Server drops the three classes and recreates them with MT enabled.
3. Server re-queues every snapshotted record into the **calling admin's tenant**
   (`identity.From(ctx).UserID` — NOT a fixed sentinel, so the data is visible to
   the admin's own JWT/api-key) via `bus.PublishIndex`/`PublishSummary`. The worker
   re-embeds and re-chunks.

Guards: admin-only; refuses if `CORTEX_MULTI_TENANT` is off; refuses if `Memory` is
already MT (one-shot). Safe to re-run `cortex import <backup>` if it stalls (upsert-by-id).

The migration targets ONE tenant (the bootstrap admin). Redistributing data across
multiple users is a manual/out-of-scope operation.

**Resource guards** (internal/rpc/gate.go): BackupSelf, RestoreSelf and Reindex allow ONE
in-flight call per tenant; BackupAll/RestoreAll/MigrateMT ONE server-wide. A second call
gets `ResourceExhausted`. Gates are per server process. Connect requests are capped by
`CORTEX_MAX_REQUEST_BYTES` (default 256 MiB — raise it for very large `restore` uploads).
There is still no per-tenant storage quota or rate limit (see docs/DESIGN.md trust model).

**Integration test gate:** `CORTEX_TEST_WEAVIATE_REST` + `CORTEX_TEST_WEAVIATE_GRPC`
without `CORTEX_TEST_MULTI_TENANT` (migration starts from a non-MT Weaviate).

```bash
# Typical migration workflow (local dev stack):
# 1. Set CORTEX_MULTI_TENANT=true in docker-compose.yml for server + worker.
# 2. docker compose up -d server worker
# 3. cortex --server http://localhost:8088 migrate-mt --yes
# 4. Watch the indexing queue drain (cortex status / Indexing UI tab).
```

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
| `get <id> [--json]` | Show one memory by id (full text + linked ids) via the `GetMemory` RPC (tenant-scoped; NotFound for unknown/other-tenant ids; no reinforcement). Same RPC backs the `cortex_memory_get` MCP tool, which also marks the id delivered in recallstate. |
| `search "<q>" [-n '*'] [-l N] [-d 0.6] [--autocut N] [-t/-T/-x tag] [--reinforce]` | Hybrid (BM25+vector) semantic search. `-d` = distance cutoff. CLI does NOT reinforce by default. |
| `delete <id>` | Delete one memory by id. |
| `export [-n '*'] [-o file]` | Dump memories (text+metadata, no vectors) to JSON (stdout by default). |
| `import <file> [--batch N]` | Restore a dump via the NATS ingest queue (worker re-embeds). Preserves ids/links. **This is how you load seed data** — point `--server` at the target. |
| `reindex [-n '*'] [--yes]` | Server snapshots then republishes all memories for re-embed. `--yes` allows a destructive class rebuild on an embedding-dimension change. |
| `migrate-mt [--yes]` | One-shot migration: snapshot + drop non-MT classes + recreate as MT + re-import to the calling admin's tenant. Admin-only; requires `CORTEX_MULTI_TENANT=true`. |
| `backup` / `backup list\|download\|delete <name>` | Admin-only FULL server backup: all tenants' memories + summaries + user/API-key registry (hashes) → timestamped `cortex-full-backup-*.json` in BackupDir, plus S3 upload when offsite is enabled. S3 is configured SOLELY via server env (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` + `CORTEX_S3_*`), held in memory — never stored in Weaviate or exposed over RPC (no S3-config RPCs; `s3ConfigFromEnv` in cmd/server → `rpc.Config.S3`; minio-go, any S3-compatible endpoint incl. SeaweedFS). Periodic: `BACKUP_INTERVAL` + `BACKUP_KEEP` env on the server. Pruning only ever touches `cortex-full-backup-*` files, never reindex snapshots (`cortex-backup-*`). |
| `backup self [-o file]` | ANY user: download a backup of their own tenant (memories + summaries) locally. |
| `restore <name> \| --file <local> \| --s3 <object-key> [--yes]` | Admin-only full restore, exactly one source: server-side name XOR uploaded local file XOR S3 object key (server downloads via env creds; `backup s3-list` to enumerate). Recreates missing users/keys (existing skipped), re-queues all records to their original tenants (worker re-embeds). Idempotent. RPCs: `ListS3Backups` + `RestoreAll{name|data|s3_key}`; helpers `listS3Backups`/`downloadFromS3` in rpc/s3.go. |
| `restore self <local-file> [--yes]` | ANY user: restore a self-backup into their OWN tenant only (file's tenant attribution ignored — load-bearing security invariant). |
| `upgrade [--yes]` | Self-update `cortex` + `cortex-mcp` to the **server's** version (checksum-verified GitHub release, atomic replace; `internal/upgrade`). Blocked on dev builds & major mismatch. Uses the public `GetVersion` RPC (the ONE auth-exempt procedure — see `ServerAuthInterceptor`). Same logic backs the `cortex_upgrade` MCP tool; the MCP server probes the version at startup and advertises upgrades via MCP instructions. |
| `version [--json]` | Print CLI + server versions (public `GetVersion` RPC, no token). `--json` → `{"client":...,"server":...}`; server unreachable → CLI version still printed, non-zero exit. |
| `users list\|get\|add\|delete\|set-role\|reset-password` | Manage users from the CLI (MT mode; needs an admin `--token`). Break-glass for fixing accounts without the UI. |
| `users apikey create\|list\|delete <username>` | Admin-provision API keys for another user (MT mode; admin `--token`). `create` prints the raw secret once — use to bootstrap headless/service accounts that can't reach the web UI to self-mint. |
| `dead [--requeue \| --purge]` | List dead-lettered (failed-to-index) memories; requeue or purge them. |
| `status` | Server health + store size (nats/weaviate/ollama/model/count). |
| `doctor` | Per-check diagnostics. |
| `summarize "<text>" --conversation <id> [-n ns]` | Save/update a conversation summary (one per conversation). |
| `summaries [-n '*'] [-l N]` | List conversation summaries, newest-updated first. |
| `recall "<q>" [-n '*'] [--fact-limit N] [-d D]` | Recall the best-matching past session: its summary + facts. |
| `hook recall [--timeout 5s] [-n '*'] [-l 3] [-d 0.5] [--min-chars 12] [--max-chars 1500] [--max-query-chunks 4]` | Claude Code **UserPromptSubmit** hook endpoint (`cmd/cli/hook.go`): reads the hook event JSON on stdin, semantically searches memories for the prompt (long prompts split into ~512-token chunks via `internal/chunk`, sampled to the cap, searched concurrently, merged by best distance) and prints a `<cortex-recall>` block on stdout for context injection. Fail-open: ANY error/timeout → no output, exit 0. No reinforcement unless `--reinforce`. **Session dedup is shared with the MCP server** via `internal/recallstate`: a single bbolt db (`~/.cache/cortex/cortex.db`, `--state-db` to override; one bucket per conversation id with a `recalled` sub-bucket + `lastSeen` stamp; idle convos auto-pruned at 7 days, `cortex state` to manage by hand; intended to also hold future per-convo state). The query logic (skip rules, chunk sampling, concurrent search, merge-by-best-distance, limit/cutoff) lives in `internal/recall`, shared with the server's `PreviewRecall` RPC; `cmd/cli/hook.go` only adds the RPC searcher, session dedup and output. **`--dry-run [--prompt]`** calls `PreviewRecall` (hook JSON or raw text on stdin) and prints injected hits + near misses; it never writes recallstate (a session id from the event JSON/`--session-id` is read via `recallstate.LoadReadOnly` to flag already-injected hits). The injected block is built by `recall.FormatBlock` (hook + `PreviewRecall.injected_block`, with per-hit `truncated`/`text_chars` for `--max-chars`). The db is NEVER held open — bbolt's flock is exclusive per open handle, so every op is open(500ms lock timeout, 3 attempts ~250ms apart)→txn→close, letting concurrent MCPs/hooks interleave. A memory delivered to a session's context once (hook injection or search result) is stubbed to ~120 chars on later `cortex_memory_search` calls (`fresh=true` bypasses, for post-compaction re-fetch); hook↔MCP sharing needs the real session id in the MCP env (`CORTEX_CONVERSATION_ID`/`CLAUDE_CODE_SESSION_ID`), else the MCP dedups in-process only. The MCP server also now ships a base "RECALL FIRST" instructions block (cmd/mcp/main.go), with the upgrade notice appended to it. |
| `candidates [-n '*'] [-l N]` | List memories flagged as likely duplicates. `candidates dismiss <id> <target-id>` marks a pair NOT duplicates. |
| `consolidate "<topic>" [-n '*'] [-l N] [-d D] [-t/-T/-x tag]` | Print the cluster of memories about a topic + manifest (read-only gather; the LLM merges). No tag flag = whole cluster, not "untagged only". |
| `state ls \| show <convo> \| clear <convo> \| purge [--older-than 168h]` | Inspect/maintain the LOCAL per-conversation bbolt state db (recall dedup): list convos (last-seen + recalled count), show delivered memory ids, drop one convo, purge idle ones. Local-only, no RPC (`cmd/cli/state.go` + `internal/recallstate`). |
| `config init [--force]` / `config show` | Scaffold / print the effective `cortex.yaml`. |
| `onboard [--force]` | Interactive first-run: prompts for server URL + API token (the only non-default values), writes `cortex.yaml` with defaults for everything else. Shares the config template with `config init`; token read no-echo. |
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

### Recall-accuracy harness

`scripts/recall_accuracy.py` measures retrieval recall@k / MRR against a running
server from a labelled query set (query → expected memory id). It is a black-box
RPC client, so the SAME harness runs before and after a code change for A/B
comparisons (`scripts/recall_compare.py` renders the before/after table). Fixtures:
`testdata/seed-long.json` (+ `recall-queries.json`) are long needle-in-haystack
memories with labelled probes. This is what validated chunking — see
docs/CHUNKING.md. The gitignored venv `scripts/.venv` holds tiktoken for fixture
token-counting.

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
