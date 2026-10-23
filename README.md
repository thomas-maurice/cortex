# Cortex — a second brain for Claude

Self-hosted, local-first **memory layer** behind an MCP server. Claude saves
facts and recalls them by meaning across sessions. Go, Connect RPC, NATS
JetStream, Weaviate, Ollama. Built to grow well beyond memory.

```
Claude ──stdio──► cortex-mcp ─┐
                              ├─Connect RPC─► cortex-server ──save──► NATS ──► worker ──► Ollama ──► Weaviate
host shell ─────► cortex CLI ─┘                    └─────────search──► Ollama embed ──► Weaviate (gRPC nearVector)
```

The **cortex-server** is the single owner of NATS, Weaviate, and Ollama. The MCP
server and the CLI are **thin clients** that only speak Connect RPC to it — they
hold no database connection of their own. This lets you self-host the brain once
and reach it from several machines.

- **Writes** are async + durable (NATS): a burst of saves queues and drains at
  the embedder's pace; nothing is lost if a backend is down.
- **Search** is synchronous (server → Ollama + Weaviate): a tool call needs an
  answer now. Weaviate is queried over **gRPC**, not GraphQL.
- **Auth** is a shared bearer token (`CORTEX_AUTH_TOKEN`); unset = open (local
  dev only). Structured behind an `Authenticator` interface so OIDC / per-client
  API keys can slot in later.

Full rationale in [`docs/DESIGN.md`](docs/DESIGN.md); background research in
[`docs/RESEARCH.md`](docs/RESEARCH.md).

## Requirements

- **Docker + Docker Compose** — runs the whole backing stack.
- **Go 1.26+** — to build the host-side MCP binary that Claude execs.
- **Ollama** — **required**, and the heart of the system: it generates every
  embedding (for both saving and searching). It runs as a container in this
  stack, so there's nothing to install separately, but Cortex **cannot save or
  search without it**, and you must pull an embedding model into it before use
  (`make model`, or `make bootstrap` which does it for you). Default model:
  `qwen3-embedding:0.6b`. Needs **Ollama >= 0.3.4** (batched `/api/embed`; the
  bundled image is newer). See [Embedding model](#embedding-model) to choose another.

- **Weaviate with `AUTOSCHEMA_ENABLED=false`** — the bundled compose files set it.
  If you run your own Weaviate you MUST set it too. Cortex declares its whole schema
  in code (`EnsureSchema`, run by server and worker at boot); Weaviate's auto-schema
  would otherwise silently create a wrong-shaped `Memory` class (not multi-tenant,
  word tokenization on exact-match keys) when a write lands while a reindex rebuild
  or `migrate-mt` is dropping/recreating classes. With it off such a write just
  fails and the worker retries it (2s apart, up to 10 attempts ≈ 20s); anything
  still failing after that is dead-lettered and can be requeued with
  `cortex dead --requeue`. **Upgrading:** the setting is read at Weaviate startup,
  so recreate the Weaviate container (`docker compose up -d weaviate`) after
  pulling the new compose file; existing data is unaffected.

> If you already run Ollama natively on the host (port 11434), remove the
> `ollama` service from `docker-compose.yml` and point `OLLAMA_URL` at your host
> instance instead — Cortex talks to it over HTTP either way.

## Quickstart

```bash
# 1. Bring up infra (nats, weaviate, ollama, worker, server) and pull the model
export CORTEX_AUTH_TOKEN=$(openssl rand -hex 16)   # the shared client token
make bootstrap          # = docker compose up -d --build + ollama pull qwen3-embedding:0.6b

# 2. Install the host-side client binaries (the MCP server Claude execs + the CLI)
curl -fsSL https://raw.githubusercontent.com/thomas-maurice/cortex/master/scripts/install.sh | bash

# 3. Point Claude at it (see below), then in a Claude session:
#    "save a memory: I prefer Go for backend services"
#    "search your memory for my language preference"
```

> Step 2 downloads the latest CI-built release binaries (no local toolchain
> needed). To build from source instead, use `make build` (-> `./bin/...`).

> Regenerate the protobuf-generated Go code with `make proto` after editing
> anything in `proto/` (needs `buf`).

`make help` lists every target. `make logs` tails the indexer. `make nuke` wipes
all data volumes.

## Installing / updating the client (`scripts/install.sh`)

The MCP server and CLI run on your machine (the server/worker run wherever you
host them). Install or update those two client binaries from the latest
CI-published release — **re-run the same command to update**:

```bash
curl -fsSL https://raw.githubusercontent.com/thomas-maurice/cortex/master/scripts/install.sh | bash
```

It detects your OS/arch, downloads the matching release tarball, verifies its
`checksums.txt`, drops `cortex-mcp` + `cortex` into `~/bin` (or `~/.local/bin`),
and clears the macOS quarantine flag so the MCP binary launches. Overrides:

| Env | Default | Purpose |
|-----|---------|---------|
| `CORTEX_VERSION` | latest release | pin a tag, e.g. `v0.0.3` |
| `CORTEX_INSTALL_DIR` | `~/bin` if present, else `~/.local/bin` | where binaries land |
| `CORTEX_BINS` | `cortex-mcp cortex` | which binaries to install |

When it finishes, the script prints the installed paths and a ready-to-paste
`claude mcp add` command (with the real `cortex-mcp` path filled in — and the
server URL too if `CORTEX_SERVER_URL` is exported). After updating `cortex-mcp`,
reconnect/restart the MCP server in Claude so it re-execs the new binary.
Binaries come from CI (goreleaser); no local Go toolchain is needed.

## Wiring into Claude Code

Claude Code discovers MCP servers from a `.mcp.json` file and, on startup, execs
the `cortex-mcp` binary over **stdio**, passing the env in that file. The binary
registers the server (`cortex`) and its tools; from then on Claude calls those
tools on its own judgement, guided by their descriptions.

There are two ways to register it:

- **Project scope (included).** A ready-to-use [`.mcp.json`](.mcp.json) lives in
  this repo. Claude Code auto-detects it whenever you launch from this directory.
  Good for working *on* Cortex.
- **User scope (global — available in *every* project).** Register it once with
  the Claude CLI (the install script prints this exact command with your real
  install path):

  ```bash
  claude mcp add --scope user cortex ~/.local/bin/cortex-mcp \
    -e CORTEX_SERVER_URL=http://localhost:8088 \
    -e CORTEX_AUTH_TOKEN=<your-token> \
    -e MEMORY_SOURCE=claude-code
  ```

  Point the command at your `cortex-mcp` binary — where the install script put
  it (`~/bin` or `~/.local/bin`) or a local build (`./bin/cortex-mcp` from
  `make build`). `--scope user` writes the server into your **global user
  config**, `~/.claude.json`, under the top-level `mcpServers` key. The
  equivalent manual entry (add it there yourself instead of running the CLI —
  `command` must be an **absolute** path here, `~` is not expanded in JSON):

  ```jsonc
  // ~/.claude.json
  {
    "mcpServers": {
      "cortex": {
        "command": "/absolute/path/to/cortex-mcp",
        "env": {
          "CORTEX_SERVER_URL": "http://localhost:8088",
          "CORTEX_AUTH_TOKEN": "<your-token>",
          "MEMORY_SOURCE": "claude-code"
        }
      }
    }
  }
  ```

  Leave `CORTEX_AUTH_TOKEN` empty/absent if the server runs in open mode. To make
  Claude *use* the memory on its own, also add the snippet from
  [Make Claude do this automatically](#make-claude-do-this-automatically) to your
  global `~/.claude/CLAUDE.md`.

The MCP client holds **no** database config — just the server URL and the auth
token. The model, namespace defaults, and all backend wiring live on the server.
The MCP client also stamps each save with the Claude Code session ID (read from
`CLAUDE_CODE_SESSION_ID`) as the memory's `conversationId`.

> **Ports:** this stack maps NATS to host `4223`, Weaviate to host `8081`, and
> the **cortex-server to host `8088`** (instead of `8080`, which collides with
> another local stack). Inside the compose network the standard ports are used;
> only the host-side mappings differ. The CLI/MCP client reach the server at
> `http://localhost:8088`.

Verify it's connected with `/mcp` inside Claude — you should see `cortex` with
its tools: `cortex_memory_save`, `cortex_memory_search`, `cortex_memory_get`, `cortex_memory_delete`,
`cortex_trash_list`, `cortex_trash_restore`, `cortex_memory_link`, `cortex_memory_unlink`, `cortex_consolidate`,
`cortex_review_candidates`, `cortex_dismiss_duplicate`, `cortex_session_summarize`,
and `cortex_recall_session`.

## Web UI

The `cortex-server` binary also serves a web UI (Vue 3 + Tailwind/shadcn-vue, embedded in
the binary via `go:embed` — nothing extra to deploy). It is served on the same
port as the Connect API; open the server's address in a browser. Views:

- **Memories** — semantic search or newest-first list; add and delete memories.
- **Graph** — a force-directed map of your memories (nodes coloured by
  namespace). Solid **green** edges are explicit links; **Connect** mode lets you
  click two memories to link them, and clicking a green edge removes it.
  Double-click (or "Find similar") adds a memory's nearest vector neighbours as
  **dashed** edges, gated by a distance cutoff; "Clear added" removes them.
- **Explore** — type any text; it is vectorised server-side and matched against
  your memories, rendered as a cloud radiating from a central query node (closer
  + bigger = more relevant, edge label = vector distance).
  **Connect** mode lets you link (or unlink) the matched memories directly from
  the cloud, handy for linking a whole topic at once.
- **Recall preview** — paste a prompt and see what the `cortex hook recall`
  hook would inject: memories with distance bars, a divider at the cutoff, and
  greyed near misses tagged *over max distance* / *over limit*. Limit, max
  distance, min chars and max query chunks default to the hook's. Per-session
  dedup is not simulated.
- **Sessions** — conversation summaries (`ListSummaries`).
- **Namespaces** — admin view of every namespace with its memory and summary
  counts and last activity. **Rename** a namespace (a metadata-only move — nothing
  is re-embedded; both memories and summaries move) or **delete** an entire
  namespace (typed confirmation; its memories go to the trash, its summaries are
  permanently removed).
- **Tags** — every tag with its memory count and last activity, a filter box,
  and **rename / merge** (renaming into an existing tag dedups) or **delete**
  (removes the tag, never the memories). Click a tag to open Memories filtered by
  it. Metadata-only: memories and their search chunks are re-tagged in place, no
  re-embed. The reserved `preference` tag needs an explicit confirmation.
  Tags are matched exactly (case- and whitespace-sensitive) and canonicalised on
  write (trimmed, deduplicated). Tags applied automatically on save
  (`host:<hostname>`, the static `save.tags` from the config) are not special: after
  a rename or delete they come back on every NEW save, so rename them in the config
  too. A rename/delete is refused while a reindex or restore is running.
- **Expiry** — New memory and the inspector's edit mode have an expiry picker
  (never / 1 day / 1 week / 1 month / custom date); memories show an "expires in 3d"
  or "expired" badge, and the Memories view has an *Include expired memories* toggle.
- **Duplicates** — review the pairs the worker flagged as likely duplicates
  (`DEDUP_DISTANCE`), side by side (text, namespace, tags, date, link count; a pair
  shows once even if both sides flag each other). Per pair: **Not duplicates**
  (dismiss, never re-flagged), **Keep left / right** (folds the other into the kept one via `MergeInto` — its links, dismissals and stats are carried over — and trashes it, with an undo toast), or **Merge…** (editor prefilled with both texts, saved
  with `supersedes` = both ids so they move to the trash once the merge is indexed; their links are carried over to the merged memory).
  CLI/MCP equivalents: `cortex candidates`, `cortex_review_candidates`.
- **Trash** — every deleted memory (explicit delete, `supersedes`, namespace delete)
  lands here first. List with deletion time and reason, **restore** (re-queued for
  indexing under the same id), **purge** one or all (confirm dialogs). Entries
  expire after `TRASH_RETENTION`.
- **Stale review** (Memories view, "Stale" filter) — never used / not used in
  30, 90 or 180 days (actual use: hook injection or full delivery to the agent; browsing
  doesn't count), oldest last use first, with use count and last-used time on each card,
  a "usage tracked since" notice (with a limited-results warning) and an opt-in for
  `preference` memories. Multi-select (with "select all on page") and **Move to trash**
  in bulk; the success toast offers **Undo** (restores the same ids from the trash).
- **Preferences** — standing, cross-project preferences: the memories in the
  `global` namespace tagged `preference`. Add / edit / delete them here without
  touching namespace or tags (both are managed for you). These are what the
  [standing-preferences SessionStart hook](#standing-preferences-sessionstart-hook)
  injects into every session.
- **Backup** — export memories (text + metadata, no vectors) to a JSON file, and
  import such a dump back. The format is identical to the `cortex export` /
  `cortex import` CLI, so dumps are interchangeable; import re-ingests through the
  normal queue (worker re-embeds), batched, and reports how many were queued.
- **Indexing** — live state of the async index queue (pending / in-flight /
  dead-lettered) and the failed-memory list, with requeue/purge. Auto-refreshes
  while open, so a burst of ingestion is actually visible as it drains.
- **Storage** — read-only view of what is physically in Weaviate: per logical class
  its alias target and version (`Memory → Memory_v2`), exact object counts, vector index/distance, stored
  dimensions (flagged if they differ from the configured model), indexing state and
  queue length, per-tenant breakdown (by username) for admins, and any leftover
  class from an interrupted migration. Non-admins see only their own tenant.
- **Status** — backing-service health and memory count.
- **Docs** — in-app setup guide: install the `cortex-mcp` binary and wire it into
  Claude Code and Claude Desktop, with copy-paste config snippets pre-filled with
  this server's address.

Sign in with `CORTEX_UI_USER` / `CORTEX_UI_PASSWORD`. Login mints a short-lived
JWT; the API then accepts **either** that JWT (browser) **or** the static
`CORTEX_AUTH_TOKEN` (MCP/CLI), so both coexist on one server.

| env var | purpose |
| --- | --- |
| `CORTEX_UI_USER` | UI login username (default `admin`) |
| `CORTEX_UI_PASSWORD` | UI login password — **plaintext, or a hash** (see below); **unset disables the UI login** |
| `CORTEX_JWT_SECRET` | explicit HS256 secret for UI JWTs. If unset, a **stable** secret is derived as `sha256("cortex/jwt-secret/v1:" + CORTEX_AUTH_TOKEN)` (so sessions survive restarts without using the API token directly as the signing key). If neither is set, a random per-process secret is used and sessions die on restart. |

`CORTEX_UI_PASSWORD` may be a **plaintext** password, an **argon2id** PHC hash
(`$argon2id$…`), or a **bcrypt** hash (`$2a$…`) — the format is auto-detected, so
you can keep the plaintext out of your env/compose file. Generate an argon2id
hash with the CLI:

```bash
cortex hash-password            # prompts (no echo), prints the hash
echo -n 'my-password' | cortex hash-password   # or pipe it
# → $argon2id$v=19$m=65536,t=1,p=10$…   ← use this as CORTEX_UI_PASSWORD
```

This is single-user by design; the JWT already carries a `role` claim, so adding
real users later is a backend-only change. Frontend lives in `ui/`; `make ui`
builds it, `make proto-ui` regenerates the typed Connect clients from the proto.

See [docs/WEB_UI.md](docs/WEB_UI.md) for the full architecture (embed/build
pipeline, auth flow, every view, and the memory-linking model).

## Using it well — memory hygiene

- **Give time-bound facts an expiry.** "Deploy freeze until Friday" or "I'm on the
  staging cluster this week" stop being true, and a stale memory is the main way a
  memory system misleads the model. Save them with `expiresIn` (MCP) /
  `--expires 7d` (CLI) — see [Expiry](#expiry-for-time-bound-facts).

The system is only as useful as what Claude puts in it. Two habits matter:

- **Save often, and save rich.** Don't hoard memories for "important" moments.
  Whenever the user states a fact, preference, decision, plan, or useful piece of
  context, save it. Saving is cheap and asynchronous.
- **Write self-contained, multi-sentence notes.** A memory should make sense on
  its own months later. Capture the *what* **and** the *why/context*, not just a
  keyword. One to a few sentences is the sweet spot — enough to rehydrate the
  context, short enough to embed cleanly. Summarising a whole discussion or
  conclusion into one good memory is exactly the point.

Good vs. weak:

```
✓ "Thomas decided to remap Cortex's NATS to host 4223 and Weaviate to 8081
   because the default 4222/8080 collided with another local stack he runs."
✗ "ports changed"
```

Memories are scoped by **namespace** + **tags**. You normally don't set the
namespace yourself — the MCP client auto-derives it per project (git origin URL →
directory name → `global`), so let it, and reach for an explicit `namespace` only
when you deliberately want a different scope. Use **tags** to filter within a
scope, e.g. a consistent `archived` tag for things you want kept but excluded from
normal search via `--exclude-tag`.

### Make Claude do this automatically

Add the following to your **`~/.claude/CLAUDE.md`** (user scope, so it applies in
every project) so Claude treats the second brain as a reflex, not an afterthought:

```markdown
## Second brain (cortex MCP)

You have a persistent memory via the `cortex` MCP server. Use it actively:

- **Recall first — search before you answer or act.** `cortex_memory_search` is the
  FIRST step of a task, not a fallback. Search at the start of essentially every
  non-trivial task, and the moment I reference anything that might carry prior
  context — a system, project, person, tool choice, past decision, convention,
  preference, error, or phrasing like "how did we", "last time", "as usual",
  "remember", or a term you don't recognise. When unsure whether it's in memory,
  search anyway — a cheap miss beats contradicting a stored decision. Only skip it
  for a pure greeting or a fully self-contained mechanical step. This applies even
  when the task looks like writing a doc, summary, or notes: a request to
  "summarise", "write up", "remember", or "explain what you learned" about a
  repo/system/decision IS a memory write — route it to cortex, and only create a
  repo file (`SUMMARY.md`, `docs/`) when explicitly asked for a tracked file. When
  both apply, cortex is the source of truth and the file is a copy.
- **Transient failures get one retry, not a silent skip.** If a cortex call
  fails on network/DNS (e.g. the VPN is down), retry once before continuing. If
  it still fails, say so explicitly and note the memory was not saved — never let
  a save or search silently vanish.
- **Save proactively and often.** Whenever I state a fact, preference, decision,
  plan, or noteworthy context — or when we reach a conclusion worth keeping —
  call `cortex_memory_save` without being asked. Saving is cheap; err on the side of more.
- **Write rich, self-contained memories.** One to a few sentences that capture the
  fact *and* enough context (who/what/why) to be understood on its own later.
  Prefer summarising a discussion into one good memory over many fragments.
- **Don't set `namespace` by default.** The MCP client auto-derives it from the
  repo's git origin URL (falling back to the directory name, then `global`), so each
  project is already scoped. Only pass `namespace` explicitly to file a memory in a
  *different* scope on purpose (e.g. a deliberately shared/global note). Use `tags`
  for filtering *within* a scope.
- **Make cortex the single memory store.** If you also use Claude Code's built-in
  file memory (`MEMORY.md`), don't duplicate the same fact into both — route new
  memories to cortex so recall isn't split across two stores.
- **Maintain a session summary — REQUIRED, not optional, and separate from
  `cortex_memory_save`.** There is exactly ONE summary per conversation; each
  `cortex_session_summarize` call REPLACES it, so always pass the *full current*
  digest (what the session is about, what was done, key outcomes) — never a delta.
  Call it **after every meaningful milestone or topic shift** (a feature landed, a
  bug root-caused, a decision made) **and again before the session ends** — do NOT
  defer it to a single call at the end, and do NOT wait to be asked. `cortex_memory_save`
  captures atomic facts; `cortex_session_summarize` captures the running narrative —
  both are expected on any non-trivial session. A stale or missing summary means the
  session is recalled wrong. Self-check: if I've done substantive work this session
  and not yet called `cortex_session_summarize`, do it now.
- **Recall past sessions.** When I refer to a previous session ("remember when
  we…"), call `cortex_recall_session` to pull that conversation's summary and the
  facts saved during it, before answering.
- **Connect related memories.** When a search surfaces a memory meaningfully
  related to another (a decision and its motivating bug, a preference and its
  project, two facts about one system), call `cortex_memory_link` with the two
  IDs. This builds a navigable knowledge graph; links are bidirectional and durable.
  Linking is queued and applied asynchronously once both endpoints are indexed, so
  you can link an ID you just saved this turn (still indexing) — or skip the extra
  call entirely and pass `linkTo` when you `cortex_memory_save` the new memory.
- **Storing preferences (the one namespace exception).** A durable, cross-project
  preference or standing rule about how I should work (conventions, do/don't, tooling
  choices, communication style) is the ONE case to override the default namespace:
  save it with `namespace: "global"` **and** `tags: ["preference"]` (plus any topical
  tags). A project-specific preference instead stays in the project's namespace with
  `tags: ["preference"]`. These are surfaced every session by the standing-preferences
  SessionStart hook (below), so they actually apply *before* I act — unlike an ordinary
  memory, which only resurfaces if a later search happens to match it. A hard guardrail
  that must NEVER be violated still belongs in this CLAUDE.md itself (the harness loads
  it deterministically every session); the `global`+`preference` store is for
  maintainable, UI-editable preferences.
```

### Standing preferences (SessionStart hook)

A `global`+`preference` memory is only a *filing convention* until something
actually pulls it into context. By default nothing does — Cortex is consulted only
when the agent chooses to call `cortex_memory_search`, and that is relevance-ranked,
so a standing rule like "never force-push" may never surface during an unrelated
task. The bundled hook closes that gap: it lists those memories **by tag** (a
deterministic `List`, no vector query) and injects them into **every** session, so
they are in front of the agent before it acts.

Install (user scope, applies in every project):

```bash
# 1. Drop the hook in place (from a checkout of this repo)
cp scripts/cortex-prefs-session-start.sh ~/.claude/hooks/
chmod +x ~/.claude/hooks/cortex-prefs-session-start.sh

# 2. Register it in ~/.claude/settings.json under hooks.SessionStart, e.g.:
#   {
#     "matcher": "startup|resume|clear|compact",
#     "hooks": [
#       { "type": "command", "command": "$HOME/.claude/hooks/cortex-prefs-session-start.sh" }
#     ]
#   }
```

It needs the `cortex` CLI installed (`scripts/install.sh`) and configured
(`~/.config/cortex/cortex.yaml` with `server` + `token`); it reuses that config. It
is **best-effort and non-blocking** — if the CLI is missing, the server is
unreachable (VPN down), or no preferences are stored, it prints nothing and exits 0,
so it can never delay or break session start (it caps the lookup at 8s). To change a
preference, edit the memory in the web UI or re-save it; the next session picks it up.

### Automatic recall (UserPromptSubmit hook)

Saving is easy to make a habit; recalling is not. Searching memory is cued by an
*absence* — the model only reaches for `cortex_memory_search` when it already
suspects there is something to find, which is exactly when it is least needed.
`cortex hook recall` turns recall into a push instead of a pull: wired as a
Claude Code **UserPromptSubmit** hook, it semantically searches your memories for
**every prompt you type** and injects the best matches into the model's context
before it starts working. Recall stops depending on the model's judgment.

```bash
# Register in ~/.claude/settings.json (user scope) under hooks.UserPromptSubmit:
#   {
#     "hooks": {
#       "UserPromptSubmit": [
#         {
#           "hooks": [
#             { "type": "command", "command": "$HOME/.local/bin/cortex hook recall", "timeout": 10 }
#           ]
#         }
#       ]
#     }
#   }
```

It needs the `cortex` CLI installed (`scripts/install.sh`) and configured
(`~/.config/cortex/cortex.yaml`, e.g. via `cortex onboard`) — the hook reuses
that server/token config. It is **fail-open and hard-bounded**: the whole run is
capped by `--timeout` (default **5s**), and any error — server unreachable, VPN
down, malformed input, deadline hit — prints nothing and exits 0, so it can
never delay or block a prompt. Behavior details (all tunable via flags on the
`command` line):

- Skips slash/bang commands and prompts shorter than `--min-chars` (12).
- Searches **all** namespaces by default (`-n '*'`) — past context is often in
  another project's scope. Injects at most `-l` 3 memories per prompt, each
  truncated to `--max-chars` 1500, gated by a deliberately strict `-d` 0.5
  distance cutoff so *silence is the common case* (injected noise teaches the
  model to ignore the block).
- **Long prompts are chunked**: pasted logs/diffs embed poorly as one blob, so
  prompts are split into ~512-token chunks, up to `--max-query-chunks` (4) of
  them searched concurrently (first + last + sampled middles — the actual ask
  lives at the ends), and results merged keeping each memory's best distance.
- **Per-session dedup, shared with the MCP server**: a memory injected once is
  not re-injected on later prompts of the same session. State lives in a single
  bbolt database (`internal/recallstate`, default `~/.cache/cortex/cortex.db`,
  created on demand, override with `--state-db`) with one bucket per
  conversation — conversations idle for a week are auto-pruned, and
  `cortex state ls|show|clear|purge` inspects/maintains the db by hand
  (`purge --older-than` for a custom threshold). The database is never
  held open: every operation is open→txn→close with a short lock timeout plus
  retries, so concurrent sessions (multiple MCP servers + hook runs) interleave
  safely. The MCP server plays the same game: `cortex_memory_search` returns a
  memory's full text only the first time it reaches a session's context —
  afterwards it comes back as a **stub** (the part that matched the new query, else a ~120-char prefix; `fresh=true` re-fetches in
  full, e.g. after compaction dropped the earlier copy). Hook and MCP share the
  per-conversation bucket when the MCP knows the real session id
  (`CLAUDE_CODE_SESSION_ID` / `CORTEX_CONVERSATION_ID` in its env); without it
  the MCP still dedups its own results in-process for the session's lifetime.
- Injections do **not** reinforce the living-memory signal unless `--reinforce`
  is set — an automatic push is not the agent choosing to recall.
- **Tune it without guessing**: `cortex hook recall --dry-run --prompt "..."` (or
  the web UI's **Recall preview** view) shows what *would* be injected for a
  prompt, the next few near misses and whether each was cut by `-d` or `-l`. The
  query logic is shared with the hook (`internal/recall`), so the preview is what
  the hook does — except per-session dedup, which is client-local state the server
  cannot see (the preview says so).

### `consolidate-memories` skill (bundled)

This repo ships a Claude Code skill at
[`.claude/skills/consolidate-memories/`](.claude/skills/consolidate-memories/SKILL.md)
that runs a one-shot **memory consolidation pass** over the current repository. It:

- gathers every source of knowledge about the repo — file auto-memory
  (`MEMORY.md`), existing Cortex memories, project docs, and the code itself;
- reconciles it against the current state, discarding anything stale;
- splits it into discrete, self-contained memories grouped by topic;
- saves them to Cortex (auto-derived namespace) and **links** related ones.

Invoke it with `/consolidate-memories`, or just ask Claude to "consolidate
memories for this repo" / "rebuild my Cortex memories". The skill is committed to
the repo, so anyone who clones it gets the same workflow. To add your own, drop a
`SKILL.md` under `.claude/skills/<name>/` — that directory is git-versioned (see
`.gitignore`), while the rest of `.claude/` (local settings, transcripts) stays
untracked.

## Tools

The MCP server exposes these tools to Claude:

| Tool | What it does |
|------|--------------|
| `cortex_memory_save` | Queue a memory for indexing. Args: `text`, `namespace?`, `tags?`, `expiresIn?` (e.g. `"7d"`, `"36h"` — set it for TIME-BOUND facts; the memory stops being recalled once it lapses), `neverExpires?` (with `supersedes`: keep the merged memory permanent even if every source expires; otherwise it inherits the latest source expiry), `linkTo?` (ids to bidirectionally link this memory to — queued and applied once both ends are indexed, so targets need not exist yet), `supersedes?` (ids this memory replaces — the worker trashes them once this one is indexed; this memory inherits their links). Returns its `id`. |
| `cortex_memory_search` | **Hybrid** search (BM25 keyword + vector, blended by `SEARCH_ALPHA`, default 0.5) so exact tokens/codenames resolve, not just semantic matches. Args: `query`, `namespace?` (`*` = all namespaces), `limit?`, `tags?` (must have all), `excludeTags?`, `maxDistance?` (relevance cutoff), `includeExpired?` (also return expired memories; hidden by default). Each hit includes its `validUntil`/`expired` when time-bound; its `id`, any `linkedIds`, `matched` (for long, chunk-indexed memories: the excerpt that matched the query; omitted when it is the whole text), and any `dupCandidates` (flagged likely-duplicates — the output hints when consolidation would help). When **living memory** is enabled (`RERANK_WEIGHT`>0, off by default), search also re-orders results by recency-decayed usage and reinforces its top hit(s) — frequently-recalled memories float up over time. See [`docs/DESIGN.md`](docs/DESIGN.md#living-memory--decay--reinforcement-opt-in). |
| `cortex_tags_list` | **Read-only** tag listing (`namespace?`): each tag in use with its memory count, most used first. Call it before saving to REUSE an existing tag instead of inventing `k8s`/`kubernetes` variants. Rename/delete are intentionally not exposed over MCP (use the CLI/UI). |
| `cortex_memory_get` | Fetch ONE memory by `id`, full text (follow `linkedIds` from search hits, or ids from `cortex_consolidate` / `cortex_review_candidates`). Same shape as a search hit; counts as delivered for session dedup so later searches stub it; does not reinforce. |
| `cortex_memory_delete` | Delete a memory by `id` (get the `id` from a `cortex_memory_search` result first). The memory goes to the **trash** (restorable for `TRASH_RETENTION`, default 7 days), not into the void. |
| `cortex_trash_list` | Read-only: list restorable deleted memories (id, namespace, deleted-at, reason, excerpt), newest first. Args: `namespace?`, `limit?` (default 20). |
| `cortex_trash_restore` | Undo a delete: restore a trashed memory by `id` (re-queued for indexing under the same id; fails if the id is not in the trash or a live memory has it). |
| `cortex_memory_link` | Explicitly link two related memories (bidirectional) so they connect in the graph. Args: `id`, `targetId` — from a prior search/recall, or an id just returned by `cortex_memory_save`. The edge is queued on the `memory.link` subject and applied once both endpoints are indexed (it waits for them), so out-of-order/just-saved ids link correctly. Claude is told to do this proactively when two memories are meaningfully related. |
| `cortex_memory_unlink` | Remove the link between two memories (queued, applied asynchronously). Args: `id`, `targetId`. |
| `cortex_consolidate` | Gather the cluster of memories about a `topic` (vector matches + their linked/duplicate neighbours, capped at `limit?`) for Claude to merge into fewer, richer memories. Read-only: returns the cluster and a `manifest` of ids; Claude commits the merge by saving compiled memories with `supersedes` set from the manifest. Args: `topic`, `namespace?`, `limit?`, `maxDistance?`, `tags?` (all), `anyTags?` (at least one), `excludeTags?`. **Omitting the tag args means no tag filter** (the whole topic cluster, every tag — not "only untagged"). |
| `cortex_review_candidates` | List memories the worker flagged as likely duplicates, each with the candidates it resembles, for review. Args: `namespace?`, `limit?`. |
| `cortex_dismiss_duplicate` | Record that two flagged memories are NOT duplicates, so the pair stops being re-flagged. Args: `id`, `targetId`. |
| `cortex_session_summarize` | Save/update the **running summary of the current conversation** (one per session, replaced each call). Args: `text`, `namespace?`. The session ID is attached automatically. **Meant to be called frequently** — see below. |
| `cortex_recall_session` | Recall a **past session** by describing it (`query`). Returns the best-matching summary **and** the facts saved during that conversation. Args: `query`, `namespace?`, `factLimit?`, `maxDistance?`. |

### Consolidating duplicates — gather, merge, supersede

Over time a topic accumulates overlapping or stale memories. `cortex_consolidate`
turns that into a safe, LLM-driven merge:

1. **Gather (server, read-only).** Given a `topic`, the server vector-searches it
   and expands the cluster with each match's `linkedIds` and `dupCandidates`,
   deduped and capped at `limit`. It returns the full cluster plus a `manifest` of
   the gathered ids. It never writes or deletes.
2. **Merge (Claude).** Claude reads the cluster and writes the fewest faithful
   memories that preserve every distinct fact and drop the redundancy.
3. **Supersede (worker).** Each compiled memory is saved with `supersedes` set to
   the source ids it absorbs (only ids from the manifest). The worker deletes
   those sources **only after the new memory is durably indexed**, and first
   carries their links over: the merged memory is linked to every live neighbour
   of the sources (excluding the sources themselves), and those neighbours stop
   pointing at the trashed ids. Claude need not pass `linkTo` for them. Access counts, not-a-duplicate dismissals,
   the oldest `createdAt` and the conversation id are carried over too.

That ordering is the safety property: because deletion happens *after* the merged
memory is persisted, a crash mid-consolidation can leave stale sources behind (the
pre-merge state) but **can never lose the merged content**. This is why Claude
sets `supersedes` rather than calling `cortex_memory_delete` itself — a manual
delete could land before the replacement is indexed and lose data.

Search results help kick this off: when a hit carries flagged `dupCandidates`, the
search output hints that `cortex_consolidate` can gather and merge the cluster.
The `cortex consolidate <topic>` CLI command prints the same cluster + manifest for
inspection (the merge itself needs an LLM).

#### How to ask for a consolidation

Just ask Claude in natural language; it picks the scope from how you phrase it:

- **This project (default).** *"Consolidate what you know about the router setup."*
  The namespace auto-derives from the repo's git remote, so the cluster is scoped
  to the current project without you saying so.
- **A specific project / everything.** *"Consolidate the WireGuard notes in the
  `homelab` namespace"* → `namespace: "homelab"`. *"…across all my projects"* →
  `namespace: "*"`.
- **By tag.** *"Consolidate everything tagged `router` and `mtu`."* → `tags`
  (must have **all**). *"…tagged `router` **or** `firewall`"* → `anyTags` (at
  least one). *"…but skip anything tagged `archived`"* → `excludeTags`.

> **What if I don't mention tags?** Then there is **no tag filter**: Cortex
> gathers the whole topical cluster in the namespace, across every tag. Omitting
> tags does **not** restrict it to untagged memories — it just means tags don't
> constrain the set (same as `cortex_memory_search`). Tag scoping is opt-in, and
> when you do scope by tag, the *entire* cluster — including linked and
> duplicate-candidate neighbours pulled in by expansion — stays inside that tag
> filter, so a tag-scoped merge can never supersede an out-of-scope memory.

> **`cortex_consolidate` vs the `/consolidate-memories` skill.** The skill is a
> broad, repo-wide pass that reconciles *all* knowledge sources (files, docs,
> code, existing memories). `cortex_consolidate` is a focused, on-demand merge of
> the memories already stored about one topic. Use the skill to bootstrap; use the
> tool to keep a topic tidy.

### Session summaries — recall a whole conversation

Beyond individual facts, Cortex keeps **one ever-current summary per conversation**
(class `ConversationSummary`, keyed deterministically by the session ID, so a
re-save overwrites in place). This powers *"do you remember the session where we
patched the router?"*: `cortex_recall_session` vector-matches the summary, then
fans out to every fact tagged with that conversation's `conversationId`.

> **This only works if the summary is kept fresh.** The `cortex_session_summarize`
> tool description instructs Claude to call it **proactively and frequently** —
> after each meaningful step or topic shift, and again before the session ends —
> always passing the *full current* summary (it replaces, not appends). A stale
> summary means a session is recalled wrong. Reinforce it in your `CLAUDE.md`
> (below) so it becomes a reflex.

#### How the conversation ID is resolved

Every memory and every summary is tagged with a `conversationId` so summaries can
fan out to the facts saved during the same session. The MCP server resolves that
ID once at startup, in priority order:

1. **`CORTEX_CONVERSATION_ID`** — an explicit override. The deterministic
   injection point: set this (e.g. from a wrapper or hook) to pin a known ID.
2. **`CLAUDE_CODE_SESSION_ID`** — Claude Code's real session ID, *if* it reaches
   the MCP server's environment.
3. **A per-process UUID** — minted as a fallback when neither of the above is
   usable (a value that arrives as an unexpanded `${...}` literal counts as
   unusable).

> **Caveat — Claude Code does not reliably pass `CLAUDE_CODE_SESSION_ID` to MCP
> servers.** It injects that variable into the environment of *tool* subprocesses
> (e.g. Bash), but a project-`.mcp.json` MCP server's process was observed
> **without** it. `.mcp.json` ships a `"CLAUDE_CODE_SESSION_ID":
> "${CLAUDE_CODE_SESSION_ID}"` forward attempt, but if Claude Code doesn't expand
> it (or doesn't hold the var itself), step 3 takes over.
>
> **What the per-process fallback means in practice:**
> - ✅ Within one MCP process, the summary and all facts share the same ID, so
>   recall links them correctly.
> - ⚠️ Reloading the MCP server mid-session mints a **new** ID — facts saved after
>   the reload won't link to summaries from before it.
> - ⚠️ The ID won't match Claude's real session ID for external cross-referencing.
>
> **For a rock-solid real ID**, set `CORTEX_CONVERSATION_ID` from a `SessionStart`
> hook (the hook receives the real `session_id` in its stdin JSON). The host-side
> CLI sidesteps all of this — it inherits the shell environment (where
> `CLAUDE_CODE_SESSION_ID` *is* present) and also accepts an explicit
> `--conversation` flag.

Memories are scoped by **namespace** + free-form **tags**. When a tool call omits
the namespace, the server uses a **per-project default detected at launch**: the
full git origin remote URL (e.g. `git@github.com:thomas-maurice/cortex.git`) if the
working directory is a repo with an origin, else the directory basename (e.g.
`2ndbrain`), else `global`. Set `DEFAULT_NAMESPACE` to override this for a given
registration. Claude can always pass an explicit `namespace` to file a memory
under a different scope.

The host-side [`cortex` CLI](#command-line-cli) adds `list`, `export`,
`reindex`, `status`, and `doctor` on top of these for terminal use and
maintenance.

## Configuration file (`cortex.yaml`)

Both the CLI and the MCP server read an optional config file so you don't have to
repeat `--server` / `CORTEX_AUTH_TOKEN` / etc. on every invocation. It lives at
`~/.config/cortex/cortex.yaml` by default (honouring `XDG_CONFIG_HOME`); override
the path with `--config <path>` (CLI) or `CORTEX_CONFIG=<path>` (CLI **and** MCP).

Every setting resolves in this order — **first non-empty wins**:

```
command-line flag  >  environment variable  >  cortex.yaml  >  built-in default
```

Scaffold a starter file with `cortex config init`, then edit it:

```yaml
# Cortex configuration, shared by the CLI and the MCP server.

# --- client settings ---
server: http://localhost:8088   # Cortex RPC server URL          (env: CORTEX_SERVER_URL)
token: ""                       # bearer token                   (env: CORTEX_AUTH_TOKEN)
namespace-default: global       # namespace used when none given (env: DEFAULT_NAMESPACE)
source: cli                     # source tag on saved memories   (env: MEMORY_SOURCE)

# --- MCP server defaults (applied when a tool call omits the field; 0 = defer to server) ---
mcp:
  search-limit: 10              # default max results for cortex_memory_search
  fact-limit: 50                # default max facts for cortex_recall_session
  max-distance: 0.55            # relevance cutoff, cosine distance; 0 = no cutoff (env: MAX_DISTANCE)
  timeout: 5s                   # per-call fail-fast deadline; a slow/unreachable server never blocks Claude (env: CORTEX_MCP_TIMEOUT)
```

A missing file is fine (the config is entirely optional); a malformed file, or a
path you explicitly pass that can't be read, fails loudly. Run `cortex config
show` to see the merged result and confirm which file was picked up.

The `mcp:` block sets the defaults the MCP tools use when Claude's call omits the
field. `max-distance` is the relevance cutoff (cosine distance; results farther
than this are dropped, `0` disables it) — its ideal value is **model-specific**
(see the CLI note below), so it lives in config where you can tune it per setup
rather than baked into the binary. The built-in fallback when there is no config
file is `0` (no cutoff), so MCP behaviour is unchanged until you opt in.

## Command-line (CLI)

`make build` also produces `./bin/cortex`, a host-side CLI that is itself a thin
Connect-RPC client of the server — handy for inspection, scripting, and
maintenance without going through Claude. It talks to the server only (no direct
NATS/Weaviate access); point it with `--server` / `CORTEX_SERVER_URL` and
authenticate with `--token` / `CORTEX_AUTH_TOKEN`.

| Command | What it does |
|---------|--------------|
| `cortex save "<text>" -n <ns> -t tag [-S <id>] [--expires 7d]` | Queue a memory (server publishes to NATS). `-S/--supersedes` lists ids this memory replaces (deleted after indexing). `--expires` (`7d`, `36h`) makes it a time-bound fact that stops being recalled after that long. `--no-expiry` keeps a merge (`-S`) permanent even when every superseded source expires. |
| `cortex edit <id> ["<text>"] [--expires 7d \| --no-expiry]` | Edit a memory's text (re-embedded) and/or its expiry. The text may be omitted when only the expiry changes: that is applied directly, with no re-embedding. |
| `cortex list -n '*' [-t tag] [-x tag] [--include-expired]` | List memories newest-first; filter by namespace/tags. Expired memories are hidden unless `--include-expired`. |
| `cortex get <id> [--json]` | Show one memory by id (incl. linked ids); `--json` uses the `export` record shape. |
| `cortex search "<q>" [-d 0.6] [-t tag] [-x tag] [--include-expired]` | Semantic search with a relevance cutoff and tag filters. Prints the matched excerpt (`matched: …`) under hits on long memories. Expired memories are hidden unless `--include-expired`. |
| `cortex consolidate "<topic>" [-n '*'] [-l N] [-t tag] [-x tag] [--include-expired]` | Print the cluster of memories about a topic + their manifest (read-only gather; the LLM does the merge). No tag flag = no tag filter (whole cluster). |
| `cortex list --stale [--since 90d] [--min-age 30d] [--include-preferences] [--include-expired] [-n ns] [-l N]` | Stale-memory review by ACTUAL USE: memories never used (or, with `--since`, not used for that long), oldest last-use first; only memories older than `--min-age` (default 30d, measured from the later of creation and tracking start; `0` disables) count; `preference`-tagged memories hidden unless `--include-preferences`; expired memories are skipped unless `--include-expired` (the expiry sweep handles them). Prints use count + last use, and "usage tracked since" with a limited-results warning. See "Stale-memory review". |
| `cortex delete <id>...` | Delete memories by ID (moved to the trash). Several ids use one `DeleteMany` call (max 500) with per-id results; non-zero exit if any failed. |
| `cortex tags [-n ns]` | List tags with memory counts and last activity. `tags rename <from> <to> [-n ns]` fixes drift (`k8s` / `kubernetes`; merges into an existing tag), `tags delete <tag> [-n ns] [--yes]` removes a tag from every memory. Touching the reserved `preference` tag needs `--yes`. |
| `cortex trash ls [-n ns] [--reason r]` / `trash restore <id>... \| --all [-n ns] [--reason r]` / `trash purge <id>... \| --all [-n ns] [--reason r] [--yes]` | Soft-delete trash: list trashed memories, restore them (re-queued for indexing, same id, with their recall history; ids are rejected if a live memory has that id; `--all` restores every match independently and reports failures) or purge them permanently. `cortex status` shows the trash count. |
| `cortex export -o backup.json` | Dump all memories (text + metadata, no vectors) to JSON. |
| `cortex import backup.json` | Restore a dump into the target via its NATS ingest queue (worker re-embeds). Preserves ids/links (over an existing memory: earliest `createdAt` wins, `conversationId` never blanked); point `--server` at the target. See [scripts/prod-to-dev.sh](scripts/prod-to-dev.sh). |
| `cortex reindex --yes` | Re-embed every memory through the worker (see below). |
| `cortex migrate status` | Admin: schema-migration history, pending migrations, migration lock holder and whether writes are paused (migrations themselves run automatically at server start). |
| `cortex migrate-mt [--yes]` | Migrate a non-MT store to multi-tenancy (one-shot; requires `CORTEX_MULTI_TENANT=true` on the server). |
| `cortex users list \| get <u> \| add <u> [--role] \| delete <u> \| set-role <u> <role> \| reset-password <u>` | Manage users (multi-tenant mode; needs an **admin** `--token`). The break-glass path to fix accounts without the web UI. |
| `cortex status` | Server health + store size (nats/weaviate/ollama/model/count). |
| `cortex doctor` | Per-check diagnostics from the server. |
| `cortex backup` / `backup list\|s3-list\|download\|delete <name>` | **Admin.** Full server-side backup: every tenant's memories + summaries plus the user/API-key registry (hashes), written as a timestamped JSON file to the server's backup dir — and, when the offsite (S3) config is enabled, also uploaded to the bucket. Manage local files with `list`/`download`/`delete`; list the offsite copies with `s3-list`. Set `BACKUP_INTERVAL` (+ `BACKUP_KEEP`, default 7) on the server for automatic periodic backups with retention. Offsite (S3) upload is configured entirely via server env vars (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` + `CORTEX_S3_BUCKET`/`CORTEX_S3_ENDPOINT`/`CORTEX_S3_PREFIX`/`CORTEX_S3_REGION`/`CORTEX_S3_USE_SSL`/`CORTEX_S3_ENABLED`); credentials are held in memory only, never stored or exposed. Works with AWS/MinIO/R2/SeaweedFS. |
| `cortex backup self [-o file]` | **Any user.** Download a backup of YOUR OWN data (memories + conversation summaries) to a local file. |
| `cortex restore <name> \| --file <local> \| --s3 <object-key> [--yes]` | **Admin.** Restore a full backup — by server-side filename, by uploading a local copy, or from the offsite S3 bucket (`--s3`, server downloads it; see `backup s3-list`): recreates missing users/API keys (existing ones untouched), re-queues every record into its original tenant; the worker re-embeds. Safe to re-run (upsert-by-id). Restoring over a memory that already exists keeps the earliest `createdAt` and never blanks its `conversationId` (a restore cannot revert a merge's provenance). |
| `cortex restore self <local-file> [--yes]` | **Any user.** Restore a self-backup into YOUR OWN tenant — tenant attribution in the file is ignored, so it can only ever write to yourself. |
| `cortex upgrade [--yes]` | Upgrade the local `cortex` + `cortex-mcp` binaries to the **server's** version: downloads the matching GitHub release, verifies its checksum, atomically replaces the binaries. Refuses on dev builds and major-version mismatches. Uses the public (unauthenticated) `GetVersion` RPC, so it works even with stale credentials. Also exposed to the LLM as the `cortex_upgrade` MCP tool — the MCP server probes the server version at startup and announces an available upgrade via its instructions. |
| `cortex version [--json]` | Print the CLI version and the server's version (public `GetVersion` RPC, no token needed). `--json` emits `{"client":...,"server":...}` for scripting; if the server is unreachable the CLI version is still printed and the exit code is non-zero. |
| `cortex summarize "<text>" --conversation <id>` | Save/update a conversation summary (unique per `--conversation`). |
| `cortex summaries [-n '*'] [-l N]` | List conversation summaries, most-recently-updated first. |
| `cortex recall "<query>"` | Recall the best-matching past session: summary + its facts. |
| `cortex hook recall [--timeout 5s]` | Claude Code **UserPromptSubmit** hook endpoint: reads the hook event on stdin, searches memories for the prompt (chunking long ones) and prints matches for injection into context. Fail-open, hard-bounded by `--timeout`. See [Automatic recall](#automatic-recall-userpromptsubmit-hook). |
| `cortex hook recall --dry-run [--prompt <text>]` | Preview what the hook **would** inject (server-side `PreviewRecall`): prints injected hits with distances, near misses with why they were excluded, and the settings used. Prompt from `--prompt` or stdin (raw text or hook-event JSON). Writes no recall state; never reinforces. Takes the same `-n/-l/-d/--min-chars/--max-chars/--max-query-chunks` flags as the hook (negative `-d` = no cutoff, `-l 0` = max 50), `--show-block` prints the exact injected block, and a hook-event `session_id` (or `--session-id`) marks memories already injected this session by reading the local recall-state db read-only. |
| `cortex state ls \| show <convo> \| clear <convo> \| purge [--older-than 168h]` | Inspect/maintain the LOCAL per-conversation state db (`~/.cache/cortex/cortex.db`) shared by the recall hook and MCP dedup: list conversations, show what was delivered to one, drop one (its memories become injectable again), or purge all idle past the threshold. No server needed. |
| `cortex config init` | Scaffold `~/.config/cortex/cortex.yaml` (won't overwrite without `--force`). |
| `cortex config show` | Print the effective config (flags + env + file merged) and which file was used. |

> `-d/--max-distance` is the relevance cutoff (cosine distance; `0` disables).
> Its ideal value is **model-specific** — qwen3's distances are compressed, so a
> tighter cutoff (~0.5–0.6) is right, while a different model needs a different
> number. Tune it against your own data.

### Changing the embedding model (reindex)

Vectors from different models are incompatible (different dimensions), so after
changing `OLLAMA_MODEL` you must re-embed. `cortex reindex` asks the server to:

1. **Back up** every memory to a timestamped JSON file (the safety net, written
   server-side under `CORTEX_BACKUP_DIR`; the response reports the path).
2. If the new model's dimension differs from what's stored, **drop and recreate**
   the Weaviate class (requires `--yes`).
3. **Republish** every record onto NATS; the worker re-embeds it with its
   currently configured model and re-stamps `model`/`dims`.

Point the **worker** at the new model first (`OLLAMA_MODEL` in `docker-compose.yml`,
then `docker compose up -d worker`), and make sure the CLI uses the same model,
before running `reindex`. Every memory records which model embedded it — visible
in `cortex list` and used by reindex to decide whether a rebuild is needed.

## Schema migrations and upgrades

Schema changes that Weaviate cannot apply in place (e.g. a property's tokenization)
are handled by versioned **schema migrations** that run **automatically** when the
server starts, before it serves. Nothing to do on upgrade: pull the new image and
restart. A migration copies the affected classes with their stored vectors
(**no re-embedding**) into a new versioned class and switches over atomically.
Indexing pauses briefly while the data is copied (each worker finishes the message it
is on, then stops fetching and NATS buffers; a write RPC like delete answers "maintenance, retry shortly"; reads keep
working) — seconds for a typical personal store, roughly one second per 8,000
objects — and resumes by itself. If the server is killed mid-migration, the next start
resumes it. With several server replicas, one migrates and the others wait; workers
wait until the schema is current.

**Upgrade order:** stop or upgrade every worker and server replica before the new
server boots. Older builds ignore the pause and cannot be detected, so a mixed fleet
can lose writes during a migration. If a worker cannot go idle within 2 minutes, or a
tenant is inactive, the migration aborts with a clear error and the server refuses to
start; fix the cause and restart (nothing is half-applied).

`cortex migrate status` (admin) shows the history, anything pending, and whether
writes are currently paused.

Migration #1 (`tags-field-tokenization`): classes become alias-backed
(`Memory` is an alias of `Memory_v1`, ...) and the **tags filter becomes exact and
case-sensitive** (it used to also match `Foo` and `foo-bar` when filtering by `foo`).
Tag values are unchanged. Fresh installs are born in this shape and skip it.
Developer docs: `docs/DESIGN.md` "Schema migrations".

## Multi-tenancy and migration (`cortex migrate-mt`)

Cortex isolates each user's memories via Weaviate native multi-tenancy, gated by
`CORTEX_MULTI_TENANT` — **on by default**. Every user's memories live in their own
Weaviate tenant; the authenticated user ID (from a JWT or API key) is the isolation
boundary — a client can never read or write another user's data. Set
`CORTEX_MULTI_TENANT=false` (on server **and** worker) for the legacy single-user
mode.

**Fresh install (default):** the schema is created with MT enabled automatically.
A **bootstrap admin** is created from `CORTEX_BOOTSTRAP_USER`/`CORTEX_BOOTSTRAP_PASSWORD`,
falling back to `CORTEX_UI_USER`/`CORTEX_UI_PASSWORD` (so the bundled compose works
out of the box with `admin`/`admin`); `CORTEX_AUTH_TOKEN` is registered as that
admin's API key, so existing MCP/CLI configs keep working. Because MT requires
authentication, **a deployment with no admin and no `CORTEX_AUTH_TOKEN` is locked
out** — set at least the UI creds or a token. Add more users in the UI (admin →
Users) or via `cortex users add`.

**Migrating an existing (non-MT) store:**

Weaviate cannot enable multi-tenancy on an existing class — the classes must be
dropped and recreated. `cortex migrate-mt` does this safely:

1. **Snapshot**: exports all memories + summaries to a server-side backup JSON file
   (same format as `cortex export`) before anything destructive. Chunks are NOT
   exported — they regenerate when the worker processes the re-import queue.
2. **Rebuild**: drops `Memory`, `MemoryChunk`, and `ConversationSummary` and
   recreates them with MT enabled.
3. **Re-import**: re-queues every snapshotted record for re-import into the
   **bootstrap admin's tenant** (`cortex-bootstrap`) via the normal NATS ingest
   queue. The worker re-embeds and rechunks each one.

```bash
# 1. Set the flag on the server and worker and restart them:
#    CORTEX_MULTI_TENANT=true  (docker-compose.yml or environment)
#    docker compose up -d server worker

# 2. Run the migration (server does all the work):
cortex --server http://localhost:8088 migrate-mt --yes

# The server reports the backup path, counts, and the tenant used.
# Wait for the worker to drain the queue (watch cortex status / the Indexing UI).
```

**Safety:**
- Refuses if `CORTEX_MULTI_TENANT` is off (set the flag first).
- Refuses if the classes are already MT (one-shot operation, nothing to redo).
- Re-running `cortex import <backup-file>` against the now-MT server completes a
  half-migrated store safely (re-import is upsert-by-id).
- All migrated data lands in the bootstrap admin's tenant. Redistributing data
  across multiple users is out of scope; this is a single-tenant-to-MT bootstrap.

## Trash (soft delete) and retention

Deletes are not immediate: `Delete` (CLI/MCP/UI), the `supersedes` deletes a
consolidation performs, and `DeleteNamespace` first copy each memory into a `Trash`
Weaviate class (same tenant, no vector; multi-tenant exactly like `Memory`) and only
then remove it. If the trash write fails, nothing is deleted. `cortex trash restore`
(or the UI / `cortex_trash_restore`) re-queues the record through the normal index
path (the worker re-embeds and re-chunks) under the SAME id; links are re-applied via
the link queue only to endpoints that still exist, and a restore never overwrites a
live memory with the same id. Trash entries are purged after `TRASH_RETENTION` (server
env; a Go duration such as `720h`, default `168h`, `0` keeps them forever; days like `30d` are NOT valid and an unparsable value stops the server at startup, since it controls permanent deletion) by an hourly sweep across all
tenants (logged with the purged count), or explicitly with `cortex trash purge`.

A restore keeps the memory's recall history (`accessCount`, `lastAccessedAt`) and
`model`/`dims`; `dupCandidates` and the index sequence are recomputed by the worker and
not kept. Links are re-applied only to endpoints that are live or restored in the same
request; a linked memory still in the trash keeps the reciprocal edge on its own entry, so
restoring it later re-links the pair. Purging (by id, `--all`, or the retention sweep) queues
link removals so live neighbours do not keep dangling ids. `Delete` accepts an optional
`reason` (`stale-review`, `duplicate-review`; default `delete`) so mass deletions can be
filtered with `--reason`.

Caveats: conversation summaries deleted by a namespace delete are NOT trashed; the
trash is **not part of backups** (full/self backups cover live memories + summaries
only); deleting a user (`DeleteUser`) deliberately wipes that user's trash along with
their memories. A reindex rebuild leaves the trash untouched, and `migrate-mt` carries
it over into the calling admin's tenant (entries keep their deletion time). Before the
classes are dropped the trash is also written to `<backup-name>.trash.json` next to the
migration backup; if re-writing it fails the error names that file, and `cortex import
<file>` re-creates its entries as live memories (extra fields are ignored). The sidecar and the reindex snapshot hold plaintext memories
(mode 0600), are never pruned and so outlive `TRASH_RETENTION` — delete them yourself.
Trash is not part of full backups: restoring a backup brings back live memories only
and the trash is lost.

## Expiry for time-bound facts

Some memories are true only for a while ("deploy freeze until Friday", "working on the
staging cluster this week"). Left alone they keep being recalled after they stop being
true. Give them an expiry:

- **Save:** MCP `expiresIn: "7d"`, CLI `cortex save --expires 7d`, the UI's expiry
  picker, or the API's `valid_until` (absolute) / `expires_in` (relative; `7d`, `36h`;
  must be > 0 and at most 10 years; exactly one of the two). When a save supersedes
  memories that ALL expire and sets no expiry, the merged memory inherits the latest
  source expiry; opt out with MCP `neverExpires: true`, CLI `--no-expiry` or API
  `never_expires` (exclusive with the other two).
- **Change:** `cortex edit <id> --expires 3d` / `--no-expiry`, or the inspector. An
  expiry-only change is a metadata merge — nothing is re-embedded.
- **Effect:** once `valid_until` has passed the memory is hidden from Search, List,
  find-similar, recall (including the `UserPromptSubmit` hook) and consolidation. Pass
  `include_expired` / `includeExpired` / `--include-expired` / the UI toggle to see
  them. Memories without an expiry are never affected.
- **Sweep:** an hourly server job moves memories that expired more than `EXPIRY_GRACE`
  ago (Go duration, default `168h`; `0` = as soon as they expire; invalid = startup
  failure) to the [trash](#trash-soft-delete-and-retention) with reason `expired`,
  across all tenants (logged with the count). During the grace period you can still
  rescue a memory by extending its expiry. Restoring a memory the sweep
  trashed clears its lapsed expiry (otherwise it would be hidden and re-swept at once);
  restoring one deleted for another reason keeps its expiry. Backup restores and imports keep
  expiries as-is and report how many restored records carry a lapsed expiry. Only memories
  the restore (re-)creates take the restored expiry — they come back hidden until it is
  changed; restoring over an EXISTING id keeps that memory's current expiry. A memory
  restored from the trash whose (non-`expired`-reason) expiry has lapsed likewise comes
  back hidden, and the restore says so. Memory counts
  (Status, Namespaces) cover live memories only, with expired ones shown separately.

## Stale-memory review

Memories that are never (or no longer) **used** are candidates for cleanup or merging.
"Used" is tracked separately from search ranking: the `useCount` / `lastUsedAt` Memory
properties are bumped by the `RecordUse` RPC, independent of `RERANK_WEIGHT` and of
reinforcement (`accessCount` / `lastAccessedAt`, which only move when reranking is on and
only for the top reinforcing hit). **What counts as a use** — a memory delivered with its
FULL text into an agent's context:

- the `cortex hook recall` prompt hook injecting it (only the memories it actually
  printed, after session dedup);
- the MCP server delivering it from `cortex_memory_search` (not stubs of already-delivered
  memories), `cortex_memory_get`, `cortex_recall_session` facts or `cortex_consolidate` clusters.

It does **not** count: UI or CLI searches/browsing, `cortex list`, the SessionStart
preferences load, `SearchSimilar`. Calls are fire-and-forget and fail-open.

**Tracking epoch.** The server records when usage tracking first started (`usage_tracking_since`
in the `cortex_meta` JetStream KV bucket, set once at first start). A never-used memory's
reference time is `max(createdAt, trackingSince)`, the min-age guard runs from that same
reference, and `List` returns `tracking_since`; the UI and CLI show "usage tracked since
<date>" and a notice when tracking began less than the requested window ago (results are
limited). Right after upgrading, expect an (almost) empty stale list for a while — that is
honest, not a bug. Memories used before tracking began are not remembered.

**Client versions matter.** Usage is only recorded by up-to-date clients: the `cortex` hook
and `cortex-mcp` must be recent enough to call `RecordUse` (run `cortex upgrade` on every
machine that uses Cortex). Memories used through older clients, or only found via CLI/UI
search, can look unused and be flagged; the tracking start date is server-wide, not
per client.

**Expiry.** Expired memories (`valid_until` passed) are skipped by the stale scan unless
`include_expired` is set: the expiry sweep owns them (trash reason `expired`), so bulk-trashing
them as `stale-review` would double-handle them.

`List` takes additive `stale` (`NEVER_RECALLED` = never used; `NOT_RECALLED_SINCE` +
`stale_days` = last use, or for never-used memories the reference time, older than N days),
`min_age_days` (default 30; 0 disables), `offset`, `include_preferences`; the response
carries `total` and `tracking_since`. Memories tagged `preference` are hidden unless
`include_preferences` (standing instructions are loaded wholesale, so they look unused).
Results are oldest-last-use first. It is a server-side cursor scan filtered in Go (an
`IsNull` filter would need `indexNullState` on existing classes), so it is correct beyond
10k memories; cost is one pass over the tenant per call, single-flight per tenant.
`DeleteMany` (max 500 ids, per-tenant single-flight, per-id results `deleted` / `not found` /
`failed`, other tenants' ids = "not found", optional `reason`, `stale-review` from the UI)
moves each id to the trash through the normal delete path (`reason` must be on the `Delete` allow-list: `delete`, `stale-review`, `duplicate-review`). `RecordUse` (max 100 ids)
increments under an in-process per-id lock (see docs/DESIGN.md). `useCount`/`lastUsedAt`
travel with backups, `cortex export`/`import`, and the trash, so history survives restore.

## Request size limit

The RPC handler rejects any request larger than `CORTEX_MAX_REQUEST_BYTES`
(default `268435456` = 256 MiB) with `resource_exhausted`. Restore uploads carry
a whole backup (bytes travel base64-encoded in the JSON the web UI sends, ~+33%),
so if a restore of a very large store fails with that error, raise it on the
server, e.g. `CORTEX_MAX_REQUEST_BYTES=1073741824`. Self-restore additionally caps
the backup payload at 100 MiB. See the trust model in `docs/DESIGN.md`.

## Observability (metrics + health)

All of these endpoints are **unauthenticated**; keep them off the public internet. `/metrics` is served only on a dedicated port, never on the API port.

| port | process | serves |
|------|---------|--------|
| 8080 (`CORTEX_LISTEN`) | server | the API + UI, plus `/healthz` and `/readyz` |
| 9090 (`API_METRICS_ADDR`) | server | `/metrics`, `/healthz`, `/readyz` |
| 9090 (`WORKER_METRICS_ADDR`) | worker | `/metrics`, `/healthz`, `/readyz` |

| endpoint | purpose |
|----------|---------|
| `GET /healthz` | Liveness: `200 ok` while the process serves HTTP. Checks no dependencies. |
| `GET /readyz` | Readiness: probes NATS, Weaviate and Ollama; `200` if all up, else `503`. Body is `{"nats":"ok","weaviate":"ok","ollama":"fail"}` (no error text). The result is cached ~1s and concurrent probes are shared, so request floods do not fan out to the backends. The worker adds its JetStream pull health. |
| `GET /metrics` | Prometheus exposition (dedicated registry + Go/process collectors). |

Env vars: `API_METRICS_ADDR` (server) and `WORKER_METRICS_ADDR` (worker), both defaulting to `:9090`; set to an empty string to disable the listener (no metrics are collected then). The ports are not published to the host in `docker-compose.yml`. A failure to bind is logged and never stops the service.

### Traefik / Prometheus

Traefik needs no change: the example router targets only port 8080 (`loadbalancer.server.port: '8080'`), so `/metrics` on `:9090` is never publicly routed. Scrape over the compose network instead. A plain compose DNS name resolves to all replicas of a scaled `worker` only through `dns_sd_configs` (type `A`), so use that for the worker:

```yaml
scrape_configs:
  - job_name: cortex-server
    static_configs:
      - targets: ['server:9090']
  - job_name: cortex-worker
    dns_sd_configs:
      - names: ['worker']   # one target per replica (--scale worker=N)
        type: A
        port: 9090
```

The runtime image is distroless (no wget/curl), so the compose healthcheck runs `cortex-server healthcheck`, which GETs the server's own `/readyz` (exit 0 = ready).

| metric | where | notes |
|--------|-------|-------|
| `cortex_rpc_requests_total{procedure,code}` | server | `code` is `ok` or a Connect code |
| `cortex_rpc_duration_seconds{procedure}` | server | histogram |
| `cortex_index_queue_pending` | server | JetStream index consumer backlog, read at scrape time |
| `cortex_queue_oldest_message_age_seconds` | server | age of the oldest message in the `MEMORY` queue; approaching `QUEUE_MAX_AGE` means unconsumed messages are about to be dropped |
| `cortex_dead_letters` | server | dead-lettered messages held in the stream |
| `cortex_worker_messages_total{subject_kind,outcome}` | worker | kind `index\|summary\|link`; outcome `ack\|retry\|dead\|term\|stale` (`stale` = acked and skipped because a newer version of that memory was already stored) |
| `cortex_embed_duration_seconds` / `cortex_embed_errors_total` | worker | Ollama embed latency (retries included) / failures |
| `cortex_index_duration_seconds` | worker | full index-message handling |
| `cortex_lock_infra_errors_total` / `cortex_lock_wait_seconds` | worker | lock-bucket (NATS KV) errors other than contention (each is repaired and retried) / time to acquire one per-id lock |
| `cortex_worker_consumer_recreated_total` | worker | durable consumers re-created after going missing (messages published meanwhile were discarded; also logged at Error) |
| `cortex_worker_fetch_errors_total` | worker | failed JetStream pulls. Worker `/readyz` fails once pulls have failed continuously for 30s. A wiped stream or deleted consumer is re-ensured automatically (see "Scaling the worker") |
| `cortex_worker_hold_timeouts_total` | worker | critical sections cut off by the lock hold deadline (the message is retried) |

## Scaling the worker

Workers scale horizontally: run as many replicas as the embedding backend can feed.

```bash
docker compose up -d --scale worker=3     # or `deploy: {replicas: 3}` in the compose file
```

`WORKER_METRICS_ADDR` is per container, so replicas do not clash (the port is not
published). `WORKER_MAX_PROCESSING` (default `10m`, `0` = off) caps one message's total processing time (then it fails and counts toward dead-lettering). `WORKER_CONCURRENCY` (default `4`) bounds in-flight messages per
consumer within one process. Correctness does not depend on a single worker: each
memory's read-modify-write runs under a per-id lock in a NATS KV bucket
(`cortex_locks`, 60s TTL), index messages carry their stream sequence so an
older edit can never overwrite a newer one (even across a recreated stream), and the index path only writes the
fields it owns (never links, reinforcement counters or not-duplicate decisions).
Details and the field-ownership table: [docs/DESIGN.md](docs/DESIGN.md#horizontal-scaling-worker-replicas).

Deleted consumer: `MEMORY` retains by **interest**, so acked history is gone and a
recreated durable consumer cannot replay anything. The worker therefore recreates a
deleted `indexer` / `linker` (and the stream, if wiped) by itself. Caveat: messages
published while a consumer is absent are discarded by JetStream (nobody was
interested), so keep that window short; the worker repairs it within about a second.
Only a legacy, not yet migrated stream (see below) is guarded: there the worker does
not recreate a consumer while the stream holds messages.

## NATS retention (queue and dead letters)

Two streams, both file-backed:

- `MEMORY` (subjects `memory.index`, `memory.summary`, `memory.link`) is a work
  queue with **InterestPolicy** retention: a message is deleted once every durable
  consumer whose filter matches it (`indexer`: index+summary, `linker`: link) has
  acked it, so disk use tracks the backlog, not history. `QUEUE_MAX_AGE` (default
  `168h`, `0` = no cap) is a safety net: a message nobody acked within it is
  **dropped**, so keep it well above any realistic worker outage.
- `MEMORY_DEAD` (subject `deadletter.memory`) holds dead letters until
  `DEAD_LETTER_MAX_AGE` (default `720h` = 30 days, `0` = keep) and is capped at 10 000
  messages / 512 MiB (oldest dropped). `cortex dead`, requeue and purge use it.

Both variables are Go durations parsed strictly (`30d` is an error: use `720h`; an
invalid value stops the server/worker) and must have the same value on server and
worker (each applies them at startup).

Automatic migration: on startup, server and workers convert an old `MEMORY` stream
(unbounded LimitsPolicy; JetStream cannot change retention in place). The old stream
is only replaced once both consumers have nothing pending or unacked; its dead
letters are copied to `MEMORY_DEAD` first (idempotent), then `MEMORY` and the
consumers are recreated. If the workers are still behind, nothing is touched: the
stream keeps serving, a retry runs every 30s (logged as `MEMORY stream migration
deferred`) and it completes once the backlog drains. A KV lock (`cortex_migrations`)
makes concurrent replicas safe. During the final seconds of the migration publishes
fail (the client sees an error and retries) rather than being lost. Upgrade the
server and ALL workers together: after the migration an old binary cannot start
(retention is immutable) and an old worker's dead-letter publish fails. Rolling back
is not supported without re-creating the old LimitsPolicy `MEMORY` stream by hand;
roll forward. Watch `cortex_queue_oldest_message_age_seconds`: approaching
`QUEUE_MAX_AGE` means workers are stuck and messages are about to be dropped.

Upgrade notes: a `cortex_locks` bucket created by a pre-release build with memory
storage must be deleted once (`nats kv del cortex_locks`), because JetStream cannot
change a bucket's storage type and the worker would fail to start. Objects written
before `indexedStream` existed carry no stream identity; if the `MEMORY` stream is
recreated before they are next edited, a message with a low sequence can be skipped
as stale for such an object (the guard then compares sequences as before); editing
the memory (or re-importing it) once the stream has caught up past the stored
sequence, or deleting it first, stamps the identity and clears it.

Caveat: multiple **server** replicas can lose reinforcement increments
(`accessCount`, a ranking signal only) because the server read-modify-writes it
without a lock.

## Embedding model

Ollama does all the embedding. The default is **`qwen3-embedding:0.6b`** (1024-dim,
639 MB, CPU-viable) — multilingual (handles French notes), 32K context, and
stronger recall than the lighter alternatives. If you want a smaller/faster
footprint for short English notes, **`nomic-embed-text`** (768-dim, 274 MB) is the
lean option; **`bge-m3`** is best once you ingest long documents.

Full evaluation, benchmark table, and the swap procedure (including the
**re-index gotcha** — changing models changes vector dimensions and requires
re-indexing) are in [`docs/EMBEDDING_MODELS.md`](docs/EMBEDDING_MODELS.md).

## Chunked retrieval

Memories are indexed as **chunks**, not just as one whole-document vector. A long
memory's single vector averages its content together, so a query about a specific
fact buried inside it matches weakly. The worker therefore splits each memory into
overlapping, token-bounded chunks (default **512 tokens, 64 overlap**;
`CHUNK_MAX_TOKENS` / `CHUNK_OVERLAP_TOKENS`), embeds each, and **search runs
against the chunks**, resolving hits back to their parent memory. The whole-memory
vector is still stored and used for duplicate detection and "find similar".

It is **optional and backward-compatible**: `CHUNKING_ENABLED` (default `true`,
set the same on worker + server) turns it off to revert to whole-memory search,
and when on, search **falls back to whole-memory vectors for any memory that has
no chunks** — so enabling it on an existing store does not require a flag-day
reindex (run `cortex reindex` at leisure for the full benefit).

In a controlled experiment this improved retrieval on long memories
(recall@1 +8.3pp, MRR +0.054) and left short-memory recall unchanged. Full design,
methodology, and before/after numbers are in [docs/CHUNKING.md](docs/CHUNKING.md);
the harness is `scripts/recall_accuracy.py`.

## Seeding a dev stack with fake data

To poke at the UI without using real memories, load the bundled fake dataset
(`testdata/seed-memories.json` — invented worldbuilding, pre-wired links, in the
`cortex export`/`import` format) into your dev stack:

```bash
make build   # produces ./bin/cortex
./bin/cortex --server http://localhost:8088 import testdata/seed-memories.json
```

It lands in `demo-*` namespaces (so it's easy to spot and bulk-delete from the
**Namespaces** view). Format and editing rules are in
[`testdata/README.md`](testdata/README.md). To copy *real* data from another
Cortex into a dev stack instead, use `scripts/prod-to-dev.sh` (export + import).

> Pass `--server` explicitly. A bare `cortex import` uses your `~/.config/cortex`
> config, which may point at a remote/prod server — `--server` (highest
> precedence: flag > env > file > default) is what targets the local stack.

## Manual smoke test (no Claude needed)

```bash
# publish a memory straight onto the stream
docker compose exec -T nats nats pub memory.index \
  '{"id":"11111111-1111-1111-1111-111111111111","text":"Thomas prefers Go for backend services","namespace":"global","tags":["pref"],"source":"manual","createdAt":"2026-06-11T12:00:00Z"}'

make logs   # worker should log: indexed id=1111... dims=1024
```

(Requires the `nats` CLI in the container image; otherwise just use the Claude
tools, which publish the same payload.)

## gRPC reflection (Bruno, grpcurl, Postman)

The server exposes the same API over **gRPC** — Connect serves the Connect, gRPC,
and gRPC-Web protocols on one port — with **server reflection** enabled, so gRPC
clients can introspect and call it without a local `.proto`.

```bash
# local (h2c, plaintext)
grpcurl -plaintext localhost:8088 list
grpcurl -plaintext localhost:8088 list cortex.v1.MemoryService
grpcurl -plaintext -d '{"query":"go","namespace":"*","limit":3}' \
  localhost:8088 cortex.v1.MemoryService/Search

# through Traefik (TLS) — no -plaintext
grpcurl cortex.example.com:443 list
```

In **Bruno** (or Postman): create a gRPC request pointing at the server
(`localhost:8088` plaintext, or `cortex.example.com:443` with TLS); reflection
populates the method list automatically. When the server has a token, add an
`authorization` metadata header with value `Bearer <token>` — actual RPC calls go
through the auth interceptor. Reflection itself is open (it exposes only the
schema, which is already public in `proto/`).

## Releases

Pushing a version tag (`git tag v1.2.3 && git push --tags`) triggers two
workflows in parallel:

- **`build.yml`** → builds and pushes the multi-arch Docker image to
  `ghcr.io/thomas-maurice/cortex`, tagged with the version (`:1.2.3`, `:1.2`,
  `:latest`).
- **`release.yml`** → runs **GoReleaser** to build standalone binaries and
  publish a GitHub Release with `tar.gz` archives for **linux & macOS × amd64 &
  arm64**, plus `checksums.txt`.

Each archive bundles all four binaries (`cortex-server`, `cortex-worker`,
`cortex-mcp`, `cortex`) — so you can run the server/worker on a box and the CLI +
MCP client on your laptop. The version is stamped into every binary
(`-ldflags -X main.version`) and into the Docker image, so `cortex --version` and
the server's `Status` report the release tag; un-tagged/local builds report `dev`.

## Project layout

```
proto/cortex/v1/   Connect RPC service + message definitions
gen/cortex/v1/     generated Go (protoc-gen-go + protoc-gen-connect-go)
cmd/server/        Connect RPC server — the only owner of NATS/Weaviate/Ollama
cmd/mcp/           MCP server (stdio) — thin RPC client Claude talks to
cmd/cli/           cortex CLI — thin RPC client for the terminal
cmd/worker/        NATS consumer — embeds via Ollama, writes to Weaviate
internal/
  memory/          shared data model (Record, Hit) + stream/class names
  bus/             NATS JetStream: connect, stream, publish
  chunk/           token-bounded, overlapping text splitter (tiktoken, offline)
  recall/          recall-hook query logic (chunk sampling, merge, limit/cutoff) shared by the hook and PreviewRecall
  embed/           Ollama embeddings client
  store/           Weaviate: schema, upsert, chunk-based gRPC search, links, delete; alias helpers, class copy/swap
  migrate/         schema-migration framework (registry, runner, lock, maintenance mode)
  rpc/             Connect service impl, auth (token+JWT), login, JWT, client helper
ui/                embedded Vue 3 SPA (Memories, Graph, Explore, Recall preview, Sessions, Namespaces, Tags, Duplicates, Trash, Preferences, Backup, Indexing, Status)
testdata/          fake seed memories for a dev stack (cortex import format)
docker-compose.yml   nats + weaviate + ollama + worker + server
deploy/truenas/      TrueNAS Scale compose (non-root, host bind mounts, Traefik h2c)
Dockerfile           node (UI) + go multi-binary build → distroless
.goreleaser.yaml     binary release build (linux/macOS × amd64/arm64)
.github/workflows/   test (PRs) · build (image → ghcr) · release (GoReleaser)
testdata/            fake seed memories + recall fixtures for a dev stack
scripts/             install, prod→dev copy, recall-accuracy harness
docs/                DESIGN.md, WEB_UI.md, CHUNKING.md, EMBEDDING_MODELS.md, RESEARCH.md
```

## Roadmap (extension points, already designed for)

- **Richer auth** — OIDC and/or per-client API keys (the `Authenticator`
  interface and bearer-token interceptor are the seam this slots into)
- LLM **extraction + dedup** (mem0-style ADD/UPDATE/DELETE/NOOP) in the worker
- **Temporal** validity windows (Zep-style "what was true last week")
- **SeaweedFS/S3** blob storage for document/file ingestion
- Scheduled **reflection / decay**

The scope is infinite — this is the foundation.
