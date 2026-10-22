# Cortex — Web UI

The `cortex-server` binary serves a Vue 3 single-page app on the same port as the
Connect RPC API. It's embedded in the Go binary, so there is nothing extra to
deploy: ship one image, get the API and the UI.

## Architecture

```
browser ──HTTP/JSON──▶ cortex-server (:8080)
                         ├── /auth/login        → JSON, mints a JWT
                         ├── /cortex.v1.*        → Connect RPC (MemoryService)
                         └── /  (catch-all)      → embedded SPA (ui/dist)
```

- The SPA lives in `ui/` (source) and is built to `ui/dist`, which is embedded
  via `//go:embed all:dist` in `ui/embed.go`. `ui.Handler()` serves the assets
  and falls back to `index.html` for unknown paths so vue-router history-mode
  deep links (e.g. `/graph`) resolve.
- The browser talks to the **same** Connect API the MCP server and CLI use, via
  typed Connect-Web clients (`createClient` from `@connectrpc/connect` v2) driven by the
  service descriptor `protoc-gen-es` generates from the proto (`ui/src/utils/connect.js`).
- One origin, no CORS: in production the SPA is served by the server; in dev the
  vite dev server proxies `/cortex.v1` and `/auth` to `:8080`.

### Stack

Vue 3 + vue-router 5 + pinia 4 (+ persistedstate 4, `pick` option) + vite 8 + Tailwind CSS v4 + shadcn-vue
(reka-ui primitives, generated into `ui/src/components/ui/`) + lucide icons +
`@connectrpc/connect-web` + vis-network (graph/explore). Light/dark theme
toggle in the navbar (persisted, defaults to the OS preference).

## Build pipeline

| Step | Command | What it does |
|------|---------|--------------|
| TS types + service descriptor | `make proto-ui` | `buf generate --template buf.gen.ui.yaml` → `ui/src/gen/cortex/v1/cortex_pb.ts` (protobuf-es v2 `protoc-gen-es` only; v2 has no separate `protoc-gen-connect-es` plugin and no `*_connect.ts`) |
| UI build | `make ui` | `npm install && vite build` → `ui/dist` |
| Server | `make build` (deps on `ui`) / `make image` | `go build` embeds `ui/dist` |

- `ui/dist` is git-ignored except a committed `.gitkeep`, so `go build` always
  compiles even on a fresh checkout (it just serves "web UI not built" until you
  build the assets).
- The Dockerfile has a `node` stage that builds `ui/dist` and overlays it into
  the Go build stage, so `make image` is self-contained.
- CI (`.github/workflows/test.yml`) builds the UI before `go test` because the
  embed test needs a real `index.html`.

## Auth

Single-user by design today, with the seam for multi-user already in place.

1. **Login** — `POST /auth/login {username, password}` (`internal/rpc/login.go`)
   checks against `CORTEX_UI_USER` / `CORTEX_UI_PASSWORD`. The password may be
   plaintext (constant-time compared), an **argon2id** PHC hash (`$argon2id$…`),
   or a **bcrypt** hash (`$2a$…`) — auto-detected by prefix; generate one with
   `cortex hash-password`. On success it mints an **HS256 JWT**
   (`internal/rpc/jwt.go`, 12h TTL) carrying `username` + `role` claims. If no
   password is configured, UI login is disabled (the API is still usable by
   MCP/CLI via the static token).
2. **API auth** — the Connect interceptor (`internal/rpc/auth.go`) accepts
   **either** the static `CORTEX_AUTH_TOKEN` (MCP/CLI) **or** a valid UI JWT
   (browser) via a `multiAuth`. With no token set, the server is open (dev).
3. **Client** — the auth store (`ui/src/stores/auth.js`) persists the JWT in
   localStorage and decodes it for `username`/`role` (decode only — the server is
   the sole trust authority and validates every request). `checkAuth()` also
   checks `exp` so an expired/stale token (e.g. after a server restart with a new
   secret) logs out cleanly instead of bouncing the user off every page.

### JWT secret precedence (`cmd/server/main.go`)

1. `CORTEX_JWT_SECRET` if set (explicit).
2. else `sha256("cortex/jwt-secret/v1:" + CORTEX_AUTH_TOKEN)` — a **stable**
   secret so sessions survive restarts, without using the API token bytes
   directly as the signing key.
3. else 32 random bytes — per-process only; UI sessions die on restart (logged).

### Multi-user (deferred)

The JWT already carries a `role` claim and the login handler is the only
credential check, so multi-user is a backend-only change: swap the single
user/pass check for a user store and emit per-user claims.

## Views

| Route | View | RPCs used |
|-------|------|-----------|
| `/` | **Memories** — search/list (search hits on long memories show a highlighted `matched:` excerpt; the inspector highlights the matched span in the full text), add (New Memory form, expiry picker), delete, *Include expired* toggle | `Search`, `List`, `Save`, `Delete` |
| `/graph` | **Graph** — memory map + explicit links | `List`, `Search`, `Link`, `Unlink` |
| `/explore` | **Explore** — query → relevance cloud | `Search` |
| `/recall-preview` | **Recall preview** — what would the recall hook inject for a prompt? | `PreviewRecall` |
| `/sessions` | **Sessions** — conversation summaries (edit in place) | `ListSummaries`, `SummarizeSession` |
| `/namespaces` | **Namespaces** — per-namespace counts; rename / delete a whole namespace | `ListNamespaces`, `RenameNamespace`, `DeleteNamespace` |
| `/tags` | **Tags** — per-tag counts + last activity; rename / merge / delete; click opens Memories filtered by the tag (`/memories?tag=…&namespace=…`) | `ListTags`, `RenameTag`, `DeleteTag` |
| `/duplicates` | **Duplicates** — side-by-side review of worker-flagged duplicate pairs; dismiss / keep one / merge | `ListDuplicateCandidates`, `DismissDuplicate`, `Delete`, `RestoreFromTrash`, `Save` (`supersedes`) |
| `/trash` | **Trash** — deleted memories; restore / purge one / purge all (confirm dialogs) | `ListTrash`, `RestoreFromTrash`, `PurgeTrash` |
| `/preferences` | **Preferences** — `global` + `preference` memories, edit/add/delete | `List`, `Save`, `UpdateMemory`, `Delete` |
| `/backup` | **Backup** — export / import memories (JSON, no vectors) | `List`, `RestoreMemories` |
| `/queue` | **Indexing** — live index-queue counts + dead letters; auto-refreshes | `IndexQueue`, `Dead` |
| `/status` | **Status** — backend health + counts | `Status` |

### Namespaces (`ui/src/views/NamespacesView.vue`)

A table of every namespace with its memory count, summary count, and last
activity. **Rename** is inline and metadata-only — the server PATCHes the
`namespace` field on each object (memories *and* conversation summaries), so
nothing is re-embedded; renaming into an existing namespace merges the two.
**Delete** removes an entire namespace and is guarded by a typed confirmation (you
must type the namespace name): its memories are moved to the trash (restorable), its
summaries are permanently removed.

### Expiry (`ExpiryPicker.vue`, `ExpiryBadge.vue`, `lib/expiry.js`)

Time-bound facts get an expiry. The picker (New memory form; the inspector's edit mode)
offers *Never expires / 1 day / 1 week / 1 month / Custom date* (a custom date means end
of that local day; the edit mode adds *Keep current*, the default when a memory already
expires, so an unrelated edit never changes the expiry). Presets are sent as
`expiresIn`, a custom date as `validUntil`, "never" on an expiring memory as
`clearValidUntil`. An expiry-only edit is applied directly by the server (no re-embed)
and toasts "Updated". Memories carry an "expires in 3d" badge (or a red "expired" one).
Expired memories are hidden by the server; the Memories view's *Include expired* switch
sets `includeExpired` on `Search`/`List`. Expired memories are moved to the Trash (reason
`expired`) by the server's hourly sweep after `EXPIRY_GRACE`.
### Duplicates (`ui/src/views/DuplicatesView.vue`)

Lists the pairs the worker flagged at index time. The server returns directed groups
(A lists B, B usually lists A); `utils/duplicates.js` (`buildPairs`, pure, no Vue)
collapses them so each unordered pair shows once, older memory on the left. Per pair:
**Not duplicates** -> `DismissDuplicate`; **Keep left/right** -> confirm, `Delete` the
other (moves to Trash; the toast has **Undo** -> `RestoreFromTrash`); **Merge…** opens a
dialog prefilled with both texts, the union of tags and the left namespace, and saves
with `supersedes` = both ids (the worker trashes both originals only after the merge is
indexed and carries their links over to the merged memory; the pair is hidden locally meanwhile). **Keep left/right** calls `MergeInto` (carries links, dismissals, stats, createdAt onto the kept memory before trashing the other). A side that is a `global` + `preference` memory gets a badge and a warning when the result would no longer be one. The truncation banner uses the server's `truncated` flag. Namespace filter is server-side
(`*` = all). No pair distance is shown (the RPC does not return it). No nav badge.

### Trash (`ui/src/views/TrashView.vue`)

Table of soft-deleted memories (text excerpt, namespace, deleted-at, reason) newest
first, with the server's retention shown. **Restore** re-queues a memory for indexing
under its original id; **Purge** deletes one permanently. The view pages (50/page), filters by
namespace and reason, supports multi-select with **Restore selected** / **Purge selected**, and
**Restore all (matching)** / **Purge all (matching)** (the latter requires typing `purge`). Deleting from the Memories view / inspector says "moved to trash".

### Tags (`ui/src/views/TagsView.vue`)

A table of every tag (exact, case-sensitive: `k8s` and `K8s` are separate rows)
with its memory count and "Newest memory" (createdAt of the newest memory carrying it), a filter box and an
optional namespace scope. **Rename** is inline; when the target already exists the
view says so and the button becomes **Merge** (memories carrying both end up with
the tag once). **Delete** removes the tag from every memory but keeps the
memories. Both are metadata-only (memories *and* their search chunks are patched,
nothing is re-embedded). The reserved `preference` tag (it drives the
standing-preferences hook) is refused by the server unless `confirm_preference`
is set; the UI asks for explicit confirmation first. Clicking a tag opens
Memories with that tag pre-selected (`?tag=` / `?namespace=` query params); because
the server filter is fuzzy, that view re-filters to exact tags, doubling the query
limit (up to 10 rounds / 10000 rows) until a page fills, and notes when fuzzy
matches were hidden. Rename/delete use the namespace the counts were loaded with
(Refresh to change it), never the live input box. Editing a memory in the inspector
or Preferences view only sends tags when the tag field was actually changed, so a
stale loaded list cannot undo a rename.

### Indexing (`ui/src/views/QueueView.vue`)

Shows the async index queue: pending, in-flight, and dead-lettered counts, plus
the list of failed memories (with requeue / purge). It **polls every second while
mounted** — indexing is fast and bursty (and usually driven in the background by
the agent or a bulk import), so a one-shot snapshot would almost always read
`0/0/0`; the poll makes a burst visible as it drains.

### Graph (`ui/src/views/GraphView.vue`)

A force-directed graph of your memories (vis-network). Nodes are memories,
coloured by namespace. Three kinds of relationship:

- **Explicit links** (solid green) — stored in `linkedIds`. Toggle **Connect**
  mode, click memory A then B to create one (`Link`); click a green edge to
  remove it (`Unlink`).
- **Semantic neighbours** (dashed blue, on demand) — double-click a memory or use
  "Find similar" (in the memory inspector's header, or in the slim on-canvas panel
  once the inspector is closed) to run a vector `Search` for its nearest
  neighbours, gated by the **Similar ≤ dist** cutoff (weak matches are dropped,
  not drawn). "Clear added" removes them.
- (Tags are shown in the click-to-inspect details panel, not as nodes.)

Reliability details: the layout uses a fixed random seed (reproducible across
reloads), auto-fits after stabilization, and the stabilization listener is reset
on each reload to avoid double-fit on rapid reloads.

### Explore (`ui/src/views/ExploreView.vue`)

Type any text → it's vectorised server-side (`Search`) and matched against your
memories, rendered as a cloud radiating from a central query node. Closer +
bigger = more relevant; edge length encodes distance; edge label is the distance.
A **Max dist** cutoff drops weak matches. Searches carry a monotonic request id so
a slow query can't overwrite a newer one's results.

### Recall preview (`ui/src/views/RecallPreviewView.vue`)

Paste a prompt → `PreviewRecall` runs the same query logic as `cortex hook
recall` (`internal/recall`: chunk + sample long prompts, concurrent search, merge
by best distance, limit/cutoff). Inputs: namespace, limit, max distance (slider),
min chars, max query chunks — defaulting to the hook's own defaults (3 / 0.5 / 12
/ 4); "Hook defaults" resets them. Output: the query chunks used, the memories
that **would be injected** (distance bar, matched chunk), a divider at the cutoff,
and greyed **near misses** tagged *over max distance* or *over limit*. A prompt the
hook would skip (slash/bang command, under min chars) says so. Read-only: tenant-
scoped, no reinforcement. **Per-session dedup is not simulated** (client-local
bbolt state the server cannot see) and the view says so. Near-miss distances come
from a second, wider search, so they can differ slightly from the injected set's.
Each injected hit shows a *truncated to N of M chars* badge when the hook's `--max-chars` would cut it, and a "Show injected text" expander prints the exact `<cortex-recall>` block (`injected_block`, built by the same `recall.FormatBlock` the hook uses). An empty namespace is sent as `*` (the hook's scope). CLI twin: `cortex hook recall --dry-run`.

## Memory linking — three ways things get connected

1. **The model** — Claude links related memories via the MCP tools
   `cortex_memory_link` / `cortex_memory_unlink` (it gets ids from
   `cortex_memory_search`). Told to do this proactively when two memories are
   meaningfully related.
2. **You** — Connect mode in the Graph view.
3. **Derived** (not stored) — dashed semantic neighbours and the Explore cloud,
   computed live from embeddings.

Explicit links (1 and 2) write the same `linkedIds` field through the same
`Link`/`Unlink` RPCs, so a link Claude makes shows up as a green edge and vice
versa. Storage/consistency details are in [DESIGN.md](DESIGN.md#data-model).

## Dev workflow

```bash
# terminal 1: backend (needs nats/weaviate/ollama, e.g. `make up`)
go run ./cmd/server          # listens on :8080

# terminal 2: hot-reloading UI
cd ui && npm run dev         # http://localhost:5173, proxies to :8080
```

After changing the proto: `make proto` (Go) and `make proto-ui` (TS types + service
descriptor). Needs `ui/node_modules` (`cd ui && npm ci`). Node >= 22.12 is required (vite 8).

protobuf-es v2 notes: messages are plain objects (no classes) — pass object literals to
RPCs; build with `create(Schema, ...)` only if you need a full message. Timestamps are
plain `{seconds, nanos}`: use `timestampDate` / `timestampFromDate` from
`@bufbuild/protobuf/wkt` (there is no `ts.toDate()`). `int64` fields are `bigint`.

## File map

```
ui/
  embed.go                 go:embed of dist + SPA handler
  vite.config.js           build + dev proxy
  buf.gen.ui.yaml          (repo root) protoc-gen-es (v2) TS generation
  src/
    main.js                pinia, router, Tailwind/theme css
    lib/theme.js           light/dark toggle (persisted)
    lib/utils.js           cn() class merger (shadcn)
    components/ui/         generated shadcn-vue components
    App.vue                navbar + router-view
    router/index.js        routes + auth guard
    stores/auth.js         JWT store (persisted, expiry-checked)
    gen/cortex/v1/cortex_pb.ts  GENERATED messages + MemoryService descriptor
    utils/connect.js       typed Connect client (createClient; attaches the JWT)
    utils/duplicates.js    pure pair-grouping/merge-draft helpers (Duplicates view)
    utils/api.js           /auth/login client
    components/Expiry*.vue expiry picker + badge (helpers in lib/expiry.js)
    views/                 LoginView, MemoriesView, GraphView, ExploreView,
                           RecallPreviewView,
                           SessionsView, NamespacesView, TagsView, DuplicatesView, TrashView, PreferencesView,
                           BackupView, QueueView (Indexing), StatusView
internal/rpc/
  jwt.go                   HS256 issue/parse
  login.go                 POST /auth/login
  auth.go                  token-or-JWT multiAuth
  service.go               Link/Unlink (mutex-guarded)
```
