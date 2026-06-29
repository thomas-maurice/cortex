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
  typed Connect-Web clients generated from the proto (`ui/src/utils/connect.js`).
- One origin, no CORS: in production the SPA is served by the server; in dev the
  vite dev server proxies `/cortex.v1` and `/auth` to `:8080`.

### Stack

Vue 3 + vue-router + pinia (+ persistedstate) + Bootstrap 5 + FontAwesome +
`@connectrpc/connect-web` + vis-network (graph/explore). Mirrors the
`thomas-maurice/nis` stack.

## Build pipeline

| Step | Command | What it does |
|------|---------|--------------|
| TS clients | `make proto-ui` | `buf generate --template buf.gen.ui.yaml` → `ui/src/gen` |
| UI build | `make ui` | `npm install && vite build` → `ui/dist` |
| Server | `make build` (deps on `ui`) / `make image` | `go build` embeds `ui/dist` |

- `ui/dist` is git-ignored except a committed `.gitkeep`, so `go build` always
  compiles even on a fresh checkout (it just serves "web UI not built" until you
  build the assets).
- The Dockerfile has a `node:22` stage that builds `ui/dist` and overlays it into
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
| `/` | **Memories** — search/list, add (New Memory form), delete | `Search`, `List`, `Save`, `Delete` |
| `/graph` | **Graph** — memory map + explicit links | `List`, `Search`, `Link`, `Unlink` |
| `/explore` | **Explore** — query → relevance cloud | `Search` |
| `/sessions` | **Sessions** — conversation summaries (edit in place) | `ListSummaries`, `SummarizeSession` |
| `/namespaces` | **Namespaces** — per-namespace counts; rename / delete a whole namespace | `ListNamespaces`, `RenameNamespace`, `DeleteNamespace` |
| `/preferences` | **Preferences** — `global` + `preference` memories, edit/add/delete | `List`, `Save`, `UpdateMemory`, `Delete` |
| `/backup` | **Backup** — export / import memories (JSON, no vectors) | `List`, `RestoreMemories` |
| `/queue` | **Indexing** — live index-queue counts + dead letters; auto-refreshes | `IndexQueue`, `Dead` |
| `/status` | **Status** — backend health + counts | `Status` |

### Namespaces (`ui/src/views/NamespacesView.vue`)

A table of every namespace with its memory count, summary count, and last
activity. **Rename** is inline and metadata-only — the server PATCHes the
`namespace` field on each object (memories *and* conversation summaries), so
nothing is re-embedded; renaming into an existing namespace merges the two.
**Delete** removes an entire namespace's memories and summaries and is guarded by
a typed confirmation (you must type the namespace name).

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
  "Find similar" to run a vector `Search` for its nearest neighbours, gated by the
  **Similar ≤ dist** cutoff (weak matches are dropped, not drawn). "Clear added"
  removes them.
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

After changing the proto: `make proto` (Go) and `make proto-ui` (TS clients).

## File map

```
ui/
  embed.go                 go:embed of dist + SPA handler
  vite.config.js           build + dev proxy
  buf.gen.ui.yaml          (repo root) TS Connect client generation
  src/
    main.js                pinia, router, Bootstrap, FontAwesome
    App.vue                navbar + router-view
    router/index.js        routes + auth guard
    stores/auth.js         JWT store (persisted, expiry-checked)
    utils/connect.js       typed Connect client (attaches the JWT)
    utils/api.js           /auth/login client
    views/                 LoginView, MemoriesView, GraphView, ExploreView,
                           SessionsView, NamespacesView, PreferencesView,
                           BackupView, QueueView (Indexing), StatusView
internal/rpc/
  jwt.go                   HS256 issue/parse
  login.go                 POST /auth/login
  auth.go                  token-or-JWT multiAuth
  service.go               Link/Unlink (mutex-guarded)
```
