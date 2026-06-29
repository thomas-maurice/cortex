# testdata — seed memories for the dev stack

`seed-memories.json` is a set of **fake, invented** memories you can load into a
local dev stack to have something to look at in the UI (Memories, Graph, Explore,
Sessions, Namespaces) without touching real data. The content is deliberately
fictional worldbuilding (a made-up space station, "Halcyon Station") — it is not
tied to anyone real, so it's safe to ship and screenshot.

## Format

The file is a JSON array in the exact shape `cortex export` produces and
`cortex import` consumes — so it round-trips through the normal ingest path. Each
record:

```json
{
  "id": "83585949-9f17-44f0-8e8e-5f8742de9f6b",   // REQUIRED to be a UUID; lets us pre-wire links
  "text": "…the memory content (the only field that gets embedded)…",
  "namespace": "demo-station",
  "tags": ["station", "overview", "vesper-prime"],
  "source": "seed",
  "createdAt": "2026-06-01T09:12:00Z",            // RFC3339
  "linkedIds": ["e21adf03-…", "a6a11abe-…"]        // optional; ids of other records
}
```

Notes / gotchas:

- **`id` must be a UUID.** Weaviate object ids are UUIDs, and we set them
  explicitly so `linkedIds` can reference siblings *in the same file*. Import
  upserts by id, so re-importing the file is idempotent (it overwrites, never
  duplicates).
- **`linkedIds` must be symmetric.** Links are bidirectional but `import` writes
  exactly what's in the field — it does not auto-mirror — so if A links to B, B
  must also list A. The seed file is already symmetric; keep it that way when
  editing.
- **No vectors, no `model`/`dims`.** Those are recomputed by the target worker's
  embedding model on import, so the seed data is safe across model changes. Leave
  them out.
- Everything is under `demo-*` namespaces (`demo-station`, `demo-crew`,
  `demo-tech`, `demo-ports`, `demo-lore`) so it's easy to spot and bulk-delete
  from the **Namespaces** UI view or with the namespace tooling.

## Load it into the dev stack

The dev server listens on `http://localhost:8088` and (in dev) has auth off, so
no token is needed:

```bash
make build                                   # produces ./bin/cortex (and the rest)
./bin/cortex --server http://localhost:8088 import testdata/seed-memories.json
```

> ⚠️ **Always pass `--server http://localhost:8088`.** The default config
> (`~/.config/cortex/cortex.yaml`) points the bare `cortex` command at the
> **production** server, so `cortex import seed-memories.json` *without* `--server`
> would load this fake data into prod. The `--server` flag overrides the config
> file (precedence: flag > env > config file > default). Sanity-check with
> `cortex --server http://localhost:8088 config show` — the `server:` line must
> say localhost.

`import` queues each memory onto NATS; the worker embeds and upserts them. Watch
the **Indexing** UI tab (it auto-refreshes) or:

```bash
./bin/cortex --server http://localhost:8088 status        # memory count climbs
./bin/cortex --server http://localhost:8088 list -n '*'   # see them
```

## Clean it up

The seed lives entirely in `demo-*` namespaces, so the quickest reset is the
**Namespaces** view → delete each `demo-*` namespace (removes its memories and
summaries). Or per-memory with `cortex delete <id>`.
