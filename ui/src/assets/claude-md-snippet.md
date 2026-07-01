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
