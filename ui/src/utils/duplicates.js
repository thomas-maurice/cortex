// Pure helpers for the Duplicates review view (no Vue / RPC imports so they can
// be exercised with plain node).

export const MERGE_SEPARATOR = '\n\n---\n\n'

function tsKey(m) {
  const t = m && m.createdAt
  if (!t) return 0
  return Number(t.seconds ?? 0) * 1e9 + Number(t.nanos ?? 0)
}

// The server returns directed groups: memory A lists B as a candidate, and B
// usually lists A back. A reviewer must see the pair once, so collapse by the
// unordered id pair. The older memory goes on the left (stable, so the layout
// does not flip between reloads); first-seen order of pairs is preserved.
export function buildPairs(groups) {
  const seen = new Map()
  for (const g of groups || []) {
    const a = g.memory
    if (!a) continue
    for (const b of g.candidates || []) {
      if (!b || a.id === b.id) continue
      const key = a.id < b.id ? `${a.id}|${b.id}` : `${b.id}|${a.id}`
      if (seen.has(key)) continue
      const [left, right] = tsKey(a) < tsKey(b) || (tsKey(a) === tsKey(b) && a.id < b.id) ? [a, b] : [b, a]
      seen.set(key, { key, left, right })
    }
  }
  return [...seen.values()]
}

export function unionTags(...lists) {
  const out = []
  for (const l of lists) for (const t of l || []) if (!out.includes(t)) out.push(t)
  return out
}

// Draft for the merge dialog: both texts, union of tags, the left namespace,
// and supersedes = both originals (the worker trashes them once the merged
// memory is durably indexed).
export function mergeDraft(pair) {
  return {
    text: pair.left.text + MERGE_SEPARATOR + pair.right.text,
    namespace: pair.left.namespace,
    tags: unionTags(pair.left.tags, pair.right.tags),
    supersedes: [pair.left.id, pair.right.id],
  }
}

// A global-namespace memory tagged `preference` is injected into every session by
// the cortex-prefs hook; merging it into another namespace (or dropping the tag)
// would silently remove the standing rule from the hook.
export function isGlobalPreference(m) {
  return !!m && m.namespace === 'global' && (m.tags || []).includes('preference')
}

function untilMs(m) {
  const t = m && m.validUntil
  return t ? Number(t.seconds ?? 0) * 1000 + Math.floor(Number(t.nanos ?? 0) / 1e6) : 0
}

// Expiry the merged memory should start with, mirroring the worker's rule
// (internal/merge.InheritedExpiry): only when BOTH originals expire does the merge
// expire, at the LATER of the two; if either is permanent it stays permanent.
// Returns { model, expiredSides } where model is an ExpiryPicker model and
// expiredSides counts originals whose expiry has already passed. A latest expiry
// already in the past cannot be a picker date (must be in the future), so it
// falls back to "never" and the dialog warns instead.
export function mergeExpiry(pair, now = Date.now()) {
  const l = untilMs(pair.left)
  const r = untilMs(pair.right)
  const expiredSides = [l, r].filter((x) => x && x <= now).length
  const none = { preset: 'none', date: '' }
  if (!l || !r) return { model: none, expiredSides }
  const latest = Math.max(l, r)
  if (latest <= now) return { model: none, expiredSides }
  const d = new Date(latest)
  const p = (n) => String(n).padStart(2, '0')
  return { model: { preset: 'custom', date: `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}` }, expiredSides }
}
