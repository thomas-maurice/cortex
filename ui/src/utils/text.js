// Shared helpers for rendering memory text in compact UI surfaces.

export function truncate(s, n = 40) {
  if (!s) return ''
  const oneLine = s.replace(/\s+/g, ' ').trim()
  return oneLine.length > n ? oneLine.slice(0, n - 1) + '…' : oneLine
}

export function formatTimestamp(ts) {
  try {
    return ts.toDate().toLocaleString()
  } catch {
    return ''
  }
}
