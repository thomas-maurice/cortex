import { timestampDate } from '@bufbuild/protobuf/wkt'

// Shared helpers for rendering memory text in compact UI surfaces.

export function truncate(s, n = 40) {
  if (!s) return ''
  const oneLine = s.replace(/\s+/g, ' ').trim()
  return oneLine.length > n ? oneLine.slice(0, n - 1) + '…' : oneLine
}

export function formatTimestamp(ts) {
  try {
    return timestampDate(ts).toLocaleString()
  } catch {
    return ''
  }
}

// splitByMatch cuts `text` into {before, hit, after} around a search match so the
// caller can render each part separately (each through renderMarkdown, i.e.
// DOMPurify) and wrap `hit` in a static highlight container. Splitting the RAW
// text BEFORE markdown rendering means no user text ever reaches the DOM except
// via the sanitizer. match.start/end are Unicode code point offsets (the server
// counts runes), whereas JS strings index by UTF-16 units, hence Array.from.
// Returns null when there is nothing safe to highlight (no/invalid offsets, or the
// span is the whole text).
export function splitByMatch(text, match) {
  if (!text || !match) return null
  const { start, end } = match
  if (!Number.isInteger(start) || !Number.isInteger(end)) return null
  const cps = Array.from(text)
  if (start < 0 || end <= start || end > cps.length) return null
  if (start === 0 && end === cps.length) return null
  return {
    before: cps.slice(0, start).join(''),
    hit: cps.slice(start, end).join(''),
    after: cps.slice(end).join(''),
  }
}
