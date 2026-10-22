import { timestampDate, timestampFromDate } from '@bufbuild/protobuf/wkt'

// Expiry helpers shared by the New-memory form, the inspector's edit mode and the
// memory cards. The server owns all validation; this only shapes requests and
// labels.

// Presets offered in the picker. `days` is sent as a relative expiresIn.
export const EXPIRY_PRESETS = [
  { value: 'none', label: 'Never expires' },
  { value: '1d', label: '1 day', expiresIn: '1d' },
  { value: '7d', label: '1 week', expiresIn: '7d' },
  { value: '30d', label: '1 month', expiresIn: '30d' },
  { value: 'custom', label: 'Custom date…' },
]

// yyyy-mm-dd in local time, for <input type="date">.
export function toDateInput(d) {
  const p = (n) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`
}

// A custom date means "until the END of that local day".
export function endOfDay(dateStr) {
  const [y, m, d] = dateStr.split('-').map(Number)
  return new Date(y, m - 1, d, 23, 59, 59)
}

// Picker model -> the request fields to merge into a Save / UpdateMemory call.
// `hadExpiry` is whether the memory being edited already has one (a "never
// expires" choice then has to clear it; for a new memory it means "send nothing").
// Returns { error } for an unusable choice.
export function expiryRequest(model, hadExpiry = false) {
  switch (model.preset) {
    case 'keep':
      return {}
    case 'none':
      return hadExpiry ? { clearValidUntil: true } : {}
    case 'custom': {
      if (!model.date) return { error: 'Pick an expiry date' }
      const d = endOfDay(model.date)
      if (!(d.getTime() > Date.now())) return { error: 'The expiry date must be in the future' }
      return { validUntil: timestampFromDate(d) }
    }
    default: {
      const p = EXPIRY_PRESETS.find((x) => x.value === model.preset)
      return p?.expiresIn ? { expiresIn: p.expiresIn } : {}
    }
  }
}

// What validUntil the memory will have after applying `req`, for updating the open
// view optimistically (undefined = unchanged).
export function expiryAfter(req) {
  if (req.clearValidUntil) return null
  if (req.validUntil) return req.validUntil
  if (req.expiresIn) {
    const days = parseInt(req.expiresIn, 10)
    return timestampFromDate(new Date(Date.now() + days * 86400000))
  }
  return undefined
}

// Badge text for a memory's validUntil: null when it never expires.
export function expiryInfo(ts) {
  if (!ts) return null
  const ms = timestampDate(ts).getTime() - Date.now()
  if (ms <= 0) return { expired: true, text: 'expired' }
  const mins = Math.ceil(ms / 60000)
  if (mins < 60) return { expired: false, text: `expires in ${mins}m` }
  const hours = Math.floor(mins / 60)
  if (hours < 48) return { expired: false, text: `expires in ${hours}h` }
  return { expired: false, text: `expires in ${Math.floor(hours / 24)}d` }
}
