// Shared by the Graph and Explore views: vis-network edge for an explicit
// memory<->memory link. a/b are vis node ids ('m:' + memory id).
export const LINK_COLOR = '#198754'

export function linkKey(a, b) {
  return 'link:' + [a, b].sort().join('|')
}

export function linkEdge(a, b) {
  return { id: linkKey(a, b), from: a, to: b, color: { color: LINK_COLOR }, width: 3, title: 'linked (click to remove)' }
}
