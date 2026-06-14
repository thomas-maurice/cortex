<template>
  <div>
    <div class="row g-2 align-items-end mb-2">
      <div class="col-auto" style="width: 160px">
        <label class="form-label small mb-1">Namespace</label>
        <input v-model="namespace" class="form-control form-control-sm" placeholder="* = all" @keyup.enter="reload" />
      </div>
      <div class="col-auto" style="width: 110px">
        <label class="form-label small mb-1">Max nodes</label>
        <input v-model.number="limit" type="number" min="1" max="500" class="form-control form-control-sm" />
      </div>
      <div class="col-auto" style="width: 120px">
        <label class="form-label small mb-1" title="Find similar drops matches whose vector distance exceeds this">Similar ≤ dist</label>
        <input v-model.number="cutoff" type="number" min="0.1" max="1" step="0.05" class="form-control form-control-sm" />
      </div>
      <div class="col-auto">
        <button class="btn btn-primary btn-sm" :disabled="loading" @click="reload">
          <font-awesome-icon :icon="['fas', 'rotate']" class="me-1" />Reload
        </button>
      </div>
      <div class="col-auto">
        <button class="btn btn-sm" :class="connectMode ? 'btn-success' : 'btn-outline-success'" @click="toggleConnect">
          <font-awesome-icon :icon="['fas', connectMode ? 'link-slash' : 'link']" class="me-1" />
          {{ connectMode ? 'Connecting…' : 'Connect' }}
        </button>
      </div>
      <div class="col-auto">
        <button class="btn btn-outline-secondary btn-sm" :disabled="!hasNeighbours" @click="clearNeighbours">
          Clear added
        </button>
      </div>
      <div class="col-auto form-check form-switch mt-3">
        <input id="physics" v-model="physicsOn" class="form-check-input" type="checkbox" @change="togglePhysics" />
        <label for="physics" class="form-check-label small">Physics</label>
      </div>
      <div class="col text-end small text-muted">{{ memoryCount }} memories</div>
    </div>

    <div v-if="connectMode" class="alert alert-success py-1 px-2 small mb-2">
      Connect mode: click a memory, then another, to link them.
      <span v-if="pendingLink">First memory selected — pick the second.</span>
    </div>
    <div v-else class="small text-muted mb-2">
      Click a memory to inspect it · <strong>double-click</strong> (or “Find similar”) to add its semantic
      neighbours · click a <span class="text-success">green link</span> to remove it · click an
      <span style="color: #fd7e14">orange dashed</span> edge to mark the pair not-a-duplicate.
    </div>

    <div v-if="notice" class="alert alert-info py-1 px-2 small mb-2">{{ notice }}</div>
    <div v-if="error" class="alert alert-danger py-2">{{ error }}</div>

    <div class="position-relative border rounded" style="height: 72vh">
      <div v-if="loading" class="position-absolute top-50 start-50 translate-middle text-muted">
        <font-awesome-icon :icon="['fas', 'spinner']" spin size="2x" />
      </div>
      <div ref="container" style="height: 100%"></div>

      <!-- Details panel for the selected memory. Read-only; no graph mutation. -->
      <div
        v-if="selected"
        class="card shadow-sm position-absolute top-0 end-0 m-2"
        style="width: 320px; max-height: calc(72vh - 1rem); overflow: auto"
      >
        <div class="card-body py-2">
          <div class="d-flex justify-content-between align-items-start mb-1">
            <span class="badge bg-secondary">
              <font-awesome-icon :icon="['fas', 'layer-group']" class="me-1" />{{ selected.namespace }}
            </span>
            <button class="btn-close btn-sm" @click="deselect"></button>
          </div>
          <div class="small mb-2 markdown-body" v-html="renderMarkdown(selected.text)"></div>
          <div class="small text-muted mb-2">
            <span v-for="t in selected.tags" :key="t" class="badge bg-info text-dark me-1">#{{ t }}</span>
          </div>
          <div v-if="selected.conversationId" class="small text-muted mb-2">
            <font-awesome-icon :icon="['fas', 'comments']" class="me-1" />
            <span class="font-monospace">{{ selected.conversationId }}</span>
          </div>
          <div class="small text-muted mb-2">
            {{ (selected.linkedIds || []).length }} explicit link(s)
            <span v-if="(selected.dupCandidates || []).length" style="color: #fd7e14">
              · {{ selected.dupCandidates.length }} duplicate candidate(s)
            </span>
          </div>
          <button class="btn btn-outline-primary btn-sm w-100 mb-2" @click="expandSemantic('m:' + selected.id)">
            <font-awesome-icon :icon="['fas', 'magnifying-glass']" class="me-1" />Find similar
          </button>
          <button class="btn btn-outline-danger btn-sm w-100" @click="deleteSelected">
            <font-awesome-icon :icon="['fas', 'trash']" class="me-1" />Delete memory
          </button>
        </div>
      </div>
    </div>

    <div class="small text-muted mt-2">
      node colour = namespace &nbsp;·&nbsp;
      <span class="text-success">green</span> = explicit link &nbsp;·&nbsp;
      <span style="color: #fd7e14">orange dashed</span> = likely duplicate (flagged) &nbsp;·&nbsp;
      <span class="text-primary">blue dashed</span> = semantic neighbour (added on demand)
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount } from 'vue'
import { useRouter } from 'vue-router'
import { Network, DataSet } from 'vis-network/standalone'
import { Code, ConnectError } from '@connectrpc/connect'
import { memoryClient } from '@/utils/connect'
import { renderMarkdown } from '@/utils/markdown'
import { useAuthStore } from '@/stores/auth'
import { truncate } from '@/utils/text'

const router = useRouter()
const auth = useAuthStore()

const container = ref(null)
const namespace = ref('*')
const limit = ref(150)
const cutoff = ref(0.45)
const loading = ref(false)
const error = ref('')
const notice = ref('')
const physicsOn = ref(true)
const connectMode = ref(false)
const pendingLink = ref(null)
const selected = ref(null)
const hasNeighbours = ref(false)
const memoryCount = ref(0)

let network = null
let nodes = null
let edges = null
// IDs present after a plain reload. Anything not in here was added by semantic
// expansion, so "Clear added" can restore the base graph cleanly.
let baseNodeIds = new Set()

const LINK_COLOR = '#198754'
const DUP_COLOR = '#fd7e14'

function handleError(e) {
  if (e instanceof ConnectError && e.code === Code.Unauthenticated) {
    auth.logout()
    router.push({ name: 'login' })
    return
  }
  error.value = e.message || 'Request failed'
}

function memNode(m) {
  return {
    id: 'm:' + m.id,
    label: truncate(m.text, 36),
    title: m.text,
    group: m.namespace || 'global',
    shape: 'dot',
    size: 16,
    mem: m,
  }
}

function linkKey(a, b) {
  return 'link:' + [a, b].sort().join('|')
}

function linkEdge(a, b) {
  return { id: linkKey(a, b), from: a, to: b, color: { color: LINK_COLOR }, width: 3, title: 'linked (click to remove)' }
}

function dupKey(a, b) {
  return 'dup:' + [a, b].sort().join('|')
}

function dupEdge(a, b) {
  return {
    id: dupKey(a, b),
    from: a,
    to: b,
    dashes: true,
    color: { color: DUP_COLOR },
    width: 2,
    title: 'likely duplicate (click to mark not a duplicate)',
  }
}

async function reload() {
  loading.value = true
  error.value = ''
  notice.value = ''
  clearPending()
  selected.value = null
  hasNeighbours.value = false
  try {
    const res = await memoryClient.list({ namespace: namespace.value, limit: limit.value })
    const mems = res.memories
    memoryCount.value = mems.length

    const nodeList = []
    const edgeList = []
    const present = new Set(mems.map((m) => 'm:' + m.id))

    for (const m of mems) {
      nodeList.push(memNode(m))
    }

    const linkSeen = new Set()
    for (const m of mems) {
      for (const lid of m.linkedIds || []) {
        const tid = 'm:' + lid
        if (!present.has(tid)) continue
        const key = linkKey('m:' + m.id, tid)
        if (linkSeen.has(key)) continue
        linkSeen.add(key)
        edgeList.push(linkEdge('m:' + m.id, tid))
      }
    }

    // Duplicate-candidate edges: heuristic, worker-flagged near-duplicates. A
    // pair already joined by an explicit link is left as the green link only.
    const dupSeen = new Set()
    for (const m of mems) {
      for (const cid of m.dupCandidates || []) {
        const tid = 'm:' + cid
        if (!present.has(tid)) continue
        const key = dupKey('m:' + m.id, tid)
        if (dupSeen.has(key) || linkSeen.has(linkKey('m:' + m.id, tid))) continue
        dupSeen.add(key)
        edgeList.push(dupEdge('m:' + m.id, tid))
      }
    }

    nodes = new DataSet(nodeList)
    edges = new DataSet(edgeList)
    baseNodeIds = new Set(nodeList.map((n) => n.id))
    render()
    network.off('stabilizationIterationsDone')
    network.once('stabilizationIterationsDone', () => network && network.fit({ animation: { duration: 400 } }))
  } catch (e) {
    handleError(e)
  } finally {
    loading.value = false
  }
}

function render() {
  const data = { nodes, edges }
  const options = {
    // Fixed seed so the layout is reproducible across reloads instead of
    // re-scrambling every time.
    layout: { randomSeed: 7, improvedLayout: true },
    nodes: { borderWidth: 1, font: { size: 12 } },
    edges: { smooth: { type: 'continuous' } },
    physics: {
      enabled: physicsOn.value,
      barnesHut: { gravitationalConstant: -8000, springLength: 120, springConstant: 0.04 },
      stabilization: { iterations: 150 },
    },
    interaction: { hover: true, tooltipDelay: 150 },
  }
  if (network) {
    network.setData(data)
    network.setOptions(options)
  } else {
    network = new Network(container.value, data, options)
    network.on('click', onClick)
    network.on('doubleClick', onDoubleClick)
  }
}

// Single click: inspect / pick link endpoints / remove a link. Never mutates the
// graph structure (other than an explicit link removal) — that was the old
// surprise where every click dumped semantic neighbours.
function onClick(params) {
  if (!params.nodes.length) {
    if (params.edges.length && String(params.edges[0]).startsWith('link:')) {
      maybeUnlink(params.edges[0])
    } else if (params.edges.length && String(params.edges[0]).startsWith('dup:')) {
      maybeDismiss(params.edges[0])
    } else {
      selected.value = null
    }
    return
  }
  const id = params.nodes[0]
  if (!String(id).startsWith('m:')) {
    selected.value = null
    return
  }
  if (connectMode.value) {
    handleConnectClick(id)
    return
  }
  const node = nodes.get(id)
  selected.value = node?.mem || null
}

// Double click is the explicit "expand semantic neighbours" gesture.
function onDoubleClick(params) {
  if (connectMode.value || !params.nodes.length) return
  const id = params.nodes[0]
  if (String(id).startsWith('m:')) expandSemantic(id)
}

function toggleConnect() {
  connectMode.value = !connectMode.value
  clearPending()
}

function clearPending() {
  pendingLink.value = null
  if (network) network.unselectAll()
}

function deselect() {
  selected.value = null
  if (network) network.unselectAll()
}

async function handleConnectClick(id) {
  if (!pendingLink.value) {
    pendingLink.value = id
    network.selectNodes([id])
    return
  }
  if (pendingLink.value === id) {
    clearPending()
    return
  }
  const a = pendingLink.value
  try {
    await memoryClient.link({ id: a.slice(2), targetId: id.slice(2) })
    const key = linkKey(a, id)
    if (!edges.get(key)) edges.add(linkEdge(a, id))
  } catch (e) {
    handleError(e)
  }
  clearPending()
}

async function maybeUnlink(eid) {
  const e = edges.get(eid)
  if (!e) return
  if (!confirm('Remove this link?')) return
  try {
    await memoryClient.unlink({ id: String(e.from).slice(2), targetId: String(e.to).slice(2) })
    edges.remove(eid)
  } catch (err) {
    handleError(err)
  }
}

// Delete the selected memory (e.g. the redundant half of a duplicate pair). Also
// prunes the node and any edges touching it from the graph so the view stays
// consistent without a full reload.
async function deleteSelected() {
  const mem = selected.value
  if (!mem) return
  if (!confirm('Delete this memory? This cannot be undone.')) return
  try {
    await memoryClient.delete({ id: mem.id })
    const nid = 'm:' + mem.id
    const touching = edges.get({ filter: (e) => e.from === nid || e.to === nid }).map((e) => e.id)
    edges.remove(touching)
    nodes.remove(nid)
    baseNodeIds.delete(nid)
    selected.value = null
  } catch (e) {
    handleError(e)
  }
}

// Clicking an orange duplicate-candidate edge dismisses it: the pair is recorded
// as confirmed-not-a-duplicate so the worker never re-flags it.
async function maybeDismiss(eid) {
  const e = edges.get(eid)
  if (!e) return
  if (!confirm('Mark these two memories as NOT duplicates? They won’t be flagged again.')) return
  try {
    await memoryClient.dismissDuplicate({ id: String(e.from).slice(2), targetId: String(e.to).slice(2) })
    edges.remove(eid)
  } catch (err) {
    handleError(err)
  }
}

async function expandSemantic(id) {
  const node = nodes.get(id)
  if (!node?.mem) return
  notice.value = ''
  try {
    // searchSimilar reuses the memory's stored vector server-side (Weaviate
    // nearObject) — it does NOT re-embed the text, so big memories cost no
    // inference. maxDistance is a server-side relevance cutoff so we never link
    // unrelated memories; the seed memory is excluded by the server.
    const res = await memoryClient.searchSimilar({ id: node.mem.id, namespace: '*', limit: 6, maxDistance: cutoff.value })
    const hits = res.hits.filter((h) => 'm:' + h.memory.id !== id)
    if (hits.length === 0) {
      notice.value = `No memories within distance ${cutoff.value} of "${truncate(node.mem.text, 30)}".`
      return
    }
    for (const h of hits) {
      const nid = 'm:' + h.memory.id
      if (!nodes.get(nid)) nodes.add(memNode(h.memory))
      const eid = id + '=>' + nid
      if (!edges.get(eid)) {
        edges.add({
          id: eid,
          from: id,
          to: nid,
          dashes: true,
          color: { color: '#0d6efd' },
          label: h.distance.toFixed(2),
          font: { size: 9, color: '#0d6efd' },
        })
        hasNeighbours.value = true
      }
    }
  } catch (e) {
    handleError(e)
  }
}

// Remove everything semantic expansion added, restoring the base graph.
function clearNeighbours() {
  if (!edges || !nodes) return
  const dashed = edges.get({ filter: (e) => String(e.id).includes('=>') }).map((e) => e.id)
  edges.remove(dashed)
  const extra = nodes.get({ filter: (n) => !baseNodeIds.has(n.id) }).map((n) => n.id)
  nodes.remove(extra)
  hasNeighbours.value = false
}

function togglePhysics() {
  if (network) network.setOptions({ physics: { enabled: physicsOn.value } })
}

onMounted(reload)
onBeforeUnmount(() => {
  if (network) {
    network.destroy()
    network = null
  }
})
</script>
