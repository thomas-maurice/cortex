<template>
  <div>
    <div class="row g-2 align-items-end mb-2">
      <div class="col">
        <label class="form-label small mb-1">Query</label>
        <input
          v-model="query"
          class="form-control form-control-sm"
          placeholder="type anything — it gets vectorised and matched against your memories"
          @keyup.enter="run"
        />
      </div>
      <div class="col-auto" style="width: 120px">
        <label class="form-label small mb-1">Namespace</label>
        <input v-model="namespace" class="form-control form-control-sm" placeholder="* = all" />
      </div>
      <div class="col-auto" style="width: 90px">
        <label class="form-label small mb-1">Limit</label>
        <input v-model.number="limit" type="number" min="1" max="100" class="form-control form-control-sm" />
      </div>
      <div class="col-auto" style="width: 110px">
        <label class="form-label small mb-1" title="relevance cutoff (lower = closer match). Search is hybrid (keyword + vector), so this blends both; raise it to surface weaker matches, lower it to tighten.">Max dist</label>
        <input v-model.number="cutoff" type="number" min="0.1" max="1.5" step="0.05" class="form-control form-control-sm" />
      </div>
      <div class="col-auto">
        <button class="btn btn-primary btn-sm" :disabled="loading || !query.trim()" @click="run">
          <font-awesome-icon :icon="['fas', 'magnifying-glass']" class="me-1" />Explore
        </button>
      </div>
      <div class="col-auto">
        <button class="btn btn-outline-secondary btn-sm" :disabled="!searched" @click="clearCloud">Clear</button>
      </div>
    </div>

    <div v-if="notice" class="alert alert-info py-1 px-2 small mb-2">{{ notice }}</div>
    <div v-if="error" class="alert alert-danger py-2">{{ error }}</div>

    <div class="position-relative border rounded" style="height: 74vh">
      <div v-if="loading" class="position-absolute top-50 start-50 translate-middle text-muted">
        <font-awesome-icon :icon="['fas', 'spinner']" spin size="2x" />
      </div>
      <div v-if="!searched && !loading" class="position-absolute top-50 start-50 translate-middle text-muted text-center">
        <font-awesome-icon :icon="['fas', 'cloud']" size="2x" class="mb-2 d-block" />
        Enter a query to see a cloud of related memories.
      </div>
      <div ref="container" style="height: 100%"></div>

      <div
        v-if="selected"
        class="card shadow-sm position-absolute top-0 end-0 m-2"
        style="width: 320px; max-height: calc(74vh - 1rem); overflow: auto"
      >
        <div class="card-body py-2">
          <div class="d-flex justify-content-between align-items-start mb-1">
            <span class="badge bg-secondary">
              <font-awesome-icon :icon="['fas', 'layer-group']" class="me-1" />{{ selected.namespace }}
            </span>
            <button class="btn-close btn-sm" @click="deselect"></button>
          </div>
          <div class="small mb-2 markdown-body" v-html="renderMarkdown(selected.text)"></div>
          <div v-if="(selected.tags || []).length" class="small text-muted mb-2">
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
          <div v-if="selected.accessCount || selected.lastAccessedAt" class="small text-muted d-flex flex-wrap gap-2 align-items-center">
            <span v-if="selected.accessCount" class="badge bg-warning text-dark" title="times the agent recalled this memory (living memory)">
              <font-awesome-icon :icon="['fas', 'fire']" class="me-1" />{{ selected.accessCount }} recall(s)
            </span>
            <span v-if="selected.lastAccessedAt" title="when this memory was last recalled">
              <font-awesome-icon :icon="['fas', 'clock-rotate-left']" class="me-1" />{{ fmtDate(selected.lastAccessedAt) }}
            </span>
          </div>
        </div>
      </div>
    </div>

    <div class="small text-muted mt-2">
      <span v-if="searched">{{ resultCount }} match(es) · </span>
      Central <span class="text-danger">★</span> = your query · closer + bigger = more relevant · edge number = vector distance.
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
const query = ref('')
const namespace = ref('*')
const limit = ref(25)
const cutoff = ref(0.6)
const loading = ref(false)
const error = ref('')
const notice = ref('')
const searched = ref(false)
const selected = ref(null)
const resultCount = ref(0)

let network = null
let nodes = null
let edges = null
// Monotonic id so a slow search can't overwrite a newer one's results.
let reqId = 0

const QUERY_ID = 'query'

function handleError(e) {
  if (e instanceof ConnectError && e.code === Code.Unauthenticated) {
    auth.logout()
    router.push({ name: 'login' })
    return
  }
  error.value = e.message || 'Request failed'
}

// fmtDate renders a protobuf Timestamp for display, empty on any failure.
function fmtDate(ts) {
  try {
    return ts.toDate().toLocaleString()
  } catch {
    return ''
  }
}

// Map a 0..~1 distance to a node size (closer = bigger) and edge length
// (closer = shorter, so it sits nearer the centre).
function sizeFor(distance) {
  return Math.max(8, 32 - distance * 32)
}
function lengthFor(distance) {
  return 70 + distance * 600
}

async function run() {
  const q = query.value.trim()
  if (!q) return
  const my = ++reqId
  loading.value = true
  error.value = ''
  notice.value = ''
  selected.value = null
  try {
    // noReinforce: exploring is not a recall — never inflate the usage signal.
    const res = await memoryClient.search({ query: q, namespace: namespace.value, limit: limit.value, maxDistance: cutoff.value, noReinforce: true })
    if (my !== reqId) return // a newer query superseded this one
    searched.value = true
    resultCount.value = res.hits.length

    const nodeList = [
      { id: QUERY_ID, label: truncate(q, 40), title: q, shape: 'star', color: '#dc3545', size: 28, x: 0, y: 0, fixed: true, physics: false },
    ]
    const edgeList = []
    for (const h of res.hits) {
      const m = h.memory
      nodeList.push({
        id: 'm:' + m.id,
        label: truncate(m.text, 30),
        title: m.text,
        group: m.namespace || 'global',
        shape: 'dot',
        size: sizeFor(h.distance),
        mem: m,
      })
      const alpha = Math.max(0.2, 1 - h.distance).toFixed(2)
      edgeList.push({
        from: QUERY_ID,
        to: 'm:' + m.id,
        length: lengthFor(h.distance),
        label: h.distance.toFixed(2),
        font: { size: 9, color: '#6c757d' },
        color: { color: `rgba(13,110,253,${alpha})` },
      })
    }

    if (res.hits.length === 0) {
      notice.value = `No memories within distance ${cutoff.value} of "${truncate(q, 40)}".`
    }

    nodes = new DataSet(nodeList)
    edges = new DataSet(edgeList)
    render()
    // Frame the cloud once it settles so every query is centred and zoomed sanely.
    // Clear any prior listener first — a rapid second search could fire a stale one.
    network.off('stabilizationIterationsDone')
    network.once('stabilizationIterationsDone', () => network && network.fit({ animation: { duration: 400 } }))
  } catch (e) {
    if (my === reqId) handleError(e)
  } finally {
    if (my === reqId) loading.value = false
  }
}

function clearCloud() {
  if (nodes) nodes.clear()
  if (edges) edges.clear()
  searched.value = false
  resultCount.value = 0
  selected.value = null
  notice.value = ''
}

function render() {
  const data = { nodes, edges }
  const options = {
    layout: { randomSeed: 7 },
    nodes: { borderWidth: 1, font: { size: 12 } },
    edges: { smooth: { type: 'continuous' } },
    physics: {
      enabled: true,
      barnesHut: { gravitationalConstant: -6000, centralGravity: 0.1, springConstant: 0.05 },
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
  }
}

function onClick(params) {
  if (!params.nodes.length) {
    selected.value = null
    return
  }
  const id = params.nodes[0]
  if (!String(id).startsWith('m:')) {
    selected.value = null
    return
  }
  selected.value = nodes.get(id)?.mem || null
}

function deselect() {
  selected.value = null
  if (network) network.unselectAll()
}

onMounted(() => {
  // Instantiate the (empty) network so the canvas is ready for the first query.
  nodes = new DataSet([])
  edges = new DataSet([])
  render()
})
onBeforeUnmount(() => {
  if (network) {
    network.destroy()
    network = null
  }
})
</script>
