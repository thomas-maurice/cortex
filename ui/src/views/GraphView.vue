<template>
  <div class="space-y-4">
    <div class="flex flex-wrap items-end gap-2">
      <div class="grid gap-1.5" style="width: 160px">
        <Label class="text-xs">Namespace</Label>
        <Input v-model="namespace" placeholder="* = all" @keyup.enter="reload" />
      </div>
      <div class="grid gap-1.5" style="width: 110px">
        <Label class="text-xs">Max nodes</Label>
        <Input v-model.number="limit" type="number" min="1" max="500" />
      </div>
      <div class="grid gap-1.5" style="width: 120px">
        <Label class="text-xs" title="Find similar drops matches whose vector distance exceeds this">Similar ≤ dist</Label>
        <Input v-model.number="cutoff" type="number" min="0.1" max="1" step="0.05" />
      </div>
      <Button size="sm" :disabled="loading" @click="reload">
        <RotateCw class="size-4" />Reload
      </Button>
      <Button size="sm" :variant="connectMode ? 'default' : 'outline'" @click="toggleConnect">
        <component :is="connectMode ? Unlink : Link2" class="size-4" />
        {{ connectMode ? 'Connecting…' : 'Connect' }}
      </Button>
      <Button size="sm" variant="outline" :disabled="!hasNeighbours" @click="clearNeighbours">
        Clear added
      </Button>
      <div class="flex items-center gap-2">
        <Switch id="physics" v-model="physicsOn" @update:modelValue="togglePhysics" />
        <Label for="physics" class="text-xs">Physics</Label>
      </div>
      <div class="ml-auto text-sm text-muted-foreground">{{ memoryCount }} memories</div>
    </div>

    <p v-if="connectMode" class="text-sm text-emerald-600 dark:text-emerald-400">
      Connect mode: click a memory, then another, to link them.
      <span v-if="pendingLink">First memory selected — pick the second.</span>
    </p>
    <p v-else class="text-sm text-muted-foreground">
      Click a memory to inspect it · <strong>double-click</strong> (or “Find similar”) to add its semantic
      neighbours · click a <span class="text-emerald-600 dark:text-emerald-400">green link</span> to remove it · click an
      <span style="color: #fd7e14">orange dashed</span> edge to mark the pair not-a-duplicate.
    </p>

    <Alert v-if="error" variant="destructive">
      <AlertDescription>{{ error }}</AlertDescription>
    </Alert>

    <!-- Legend: one chip per namespace currently present in the loaded nodes,
         colored to match the node fill so the graph is readable at a glance. -->
    <div v-if="presentNamespaces.length" class="flex flex-wrap gap-1">
      <Badge v-for="ns in presentNamespaces" :key="ns" variant="outline" :style="nsChipStyle(ns, theme === 'dark')">
        {{ ns }}
      </Badge>
    </div>

    <div class="relative rounded-md border" style="height: 72vh">
      <div v-if="loading" class="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 text-muted-foreground">
        <Loader2 class="size-8 animate-spin" />
      </div>
      <div ref="container" style="height: 100%"></div>

      <!-- Slim panel for the selected memory's link/similar controls only.
           Text display, edit, and delete live in the shared MemoryInspector
           (opened on click) so they stay in sync with every other view. -->
      <Card v-if="selected" class="absolute top-0 right-0 m-2 shadow-sm" style="width: 220px">
        <CardContent class="py-2">
          <div class="mb-2 flex items-start justify-between gap-2">
            <div class="text-sm text-muted-foreground">
              {{ (selected.linkedIds || []).length }} explicit link(s)
              <span v-if="(selected.dupCandidates || []).length" style="color: #fd7e14">
                · {{ selected.dupCandidates.length }} duplicate candidate(s)
              </span>
            </div>
            <Button variant="ghost" size="icon" class="size-6 shrink-0" aria-label="Close details panel" @click="deselect">
              <X class="size-3.5" />
            </Button>
          </div>
          <Button variant="outline" size="sm" class="w-full" @click="expandSemantic('m:' + selected.id)">
            <Search class="size-4" />Find similar
          </Button>
        </CardContent>
      </Card>
    </div>

    <div class="text-sm text-muted-foreground">
      node colour = namespace &nbsp;·&nbsp;
      <span class="text-emerald-600 dark:text-emerald-400">green</span> = explicit link &nbsp;·&nbsp;
      <span style="color: #fd7e14">orange dashed</span> = likely duplicate (flagged) &nbsp;·&nbsp;
      <span class="text-primary">blue dashed</span> = semantic neighbour (added on demand)
    </div>
  </div>
</template>

<script setup>
import { ref, watch, onMounted, onBeforeUnmount } from 'vue'
import { useRouter } from 'vue-router'
import { Network, DataSet } from 'vis-network/standalone'
import { Code, ConnectError } from '@connectrpc/connect'
import { toast } from 'vue-sonner'
import { memoryClient } from '@/utils/connect'
import { useAuthStore } from '@/stores/auth'
import { theme } from '@/lib/theme'
import { nsNodeColors, nsChipStyle } from '@/lib/nsColor'
import { confirmDialog } from '@/lib/confirm'
import { openInspector, inspectorChanged } from '@/lib/inspector'
import { truncate } from '@/utils/text'
import { Link2, Loader2, RotateCw, Search, Unlink, X } from 'lucide-vue-next'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'

const router = useRouter()
const auth = useAuthStore()

const container = ref(null)
const namespace = ref('*')
const limit = ref(150)
const cutoff = ref(0.45)
const loading = ref(false)
const error = ref('')
const physicsOn = ref(true)
const connectMode = ref(false)
const pendingLink = ref(null)
const selected = ref(null)
const hasNeighbours = ref(false)
const memoryCount = ref(0)
// Distinct namespaces among the currently loaded nodes, for the legend.
const presentNamespaces = ref([])

// Recolor nodes and labels when the theme toggles while a graph is on screen.
watch(theme, () => { if (network) { recolorNodes(); render() } })
// Text/edit/delete now live in the shared MemoryInspector; reload once it
// reports a change so the graph reflects it.
watch(inspectorChanged, reload)

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
    color: nsNodeColors(m.namespace || 'global', theme.value === 'dark'),
    shape: 'dot',
    size: 16,
    mem: m,
  }
}

// Recompute node fill colors in place after a theme change (the DataSet holds
// the color objects computed at creation time, so they'd otherwise go stale).
function recolorNodes() {
  if (!nodes) return
  const dark = theme.value === 'dark'
  nodes.update(nodes.get().map((n) => ({ id: n.id, color: nsNodeColors(n.mem?.namespace || 'global', dark) })))
}

// Legend: distinct namespaces among the nodes currently on screen.
function refreshNamespaces() {
  presentNamespaces.value = nodes ? [...new Set(nodes.get().map((n) => n.mem?.namespace || 'global'))].sort() : []
}

// Resolver passed to the inspector so it can follow a memory's linkedIds
// within this view's already-loaded nodes (there is no get-by-id RPC).
function resolveMemory(id) {
  return nodes?.get('m:' + id)?.mem
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
    refreshNamespaces()
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
    // Label color follows the app theme — vis-network defaults to near-black,
    // which is unreadable on the dark background.
    nodes: { borderWidth: 1, font: { size: 12, color: theme.value === 'dark' ? '#e4e4e7' : '#18181b' } },
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
  if (selected.value) openInspector(selected.value, resolveMemory, (mid) => expandSemantic('m:' + mid))
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
  if (!(await confirmDialog('Remove this link?', { actionLabel: 'Remove' }))) return
  try {
    await memoryClient.unlink({ id: String(e.from).slice(2), targetId: String(e.to).slice(2) })
    edges.remove(eid)
  } catch (err) {
    handleError(err)
  }
}

// Clicking an orange duplicate-candidate edge dismisses it: the pair is recorded
// as confirmed-not-a-duplicate so the worker never re-flags it.
async function maybeDismiss(eid) {
  const e = edges.get(eid)
  if (!e) return
  if (!(await confirmDialog('Mark these two memories as NOT duplicates? They won’t be flagged again.', { actionLabel: 'Mark not duplicate' }))) return
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
  try {
    // searchSimilar reuses the memory's stored vector server-side (Weaviate
    // nearObject) — it does NOT re-embed the text, so big memories cost no
    // inference. maxDistance is a server-side relevance cutoff so we never link
    // unrelated memories; the seed memory is excluded by the server.
    const res = await memoryClient.searchSimilar({ id: node.mem.id, namespace: '*', limit: 6, maxDistance: cutoff.value })
    const hits = res.hits.filter((h) => 'm:' + h.memory.id !== id)
    if (hits.length === 0) {
      toast.info(`No memories within distance ${cutoff.value} of "${truncate(node.mem.text, 30)}".`)
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
    refreshNamespaces()
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
  refreshNamespaces()
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
