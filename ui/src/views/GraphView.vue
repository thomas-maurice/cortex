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

    <Alert v-if="notice">
      <AlertDescription>{{ notice }}</AlertDescription>
    </Alert>
    <Alert v-if="error" variant="destructive">
      <AlertDescription>{{ error }}</AlertDescription>
    </Alert>

    <div class="relative rounded-md border" style="height: 72vh">
      <div v-if="loading" class="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 text-muted-foreground">
        <Loader2 class="size-8 animate-spin" />
      </div>
      <div ref="container" style="height: 100%"></div>

      <!-- Details panel for the selected memory. Read-only; no graph mutation. -->
      <Card
        v-if="selected"
        class="absolute top-0 right-0 m-2 shadow-sm"
        style="width: 320px; max-height: calc(72vh - 1rem); overflow: auto"
      >
        <CardContent class="py-2">
          <div class="mb-1 flex items-start justify-between">
            <Badge variant="secondary">
              <Layers class="size-3" />{{ selected.namespace }}
            </Badge>
            <Button variant="ghost" size="icon" class="size-6" aria-label="Close details panel" @click="deselect">
              <X class="size-3.5" />
            </Button>
          </div>
          <!-- Edit mode: textarea + tags, replaces the read view in place. -->
          <div v-if="editing" class="space-y-2">
            <Textarea v-model="editText" rows="8" placeholder="Memory text (Markdown)…" />
            <Input v-model="editTags" placeholder="tags, comma separated" />
            <div class="flex gap-2">
              <Button size="sm" class="flex-1" :disabled="!editText.trim() || savingEdit" @click="saveEdit">
                <Pencil class="size-4" />Save
              </Button>
              <Button size="sm" variant="outline" class="flex-1" :disabled="savingEdit" @click="editing = false">Cancel</Button>
            </div>
            <p v-if="savingEdit" class="text-sm text-muted-foreground">Queued for re-indexing…</p>
          </div>
          <template v-else>
            <div class="mb-2 text-sm markdown-body" v-html="renderMarkdown(selected.text)"></div>
            <div class="mb-2 text-sm text-muted-foreground">
              <Badge v-for="t in selected.tags" :key="t" variant="outline" class="mr-1">#{{ t }}</Badge>
            </div>
            <div v-if="selected.conversationId" class="mb-2 flex items-center gap-1 text-sm text-muted-foreground">
              <MessagesSquare class="size-3.5" />
              <span class="font-mono">{{ selected.conversationId }}</span>
            </div>
            <div class="mb-2 text-sm text-muted-foreground">
              {{ (selected.linkedIds || []).length }} explicit link(s)
              <span v-if="(selected.dupCandidates || []).length" style="color: #fd7e14">
                · {{ selected.dupCandidates.length }} duplicate candidate(s)
              </span>
            </div>
            <div v-if="selected.accessCount || selected.lastAccessedAt" class="mb-2 flex flex-wrap items-center gap-2 text-sm text-muted-foreground">
              <Badge v-if="selected.accessCount" variant="secondary" class="text-amber-600 dark:text-amber-400" title="times the agent recalled this memory (living memory)">
                <Flame class="size-3" />{{ selected.accessCount }} recall(s)
              </Badge>
              <span v-if="selected.lastAccessedAt" class="inline-flex items-center gap-1" title="when this memory was last recalled">
                <History class="size-3" />{{ fmtDate(selected.lastAccessedAt) }}
              </span>
            </div>
            <Button variant="outline" size="sm" class="mb-2 w-full" @click="expandSemantic('m:' + selected.id)">
              <Search class="size-4" />Find similar
            </Button>
            <Button variant="outline" size="sm" class="mb-2 w-full" @click="startEdit">
              <Pencil class="size-4" />Edit memory
            </Button>
            <Button variant="outline" size="sm" class="w-full text-destructive hover:text-destructive" @click="deleteSelected">
              <Trash2 class="size-4" />Delete memory
            </Button>
          </template>
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
import { memoryClient } from '@/utils/connect'
import { renderMarkdown } from '@/utils/markdown'
import { useAuthStore } from '@/stores/auth'
import { theme } from '@/lib/theme'
import { truncate } from '@/utils/text'
import {
  Flame,
  History,
  Layers,
  Link2,
  Loader2,
  MessagesSquare,
  Pencil,
  RotateCw,
  Search,
  Trash2,
  Unlink,
  X,
} from 'lucide-vue-next'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { Textarea } from '@/components/ui/textarea'

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

// Inline edit state for the selected memory's card.
const editing = ref(false)
const editText = ref('')
const editTags = ref('')
const savingEdit = ref(false)
// Changing/closing the selection always drops out of edit mode.
watch(selected, () => { editing.value = false })
// Recolor node labels when the theme toggles while a graph is on screen.
watch(theme, () => { if (network) render() })

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

// fmtDate renders a protobuf Timestamp for display, empty on any failure.
function fmtDate(ts) {
  try {
    return ts.toDate().toLocaleString()
  } catch {
    return ''
  }
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
// startEdit seeds the edit form from the selected memory. saveEdit calls
// UpdateMemory (text + tags; namespace is left untouched so the memory does not
// move) and patches the node in place so the graph reflects the change without a
// full reload — the worker re-embeds asynchronously.
function startEdit() {
  if (!selected.value) return
  editText.value = selected.value.text
  editTags.value = (selected.value.tags || []).join(', ')
  editing.value = true
}

async function saveEdit() {
  const mem = selected.value
  if (!mem) return
  savingEdit.value = true
  error.value = ''
  try {
    const tags = editTags.value.split(',').map((t) => t.trim()).filter(Boolean)
    await memoryClient.updateMemory({ id: mem.id, text: editText.value, tags, replaceTags: true })
    const updated = { ...mem, text: editText.value, tags }
    nodes.update({ id: 'm:' + mem.id, label: truncate(editText.value, 30), title: editText.value, mem: updated })
    selected.value = updated // resets editing via the watch
  } catch (e) {
    handleError(e)
  } finally {
    savingEdit.value = false
  }
}

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
