<template>
  <div class="space-y-4">
    <div class="flex flex-wrap items-end gap-2">
      <div class="grid flex-1 gap-1.5">
        <Label class="text-xs">Query</Label>
        <Input
          v-model="query"
          placeholder="type anything — it gets vectorised and matched against your memories"
          @keyup.enter="run"
        />
      </div>
      <div class="grid gap-1.5" style="width: 120px">
        <Label class="text-xs">Namespace</Label>
        <Input v-model="namespace" placeholder="* = all" />
      </div>
      <div class="grid gap-1.5" style="width: 90px">
        <Label class="text-xs">Limit</Label>
        <Input v-model.number="limit" type="number" min="1" max="100" />
      </div>
      <div class="grid gap-1.5" style="width: 110px">
        <Label class="text-xs" title="relevance cutoff (lower = closer match). Search is hybrid (keyword + vector), so this blends both; raise it to surface weaker matches, lower it to tighten.">Max dist</Label>
        <Input v-model.number="cutoff" type="number" min="0.1" max="1.5" step="0.05" />
      </div>
      <Button size="sm" :disabled="loading || !query.trim()" @click="run">
        <Search class="size-4" />Explore
      </Button>
      <Button size="sm" variant="outline" :disabled="!searched" @click="clearCloud">Clear</Button>
    </div>

    <Alert v-if="notice">
      <AlertDescription>{{ notice }}</AlertDescription>
    </Alert>
    <Alert v-if="error" variant="destructive">
      <AlertDescription>{{ error }}</AlertDescription>
    </Alert>

    <div class="relative rounded-md border" style="height: 74vh">
      <div v-if="loading" class="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 text-muted-foreground">
        <Loader2 class="size-8 animate-spin" />
      </div>
      <div v-if="!searched && !loading" class="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 text-center text-muted-foreground">
        <Cloud class="mx-auto mb-2 size-8" />
        Enter a query to see a cloud of related memories.
      </div>
      <div ref="container" style="height: 100%"></div>

      <Card
        v-if="selected"
        class="absolute top-0 right-0 m-2 shadow-sm"
        style="width: 320px; max-height: calc(74vh - 1rem); overflow: auto"
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
            <div v-if="(selected.tags || []).length" class="mb-2 text-sm text-muted-foreground">
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
      <span v-if="searched">{{ resultCount }} match(es) · </span>
      Central <span class="text-destructive">★</span> = your query · closer + bigger = more relevant · edge number = vector distance.
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
  Cloud,
  Flame,
  History,
  Layers,
  Loader2,
  MessagesSquare,
  Pencil,
  Search,
  Trash2,
  X,
} from 'lucide-vue-next'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Textarea } from '@/components/ui/textarea'

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

// startEdit seeds the edit form from the selected memory. saveEdit calls
// UpdateMemory (text + tags; namespace untouched so the memory does not move) and
// patches the cloud node in place; the worker re-embeds asynchronously.
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
    selected.value = null
  } catch (e) {
    handleError(e)
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
    // Label color follows the app theme — vis-network defaults to near-black,
    // which is unreadable on the dark background.
    nodes: { borderWidth: 1, font: { size: 12, color: theme.value === 'dark' ? '#e4e4e7' : '#18181b' } },
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
