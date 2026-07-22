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
import { toast } from 'vue-sonner'
import { memoryClient } from '@/utils/connect'
import { useAuthStore } from '@/stores/auth'
import { theme } from '@/lib/theme'
import { truncate } from '@/utils/text'
import { nsNodeColors } from '@/lib/nsColor'
import { openInspector, inspectorChanged } from '@/lib/inspector'
import { Cloud, Loader2, Search } from 'lucide-vue-next'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'

const router = useRouter()
const auth = useAuthStore()

const container = ref(null)
const query = ref('')
const namespace = ref('*')
const limit = ref(25)
const cutoff = ref(0.6)
const loading = ref(false)
const error = ref('')
const searched = ref(false)
const resultCount = ref(0)

// Recolor nodes + labels when the theme toggles while a graph is on screen.
watch(theme, () => { if (network) { recolorNodes(); render() } })
// An inspector edit/delete can change the memory this view is showing —
// re-run the active search so the cloud reflects it.
watch(inspectorChanged, () => { if (searched.value) run() })

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

// resolveMemory looks up a linked id within the currently loaded result set,
// for the inspector to follow links without a get-memory-by-id RPC.
function resolveMemory(id) {
  return nodes?.get('m:' + id)?.mem
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
    const dark = theme.value === 'dark'
    for (const h of res.hits) {
      const m = h.memory
      nodeList.push({
        id: 'm:' + m.id,
        label: truncate(m.text, 30),
        title: m.text,
        color: nsNodeColors(m.namespace, dark),
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
      toast.info(`No memories within distance ${cutoff.value} of "${truncate(q, 40)}".`)
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
}

// Repaint memory-node colors in place when the theme toggles, so an existing
// cloud doesn't stay stuck with the previous theme's palette.
function recolorNodes() {
  if (!nodes) return
  const dark = theme.value === 'dark'
  const updates = nodes
    .get({ filter: (n) => n.id !== QUERY_ID })
    .map((n) => ({ id: n.id, color: nsNodeColors(n.mem?.namespace, dark) }))
  if (updates.length) nodes.update(updates)
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
  if (!params.nodes.length) return
  const id = params.nodes[0]
  if (!String(id).startsWith('m:')) return
  const mem = nodes.get(id)?.mem
  if (mem) openInspector(mem, resolveMemory)
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
