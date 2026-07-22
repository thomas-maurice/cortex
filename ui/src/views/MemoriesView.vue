<template>
  <div class="space-y-4">
    <div>
      <Button variant="default" size="sm" @click="showNew = !showNew">
        <Plus class="size-4" />New memory
      </Button>
      <Card v-if="showNew" class="mt-2">
        <CardContent class="space-y-2 py-4">
          <Textarea v-model="draft.text" rows="3" placeholder="Memory text…" />
          <div class="flex flex-wrap gap-2">
            <Input v-model="draft.namespace" class="flex-1" placeholder="namespace (blank = server default)" />
            <Input v-model="draft.tags" class="flex-1" placeholder="tags, comma separated" />
            <Button size="sm" :disabled="!draft.text.trim() || saving" @click="save">Save</Button>
          </div>
        </CardContent>
      </Card>
    </div>

    <div class="flex flex-wrap items-end gap-2">
      <div class="grid gap-1.5" style="width: 180px">
        <Label class="text-xs">Namespace</Label>
        <Input v-model="namespace" placeholder="* = all" @keyup.enter="reload" />
      </div>
      <div class="grid flex-1 gap-1.5">
        <Label class="text-xs">Search</Label>
        <Input v-model="query" placeholder="semantic query (blank = list newest)" @keyup.enter="reload" />
      </div>
      <Button :disabled="loading" @click="reload">
        <component :is="query ? SearchIcon : RotateCw" class="size-4" />
        {{ query ? 'Search' : 'Refresh' }}
      </Button>
    </div>

    <div v-if="activeTags.length" class="flex flex-wrap items-center gap-2">
      <span class="text-xs text-muted-foreground">Filtering by tag:</span>
      <Badge
        v-for="t in activeTags"
        :key="t"
        variant="secondary"
        class="cursor-pointer hover:bg-secondary/80"
        title="Remove tag filter"
        @click="toggleTag(t)"
      >
        <Tag class="size-3" />{{ t }}
        <X class="size-3" />
      </Badge>
    </div>

    <Alert v-if="error" variant="destructive">
      <AlertDescription>{{ error }}</AlertDescription>
    </Alert>

    <div v-if="loading" class="py-16 text-center text-muted-foreground" role="status" aria-live="polite" aria-label="Loading memories">
      <Loader2 class="mx-auto size-8 animate-spin" />
    </div>

    <div v-else-if="memories.length === 0" class="py-16 text-center text-muted-foreground">
      <Database class="mx-auto mb-2 size-8" />
      No memories found.
    </div>

    <Card v-for="m in memories" :key="m.id">
      <CardContent class="py-4">
        <div class="flex items-start justify-between gap-3">
          <div class="min-w-0 flex-1 cursor-pointer" @click="openInspector(m, resolveMemory)">
            <ClampedMarkdown :html="renderMarkdown(m.text)" />
          </div>
          <div class="flex shrink-0 gap-1">
            <Button variant="outline" size="icon" class="size-8" title="Edit" aria-label="Edit memory" @click="openInspector(m, resolveMemory)">
              <Pencil class="size-3.5" />
            </Button>
            <Button variant="outline" size="icon" class="size-8 text-destructive hover:text-destructive" title="Delete" aria-label="Delete memory" @click="remove(m.id)">
              <Trash2 class="size-3.5" />
            </Button>
          </div>
        </div>
        <div class="mt-2 flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
          <Badge
            variant="secondary"
            class="cursor-pointer hover:bg-secondary/80"
            title="Filter by namespace"
            @click="filterByNamespace(m.namespace)"
          >
            <Layers class="size-3" />{{ m.namespace }}
          </Badge>
          <Badge
            v-for="t in m.tags"
            :key="t"
            variant="outline"
            class="cursor-pointer hover:bg-accent"
            title="Filter by tag"
            @click="toggleTag(t)"
          >
            <Tag class="size-3" />{{ t }}
          </Badge>
          <span v-if="m.source">src: {{ m.source }}</span>
          <span v-if="m.conversationId" class="inline-flex items-center gap-1 font-mono">
            <MessagesSquare class="size-3" />{{ m.conversationId }}
          </span>
          <span v-if="m.createdAt">{{ formatDate(m.createdAt) }}</span>
          <span v-if="m._distance !== undefined">dist: {{ m._distance.toFixed(3) }}</span>
          <Badge v-if="m.accessCount" variant="secondary" class="text-amber-600 dark:text-amber-400" title="times the agent recalled this memory (living memory)">
            <Flame class="size-3" />{{ m.accessCount }}
          </Badge>
          <span v-if="m.lastAccessedAt" class="inline-flex items-center gap-1" title="when this memory was last recalled">
            <History class="size-3" />{{ formatDate(m.lastAccessedAt) }}
          </span>
        </div>
      </CardContent>
    </Card>
  </div>
</template>

<script setup>
import { ref, onMounted, watch } from 'vue'
import { useRouter } from 'vue-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { toast } from 'vue-sonner'
import { memoryClient } from '@/utils/connect'
import { renderMarkdown } from '@/utils/markdown'
import { useAuthStore } from '@/stores/auth'
import { confirmDialog } from '@/lib/confirm'
import { openInspector, inspectorChanged } from '@/lib/inspector'
import ClampedMarkdown from '@/components/ClampedMarkdown.vue'
import {
  Database,
  Flame,
  History,
  Layers,
  Loader2,
  MessagesSquare,
  Pencil,
  Plus,
  RotateCw,
  Search as SearchIcon,
  Tag,
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

const namespace = ref('*')
const query = ref('')
const activeTags = ref([])
const memories = ref([])
const loading = ref(false)
const error = ref('')

const showNew = ref(false)
const saving = ref(false)
const draft = ref({ text: '', namespace: '', tags: '' })

function formatDate(ts) {
  try {
    return ts.toDate().toLocaleString()
  } catch {
    return ''
  }
}

function handleError(e) {
  if (e instanceof ConnectError && e.code === Code.Unauthenticated) {
    auth.logout()
    router.push({ name: 'login' })
    return
  }
  error.value = e.message || 'Request failed'
}

async function reload() {
  loading.value = true
  error.value = ''
  try {
    if (query.value.trim()) {
      // noReinforce: browsing in the UI must not count as a recall — only the
      // agent's (MCP) searches feed the living-memory usage signal.
      const res = await memoryClient.search({ query: query.value, namespace: namespace.value, limit: 50, tags: activeTags.value, noReinforce: true })
      memories.value = res.hits.map((h) => ({ ...h.memory, _distance: h.distance }))
    } else {
      const res = await memoryClient.list({ namespace: namespace.value, limit: 50, tags: activeTags.value })
      memories.value = res.memories
    }
  } catch (e) {
    handleError(e)
  } finally {
    loading.value = false
  }
}

function filterByNamespace(ns) {
  namespace.value = ns
  reload()
}

function toggleTag(t) {
  const i = activeTags.value.indexOf(t)
  if (i === -1) activeTags.value.push(t)
  else activeTags.value.splice(i, 1)
  reload()
}

function resolveMemory(id) {
  return memories.value.find((x) => x.id === id)
}

async function save() {
  saving.value = true
  error.value = ''
  try {
    const tags = draft.value.tags.split(',').map((t) => t.trim()).filter(Boolean)
    await memoryClient.save({ text: draft.value.text, namespace: draft.value.namespace, tags })
    draft.value = { text: '', namespace: '', tags: '' }
    toast.success('Queued for indexing — it will appear shortly.')
  } catch (e) {
    handleError(e)
  } finally {
    saving.value = false
  }
}

async function remove(id) {
  if (!(await confirmDialog('Delete this memory?', { actionLabel: 'Delete' }))) return
  try {
    await memoryClient.delete({ id })
    memories.value = memories.value.filter((m) => m.id !== id)
    toast.success('Memory deleted')
  } catch (e) {
    handleError(e)
  }
}

watch(inspectorChanged, reload)

onMounted(reload)
</script>
