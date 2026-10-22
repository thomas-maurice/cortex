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
          <ExpiryPicker v-model:model="draftExpiry" />
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
        <Input v-model="query" :disabled="staleActive" :placeholder="staleActive ? 'search is off while reviewing stale memories' : 'semantic query (blank = list newest)'" @keyup.enter="reload" />
      </div>
      <div class="grid gap-1.5" style="width: 220px">
        <Label class="text-xs">Stale</Label>
        <Select v-model="stale" :disabled="loading">
          <SelectTrigger size="sm" aria-label="Stale filter">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="off">Off (newest first)</SelectItem>
            <SelectItem value="never">Never used</SelectItem>
            <SelectItem value="30">Not used in 30 days</SelectItem>
            <SelectItem value="90">Not used in 90 days</SelectItem>
            <SelectItem value="180">Not used in 180 days</SelectItem>
          </SelectContent>
        </Select>
      </div>
      <Button :disabled="loading" @click="reload">
        <component :is="query ? SearchIcon : RotateCw" class="size-4" />
        {{ query ? 'Search' : 'Refresh' }}
      </Button>
    </div>
    <div class="flex items-center gap-2">
      <Switch id="include-expired" v-model="includeExpired" />
      <Label for="include-expired" class="text-xs text-muted-foreground">Include expired memories (hidden from search and recall by default)</Label>
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

    <div v-if="staleActive" class="space-y-1 text-xs text-muted-foreground">
      <p>
        Stale review (by actual use: injected by the recall hook or delivered in full by the agent; browsing here does not count), oldest last use first.
        Memories younger than {{ STALE_MIN_AGE_DAYS }} days are never listed.
        {{ total }} match<span v-if="total !== 1">es</span>.
      </p>
      <p v-if="trackingSince">
        Usage tracked since {{ formatDate(trackingSince) }}.
        <span v-if="trackingLimited" class="font-medium text-amber-600 dark:text-amber-400">
          That is less than the {{ staleWindowDays }}-day window, so results are limited: memories cannot be judged unused before tracking began.
        </span>
      </p>
      <label class="inline-flex cursor-pointer items-center gap-2">
        <input v-model="includePrefs" type="checkbox" class="size-4" :disabled="loading" />
        Include memories tagged "preference" (hidden by default)
      </label>
    </div>

    <div v-if="memories.length" class="flex flex-wrap items-center gap-3 text-sm">
      <label class="inline-flex cursor-pointer items-center gap-2">
        <input type="checkbox" class="size-4" :checked="allSelected" :indeterminate.prop="someSelected && !allSelected" aria-label="Select all on page" @change="toggleAll" />
        Select all on page
      </label>
      <span v-if="selected.size" class="text-muted-foreground">{{ selected.size }} selected</span>
      <Button v-if="selected.size" variant="destructive" size="sm" :disabled="bulkBusy" @click="bulkTrash">
        <Trash2 class="size-4" />Move {{ selected.size }} to trash
      </Button>
      <Button v-if="selected.size" variant="ghost" size="sm" @click="selected = new Set()">Clear</Button>
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

    <p v-if="filteredNote" class="text-xs text-muted-foreground">{{ filteredNote }}</p>

    <Card v-for="m in memories" :key="m.id">
      <CardContent class="py-4">
        <div class="flex items-start justify-between gap-3">
          <input type="checkbox" class="mt-1 size-4 shrink-0" :checked="selected.has(m.id)" aria-label="Select memory" @change="toggleOne(m.id)" />
          <div class="min-w-0 flex-1 cursor-pointer" @click="openInspector(m, resolveMemory)">
            <!-- Search hits on long memories: the matched chunk, as PLAIN text (escaped by
                 Vue interpolation, never v-html) so it can't inject markup. -->
            <p
              v-if="m._match && m._match.text && m._match.text.trim() !== m.text.trim()"
              class="mb-2 line-clamp-3 rounded-md border-l-2 border-amber-500 bg-amber-500/10 px-2 py-1 text-xs"
              title="The part of this memory that matched your search"
            >
              <span class="font-medium text-amber-700 dark:text-amber-400">matched:</span>
              {{ m._match.text.trim() }}
            </p>
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
          <ExpiryBadge :valid-until="m.validUntil" />
          <span v-if="m._distance !== undefined">dist: {{ m._distance.toFixed(3) }}</span>
          <Badge v-if="staleActive" variant="outline" title="times this memory was delivered into an agent's context">
            <Flame class="size-3" />{{ m.useCount }} use{{ m.useCount === 1 ? '' : 's' }}
          </Badge>
          <span v-if="staleActive && !m.lastUsedAt" class="inline-flex items-center gap-1 text-amber-600 dark:text-amber-400">
            <History class="size-3" />never used
          </span>
          <span v-if="staleActive && m.lastUsedAt" class="inline-flex items-center gap-1" title="when this memory was last delivered into an agent's context">
            <History class="size-3" />last used {{ formatDate(m.lastUsedAt) }}
          </span>
          <Badge v-if="!staleActive && m.accessCount" variant="secondary" class="text-amber-600 dark:text-amber-400" title="times the agent recalled this memory (living memory)">
            <Flame class="size-3" />{{ m.accessCount }}
          </Badge>
          <span v-if="!staleActive && m.lastAccessedAt" class="inline-flex items-center gap-1" title="when this memory was last recalled">
            <History class="size-3" />{{ formatDate(m.lastAccessedAt) }}
          </span>
        </div>
      </CardContent>
    </Card>

    <div v-if="staleActive && total > PAGE_SIZE" class="flex items-center justify-center gap-3 text-sm">
      <Button variant="outline" size="sm" :disabled="loading || offset === 0" @click="goPage(-1)">Previous</Button>
      <span class="text-muted-foreground">{{ offset + 1 }}–{{ Math.min(offset + PAGE_SIZE, total) }} of {{ total }}</span>
      <Button variant="outline" size="sm" :disabled="loading || offset + PAGE_SIZE >= total" @click="goPage(1)">Next</Button>
    </div>
  </div>
</template>

<script setup>
import { timestampDate } from '@bufbuild/protobuf/wkt'
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { toast } from 'vue-sonner'
import { memoryClient } from '@/utils/connect'
import { renderMarkdown } from '@/utils/markdown'
import { useAuthStore } from '@/stores/auth'
import { confirmDialog } from '@/lib/confirm'
import { openInspector, inspectorChanged } from '@/lib/inspector'
import ClampedMarkdown from '@/components/ClampedMarkdown.vue'
import ExpiryBadge from '@/components/ExpiryBadge.vue'
import ExpiryPicker from '@/components/ExpiryPicker.vue'
import { expiryRequest } from '@/lib/expiry'
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
import { Switch } from '@/components/ui/switch'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Textarea } from '@/components/ui/textarea'
import { StaleMode } from '@/gen/cortex/v1/cortex_pb'

const router = useRouter()
const route = useRoute()
const auth = useAuthStore()

// ?tag=… (repeatable) and ?namespace=… preselect filters, e.g. from the Tags view.
const qTags = [route.query.tag].flat().filter((t) => typeof t === 'string' && t)
const namespace = ref(typeof route.query.namespace === 'string' && route.query.namespace ? route.query.namespace : '*')
const query = ref('')
const activeTags = ref(qTags)
const memories = ref([])
const loading = ref(false)
const error = ref('')

const STALE_MIN_AGE_DAYS = 30 // sent explicitly so the UI text can't drift from the server default
// stale: 'off' | 'never' | '30' | '90' | '180' (days not recalled). Stale mode is a
// server-side scan (List RPC), ignores the semantic query, and is paginated.
const stale = ref('off')
const staleActive = computed(() => stale.value !== 'off')
const includePrefs = ref(false)
const trackingSince = ref(null)
const offset = ref(0)
const total = ref(0)

// The span being judged: the idle threshold, or the min-age guard for "never used".
const staleWindowDays = computed(() => (stale.value === 'never' ? STALE_MIN_AGE_DAYS : Number(stale.value)))
const trackingLimited = computed(() => {
  if (!trackingSince.value) return false
  return Date.now() - timestampDate(trackingSince.value).getTime() < staleWindowDays.value * 86400000
})

const selected = ref(new Set())
const bulkBusy = ref(false)
const allSelected = computed(() => memories.value.length > 0 && memories.value.every((m) => selected.value.has(m.id)))
const someSelected = computed(() => memories.value.some((m) => selected.value.has(m.id)))

function toggleOne(id) {
  const next = new Set(selected.value)
  if (!next.delete(id)) next.add(id)
  selected.value = next
}

function toggleAll() {
  selected.value = allSelected.value ? new Set() : new Set(memories.value.map((m) => m.id))
}

const showNew = ref(false)
const saving = ref(false)
const draft = ref({ text: '', namespace: '', tags: '' })
const draftExpiry = ref({ preset: 'none', date: '' })
const includeExpired = ref(false)

function formatDate(ts) {
  try {
    return timestampDate(ts).toLocaleString()
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

// Weaviate's `tags` is word-tokenized, so its tag filter is fuzzy (case/token
// matches); the Tags view counts exact tags. We re-filter to exact,
// case-sensitive equality so the list matches the Tags view count. Because the
// fuzzy server page can contain non-matches, a fixed 50-row page could come back
// short (or empty) even though exact matches exist further down: when filtering
// shrank a FULL server page, re-ask with a doubled limit (the API has no offset),
// bounded to MAX_ROUNDS, until the exact list fills a page or the server runs out.
const PAGE = 50
const MAX_ROUNDS = 10
const SERVER_MAX = 10000
const PAGE_SIZE = PAGE
const filteredNote = ref('')

function exactTagMatch(list) {
  return activeTags.value.length
    ? list.filter((m) => activeTags.value.every((t) => (m.tags || []).includes(t)))
    : list
}

// fetchPage(limit) -> array of memories. Returns the exact-filtered page.
async function fetchExact(fetchPage) {
  filteredNote.value = ''
  let limit = PAGE
  for (let round = 0; round < MAX_ROUNDS; round++) {
    const raw = await fetchPage(limit)
    const exact = exactTagMatch(raw)
    const serverExhausted = raw.length < limit
    if (exact.length >= PAGE || serverExhausted || limit >= SERVER_MAX || !activeTags.value.length) {
      if (raw.length > exact.length) {
        filteredNote.value = `${raw.length - exact.length} fuzzy tag match(es) hidden: only exact, case-sensitive tags are shown.`
      }
      return exact.slice(0, PAGE)
    }
    limit = Math.min(limit * 2, SERVER_MAX)
  }
  filteredNote.value = 'Search depth limit reached: some matches may not be shown.'
  return exactTagMatch(await fetchPage(limit)).slice(0, PAGE)
}

// reload restarts from the first page (filters changed / refresh); load keeps
// the current page (used after edits and bulk deletes).
function reload() {
  offset.value = 0
  return load()
}

function goPage(dir) {
  offset.value = Math.max(0, offset.value + dir * PAGE_SIZE)
  return load()
}

async function load(retry = false) {
  // Stale mode is a full server-side scan (single-flight per tenant): never overlap loads.
  if (staleActive.value && loading.value && !retry) return
  loading.value = true
  error.value = ''
  selected.value = new Set()
  try {
    if (staleActive.value) {
      // The server filters tags exactly in stale mode: no client-side post-filter (it would break totals/pages).
      filteredNote.value = ''
      const req = { namespace: namespace.value, limit: PAGE_SIZE, offset: offset.value, tags: activeTags.value, minAgeDays: STALE_MIN_AGE_DAYS, includePreferences: includePrefs.value, includeExpired: includeExpired.value }
      if (stale.value === 'never') {
        req.stale = StaleMode.NEVER_RECALLED
      } else {
        req.stale = StaleMode.NOT_RECALLED_SINCE
        req.staleDays = Number(stale.value)
      }
      const res = await memoryClient.list(req)
      memories.value = res.memories
      total.value = res.total
      trackingSince.value = res.trackingSince ?? null
      // The last page can vanish after deletes: step back instead of showing nothing.
      if (res.memories.length === 0 && offset.value > 0 && res.total > 0) {
        offset.value = Math.max(0, Math.floor((res.total - 1) / PAGE_SIZE) * PAGE_SIZE)
        return load(true)
      }
    } else if (query.value.trim()) {
      // noReinforce: browsing in the UI must not count as a recall — only the
      // agent's (MCP) searches feed the living-memory usage signal.
      memories.value = await fetchExact(async (limit) => {
        const res = await memoryClient.search({ query: query.value, namespace: namespace.value, limit, tags: activeTags.value, noReinforce: true, includeExpired: includeExpired.value })
        return res.hits.map((h) => ({ ...h.memory, _distance: h.distance, _match: h.match }))
      })
    } else {
      memories.value = await fetchExact(async (limit) => {
        const res = await memoryClient.list({ namespace: namespace.value, limit, tags: activeTags.value, includeExpired: includeExpired.value })
        return res.memories
      })
    }
  } catch (e) {
    if (staleActive.value && e instanceof ConnectError && e.code === Code.ResourceExhausted) {
      toast.info('Stale scan is busy — try again in a moment.')
    } else {
      handleError(e)
    }
  } finally {
    loading.value = false
  }
}

function filterByNamespace(ns) {
  if (staleActive.value && loading.value) return
  namespace.value = ns
  reload()
}

function toggleTag(t) {
  if (staleActive.value && loading.value) return
  const i = activeTags.value.indexOf(t)
  if (i === -1) activeTags.value.push(t)
  else activeTags.value.splice(i, 1)
  reload()
}

function resolveMemory(id) {
  return memories.value.find((x) => x.id === id)
}

async function save() {
  const exp = expiryRequest(draftExpiry.value)
  if (exp.error) {
    error.value = exp.error
    return
  }
  saving.value = true
  error.value = ''
  try {
    const tags = draft.value.tags.split(',').map((t) => t.trim()).filter(Boolean)
    await memoryClient.save({ text: draft.value.text, namespace: draft.value.namespace, tags, ...exp })
    draft.value = { text: '', namespace: '', tags: '' }
    draftExpiry.value = { preset: 'none', date: '' }
    toast.success('Queued for indexing — it will appear shortly.')
  } catch (e) {
    handleError(e)
  } finally {
    saving.value = false
  }
}

async function remove(id) {
  if (!(await confirmDialog('Delete this memory? It is moved to the trash and can be restored from there.', { actionLabel: 'Delete' }))) return
  try {
    await memoryClient.delete({ id })
    memories.value = memories.value.filter((m) => m.id !== id)
    toast.success('Memory moved to trash')
  } catch (e) {
    handleError(e)
  }
}

async function bulkTrash() {
  const ids = [...selected.value]
  if (!ids.length) return
  if (!(await confirmDialog(`Move ${ids.length} memor${ids.length === 1 ? 'y' : 'ies'} to the trash? They can be restored from the trash (or with Undo).`, { actionLabel: 'Move to trash' }))) return
  bulkBusy.value = true
  try {
    const res = await memoryClient.deleteMany({ ids, reason: staleActive.value ? 'stale-review' : '' })
    const done = res.results.filter((r) => r.deleted).map((r) => r.id)
    const failed = res.results.filter((r) => !r.deleted)
    if (failed.length) toast.error(`${failed.length} could not be deleted: ${failed[0].error}`)
    if (done.length) {
      toast.success(`${done.length} memor${done.length === 1 ? 'y' : 'ies'} moved to trash`, {
        duration: 10000,
        action: { label: 'Undo', onClick: () => undoTrash(done) },
      })
    }
    await load()
  } catch (e) {
    handleError(e)
  } finally {
    bulkBusy.value = false
  }
}

async function undoTrash(ids) {
  try {
    await memoryClient.restoreFromTrash({ ids })
    toast.success('Restored — memories are re-queued for indexing and will reappear shortly.')
    await load()
  } catch (e) {
    handleError(e)
  }
}

watch(inspectorChanged, () => load())
watch(stale, reload)
watch(includeExpired, reload)
watch(includePrefs, reload)

onMounted(reload)
</script>
