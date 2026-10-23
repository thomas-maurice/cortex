<template>
  <div class="space-y-4">
    <h1 class="flex items-center gap-2 text-xl font-semibold"><Trash2 class="size-5" />Trash</h1>

    <p class="text-sm text-muted-foreground">
      Deleted memories are kept here and can be restored
      <template v-if="retentionDays > 0">until {{ retentionLabel }} after deletion, then they are purged automatically.</template>
      <template v-else>until you purge them (automatic purging is disabled).</template>
    </p>

    <Alert v-if="error" variant="destructive">
      <AlertDescription>{{ error }}</AlertDescription>
    </Alert>

    <div class="flex flex-wrap items-center gap-2">
      <Input v-model="nsFilter" class="h-8 w-48" placeholder="namespace…" aria-label="Filter by namespace" @keyup.enter="applyFilters" />
      <select
        v-model="reasonFilter"
        class="h-8 rounded-md border bg-background px-2 text-sm"
        aria-label="Filter by reason"
        @change="applyFilters"
      >
        <option value="">All reasons</option>
        <option value="delete">delete</option>
        <option value="stale-review">stale-review</option>
        <option value="duplicate-review">duplicate-review</option>
        <option value="supersedes">supersedes</option>
        <option value="merged-into">merged-into</option>
        <option value="namespace-delete">namespace-delete</option>
        <option value="expired">expired</option>
      </select>
      <Button size="sm" variant="outline" :disabled="loading" @click="applyFilters">Filter</Button>
      <Button size="sm" :disabled="loading" @click="reload">
        <RotateCw class="size-4" />Refresh
      </Button>
    </div>

    <div class="flex flex-wrap items-center gap-2">
      <Button size="sm" variant="outline" :disabled="busy || selected.size === 0" @click="restoreSelected">
        <Undo2 class="size-4" />Restore selected ({{ selected.size }})
      </Button>
      <Button size="sm" variant="outline" class="text-destructive hover:text-destructive" :disabled="busy || selected.size === 0" @click="purgeSelected">
        <Trash2 class="size-4" />Purge selected ({{ selected.size }})
      </Button>
      <Button size="sm" variant="outline" :disabled="busy || total === 0" @click="restoreAllMatching">
        <Undo2 class="size-4" />Restore all{{ filtered ? ' matching' : '' }} ({{ total }})
      </Button>
      <Button size="sm" variant="outline" class="text-destructive hover:text-destructive" :disabled="busy || total === 0" @click="purgeAll">
        <Trash2 class="size-4" />Purge all{{ filtered ? ' matching' : '' }}
      </Button>
    </div>

    <div v-if="loading" class="py-16 text-center text-muted-foreground" role="status" aria-live="polite" aria-label="Loading trash">
      <Loader2 class="mx-auto size-8 animate-spin" />
    </div>

    <div v-else-if="entries.length === 0" class="py-16 text-center text-muted-foreground">
      <Trash2 class="mx-auto mb-2 size-8" />
      Trash is empty.
    </div>

    <div v-else class="rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead style="width: 1%">
              <input type="checkbox" aria-label="Select all shown" :checked="allShownSelected" @change="toggleAllShown" />
            </TableHead>
            <TableHead>Memory</TableHead>
            <TableHead>Namespace</TableHead>
            <TableHead>Deleted</TableHead>
            <TableHead>Reason</TableHead>
            <TableHead class="text-right" style="width: 1%">Actions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          <TableRow v-for="e in entries" :key="e.id">
            <TableCell>
              <input type="checkbox" :aria-label="`Select ${e.id}`" :checked="selected.has(e.id)" @change="toggle(e.id)" />
            </TableCell>
            <TableCell class="max-w-[480px]">
              <div class="truncate" :title="e.text">{{ truncate(e.text, 140) }}</div>
              <div class="font-mono text-xs text-muted-foreground">{{ e.id }}</div>
              <ExpiryBadge v-if="e.validUntil" :valid-until="e.validUntil" class="mt-1" />
            </TableCell>
            <TableCell class="font-mono">{{ e.namespace || '(empty)' }}</TableCell>
            <TableCell class="text-sm text-muted-foreground whitespace-nowrap">{{ e.deletedAt ? formatTimestamp(e.deletedAt) : '—' }}</TableCell>
            <TableCell class="text-sm text-muted-foreground">{{ e.reason }}</TableCell>
            <TableCell class="text-right whitespace-nowrap">
              <Button variant="outline" size="icon" class="mr-1 size-8" title="Restore" aria-label="Restore memory" :disabled="busy" @click="restore(e)">
                <Undo2 class="size-3.5" />
              </Button>
              <Button variant="outline" size="icon" class="size-8 text-destructive hover:text-destructive" title="Purge permanently" aria-label="Purge memory" :disabled="busy" @click="purge(e)">
                <Trash2 class="size-3.5" />
              </Button>
            </TableCell>
          </TableRow>
        </TableBody>
      </Table>
      <div class="flex items-center justify-between border-t p-2 text-xs text-muted-foreground">
        <span>{{ offset + 1 }}–{{ offset + entries.length }} of {{ total }}</span>
        <span class="flex gap-1">
          <Button size="sm" variant="outline" :disabled="loading || offset === 0" @click="page(-1)">Previous</Button>
          <Button size="sm" variant="outline" :disabled="loading || offset + entries.length >= total" @click="page(1)">Next</Button>
        </span>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { toast } from 'vue-sonner'
import { memoryClient } from '@/utils/connect'
import { confirmDialog } from '@/lib/confirm'
import { useAuthStore } from '@/stores/auth'
import { formatTimestamp, truncate } from '@/utils/text'
import { Loader2, RotateCw, Trash2, Undo2 } from 'lucide-vue-next'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

const router = useRouter()
const auth = useAuthStore()

const PAGE = 50
const entries = ref([])
const total = ref(0)
const offset = ref(0)
const retentionSeconds = ref(0)
const loading = ref(false)
const busy = ref(false)
const error = ref('')
const nsFilter = ref('')
const reasonFilter = ref('')
// Filters actually applied to the loaded page (the inputs may be edited ahead).
const applied = ref({ namespace: '', reason: '' })
const selected = ref(new Set())

const filtered = computed(() => !!(applied.value.namespace || applied.value.reason))
const allShownSelected = computed(() => entries.value.length > 0 && entries.value.every((e) => selected.value.has(e.id)))
const retentionDays = computed(() => Number(retentionSeconds.value) / 86400)
const retentionLabel = computed(() => {
  const d = retentionDays.value
  return d >= 1 ? `${Math.round(d * 10) / 10} days` : `${Math.round(d * 24)} hours`
})

function handleError(e) {
  if (e instanceof ConnectError && e.code === Code.Unauthenticated) {
    auth.logout()
    router.push({ name: 'login' })
    return true
  }
  error.value = e.message || 'Request failed'
  return false
}

function toggle(id) {
  const next = new Set(selected.value)
  if (next.has(id)) next.delete(id)
  else next.add(id)
  selected.value = next
}

function toggleAllShown() {
  const next = new Set(selected.value)
  if (allShownSelected.value) entries.value.forEach((e) => next.delete(e.id))
  else entries.value.forEach((e) => next.add(e.id))
  selected.value = next
}

async function reload() {
  loading.value = true
  error.value = ''
  try {
    const res = await memoryClient.listTrash({
      namespace: applied.value.namespace,
      reason: applied.value.reason,
      limit: PAGE,
      offset: offset.value,
    })
    // A page emptied by restores/purges: step back instead of showing nothing.
    if (res.entries.length === 0 && res.total > 0 && offset.value > 0) {
      offset.value = Math.max(0, offset.value - PAGE)
      loading.value = false
      return reload()
    }
    entries.value = res.entries
    total.value = res.total
    retentionSeconds.value = res.retentionSeconds
    // Drop selections that no longer exist.
    const ids = new Set(res.entries.map((e) => e.id))
    selected.value = new Set([...selected.value].filter((id) => ids.has(id)))
  } catch (e) {
    if (handleError(e)) return
  } finally {
    loading.value = false
  }
}

function applyFilters() {
  applied.value = { namespace: nsFilter.value.trim(), reason: reasonFilter.value }
  offset.value = 0
  selected.value = new Set()
  return reload()
}

function page(dir) {
  offset.value = Math.max(0, offset.value + dir * PAGE)
  return reload()
}

// run wraps a mutation: busy flag, error handling, reload.
async function run(fn) {
  busy.value = true
  error.value = ''
  try {
    await fn()
    await reload()
  } catch (err) {
    handleError(err)
  } finally {
    busy.value = false
  }
}

function restoreToast(res) {
  const failed = Number(res.failed || 0)
  if (failed > 0) toast.warning(`Restored ${Number(res.queued)}; ${failed} failed: ${(res.failures || []).slice(0, 3).join('; ')}`)
  else toast.success(`Restored ${Number(res.queued)} (queued for indexing)`)
  const expired = Number(res.expiredQueued || 0)
  if (expired > 0) toast.warning(`${expired} of them already expired: hidden from search/list until their expiry is changed`)
}

async function restore(e) {
  if (!(await confirmDialog('Restore this memory? It is re-queued for indexing under its original id.', { title: 'Restore memory', actionLabel: 'Restore', destructive: false }))) return
  await run(async () => {
    const res = await memoryClient.restoreFromTrash({ ids: [e.id] })
    toast.success(Number(res.expiredQueued) > 0
      ? 'Memory restored, but already expired (hidden until its expiry is changed)'
      : 'Memory restored (queued for indexing)')
  })
}

async function purge(e) {
  if (!(await confirmDialog('Permanently delete this memory? This cannot be undone.', { title: 'Purge memory', actionLabel: 'Purge' }))) return
  await run(async () => {
    await memoryClient.purgeTrash({ ids: [e.id] })
    toast.success('Memory purged')
  })
}

async function restoreSelected() {
  const ids = [...selected.value]
  if (!(await confirmDialog(`Restore ${ids.length} selected memories?`, { title: 'Restore selected', actionLabel: 'Restore', destructive: false }))) return
  await run(async () => {
    restoreToast(await memoryClient.restoreFromTrash({ ids }))
    selected.value = new Set()
  })
}

async function purgeSelected() {
  const ids = [...selected.value]
  if (!(await confirmDialog(`Permanently delete ${ids.length} selected memories? This cannot be undone.`, { title: 'Purge selected', actionLabel: 'Purge' }))) return
  await run(async () => {
    const res = await memoryClient.purgeTrash({ ids })
    toast.success(`Purged ${Number(res.purged)} memories`)
    selected.value = new Set()
  })
}

async function restoreAllMatching() {
  const what = filtered.value ? `all ${total.value} memories matching the filters` : `all ${total.value} trashed memories`
  if (!(await confirmDialog(`Restore ${what}?`, { title: 'Restore all', actionLabel: 'Restore all', destructive: false }))) return
  await run(async () => {
    restoreToast(await memoryClient.restoreFromTrash({ all: true, namespace: applied.value.namespace, reason: applied.value.reason }))
    selected.value = new Set()
  })
}

async function purgeAll() {
  const what = filtered.value ? `all ${total.value} memories matching the filters` : `all ${total.value} trashed memories`
  const ok = await confirmDialog(`Permanently delete ${what}? This cannot be undone.`, {
    title: 'Purge all',
    actionLabel: 'Purge all',
    typeToConfirm: 'purge',
  })
  if (!ok) return
  await run(async () => {
    const res = await memoryClient.purgeTrash({ all: true, namespace: applied.value.namespace, reason: applied.value.reason })
    toast.success(`Purged ${Number(res.purged)} memories`)
    selected.value = new Set()
  })
}

onMounted(reload)
</script>
