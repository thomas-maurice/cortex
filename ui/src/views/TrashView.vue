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

    <div class="flex items-center gap-2">
      <Button size="sm" :disabled="loading" @click="reload">
        <RotateCw class="size-4" />Refresh
      </Button>
      <Button
        size="sm"
        variant="outline"
        class="text-destructive hover:text-destructive"
        :disabled="busy || total === 0"
        @click="purgeAll"
      >
        <Trash2 class="size-4" />Purge all
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
            <TableHead>Memory</TableHead>
            <TableHead>Namespace</TableHead>
            <TableHead>Deleted</TableHead>
            <TableHead>Reason</TableHead>
            <TableHead class="text-right" style="width: 1%">Actions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          <TableRow v-for="e in entries" :key="e.id">
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
      <p v-if="total > entries.length" class="border-t p-2 text-center text-xs text-muted-foreground">
        Showing the {{ entries.length }} most recently deleted of {{ total }}.
      </p>
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
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

const router = useRouter()
const auth = useAuthStore()

const entries = ref([])
const total = ref(0)
const retentionSeconds = ref(0)
const loading = ref(false)
const busy = ref(false)
const error = ref('')

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

async function reload() {
  loading.value = true
  error.value = ''
  try {
    const res = await memoryClient.listTrash({ limit: 200 })
    entries.value = res.entries
    total.value = res.total
    retentionSeconds.value = res.retentionSeconds
  } catch (e) {
    if (handleError(e)) return
  } finally {
    loading.value = false
  }
}

async function restore(e) {
  if (!(await confirmDialog('Restore this memory? It is re-queued for indexing under its original id.', { title: 'Restore memory', actionLabel: 'Restore', destructive: false }))) return
  busy.value = true
  error.value = ''
  try {
    const res = await memoryClient.restoreFromTrash({ ids: [e.id] })
    toast.success(Number(res.expiredQueued) > 0
      ? 'Memory restored, but already expired (hidden until its expiry is changed)'
      : 'Memory restored (queued for indexing)')
    await reload()
  } catch (err) {
    handleError(err)
  } finally {
    busy.value = false
  }
}

async function purge(e) {
  if (!(await confirmDialog('Permanently delete this memory? This cannot be undone.', { title: 'Purge memory', actionLabel: 'Purge' }))) return
  busy.value = true
  error.value = ''
  try {
    await memoryClient.purgeTrash({ ids: [e.id] })
    toast.success('Memory purged')
    await reload()
  } catch (err) {
    handleError(err)
  } finally {
    busy.value = false
  }
}

async function purgeAll() {
  const ok = await confirmDialog(
    `Permanently delete all ${total.value} trashed memories? This cannot be undone.`,
    { title: 'Purge all', actionLabel: 'Purge all', typeToConfirm: 'purge' },
  )
  if (!ok) return
  busy.value = true
  error.value = ''
  try {
    const res = await memoryClient.purgeTrash({ all: true })
    toast.success(`Purged ${Number(res.purged)} memories`)
    await reload()
  } catch (err) {
    handleError(err)
  } finally {
    busy.value = false
  }
}

onMounted(reload)
</script>
