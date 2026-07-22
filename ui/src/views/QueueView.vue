<template>
  <div class="space-y-4">
    <h2 class="flex items-center gap-2 text-xl font-semibold tracking-tight">
      <ListChecks class="size-5" />Indexing
    </h2>

    <Alert v-if="error" variant="destructive">
      <AlertDescription>{{ error }}</AlertDescription>
    </Alert>

    <div class="grid gap-2 md:grid-cols-3">
      <Card class="text-center">
        <CardContent class="py-4">
          <div class="text-2xl font-semibold">{{ counts.pending }}</div>
          <div class="text-sm text-muted-foreground">Pending</div>
        </CardContent>
      </Card>
      <Card class="text-center">
        <CardContent class="py-4">
          <div class="text-2xl font-semibold">{{ counts.inFlight }}</div>
          <div class="text-sm text-muted-foreground">In flight</div>
        </CardContent>
      </Card>
      <Card class="text-center" :class="{ 'border-destructive': counts.dead > 0 }">
        <CardContent class="py-4">
          <div class="text-2xl font-semibold" :class="{ 'text-destructive': counts.dead > 0 }">{{ counts.dead }}</div>
          <div class="text-sm text-muted-foreground">Dead-lettered</div>
        </CardContent>
      </Card>
    </div>

    <div class="flex items-center gap-2">
      <Button size="sm" :disabled="loading" @click="reload()">
        <RotateCw class="size-4" />Refresh
      </Button>
      <Button variant="outline" size="sm" class="text-amber-600 hover:text-amber-600 dark:text-amber-400" :disabled="busy || counts.dead === 0" @click="requeue">
        <RotateCcw class="size-4" />Requeue all
      </Button>
      <Button variant="outline" size="sm" class="text-destructive hover:text-destructive" :disabled="busy || counts.dead === 0" @click="purge">
        <Trash2 class="size-4" />Purge all
      </Button>
      <Badge v-if="!consumerPresent" variant="secondary">worker offline</Badge>
    </div>

    <div class="space-y-2">
      <h3 class="text-sm font-medium text-muted-foreground">Failed memories</h3>
      <div v-if="dead.length === 0" class="py-8 text-center text-muted-foreground">
        <CircleCheck class="mx-auto mb-2 size-8 text-emerald-600 dark:text-emerald-400" />
        No dead-lettered memories.
      </div>

      <div v-else class="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Memory</TableHead>
              <TableHead>Error</TableHead>
              <TableHead>Namespace</TableHead>
              <TableHead>Attempts</TableHead>
              <TableHead>Failed</TableHead>
              <TableHead>ID</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            <TableRow v-for="(dl, i) in dead" :key="i">
              <TableCell class="max-w-sm whitespace-pre-wrap">{{ dl.record?.text }}</TableCell>
              <TableCell class="whitespace-normal text-destructive">
                <span class="inline-flex items-center gap-1"><TriangleAlert class="size-3.5" />{{ dl.error }}</span>
              </TableCell>
              <TableCell>
                <Badge v-if="dl.record?.namespace" variant="secondary">{{ dl.record.namespace }}</Badge>
              </TableCell>
              <TableCell>{{ dl.deliveries }}</TableCell>
              <TableCell>{{ dl.failedAt ? formatDate(dl.failedAt) : '' }}</TableCell>
              <TableCell class="text-muted-foreground">{{ dl.record?.id }}</TableCell>
            </TableRow>
          </TableBody>
        </Table>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, onUnmounted } from 'vue'
import { useRouter } from 'vue-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { memoryClient } from '@/utils/connect'
import { DeadAction } from '@/gen/cortex/v1/cortex_pb'
import { useAuthStore } from '@/stores/auth'
import { CircleCheck, ListChecks, RotateCcw, RotateCw, Trash2, TriangleAlert } from 'lucide-vue-next'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

const router = useRouter()
const auth = useAuthStore()

// int64 fields arrive as bigint; coerce to number for display and comparison.
const counts = reactive({ pending: 0, inFlight: 0, dead: 0 })
const consumerPresent = ref(true)
const dead = ref([])
const loading = ref(false)
const busy = ref(false)
const error = ref('')

// Indexing is fast and bursty (and usually driven in the background by the agent
// or a bulk import), so a one-shot snapshot almost always reads 0/0/0 and looks
// like "nothing is ever indexed". Poll while the view is mounted so live queue
// activity actually shows. Background polls skip the spinner and don't stack.
const POLL_MS = 1000
let pollTimer = null
let polling = false

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
    return true
  }
  error.value = e.message || 'Request failed'
  return false
}

async function reload(background = false) {
  // A slow poll must not stack on top of the previous one.
  if (background && polling) return
  polling = true
  if (!background) loading.value = true
  try {
    const q = await memoryClient.indexQueue({})
    counts.pending = Number(q.pending)
    counts.inFlight = Number(q.inFlight)
    counts.dead = Number(q.dead)
    consumerPresent.value = q.consumerPresent
    const d = await memoryClient.dead({ action: DeadAction.LIST })
    dead.value = d.deadLetters
    // A successful poll clears a previous transient error.
    error.value = ''
  } catch (e) {
    if (handleError(e)) {
      stopPolling()
      return
    }
  } finally {
    polling = false
    if (!background) loading.value = false
  }
}

function stopPolling() {
  if (pollTimer) {
    clearInterval(pollTimer)
    pollTimer = null
  }
}

async function requeue() {
  if (!confirm('Requeue all dead-lettered memories for indexing?')) return
  busy.value = true
  try {
    await memoryClient.dead({ action: DeadAction.REQUEUE })
    await reload()
  } catch (e) {
    handleError(e)
  } finally {
    busy.value = false
  }
}

async function purge() {
  if (!confirm('Permanently purge all dead-lettered memories? This cannot be undone.')) return
  busy.value = true
  try {
    await memoryClient.dead({ action: DeadAction.PURGE })
    await reload()
  } catch (e) {
    handleError(e)
  } finally {
    busy.value = false
  }
}

onMounted(() => {
  reload()
  pollTimer = setInterval(() => reload(true), POLL_MS)
})

onUnmounted(stopPolling)
</script>
