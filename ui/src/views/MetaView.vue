<template>
  <div class="space-y-4">
    <h2 class="flex items-center gap-2 text-xl font-semibold tracking-tight">
      <FileJson class="size-5" />Metadata
    </h2>
    <p class="text-sm text-muted-foreground">
      Read-only debug view. Durable facts live in Weaviate and survive a NATS wipe; coordination state lives in NATS and is
      disposable.
    </p>

    <Alert v-if="error" variant="destructive">
      <AlertDescription>{{ error }}</AlertDescription>
    </Alert>

    <div class="flex items-center gap-2">
      <Button size="sm" :disabled="loading" @click="reload()">
        <RotateCw class="size-4" />Refresh
      </Button>
    </div>

    <template v-if="info">
      <h3 class="font-medium">Durable (Weaviate CortexMeta)</h3>
      <div class="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Key</TableHead>
              <TableHead>Value</TableHead>
              <TableHead>Updated</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            <TableRow v-for="e in info.durable" :key="e.key">
              <TableCell class="font-medium">{{ e.key }}</TableCell>
              <TableCell>
                <details>
                  <summary class="cursor-pointer truncate text-muted-foreground">{{ preview(e.value) }}</summary>
                  <pre class="mt-1 whitespace-pre-wrap text-xs">{{ pretty(e.value) }}</pre>
                </details>
              </TableCell>
              <TableCell class="text-muted-foreground">{{ when(e.updatedAt) }}</TableCell>
            </TableRow>
            <TableRow v-if="!info.durable.length">
              <TableCell colspan="3" class="text-muted-foreground">No entries.</TableCell>
            </TableRow>
          </TableBody>
        </Table>
      </div>

      <h3 class="font-medium">Coordination (NATS KV)</h3>
      <div class="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Key</TableHead>
              <TableHead>Value</TableHead>
              <TableHead>Revision</TableHead>
              <TableHead>Created</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            <TableRow v-for="e in info.coordination" :key="e.key">
              <TableCell class="font-medium">{{ e.key }}</TableCell>
              <TableCell>
                <details>
                  <summary class="cursor-pointer truncate text-muted-foreground">{{ preview(e.value) }}</summary>
                  <pre class="mt-1 whitespace-pre-wrap text-xs">{{ pretty(e.value) }}</pre>
                </details>
              </TableCell>
              <TableCell>{{ Number(e.revision) }}</TableCell>
              <TableCell class="text-muted-foreground">{{ when(e.createdAt) }}</TableCell>
            </TableRow>
            <TableRow v-if="!info.coordination.length">
              <TableCell colspan="4" class="text-muted-foreground">No keys.</TableCell>
            </TableRow>
          </TableBody>
        </Table>
      </div>
    </template>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { timestampDate } from '@bufbuild/protobuf/wkt'
import { memoryClient } from '@/utils/connect'
import { useAuthStore } from '@/stores/auth'
import { FileJson, RotateCw } from 'lucide-vue-next'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

const router = useRouter()
const auth = useAuthStore()

const info = ref(null)
const loading = ref(false)
const error = ref('')

// Values are JSON payloads (or plain strings such as a lock owner): pretty-print
// when they parse, show verbatim otherwise.
function pretty(v) {
  try {
    return JSON.stringify(JSON.parse(v), null, 2)
  } catch {
    return v
  }
}

function preview(v) {
  const s = v.replace(/\s+/g, ' ')
  return s.length > 80 ? s.slice(0, 80) + '…' : s
}

function when(ts) {
  return ts ? timestampDate(ts).toLocaleString() : '—'
}

async function reload() {
  loading.value = true
  try {
    info.value = await memoryClient.listMeta({})
    error.value = ''
  } catch (e) {
    if (e instanceof ConnectError && e.code === Code.Unauthenticated) {
      auth.logout()
      router.push({ name: 'login' })
      return
    }
    error.value = e.message || 'Request failed'
  } finally {
    loading.value = false
  }
}

onMounted(reload)
</script>
