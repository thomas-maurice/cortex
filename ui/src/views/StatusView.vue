<template>
  <div class="space-y-4">
    <h2 class="flex items-center gap-2 text-xl font-semibold tracking-tight">
      <Server class="size-5" />Server status
    </h2>

    <Alert v-if="error" variant="destructive">
      <AlertDescription>{{ error }}</AlertDescription>
    </Alert>

    <div v-if="loading" class="py-16 text-center text-muted-foreground" role="status" aria-live="polite" aria-label="Loading status">
      <Loader2 class="mx-auto size-8 animate-spin" />
    </div>

    <div v-else-if="status" class="rounded-md border">
      <Table>
        <TableBody>
          <TableRow><TableHead>NATS</TableHead><TableCell><health :ok="status.natsOk" /></TableCell></TableRow>
          <TableRow><TableHead>Weaviate</TableHead><TableCell><health :ok="status.weaviateOk" /></TableCell></TableRow>
          <TableRow><TableHead>Ollama</TableHead><TableCell><health :ok="status.ollamaOk" /></TableCell></TableRow>
          <TableRow>
            <TableHead>Model</TableHead>
            <TableCell>
              {{ status.model }}
              <Badge v-if="status.ollamaOk && !status.modelPresent" variant="secondary" class="ml-2 text-amber-600 dark:text-amber-400">not downloaded</Badge>
            </TableCell>
          </TableRow>
          <TableRow><TableHead>Dimensions</TableHead><TableCell>{{ status.dims }}</TableCell></TableRow>
          <TableRow><TableHead>Memories</TableHead><TableCell>{{ status.memoryCount }}</TableCell></TableRow>
          <TableRow><TableHead>Version</TableHead><TableCell>{{ status.version }}</TableCell></TableRow>
        </TableBody>
      </Table>
    </div>

    <Alert v-if="status && status.ollamaOk && !status.modelPresent" class="border-amber-500/50">
      <AlertDescription class="space-y-2 text-amber-600 dark:text-amber-400">
        <p>
          The embedding model <code>{{ status.model }}</code> is not downloaded in Ollama.
          Nothing can be embedded or searched until it is pulled.
        </p>
        <p v-if="pullError" class="text-sm text-destructive">{{ pullError }}</p>
        <Button size="sm" variant="outline" class="border-amber-500/50 text-amber-600 hover:text-amber-600 dark:text-amber-400" :disabled="pulling || loading" @click="pullModel">
          <Loader2 v-if="pulling" class="size-4 animate-spin" />
          <Download v-else class="size-4" />
          {{ pulling ? 'Pulling…' : 'Pull model' }}
        </Button>
      </AlertDescription>
    </Alert>

    <Button size="sm" :disabled="loading || pulling" @click="reload">
      <RotateCw class="size-4" />Refresh
    </Button>
  </div>
</template>

<script setup>
import { ref, onMounted, h } from 'vue'
import { useRouter } from 'vue-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { memoryClient } from '@/utils/connect'
import { useAuthStore } from '@/stores/auth'
import { CircleCheck, CircleX, Download, Loader2, RotateCw, Server } from 'lucide-vue-next'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Table, TableBody, TableCell, TableHead, TableRow } from '@/components/ui/table'

const router = useRouter()
const auth = useAuthStore()

const status = ref(null)
const loading = ref(false)
const error = ref('')
const pulling = ref(false)
const pullError = ref('')

// Tiny inline component: green check / red cross for a boolean health flag.
const health = (props) =>
  h(
    'span',
    { class: 'inline-flex items-center gap-1 ' + (props.ok ? 'text-emerald-600 dark:text-emerald-400' : 'text-destructive') },
    [h(props.ok ? CircleCheck : CircleX, { class: 'size-4' }), props.ok ? 'ok' : 'down']
  )
health.props = ['ok']

async function reload() {
  loading.value = true
  error.value = ''
  try {
    status.value = await memoryClient.status({})
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

async function pullModel() {
  pulling.value = true
  pullError.value = ''
  try {
    await memoryClient.pullModel({})
    await reload()
  } catch (e) {
    if (e instanceof ConnectError && e.code === Code.Unauthenticated) {
      auth.logout()
      router.push({ name: 'login' })
      return
    }
    pullError.value = e.message || 'Pull failed'
  } finally {
    pulling.value = false
  }
}

onMounted(reload)
</script>
