<template>
  <div class="space-y-4">
    <h4 class="flex items-center gap-2 text-lg font-semibold"><KeyRound class="size-5" />API Keys</h4>

    <Alert v-if="error" variant="destructive"><AlertDescription>{{ error }}</AlertDescription></Alert>

    <!-- New key reveal (shown immediately after creation) -->
    <Alert v-if="createdKey" class="border-amber-500/50 text-amber-600 dark:text-amber-400">
      <AlertDescription class="text-amber-600 dark:text-amber-400">
        <strong>Copy your new API key now — it won't be shown again.</strong>
        <div class="mt-2 flex items-center gap-2">
          <code class="flex-1 select-all font-mono text-sm">{{ createdKey }}</code>
          <Button variant="outline" size="sm" @click="copyKey(createdKey)">
            <Copy class="size-4" />Copy
          </Button>
        </div>
        <Button variant="outline" size="sm" class="mt-2" @click="createdKey = ''">Dismiss</Button>
      </AlertDescription>
    </Alert>

    <!-- Create key form -->
    <div class="flex flex-wrap items-end gap-2">
      <div class="grid gap-1.5">
        <Label class="text-xs">Label (optional)</Label>
        <Input
          v-model="newLabel"
          class="h-8"
          placeholder="e.g. laptop, ci"
          :disabled="busy"
          @keyup.enter="createKey"
        />
      </div>
      <Button size="sm" :disabled="busy" @click="createKey">
        <Plus class="size-4" />New key
      </Button>
      <Button variant="outline" size="sm" :disabled="loading" @click="reload">
        <RotateCw class="size-4" />Refresh
      </Button>
    </div>

    <div v-if="loading" class="py-16 text-center text-muted-foreground" role="status" aria-label="Loading API keys">
      <Loader2 class="mx-auto size-8 animate-spin" />
    </div>

    <div v-else-if="keys.length === 0" class="py-16 text-center text-muted-foreground">
      <KeyRound class="mx-auto mb-2 size-8" />
      No API keys yet. Create one above to use with the MCP server or CLI.
    </div>

    <div v-else class="rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Prefix</TableHead>
            <TableHead>Label</TableHead>
            <TableHead>Created</TableHead>
            <TableHead>Last used</TableHead>
            <TableHead class="text-right">Actions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          <TableRow v-for="k in keys" :key="k.id">
            <TableCell class="font-mono">{{ k.prefix }}…</TableCell>
            <TableCell class="text-sm text-muted-foreground">{{ k.label || '—' }}</TableCell>
            <TableCell class="text-sm text-muted-foreground">{{ k.createdAt ? formatTimestamp(k.createdAt) : '—' }}</TableCell>
            <TableCell class="text-sm text-muted-foreground">{{ k.lastUsedAt ? formatTimestamp(k.lastUsedAt) : 'never' }}</TableCell>
            <TableCell class="text-right">
              <Button
                variant="outline"
                size="icon"
                class="size-8 text-destructive hover:text-destructive"
                title="Revoke key"
                aria-label="Revoke key"
                :disabled="busy"
                @click="revokeKey(k)"
              >
                <Trash2 class="size-3.5" />
              </Button>
            </TableCell>
          </TableRow>
        </TableBody>
      </Table>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { toast } from 'vue-sonner'
import { Code, ConnectError } from '@connectrpc/connect'
import { memoryClient } from '@/utils/connect'
import { useAuthStore } from '@/stores/auth'
import { confirmDialog } from '@/lib/confirm'
import { formatTimestamp } from '@/utils/text'
import { Copy, KeyRound, Loader2, Plus, RotateCw, Trash2 } from 'lucide-vue-next'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

const router = useRouter()
const auth = useAuthStore()

const keys = ref([])
const loading = ref(false)
const busy = ref(false)
const error = ref('')
const newLabel = ref('')
const createdKey = ref('')

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
    const res = await memoryClient.listApiKeys({})
    keys.value = res.keys
  } catch (e) {
    if (handleError(e)) return
  } finally {
    loading.value = false
  }
}

async function createKey() {
  busy.value = true
  error.value = ''
  createdKey.value = ''
  try {
    const res = await memoryClient.createApiKey({ label: newLabel.value.trim() })
    createdKey.value = res.rawKey
    toast.success('API key created. Copy it now — it will not be shown again.')
    newLabel.value = ''
    await reload()
  } catch (e) {
    handleError(e)
  } finally {
    busy.value = false
  }
}

async function revokeKey(k) {
  const label = k.label ? `"${k.label}" (${k.prefix}…)` : `${k.prefix}…`
  if (!(await confirmDialog(`Revoke API key ${label}? Any client using it will lose access immediately.`, { actionLabel: 'Revoke' }))) return
  busy.value = true
  error.value = ''
  try {
    await memoryClient.deleteApiKey({ id: k.id })
    toast.success(`Key ${k.prefix}… revoked.`)
    await reload()
  } catch (e) {
    handleError(e)
  } finally {
    busy.value = false
  }
}

async function copyKey(raw) {
  try {
    await navigator.clipboard.writeText(raw)
    toast.success('Key copied to clipboard.')
  } catch {
    error.value = 'Could not copy — please select and copy the key manually.'
  }
}

onMounted(reload)
</script>
