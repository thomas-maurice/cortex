<template>
  <div class="space-y-4">
    <h1 class="flex items-center gap-2 text-xl font-semibold"><Layers class="size-5" />Namespaces</h1>

    <Alert v-if="error" variant="destructive">
      <AlertDescription>{{ error }}</AlertDescription>
    </Alert>

    <div class="flex items-center gap-2">
      <Button size="sm" :disabled="loading" @click="reload">
        <RotateCw class="size-4" />Refresh
      </Button>
    </div>

    <div v-if="loading" class="py-16 text-center text-muted-foreground" role="status" aria-live="polite" aria-label="Loading namespaces">
      <Loader2 class="mx-auto size-8 animate-spin" />
    </div>

    <div v-else-if="namespaces.length === 0" class="py-16 text-center text-muted-foreground">
      <Layers class="mx-auto mb-2 size-8" />
      No namespaces yet.
    </div>

    <div v-else class="rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Namespace</TableHead>
            <TableHead class="text-right">Memories</TableHead>
            <TableHead class="text-right">Summaries</TableHead>
            <TableHead>Last activity</TableHead>
            <TableHead class="text-right" style="width: 1%">Actions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          <TableRow v-for="ns in namespaces" :key="ns.name">
            <TableCell>
              <Input
                v-if="renameOf === ns.name"
                v-model="renameTo"
                class="h-8 max-w-[360px]"
                :disabled="busy"
                placeholder="new namespace…"
                @keyup.enter="confirmRename(ns)"
                @keyup.esc="cancelRename"
              />
              <span v-else class="font-mono">{{ ns.name || '(empty)' }}</span>
            </TableCell>
            <TableCell class="text-right">{{ Number(ns.memoryCount) }}</TableCell>
            <TableCell class="text-right">{{ Number(ns.summaryCount) }}</TableCell>
            <TableCell class="text-sm text-muted-foreground">{{ ns.lastUpdated ? formatTimestamp(ns.lastUpdated) : '—' }}</TableCell>
            <TableCell class="text-right whitespace-nowrap">
              <template v-if="renameOf === ns.name">
                <Button size="sm" class="mr-1" :disabled="busy || !renameTo.trim() || renameTo.trim() === ns.name" @click="confirmRename(ns)">
                  Save
                </Button>
                <Button size="sm" variant="outline" :disabled="busy" @click="cancelRename">Cancel</Button>
              </template>
              <template v-else>
                <Button variant="outline" size="icon" class="mr-1 size-8" title="Rename" aria-label="Rename" :disabled="busy" @click="startRename(ns)">
                  <Pencil class="size-3.5" />
                </Button>
                <Button variant="outline" size="icon" class="size-8 text-destructive hover:text-destructive" title="Delete namespace" aria-label="Delete namespace" :disabled="busy" @click="remove(ns)">
                  <Trash2 class="size-3.5" />
                </Button>
              </template>
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
import { Code, ConnectError } from '@connectrpc/connect'
import { toast } from 'vue-sonner'
import { memoryClient } from '@/utils/connect'
import { confirmDialog } from '@/lib/confirm'
import { useAuthStore } from '@/stores/auth'
import { formatTimestamp } from '@/utils/text'
import { Layers, Loader2, Pencil, RotateCw, Trash2 } from 'lucide-vue-next'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

const router = useRouter()
const auth = useAuthStore()

const namespaces = ref([])
const loading = ref(false)
const busy = ref(false)
const error = ref('')

const renameOf = ref(null)
const renameTo = ref('')

function handleError(e) {
  if (e instanceof ConnectError && e.code === Code.Unauthenticated) {
    auth.logout()
    router.push({ name: 'login' })
    return true
  }
  error.value = e.message || 'Request failed'
  return false
}

function startRename(ns) {
  error.value = ''
  renameOf.value = ns.name
  renameTo.value = ns.name
}

function cancelRename() {
  renameOf.value = null
  renameTo.value = ''
}

async function confirmRename(ns) {
  const to = renameTo.value.trim()
  if (!to || to === ns.name) return
  busy.value = true
  error.value = ''
  try {
    const res = await memoryClient.renameNamespace({ from: ns.name, to })
    toast.success(`Renamed "${ns.name}" → "${to}" (${Number(res.memoriesUpdated)} memories, ${Number(res.summariesUpdated)} summaries).`)
    cancelRename()
    await reload()
  } catch (e) {
    handleError(e)
  } finally {
    busy.value = false
  }
}

async function remove(ns) {
  // Bulk irreversible delete.
  const ok = await confirmDialog(
    `Delete the entire "${ns.name}" namespace? This permanently removes ` +
      `${Number(ns.memoryCount)} memories and ${Number(ns.summaryCount)} summaries and cannot be undone.`,
    { title: 'Delete namespace', actionLabel: 'Delete', destructive: true, typeToConfirm: ns.name },
  )
  if (!ok) return
  busy.value = true
  error.value = ''
  try {
    const res = await memoryClient.deleteNamespace({ namespace: ns.name })
    toast.success(`Deleted "${ns.name}" (${Number(res.memoriesDeleted)} memories, ${Number(res.summariesDeleted)} summaries).`)
    await reload()
  } catch (e) {
    handleError(e)
  } finally {
    busy.value = false
  }
}

async function reload() {
  loading.value = true
  error.value = ''
  try {
    const res = await memoryClient.listNamespaces({})
    namespaces.value = res.namespaces
  } catch (e) {
    if (handleError(e)) return
  } finally {
    loading.value = false
  }
}

onMounted(reload)
</script>
