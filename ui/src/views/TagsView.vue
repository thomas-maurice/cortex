<template>
  <div class="space-y-4">
    <h1 class="flex items-center gap-2 text-xl font-semibold"><Tag class="size-5" />Tags</h1>

    <Alert v-if="error" variant="destructive">
      <AlertDescription>{{ error }}</AlertDescription>
    </Alert>

    <div class="flex flex-wrap items-center gap-2">
      <Input v-model="filter" class="h-8 max-w-[260px]" placeholder="Filter tags…" aria-label="Filter tags" />
      <Input v-model="namespace" class="h-8 max-w-[200px]" placeholder="namespace (all)" aria-label="Namespace" @keyup.enter="reload" />
      <Button size="sm" :disabled="loading" @click="reload">
        <RotateCw class="size-4" />Refresh
      </Button>
    </div>

    <div v-if="loading" class="py-16 text-center text-muted-foreground" role="status" aria-live="polite" aria-label="Loading tags">
      <Loader2 class="mx-auto size-8 animate-spin" />
    </div>

    <div v-else-if="visible.length === 0" class="py-16 text-center text-muted-foreground">
      <Tag class="mx-auto mb-2 size-8" />
      {{ tags.length === 0 ? 'No tags yet.' : 'No tags match the filter.' }}
    </div>

    <div v-else class="rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Tag</TableHead>
            <TableHead class="text-right">Memories</TableHead>
            <TableHead>Last activity</TableHead>
            <TableHead class="text-right" style="width: 1%">Actions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          <TableRow v-for="t in visible" :key="t.name">
            <TableCell>
              <template v-if="renameOf === t.name">
                <Input
                  v-model="renameTo"
                  class="h-8 max-w-[360px]"
                  :disabled="busy"
                  placeholder="new tag…"
                  @keyup.enter="confirmRename(t)"
                  @keyup.esc="cancelRename"
                />
                <p v-if="targetExists" class="mt-1 text-xs text-muted-foreground">
                  "{{ renameTo.trim() }}" already exists: memories will be merged into it.
                </p>
              </template>
              <button v-else class="font-mono hover:underline" title="Show memories with this tag" @click="openMemories(t)">{{ t.name }}</button>
            </TableCell>
            <TableCell class="text-right">{{ Number(t.memoryCount) }}</TableCell>
            <TableCell class="text-sm text-muted-foreground">{{ t.lastActivity ? formatTimestamp(t.lastActivity) : '—' }}</TableCell>
            <TableCell class="text-right whitespace-nowrap">
              <template v-if="renameOf === t.name">
                <Button size="sm" class="mr-1" :disabled="busy || !renameTo.trim() || renameTo.trim() === t.name" @click="confirmRename(t)">
                  {{ targetExists ? 'Merge' : 'Save' }}
                </Button>
                <Button size="sm" variant="outline" :disabled="busy" @click="cancelRename">Cancel</Button>
              </template>
              <template v-else>
                <Button variant="outline" size="icon" class="mr-1 size-8" title="Rename / merge" aria-label="Rename" :disabled="busy" @click="startRename(t)">
                  <Pencil class="size-3.5" />
                </Button>
                <Button variant="outline" size="icon" class="size-8 text-destructive hover:text-destructive" title="Delete tag" aria-label="Delete tag" :disabled="busy" @click="remove(t)">
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
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { toast } from 'vue-sonner'
import { memoryClient } from '@/utils/connect'
import { confirmDialog } from '@/lib/confirm'
import { useAuthStore } from '@/stores/auth'
import { formatTimestamp } from '@/utils/text'
import { Loader2, Pencil, RotateCw, Tag, Trash2 } from 'lucide-vue-next'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

const router = useRouter()
const auth = useAuthStore()

const tags = ref([])
const loading = ref(false)
const busy = ref(false)
const error = ref('')
const filter = ref('')
const namespace = ref('')

const renameOf = ref(null)
const renameTo = ref('')

// The reserved tag behind the cortex-prefs session hook: the server refuses to
// touch it without confirm_preference, so ask explicitly.
const PREFERENCE = 'preference'

const visible = computed(() => {
  const q = filter.value.trim().toLowerCase()
  return q ? tags.value.filter((t) => t.name.toLowerCase().includes(q)) : tags.value
})

const targetExists = computed(() => {
  const to = renameTo.value.trim()
  return !!to && to !== renameOf.value && tags.value.some((t) => t.name === to)
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

function openMemories(t) {
  const query = { tag: t.name }
  if (namespace.value.trim()) query.namespace = namespace.value.trim()
  router.push({ name: 'memories', query })
}

function startRename(t) {
  error.value = ''
  renameOf.value = t.name
  renameTo.value = t.name
}

function cancelRename() {
  renameOf.value = null
  renameTo.value = ''
}

async function confirmPreference(verb) {
  return confirmDialog(
    `The "${PREFERENCE}" tag drives the standing-preferences session hook. ${verb} it changes what is injected into every session.`,
    { title: 'Reserved tag', actionLabel: 'I understand', destructive: true },
  )
}

async function confirmRename(t) {
  const to = renameTo.value.trim()
  if (!to || to === t.name) return
  const touchesPref = t.name.toLowerCase() === PREFERENCE || to.toLowerCase() === PREFERENCE
  if (touchesPref && !(await confirmPreference('Renaming'))) return
  busy.value = true
  error.value = ''
  try {
    const res = await memoryClient.renameTag({
      from: t.name,
      to,
      namespace: namespace.value.trim(),
      confirmPreference: touchesPref,
    })
    toast.success(`Renamed "${t.name}" → "${to}" on ${Number(res.memoriesUpdated)} memories.`)
    cancelRename()
    await reload()
  } catch (e) {
    handleError(e)
  } finally {
    busy.value = false
  }
}

async function remove(t) {
  const isPref = t.name.toLowerCase() === PREFERENCE
  const ok = await confirmDialog(
    `Remove the tag "${t.name}" from ${Number(t.memoryCount)} memories? The memories themselves are kept.` +
      (isPref ? ` "${PREFERENCE}" drives the standing-preferences session hook.` : ''),
    { title: 'Delete tag', actionLabel: 'Delete', destructive: true },
  )
  if (!ok) return
  busy.value = true
  error.value = ''
  try {
    const res = await memoryClient.deleteTag({ tag: t.name, namespace: namespace.value.trim(), confirmPreference: isPref })
    toast.success(`Removed "${t.name}" from ${Number(res.memoriesUpdated)} memories.`)
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
    const res = await memoryClient.listTags({ namespace: namespace.value.trim() })
    tags.value = res.tags
  } catch (e) {
    if (handleError(e)) return
  } finally {
    loading.value = false
  }
}

onMounted(reload)
</script>
