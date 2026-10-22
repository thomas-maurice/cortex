<template>
  <div class="space-y-4">
    <h1 class="flex items-center gap-2 text-xl font-semibold">
      <Copy class="size-5" />Duplicates
      <Badge v-if="!loading" variant="secondary">{{ pairs.length }}</Badge>
    </h1>

    <p class="text-sm text-muted-foreground">
      Pairs the worker flagged as likely duplicates when they were indexed. Review each pair:
      dismiss it, keep one side, or merge them into a new memory. Deleting moves the memory to the
      <router-link :to="{ name: 'trash' }" class="underline">Trash</router-link>, where it can be restored.
    </p>

    <Alert v-if="error" variant="destructive">
      <AlertDescription>{{ error }}</AlertDescription>
    </Alert>

    <div class="flex flex-wrap items-end gap-2">
      <div class="grid gap-1.5" style="width: 180px">
        <Label class="text-xs">Namespace</Label>
        <Input v-model="namespace" placeholder="* = all" @keyup.enter="reload" />
      </div>
      <Button :disabled="loading" @click="reload"><RotateCw class="size-4" />Refresh</Button>
    </div>

    <Alert v-if="truncated">
      <AlertDescription>Showing the first {{ LIMIT }} flagged memories — resolve some and refresh to see more.</AlertDescription>
    </Alert>

    <div v-if="loading" class="py-16 text-center text-muted-foreground" role="status" aria-live="polite" aria-label="Loading duplicates">
      <Loader2 class="mx-auto size-8 animate-spin" />
    </div>

    <div v-else-if="pairs.length === 0" class="py-16 text-center text-muted-foreground">
      <Copy class="mx-auto mb-2 size-8" />
      No duplicate candidates to review.
    </div>

    <template v-else>
    <Card v-for="p in pairs" :key="p.key">
      <CardContent class="space-y-3 py-4">
        <div class="grid gap-4 md:grid-cols-2">
          <div v-for="(m, i) in [p.left, p.right]" :key="m.id" class="min-w-0 space-y-2 rounded-md border p-3">
            <ClampedMarkdown :html="renderMarkdown(m.text)" />
            <div class="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
              <Badge variant="secondary"><Layers class="size-3" />{{ m.namespace }}</Badge>
              <Badge v-for="t in m.tags" :key="t" variant="outline"><Tag class="size-3" />{{ t }}</Badge>
              <span v-if="m.createdAt">{{ formatTimestamp(m.createdAt) }}</span>
              <span class="inline-flex items-center gap-1" title="explicit links"><Link2 class="size-3" />{{ (m.linkedIds || []).length }}</span>
              <span class="font-mono">{{ m.id.slice(0, 8) }}</span>
            </div>
            <Button
              variant="outline"
              size="sm"
              :disabled="busy"
              @click="keep(p, i === 0 ? 'left' : 'right')"
            >
              <Check class="size-4" />Keep {{ i === 0 ? 'left' : 'right' }}
            </Button>
          </div>
        </div>
        <div class="flex flex-wrap gap-2">
          <Button variant="outline" size="sm" :disabled="busy" @click="dismiss(p)"><X class="size-4" />Not duplicates</Button>
          <Button size="sm" :disabled="busy" @click="openMerge(p)"><Merge class="size-4" />Merge…</Button>
        </div>
      </CardContent>
    </Card>
    </template>

    <Dialog v-model:open="mergeOpen">
      <DialogContent class="sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>Merge into a new memory</DialogTitle>
          <DialogDescription>
            Edit the merged text, then save. Once the new memory is indexed, <strong>both originals are
            moved to the Trash</strong> (restorable until the retention period ends).
          </DialogDescription>
        </DialogHeader>
        <div v-if="draft" class="space-y-2">
          <p v-if="crossNamespace" class="text-sm text-amber-600 dark:text-amber-400">
            The two memories are in different namespaces ({{ mergePair.left.namespace }} / {{ mergePair.right.namespace }}).
            The merged memory is saved into the namespace set below; both originals are still replaced.
          </p>
          <Textarea v-model="draft.text" rows="12" placeholder="Merged memory text…" />
          <div class="flex flex-wrap gap-2">
            <Input v-model="draft.namespace" class="flex-1" placeholder="namespace" />
            <Input v-model="draft.tags" class="flex-1" placeholder="tags, comma separated" />
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" :disabled="saving" @click="mergeOpen = false">Cancel</Button>
          <Button :disabled="!draft || !draft.text.trim() || saving" @click="saveMerge">Save merged memory</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { toast } from 'vue-sonner'
import { memoryClient } from '@/utils/connect'
import { renderMarkdown } from '@/utils/markdown'
import { confirmDialog } from '@/lib/confirm'
import { useAuthStore } from '@/stores/auth'
import { formatTimestamp } from '@/utils/text'
import { buildPairs, mergeDraft } from '@/utils/duplicates'
import ClampedMarkdown from '@/components/ClampedMarkdown.vue'
import { Check, Copy, Layers, Link2, Loader2, Merge, RotateCw, Tag, X } from 'lucide-vue-next'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Textarea } from '@/components/ui/textarea'

const router = useRouter()
const auth = useAuthStore()

const namespace = ref('*')
const LIMIT = 200
const pairs = ref([])
const truncated = ref(false)
const loading = ref(false)
const busy = ref(false)
const saving = ref(false)
const error = ref('')

const mergeOpen = ref(false)
const draft = ref(null)
const mergePair = ref(null)
const crossNamespace = computed(() => mergePair.value && mergePair.value.left.namespace !== mergePair.value.right.namespace)

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
    const res = await memoryClient.listDuplicateCandidates({ namespace: namespace.value, limit: LIMIT })
    truncated.value = res.groups.length >= LIMIT
    pairs.value = buildPairs(res.groups)
  } catch (e) {
    handleError(e)
  } finally {
    loading.value = false
  }
}

// Drop the acted-on pair plus every other pair touching a removed id (a memory
// can be flagged against several others; once it is gone those pairs are moot).
function drop(p, removedIds = []) {
  pairs.value = pairs.value.filter(
    (x) => x.key !== p.key && !removedIds.includes(x.left.id) && !removedIds.includes(x.right.id),
  )
}

async function dismiss(p) {
  busy.value = true
  error.value = ''
  try {
    await memoryClient.dismissDuplicate({ id: p.left.id, targetId: p.right.id })
    drop(p)
    toast.success('Marked as not duplicates')
  } catch (e) {
    handleError(e)
  } finally {
    busy.value = false
  }
}

async function keep(p, side) {
  const gone = side === 'left' ? p.right : p.left
  busy.value = true // disable all actions while the confirm dialog is open
  const ok = await confirmDialog(
    `Keep the ${side} memory and delete the other one? The deleted memory is moved to the Trash and can be restored from there.`,
    { title: 'Resolve duplicate', actionLabel: 'Delete the other' },
  )
  if (!ok) { busy.value = false; return }
  error.value = ''
  try {
    await memoryClient.delete({ id: gone.id })
    drop(p, [gone.id])
    toast.success('Moved to trash', {
      action: {
        label: 'Undo',
        onClick: async () => {
          try {
            await memoryClient.restoreFromTrash({ ids: [gone.id] })
            toast.success('Restored (queued for indexing)')
            await reload()
          } catch (e) {
            handleError(e)
          }
        },
      },
    })
  } catch (e) {
    handleError(e)
  } finally {
    busy.value = false
  }
}

function openMerge(p) {
  mergePair.value = p
  const d = mergeDraft(p)
  draft.value = { text: d.text, namespace: d.namespace, tags: d.tags.join(', '), supersedes: d.supersedes }
  mergeOpen.value = true
}

async function saveMerge() {
  saving.value = true
  error.value = ''
  try {
    const tags = draft.value.tags.split(',').map((t) => t.trim()).filter(Boolean)
    await memoryClient.save({
      text: draft.value.text,
      namespace: draft.value.namespace,
      tags,
      supersedes: draft.value.supersedes,
    })
    mergeOpen.value = false
    // The originals stay live until the worker indexes the merge, so a reload
    // would still list the pair — hide it locally instead.
    drop(mergePair.value, draft.value.supersedes)
    toast.success('Merged memory queued — both originals move to the trash once it is indexed.')
  } catch (e) {
    handleError(e)
  } finally {
    saving.value = false
  }
}

onMounted(reload)
</script>
