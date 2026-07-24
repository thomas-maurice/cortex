<template>
  <Sheet :open="!!inspectorMemory" @update:open="(v) => !v && closeInspector()">
    <SheetContent side="right" class="w-full gap-0 overflow-y-auto sm:max-w-xl">
      <SheetHeader class="pb-2">
        <SheetTitle class="flex flex-wrap items-center gap-2 pr-6 text-base">
          <Badge variant="secondary"><Layers class="size-3" />{{ m?.namespace }}</Badge>
          <Badge v-for="t in m?.tags || []" :key="t" variant="outline"><Tag class="size-3" />{{ t }}</Badge>
          <Button v-if="inspectorFindSimilar" variant="outline" size="sm" class="ml-auto" @click="findSimilar">
            <Search class="size-4" />Find similar
          </Button>
        </SheetTitle>
        <SheetDescription class="sr-only">Memory details</SheetDescription>
      </SheetHeader>

      <div v-if="m" class="space-y-4 px-4 pb-6">
        <!-- Edit mode -->
        <div v-if="editing" class="space-y-2">
          <Textarea v-model="editText" rows="10" placeholder="Memory text (Markdown)…" />
          <div class="flex flex-wrap gap-2">
            <Input v-model="editNamespace" class="flex-1" placeholder="namespace" />
            <Input v-model="editTags" class="flex-1" placeholder="tags, comma separated" />
          </div>
          <div class="flex items-center gap-2">
            <Button size="sm" :disabled="!editText.trim() || saving" @click="saveEdit">Save</Button>
            <Button size="sm" variant="outline" :disabled="saving" @click="editing = false">Cancel</Button>
          </div>
        </div>

        <!-- Read mode -->
        <template v-else>
          <div class="markdown-body text-sm" v-html="renderMarkdown(m.text)"></div>

          <div class="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
            <span v-if="m.source">src: {{ m.source }}</span>
            <span v-if="m.conversationId" class="inline-flex items-center gap-1 font-mono">
              <MessagesSquare class="size-3" />{{ m.conversationId }}
            </span>
            <span v-if="m.createdAt">{{ fmtDate(m.createdAt) }}</span>
            <Badge v-if="m.accessCount" variant="secondary" class="text-amber-600 dark:text-amber-400" title="times the agent recalled this memory">
              <Flame class="size-3" />{{ m.accessCount }}
            </Badge>
            <span v-if="m.lastAccessedAt" class="inline-flex items-center gap-1" title="when this memory was last recalled">
              <History class="size-3" />{{ fmtDate(m.lastAccessedAt) }}
            </span>
          </div>

          <div v-if="(m.linkedIds || []).length" class="space-y-1">
            <p class="text-xs font-medium text-muted-foreground">Linked memories</p>
            <div class="flex flex-wrap gap-1">
              <!-- No get-by-id RPC: follow the link when the host view has the
                   memory loaded, otherwise copy the id. -->
              <Badge
                v-for="lid in m.linkedIds"
                :key="lid"
                variant="outline"
                class="cursor-pointer font-mono hover:bg-accent"
                :title="resolve(lid) ? 'Open linked memory' : 'Not loaded in this view — click to copy id'"
                @click="followLink(lid)"
              >
                <Link2 class="size-3" />{{ lid.slice(0, 8) }}…
              </Badge>
            </div>
          </div>

          <div class="flex gap-2 pt-2">
            <Button variant="outline" size="sm" class="flex-1" @click="startEdit"><Pencil class="size-4" />Edit</Button>
            <Button variant="outline" size="sm" class="flex-1 text-destructive hover:text-destructive" @click="remove"><Trash2 class="size-4" />Delete</Button>
          </div>
        </template>
      </div>
    </SheetContent>
  </Sheet>
</template>

<script setup>
import { computed, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { toast } from 'vue-sonner'
import { memoryClient } from '@/utils/connect'
import { renderMarkdown } from '@/utils/markdown'
import { useAuthStore } from '@/stores/auth'
import { confirmDialog } from '@/lib/confirm'
import {
  inspectorMemory,
  inspectorResolver,
  inspectorFindSimilar,
  openInspector,
  closeInspector,
  bumpInspectorChanged,
} from '@/lib/inspector'
import { Flame, History, Layers, Link2, MessagesSquare, Pencil, Search, Tag, Trash2 } from 'lucide-vue-next'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet'

const router = useRouter()
const auth = useAuthStore()

const m = computed(() => inspectorMemory.value)

const editing = ref(false)
const editText = ref('')
const editNamespace = ref('')
const editTags = ref('')
const saving = ref(false)

// Switching to another memory always drops out of edit mode.
watch(inspectorMemory, () => { editing.value = false })

function fmtDate(ts) {
  try {
    return ts.toDate().toLocaleString()
  } catch {
    return ''
  }
}

function handleError(e) {
  if (e instanceof ConnectError && e.code === Code.Unauthenticated) {
    closeInspector()
    auth.logout()
    router.push({ name: 'login' })
    return
  }
  toast.error(e.message || 'Request failed')
}

function resolve(id) {
  return inspectorResolver.value ? inspectorResolver.value(id) : undefined
}

// Close the sheet before expanding: the result (new neighbours) appears in the
// host view behind it.
function findSimilar() {
  const fn = inspectorFindSimilar.value
  const id = m.value.id
  closeInspector()
  fn(id)
}

async function followLink(id) {
  const target = resolve(id)
  if (target) {
    openInspector(target, inspectorResolver.value, inspectorFindSimilar.value)
    return
  }
  try {
    await navigator.clipboard.writeText(id)
    toast.info('Memory not loaded in this view — id copied')
  } catch {
    toast.info(id)
  }
}

function startEdit() {
  editText.value = m.value.text
  editNamespace.value = m.value.namespace || ''
  editTags.value = (m.value.tags || []).join(', ')
  editing.value = true
}

async function saveEdit() {
  saving.value = true
  try {
    const tags = editTags.value.split(',').map((t) => t.trim()).filter(Boolean)
    await memoryClient.updateMemory({
      id: m.value.id,
      text: editText.value,
      tags,
      replaceTags: true,
      namespace: editNamespace.value,
    })
    editing.value = false
    // Reflect the edit in the open sheet immediately — the server copy catches
    // up asynchronously via the re-index queue.
    inspectorMemory.value = { ...m.value, text: editText.value, tags, namespace: editNamespace.value }
    toast.success('Queued for re-indexing — changes appear shortly')
    bumpInspectorChanged()
  } catch (e) {
    handleError(e)
  } finally {
    saving.value = false
  }
}

async function remove() {
  if (!(await confirmDialog('Delete this memory?', { actionLabel: 'Delete' }))) return
  try {
    await memoryClient.delete({ id: m.value.id })
    toast.success('Memory deleted')
    closeInspector()
    bumpInspectorChanged()
  } catch (e) {
    handleError(e)
  }
}
</script>
