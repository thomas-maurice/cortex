<template>
  <div class="space-y-4">
    <div class="flex flex-wrap items-end gap-2">
      <div class="grid gap-1.5" style="width: 180px">
        <Label class="text-xs">Namespace</Label>
        <Input v-model="namespace" placeholder="* = all" @keyup.enter="reload" />
      </div>
      <Button :disabled="loading" @click="reload">
        <RotateCw class="size-4" />Refresh
      </Button>
    </div>

    <Alert v-if="error" variant="destructive">
      <AlertDescription>{{ error }}</AlertDescription>
    </Alert>

    <div v-if="loading" class="py-16 text-center text-muted-foreground" role="status" aria-live="polite" aria-label="Loading summaries">
      <Loader2 class="mx-auto size-8 animate-spin" />
    </div>

    <div v-else-if="summaries.length === 0" class="py-16 text-center text-muted-foreground">
      <MessagesSquare class="mx-auto mb-2 size-8" />
      No conversation summaries yet.
    </div>

    <Card v-for="s in summaries" :key="s.conversationId">
      <CardContent class="py-4">
        <div v-if="editId === s.conversationId" class="space-y-2">
          <Textarea v-model="editText" rows="6" placeholder="Summary text (Markdown)…" />
          <div class="flex items-center gap-2">
            <Button size="sm" :disabled="!editText.trim() || editing" @click="saveEdit(s)">Save</Button>
            <Button size="sm" variant="outline" :disabled="editing" @click="editId = null">Cancel</Button>
            <span v-if="editing" class="text-sm text-muted-foreground">Queued for re-indexing…</span>
          </div>
        </div>
        <template v-else>
          <div class="flex items-start justify-between gap-3">
            <ClampedMarkdown class="min-w-0" :html="renderMarkdown(s.text)" />
            <Button variant="outline" size="icon" class="size-8 shrink-0" title="Edit" aria-label="Edit session" @click="startEdit(s)">
              <Pencil class="size-3.5" />
            </Button>
          </div>
        </template>
        <div class="mt-2 flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
          <Badge
            variant="secondary"
            class="cursor-pointer hover:bg-secondary/80"
            title="Filter by this namespace"
            @click="filterByNamespace(s.namespace)"
          >
            <Layers class="size-3" />{{ s.namespace }}
          </Badge>
          <span class="inline-flex items-center gap-1 font-mono">
            <MessagesSquare class="size-3" />{{ s.conversationId }}
          </span>
          <span v-if="s.source">src: {{ s.source }}</span>
          <span v-if="s.createdAt">created {{ formatTimestamp(s.createdAt) }}</span>
          <span v-if="s.updatedAt" class="inline-flex items-center gap-1">
            <History class="size-3" />updated {{ formatTimestamp(s.updatedAt) }}
          </span>
        </div>
      </CardContent>
    </Card>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { toast } from 'vue-sonner'
import { memoryClient } from '@/utils/connect'
import { renderMarkdown } from '@/utils/markdown'
import { useAuthStore } from '@/stores/auth'
import { formatTimestamp } from '@/utils/text'
import { History, Layers, Loader2, MessagesSquare, Pencil, RotateCw } from 'lucide-vue-next'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import ClampedMarkdown from '@/components/ClampedMarkdown.vue'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Textarea } from '@/components/ui/textarea'

const router = useRouter()
const auth = useAuthStore()

const namespace = ref('*')
const summaries = ref([])
const loading = ref(false)
const error = ref('')

const editId = ref(null)
const editText = ref('')
const editing = ref(false)

function startEdit(s) {
  editId.value = s.conversationId
  editText.value = s.text
}

function filterByNamespace(ns) {
  namespace.value = ns
  reload()
}

async function saveEdit(s) {
  editing.value = true
  error.value = ''
  try {
    // SummarizeSession upserts by conversationId, so saving with the same ID
    // replaces the summary in place — i.e. an edit.
    await memoryClient.summarizeSession({
      conversationId: s.conversationId,
      text: editText.value,
      namespace: s.namespace,
    })
    editId.value = null
    toast.success('Queued for re-indexing — changes appear shortly')
    setTimeout(reload, 1200)
  } catch (e) {
    if (e instanceof ConnectError && e.code === Code.Unauthenticated) {
      auth.logout()
      router.push({ name: 'login' })
      return
    }
    error.value = e.message || 'Request failed'
  } finally {
    editing.value = false
  }
}

async function reload() {
  loading.value = true
  error.value = ''
  try {
    const res = await memoryClient.listSummaries({ namespace: namespace.value, limit: 50 })
    summaries.value = res.summaries
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
