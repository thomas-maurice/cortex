<template>
  <div class="space-y-4">
    <Alert>
      <SlidersHorizontal class="size-4" />
      <AlertDescription>
        <strong class="text-foreground">Standing preferences</strong> — cross-project rules stored as memories in the
        <code>global</code> namespace, tagged <code>preference</code>. The SessionStart hook injects
        these into <em>every</em> Claude session, so they apply before the agent acts. Editing here is
        all you need; the namespace and <code>preference</code> tag are managed for you.
      </AlertDescription>
    </Alert>

    <div>
      <Button size="sm" @click="showNew = !showNew">
        <Plus class="size-4" />New preference
      </Button>
      <Card v-if="showNew" class="mt-2">
        <CardContent class="space-y-2 py-4">
          <Textarea v-model="draft.text" rows="3" placeholder="e.g. Never commit or push unless explicitly instructed to." />
          <div class="flex flex-wrap gap-2">
            <Input v-model="draft.tags" class="flex-1" placeholder="extra tags, comma separated (optional)" />
            <Button size="sm" :disabled="!draft.text.trim() || saving" @click="save">Save</Button>
          </div>
          <p v-if="saved" class="text-sm text-emerald-600 dark:text-emerald-400">Queued for indexing — it will appear shortly.</p>
        </CardContent>
      </Card>
    </div>

    <Alert v-if="error" variant="destructive">
      <AlertDescription>{{ error }}</AlertDescription>
    </Alert>

    <div v-if="loading" class="py-16 text-center text-muted-foreground" role="status" aria-live="polite" aria-label="Loading preferences">
      <Loader2 class="mx-auto size-8 animate-spin" />
    </div>

    <div v-else-if="prefs.length === 0" class="py-16 text-center text-muted-foreground">
      <SlidersHorizontal class="mx-auto mb-2 size-8" />
      No preferences yet. Add one above — it will apply from your next session.
    </div>

    <Card v-for="m in prefs" :key="m.id">
      <CardContent class="py-4">
        <div v-if="editId === m.id" class="space-y-2">
          <Textarea v-model="editDraft.text" rows="5" placeholder="Preference text (Markdown)…" />
          <Input v-model="editDraft.tags" placeholder="extra tags, comma separated (optional)" />
          <div class="flex items-center gap-2">
            <Button size="sm" :disabled="!editDraft.text.trim() || editing" @click="saveEdit(m)">Save</Button>
            <Button size="sm" variant="outline" :disabled="editing" @click="cancelEdit">Cancel</Button>
            <span v-if="editing" class="text-sm text-muted-foreground">Queued for re-indexing…</span>
          </div>
        </div>
        <template v-else>
          <div class="flex items-start justify-between gap-3">
            <div class="markdown-body min-w-0 text-sm" v-html="renderMarkdown(m.text)"></div>
            <div class="flex shrink-0 gap-1">
              <Button variant="outline" size="icon" class="size-8" title="Edit" aria-label="Edit" @click="startEdit(m)">
                <Pencil class="size-3.5" />
              </Button>
              <Button variant="outline" size="icon" class="size-8 text-destructive hover:text-destructive" title="Delete" aria-label="Delete" @click="remove(m.id)">
                <Trash2 class="size-3.5" />
              </Button>
            </div>
          </div>
          <div class="mt-2 flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
            <Badge v-for="t in extraTags(m.tags)" :key="t" variant="outline">
              <Tag class="size-3" />{{ t }}
            </Badge>
            <span v-if="m.createdAt">{{ formatDate(m.createdAt) }}</span>
          </div>
        </template>
      </CardContent>
    </Card>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { memoryClient } from '@/utils/connect'
import { renderMarkdown } from '@/utils/markdown'
import { useAuthStore } from '@/stores/auth'
import { Loader2, Pencil, Plus, SlidersHorizontal, Tag, Trash2 } from 'lucide-vue-next'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'

const router = useRouter()
const auth = useAuthStore()

// Preferences are just memories scoped to this namespace + tag. Centralised here
// so the page, the SessionStart hook, and CLAUDE.md all agree on the convention.
const PREF_NAMESPACE = 'global'
const PREF_TAG = 'preference'

const prefs = ref([])
const loading = ref(false)
const error = ref('')

const showNew = ref(false)
const saving = ref(false)
const saved = ref(false)
const draft = ref({ text: '', tags: '' })

const editId = ref(null)
const editing = ref(false)
const editDraft = ref({ text: '', tags: '' })

function formatDate(ts) {
  try {
    return ts.toDate().toLocaleString()
  } catch {
    return ''
  }
}

// extraTags hides the always-present "preference" tag, leaving only the user's
// topical tags for display.
function extraTags(tags) {
  return (tags || []).filter((t) => t !== PREF_TAG)
}

// withPrefTag normalises a comma-separated tag string into the stored tag set:
// always includes PREF_TAG, deduped, no blanks.
function withPrefTag(csv) {
  const extra = (csv || '').split(',').map((t) => t.trim()).filter(Boolean)
  return [...new Set([PREF_TAG, ...extra])]
}

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
    const res = await memoryClient.list({ namespace: PREF_NAMESPACE, tags: [PREF_TAG], limit: 100 })
    prefs.value = res.memories
  } catch (e) {
    handleError(e)
  } finally {
    loading.value = false
  }
}

async function save() {
  saving.value = true
  saved.value = false
  error.value = ''
  try {
    await memoryClient.save({ text: draft.value.text, namespace: PREF_NAMESPACE, tags: withPrefTag(draft.value.tags) })
    draft.value = { text: '', tags: '' }
    saved.value = true
    setTimeout(reload, 1200) // indexing is async; give the worker a moment
  } catch (e) {
    handleError(e)
  } finally {
    saving.value = false
  }
}

function startEdit(m) {
  editId.value = m.id
  editDraft.value = { text: m.text, tags: extraTags(m.tags).join(', ') }
}

function cancelEdit() {
  editId.value = null
}

async function saveEdit(m) {
  editing.value = true
  error.value = ''
  try {
    // replaceTags keeps the canonical preference tag set; namespace omitted so the
    // memory stays in global (UpdateMemory leaves namespace untouched when empty).
    await memoryClient.updateMemory({
      id: m.id,
      text: editDraft.value.text,
      tags: withPrefTag(editDraft.value.tags),
      replaceTags: true,
    })
    editId.value = null
    setTimeout(reload, 1200)
  } catch (e) {
    handleError(e)
  } finally {
    editing.value = false
  }
}

async function remove(id) {
  if (!confirm('Delete this preference? This cannot be undone.')) return
  error.value = ''
  try {
    await memoryClient.delete({ id })
    prefs.value = prefs.value.filter((m) => m.id !== id)
  } catch (e) {
    handleError(e)
  }
}

onMounted(reload)
</script>
