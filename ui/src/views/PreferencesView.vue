<template>
  <div>
    <div class="alert alert-secondary py-2 small d-flex align-items-start gap-2">
      <font-awesome-icon :icon="['fas', 'sliders']" class="mt-1" />
      <div>
        <strong>Standing preferences</strong> — cross-project rules stored as memories in the
        <code>global</code> namespace, tagged <code>preference</code>. The SessionStart hook injects
        these into <em>every</em> Claude session, so they apply before the agent acts. Editing here is
        all you need; the namespace and <code>preference</code> tag are managed for you.
      </div>
    </div>

    <div class="mb-3">
      <button class="btn btn-success btn-sm" @click="showNew = !showNew">
        <font-awesome-icon :icon="['fas', 'plus']" class="me-1" />New preference
      </button>
      <div v-if="showNew" class="card mt-2">
        <div class="card-body py-2">
          <textarea v-model="draft.text" class="form-control form-control-sm mb-2" rows="3"
            placeholder="e.g. Never commit or push unless explicitly instructed to."></textarea>
          <div class="row g-2">
            <div class="col"><input v-model="draft.tags" class="form-control form-control-sm" placeholder="extra tags, comma separated (optional)" /></div>
            <div class="col-auto">
              <button class="btn btn-primary btn-sm" :disabled="!draft.text.trim() || saving" @click="save">Save</button>
            </div>
          </div>
          <div v-if="saved" class="small text-success mt-2">Queued for indexing — it will appear shortly.</div>
        </div>
      </div>
    </div>

    <div v-if="error" class="alert alert-danger py-2">{{ error }}</div>

    <div v-if="loading" class="text-center text-muted py-5">
      <font-awesome-icon :icon="['fas', 'spinner']" spin size="2x" />
    </div>

    <div v-else-if="prefs.length === 0" class="text-center text-muted py-5">
      <font-awesome-icon :icon="['fas', 'sliders']" size="2x" class="mb-2 d-block" />
      No preferences yet. Add one above — it will apply from your next session.
    </div>

    <div v-for="m in prefs" :key="m.id" class="card mb-2">
      <div class="card-body py-2">
        <div v-if="editId === m.id">
          <textarea v-model="editDraft.text" class="form-control form-control-sm mb-2" rows="5" placeholder="Preference text (Markdown)…"></textarea>
          <input v-model="editDraft.tags" class="form-control form-control-sm mb-2" placeholder="extra tags, comma separated (optional)" />
          <div class="d-flex gap-2 align-items-center">
            <button class="btn btn-primary btn-sm" :disabled="!editDraft.text.trim() || editing" @click="saveEdit(m)">Save</button>
            <button class="btn btn-outline-secondary btn-sm" :disabled="editing" @click="cancelEdit">Cancel</button>
            <span v-if="editing" class="small text-muted">Queued for re-indexing…</span>
          </div>
        </div>
        <template v-else>
          <div class="d-flex justify-content-between align-items-start">
            <div class="mb-1 me-3 markdown-body" v-html="renderMarkdown(m.text)"></div>
            <div class="d-flex gap-1 flex-shrink-0">
              <button class="btn btn-outline-secondary btn-sm" title="Edit" @click="startEdit(m)">
                <font-awesome-icon :icon="['fas', 'pen']" />
              </button>
              <button class="btn btn-outline-danger btn-sm" title="Delete" @click="remove(m.id)">
                <font-awesome-icon :icon="['fas', 'trash']" />
              </button>
            </div>
          </div>
          <div class="small text-muted d-flex flex-wrap gap-2 align-items-center mt-1">
            <span v-for="t in extraTags(m.tags)" :key="t" class="badge bg-info text-dark">
              <font-awesome-icon :icon="['fas', 'tag']" class="me-1" />{{ t }}
            </span>
            <span v-if="m.createdAt">{{ formatDate(m.createdAt) }}</span>
          </div>
        </template>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { memoryClient } from '@/utils/connect'
import { renderMarkdown } from '@/utils/markdown'
import { useAuthStore } from '@/stores/auth'

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
