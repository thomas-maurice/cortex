<template>
  <div>
    <div class="row g-2 align-items-end mb-3">
      <div class="col-auto" style="width: 180px">
        <label class="form-label small mb-1">Namespace</label>
        <input v-model="namespace" class="form-control form-control-sm" placeholder="* = all" @keyup.enter="reload" />
      </div>
      <div class="col-auto">
        <button class="btn btn-primary btn-sm" :disabled="loading" @click="reload">
          <font-awesome-icon :icon="['fas', 'rotate']" class="me-1" />Refresh
        </button>
      </div>
    </div>

    <div v-if="error" class="alert alert-danger py-2">{{ error }}</div>

    <div v-if="loading" class="text-center text-muted py-5">
      <font-awesome-icon :icon="['fas', 'spinner']" spin size="2x" />
    </div>

    <div v-else-if="summaries.length === 0" class="text-center text-muted py-5">
      <font-awesome-icon :icon="['fas', 'comments']" size="2x" class="mb-2 d-block" />
      No conversation summaries yet.
    </div>

    <div v-for="s in summaries" :key="s.conversationId" class="card mb-2">
      <div class="card-body py-2">
        <div v-if="editId === s.conversationId">
          <textarea v-model="editText" class="form-control form-control-sm mb-2" rows="6" placeholder="Summary text (Markdown)…"></textarea>
          <div class="d-flex gap-2 align-items-center">
            <button class="btn btn-primary btn-sm" :disabled="!editText.trim() || editing" @click="saveEdit(s)">Save</button>
            <button class="btn btn-outline-secondary btn-sm" :disabled="editing" @click="editId = null">Cancel</button>
            <span v-if="editing" class="small text-muted">Queued for re-indexing…</span>
          </div>
        </div>
        <template v-else>
          <div class="d-flex justify-content-between align-items-start">
            <div class="mb-1 me-3 markdown-body" v-html="renderMarkdown(s.text)"></div>
            <button class="btn btn-outline-secondary btn-sm flex-shrink-0" title="Edit" @click="startEdit(s)">
              <font-awesome-icon :icon="['fas', 'pen']" />
            </button>
          </div>
        </template>
        <div class="small text-muted d-flex flex-wrap gap-2 mt-1">
          <span class="badge bg-secondary"><font-awesome-icon :icon="['fas', 'layer-group']" class="me-1" />{{ s.namespace }}</span>
          <span class="font-monospace">{{ s.conversationId }}</span>
          <span v-if="s.updatedAt">updated {{ formatTimestamp(s.updatedAt) }}</span>
        </div>
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
import { formatTimestamp } from '@/utils/text'

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
