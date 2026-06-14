<template>
  <div>
    <div class="mb-3">
      <button class="btn btn-success btn-sm" @click="showNew = !showNew">
        <font-awesome-icon :icon="['fas', 'plus']" class="me-1" />New memory
      </button>
      <div v-if="showNew" class="card mt-2">
        <div class="card-body py-2">
          <textarea v-model="draft.text" class="form-control form-control-sm mb-2" rows="3" placeholder="Memory text…"></textarea>
          <div class="row g-2">
            <div class="col"><input v-model="draft.namespace" class="form-control form-control-sm" placeholder="namespace (blank = server default)" /></div>
            <div class="col"><input v-model="draft.tags" class="form-control form-control-sm" placeholder="tags, comma separated" /></div>
            <div class="col-auto">
              <button class="btn btn-primary btn-sm" :disabled="!draft.text.trim() || saving" @click="save">Save</button>
            </div>
          </div>
          <div v-if="saved" class="small text-success mt-2">Queued for indexing — it will appear shortly.</div>
        </div>
      </div>
    </div>

    <div class="row g-2 align-items-end mb-3">
      <div class="col-auto" style="width: 180px">
        <label class="form-label small mb-1">Namespace</label>
        <input v-model="namespace" class="form-control form-control-sm" placeholder="* = all" @keyup.enter="reload" />
      </div>
      <div class="col">
        <label class="form-label small mb-1">Search</label>
        <input v-model="query" class="form-control form-control-sm" placeholder="semantic query (blank = list newest)" @keyup.enter="reload" />
      </div>
      <div class="col-auto">
        <button class="btn btn-primary btn-sm" :disabled="loading" @click="reload">
          <font-awesome-icon :icon="['fas', query ? 'magnifying-glass' : 'rotate']" class="me-1" />
          {{ query ? 'Search' : 'Refresh' }}
        </button>
      </div>
    </div>

    <div v-if="error" class="alert alert-danger py-2">{{ error }}</div>

    <div v-if="loading" class="text-center text-muted py-5">
      <font-awesome-icon :icon="['fas', 'spinner']" spin size="2x" />
    </div>

    <div v-else-if="memories.length === 0" class="text-center text-muted py-5">
      <font-awesome-icon :icon="['fas', 'database']" size="2x" class="mb-2 d-block" />
      No memories found.
    </div>

    <div v-for="m in memories" :key="m.id" class="card mb-2">
      <div class="card-body py-2">
        <div class="d-flex justify-content-between align-items-start">
          <div class="mb-1 me-3 markdown-body" v-html="renderMarkdown(m.text)"></div>
          <button class="btn btn-outline-danger btn-sm flex-shrink-0" title="Delete" @click="remove(m.id)">
            <font-awesome-icon :icon="['fas', 'trash']" />
          </button>
        </div>
        <div class="small text-muted d-flex flex-wrap gap-2 align-items-center">
          <span class="badge bg-secondary">
            <font-awesome-icon :icon="['fas', 'layer-group']" class="me-1" />{{ m.namespace }}
          </span>
          <span v-for="t in m.tags" :key="t" class="badge bg-info text-dark">
            <font-awesome-icon :icon="['fas', 'tag']" class="me-1" />{{ t }}
          </span>
          <span v-if="m.source">src: {{ m.source }}</span>
          <span v-if="m.conversationId" class="font-monospace">
            <font-awesome-icon :icon="['fas', 'comments']" class="me-1" />{{ m.conversationId }}
          </span>
          <span v-if="m.createdAt">{{ formatDate(m.createdAt) }}</span>
          <span v-if="m._distance !== undefined">dist: {{ m._distance.toFixed(3) }}</span>
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

const router = useRouter()
const auth = useAuthStore()

const namespace = ref('*')
const query = ref('')
const memories = ref([])
const loading = ref(false)
const error = ref('')

const showNew = ref(false)
const saving = ref(false)
const saved = ref(false)
const draft = ref({ text: '', namespace: '', tags: '' })

function formatDate(ts) {
  try {
    return ts.toDate().toLocaleString()
  } catch {
    return ''
  }
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
    if (query.value.trim()) {
      const res = await memoryClient.search({ query: query.value, namespace: namespace.value, limit: 50 })
      memories.value = res.hits.map((h) => ({ ...h.memory, _distance: h.distance }))
    } else {
      const res = await memoryClient.list({ namespace: namespace.value, limit: 50 })
      memories.value = res.memories
    }
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
    const tags = draft.value.tags.split(',').map((t) => t.trim()).filter(Boolean)
    await memoryClient.save({ text: draft.value.text, namespace: draft.value.namespace, tags })
    draft.value = { text: '', namespace: '', tags: '' }
    saved.value = true
  } catch (e) {
    handleError(e)
  } finally {
    saving.value = false
  }
}

async function remove(id) {
  if (!confirm('Delete this memory?')) return
  try {
    await memoryClient.delete({ id })
    memories.value = memories.value.filter((m) => m.id !== id)
  } catch (e) {
    handleError(e)
  }
}

onMounted(reload)
</script>
