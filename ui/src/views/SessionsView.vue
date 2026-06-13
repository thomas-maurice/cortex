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
        <p class="mb-1" style="white-space: pre-wrap">{{ s.text }}</p>
        <div class="small text-muted d-flex flex-wrap gap-2">
          <span class="badge bg-secondary"><font-awesome-icon :icon="['fas', 'layer-group']" class="me-1" />{{ s.namespace }}</span>
          <span class="font-monospace">{{ truncate(s.conversationId, 28) }}</span>
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
import { useAuthStore } from '@/stores/auth'
import { truncate, formatTimestamp } from '@/utils/text'

const router = useRouter()
const auth = useAuthStore()

const namespace = ref('*')
const summaries = ref([])
const loading = ref(false)
const error = ref('')

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
