<template>
  <div>
    <h4 class="mb-3"><font-awesome-icon :icon="['fas', 'list-check']" class="me-2" />Indexing</h4>

    <div v-if="error" class="alert alert-danger py-2">{{ error }}</div>

    <div class="row g-2 mb-3">
      <div class="col">
        <div class="card text-center">
          <div class="card-body py-2">
            <div class="h4 mb-0">{{ counts.pending }}</div>
            <div class="small text-muted">Pending</div>
          </div>
        </div>
      </div>
      <div class="col">
        <div class="card text-center">
          <div class="card-body py-2">
            <div class="h4 mb-0">{{ counts.inFlight }}</div>
            <div class="small text-muted">In flight</div>
          </div>
        </div>
      </div>
      <div class="col">
        <div class="card text-center" :class="{ 'border-danger': counts.dead > 0 }">
          <div class="card-body py-2">
            <div class="h4 mb-0" :class="{ 'text-danger': counts.dead > 0 }">{{ counts.dead }}</div>
            <div class="small text-muted">Dead-lettered</div>
          </div>
        </div>
      </div>
    </div>

    <div class="d-flex align-items-center gap-2 mb-3">
      <button class="btn btn-primary btn-sm" :disabled="loading" @click="reload">
        <font-awesome-icon :icon="['fas', 'rotate']" class="me-1" />Refresh
      </button>
      <button class="btn btn-outline-warning btn-sm" :disabled="busy || counts.dead === 0" @click="requeue">
        <font-awesome-icon :icon="['fas', 'arrow-rotate-left']" class="me-1" />Requeue all
      </button>
      <button class="btn btn-outline-danger btn-sm" :disabled="busy || counts.dead === 0" @click="purge">
        <font-awesome-icon :icon="['fas', 'trash']" class="me-1" />Purge all
      </button>
      <span v-if="!consumerPresent" class="badge bg-secondary">worker offline</span>
    </div>

    <h6 class="text-muted">Failed memories</h6>
    <div v-if="dead.length === 0" class="text-center text-muted py-4">
      <font-awesome-icon :icon="['fas', 'circle-check']" size="2x" class="mb-2 d-block" />
      No dead-lettered memories.
    </div>

    <div v-for="(dl, i) in dead" :key="i" class="card mb-2 border-danger">
      <div class="card-body py-2">
        <p class="mb-1" style="white-space: pre-wrap">{{ dl.record?.text }}</p>
        <div class="small text-danger mb-1"><font-awesome-icon :icon="['fas', 'triangle-exclamation']" class="me-1" />{{ dl.error }}</div>
        <div class="small text-muted d-flex flex-wrap gap-2">
          <span v-if="dl.record?.namespace" class="badge bg-secondary">{{ dl.record.namespace }}</span>
          <span>{{ dl.deliveries }} attempts</span>
          <span v-if="dl.failedAt">failed {{ formatDate(dl.failedAt) }}</span>
          <span v-if="dl.record?.id" class="text-muted">{{ dl.record.id }}</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { memoryClient } from '@/utils/connect'
import { DeadAction } from '@/gen/cortex/v1/cortex_pb'
import { useAuthStore } from '@/stores/auth'

const router = useRouter()
const auth = useAuthStore()

// int64 fields arrive as bigint; coerce to number for display and comparison.
const counts = reactive({ pending: 0, inFlight: 0, dead: 0 })
const consumerPresent = ref(true)
const dead = ref([])
const loading = ref(false)
const busy = ref(false)
const error = ref('')

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
    return true
  }
  error.value = e.message || 'Request failed'
  return false
}

async function reload() {
  loading.value = true
  error.value = ''
  try {
    const q = await memoryClient.indexQueue({})
    counts.pending = Number(q.pending)
    counts.inFlight = Number(q.inFlight)
    counts.dead = Number(q.dead)
    consumerPresent.value = q.consumerPresent
    const d = await memoryClient.dead({ action: DeadAction.LIST })
    dead.value = d.deadLetters
  } catch (e) {
    if (handleError(e)) return
  } finally {
    loading.value = false
  }
}

async function requeue() {
  if (!confirm('Requeue all dead-lettered memories for indexing?')) return
  busy.value = true
  try {
    await memoryClient.dead({ action: DeadAction.REQUEUE })
    await reload()
  } catch (e) {
    handleError(e)
  } finally {
    busy.value = false
  }
}

async function purge() {
  if (!confirm('Permanently purge all dead-lettered memories? This cannot be undone.')) return
  busy.value = true
  try {
    await memoryClient.dead({ action: DeadAction.PURGE })
    await reload()
  } catch (e) {
    handleError(e)
  } finally {
    busy.value = false
  }
}

onMounted(reload)
</script>
