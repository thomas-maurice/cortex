<template>
  <div style="max-width: 560px">
    <h4 class="mb-3"><font-awesome-icon :icon="['fas', 'server']" class="me-2" />Server status</h4>

    <div v-if="error" class="alert alert-danger py-2">{{ error }}</div>

    <div v-if="loading" class="text-center text-muted py-5">
      <font-awesome-icon :icon="['fas', 'spinner']" spin size="2x" />
    </div>

    <table v-else-if="status" class="table table-sm">
      <tbody>
        <tr><th>NATS</th><td><health :ok="status.natsOk" /></td></tr>
        <tr><th>Weaviate</th><td><health :ok="status.weaviateOk" /></td></tr>
        <tr><th>Ollama</th><td><health :ok="status.ollamaOk" /></td></tr>
        <tr>
          <th>Model</th>
          <td>
            {{ status.model }}
            <span v-if="status.ollamaOk && !status.modelPresent" class="badge bg-warning text-dark ms-2">not downloaded</span>
          </td>
        </tr>
        <tr><th>Dimensions</th><td>{{ status.dims }}</td></tr>
        <tr><th>Memories</th><td>{{ status.memoryCount }}</td></tr>
        <tr><th>Version</th><td>{{ status.version }}</td></tr>
      </tbody>
    </table>

    <div v-if="status && status.ollamaOk && !status.modelPresent" class="alert alert-warning py-2">
      <div class="mb-2">
        The embedding model <code>{{ status.model }}</code> is not downloaded in Ollama.
        Nothing can be embedded or searched until it is pulled.
      </div>
      <div v-if="pullError" class="text-danger small mb-2">{{ pullError }}</div>
      <button class="btn btn-warning btn-sm" :disabled="pulling || loading" @click="pullModel">
        <font-awesome-icon :icon="['fas', pulling ? 'spinner' : 'download']" :spin="pulling" class="me-1" />
        {{ pulling ? 'Pulling…' : 'Pull model' }}
      </button>
    </div>

    <button class="btn btn-primary btn-sm" :disabled="loading || pulling" @click="reload">
      <font-awesome-icon :icon="['fas', 'rotate']" class="me-1" />Refresh
    </button>
  </div>
</template>

<script setup>
import { ref, onMounted, h } from 'vue'
import { useRouter } from 'vue-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { memoryClient } from '@/utils/connect'
import { useAuthStore } from '@/stores/auth'

const router = useRouter()
const auth = useAuthStore()

const status = ref(null)
const loading = ref(false)
const error = ref('')
const pulling = ref(false)
const pullError = ref('')

// Tiny inline component: green check / red cross for a boolean health flag.
const health = (props) =>
  h(
    'span',
    { class: props.ok ? 'text-success' : 'text-danger' },
    props.ok ? '✓ ok' : '✗ down'
  )
health.props = ['ok']

async function reload() {
  loading.value = true
  error.value = ''
  try {
    status.value = await memoryClient.status({})
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

async function pullModel() {
  pulling.value = true
  pullError.value = ''
  try {
    await memoryClient.pullModel({})
    await reload()
  } catch (e) {
    if (e instanceof ConnectError && e.code === Code.Unauthenticated) {
      auth.logout()
      router.push({ name: 'login' })
      return
    }
    pullError.value = e.message || 'Pull failed'
  } finally {
    pulling.value = false
  }
}

onMounted(reload)
</script>
