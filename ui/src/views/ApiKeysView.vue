<template>
  <div>
    <h4 class="mb-3"><font-awesome-icon :icon="['fas', 'key']" class="me-2" />API Keys</h4>

    <div v-if="error" class="alert alert-danger py-2">{{ error }}</div>
    <div v-if="notice" class="alert alert-success py-2">{{ notice }}</div>

    <!-- New key reveal (shown immediately after creation) -->
    <div v-if="createdKey" class="alert alert-warning">
      <strong>Copy your new API key now — it won't be shown again.</strong>
      <div class="d-flex align-items-center gap-2 mt-2">
        <code class="flex-grow-1 user-select-all">{{ createdKey }}</code>
        <button class="btn btn-sm btn-outline-dark" @click="copyKey(createdKey)">
          <font-awesome-icon :icon="['fas', 'copy']" /> Copy
        </button>
      </div>
      <button class="btn btn-sm btn-secondary mt-2" @click="createdKey = ''">Dismiss</button>
    </div>

    <!-- Create key form -->
    <div class="d-flex align-items-end gap-2 mb-3">
      <div>
        <label class="form-label small mb-1">Label (optional)</label>
        <input
          v-model="newLabel"
          class="form-control form-control-sm"
          placeholder="e.g. laptop, ci"
          :disabled="busy"
          @keyup.enter="createKey"
        />
      </div>
      <button class="btn btn-primary btn-sm" :disabled="busy" @click="createKey">
        <font-awesome-icon :icon="['fas', 'plus']" class="me-1" />New key
      </button>
      <button class="btn btn-outline-secondary btn-sm" :disabled="loading" @click="reload">
        <font-awesome-icon :icon="['fas', 'rotate']" class="me-1" />Refresh
      </button>
    </div>

    <div v-if="loading" class="text-center text-muted py-5">
      <font-awesome-icon :icon="['fas', 'spinner']" spin size="2x" />
    </div>

    <div v-else-if="keys.length === 0" class="text-center text-muted py-5">
      <font-awesome-icon :icon="['fas', 'key']" size="2x" class="mb-2 d-block" />
      No API keys yet. Create one above to use with the MCP server or CLI.
    </div>

    <table v-else class="table table-sm align-middle">
      <thead>
        <tr>
          <th>Prefix</th>
          <th>Label</th>
          <th>Created</th>
          <th>Last used</th>
          <th class="text-end" style="width: 1%">Actions</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="k in keys" :key="k.id">
          <td class="font-monospace">{{ k.prefix }}…</td>
          <td class="text-muted small">{{ k.label || '—' }}</td>
          <td class="small text-muted">{{ k.createdAt ? formatTimestamp(k.createdAt) : '—' }}</td>
          <td class="small text-muted">{{ k.lastUsedAt ? formatTimestamp(k.lastUsedAt) : 'never' }}</td>
          <td class="text-end">
            <button
              class="btn btn-outline-danger btn-sm"
              title="Revoke key"
              :disabled="busy"
              @click="revokeKey(k)"
            >
              <font-awesome-icon :icon="['fas', 'trash']" />
            </button>
          </td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { memoryClient } from '@/utils/connect'
import { useAuthStore } from '@/stores/auth'
import { formatTimestamp } from '@/utils/text'

const router = useRouter()
const auth = useAuthStore()

const keys = ref([])
const loading = ref(false)
const busy = ref(false)
const error = ref('')
const notice = ref('')
const newLabel = ref('')
const createdKey = ref('')

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
    const res = await memoryClient.listApiKeys({})
    keys.value = res.keys
  } catch (e) {
    if (handleError(e)) return
  } finally {
    loading.value = false
  }
}

async function createKey() {
  busy.value = true
  error.value = ''
  notice.value = ''
  createdKey.value = ''
  try {
    const res = await memoryClient.createApiKey({ label: newLabel.value.trim() })
    createdKey.value = res.rawKey
    notice.value = 'API key created. Copy it now — it will not be shown again.'
    newLabel.value = ''
    await reload()
  } catch (e) {
    handleError(e)
  } finally {
    busy.value = false
  }
}

async function revokeKey(k) {
  const label = k.label ? `"${k.label}" (${k.prefix}…)` : `${k.prefix}…`
  if (!window.confirm(`Revoke API key ${label}? Any client using it will lose access immediately.`)) return
  busy.value = true
  error.value = ''
  notice.value = ''
  try {
    await memoryClient.deleteApiKey({ id: k.id })
    notice.value = `Key ${k.prefix}… revoked.`
    await reload()
  } catch (e) {
    handleError(e)
  } finally {
    busy.value = false
  }
}

async function copyKey(raw) {
  try {
    await navigator.clipboard.writeText(raw)
    notice.value = 'Key copied to clipboard.'
  } catch {
    error.value = 'Could not copy — please select and copy the key manually.'
  }
}

onMounted(reload)
</script>
