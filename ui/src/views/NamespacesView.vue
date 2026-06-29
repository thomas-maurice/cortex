<template>
  <div>
    <h4 class="mb-3"><font-awesome-icon :icon="['fas', 'layer-group']" class="me-2" />Namespaces</h4>

    <div v-if="error" class="alert alert-danger py-2">{{ error }}</div>
    <div v-if="notice" class="alert alert-success py-2">{{ notice }}</div>

    <div class="d-flex align-items-center gap-2 mb-3">
      <button class="btn btn-primary btn-sm" :disabled="loading" @click="reload">
        <font-awesome-icon :icon="['fas', 'rotate']" class="me-1" />Refresh
      </button>
    </div>

    <div v-if="loading" class="text-center text-muted py-5">
      <font-awesome-icon :icon="['fas', 'spinner']" spin size="2x" />
    </div>

    <div v-else-if="namespaces.length === 0" class="text-center text-muted py-5">
      <font-awesome-icon :icon="['fas', 'layer-group']" size="2x" class="mb-2 d-block" />
      No namespaces yet.
    </div>

    <table v-else class="table table-sm align-middle">
      <thead>
        <tr>
          <th>Namespace</th>
          <th class="text-end">Memories</th>
          <th class="text-end">Summaries</th>
          <th>Last activity</th>
          <th class="text-end" style="width: 1%">Actions</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="ns in namespaces" :key="ns.name">
          <td>
            <template v-if="renameOf === ns.name">
              <input
                v-model="renameTo"
                class="form-control form-control-sm"
                style="max-width: 360px"
                :disabled="busy"
                placeholder="new namespace…"
                @keyup.enter="confirmRename(ns)"
                @keyup.esc="cancelRename"
              />
            </template>
            <span v-else class="font-monospace">{{ ns.name || '(empty)' }}</span>
          </td>
          <td class="text-end">{{ Number(ns.memoryCount) }}</td>
          <td class="text-end">{{ Number(ns.summaryCount) }}</td>
          <td class="small text-muted">{{ ns.lastUpdated ? formatTimestamp(ns.lastUpdated) : '—' }}</td>
          <td class="text-end text-nowrap">
            <template v-if="renameOf === ns.name">
              <button class="btn btn-primary btn-sm me-1" :disabled="busy || !renameTo.trim() || renameTo.trim() === ns.name" @click="confirmRename(ns)">
                Save
              </button>
              <button class="btn btn-outline-secondary btn-sm" :disabled="busy" @click="cancelRename">Cancel</button>
            </template>
            <template v-else>
              <button class="btn btn-outline-secondary btn-sm me-1" title="Rename" :disabled="busy" @click="startRename(ns)">
                <font-awesome-icon :icon="['fas', 'pen']" />
              </button>
              <button class="btn btn-outline-danger btn-sm" title="Delete namespace" :disabled="busy" @click="remove(ns)">
                <font-awesome-icon :icon="['fas', 'trash']" />
              </button>
            </template>
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

const namespaces = ref([])
const loading = ref(false)
const busy = ref(false)
const error = ref('')
const notice = ref('')

const renameOf = ref(null)
const renameTo = ref('')

function handleError(e) {
  if (e instanceof ConnectError && e.code === Code.Unauthenticated) {
    auth.logout()
    router.push({ name: 'login' })
    return true
  }
  error.value = e.message || 'Request failed'
  return false
}

function startRename(ns) {
  error.value = ''
  notice.value = ''
  renameOf.value = ns.name
  renameTo.value = ns.name
}

function cancelRename() {
  renameOf.value = null
  renameTo.value = ''
}

async function confirmRename(ns) {
  const to = renameTo.value.trim()
  if (!to || to === ns.name) return
  busy.value = true
  error.value = ''
  notice.value = ''
  try {
    const res = await memoryClient.renameNamespace({ from: ns.name, to })
    notice.value = `Renamed "${ns.name}" → "${to}" (${Number(res.memoriesUpdated)} memories, ${Number(res.summariesUpdated)} summaries).`
    cancelRename()
    await reload()
  } catch (e) {
    handleError(e)
  } finally {
    busy.value = false
  }
}

async function remove(ns) {
  // Bulk irreversible delete — require typing the namespace name to confirm.
  const typed = window.prompt(
    `Delete the entire "${ns.name}" namespace? This permanently removes ` +
      `${Number(ns.memoryCount)} memories and ${Number(ns.summaryCount)} summaries and cannot be undone.\n\n` +
      `Type the namespace name to confirm:`,
  )
  if (typed === null) return
  if (typed !== ns.name) {
    error.value = 'Confirmation text did not match — namespace not deleted.'
    return
  }
  busy.value = true
  error.value = ''
  notice.value = ''
  try {
    const res = await memoryClient.deleteNamespace({ namespace: ns.name })
    notice.value = `Deleted "${ns.name}" (${Number(res.memoriesDeleted)} memories, ${Number(res.summariesDeleted)} summaries).`
    await reload()
  } catch (e) {
    handleError(e)
  } finally {
    busy.value = false
  }
}

async function reload() {
  loading.value = true
  error.value = ''
  try {
    const res = await memoryClient.listNamespaces({})
    namespaces.value = res.namespaces
  } catch (e) {
    if (handleError(e)) return
  } finally {
    loading.value = false
  }
}

onMounted(reload)
</script>
