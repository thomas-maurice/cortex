<template>
  <div>
    <h4 class="mb-3"><font-awesome-icon :icon="['fas', 'users']" class="me-2" />Users</h4>

    <div v-if="error" class="alert alert-danger py-2">{{ error }}</div>
    <div v-if="notice" class="alert alert-success py-2">{{ notice }}</div>

    <!-- Create user form -->
    <div class="card mb-3">
      <div class="card-header py-2">Create user</div>
      <div class="card-body">
        <div class="row g-2 align-items-end">
          <div class="col-auto">
            <label class="form-label small mb-1">Username</label>
            <input v-model="newUser.username" class="form-control form-control-sm" placeholder="username" :disabled="busy" />
          </div>
          <div class="col-auto">
            <label class="form-label small mb-1">Password</label>
            <input v-model="newUser.password" type="password" class="form-control form-control-sm" placeholder="password" :disabled="busy" />
          </div>
          <div class="col-auto">
            <label class="form-label small mb-1">Role</label>
            <select v-model="newUser.role" class="form-select form-select-sm" :disabled="busy">
              <option value="user">user</option>
              <option value="admin">admin</option>
            </select>
          </div>
          <div class="col-auto">
            <button
              class="btn btn-primary btn-sm"
              :disabled="busy || !newUser.username.trim() || !newUser.password"
              @click="createUser"
            >
              <font-awesome-icon :icon="['fas', 'user-plus']" class="me-1" />Create
            </button>
          </div>
        </div>
      </div>
    </div>

    <div class="d-flex align-items-center gap-2 mb-3">
      <button class="btn btn-primary btn-sm" :disabled="loading" @click="reload">
        <font-awesome-icon :icon="['fas', 'rotate']" class="me-1" />Refresh
      </button>
    </div>

    <div v-if="loading" class="text-center text-muted py-5">
      <font-awesome-icon :icon="['fas', 'spinner']" spin size="2x" />
    </div>

    <div v-else-if="users.length === 0" class="text-center text-muted py-5">
      <font-awesome-icon :icon="['fas', 'users']" size="2x" class="mb-2 d-block" />
      No users yet.
    </div>

    <table v-else class="table table-sm align-middle">
      <thead>
        <tr>
          <th>Username</th>
          <th>Role</th>
          <th>Created</th>
          <th class="text-end" style="width: 1%">Actions</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="u in users" :key="u.id">
          <td class="font-monospace">{{ u.username }}</td>
          <td>
            <span :class="u.role === 'admin' ? 'badge bg-warning text-dark' : 'badge bg-secondary'">
              {{ u.role }}
            </span>
          </td>
          <td class="small text-muted">{{ u.createdAt ? formatTimestamp(u.createdAt) : '—' }}</td>
          <td class="text-end text-nowrap">
            <!-- Reset password -->
            <button
              class="btn btn-outline-secondary btn-sm me-1"
              title="Reset password"
              :disabled="busy"
              @click="startResetPassword(u)"
            >
              <font-awesome-icon :icon="['fas', 'key']" />
            </button>
            <!-- Toggle role -->
            <button
              class="btn btn-outline-secondary btn-sm me-1"
              :title="u.role === 'admin' ? 'Demote to user' : 'Promote to admin'"
              :disabled="busy"
              @click="toggleRole(u)"
            >
              <font-awesome-icon :icon="u.role === 'admin' ? ['fas', 'arrow-down'] : ['fas', 'arrow-up']" />
            </button>
            <!-- Delete -->
            <button
              class="btn btn-outline-danger btn-sm"
              title="Delete user"
              :disabled="busy"
              @click="deleteUser(u)"
            >
              <font-awesome-icon :icon="['fas', 'trash']" />
            </button>
          </td>
        </tr>
      </tbody>
    </table>

    <!-- Reset password modal (inline) -->
    <div v-if="resetTarget" class="modal d-block" tabindex="-1" style="background: rgba(0,0,0,.4)">
      <div class="modal-dialog modal-dialog-centered">
        <div class="modal-content">
          <div class="modal-header py-2">
            <h6 class="modal-title">Reset password for <strong>{{ resetTarget.username }}</strong></h6>
            <button type="button" class="btn-close" @click="cancelReset" :disabled="busy"></button>
          </div>
          <div class="modal-body">
            <input
              v-model="resetPassword"
              type="password"
              class="form-control"
              placeholder="New password"
              :disabled="busy"
              @keyup.enter="confirmReset"
            />
          </div>
          <div class="modal-footer py-2">
            <button class="btn btn-secondary btn-sm" @click="cancelReset" :disabled="busy">Cancel</button>
            <button
              class="btn btn-primary btn-sm"
              :disabled="busy || !resetPassword"
              @click="confirmReset"
            >
              Reset
            </button>
          </div>
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
import { formatTimestamp } from '@/utils/text'

const router = useRouter()
const auth = useAuthStore()

const users = ref([])
const loading = ref(false)
const busy = ref(false)
const error = ref('')
const notice = ref('')

const newUser = ref({ username: '', password: '', role: 'user' })

const resetTarget = ref(null)
const resetPassword = ref('')

function handleError(e) {
  if (e instanceof ConnectError && e.code === Code.Unauthenticated) {
    auth.logout()
    router.push({ name: 'login' })
    return true
  }
  if (e instanceof ConnectError && e.code === Code.PermissionDenied) {
    router.push({ name: 'memories' })
    return true
  }
  error.value = e.message || 'Request failed'
  return false
}

async function reload() {
  loading.value = true
  error.value = ''
  try {
    const res = await memoryClient.listUsers({})
    users.value = res.users
  } catch (e) {
    if (handleError(e)) return
  } finally {
    loading.value = false
  }
}

async function createUser() {
  if (!newUser.value.username.trim() || !newUser.value.password) return
  busy.value = true
  error.value = ''
  notice.value = ''
  try {
    await memoryClient.createUser({
      username: newUser.value.username.trim(),
      password: newUser.value.password,
      role: newUser.value.role,
    })
    notice.value = `User "${newUser.value.username.trim()}" created.`
    newUser.value = { username: '', password: '', role: 'user' }
    await reload()
  } catch (e) {
    handleError(e)
  } finally {
    busy.value = false
  }
}

async function deleteUser(u) {
  if (!window.confirm(`Delete user "${u.username}"? This removes their API keys and all their memories.`)) return
  busy.value = true
  error.value = ''
  notice.value = ''
  try {
    await memoryClient.deleteUser({ username: u.username })
    notice.value = `User "${u.username}" deleted.`
    await reload()
  } catch (e) {
    handleError(e)
  } finally {
    busy.value = false
  }
}

async function toggleRole(u) {
  const newRole = u.role === 'admin' ? 'user' : 'admin'
  const action = newRole === 'admin' ? 'promote to admin' : 'demote to user'
  if (!window.confirm(`${action} "${u.username}"?`)) return
  busy.value = true
  error.value = ''
  notice.value = ''
  try {
    await memoryClient.setUserRole({ username: u.username, role: newRole })
    notice.value = `Role updated for "${u.username}".`
    await reload()
  } catch (e) {
    handleError(e)
  } finally {
    busy.value = false
  }
}

function startResetPassword(u) {
  error.value = ''
  notice.value = ''
  resetTarget.value = u
  resetPassword.value = ''
}

function cancelReset() {
  resetTarget.value = null
  resetPassword.value = ''
}

async function confirmReset() {
  if (!resetPassword.value) return
  busy.value = true
  error.value = ''
  try {
    await memoryClient.resetUserPassword({
      username: resetTarget.value.username,
      newPassword: resetPassword.value,
    })
    notice.value = `Password reset for "${resetTarget.value.username}".`
    cancelReset()
  } catch (e) {
    handleError(e)
  } finally {
    busy.value = false
  }
}

onMounted(reload)
</script>
