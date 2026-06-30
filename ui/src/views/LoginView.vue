<template>
  <div class="d-flex justify-content-center align-items-center" style="min-height: 80vh">
    <div class="card shadow-lg" style="width: 100%; max-width: 400px">
      <div class="card-body p-5">
        <div class="text-center mb-4">
          <font-awesome-icon :icon="['fas', 'brain']" size="3x" class="text-primary mb-3" />
          <h3 class="card-title">Cortex</h3>
          <p class="text-muted">Sign in to your second brain</p>
        </div>

        <div v-if="error" class="alert alert-danger" role="alert">{{ error }}</div>

        <form @submit.prevent="handleLogin">
          <div class="mb-3">
            <label for="username" class="form-label">Username</label>
            <input id="username" v-model="username" type="text" class="form-control" required :disabled="loading" />
          </div>
          <div class="mb-4">
            <label for="password" class="form-label">Password</label>
            <input id="password" v-model="password" type="password" class="form-control" required :disabled="loading" />
          </div>
          <button type="submit" class="btn btn-primary w-100" :disabled="loading">
            <span v-if="loading" class="spinner-border spinner-border-sm me-2"></span>
            {{ loading ? 'Signing in...' : 'Sign In' }}
          </button>
        </form>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { useAuthStore } from '@/stores/auth'
import { memoryClient } from '@/utils/connect'
import { login } from '@/utils/api'

const router = useRouter()
const route = useRoute()
const auth = useAuthStore()

const username = ref('')
const password = ref('')
const loading = ref(false)
const error = ref('')

async function handleLogin() {
  error.value = ''
  loading.value = true
  try {
    const token = await login(username.value, password.value)
    auth.login(token)
    // Probe whether CORTEX_MULTI_TENANT is on: listApiKeys returns
    // FailedPrecondition when MT is disabled; any other response means MT is on.
    // This sets auth.multiTenant so the nav shows/hides Users + API Keys.
    // The probe fires once after login; the result is persisted in localStorage.
    try {
      await memoryClient.listApiKeys({})
      auth.setMultiTenant(true)
    } catch (probeErr) {
      const isFP = probeErr instanceof ConnectError && probeErr.code === Code.FailedPrecondition
      auth.setMultiTenant(!isFP)
    }
    router.push(route.query.redirect || '/')
  } catch (e) {
    error.value = e.response?.status === 401 ? 'Invalid credentials' : 'Login failed'
  } finally {
    loading.value = false
  }
}
</script>
