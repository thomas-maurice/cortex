<template>
  <div class="flex items-center justify-center" style="min-height: 80vh">
    <Card class="w-full max-w-sm shadow-lg">
      <CardHeader class="text-center">
        <Brain class="mx-auto mb-2 size-10 text-primary" />
        <CardTitle class="text-xl">Cortex</CardTitle>
        <CardDescription>Sign in to your second brain</CardDescription>
      </CardHeader>
      <CardContent>
        <Alert v-if="error" variant="destructive" class="mb-4">
          <AlertDescription>{{ error }}</AlertDescription>
        </Alert>

        <form class="grid gap-4" @submit.prevent="handleLogin">
          <div class="grid gap-2">
            <Label for="username">Username</Label>
            <Input id="username" v-model="username" type="text" required :disabled="loading" />
          </div>
          <div class="grid gap-2">
            <Label for="password">Password</Label>
            <Input id="password" v-model="password" type="password" required :disabled="loading" />
          </div>
          <Button type="submit" class="w-full" :disabled="loading">
            <Loader2 v-if="loading" class="size-4 animate-spin" />
            {{ loading ? 'Signing in...' : 'Sign In' }}
          </Button>
        </form>
      </CardContent>
    </Card>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { useAuthStore } from '@/stores/auth'
import { memoryClient } from '@/utils/connect'
import { login } from '@/utils/api'
import { Brain, Loader2 } from 'lucide-vue-next'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Alert, AlertDescription } from '@/components/ui/alert'

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
