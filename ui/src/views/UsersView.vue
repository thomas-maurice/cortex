<template>
  <div class="space-y-4">
    <h4 class="flex items-center gap-2 text-lg font-semibold"><Users class="size-5" />Users</h4>

    <Alert v-if="error" variant="destructive"><AlertDescription>{{ error }}</AlertDescription></Alert>
    <p v-if="notice" class="text-sm text-emerald-600 dark:text-emerald-400">{{ notice }}</p>

    <!-- Create user form -->
    <Card>
      <CardHeader class="py-2">
        <CardTitle class="text-sm">Create user</CardTitle>
      </CardHeader>
      <CardContent>
        <div class="flex flex-wrap items-end gap-2">
          <div class="grid gap-1.5">
            <Label class="text-xs">Username</Label>
            <Input v-model="newUser.username" class="h-8" placeholder="username" :disabled="busy" />
          </div>
          <div class="grid gap-1.5">
            <Label class="text-xs">Password</Label>
            <Input v-model="newUser.password" type="password" class="h-8" placeholder="password" :disabled="busy" />
          </div>
          <div class="grid gap-1.5">
            <Label class="text-xs">Role</Label>
            <Select v-model="newUser.role" :disabled="busy">
              <SelectTrigger size="sm">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="user">user</SelectItem>
                <SelectItem value="admin">admin</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <Button
            size="sm"
            :disabled="busy || !newUser.username.trim() || !newUser.password"
            @click="createUser"
          >
            <UserPlus class="size-4" />Create
          </Button>
        </div>
      </CardContent>
    </Card>

    <div class="flex items-center gap-2">
      <Button size="sm" :disabled="loading" @click="reload">
        <RotateCw class="size-4" />Refresh
      </Button>
    </div>

    <div v-if="loading" class="py-16 text-center text-muted-foreground">
      <Loader2 class="mx-auto size-8 animate-spin" />
    </div>

    <div v-else-if="users.length === 0" class="py-16 text-center text-muted-foreground">
      <Users class="mx-auto mb-2 size-8" />
      No users yet.
    </div>

    <div v-else class="rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Username</TableHead>
            <TableHead>Role</TableHead>
            <TableHead>Created</TableHead>
            <TableHead class="text-right">Actions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          <TableRow v-for="u in users" :key="u.id">
            <TableCell class="font-mono">{{ u.username }}</TableCell>
            <TableCell>
              <Badge v-if="u.role === 'admin'" variant="secondary" class="text-amber-600 dark:text-amber-400">{{ u.role }}</Badge>
              <Badge v-else variant="secondary">{{ u.role }}</Badge>
            </TableCell>
            <TableCell class="text-sm text-muted-foreground">{{ u.createdAt ? formatTimestamp(u.createdAt) : '—' }}</TableCell>
            <TableCell class="text-right">
              <div class="inline-flex gap-1">
                <!-- Reset password -->
                <Button
                  variant="outline"
                  size="icon"
                  class="size-8"
                  title="Reset password"
                  aria-label="Reset password"
                  :disabled="busy"
                  @click="startResetPassword(u)"
                >
                  <KeyRound class="size-3.5" />
                </Button>
                <!-- Toggle role -->
                <Button
                  variant="outline"
                  size="icon"
                  class="size-8"
                  :title="u.role === 'admin' ? 'Demote to user' : 'Promote to admin'"
                  :aria-label="u.role === 'admin' ? 'Demote to user' : 'Promote to admin'"
                  :disabled="busy"
                  @click="toggleRole(u)"
                >
                  <ArrowDown v-if="u.role === 'admin'" class="size-3.5" />
                  <ArrowUp v-else class="size-3.5" />
                </Button>
                <!-- Delete -->
                <Button
                  variant="outline"
                  size="icon"
                  class="size-8 text-destructive hover:text-destructive"
                  title="Delete user"
                  aria-label="Delete user"
                  :disabled="busy"
                  @click="deleteUser(u)"
                >
                  <Trash2 class="size-3.5" />
                </Button>
              </div>
            </TableCell>
          </TableRow>
        </TableBody>
      </Table>
    </div>

    <!-- Reset password modal (inline) -->
    <Dialog v-model:open="resetOpen">
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Reset password for <strong>{{ resetTarget?.username }}</strong></DialogTitle>
        </DialogHeader>
        <Input
          v-model="resetPassword"
          type="password"
          placeholder="New password"
          :disabled="busy"
          @keyup.enter="confirmReset"
        />
        <DialogFooter>
          <Button variant="outline" size="sm" @click="cancelReset" :disabled="busy">Cancel</Button>
          <Button
            size="sm"
            :disabled="busy || !resetPassword"
            @click="confirmReset"
          >
            Reset
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { memoryClient } from '@/utils/connect'
import { useAuthStore } from '@/stores/auth'
import { formatTimestamp } from '@/utils/text'
import { ArrowDown, ArrowUp, KeyRound, Loader2, RotateCw, Trash2, UserPlus, Users } from 'lucide-vue-next'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

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

// Bridges the existing resetTarget-as-visibility-flag pattern to Dialog's
// boolean v-model:open, without introducing a separate visibility ref.
const resetOpen = computed({
  get: () => !!resetTarget.value,
  set: (v) => {
    if (!v) cancelReset()
  },
})

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
