<template>
  <div>
    <nav v-if="auth.checkAuth()" class="navbar navbar-expand navbar-dark bg-dark px-3">
      <router-link class="navbar-brand" :to="{ name: 'memories' }">
        <font-awesome-icon :icon="['fas', 'brain']" class="me-2" />Cortex
      </router-link>
      <ul class="navbar-nav me-auto">
        <li class="nav-item">
          <router-link class="nav-link" :to="{ name: 'memories' }">Memories</router-link>
        </li>
        <li class="nav-item">
          <router-link class="nav-link" :to="{ name: 'graph' }">Graph</router-link>
        </li>
        <li class="nav-item">
          <router-link class="nav-link" :to="{ name: 'explore' }">Explore</router-link>
        </li>
        <li class="nav-item">
          <router-link class="nav-link" :to="{ name: 'sessions' }">Sessions</router-link>
        </li>
        <li class="nav-item">
          <router-link class="nav-link" :to="{ name: 'namespaces' }">Namespaces</router-link>
        </li>
        <li class="nav-item">
          <router-link class="nav-link" :to="{ name: 'preferences' }">Preferences</router-link>
        </li>
        <li class="nav-item">
          <router-link class="nav-link" :to="{ name: 'backup' }">Backup</router-link>
        </li>
        <!-- Indexing / Queue is a shared-queue concern (global across all
             tenants) so it is shown only to admins in MT mode. In single-user
             mode (multiTenant=false) it always shows. The server enforces this
             regardless (Dead + IndexQueue are admin-only when MT is on). -->
        <li v-if="!auth.multiTenant || auth.isAdmin" class="nav-item">
          <router-link class="nav-link" :to="{ name: 'queue' }">Indexing</router-link>
        </li>
        <li class="nav-item">
          <router-link class="nav-link" :to="{ name: 'status' }">Status</router-link>
        </li>
        <li class="nav-item">
          <router-link class="nav-link" :to="{ name: 'documentation' }">Docs</router-link>
        </li>
        <!-- P6: API keys — shown only when multi-tenancy is enabled (single-user
             mode uses CORTEX_AUTH_TOKEN, not per-user keys). -->
        <li v-if="auth.multiTenant" class="nav-item">
          <router-link class="nav-link" :to="{ name: 'apikeys' }">API Keys</router-link>
        </li>
        <!-- P5: user management — admin-only, MT mode only. -->
        <li v-if="auth.multiTenant && auth.isAdmin" class="nav-item">
          <router-link class="nav-link" :to="{ name: 'users' }">Users</router-link>
        </li>
      </ul>
      <!-- P7: show the authenticated username so users know who they are. -->
      <span v-if="auth.username" class="navbar-text me-3 small text-muted">
        <font-awesome-icon :icon="['fas', 'user']" class="me-1" />{{ auth.username }}
        <span v-if="auth.isAdmin" class="badge bg-warning text-dark ms-1 small">admin</span>
      </span>
      <button class="btn btn-outline-light btn-sm" @click="logout">
        <font-awesome-icon :icon="['fas', 'right-from-bracket']" class="me-1" />Logout
      </button>
    </nav>

    <main class="container-fluid py-4">
      <router-view />
    </main>

    <footer class="text-center text-muted small py-3">
      <a
        href="https://github.com/thomas-maurice/cortex"
        target="_blank"
        rel="noopener noreferrer"
        class="text-muted text-decoration-none"
      >
        <font-awesome-icon :icon="['fab', 'github']" class="me-1" />thomas-maurice/cortex
      </a>
    </footer>
  </div>
</template>

<script setup>
import { useRouter } from 'vue-router'
import { useAuthStore } from '@/stores/auth'

const auth = useAuthStore()
const router = useRouter()

function logout() {
  auth.logout()
  router.push({ name: 'login' })
}
</script>
