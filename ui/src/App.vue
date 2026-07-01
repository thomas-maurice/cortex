<template>
  <div>
    <nav v-if="auth.checkAuth()" class="navbar navbar-expand navbar-dark bg-dark px-3">
      <router-link class="navbar-brand" :to="{ name: 'memories' }">
        <font-awesome-icon :icon="['fas', 'brain']" class="me-2" />Cortex
      </router-link>
      <ul class="navbar-nav me-auto">
        <!-- Core memory work — kept top-level since it's used constantly. -->
        <li class="nav-item">
          <router-link class="nav-link" :to="{ name: 'memories' }">Memories</router-link>
        </li>
        <li class="nav-item">
          <router-link class="nav-link" :to="{ name: 'graph' }">Graph</router-link>
        </li>
        <li class="nav-item">
          <router-link class="nav-link" :to="{ name: 'explore' }">Explore</router-link>
        </li>

        <!-- Organize: occasional data-management views. -->
        <li class="nav-item dropdown">
          <a
            class="nav-link dropdown-toggle"
            :class="{ active: organizeRoutes.includes($route.name) }"
            href="#"
            role="button"
            data-bs-toggle="dropdown"
            aria-expanded="false"
          >Organize</a>
          <ul class="dropdown-menu">
            <li><router-link class="dropdown-item" :to="{ name: 'sessions' }">Sessions</router-link></li>
            <li><router-link class="dropdown-item" :to="{ name: 'namespaces' }">Namespaces</router-link></li>
            <li><router-link class="dropdown-item" :to="{ name: 'preferences' }">Preferences</router-link></li>
            <li><router-link class="dropdown-item" :to="{ name: 'backup' }">Backup</router-link></li>
          </ul>
        </li>

        <!-- System: operational / admin views, rarely visited. Status is always
             present so this dropdown always renders; the other items are gated. -->
        <li class="nav-item dropdown">
          <a
            class="nav-link dropdown-toggle"
            :class="{ active: systemRoutes.includes($route.name) }"
            href="#"
            role="button"
            data-bs-toggle="dropdown"
            aria-expanded="false"
          >System</a>
          <ul class="dropdown-menu">
            <li><router-link class="dropdown-item" :to="{ name: 'status' }">Status</router-link></li>
            <!-- Indexing / Queue: admin-only in MT mode, always in single-user mode.
                 The server enforces this regardless (Dead + IndexQueue are admin-only
                 when MT is on). -->
            <li v-if="!auth.multiTenant || auth.isAdmin">
              <router-link class="dropdown-item" :to="{ name: 'queue' }">Indexing</router-link>
            </li>
            <!-- P6: API keys — MT mode only (single-user mode uses CORTEX_AUTH_TOKEN). -->
            <li v-if="auth.multiTenant">
              <router-link class="dropdown-item" :to="{ name: 'apikeys' }">API Keys</router-link>
            </li>
            <!-- P5: user management — admin-only, MT mode only. -->
            <li v-if="auth.multiTenant && auth.isAdmin">
              <router-link class="dropdown-item" :to="{ name: 'users' }">Users</router-link>
            </li>
          </ul>
        </li>
      </ul>

      <!-- Docs sits apart on the right — a one-time reference, not part of the
           daily flow. -->
      <ul class="navbar-nav">
        <li class="nav-item">
          <router-link class="nav-link" :to="{ name: 'documentation' }">
            <font-awesome-icon :icon="['fas', 'book']" class="me-1" />Docs
          </router-link>
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

// Route names grouped under each nav dropdown, so the toggle shows an active
// state when one of its children is the current route.
const organizeRoutes = ['sessions', 'namespaces', 'preferences', 'backup']
const systemRoutes = ['status', 'queue', 'apikeys', 'users']

function logout() {
  auth.logout()
  router.push({ name: 'login' })
}
</script>
