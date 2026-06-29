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
        <li class="nav-item">
          <router-link class="nav-link" :to="{ name: 'queue' }">Indexing</router-link>
        </li>
        <li class="nav-item">
          <router-link class="nav-link" :to="{ name: 'status' }">Status</router-link>
        </li>
      </ul>
      <span class="navbar-text me-3 small">{{ auth.username }}</span>
      <button class="btn btn-outline-light btn-sm" @click="logout">
        <font-awesome-icon :icon="['fas', 'right-from-bracket']" class="me-1" />Logout
      </button>
    </nav>

    <main class="container-fluid py-4">
      <router-view />
    </main>
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
