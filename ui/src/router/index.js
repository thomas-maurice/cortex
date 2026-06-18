import { createRouter, createWebHistory } from 'vue-router'
import { useAuthStore } from '@/stores/auth'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/login',
      name: 'login',
      component: () => import('@/views/LoginView.vue'),
      meta: { requiresAuth: false },
    },
    {
      path: '/',
      name: 'memories',
      component: () => import('@/views/MemoriesView.vue'),
      meta: { requiresAuth: true },
    },
    {
      path: '/graph',
      name: 'graph',
      component: () => import('@/views/GraphView.vue'),
      meta: { requiresAuth: true },
    },
    {
      path: '/explore',
      name: 'explore',
      component: () => import('@/views/ExploreView.vue'),
      meta: { requiresAuth: true },
    },
    {
      path: '/sessions',
      name: 'sessions',
      component: () => import('@/views/SessionsView.vue'),
      meta: { requiresAuth: true },
    },
    {
      path: '/preferences',
      name: 'preferences',
      component: () => import('@/views/PreferencesView.vue'),
      meta: { requiresAuth: true },
    },
    {
      path: '/queue',
      name: 'queue',
      component: () => import('@/views/QueueView.vue'),
      meta: { requiresAuth: true },
    },
    {
      path: '/status',
      name: 'status',
      component: () => import('@/views/StatusView.vue'),
      meta: { requiresAuth: true },
    },
  ],
})

router.beforeEach((to) => {
  const auth = useAuthStore()
  const requiresAuth = to.meta.requiresAuth !== false
  if (requiresAuth && !auth.checkAuth()) {
    return { name: 'login', query: { redirect: to.fullPath } }
  }
  if (to.name === 'login' && auth.checkAuth()) {
    return { name: 'memories' }
  }
})

export default router
