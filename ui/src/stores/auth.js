import { defineStore } from 'pinia'
import { jwtDecode } from 'jwt-decode'

// Mirrors the nis auth store: the JWT is the single source of truth, decoded for
// username/role. Persisted to localStorage so a refresh keeps you logged in.
// NOTE: jwtDecode does NOT verify the signature — it only decodes the payload
// for display purposes. The server validates every request and is the sole trust
// authority; never treat a client-side decode as authentication.
export const useAuthStore = defineStore('auth', {
  state: () => ({
    token: '',
    username: '',
    role: '',
    isAdmin: false,
    loggedIn: false,
    // multiTenant is true when the server has CORTEX_MULTI_TENANT=true. It is
    // probed at login via a listApiKeys call: FailedPrecondition → false, any
    // other response → true. Controls visibility of the Users and API Keys nav
    // entries. Persisted so it survives a page refresh without an extra probe.
    multiTenant: false,
  }),

  actions: {
    login(token) {
      this.logout()
      this.token = token
      this.loggedIn = true
      try {
        const decoded = jwtDecode(token)
        this.username = decoded.username || ''
        this.role = decoded.role || ''
        this.isAdmin = decoded.role === 'admin'
      } catch (e) {
        console.error('failed to decode JWT', e)
        this.logout()
      }
    },

    // setMultiTenant is called after login to cache the MT flag. The caller
    // (LoginView) probes listApiKeys once; FailedPrecondition = MT off.
    setMultiTenant(enabled) {
      this.multiTenant = enabled
    },

    logout() {
      this.$reset()
    },

    checkAuth() {
      if (!this.loggedIn || this.token === '') return false
      try {
        const decoded = jwtDecode(this.token)
        if (decoded.exp !== undefined && decoded.exp < Date.now() / 1000) {
          this.logout()
          return false
        }
      } catch (e) {
        this.logout()
        return false
      }
      return true
    },
  },

  persist: {
    storage: localStorage,
    paths: ['token', 'username', 'role', 'isAdmin', 'loggedIn', 'multiTenant'],
  },
})
