import { createPromiseClient, ConnectError, Code } from '@connectrpc/connect'
import { createConnectTransport } from '@connectrpc/connect-web'
import { MemoryService } from '@/gen/cortex/v1/cortex_connect'
import { useAuthStore } from '@/stores/auth'
import router from '@/router'

// One origin in both dev and prod: in dev the vite proxy forwards /cortex.v1 to
// the Go server, in prod the SPA is served by that same server.
const transport = createConnectTransport({
  baseUrl: window.location.origin,
  interceptors: [
    // Attach JWT to outgoing requests.
    (next) => async (req) => {
      const auth = useAuthStore()
      if (auth.token) {
        req.header.set('Authorization', `Bearer ${auth.token}`)
      }
      return next(req)
    },
    // Handle Unauthenticated errors globally: clear auth state and redirect to
    // /login (preserving the intended destination), unless we are already on
    // /login to avoid redirect loops. Always rethrows so callers' own error
    // handling still runs.
    (next) => async (req) => {
      try {
        return await next(req)
      } catch (err) {
        if (
          err instanceof ConnectError &&
          err.code === Code.Unauthenticated &&
          router.currentRoute.value.name !== 'login'
        ) {
          useAuthStore().logout()
          router.push({
            name: 'login',
            query: { redirect: router.currentRoute.value.fullPath },
          })
        }
        throw err
      }
    },
  ],
})

export const memoryClient = createPromiseClient(MemoryService, transport)
