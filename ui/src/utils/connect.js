import { createPromiseClient } from '@connectrpc/connect'
import { createConnectTransport } from '@connectrpc/connect-web'
import { MemoryService } from '@/gen/cortex/v1/cortex_connect'
import { useAuthStore } from '@/stores/auth'

// One origin in both dev and prod: in dev the vite proxy forwards /cortex.v1 to
// the Go server, in prod the SPA is served by that same server.
const transport = createConnectTransport({
  baseUrl: window.location.origin,
  interceptors: [
    (next) => async (req) => {
      const auth = useAuthStore()
      if (auth.token) {
        req.header.set('Authorization', `Bearer ${auth.token}`)
      }
      return next(req)
    },
  ],
})

export const memoryClient = createPromiseClient(MemoryService, transport)
