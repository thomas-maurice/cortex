<template>
  <div class="space-y-4">
    <h2 class="flex items-center gap-2 text-xl font-semibold tracking-tight">
      <Database class="size-5" />Storage
      <Badge v-if="info?.scoped" variant="secondary">Your data only</Badge>
    </h2>

    <Alert v-if="error" variant="destructive">
      <AlertDescription>{{ error }}</AlertDescription>
    </Alert>

    <div v-if="info" class="grid gap-2 md:grid-cols-4">
      <Card v-for="n in info.nodes" :key="n.name">
        <CardContent class="py-3 text-sm">
          <div class="font-medium">Weaviate {{ n.version }}</div>
          <div class="text-muted-foreground">
            {{ n.name }} · {{ n.status }} · {{ n.operationalMode }} · {{ Number(n.shardCount) }} shards
          </div>
        </CardContent>
      </Card>
      <Card>
        <CardContent class="py-3 text-sm">
          <div class="font-medium">Schema v{{ info.schemaVersion }}</div>
          <Badge v-if="info.maintenance" variant="destructive">maintenance</Badge>
          <span v-else class="text-muted-foreground">no migration running</span>
        </CardContent>
      </Card>
      <Card>
        <CardContent class="py-3 text-sm">
          <div class="font-medium">{{ info.embedModel || 'embedder' }}</div>
          <div class="text-muted-foreground">
            {{ info.embedDims > 0 ? info.embedDims + ' dims' : 'dims unavailable (probe failed)' }}
          </div>
        </CardContent>
      </Card>
    </div>

    <Card v-if="info && info.orphanClasses.length" class="border-amber-500">
      <CardContent class="space-y-1 py-3 text-sm">
        <div class="flex items-center gap-2 font-medium text-amber-600 dark:text-amber-400">
          <TriangleAlert class="size-4" />Orphan classes
        </div>
        <div class="text-muted-foreground">
          No alias points at these: leftover from an interrupted migration?
        </div>
        <div class="flex flex-wrap gap-1">
          <Badge v-for="o in info.orphanClasses" :key="o" variant="outline">{{ o }}</Badge>
        </div>
      </CardContent>
    </Card>

    <div class="flex items-center gap-2">
      <Button size="sm" :disabled="loading" @click="reload()">
        <RotateCw class="size-4" />Refresh
      </Button>
    </div>

    <div v-if="info" class="rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Class</TableHead>
            <TableHead>Physical</TableHead>
            <TableHead>Objects</TableHead>
            <TableHead>Tenants</TableHead>
            <TableHead>Index</TableHead>
            <TableHead>Stored dims</TableHead>
            <TableHead>Indexing</TableHead>
            <TableHead>Queue</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          <template v-for="c in info.classes" :key="c.logical">
            <TableRow>
              <TableCell class="font-medium">
                <button
                  v-if="c.multiTenant && c.tenants.length"
                  class="inline-flex items-center gap-1"
                  @click="toggle(c.logical)"
                >
                  <ChevronDown v-if="expanded.has(c.logical)" class="size-4" />
                  <ChevronRight v-else class="size-4" />{{ c.logical }}
                </button>
                <span v-else>{{ c.logical }}</span>
              </TableCell>
              <TableCell>
                <Badge variant="secondary">{{ c.aliased ? c.logical + ' → ' + c.physical : c.physical + ' (plain)' }}</Badge>
              </TableCell>
              <TableCell>{{ count(c.objectCount) }}</TableCell>
              <TableCell>{{ c.multiTenant ? c.tenantCount : '—' }}</TableCell>
              <TableCell class="text-muted-foreground">
                {{ c.vectorIndexType ? c.vectorIndexType + (c.distance ? ' / ' + c.distance : '') : '—' }}
              </TableCell>
              <TableCell>
                <template v-if="hasVectors(c)">
                  <span v-if="c.storedDims > 0">{{ c.storedDims }}</span>
                  <span v-else class="text-muted-foreground">—</span>
                  <Badge v-if="mismatch(c)" variant="destructive" class="ml-1">model mismatch</Badge>
                </template>
                <span v-else class="text-muted-foreground">—</span>
              </TableCell>
              <TableCell>
                <Badge v-if="c.indexingStatus" :variant="statusVariant(c.indexingStatus)">{{ c.indexingStatus }}</Badge>
                <span v-else class="text-muted-foreground">—</span>
              </TableCell>
              <TableCell>{{ Number(c.vectorQueueLength) }}</TableCell>
            </TableRow>
            <TableRow v-if="expanded.has(c.logical)" class="bg-muted/30">
              <TableCell colspan="8">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Tenant</TableHead>
                      <TableHead>Activity</TableHead>
                      <TableHead>Objects</TableHead>
                      <TableHead>Indexing</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    <TableRow v-for="t in c.tenants" :key="t.tenant">
                      <TableCell>{{ t.username || t.tenant }}</TableCell>
                      <TableCell>{{ t.activity || '—' }}</TableCell>
                      <TableCell>{{ count(t.objectCount) }}</TableCell>
                      <TableCell>
                        <Badge v-if="t.indexingStatus" :variant="statusVariant(t.indexingStatus)">{{ t.indexingStatus }}</Badge>
                        <span v-else class="text-muted-foreground">—</span>
                      </TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </TableCell>
            </TableRow>
          </template>
        </TableBody>
      </Table>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { memoryClient } from '@/utils/connect'
import { useAuthStore } from '@/stores/auth'
import { ChevronDown, ChevronRight, Database, RotateCw, TriangleAlert } from 'lucide-vue-next'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

const router = useRouter()
const auth = useAuthStore()

const info = ref(null)
const loading = ref(false)
const error = ref('')
const expanded = ref(new Set())

// Classes that store vectors (and so have meaningful stored dims).
const VECTOR_CLASSES = ['Memory', 'MemoryChunk', 'ConversationSummary']
const hasVectors = (c) => VECTOR_CLASSES.includes(c.logical)

// int64 fields arrive as bigint; -1 means the source was unavailable.
function count(v) {
  const n = Number(v)
  return n < 0 ? 'unavailable' : n.toLocaleString()
}

function mismatch(c) {
  return info.value.embedDims > 0 && c.storedDims > 0 && c.storedDims !== info.value.embedDims
}

function statusVariant(s) {
  return s === 'READY' ? 'secondary' : 'destructive'
}

function toggle(name) {
  const next = new Set(expanded.value)
  if (!next.delete(name)) next.add(name)
  expanded.value = next
}

async function reload() {
  loading.value = true
  try {
    info.value = await memoryClient.getStorageInfo({})
    error.value = ''
  } catch (e) {
    if (e instanceof ConnectError && e.code === Code.Unauthenticated) {
      auth.logout()
      router.push({ name: 'login' })
      return
    }
    error.value = e.message || 'Request failed'
  } finally {
    loading.value = false
  }
}

onMounted(reload)
</script>
