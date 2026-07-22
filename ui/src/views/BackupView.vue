<template>
  <div class="space-y-4">
    <Alert>
      <Archive class="size-4" />
      <AlertDescription>
        Export dumps memories (text + metadata, <strong>no vectors</strong>) to a JSON file. Import
        re-ingests such a dump through the normal indexing queue — the worker re-embeds each one, so a
        restore is safe across embedding-model changes. The format is identical to the
        <code>cortex export</code> / <code>cortex import</code> CLI, so dumps are interchangeable.
        An existing id is overwritten (upsert).
      </AlertDescription>
    </Alert>

    <Alert v-if="error" variant="destructive">
      <AlertDescription class="whitespace-pre-wrap">{{ error }}</AlertDescription>
    </Alert>

    <!-- Export -->
    <Card class="gap-3 py-4">
      <CardHeader>
        <CardTitle class="flex items-center gap-2 text-base">
          <Download class="size-4" />Export
        </CardTitle>
      </CardHeader>
      <CardContent class="space-y-2">
        <div class="flex flex-wrap items-end gap-2">
          <div class="grid gap-1.5" style="width: 220px">
            <Label class="text-xs">Namespace</Label>
            <Input v-model="exportNs" placeholder="* = all namespaces" />
          </div>
          <Button size="sm" :disabled="exporting" @click="doExport">
            <component :is="exporting ? Loader2 : Download" :class="['size-4', exporting && 'animate-spin']" />
            {{ exporting ? 'Exporting…' : 'Export to JSON' }}
          </Button>
        </div>
      </CardContent>
    </Card>

    <!-- Import -->
    <Card class="gap-3 py-4">
      <CardHeader>
        <CardTitle class="flex items-center gap-2 text-base">
          <Upload class="size-4" />Import
        </CardTitle>
      </CardHeader>
      <CardContent class="space-y-2">
        <div class="flex flex-wrap items-end gap-2">
          <div class="grid flex-1 gap-1.5">
            <Label class="text-xs">Dump file (.json)</Label>
            <input
              ref="fileInput"
              type="file"
              accept="application/json,.json"
              class="file:text-foreground border-input h-9 w-full min-w-0 rounded-md border bg-transparent px-3 py-1 text-sm shadow-xs file:inline-flex file:h-7 file:border-0 file:bg-transparent file:text-sm file:font-medium"
              @change="onFile"
            />
          </div>
          <Button size="sm" :disabled="importing || !file" @click="doImport">
            <component :is="importing ? Loader2 : Upload" :class="['size-4', importing && 'animate-spin']" />
            {{ importing ? 'Importing…' : 'Import' }}
          </Button>
        </div>
        <p v-if="importInfo" class="text-sm text-muted-foreground">{{ importInfo }}</p>
      </CardContent>
    </Card>

    <!-- My data backup (all logged-in users) -->
    <Card class="gap-3 py-4">
      <CardHeader>
        <CardTitle class="flex items-center gap-2 text-base">
          <ShieldCheck class="size-4" />My data backup
        </CardTitle>
      </CardHeader>
      <CardContent class="space-y-3">
        <p class="text-sm text-muted-foreground">
          Server-generated backup of your own memories and conversation summaries — unlike the
          client-side export above, this is a single versioned file that includes summaries and is
          produced entirely on the server. Restore always writes into your own data; re-running is
          safe (memories are upserted by id).
        </p>

        <!-- Download my backup -->
        <div class="flex flex-wrap items-center gap-2">
          <Button size="sm" :disabled="selfBacking" @click="doBackupSelf">
            <component :is="selfBacking ? Loader2 : Download" :class="['size-4', selfBacking && 'animate-spin']" />
            {{ selfBacking ? 'Preparing…' : 'Download my backup' }}
          </Button>
        </div>

        <!-- Restore my backup -->
        <div class="grid gap-1.5">
          <Label class="text-xs">Restore from file</Label>
          <div class="flex flex-wrap items-end gap-2">
            <input
              ref="selfFileInput"
              type="file"
              class="file:text-foreground border-input h-9 w-full min-w-0 flex-1 rounded-md border bg-transparent px-3 py-1 text-sm shadow-xs file:inline-flex file:h-7 file:border-0 file:bg-transparent file:text-sm file:font-medium"
              @change="onSelfFile"
            />
            <Button variant="outline" size="sm" :disabled="selfRestoring || !selfFile" @click="doRestoreSelf">
              <component :is="selfRestoring ? Loader2 : RotateCcw" :class="['size-4', selfRestoring && 'animate-spin']" />
              {{ selfRestoring ? 'Restoring…' : 'Restore' }}
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>

    <!-- Server backups (admin only) -->
    <template v-if="auth.isAdmin">
      <Separator class="my-4" />

      <Alert>
        <Server class="size-4" />
        <AlertDescription>
          <strong>Server-side full backup</strong> — captures ALL tenants' memories, conversation summaries,
          and the user registry (users + API keys) into a single file written to the server's configured
          backup directory. Only admins can trigger or restore backups. A restore hands memories and
          summaries to the indexing worker for re-embedding; users and API keys that already exist are
          left untouched.
        </AlertDescription>
      </Alert>

      <!-- Run backup -->
      <Card class="gap-3 py-4">
        <CardHeader>
          <CardTitle class="flex items-center gap-2 text-base">
            <Archive class="size-4" />Server backups
          </CardTitle>
        </CardHeader>
        <CardContent class="space-y-3">
          <Button size="sm" :disabled="backingUp" @click="doBackupAll">
            <component :is="backingUp ? Loader2 : Save" :class="['size-4', backingUp && 'animate-spin']" />
            {{ backingUp ? 'Backing up…' : 'Run full backup now' }}
          </Button>
        </CardContent>
      </Card>

      <!-- Backup list -->
      <Card class="gap-3 py-4">
        <CardHeader>
          <CardTitle class="flex items-center gap-2 text-base">
            <List class="size-4" />Existing backups
          </CardTitle>
          <CardAction>
            <Button variant="outline" size="sm" :disabled="backupsLoading" @click="loadBackups">
              <RotateCw class="size-4" />Refresh
            </Button>
          </CardAction>
        </CardHeader>
        <CardContent class="space-y-3">
          <div v-if="backupsLoading" class="py-6 text-center text-muted-foreground">
            <Loader2 class="mx-auto size-8 animate-spin" />
          </div>

          <div v-else-if="backups.length === 0" class="py-6 text-center text-muted-foreground">
            <PackageOpen class="mx-auto mb-2 size-8" />
            No server backups yet.
          </div>

          <div v-else class="rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  <TableHead class="text-right">Size</TableHead>
                  <TableHead>Created</TableHead>
                  <TableHead class="w-px text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                <TableRow v-for="b in backups" :key="b.name">
                  <TableCell class="font-mono text-sm">{{ b.name }}</TableCell>
                  <TableCell class="text-right text-sm text-muted-foreground">{{ formatSize(b.sizeBytes) }}</TableCell>
                  <TableCell class="text-sm text-muted-foreground">{{ b.createdAt ? formatTimestamp(b.createdAt) : '—' }}</TableCell>
                  <TableCell class="text-right">
                    <Button
                      variant="outline"
                      size="icon"
                      class="size-8"
                      title="Download this backup"
                      aria-label="Download this backup"
                      :disabled="downloading"
                      @click="doDownloadBackup(b)"
                    >
                      <component :is="downloading ? Loader2 : Download" :class="['size-3.5', downloading && 'animate-spin']" />
                    </Button>
                    <Button
                      variant="outline"
                      size="icon"
                      class="ml-1 size-8"
                      title="Restore this backup"
                      aria-label="Restore this backup"
                      :disabled="restoring"
                      @click="doRestoreAll(b)"
                    >
                      <component :is="restoring ? Loader2 : RotateCcw" :class="['size-3.5', restoring && 'animate-spin']" />
                    </Button>
                    <Button
                      variant="outline"
                      size="icon"
                      class="ml-1 size-8 text-destructive hover:text-destructive"
                      title="Delete this backup"
                      aria-label="Delete this backup"
                      :disabled="deleting"
                      @click="doDeleteBackup(b)"
                    >
                      <component :is="deleting ? Loader2 : Trash2" :class="['size-3.5', deleting && 'animate-spin']" />
                    </Button>
                  </TableCell>
                </TableRow>
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>

      <!-- Offsite (S3) backups -->
      <Card class="gap-3 py-4">
        <CardHeader>
          <CardTitle class="flex items-center gap-2 text-base">
            <CloudUpload class="size-4" />Offsite (S3) backups
          </CardTitle>
          <CardAction>
            <Button variant="outline" size="sm" :disabled="s3Loading" @click="loadS3Backups">
              <RotateCw class="size-4" />Refresh
            </Button>
          </CardAction>
        </CardHeader>
        <CardContent class="space-y-3">
          <p class="text-sm text-muted-foreground">
            Backups uploaded to the server's configured S3 bucket. S3 is configured on the server
            via env vars (<code>CORTEX_S3_*</code> / <code>AWS_*</code>); restore downloads the
            object back through the server.
          </p>

          <div v-if="s3Loading" class="py-6 text-center text-muted-foreground">
            <Loader2 class="mx-auto size-8 animate-spin" />
          </div>

          <div v-else-if="s3Unavailable" class="py-6 text-center text-muted-foreground">
            <CloudUpload class="mx-auto mb-2 size-8" />
            Offsite S3 backup is not configured on the server.
          </div>

          <div v-else-if="s3Backups.length === 0" class="py-6 text-center text-muted-foreground">
            <PackageOpen class="mx-auto mb-2 size-8" />
            No backups in the S3 bucket yet.
          </div>

          <div v-else class="rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Object key</TableHead>
                  <TableHead class="text-right">Size</TableHead>
                  <TableHead>Modified</TableHead>
                  <TableHead class="w-px text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                <TableRow v-for="b in s3Backups" :key="b.name">
                  <TableCell class="font-mono text-sm">{{ b.name }}</TableCell>
                  <TableCell class="text-right text-sm text-muted-foreground">{{ formatSize(b.sizeBytes) }}</TableCell>
                  <TableCell class="text-sm text-muted-foreground">{{ b.createdAt ? formatTimestamp(b.createdAt) : '—' }}</TableCell>
                  <TableCell class="text-right">
                    <Button
                      variant="outline"
                      size="icon"
                      class="size-8"
                      title="Restore this backup from S3"
                      aria-label="Restore this backup from S3"
                      :disabled="restoring"
                      @click="doRestoreS3(b)"
                    >
                      <component :is="restoring ? Loader2 : RotateCcw" :class="['size-3.5', restoring && 'animate-spin']" />
                    </Button>
                  </TableCell>
                </TableRow>
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>

    </template>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { toast } from 'vue-sonner'
import { Timestamp } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { memoryClient } from '@/utils/connect'
import { useAuthStore } from '@/stores/auth'
import { confirmDialog } from '@/lib/confirm'
import { formatTimestamp } from '@/utils/text'
import {
  Archive,
  CloudUpload,
  Download,
  List,
  Loader2,
  PackageOpen,
  RotateCcw,
  RotateCw,
  Save,
  Server,
  ShieldCheck,
  Trash2,
  Upload,
} from 'lucide-vue-next'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Card, CardAction, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Separator } from '@/components/ui/separator'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

const router = useRouter()
const auth = useAuthStore()

// Matches the server's QUERY_MAXIMUM_RESULTS / CLI allLimit. A personal store
// stays well under this; if a dump is ever truncated we say so rather than
// silently dropping memories.
const MAX_EXPORT = 10000
const IMPORT_BATCH = 500

const error = ref('')

const exportNs = ref('*')
const exporting = ref(false)

const fileInput = ref(null)
const file = ref(null)
const importing = ref(false)
const importInfo = ref('')

// My data (self) backup state
const selfBacking = ref(false)
const selfFileInput = ref(null)
const selfFile = ref(null)
const selfRestoring = ref(false)

// Server-backup state (admin only)
const backingUp = ref(false)
const backups = ref([])
const backupsLoading = ref(false)
const restoring = ref(false)

// Offsite (S3) backups (admin). s3Unavailable is set when the server has no S3
// configured (FailedPrecondition), so we show a hint instead of an error.
const s3Backups = ref([])
const s3Loading = ref(false)
const s3Unavailable = ref(false)
const downloading = ref(false)
const deleting = ref(false)

// formatSize renders a BigInt byte count as a human-readable string.
function formatSize(bytes) {
  const n = Number(bytes)
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MB`
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GB`
}

function handleError(e) {
  if (e instanceof ConnectError && e.code === Code.Unauthenticated) {
    auth.logout()
    router.push({ name: 'login' })
    return
  }
  error.value = e.message || 'Request failed'
}

// rfc3339 renders a protobuf Timestamp as the CLI does (RFC3339, no millis).
function rfc3339(ts) {
  if (!ts) return ''
  try {
    return ts.toDate().toISOString().replace(/\.\d{3}Z$/, 'Z')
  } catch {
    return ''
  }
}

// toExportRecord mirrors the Go exportRecord JSON shape (omitting empty optional
// fields) so a UI dump is byte-compatible with `cortex export`.
function toExportRecord(m) {
  const r = { id: m.id, text: m.text, namespace: m.namespace, source: m.source, createdAt: rfc3339(m.createdAt) }
  if (m.tags?.length) r.tags = m.tags
  if (m.model) r.model = m.model
  if (m.dims) r.dims = m.dims
  if (m.conversationId) r.conversationId = m.conversationId
  if (m.linkedIds?.length) r.linkedIds = m.linkedIds
  if (m.notDuplicateOf?.length) r.notDuplicateOf = m.notDuplicateOf
  return r
}

async function doExport() {
  exporting.value = true
  error.value = ''
  try {
    const ns = exportNs.value.trim() || '*'
    const res = await memoryClient.list({ namespace: ns, limit: MAX_EXPORT })
    const recs = res.memories.map(toExportRecord)
    const json = JSON.stringify(recs, null, 2)

    const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-')
    const nsTag = ns === '*' ? 'all' : ns.replace(/[^a-zA-Z0-9._-]+/g, '_')
    download(json, `cortex-export-${nsTag}-${stamp}.json`)

    let msg = `Exported ${recs.length} mem`.concat(recs.length === 1 ? 'ory.' : 'ories.')
    if (recs.length >= MAX_EXPORT) {
      msg += ` (capped at ${MAX_EXPORT} — the store may hold more; export per-namespace to be sure.)`
    }
    toast.success(msg)
  } catch (e) {
    handleError(e)
  } finally {
    exporting.value = false
  }
}

function download(text, filename) {
  const url = URL.createObjectURL(new Blob([text], { type: 'application/json' }))
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  a.remove()
  URL.revokeObjectURL(url)
}

function downloadBytes(data, filename) {
  const url = URL.createObjectURL(new Blob([data], { type: 'application/octet-stream' }))
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  a.remove()
  URL.revokeObjectURL(url)
}

function onFile(e) {
  file.value = e.target.files?.[0] || null
  importInfo.value = file.value ? `Selected: ${file.value.name} (${(file.value.size / 1024).toFixed(1)} KB)` : ''
}

function onSelfFile(e) {
  selfFile.value = e.target.files?.[0] || null
}

// ---- My data (self) backup ----

async function doBackupSelf() {
  selfBacking.value = true
  error.value = ''
  try {
    const res = await memoryClient.backupSelf({})
    downloadBytes(res.data, res.name)
    toast.success(`Downloaded: ${res.memories} memories, ${res.summaries} summaries.`)
  } catch (e) {
    handleError(e)
  } finally {
    selfBacking.value = false
  }
}

async function doRestoreSelf() {
  if (!selfFile.value) return
  if (!(await confirmDialog(
    `Restore "${selfFile.value.name}" into your account? Existing memories are upserted by id — re-running is safe.`,
    { actionLabel: 'Restore' }
  ))) return

  selfRestoring.value = true
  error.value = ''
  try {
    const buf = await selfFile.value.arrayBuffer()
    const res = await memoryClient.restoreSelf({ data: new Uint8Array(buf) })
    toast.success(
      `Queued ${res.memoriesQueued} memories and ${res.summariesQueued} summaries for re-indexing.`
    )
    selfFile.value = null
    if (selfFileInput.value) selfFileInput.value.value = ''
  } catch (e) {
    handleError(e)
  } finally {
    selfRestoring.value = false
  }
}

// ---- Server backup (admin) ----

async function loadBackups() {
  backupsLoading.value = true
  error.value = ''
  try {
    const res = await memoryClient.listBackups({})
    backups.value = res.backups
  } catch (e) {
    handleError(e)
  } finally {
    backupsLoading.value = false
  }
}

async function doBackupAll() {
  backingUp.value = true
  error.value = ''
  try {
    const res = await memoryClient.backupAll({})
    let msg = `Backup written: ${res.path} — ${res.tenants} tenant(s), ${res.memories} memories, ` +
      `${res.summaries} summaries, ${res.users} users, ${res.apiKeys} API key(s).`
    if (res.s3Result) msg += ` Offsite: ${res.s3Result}`
    toast.success(msg)
    await loadBackups()
  } catch (e) {
    handleError(e)
  } finally {
    backingUp.value = false
  }
}

async function doDownloadBackup(b) {
  downloading.value = true
  error.value = ''
  try {
    const res = await memoryClient.downloadBackup({ name: b.name })
    downloadBytes(res.data, res.name)
  } catch (e) {
    handleError(e)
  } finally {
    downloading.value = false
  }
}

async function doRestoreAll(b) {
  if (!(await confirmDialog(
    `Restore backup "${b.name}"? Existing users and API keys are left untouched. Memories and ` +
    `summaries will be queued for re-embedding — this may take a while.`,
    { actionLabel: 'Restore' }
  ))) return

  restoring.value = true
  error.value = ''
  try {
    const res = await memoryClient.restoreAll({ name: b.name })
    toast.success(
      `Restore complete: ${res.usersCreated} user(s) created, ${res.usersSkipped} skipped; ` +
      `${res.apiKeysCreated} API key(s) created, ${res.apiKeysSkipped} skipped; ` +
      `${res.memoriesQueued} memories and ${res.summariesQueued} summaries queued for re-indexing.`
    )
  } catch (e) {
    handleError(e)
  } finally {
    restoring.value = false
  }
}

async function doDeleteBackup(b) {
  if (!(await confirmDialog(`Delete backup "${b.name}"? This cannot be undone.`, { actionLabel: 'Delete' }))) return

  deleting.value = true
  error.value = ''
  try {
    await memoryClient.deleteBackup({ name: b.name })
    await loadBackups()
  } catch (e) {
    handleError(e)
  } finally {
    deleting.value = false
  }
}

async function loadS3Backups() {
  s3Loading.value = true
  s3Unavailable.value = false
  error.value = ''
  try {
    const res = await memoryClient.listS3Backups({})
    s3Backups.value = res.backups
  } catch (e) {
    // FailedPrecondition = S3 not configured on the server: a normal state, not an error.
    if (e instanceof ConnectError && e.code === Code.FailedPrecondition) {
      s3Unavailable.value = true
      s3Backups.value = []
    } else {
      handleError(e)
    }
  } finally {
    s3Loading.value = false
  }
}

async function doRestoreS3(b) {
  if (!(await confirmDialog(
    `Restore backup "${b.name}" from S3? Existing users and API keys are left untouched. Memories ` +
    `and summaries will be queued for re-embedding — this may take a while.`,
    { actionLabel: 'Restore' }
  ))) return

  restoring.value = true
  error.value = ''
  try {
    const res = await memoryClient.restoreAll({ s3Key: b.name })
    toast.success(
      `Restore complete: ${res.usersCreated} user(s) created, ${res.usersSkipped} skipped; ` +
      `${res.apiKeysCreated} API key(s) created, ${res.apiKeysSkipped} skipped; ` +
      `${res.memoriesQueued} memories and ${res.summariesQueued} summaries queued for re-indexing.`
    )
  } catch (e) {
    handleError(e)
  } finally {
    restoring.value = false
  }
}


onMounted(() => {
  if (auth.isAdmin) {
    loadBackups()
    loadS3Backups()
  }
})

async function doImport() {
  if (!file.value) return
  error.value = ''
  try {
    const text = await file.value.text()
    let recs
    try {
      recs = JSON.parse(text)
    } catch (e) {
      error.value = `Could not parse the file as JSON: ${e.message}`
      return
    }
    if (!Array.isArray(recs)) {
      error.value = 'Expected a JSON array of memories (a `cortex export` dump).'
      return
    }

    // Map to Memory protos, dropping records with no text (the server skips them
    // anyway) so the queued count is honest.
    const mems = []
    let skipped = 0
    for (const r of recs) {
      if (!r || typeof r.text !== 'string' || r.text.trim() === '') {
        skipped++
        continue
      }
      const m = {
        id: r.id || '',
        text: r.text,
        namespace: r.namespace || '',
        source: r.source || '',
        tags: r.tags || [],
        model: r.model || '',
        dims: r.dims || 0,
        conversationId: r.conversationId || '',
        linkedIds: r.linkedIds || [],
        notDuplicateOf: r.notDuplicateOf || [],
      }
      if (r.createdAt) {
        const d = new Date(r.createdAt)
        if (!isNaN(d.getTime())) m.createdAt = Timestamp.fromDate(d)
      }
      mems.push(m)
    }

    if (mems.length === 0) {
      error.value = `Nothing to import (${skipped} record(s) had no text).`
      return
    }
    if (!(await confirmDialog(
      `Import ${mems.length} memor${mems.length === 1 ? 'y' : 'ies'}? Existing ids will be overwritten.`,
      { actionLabel: 'Import' }
    ))) {
      return
    }

    importing.value = true
    let queued = 0
    for (let start = 0; start < mems.length; start += IMPORT_BATCH) {
      const batch = mems.slice(start, start + IMPORT_BATCH)
      importInfo.value = `Restoring ${Math.min(start + batch.length, mems.length)}/${mems.length}…`
      const resp = await memoryClient.restoreMemories({ memories: batch })
      queued += resp.queued
    }
    toast.success(`Queued ${queued}/${mems.length} for re-indexing` +
      (skipped ? ` (${skipped} skipped — no text).` : '.') +
      ' They will appear once the worker re-embeds them.')
    importInfo.value = ''
    file.value = null
    if (fileInput.value) fileInput.value.value = ''
  } catch (e) {
    handleError(e)
  } finally {
    importing.value = false
  }
}
</script>
