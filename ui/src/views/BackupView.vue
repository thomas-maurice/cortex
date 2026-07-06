<template>
  <div>
    <div class="alert alert-secondary py-2 small d-flex align-items-start gap-2">
      <font-awesome-icon :icon="['fas', 'box-archive']" class="mt-1" />
      <div>
        Export dumps memories (text + metadata, <strong>no vectors</strong>) to a JSON file. Import
        re-ingests such a dump through the normal indexing queue — the worker re-embeds each one, so a
        restore is safe across embedding-model changes. The format is identical to the
        <code>cortex export</code> / <code>cortex import</code> CLI, so dumps are interchangeable.
        An existing id is overwritten (upsert).
      </div>
    </div>

    <div v-if="error" class="alert alert-danger py-2" style="white-space: pre-wrap">{{ error }}</div>

    <!-- Export -->
    <div class="card mb-3">
      <div class="card-body py-3">
        <h6 class="mb-2"><font-awesome-icon :icon="['fas', 'download']" class="me-2" />Export</h6>
        <div class="row g-2 align-items-end">
          <div class="col-auto" style="width: 220px">
            <label class="form-label small mb-1">Namespace</label>
            <input v-model="exportNs" class="form-control form-control-sm" placeholder="* = all namespaces" />
          </div>
          <div class="col-auto">
            <button class="btn btn-primary btn-sm" :disabled="exporting" @click="doExport">
              <font-awesome-icon :icon="['fas', exporting ? 'spinner' : 'download']" :spin="exporting" class="me-1" />
              {{ exporting ? 'Exporting…' : 'Export to JSON' }}
            </button>
          </div>
        </div>
        <div v-if="exportMsg" class="small text-success mt-2">{{ exportMsg }}</div>
      </div>
    </div>

    <!-- Import -->
    <div class="card mb-3">
      <div class="card-body py-3">
        <h6 class="mb-2"><font-awesome-icon :icon="['fas', 'upload']" class="me-2" />Import</h6>
        <div class="row g-2 align-items-end">
          <div class="col">
            <label class="form-label small mb-1">Dump file (.json)</label>
            <input ref="fileInput" type="file" accept="application/json,.json" class="form-control form-control-sm" @change="onFile" />
          </div>
          <div class="col-auto">
            <button class="btn btn-primary btn-sm" :disabled="importing || !file" @click="doImport">
              <font-awesome-icon :icon="['fas', importing ? 'spinner' : 'upload']" :spin="importing" class="me-1" />
              {{ importing ? 'Importing…' : 'Import' }}
            </button>
          </div>
        </div>
        <div v-if="importInfo" class="small text-muted mt-2">{{ importInfo }}</div>
        <div v-if="importMsg" class="small text-success mt-2">{{ importMsg }}</div>
      </div>
    </div>

    <!-- My data backup (all logged-in users) -->
    <div class="card mb-3">
      <div class="card-body py-3">
        <h6 class="mb-2"><font-awesome-icon :icon="['fas', 'user-shield']" class="me-2" />My data backup</h6>
        <p class="small text-muted mb-3">
          Server-generated backup of your own memories and conversation summaries — unlike the
          client-side export above, this is a single versioned file that includes summaries and is
          produced entirely on the server. Restore always writes into your own data; re-running is
          safe (memories are upserted by id).
        </p>

        <!-- Download my backup -->
        <div class="d-flex align-items-center gap-2 flex-wrap mb-3">
          <button class="btn btn-primary btn-sm" :disabled="selfBacking" @click="doBackupSelf">
            <font-awesome-icon :icon="['fas', selfBacking ? 'spinner' : 'download']" :spin="selfBacking" class="me-1" />
            {{ selfBacking ? 'Preparing…' : 'Download my backup' }}
          </button>
          <span v-if="selfBackupMsg" class="small text-success">{{ selfBackupMsg }}</span>
        </div>

        <!-- Restore my backup -->
        <div>
          <label class="form-label small mb-1">Restore from file</label>
          <div class="row g-2 align-items-end">
            <div class="col">
              <input
                ref="selfFileInput"
                type="file"
                class="form-control form-control-sm"
                @change="onSelfFile"
              />
            </div>
            <div class="col-auto">
              <button class="btn btn-outline-primary btn-sm" :disabled="selfRestoring || !selfFile" @click="doRestoreSelf">
                <font-awesome-icon :icon="['fas', selfRestoring ? 'spinner' : 'rotate-left']" :spin="selfRestoring" class="me-1" />
                {{ selfRestoring ? 'Restoring…' : 'Restore' }}
              </button>
            </div>
          </div>
          <div v-if="selfRestoreMsg" class="alert alert-success alert-dismissible py-2 mt-2 mb-0 small">
            <button type="button" class="btn-close py-2" @click="selfRestoreMsg = ''"></button>
            {{ selfRestoreMsg }}
            Memories and summaries are re-embedded asynchronously — check the
            <router-link :to="{ name: 'queue' }">Indexing</router-link> view for progress.
          </div>
        </div>
      </div>
    </div>

    <!-- Server backups (admin only) -->
    <template v-if="auth.isAdmin">
      <hr class="my-4" />

      <div class="alert alert-secondary py-2 small d-flex align-items-start gap-2">
        <font-awesome-icon :icon="['fas', 'server']" class="mt-1" />
        <div>
          <strong>Server-side full backup</strong> — captures ALL tenants' memories, conversation summaries,
          and the user registry (users + API keys) into a single file written to the server's configured
          backup directory. Only admins can trigger or restore backups. A restore hands memories and
          summaries to the indexing worker for re-embedding; users and API keys that already exist are
          left untouched.
        </div>
      </div>

      <!-- Run backup -->
      <div class="card mb-3">
        <div class="card-body py-3">
          <h6 class="mb-2"><font-awesome-icon :icon="['fas', 'box-archive']" class="me-2" />Server backups</h6>
          <button class="btn btn-primary btn-sm" :disabled="backingUp" @click="doBackupAll">
            <font-awesome-icon :icon="['fas', backingUp ? 'spinner' : 'floppy-disk']" :spin="backingUp" class="me-1" />
            {{ backingUp ? 'Backing up…' : 'Run full backup now' }}
          </button>

          <div v-if="backupSuccess" class="alert alert-success alert-dismissible py-2 mt-3 mb-0 small">
            <button type="button" class="btn-close py-2" @click="backupSuccess = null"></button>
            <strong>Backup written:</strong> <code>{{ backupSuccess.path }}</code><br />
            {{ backupSuccess.tenants }} tenant(s), {{ backupSuccess.memories }} memories,
            {{ backupSuccess.summaries }} summaries, {{ backupSuccess.users }} users,
            {{ backupSuccess.apiKeys }} API key(s).
            <span v-if="backupSuccess.s3Result">
              <br /><strong>Offsite:</strong> {{ backupSuccess.s3Result }}
            </span>
          </div>
        </div>
      </div>

      <!-- Backup list -->
      <div class="card mb-3">
        <div class="card-body py-3">
          <div class="d-flex align-items-center gap-2 mb-3">
            <h6 class="mb-0"><font-awesome-icon :icon="['fas', 'list']" class="me-2" />Existing backups</h6>
            <button class="btn btn-outline-secondary btn-sm ms-auto" :disabled="backupsLoading" @click="loadBackups">
              <font-awesome-icon :icon="['fas', 'rotate']" class="me-1" />Refresh
            </button>
          </div>

          <div v-if="backupsLoading" class="text-center text-muted py-4">
            <font-awesome-icon :icon="['fas', 'spinner']" spin size="2x" />
          </div>

          <div v-else-if="backups.length === 0" class="text-center text-muted py-4">
            <font-awesome-icon :icon="['fas', 'box-open']" size="2x" class="mb-2 d-block" />
            No server backups yet.
          </div>

          <table v-else class="table table-sm align-middle mb-0">
            <thead>
              <tr>
                <th>Name</th>
                <th class="text-end">Size</th>
                <th>Created</th>
                <th class="text-end" style="width: 1%">Actions</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="b in backups" :key="b.name">
                <td class="font-monospace small">{{ b.name }}</td>
                <td class="text-end small text-muted">{{ formatSize(b.sizeBytes) }}</td>
                <td class="small text-muted">{{ b.createdAt ? formatTimestamp(b.createdAt) : '—' }}</td>
                <td class="text-end text-nowrap">
                  <button
                    class="btn btn-outline-secondary btn-sm me-1"
                    title="Download this backup"
                    :disabled="downloading"
                    @click="doDownloadBackup(b)"
                  >
                    <font-awesome-icon :icon="['fas', downloading ? 'spinner' : 'download']" :spin="downloading" />
                  </button>
                  <button
                    class="btn btn-outline-warning btn-sm me-1"
                    title="Restore this backup"
                    :disabled="restoring"
                    @click="doRestoreAll(b)"
                  >
                    <font-awesome-icon :icon="['fas', restoring ? 'spinner' : 'rotate-left']" :spin="restoring" />
                  </button>
                  <button
                    class="btn btn-outline-danger btn-sm"
                    title="Delete this backup"
                    :disabled="deleting"
                    @click="doDeleteBackup(b)"
                  >
                    <font-awesome-icon :icon="['fas', deleting ? 'spinner' : 'trash']" :spin="deleting" />
                  </button>
                </td>
              </tr>
            </tbody>
          </table>

          <div v-if="restoreMsg" class="alert alert-success alert-dismissible py-2 mt-3 mb-0 small">
            <button type="button" class="btn-close py-2" @click="restoreMsg = ''"></button>
            {{ restoreMsg }}
            Memories and summaries are re-embedded asynchronously — check the
            <router-link :to="{ name: 'queue' }">Indexing</router-link> view for progress.
          </div>
        </div>
      </div>

      <!-- Offsite (S3) backups -->
      <div class="card">
        <div class="card-body py-3">
          <div class="d-flex align-items-center gap-2 mb-2">
            <h6 class="mb-0"><font-awesome-icon :icon="['fas', 'cloud-arrow-up']" class="me-2" />Offsite (S3) backups</h6>
            <button class="btn btn-outline-secondary btn-sm ms-auto" :disabled="s3Loading" @click="loadS3Backups">
              <font-awesome-icon :icon="['fas', 'rotate']" class="me-1" />Refresh
            </button>
          </div>
          <p class="small text-muted mb-3">
            Backups uploaded to the server's configured S3 bucket. S3 is configured on the server
            via env vars (<code>CORTEX_S3_*</code> / <code>AWS_*</code>); restore downloads the
            object back through the server.
          </p>

          <div v-if="s3Loading" class="text-center text-muted py-4">
            <font-awesome-icon :icon="['fas', 'spinner']" spin size="2x" />
          </div>

          <div v-else-if="s3Unavailable" class="text-center text-muted py-4">
            <font-awesome-icon :icon="['fas', 'cloud-arrow-up']" size="2x" class="mb-2 d-block" />
            Offsite S3 backup is not configured on the server.
          </div>

          <div v-else-if="s3Backups.length === 0" class="text-center text-muted py-4">
            <font-awesome-icon :icon="['fas', 'box-open']" size="2x" class="mb-2 d-block" />
            No backups in the S3 bucket yet.
          </div>

          <table v-else class="table table-sm align-middle mb-0">
            <thead>
              <tr>
                <th>Object key</th>
                <th class="text-end">Size</th>
                <th>Modified</th>
                <th class="text-end" style="width: 1%">Actions</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="b in s3Backups" :key="b.name">
                <td class="font-monospace small">{{ b.name }}</td>
                <td class="text-end small text-muted">{{ formatSize(b.sizeBytes) }}</td>
                <td class="small text-muted">{{ b.createdAt ? formatTimestamp(b.createdAt) : '—' }}</td>
                <td class="text-end text-nowrap">
                  <button
                    class="btn btn-outline-warning btn-sm"
                    title="Restore this backup from S3"
                    :disabled="restoring"
                    @click="doRestoreS3(b)"
                  >
                    <font-awesome-icon :icon="['fas', restoring ? 'spinner' : 'rotate-left']" :spin="restoring" />
                  </button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

    </template>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { Timestamp } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { memoryClient } from '@/utils/connect'
import { useAuthStore } from '@/stores/auth'
import { formatTimestamp } from '@/utils/text'

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
const exportMsg = ref('')

const fileInput = ref(null)
const file = ref(null)
const importing = ref(false)
const importInfo = ref('')
const importMsg = ref('')

// My data (self) backup state
const selfBacking = ref(false)
const selfBackupMsg = ref('')
const selfFileInput = ref(null)
const selfFile = ref(null)
const selfRestoring = ref(false)
const selfRestoreMsg = ref('')

// Server-backup state (admin only)
const backingUp = ref(false)
const backupSuccess = ref(null)   // BackupAllResponse fields when last backup succeeded
const backups = ref([])
const backupsLoading = ref(false)
const restoring = ref(false)

// Offsite (S3) backups (admin). s3Unavailable is set when the server has no S3
// configured (FailedPrecondition), so we show a hint instead of an error.
const s3Backups = ref([])
const s3Loading = ref(false)
const s3Unavailable = ref(false)
const restoreMsg = ref('')
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
  exportMsg.value = ''
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
    exportMsg.value = msg
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
  importMsg.value = ''
  importInfo.value = file.value ? `Selected: ${file.value.name} (${(file.value.size / 1024).toFixed(1)} KB)` : ''
}

function onSelfFile(e) {
  selfFile.value = e.target.files?.[0] || null
  selfRestoreMsg.value = ''
}

// ---- My data (self) backup ----

async function doBackupSelf() {
  selfBacking.value = true
  selfBackupMsg.value = ''
  error.value = ''
  try {
    const res = await memoryClient.backupSelf({})
    downloadBytes(res.data, res.name)
    selfBackupMsg.value = `Downloaded: ${res.memories} memories, ${res.summaries} summaries.`
  } catch (e) {
    handleError(e)
  } finally {
    selfBacking.value = false
  }
}

async function doRestoreSelf() {
  if (!selfFile.value) return
  if (!window.confirm(
    `Restore "${selfFile.value.name}" into your account?\n\n` +
    `Existing memories are upserted by id — re-running is safe.`
  )) return

  selfRestoring.value = true
  selfRestoreMsg.value = ''
  error.value = ''
  try {
    const buf = await selfFile.value.arrayBuffer()
    const res = await memoryClient.restoreSelf({ data: new Uint8Array(buf) })
    selfRestoreMsg.value =
      `Queued ${res.memoriesQueued} memories and ${res.summariesQueued} summaries for re-indexing.`
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
  backupSuccess.value = null
  try {
    const res = await memoryClient.backupAll({})
    backupSuccess.value = {
      path: res.path,
      tenants: res.tenants,
      memories: res.memories,
      summaries: res.summaries,
      users: res.users,
      apiKeys: res.apiKeys,
      s3Result: res.s3Result,
    }
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
  if (!window.confirm(
    `Restore backup "${b.name}"?\n\n` +
    `Existing users and API keys are left untouched. Memories and summaries will be queued ` +
    `for re-embedding — this may take a while.`
  )) return

  restoring.value = true
  restoreMsg.value = ''
  error.value = ''
  try {
    const res = await memoryClient.restoreAll({ name: b.name })
    restoreMsg.value =
      `Restore complete: ${res.usersCreated} user(s) created, ${res.usersSkipped} skipped; ` +
      `${res.apiKeysCreated} API key(s) created, ${res.apiKeysSkipped} skipped; ` +
      `${res.memoriesQueued} memories and ${res.summariesQueued} summaries queued for re-indexing.`
  } catch (e) {
    handleError(e)
  } finally {
    restoring.value = false
  }
}

async function doDeleteBackup(b) {
  if (!window.confirm(`Delete backup "${b.name}"? This cannot be undone.`)) return

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
  if (!window.confirm(
    `Restore backup "${b.name}" from S3?\n\n` +
    `Existing users and API keys are left untouched. Memories and summaries will be queued ` +
    `for re-embedding — this may take a while.`
  )) return

  restoring.value = true
  restoreMsg.value = ''
  error.value = ''
  try {
    const res = await memoryClient.restoreAll({ s3Key: b.name })
    restoreMsg.value =
      `Restore complete: ${res.usersCreated} user(s) created, ${res.usersSkipped} skipped; ` +
      `${res.apiKeysCreated} API key(s) created, ${res.apiKeysSkipped} skipped; ` +
      `${res.memoriesQueued} memories and ${res.summariesQueued} summaries queued for re-indexing.`
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
  importMsg.value = ''
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
    if (!confirm(`Import ${mems.length} memor${mems.length === 1 ? 'y' : 'ies'}? Existing ids will be overwritten.`)) {
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
    importMsg.value = `Queued ${queued}/${mems.length} for re-indexing` +
      (skipped ? ` (${skipped} skipped — no text).` : '.') +
      ' They will appear once the worker re-embeds them.'
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
