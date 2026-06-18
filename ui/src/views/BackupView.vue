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
    <div class="card">
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
  </div>
</template>

<script setup>
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { Timestamp } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { memoryClient } from '@/utils/connect'
import { useAuthStore } from '@/stores/auth'

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

function onFile(e) {
  file.value = e.target.files?.[0] || null
  importMsg.value = ''
  importInfo.value = file.value ? `Selected: ${file.value.name} (${(file.value.size / 1024).toFixed(1)} KB)` : ''
}

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
