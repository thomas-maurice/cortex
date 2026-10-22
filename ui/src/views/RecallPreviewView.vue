<template>
  <div class="space-y-4">
    <div class="grid gap-1.5">
      <Label class="text-xs">Prompt</Label>
      <Textarea
        v-model="prompt"
        rows="6"
        placeholder="Paste a prompt as you would send it to Claude Code. Long prompts are split into ~512-token chunks, exactly like the hook does. (Ctrl/Cmd+Enter to preview)"
        @keydown.enter.ctrl.prevent="run"
        @keydown.enter.meta.prevent="run"
      />
    </div>

    <div class="flex flex-wrap items-end gap-3">
      <div class="grid gap-1.5" style="width: 120px">
        <Label class="text-xs">Namespace</Label>
        <Input v-model="namespace" placeholder="* = all" />
      </div>
      <div class="grid gap-1.5" style="width: 90px">
        <Label class="text-xs" title="max memories injected per prompt (hook --limit)">Limit</Label>
        <Input v-model.number="limit" type="number" min="1" max="50" />
      </div>
      <div class="grid gap-1.5" style="width: 220px">
        <Label class="text-xs" title="relevance cutoff (hook --max-distance); lower = stricter, 0 = no cutoff">
          Max distance: {{ maxDistance.toFixed(2) }}
        </Label>
        <input v-model.number="maxDistance" type="range" min="0" max="1.5" step="0.01" class="h-9 w-full accent-primary" />
      </div>
      <div class="grid gap-1.5" style="width: 100px">
        <Label class="text-xs" title="prompts shorter than this are skipped (hook --min-chars)">Min chars</Label>
        <Input v-model.number="minChars" type="number" min="0" max="10000" />
      </div>
      <div class="grid gap-1.5" style="width: 130px">
        <Label class="text-xs" title="long prompts are sampled down to this many chunks; 0 = every chunk (hook --max-query-chunks)">Max query chunks</Label>
        <Input v-model.number="maxQueryChunks" type="number" min="0" max="32" />
      </div>
      <div class="grid gap-1.5" style="width: 100px">
        <Label class="text-xs" title="each injected memory is cut to this many characters; 0 = no cap (hook --max-chars)">Max chars</Label>
        <Input v-model.number="maxChars" type="number" min="0" max="100000" />
      </div>
      <Button :disabled="loading || !prompt.trim()" @click="run">
        <Loader2 v-if="loading" class="size-4 animate-spin" /><Eye v-else class="size-4" />Preview
      </Button>
      <Button variant="outline" size="sm" @click="resetDefaults">Hook defaults</Button>
    </div>

    <Alert v-if="error" variant="destructive">
      <AlertDescription>{{ error }}</AlertDescription>
    </Alert>

    <template v-if="result">
      <Alert v-if="result.skipped">
        <AlertDescription>
          The hook would <strong>skip</strong> this prompt and inject nothing —
          <span v-if="result.skipReason === 'directive'">it is a slash/bang command.</span>
          <span v-else>it is shorter than {{ result.minChars }} characters (min chars).</span>
        </AlertDescription>
      </Alert>

      <template v-else>
        <div class="text-sm text-muted-foreground">
          {{ result.queries.length }} query chunk(s) searched<span v-if="result.totalChunks > result.queries.length"> (sampled from {{ result.totalChunks }})</span>
          · limit {{ result.limit }} · max distance {{ result.maxDistance.toFixed(2) }}
        </div>

        <details v-if="result.queries.length > 1 || result.totalChunks > 1" class="text-sm">
          <summary class="cursor-pointer text-muted-foreground">Show query chunks</summary>
          <ol class="mt-2 space-y-1">
            <li v-for="(q, i) in result.queries" :key="i" class="rounded border p-2 font-mono text-xs">
              <span class="mr-2 font-semibold">#{{ i }}</span>{{ truncate(q, 200) }}
            </li>
          </ol>
        </details>

        <details v-if="result.injectedBlock" class="text-sm">
          <summary class="cursor-pointer text-muted-foreground">Show injected text (exactly what the model sees)</summary>
          <pre class="mt-2 max-h-96 overflow-auto whitespace-pre-wrap rounded border p-2 text-xs">{{ result.injectedBlock }}</pre>
        </details>

        <section class="space-y-2">
          <h2 class="text-sm font-semibold">Would be injected ({{ result.injected.length }})</h2>
          <div v-if="result.injected.length === 0" class="rounded-md border border-dashed py-6 text-center text-muted-foreground">
            <Shield class="mx-auto mb-1 size-6" />Nothing — the hook would stay silent for this prompt.
          </div>
          <Card v-for="h in result.injected" :key="h.memory.id">
            <CardContent class="space-y-2 py-3">
              <div class="flex items-center gap-3">
                <Progress :model-value="closeness(h.distance)" class="h-2 w-40 shrink-0" />
                <span class="font-mono text-xs">dist {{ h.distance.toFixed(3) }}</span>
                <Badge variant="secondary" :style="nsChipStyle(h.memory.namespace, theme === 'dark')">{{ h.memory.namespace }}</Badge>
                <span v-if="result.queries.length > 1" class="text-xs text-muted-foreground">matched chunk #{{ h.queryIndex }}</span>
                <Badge v-if="h.truncated" variant="outline" title="the hook cuts each memory to max chars; the relevant part may be past the cut">
                  truncated to {{ result.maxChars }} of {{ h.textChars }} chars
                </Badge>
              </div>
              <button type="button" class="block w-full text-left text-sm hover:underline" @click="openInspector(h.memory)">
                {{ truncate(h.memory.text, 300) }}
              </button>
              <div class="font-mono text-xs text-muted-foreground">{{ h.memory.id }}</div>
            </CardContent>
          </Card>
        </section>

        <div class="flex items-center gap-3 text-xs text-muted-foreground" role="separator">
          <div class="h-px flex-1 border-t border-dashed border-destructive/60"></div>
          cutoff: max distance {{ result.maxDistance > 0 ? result.maxDistance.toFixed(2) : 'none' }} · limit {{ result.limit }}
          <div class="h-px flex-1 border-t border-dashed border-destructive/60"></div>
        </div>

        <section class="space-y-2">
          <h2 class="text-sm font-semibold text-muted-foreground">Near misses ({{ result.nearMisses.length }})</h2>
          <div v-if="result.nearMisses.length === 0" class="text-sm text-muted-foreground">No other results.</div>
          <Card v-for="h in result.nearMisses" :key="h.memory.id" class="opacity-60">
            <CardContent class="space-y-2 py-3">
              <div class="flex flex-wrap items-center gap-3">
                <Progress :model-value="closeness(h.distance)" class="h-2 w-40 shrink-0" />
                <span class="font-mono text-xs">dist {{ h.distance.toFixed(3) }}</span>
                <Badge variant="outline">{{ reasonLabel(h) }}</Badge>
                <Badge variant="secondary" :style="nsChipStyle(h.memory.namespace, theme === 'dark')">{{ h.memory.namespace }}</Badge>
              </div>
              <button type="button" class="block w-full text-left text-sm hover:underline" @click="openInspector(h.memory)">
                {{ truncate(h.memory.text, 300) }}
              </button>
              <div class="font-mono text-xs text-muted-foreground">{{ h.memory.id }}</div>
            </CardContent>
          </Card>
          <p class="text-xs text-muted-foreground">
            Near-miss distances come from a second, wider search and can differ slightly from the injected set's.
          </p>
        </section>
      </template>

      <Alert>
        <Info class="size-4" />
        <AlertDescription>{{ result.note }}</AlertDescription>
      </Alert>
    </template>
    <div v-else-if="!loading" class="py-16 text-center text-muted-foreground">
      <Eye class="mx-auto mb-2 size-8" />
      Enter a prompt to see which memories the recall hook would inject.
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { Code, ConnectError } from '@connectrpc/connect'
import { Eye, Info, Loader2, Shield } from 'lucide-vue-next'
import { memoryClient } from '@/utils/connect'
import { useAuthStore } from '@/stores/auth'
import { truncate } from '@/utils/text'
import { nsChipStyle } from '@/lib/nsColor'
import { theme } from '@/lib/theme'
import { openInspector } from '@/lib/inspector'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Progress } from '@/components/ui/progress'
import { Textarea } from '@/components/ui/textarea'

const router = useRouter()
const auth = useAuthStore()

// Defaults mirror `cortex hook recall` (internal/recall Default* constants).
const DEFAULTS = { limit: 3, maxDistance: 0.5, minChars: 12, maxQueryChunks: 4, maxChars: 1500 }

const prompt = ref('')
const namespace = ref('*')
const limit = ref(DEFAULTS.limit)
const maxDistance = ref(DEFAULTS.maxDistance)
const minChars = ref(DEFAULTS.minChars)
const maxQueryChunks = ref(DEFAULTS.maxQueryChunks)
const maxChars = ref(DEFAULTS.maxChars)
const loading = ref(false)
const error = ref('')
const result = ref(null)
// Monotonic id so a slow preview can't overwrite a newer one's results.
let reqId = 0

function resetDefaults() {
  namespace.value = '*'
  maxChars.value = DEFAULTS.maxChars
  limit.value = DEFAULTS.limit
  maxDistance.value = DEFAULTS.maxDistance
  minChars.value = DEFAULTS.minChars
  maxQueryChunks.value = DEFAULTS.maxQueryChunks
}

// Distance bars show closeness: full = identical, empty = distance >= 1.
function closeness(d) {
  return Math.round(Math.min(1, Math.max(0, 1 - d)) * 100)
}

function reasonLabel(h) {
  return h.reason === 'over_max_distance' ? 'over max distance' : 'over limit'
}

async function run() {
  if (loading.value || !prompt.value.trim()) return
  const my = ++reqId
  loading.value = true
  error.value = ''
  try {
    // PreviewRecall is read-only: tenant-scoped, never reinforces.
    const res = await memoryClient.previewRecall({
      prompt: prompt.value,
      // The hook searches '*'; the server would map '' to its default namespace.
      namespace: namespace.value.trim() || '*',
      limit: limit.value,
      maxDistance: maxDistance.value,
      minChars: minChars.value,
      maxQueryChunks: maxQueryChunks.value,
      maxChars: maxChars.value,
    })
    if (my !== reqId) return
    result.value = res
  } catch (e) {
    if (e instanceof ConnectError && e.code === Code.Unauthenticated) {
      auth.logout()
      router.push({ name: 'login' })
      return
    }
    error.value = e.message || 'Request failed'
  } finally {
    if (my === reqId) loading.value = false
  }
}
</script>
