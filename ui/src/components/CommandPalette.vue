<template>
  <CommandDialog v-model:open="open">
    <CommandInput v-model="query" placeholder="Search memories or jump to a view…" />
    <CommandList>
      <CommandEmpty>{{ searching ? 'Searching…' : 'No results.' }}</CommandEmpty>

      <CommandGroup heading="Views">
        <CommandItem
          v-for="v in visibleViews"
          :key="v.route"
          :value="'view ' + v.label"
          @select="go(v.route)"
        >
          <component :is="v.icon" class="size-4" />{{ v.label }}
        </CommandItem>
      </CommandGroup>

      <CommandGroup v-if="hits.length" heading="Memories">
        <CommandItem
          v-for="h in hits"
          :key="h.memory.id"
          :value="h.memory.id"
          @select="openHit(h)"
        >
          <!-- Semantic hits rarely contain the query text, and the Command
               filter matches on rendered textContent — embed the query
               invisibly so hits are never filtered out. -->
          <span class="hidden">{{ query }}</span>
          <Database class="size-4 shrink-0" />
          <span class="min-w-0 flex-1 truncate">{{ h.memory.text }}</span>
          <Badge variant="secondary" class="ml-2 shrink-0">{{ h.memory.namespace }}</Badge>
        </CommandItem>
      </CommandGroup>
    </CommandList>
  </CommandDialog>
</template>

<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { memoryClient } from '@/utils/connect'
import { useAuthStore } from '@/stores/auth'
import { openInspector } from '@/lib/inspector'
import {
  Archive,
  BookOpen,
  Database,
  KeyRound,
  Layers,
  ListChecks,
  MessagesSquare,
  Network,
  Search,
  Server,
  SlidersHorizontal,
  Users,
} from 'lucide-vue-next'
import { Badge } from '@/components/ui/badge'
import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from '@/components/ui/command'

const router = useRouter()
const auth = useAuthStore()

const open = ref(false)
const query = ref('')
const hits = ref([])
const searching = ref(false)

const views = [
  { route: 'memories', label: 'Memories', icon: Database },
  { route: 'graph', label: 'Graph', icon: Network },
  { route: 'explore', label: 'Explore', icon: Search },
  { route: 'sessions', label: 'Sessions', icon: MessagesSquare },
  { route: 'namespaces', label: 'Namespaces', icon: Layers },
  { route: 'preferences', label: 'Preferences', icon: SlidersHorizontal },
  { route: 'backup', label: 'Backup', icon: Archive },
  { route: 'status', label: 'Status', icon: Server },
  { route: 'queue', label: 'Indexing', icon: ListChecks, gate: () => !auth.multiTenant || auth.isAdmin },
  { route: 'apikeys', label: 'API Keys', icon: KeyRound, gate: () => auth.multiTenant },
  { route: 'users', label: 'Users', icon: Users, gate: () => auth.multiTenant && auth.isAdmin },
  { route: 'documentation', label: 'Docs', icon: BookOpen },
]

const visibleViews = computed(() => views.filter((v) => !v.gate || v.gate()))

function onKeydown(e) {
  if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
    e.preventDefault()
    open.value = !open.value
  }
}
onMounted(() => window.addEventListener('keydown', onKeydown))
onBeforeUnmount(() => window.removeEventListener('keydown', onKeydown))

// Exposed so the navbar search button can open the palette.
defineExpose({ show: () => (open.value = true) })

let timer = null
let seq = 0
watch(query, (q) => {
  clearTimeout(timer)
  if (!q.trim() || !auth.checkAuth()) {
    hits.value = []
    return
  }
  timer = setTimeout(async () => {
    const mySeq = ++seq
    searching.value = true
    try {
      // noReinforce: palette browsing must not feed the living-memory signal.
      const res = await memoryClient.search({ query: q, namespace: '*', limit: 8, noReinforce: true })
      if (mySeq === seq) hits.value = res.hits
    } catch {
      if (mySeq === seq) hits.value = []
    } finally {
      if (mySeq === seq) searching.value = false
    }
  }, 250)
})

watch(open, (v) => {
  if (!v) {
    query.value = ''
    hits.value = []
  }
})

function go(route) {
  open.value = false
  router.push({ name: route })
}

function openHit(h) {
  open.value = false
  openInspector(h.memory)
}
</script>
