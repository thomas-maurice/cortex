import { ref } from 'vue'

// Global state for the MemoryInspector drawer (mounted once in App.vue) so any
// view — and the command palette — can open a memory without owning the UI.

export const inspectorMemory = ref(null)
// Optional host-provided resolver: (id) => memory | undefined. Used to follow
// linked ids within the host view's already-loaded set (there is no
// get-memory-by-id RPC).
export const inspectorResolver = ref(null)
// Bumped after an edit/delete performed inside the inspector; host views watch
// this to reload their lists.
export const inspectorChanged = ref(0)

export function openInspector(memory, resolver = null) {
  inspectorMemory.value = memory
  inspectorResolver.value = resolver
}

export function closeInspector() {
  inspectorMemory.value = null
  inspectorResolver.value = null
}

export function bumpInspectorChanged() {
  inspectorChanged.value++
}
