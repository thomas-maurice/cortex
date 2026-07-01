<template>
  <div class="mb-3">
    <!-- Toolbar header holds the optional filename label and the copy button.
         It sizes to the button's height, so nothing overlaps the code below. -->
    <div
      class="d-flex align-items-center justify-content-between bg-light border border-bottom-0 rounded-top px-2 py-1"
    >
      <span class="small text-muted font-monospace text-truncate">{{ lang }}</span>
      <button
        class="btn btn-sm btn-outline-secondary py-0 ms-2 flex-shrink-0"
        @click="copy"
      >
        <font-awesome-icon :icon="['fas', 'copy']" class="me-1" />
        <span class="small">{{ copied ? 'Copied!' : 'Copy' }}</span>
      </button>
    </div>
    <pre class="bg-light border border-top-0 rounded-bottom p-3 mb-0"><code>{{ text }}</code></pre>
  </div>
</template>

<script setup>
import { ref, onBeforeUnmount } from 'vue'

// CodeBlock renders a monospace snippet with an optional filename/label header
// and a copy-to-clipboard button. Each instance owns its own transient "Copied!"
// state, so several blocks on a page don't share (or fight over) one flag.
const props = defineProps({
  text: { type: String, required: true },
  lang: { type: String, default: '' },
})

const copied = ref(false)
let timer

async function copy() {
  try {
    await navigator.clipboard.writeText(props.text)
    copied.value = true
    clearTimeout(timer)
    timer = setTimeout(() => {
      copied.value = false
    }, 1500)
  } catch {
    copied.value = false
  }
}

onBeforeUnmount(() => clearTimeout(timer))
</script>
