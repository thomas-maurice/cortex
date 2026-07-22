<template>
  <div class="mb-3 overflow-hidden rounded-md border bg-muted font-mono text-sm">
    <!-- Toolbar header holds the optional filename label and the copy button.
         It sizes to the button's height, so nothing overlaps the code below. -->
    <div class="flex items-center justify-between gap-2 border-b px-2 py-1">
      <span class="truncate text-xs text-muted-foreground">{{ lang }}</span>
      <Button
        variant="ghost"
        size="icon"
        class="size-8 shrink-0"
        :title="copied ? 'Copied!' : 'Copy'"
        :aria-label="copied ? 'Copied' : 'Copy'"
        @click="copy"
      >
        <Check v-if="copied" class="size-4" />
        <Copy v-else class="size-4" />
      </Button>
    </div>
    <pre class="mb-0 overflow-x-auto p-3"><code>{{ text }}</code></pre>
  </div>
</template>

<script setup>
import { ref, onBeforeUnmount } from 'vue'
import { Check, Copy } from 'lucide-vue-next'
import { Button } from '@/components/ui/button'

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
