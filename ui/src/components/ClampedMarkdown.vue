<template>
  <div>
    <div
      ref="body"
      class="markdown-body min-w-0 text-sm"
      :class="{ 'max-h-40 overflow-hidden': clamped }"
      v-html="html"
    ></div>
    <button
      v-if="overflowing || !clamped"
      class="mt-1 inline-flex items-center gap-1 text-xs font-medium text-muted-foreground hover:text-foreground"
      @click.stop="clamped = !clamped"
    >
      <ChevronDown class="size-3.5 transition-transform" :class="{ 'rotate-180': !clamped }" />
      {{ clamped ? 'Show more' : 'Show less' }}
    </button>
  </div>
</template>

<script setup>
import { nextTick, onMounted, ref, watch } from 'vue'
import { ChevronDown } from 'lucide-vue-next'

// Long memories dominate list views; clamp to ~10rem and let the reader opt
// in. Overflow is measured (not guessed from length) so short markdown never
// shows a pointless toggle.
const props = defineProps({ html: { type: String, required: true } })

const body = ref(null)
const clamped = ref(true)
const overflowing = ref(false)

function measure() {
  if (!body.value) return
  overflowing.value = body.value.scrollHeight > body.value.clientHeight + 2
}

onMounted(measure)
watch(() => props.html, async () => {
  clamped.value = true
  await nextTick()
  measure()
})
</script>
