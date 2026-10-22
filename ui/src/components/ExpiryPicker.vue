<template>
  <div class="flex flex-wrap items-center gap-2">
    <Label class="inline-flex items-center gap-1 text-xs text-muted-foreground"><Hourglass class="size-3" />Expiry</Label>
    <Select :model-value="model.preset" @update:model-value="(v) => update({ preset: v })">
      <SelectTrigger size="sm" class="w-44" aria-label="Expiry">
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        <SelectItem v-if="allowKeep" value="keep">Keep current</SelectItem>
        <SelectItem v-for="p in EXPIRY_PRESETS" :key="p.value" :value="p.value">{{ p.label }}</SelectItem>
      </SelectContent>
    </Select>
    <Input
      v-if="model.preset === 'custom'"
      type="date"
      class="w-40"
      :min="minDate"
      :model-value="model.date"
      aria-label="Expiry date"
      @update:model-value="(v) => update({ date: v })"
    />
  </div>
</template>

<script setup>
import { computed } from 'vue'
import { Hourglass } from 'lucide-vue-next'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { EXPIRY_PRESETS, toDateInput } from '@/lib/expiry'

// v-model: { preset: 'keep'|'none'|'1d'|'7d'|'30d'|'custom', date: 'yyyy-mm-dd' }.
// `allowKeep` adds the "Keep current" choice used when editing a memory.
const props = defineProps({
  model: { type: Object, required: true },
  allowKeep: { type: Boolean, default: false },
})
const emit = defineEmits(['update:model'])

const minDate = computed(() => toDateInput(new Date()))

function update(patch) {
  const next = { ...props.model, ...patch }
  // Picking "custom" for the first time starts on tomorrow.
  if (next.preset === 'custom' && !next.date) next.date = toDateInput(new Date(Date.now() + 86400000))
  emit('update:model', next)
}
</script>
