<template>
  <Badge
    v-if="info"
    variant="secondary"
    :class="info.expired ? 'text-destructive' : 'text-sky-600 dark:text-sky-400'"
    :title="(info.expired ? 'Expired ' : 'Expires ') + fullDate + (info.expired ? ' — hidden from search and recall' : '')"
  >
    <Hourglass class="size-3" />{{ info.text }}
  </Badge>
</template>

<script setup>
import { computed } from 'vue'
import { timestampDate } from '@bufbuild/protobuf/wkt'
import { Hourglass } from 'lucide-vue-next'
import { Badge } from '@/components/ui/badge'
import { expiryInfo } from '@/lib/expiry'

const props = defineProps({ validUntil: { type: Object, default: null } })
const info = computed(() => expiryInfo(props.validUntil))
const fullDate = computed(() => (props.validUntil ? timestampDate(props.validUntil).toLocaleString() : ''))
</script>
