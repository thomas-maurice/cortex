<script setup>
import { reactiveOmit } from "@vueuse/core";
import { ListboxGroup, ListboxGroupLabel, useId } from "reka-ui";
import { computed, onMounted, onUnmounted } from "vue";
import { cn } from "@/lib/utils";
import { provideCommandGroupContext, useCommand } from ".";

const props = defineProps({
  asChild: { type: Boolean, required: false },
  as: { type: null, required: false },
  class: {
    type: [Boolean, null, String, Object, Array],
    required: false,
    skipCheck: true,
  },
  heading: { type: String, required: false },
  // Render the group and its items regardless of the filter state. Needed for
  // items that arrive asynchronously (e.g. server-side search hits): the filter
  // only recomputes on search-text changes, so late-mounting items would
  // otherwise stay hidden until the next keystroke.
  forceMount: { type: Boolean, required: false },
});

const delegatedProps = reactiveOmit(props, "class", "forceMount");

const { allGroups, filterState } = useCommand();
const id = useId();

const isRender = computed(() =>
  props.forceMount || !filterState.search
    ? true
    : filterState.filtered.groups.has(id),
);

provideCommandGroupContext({ id, forceMount: props.forceMount });
onMounted(() => {
  if (!allGroups.value.has(id)) allGroups.value.set(id, new Set());
});
onUnmounted(() => {
  allGroups.value.delete(id);
});
</script>

<template>
  <ListboxGroup
    v-bind="delegatedProps"
    :id="id"
    data-slot="command-group"
    :class="cn('text-foreground overflow-hidden p-1', props.class)"
    :hidden="isRender ? undefined : true"
  >
    <ListboxGroupLabel
      v-if="heading"
      data-slot="command-group-heading"
      class="px-2 py-1.5 text-xs font-medium text-muted-foreground"
    >
      {{ heading }}
    </ListboxGroupLabel>
    <slot />
  </ListboxGroup>
</template>
