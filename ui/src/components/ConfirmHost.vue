<template>
  <AlertDialog :open="!!confirmState" @update:open="(v) => !v && dismiss()">
    <AlertDialogContent>
      <AlertDialogHeader>
        <AlertDialogTitle>{{ confirmState?.title }}</AlertDialogTitle>
        <AlertDialogDescription>{{ confirmState?.message }}</AlertDialogDescription>
      </AlertDialogHeader>
      <div v-if="confirmState?.typeToConfirm" class="grid gap-1.5">
        <Label class="text-xs" for="confirm-typed">Type <span class="font-mono font-semibold">{{ confirmState.typeToConfirm }}</span> to confirm</Label>
        <Input id="confirm-typed" v-model="typed" autocomplete="off" @keyup.enter="typedOk && settle(true)" />
      </div>
      <AlertDialogFooter>
        <AlertDialogCancel @click="settle(false)">Cancel</AlertDialogCancel>
        <AlertDialogAction
          :disabled="!typedOk"
          :class="confirmState?.destructive ? 'bg-destructive text-white hover:bg-destructive/90' : ''"
          @click="settle(true)"
        >{{ confirmState?.actionLabel }}</AlertDialogAction>
      </AlertDialogFooter>
    </AlertDialogContent>
  </AlertDialog>
</template>

<script setup>
import { computed, ref, watch } from 'vue'
import { confirmState, settleConfirm } from '@/lib/confirm'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'

const typed = ref('')
watch(confirmState, () => { typed.value = '' })

const typedOk = computed(
  () => !confirmState.value?.typeToConfirm || typed.value === confirmState.value.typeToConfirm
)

function settle(result) {
  settleConfirm(result)
}

function dismiss() {
  settleConfirm(false)
}
</script>
