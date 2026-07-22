import { ref } from 'vue'

// Promise-based confirm backed by the shadcn AlertDialog that ConfirmHost.vue
// (mounted once in App.vue) renders. Replaces native confirm() so dialogs
// match the app style: `if (!(await confirmDialog('Delete?'))) return`.
export const confirmState = ref(null)

export function confirmDialog(message, opts = {}) {
  return new Promise((resolve) => {
    confirmState.value = {
      message,
      title: opts.title || 'Are you sure?',
      actionLabel: opts.actionLabel || 'Confirm',
      destructive: opts.destructive !== false, // most confirms here guard deletes
      // When set, the user must type this exact string to enable the action —
      // used for irreversible bulk deletes (e.g. a whole namespace).
      typeToConfirm: opts.typeToConfirm || null,
      resolve,
    }
  })
}

// Called by ConfirmHost when the user picks an outcome (or dismisses).
export function settleConfirm(result) {
  if (confirmState.value) {
    confirmState.value.resolve(result)
    confirmState.value = null
  }
}
