import { ref } from 'vue'

// 'light' | 'dark', persisted; defaults to the OS preference on first visit.
const stored = localStorage.getItem('cortex-theme')
export const theme = ref(
  stored === 'light' || stored === 'dark'
    ? stored
    : window.matchMedia('(prefers-color-scheme: dark)').matches
      ? 'dark'
      : 'light'
)

function apply() {
  document.documentElement.classList.toggle('dark', theme.value === 'dark')
}

export function initTheme() {
  apply()
}

export function toggleTheme() {
  theme.value = theme.value === 'dark' ? 'light' : 'dark'
  localStorage.setItem('cortex-theme', theme.value)
  apply()
}
