<template>
  <div class="min-h-screen flex flex-col">
    <header v-if="auth.checkAuth()" class="border-b bg-background sticky top-0 z-40">
      <nav class="flex h-14 items-center gap-1 px-4">
        <router-link
          :to="{ name: 'memories' }"
          class="mr-4 flex items-center gap-2 font-semibold tracking-tight"
        >
          <Brain class="size-5" />Cortex
        </router-link>

        <!-- Core memory work — kept top-level since it's used constantly. -->
        <router-link
          v-for="item in coreNav"
          :key="item.name"
          :to="{ name: item.name }"
          class="rounded-md px-3 py-1.5 text-sm font-medium transition-colors hover:bg-accent hover:text-accent-foreground"
          :class="$route.name === item.name ? 'bg-accent text-accent-foreground' : 'text-muted-foreground'"
        >{{ item.label }}</router-link>

        <!-- Organize: occasional data-management views. -->
        <DropdownMenu>
          <DropdownMenuTrigger
            class="flex items-center gap-1 rounded-md px-3 py-1.5 text-sm font-medium transition-colors hover:bg-accent hover:text-accent-foreground"
            :class="organizeRoutes.includes($route.name) ? 'bg-accent text-accent-foreground' : 'text-muted-foreground'"
          >Organize<ChevronDown class="size-3.5" /></DropdownMenuTrigger>
          <DropdownMenuContent align="start">
            <DropdownMenuItem as-child><router-link :to="{ name: 'sessions' }">Sessions</router-link></DropdownMenuItem>
            <DropdownMenuItem as-child><router-link :to="{ name: 'namespaces' }">Namespaces</router-link></DropdownMenuItem>
            <DropdownMenuItem as-child><router-link :to="{ name: 'preferences' }">Preferences</router-link></DropdownMenuItem>
            <DropdownMenuItem as-child><router-link :to="{ name: 'backup' }">Backup</router-link></DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>

        <!-- System: operational / admin views, rarely visited. Status is always
             present so this dropdown always renders; the other items are gated.
             (Server enforces the gates regardless.) -->
        <DropdownMenu>
          <DropdownMenuTrigger
            class="flex items-center gap-1 rounded-md px-3 py-1.5 text-sm font-medium transition-colors hover:bg-accent hover:text-accent-foreground"
            :class="systemRoutes.includes($route.name) ? 'bg-accent text-accent-foreground' : 'text-muted-foreground'"
          >System<ChevronDown class="size-3.5" /></DropdownMenuTrigger>
          <DropdownMenuContent align="start">
            <DropdownMenuItem as-child><router-link :to="{ name: 'status' }">Status</router-link></DropdownMenuItem>
            <DropdownMenuItem v-if="!auth.multiTenant || auth.isAdmin" as-child>
              <router-link :to="{ name: 'queue' }">Indexing</router-link>
            </DropdownMenuItem>
            <DropdownMenuItem v-if="auth.multiTenant" as-child>
              <router-link :to="{ name: 'apikeys' }">API Keys</router-link>
            </DropdownMenuItem>
            <DropdownMenuItem v-if="auth.multiTenant && auth.isAdmin" as-child>
              <router-link :to="{ name: 'users' }">Users</router-link>
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>

        <div class="ml-auto flex items-center gap-2">
          <!-- Docs sits apart on the right — a one-time reference, not part of
               the daily flow. -->
          <router-link
            :to="{ name: 'documentation' }"
            class="flex items-center gap-1.5 rounded-md px-3 py-1.5 text-sm font-medium transition-colors hover:bg-accent hover:text-accent-foreground"
            :class="$route.name === 'documentation' ? 'bg-accent text-accent-foreground' : 'text-muted-foreground'"
          ><BookOpen class="size-4" />Docs</router-link>

          <span v-if="auth.username" class="flex items-center gap-1.5 text-sm text-muted-foreground">
            <UserIcon class="size-4" />{{ auth.username }}
            <Badge v-if="auth.isAdmin" variant="secondary">admin</Badge>
          </span>

          <Button variant="ghost" size="icon" aria-label="Toggle theme" @click="toggleTheme">
            <Sun v-if="theme === 'dark'" class="size-4" />
            <Moon v-else class="size-4" />
          </Button>
          <Button variant="outline" size="sm" @click="logout">
            <LogOut class="size-4" />Logout
          </Button>
        </div>
      </nav>
    </header>

    <main class="flex-1 p-6">
      <router-view />
    </main>

    <footer class="py-4 text-center text-sm text-muted-foreground">
      <a
        href="https://github.com/thomas-maurice/cortex"
        target="_blank"
        rel="noopener noreferrer"
        class="inline-flex items-center gap-1.5 hover:text-foreground transition-colors"
      >
        <Github class="size-4" />thomas-maurice/cortex
      </a>
    </footer>
  </div>
</template>

<script setup>
import { useRouter } from 'vue-router'
import { useAuthStore } from '@/stores/auth'
import { theme, toggleTheme } from '@/lib/theme'
import { Brain, BookOpen, ChevronDown, Github, LogOut, Moon, Sun, User as UserIcon } from 'lucide-vue-next'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'

const auth = useAuthStore()
const router = useRouter()

const coreNav = [
  { name: 'memories', label: 'Memories' },
  { name: 'graph', label: 'Graph' },
  { name: 'explore', label: 'Explore' },
]

// Route names grouped under each nav dropdown, so the toggle shows an active
// state when one of its children is the current route.
const organizeRoutes = ['sessions', 'namespaces', 'preferences', 'backup']
const systemRoutes = ['status', 'queue', 'apikeys', 'users']

function logout() {
  auth.logout()
  router.push({ name: 'login' })
}
</script>
