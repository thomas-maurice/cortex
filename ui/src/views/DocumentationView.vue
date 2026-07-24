<template>
  <div class="space-y-4">
    <h4 class="mb-3 flex items-center gap-2 text-2xl font-semibold">
      <BookOpen class="size-5" />Documentation
    </h4>
    <p class="text-muted-foreground">
      How to install the Cortex MCP server on your machine and wire it into Claude Code and
      Claude&nbsp;Desktop so Claude can save and search your memories. The snippets below are
      filled in with <strong>this server's address</strong> ({{ serverUrl }}).
    </p>

    <!-- ── Overview ─────────────────────────────────────────────── -->
    <Card>
      <CardHeader>
        <CardTitle>How it fits together</CardTitle>
      </CardHeader>
      <CardContent>
        <p class="mb-2">
          Claude talks to a small host-side binary, <code>cortex-mcp</code>, over stdio. That
          binary is a thin client of this server — it holds no database, just the server URL and
          an auth token, and forwards every memory tool call here.
        </p>
        <pre class="mb-0 rounded-md border bg-muted p-2 font-mono text-sm">Claude
  │ stdio
  ▼
cortex-mcp
  │ HTTP
  ▼
cortex-server  (this UI + API)
  │
  ▼
Weaviate / Ollama</pre>
      </CardContent>
    </Card>

    <!-- ── Step 1: install ──────────────────────────────────────── -->
    <Card>
      <CardHeader>
        <CardTitle>1. Install the client binaries</CardTitle>
      </CardHeader>
      <CardContent>
        <p class="mb-2">
          Install (or update) <code>cortex-mcp</code> and the <code>cortex</code> CLI from the
          latest release. Re-run the same command any time to update.
        </p>
        <CodeBlock :text="installCmd" />
        <p class="mb-0 text-sm text-muted-foreground">
          It detects your OS/arch, verifies checksums, and drops the binaries into
          <code>~/bin</code> or <code>~/.local/bin</code>. When it finishes it prints the
          installed paths and a ready-to-paste <code>claude mcp add</code> command for step 3 —
          with the real <code>cortex-mcp</code> path filled in. To build from source instead,
          run <code>make build</code> and use <code>./bin/cortex-mcp</code>.
        </p>
      </CardContent>
    </Card>

    <!-- ── Token ────────────────────────────────────────────────── -->
    <Card>
      <CardHeader>
        <CardTitle>2. Get an auth token</CardTitle>
      </CardHeader>
      <CardContent>
        <template v-if="auth.multiTenant">
          <p class="mb-2">
            This server runs in multi-tenant mode, so each user authenticates with a personal
            API key. Create one in the
            <router-link :to="{ name: 'apikeys' }">API&nbsp;Keys</router-link> tab and copy it —
            it is shown only once. Use it as <code>CORTEX_AUTH_TOKEN</code> below.
          </p>
        </template>
        <template v-else>
          <p class="mb-2">
            This server runs in single-user mode. Authenticate with the shared
            <code>CORTEX_AUTH_TOKEN</code> the server was started with. If the server runs open
            (no token configured), leave <code>CORTEX_AUTH_TOKEN</code> empty or omit it entirely.
          </p>
        </template>
      </CardContent>
    </Card>

    <!-- ── Step 3a: Claude Code ─────────────────────────────────── -->
    <Card>
      <CardHeader>
        <CardTitle>3. Configure Claude Code</CardTitle>
      </CardHeader>
      <CardContent>
        <p class="mb-2">
          Register the server once at <strong>user scope</strong> so it's available in every
          project. The quickest way is the Claude CLI — the install script prints this exact
          command with your real install path; otherwise adjust the <code>cortex-mcp</code> path
          to where step 1 put it:
        </p>
        <CodeBlock :text="claudeMcpAdd" />
        <p class="mb-2">
          That writes the entry into your global config, <code>~/.claude.json</code>, under the
          <code>mcpServers</code> key. To add it by hand instead, put:
        </p>
        <CodeBlock :text="claudeCodeJson" lang="~/.claude.json" />
        <p class="mb-0 text-sm text-muted-foreground">
          In JSON configs <code>command</code> must be an <strong>absolute</strong> path
          (<code>~</code> is not expanded there). A project-scoped <code>.mcp.json</code> in a
          repo works too and is auto-detected when you launch Claude Code from that directory.
        </p>
      </CardContent>
    </Card>

    <!-- ── Step 3b: Claude Desktop ──────────────────────────────── -->
    <Card>
      <CardHeader>
        <CardTitle>4. Configure Claude Desktop</CardTitle>
      </CardHeader>
      <CardContent>
        <p class="mb-2">
          Claude Desktop reads MCP servers from <code>claude_desktop_config.json</code>. Open it
          from <em>Settings → Developer → Edit Config</em>, or edit it directly:
        </p>
        <ul class="mb-2 text-sm">
          <li>macOS: <code>~/Library/Application&nbsp;Support/Claude/claude_desktop_config.json</code></li>
          <li>Windows: <code>%APPDATA%\Claude\claude_desktop_config.json</code></li>
        </ul>
        <p class="mb-2">Add (or merge) this <code>cortex</code> entry:</p>
        <CodeBlock :text="claudeDesktopJson" lang="claude_desktop_config.json" />
        <p class="mb-0 text-sm text-muted-foreground">
          Fully quit and reopen Claude Desktop after saving — it only reloads MCP servers on
          restart.
        </p>
      </CardContent>
    </Card>

    <!-- ── Verify ───────────────────────────────────────────────── -->
    <Card>
      <CardHeader>
        <CardTitle>5. Verify &amp; use it</CardTitle>
      </CardHeader>
      <CardContent>
        <p class="mb-2">
          In Claude Code, run <code>/mcp</code> — you should see <code>cortex</code> connected
          with its tools (<code>cortex_memory_save</code>, <code>cortex_memory_search</code>, and
          the rest). In Claude Desktop, the tools appear under the MCP/tools indicator. Then just
          ask:
        </p>
        <pre class="mb-2 rounded-md border bg-muted p-2 font-mono text-sm">save a memory: I prefer Go for backend services
search your memory for my language preference</pre>
        <p class="mb-0 text-sm text-muted-foreground">
          To make Claude reach for the memory on its own, add the snippet below to your global
          <code>~/.claude/CLAUDE.md</code>.
        </p>
      </CardContent>
    </Card>

    <!-- ── Make Claude use it automatically ─────────────────────── -->
    <Card>
      <CardHeader>
        <CardTitle>6. Make Claude use it automatically</CardTitle>
      </CardHeader>
      <CardContent>
        <p class="mb-2">
          Installing the MCP server gives Claude the memory <em>tools</em>; this snippet makes it
          actually <em>use</em> them — search before answering, save proactively, and keep a
          running session summary. Paste it into your global <code>~/.claude/CLAUDE.md</code>
          (user scope, so it applies in every project).
        </p>
        <CodeBlock :text="claudeMd" lang="~/.claude/CLAUDE.md" />
      </CardContent>
    </Card>

    <!-- ── Automatic recall hook ────────────────────────────────── -->
    <Card>
      <CardHeader>
        <CardTitle>7. Automatic recall on every prompt (recommended)</CardTitle>
      </CardHeader>
      <CardContent>
        <p class="mb-2">
          The CLAUDE.md snippet asks the model to search memory; this hook stops asking. Wired as
          a Claude Code <code>UserPromptSubmit</code> hook, <code>cortex hook recall</code>
          semantically searches your memories for <strong>every prompt you type</strong> and
          injects the best matches into the model's context before it starts working. Add to your
          <code>~/.claude/settings.json</code> (adjust the path to where step 1 installed the
          <code>cortex</code> CLI):
        </p>
        <CodeBlock :text="recallHookJson" lang="~/.claude/settings.json" />
        <p class="mb-2 text-sm text-muted-foreground">
          It reuses the CLI config (<code>~/.config/cortex/cortex.yaml</code>, set up via
          <code>cortex onboard</code>). It is <strong>fail-open and hard-bounded</strong>: the
          whole run is capped by <code>--timeout</code> (default 5s) and any error — server
          unreachable, VPN down — produces no output and exit 0, so it can never block or delay
          a prompt.
        </p>
        <ul class="mb-0 text-sm text-muted-foreground">
          <li>Skips slash commands and prompts shorter than <code>--min-chars</code> (12).</li>
          <li>Injects at most <code>-l</code> 3 memories, each capped at <code>--max-chars</code> 1500, behind a strict <code>-d</code> 0.5 distance cutoff — silence is the common case.</li>
          <li>Long prompts (pasted logs/diffs) are split into ~512-token chunks searched concurrently (<code>--max-query-chunks</code>, default 4) and merged by best distance.</li>
          <li>Per-session dedup: the same memory is never injected twice in one session.</li>
        </ul>
      </CardContent>
    </Card>
  </div>
</template>

<script setup>
import { computed } from 'vue'
import { useAuthStore } from '@/stores/auth'
import CodeBlock from '@/components/CodeBlock.vue'
import { BookOpen } from 'lucide-vue-next'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
// The reflex snippet is kept verbatim in a Markdown asset (it is full of
// backticks and both quote styles, so a JS string literal would be fragile) and
// imported raw. It stays byte-identical to the README's CLAUDE.md block.
import claudeMd from '@/assets/claude-md-snippet.md?raw'

const auth = useAuthStore()

// The server that serves this UI is the same one the MCP client must reach
// (the UI and Connect API share a port), so window.location.origin is the exact
// value to put in CORTEX_SERVER_URL — the snippets are correct for this deploy.
const serverUrl = window.location.origin

// The token placeholder depends on the auth mode: a personal key in MT mode, or
// the shared server token otherwise.
const tokenPlaceholder = auth.multiTenant ? '<your-api-key>' : '<CORTEX_AUTH_TOKEN>'

const installCmd = 'curl -fsSL https://raw.githubusercontent.com/thomas-maurice/cortex/master/scripts/install.sh | bash'

const claudeMcpAdd = computed(() =>
  [
    'claude mcp add --scope user cortex ~/.local/bin/cortex-mcp \\',
    `  -e CORTEX_SERVER_URL=${serverUrl} \\`,
    `  -e CORTEX_AUTH_TOKEN=${tokenPlaceholder} \\`,
    '  -e MEMORY_SOURCE=claude-code',
  ].join('\n')
)

const claudeCodeJson = computed(() => mcpJson('claude-code'))
const claudeDesktopJson = computed(() => mcpJson('claude-desktop'))

// The per-prompt auto-recall hook (step 7). The outer "timeout" is Claude
// Code's own cap on the hook process; the CLI's --timeout (default 5s) fires
// first so the hook exits silently instead of being killed.
const recallHookJson = JSON.stringify(
  {
    hooks: {
      UserPromptSubmit: [
        {
          hooks: [
            { type: 'command', command: '/absolute/path/to/cortex hook recall', timeout: 10 },
          ],
        },
      ],
    },
  },
  null,
  2
)

// mcpJson renders the mcpServers config block shared by Claude Code
// (~/.claude.json) and Claude Desktop (claude_desktop_config.json); only the
// MEMORY_SOURCE tag differs so saves are attributable to the right client.
function mcpJson(source) {
  return JSON.stringify(
    {
      mcpServers: {
        cortex: {
          command: '/absolute/path/to/cortex-mcp',
          env: {
            CORTEX_SERVER_URL: serverUrl,
            CORTEX_AUTH_TOKEN: tokenPlaceholder,
            MEMORY_SOURCE: source,
          },
        },
      },
    },
    null,
    2
  )
}
</script>
