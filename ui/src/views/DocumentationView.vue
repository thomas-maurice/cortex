<template>
  <div class="documentation">
    <h4 class="mb-3"><font-awesome-icon :icon="['fas', 'book']" class="me-2" />Documentation</h4>
    <p class="text-muted">
      How to install the Cortex MCP server on your machine and wire it into Claude Code and
      Claude&nbsp;Desktop so Claude can save and search your memories. The snippets below are
      filled in with <strong>this server's address</strong> ({{ serverUrl }}).
    </p>

    <!-- ── Overview ─────────────────────────────────────────────── -->
    <section class="card mb-4">
      <div class="card-body">
        <h5 class="card-title">How it fits together</h5>
        <p class="mb-2">
          Claude talks to a small host-side binary, <code>cortex-mcp</code>, over stdio. That
          binary is a thin client of this server — it holds no database, just the server URL and
          an auth token, and forwards every memory tool call here.
        </p>
        <pre class="bg-light border rounded p-2 mb-0 small">Claude ──stdio──► cortex-mcp ──HTTP──► cortex-server (this UI + API) ──► Weaviate / Ollama</pre>
      </div>
    </section>

    <!-- ── Step 1: install ──────────────────────────────────────── -->
    <section class="card mb-4">
      <div class="card-body">
        <h5 class="card-title">1. Install the client binaries</h5>
        <p class="mb-2">
          Install (or update) <code>cortex-mcp</code> and the <code>cortex</code> CLI from the
          latest release. Re-run the same command any time to update.
        </p>
        <CodeBlock :text="installCmd" />
        <p class="text-muted small mb-0">
          It detects your OS/arch, verifies checksums, and drops the binaries into
          <code>~/bin</code> or <code>~/.local/bin</code>. To build from source instead, run
          <code>make build</code> and use <code>./bin/cortex-mcp</code>. Note the full path to
          <code>cortex-mcp</code> — you'll need it below (e.g.
          <code>/usr/local/bin/cortex-mcp</code>).
        </p>
      </div>
    </section>

    <!-- ── Token ────────────────────────────────────────────────── -->
    <section class="card mb-4">
      <div class="card-body">
        <h5 class="card-title">2. Get an auth token</h5>
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
      </div>
    </section>

    <!-- ── Step 3a: Claude Code ─────────────────────────────────── -->
    <section class="card mb-4">
      <div class="card-body">
        <h5 class="card-title">3. Configure Claude Code</h5>
        <p class="mb-2">
          Register the server once at <strong>user scope</strong> so it's available in every
          project. The quickest way is the Claude CLI:
        </p>
        <CodeBlock :text="claudeMcpAdd" />
        <p class="mb-2">
          That writes the entry into your global config, <code>~/.claude.json</code>, under the
          <code>mcpServers</code> key. To add it by hand instead, put:
        </p>
        <CodeBlock :text="claudeCodeJson" lang="~/.claude.json" />
        <p class="text-muted small mb-0">
          A project-scoped <code>.mcp.json</code> in a repo works too and is auto-detected when
          you launch Claude Code from that directory.
        </p>
      </div>
    </section>

    <!-- ── Step 3b: Claude Desktop ──────────────────────────────── -->
    <section class="card mb-4">
      <div class="card-body">
        <h5 class="card-title">4. Configure Claude Desktop</h5>
        <p class="mb-2">
          Claude Desktop reads MCP servers from <code>claude_desktop_config.json</code>. Open it
          from <em>Settings → Developer → Edit Config</em>, or edit it directly:
        </p>
        <ul class="small mb-2">
          <li>macOS: <code>~/Library/Application&nbsp;Support/Claude/claude_desktop_config.json</code></li>
          <li>Windows: <code>%APPDATA%\Claude\claude_desktop_config.json</code></li>
        </ul>
        <p class="mb-2">Add (or merge) this <code>cortex</code> entry:</p>
        <CodeBlock :text="claudeDesktopJson" lang="claude_desktop_config.json" />
        <p class="text-muted small mb-0">
          Fully quit and reopen Claude Desktop after saving — it only reloads MCP servers on
          restart.
        </p>
      </div>
    </section>

    <!-- ── Verify ───────────────────────────────────────────────── -->
    <section class="card mb-4">
      <div class="card-body">
        <h5 class="card-title">5. Verify &amp; use it</h5>
        <p class="mb-2">
          In Claude Code, run <code>/mcp</code> — you should see <code>cortex</code> connected
          with its tools (<code>cortex_memory_save</code>, <code>cortex_memory_search</code>, and
          the rest). In Claude Desktop, the tools appear under the MCP/tools indicator. Then just
          ask:
        </p>
        <pre class="bg-light border rounded p-2 mb-2 small">save a memory: I prefer Go for backend services
search your memory for my language preference</pre>
        <p class="text-muted small mb-0">
          To make Claude reach for the memory on its own, add the snippet below to your global
          <code>~/.claude/CLAUDE.md</code>.
        </p>
      </div>
    </section>

    <!-- ── Make Claude use it automatically ─────────────────────── -->
    <section class="card mb-4">
      <div class="card-body">
        <h5 class="card-title">6. Make Claude use it automatically</h5>
        <p class="mb-2">
          Installing the MCP server gives Claude the memory <em>tools</em>; this snippet makes it
          actually <em>use</em> them — search before answering, save proactively, and keep a
          running session summary. Paste it into your global <code>~/.claude/CLAUDE.md</code>
          (user scope, so it applies in every project).
        </p>
        <CodeBlock :text="claudeMd" lang="~/.claude/CLAUDE.md" />
      </div>
    </section>
  </div>
</template>

<script setup>
import { computed } from 'vue'
import { useAuthStore } from '@/stores/auth'
import CodeBlock from '@/components/CodeBlock.vue'
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
    'claude mcp add --scope user cortex /usr/local/bin/cortex-mcp \\',
    `  -e CORTEX_SERVER_URL=${serverUrl} \\`,
    `  -e CORTEX_AUTH_TOKEN=${tokenPlaceholder} \\`,
    '  -e MEMORY_SOURCE=claude-code',
  ].join('\n')
)

const claudeCodeJson = computed(() => mcpJson('claude-code'))
const claudeDesktopJson = computed(() => mcpJson('claude-desktop'))

// mcpJson renders the mcpServers config block shared by Claude Code
// (~/.claude.json) and Claude Desktop (claude_desktop_config.json); only the
// MEMORY_SOURCE tag differs so saves are attributable to the right client.
function mcpJson(source) {
  return JSON.stringify(
    {
      mcpServers: {
        cortex: {
          command: '/usr/local/bin/cortex-mcp',
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
