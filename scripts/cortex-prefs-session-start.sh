#!/usr/bin/env bash
# Cortex standing-preferences SessionStart hook.
#
# Injects durable user preferences — Cortex memories in namespace "global" tagged
# "preference" — into every session, so standing rules are deterministically in
# front of the agent BEFORE it acts. This is the enforcement the bare
# "global + preference" convention otherwise lacks: a normal cortex_memory_search
# is relevance-ranked and might never surface a preference, whereas this lists
# them by tag (no vector query) and always injects them.
#
# Best-effort and non-blocking: if the cortex CLI is missing, the server is
# unreachable (e.g. VPN down), or nothing is stored, it prints nothing and exits 0
# so it can never delay or break session start.
#
# Install: copy to ~/.claude/hooks/ and register in ~/.claude/settings.json under
# hooks.SessionStart (see the repo README, "Standing preferences" section).
set -uo pipefail

# Locate the cortex CLI: explicit override, ~/bin, or PATH.
CORTEX_BIN="${CORTEX_BIN:-$HOME/bin/cortex}"
if [ ! -x "$CORTEX_BIN" ]; then
  if command -v cortex >/dev/null 2>&1; then CORTEX_BIN="cortex"; else exit 0; fi
fi

# Hard cap so a hung/unreachable server can't stall session start.
TIMEOUT=""
if command -v timeout >/dev/null 2>&1; then TIMEOUT="timeout 8"; fi

# Deterministic, complete listing by tag — no embedding, no relevance cutoff.
prefs=$($TIMEOUT "$CORTEX_BIN" list -n global -t preference -l 100 2>/dev/null) || exit 0

# Nothing came back (server down, or none stored) -> inject nothing. The CLI
# prints the literal "No memories found." sentinel when the list is empty; treat
# that (and pure whitespace) as nothing.
trimmed=$(printf '%s' "$prefs" | tr -d '[:space:]')
if [ -z "$trimmed" ] || [ "$prefs" = "No memories found." ]; then exit 0; fi

read -r -d '' header <<'EOF' || true
# Standing preferences (Cortex: namespace=global, tag=preference)

Durable, user-set preferences pulled deterministically at session start. Treat them
as always in effect for this session, alongside CLAUDE.md. To change one, update the
matching Cortex memory (web UI, or cortex_memory_save with namespace=global,
tags=[preference]) — do not silently ignore it.
EOF

context=$(printf '%s\n\n%s\n' "$header" "$prefs")

# SessionStart hooks add `additionalContext` to the session. jq builds the JSON so
# preference text with quotes/newlines is escaped safely.
jq -nc --arg ctx "$context" \
  '{hookSpecificOutput: {hookEventName: "SessionStart", additionalContext: $ctx}}'
