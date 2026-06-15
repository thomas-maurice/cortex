#!/usr/bin/env bash
#
# Copy every memory from a SOURCE Cortex into a TARGET Cortex (e.g. prod -> a
# local dev stack) so you can test against real data. It is a dump + restore:
#
#   cortex export   (SOURCE)  -> JSON
#   cortex import   (TARGET)  -> republished onto the target's NATS index queue
#
# The restore goes through the TARGET's normal ingest path: the worker re-embeds
# each memory with ITS model and upserts it. Vectors are never copied, so this is
# safe even if the two deployments run different embedding models. Ids, namespace,
# tags, createdAt, links and not-duplicate decisions are preserved; an existing id
# on the target is overwritten.
#
# Usage:
#   SRC_SERVER=https://cortex.lil.maurice.fr SRC_TOKEN=... \
#   DST_SERVER=http://localhost:8088        DST_TOKEN=... \
#   scripts/prod-to-dev.sh
#
# NOTE: memories may contain sensitive data — only restore into a stack you trust.
set -euo pipefail

SRC_SERVER="${SRC_SERVER:?set SRC_SERVER, e.g. https://cortex.lil.maurice.fr}"
DST_SERVER="${DST_SERVER:?set DST_SERVER, e.g. http://localhost:8088}"
SRC_TOKEN="${SRC_TOKEN:-}"
DST_TOKEN="${DST_TOKEN:-}"
CORTEX_BIN="${CORTEX_BIN:-cortex}"   # must be new enough to have `import`
NAMESPACE="${NAMESPACE:-*}"          # "*" = all namespaces

command -v "$CORTEX_BIN" >/dev/null 2>&1 || { echo "error: '$CORTEX_BIN' not found on PATH" >&2; exit 1; }

dump="$(mktemp -t cortex-dump.XXXXXX)"
trap 'rm -f "$dump"' EXIT

echo "==> exporting from $SRC_SERVER (namespace=$NAMESPACE)"
"$CORTEX_BIN" --server "$SRC_SERVER" ${SRC_TOKEN:+--token "$SRC_TOKEN"} export -n "$NAMESPACE" -o "$dump"

echo "==> importing into $DST_SERVER (via its NATS ingest queue)"
"$CORTEX_BIN" --server "$DST_SERVER" ${DST_TOKEN:+--token "$DST_TOKEN"} import "$dump"

echo "==> done: $SRC_SERVER -> $DST_SERVER"
echo "    The target worker is re-embedding now; check progress with:"
echo "    $CORTEX_BIN --server $DST_SERVER status   # watch the memory count climb"
