#!/usr/bin/env bash
# Throwaway Weaviate for the store/rpc integration tests: one container PER RUN,
# random host ports, removed on exit. Never share one between concurrent runs.
#
#   scripts/wv-test.sh run [go test args...]   start, run `go test`, always clean up
#   scripts/wv-test.sh start                   start; prints `export` lines (eval them)
#   scripts/wv-test.sh stop <container-name>   remove a container from `start`
#
# `run` defaults to `./internal/store`; extra args replace that, e.g.
#   scripts/wv-test.sh run ./internal/store
#   CORTEX_TEST_MULTI_TENANT=1 scripts/wv-test.sh run -p 1 ./internal/store ./internal/rpc
# (the rpc integration test only runs in MT mode)
set -euo pipefail

IMAGE="${WV_IMAGE:-cr.weaviate.io/semitechnologies/weaviate:1.38.0}"

start() {
  local name="wv-test-$$-$RANDOM"
  # For `run`, arm cleanup before the container exists so Ctrl-C during the
  # readiness wait cannot leak it.
  [ "${WV_CLEANUP:-}" = 1 ] && trap 'docker rm -f "'"$name"'" >/dev/null 2>&1 || true' EXIT
  docker run -d --name "$name" -p 127.0.0.1::8080 -p 127.0.0.1::50051 \
    -e AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
    -e PERSISTENCE_DATA_PATH=/var/lib/weaviate \
    -e AUTOSCHEMA_ENABLED=false \
    -e DEFAULT_VECTORIZER_MODULE=none -e ENABLE_MODULES="" \
    "$IMAGE" >/dev/null
  local rest grpc
  rest=$(docker port "$name" 8080/tcp | head -n1)
  grpc=$(docker port "$name" 50051/tcp | head -n1)
  for _ in $(seq 1 60); do
    curl -fs "http://$rest/v1/.well-known/ready" >/dev/null 2>&1 && break
    sleep 1
  done
  curl -fs "http://$rest/v1/.well-known/ready" >/dev/null || { docker rm -f "$name" >/dev/null; echo "weaviate not ready" >&2; exit 1; }
  WV_NAME=$name
  WV_REST=${rest/0.0.0.0/127.0.0.1}
  WV_GRPC=${grpc/0.0.0.0/127.0.0.1}
}

case "${1:-}" in
  start)
    start
    echo "export CORTEX_TEST_WEAVIATE_REST=$WV_REST CORTEX_TEST_WEAVIATE_GRPC=$WV_GRPC"
    echo "# cleanup: scripts/wv-test.sh stop $WV_NAME" >&2
    ;;
  stop)
    docker rm -f "${2:?container name}" >/dev/null
    ;;
  run)
    shift
    [ $# -gt 0 ] || set -- ./internal/store
    WV_CLEANUP=1 start
    CORTEX_TEST_WEAVIATE_REST=$WV_REST CORTEX_TEST_WEAVIATE_GRPC=$WV_GRPC go test -count=1 "$@"
    ;;
  *)
    sed -n '2,/^set -euo/p' "$0" | sed '$d' >&2
    exit 2
    ;;
esac
