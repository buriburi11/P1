#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
WHOIS_PORT=${WHOIS_PORT:-21043}
REDIS_PORT=${REDIS_PORT:-21379}
TIME_PORT=${TIME_PORT:-21037}
FLLM_PORT=${FLLM_PORT:-21904}
PIDS=()
CLEANED=false

cleanup() {
    if [[ "$CLEANED" == true ]]; then return; fi
    CLEANED=true
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    for pid in "${PIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
}

trap cleanup EXIT INT TERM

(cd "$SCRIPT_DIR/services/time" && exec java TimeServer.java "$TIME_PORT") &
PIDS+=("$!")

(cd "$SCRIPT_DIR/services/whois" && exec java WhoisServer.java "$WHOIS_PORT") &
PIDS+=("$!")

(cd "$SCRIPT_DIR/services/redis-resp" && exec java RedisRespServer.java "$REDIS_PORT") &
PIDS+=("$!")

java -jar "$SCRIPT_DIR/services/fllm/eliza-provider.jar" --port "$FLLM_PORT" &
PIDS+=("$!")

printf 'Starting P1 services:\n'
printf '  Time:   127.0.0.1:%s\n' "$TIME_PORT"
printf '  WHOIS:  127.0.0.1:%s\n' "$WHOIS_PORT"
printf '  Redis:  127.0.0.1:%s\n' "$REDIS_PORT"
printf '  fLLM:   http://127.0.0.1:%s/v1/chat/completions\n' "$FLLM_PORT"
printf 'Press Ctrl-C to stop all services.\n'

wait -n "${PIDS[@]}"
