#!/bin/bash

set -euo pipefail

readonly WORKSPACE_DIR="${CONDUCTOR_WORKSPACE_PATH:-$(pwd)}"
readonly WORKSPACE_NAME="${CONDUCTOR_WORKSPACE_NAME:-$(basename "$WORKSPACE_DIR")}"
readonly ENV_FILE="$WORKSPACE_DIR/.env.test"
readonly WORKSPACE_CHECKSUM="$(printf '%s' "$WORKSPACE_NAME" | cksum | cut -d' ' -f1)"
readonly REDIS_CONTAINER="redis"
REDIS_DB="$((WORKSPACE_CHECKSUM % 15 + 1))"

if [[ -f "$ENV_FILE" ]]; then
  while IFS= read -r line; do
    if [[ "$line" =~ ^REDIS_URL=redis://(localhost|127\.0\.0\.1):6379/([0-9]+)$ ]]; then
      REDIS_DB="${BASH_REMATCH[2]}"
      break
    fi
  done < "$ENV_FILE"
fi

echo "Archiving workspace: $WORKSPACE_NAME"

if command -v redis6-cli >/dev/null 2>&1 \
  && redis6-cli -h 127.0.0.1 -p 6379 ping >/dev/null 2>&1; then
  echo "Flushing Redis database $REDIS_DB..."
  redis6-cli -h 127.0.0.1 -p 6379 -n "$REDIS_DB" FLUSHDB
elif command -v redis-cli >/dev/null 2>&1 \
  && redis-cli -h 127.0.0.1 -p 6379 ping >/dev/null 2>&1; then
  echo "Flushing Redis database $REDIS_DB..."
  redis-cli -h 127.0.0.1 -p 6379 -n "$REDIS_DB" FLUSHDB
elif command -v docker >/dev/null 2>&1 \
  && docker ps --format '{{.Names}}' | grep -qx "$REDIS_CONTAINER"; then
  echo "Flushing Redis database $REDIS_DB..."
  docker exec "$REDIS_CONTAINER" redis-cli -n "$REDIS_DB" FLUSHDB
else
  echo "Redis is not running, skipping cleanup"
fi

echo "Workspace archive complete!"
