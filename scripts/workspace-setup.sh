#!/bin/bash

set -euo pipefail

readonly WORKSPACE_DIR="${CONDUCTOR_WORKSPACE_PATH:-$(pwd)}"
readonly WORKSPACE_NAME="${CONDUCTOR_WORKSPACE_NAME:-$(basename "$WORKSPACE_DIR")}"
readonly ENV_FILE="$WORKSPACE_DIR/.env.test"

# Redis provides 16 databases by default. Reserve database 0 for development
# outside Conductor and distribute workspaces over databases 1 through 15.
readonly WORKSPACE_CHECKSUM="$(printf '%s' "$WORKSPACE_NAME" | cksum | cut -d' ' -f1)"
readonly GENERATED_REDIS_DB="$((WORKSPACE_CHECKSUM % 15 + 1))"
REDIS_DB="$GENERATED_REDIS_DB"

local_redis_db_from_env() {
  local line

  while IFS= read -r line; do
    if [[ "$line" =~ ^REDIS_URL=redis://(localhost|127\.0\.0\.1):6379/([0-9]+)$ ]]; then
      printf '%s\n' "${BASH_REMATCH[2]}"
      return
    fi
  done < "$ENV_FILE"

  return 1
}

install_cloud_dependencies() {
  local -a packages=()

  command -v cargo >/dev/null 2>&1 || packages+=(rust cargo)
  command -v cc >/dev/null 2>&1 || packages+=(gcc)
  command -v cargo-fmt >/dev/null 2>&1 || packages+=(rustfmt)
  command -v cargo-clippy >/dev/null 2>&1 || packages+=(clippy)
  command -v redis6-server >/dev/null 2>&1 || packages+=(redis6)

  if ((${#packages[@]} > 0)); then
    echo "Installing cloud development dependencies: ${packages[*]}"
    sudo dnf install -y "${packages[@]}"
  fi
}

start_cloud_redis() {
  if redis6-cli -h 127.0.0.1 -p 6379 ping >/dev/null 2>&1; then
    echo "Redis is already running"
    return
  fi

  local runtime_dir="${TMPDIR:-/tmp}"
  echo "Starting Redis..."
  redis6-server \
    --daemonize yes \
    --bind 127.0.0.1 \
    --port 6379 \
    --protected-mode yes \
    --databases 16 \
    --save "" \
    --appendonly no \
    --dir "$runtime_dir" \
    --pidfile "$runtime_dir/oxana-redis.pid" \
    --logfile "$runtime_dir/oxana-redis.log"

  for _ in {1..50}; do
    if redis6-cli -h 127.0.0.1 -p 6379 ping >/dev/null 2>&1; then
      return
    fi
    sleep 0.1
  done

  echo "Redis failed to start. Log output:" >&2
  sed -n '1,160p' "$runtime_dir/oxana-redis.log" >&2
  return 1
}

echo "Setting up workspace: $WORKSPACE_NAME"

if [[ "${CONDUCTOR_IS_LOCAL:-1}" == "0" ]]; then
  install_cloud_dependencies
  start_cloud_redis
fi

if [[ -f "$ENV_FILE" ]]; then
  if configured_redis_db="$(local_redis_db_from_env)"; then
    if [[ "${CONDUCTOR_IS_LOCAL:-1}" == "0" ]] && ((10#$configured_redis_db >= 16)); then
      echo "Updating .env.test to use a database supported by cloud Redis..."
      printf 'REDIS_URL=redis://127.0.0.1:6379/%s\n' "$GENERATED_REDIS_DB" > "$ENV_FILE"
    else
      REDIS_DB="$configured_redis_db"
      echo ".env.test already exists, skipping"
    fi
  else
    echo ".env.test already exists with a custom Redis URL, skipping"
  fi
else
  echo "Creating .env.test..."
  printf 'REDIS_URL=redis://127.0.0.1:6379/%s\n' "$REDIS_DB" > "$ENV_FILE"
fi

echo "Fetching Rust dependencies..."
(cd "$WORKSPACE_DIR" && cargo fetch --locked)

echo
echo "Workspace setup complete!"
echo "  Redis DB: $REDIS_DB"
