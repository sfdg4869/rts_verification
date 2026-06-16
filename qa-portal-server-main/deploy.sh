#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

load_env_file() {
  local env_file="$1"
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
    [[ "$line" != *=* ]] && continue
    local key="${line%%=*}"
    local value="${line#*=}"
    export "$key=$value"
  done < "$env_file"
}

cleanup_staged_oracle_zip() {
  if [[ "${ORACLE_ZIP_STAGED_BY_SCRIPT:-0}" == "1" && -f "$STAGED_ORACLE_ZIP" ]]; then
    rm -f "$STAGED_ORACLE_ZIP"
  fi
}

if docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(docker-compose)
else
  echo "docker compose is not installed" >&2
  exit 1
fi

mkdir -p app/resource

if [[ ! -f .env ]]; then
  echo "Missing .env. Copy .env.example and fill in real values first." >&2
  exit 1
fi

load_env_file .env

if [[ $# -ge 1 && -n "${1:-}" ]]; then
  export QA_PORTAL_IMAGE="$1"
fi

if [[ -z "${QA_PORTAL_IMAGE:-}" ]]; then
  echo "QA_PORTAL_IMAGE is not set. Pass an image ref or define it in .env." >&2
  exit 1
fi

COMPOSE_FILES=(-f docker-compose.yml)
STAGED_ORACLE_ZIP="$SCRIPT_DIR/instantclient.zip"
ORACLE_ZIP_STAGED_BY_SCRIPT=0
trap cleanup_staged_oracle_zip EXIT

echo "Deploying image: ${QA_PORTAL_IMAGE}"

if [[ -n "${ORACLE_CLIENT_ZIP_PATH:-}" ]]; then
  if [[ ! -f "${ORACLE_CLIENT_ZIP_PATH}" ]]; then
    echo "ORACLE_CLIENT_ZIP_PATH does not exist: ${ORACLE_CLIENT_ZIP_PATH}" >&2
    exit 1
  fi

  cp "${ORACLE_CLIENT_ZIP_PATH}" "$STAGED_ORACLE_ZIP"
  ORACLE_ZIP_STAGED_BY_SCRIPT=1
  COMPOSE_FILES+=(-f docker-compose.oracle-zip-build.yml)

  echo "Oracle Instant Client zip staged: ${ORACLE_CLIENT_ZIP_PATH}"
  "${COMPOSE_CMD[@]}" "${COMPOSE_FILES[@]}" config >/dev/null
  "${COMPOSE_CMD[@]}" "${COMPOSE_FILES[@]}" build web
else
  "${COMPOSE_CMD[@]}" "${COMPOSE_FILES[@]}" config >/dev/null
  "${COMPOSE_CMD[@]}" "${COMPOSE_FILES[@]}" pull
fi

"${COMPOSE_CMD[@]}" "${COMPOSE_FILES[@]}" up -d --remove-orphans
"${COMPOSE_CMD[@]}" "${COMPOSE_FILES[@]}" ps
