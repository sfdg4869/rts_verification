#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

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

if [[ $# -ge 1 && -n "${1:-}" ]]; then
  export QA_PORTAL_IMAGE="$1"
fi

if [[ -z "${QA_PORTAL_IMAGE:-}" ]]; then
  echo "QA_PORTAL_IMAGE is not set. Pass an image ref or define it in .env." >&2
  exit 1
fi

echo "Deploying image: ${QA_PORTAL_IMAGE}"
"${COMPOSE_CMD[@]}" -f docker-compose.yml config >/dev/null
"${COMPOSE_CMD[@]}" -f docker-compose.yml pull
"${COMPOSE_CMD[@]}" -f docker-compose.yml up -d --remove-orphans
"${COMPOSE_CMD[@]}" -f docker-compose.yml ps
