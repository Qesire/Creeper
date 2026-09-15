#!/usr/bin/env bash
set -euo pipefail

: "${FABRIC_CLOUDFLARE_WORKER_URL:?set FABRIC_CLOUDFLARE_WORKER_URL}"

BASE="${FABRIC_CLOUDFLARE_WORKER_URL%/}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

echo "== cloudflare worker health =="
curl -fsS "${BASE}/healthz"
echo
curl -fsS "${BASE}/meta"
echo

if [[ "${FABRIC_SKIP_WRANGLER_STATUS:-0}" != "1" ]]; then
  cd "${SCRIPT_DIR}"
  WRANGLER="${WRANGLER:-npx --yes wrangler@latest}"
  ${WRANGLER} deployments list
fi
