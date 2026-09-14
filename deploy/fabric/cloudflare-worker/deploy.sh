#!/usr/bin/env bash
set -euo pipefail

: "${CREEPER_WORKER_SECRET:?set CREEPER_WORKER_SECRET before deployment}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

if [[ ! -f wrangler.jsonc ]]; then
  cp wrangler.jsonc.example wrangler.jsonc
  echo "created wrangler.jsonc from example; edit coordinator/worker values and rerun" >&2
  exit 2
fi

WRANGLER="${WRANGLER:-npx --yes wrangler@latest}"
printf '%s' "${CREEPER_WORKER_SECRET}" | ${WRANGLER} secret put CREEPER_WORKER_SECRET
${WRANGLER} deploy
