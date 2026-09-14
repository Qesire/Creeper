#!/usr/bin/env bash
set -euo pipefail

CONFIG="${FABRIC_WORKER_CONFIG:-/etc/creeper-fabric/worker.toml}"
SERVICE="${FABRIC_WORKER_SERVICE:-creeper-fabric-worker.service}"

[[ -r "${CONFIG}" ]] || {
  echo "worker config not readable: ${CONFIG}" >&2
  exit 2
}

COORDINATOR="$(
  sed -n 's/^[[:space:]]*coordinator_url[[:space:]]*=[[:space:]]*"([^"]*)".*/\1/p' "${CONFIG}" |
    head -n1
)"
WORKER_ID="$(
  sed -n 's/^[[:space:]]*worker_id[[:space:]]*=[[:space:]]*"([^"]*)".*/\1/p' "${CONFIG}" |
    head -n1
)"
REGION="$(
  sed -n 's/^[[:space:]]*region[[:space:]]*=[[:space:]]*"([^"]*)".*/\1/p' "${CONFIG}" |
    head -n1
)"

[[ -n "${COORDINATOR}" && -n "${WORKER_ID}" && -n "${REGION}" ]] || {
  echo "worker config missing coordinator/worker_id/region" >&2
  exit 2
}

echo "worker_id=${WORKER_ID}"
echo "region=${REGION}"
echo "coordinator=${COORDINATOR}"

curl -fsS "${COORDINATOR%/}/healthz"
echo
curl -fsS "${COORDINATOR%/}/meta"
echo

systemctl is-active "${SERVICE}"
journalctl -u "${SERVICE}" -n 30 --no-pager
