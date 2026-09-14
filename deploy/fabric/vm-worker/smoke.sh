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
META_JSON="$(curl -fsS "${COORDINATOR%/}/meta")"
printf '%s\n' "${META_JSON}"
PYTHON_BIN="${FABRIC_PYTHON:-/opt/creeper-fabric/.venv/bin/python}"
if [[ -x "${PYTHON_BIN}" ]]; then
  AUTHORITY_TIME="$(
    printf '%s' "${META_JSON}" |
      "${PYTHON_BIN}" -c \
        'import json,sys; print(int(float(json.load(sys.stdin)["server_unix_time"])))'
  )"
  LOCAL_TIME="$(date +%s)"
  CLOCK_SKEW=$(( LOCAL_TIME > AUTHORITY_TIME ? LOCAL_TIME - AUTHORITY_TIME : AUTHORITY_TIME - LOCAL_TIME ))
  MAX_CLOCK_SKEW="${FABRIC_DEPLOY_MAX_CLOCK_SKEW_SECONDS:-240}"
  echo "clock_skew_seconds=${CLOCK_SKEW}"
  if (( CLOCK_SKEW > MAX_CLOCK_SKEW )); then
    echo "clock skew too large for Fabric HMAC" >&2
    exit 2
  fi
fi
echo

systemctl is-active "${SERVICE}"
journalctl -u "${SERVICE}" -n 30 --no-pager
