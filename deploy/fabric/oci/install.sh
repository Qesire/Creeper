#!/usr/bin/env bash
set -euo pipefail

export FABRIC_PROFILE="${FABRIC_PROFILE:-oci-explorer}"
if [[ -z "${FABRIC_REGION:-}" ]] && command -v curl >/dev/null 2>&1; then
  OCI_REGION="$(
    curl -fsS --max-time 2 \
      -H 'Authorization: Bearer Oracle' \
      http://169.254.169.254/opc/v2/instance/canonicalRegionName \
      2>/dev/null || true
  )"
  if [[ -n "${OCI_REGION}" ]]; then
    FABRIC_REGION="oci-${OCI_REGION}"
  fi
fi
: "${FABRIC_REGION:=oci-unknown}"
export FABRIC_REGION

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec bash "${SCRIPT_DIR}/../vm-worker/install.sh"
