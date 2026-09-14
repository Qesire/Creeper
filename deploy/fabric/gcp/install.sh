#!/usr/bin/env bash
set -euo pipefail

export FABRIC_PROFILE="${FABRIC_PROFILE:-gcp-metered}"
if [[ -z "${FABRIC_REGION:-}" ]]; then
  if command -v curl >/dev/null 2>&1; then
    ZONE="$(
      curl -fsS -H 'Metadata-Flavor: Google'         http://metadata.google.internal/computeMetadata/v1/instance/zone         2>/dev/null || true
    )"
    if [[ -n "${ZONE}" ]]; then
      FABRIC_REGION="gcp-${ZONE##*/}"
    fi
  fi
fi
: "${FABRIC_REGION:=gcp-unknown}"
export FABRIC_REGION

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec bash "${SCRIPT_DIR}/../vm-worker/install.sh"
