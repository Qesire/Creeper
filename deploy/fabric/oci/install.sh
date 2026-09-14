#!/usr/bin/env bash
set -euo pipefail

export FABRIC_PROFILE="${FABRIC_PROFILE:-oci-explorer}"
: "${FABRIC_REGION:=oci-home}"
export FABRIC_REGION

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec bash "${SCRIPT_DIR}/../vm-worker/install.sh"
