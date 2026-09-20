#!/usr/bin/env bash
set -euo pipefail

for name in OCI_COMPARTMENT_OCID OCI_SUBNET_OCID OCI_IMAGE_OCID OCI_AVAILABILITY_DOMAIN; do
  [[ -n "${!name:-}" ]] || {
    echo "set $name" >&2
    exit 2
  }
done
command -v oci >/dev/null 2>&1 || {
  echo "OCI CLI is required" >&2
  exit 2
}

NAME="${CREEPER_VM_NAME:-creeper-oci-e2micro-01}"
SHAPE="${CREEPER_OCI_SHAPE:-VM.Standard.E2.1.Micro}"
SSH_KEY="${CREEPER_SSH_PUBLIC_KEY:-$HOME/.ssh/id_ed25519.pub}"
APPLY="${CREEPER_APPLY:-0}"
[[ -s "$SSH_KEY" ]] || {
  echo "SSH public key not found: $SSH_KEY" >&2
  exit 2
}

case "$SHAPE" in
  VM.Standard.E2.1.Micro)
    SHAPE_ARGS=()
    ;;
  VM.Standard.A1.Flex)
    OCPUS="${CREEPER_OCI_OCPUS:-1}"
    MEMORY_GB="${CREEPER_OCI_MEMORY_GB:-6}"
    SHAPE_ARGS=(--shape-config "{\"ocpus\":$OCPUS,\"memoryInGBs\":$MEMORY_GB}")
    ;;
  *)
    echo "helper accepts only OCI Always Free eligible E2.1.Micro or A1.Flex shapes" >&2
    exit 2
    ;;
esac

METADATA="$(python3 - "$SSH_KEY" <<'PY'
import json
from pathlib import Path
import sys
print(json.dumps({"ssh_authorized_keys": Path(sys.argv[1]).read_text().strip()}))
PY
)"

CMD=(
  oci compute instance launch
  --compartment-id "$OCI_COMPARTMENT_OCID"
  --availability-domain "$OCI_AVAILABILITY_DOMAIN"
  --subnet-id "$OCI_SUBNET_OCID"
  --image-id "$OCI_IMAGE_OCID"
  --shape "$SHAPE"
  "${SHAPE_ARGS[@]}"
  --display-name "$NAME"
  --assign-public-ip true
  --boot-volume-size-in-gbs 47
  --metadata "$METADATA"
)

printf 'OCI Always Free capacity and the 200GB total block-volume quota are tenancy-wide. Confirm the Authority volume leaves room before creating workers.\n' >&2
printf 'Command:'
printf ' %q' "${CMD[@]}"
printf '\n'

if [[ "$APPLY" == "1" ]]; then
  "${CMD[@]}"
else
  echo "dry-run only; set CREEPER_APPLY=1 to execute"
fi
