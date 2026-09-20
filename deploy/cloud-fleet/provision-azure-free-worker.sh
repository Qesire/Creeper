#!/usr/bin/env bash
set -euo pipefail

RESOURCE_GROUP="${CREEPER_AZ_RESOURCE_GROUP:-creeper-workers}"
LOCATION="${CREEPER_AZ_LOCATION:-eastus}"
NAME="${CREEPER_VM_NAME:-creeper-az-b2ats-01}"
SIZE="${CREEPER_AZ_SIZE:-Standard_B2ats_v2}"
ADMIN_USER="${CREEPER_SSH_USER:-ubuntu}"
SSH_KEY="${CREEPER_SSH_PUBLIC_KEY:-$HOME/.ssh/id_ed25519.pub}"
APPLY="${CREEPER_APPLY:-0}"

command -v az >/dev/null 2>&1 || {
  echo "Azure CLI (az) is required" >&2
  exit 2
}
[[ -s "$SSH_KEY" ]] || {
  echo "SSH public key not found: $SSH_KEY" >&2
  exit 2
}
case "$SIZE" in
  Standard_B1s|Standard_B2pts_v2|Standard_B2ats_v2) ;;
  *)
    echo "This helper intentionally accepts only Azure free-account VM sizes: B1s/B2pts_v2/B2ats_v2" >&2
    exit 2
    ;;
esac

GROUP_CMD=(az group create --name "$RESOURCE_GROUP" --location "$LOCATION")
VM_CMD=(
  az vm create
  --resource-group "$RESOURCE_GROUP"
  --name "$NAME"
  --location "$LOCATION"
  --image "Canonical:ubuntu-24_04-lts:server:latest"
  --size "$SIZE"
  --admin-username "$ADMIN_USER"
  --ssh-key-values "$SSH_KEY"
  --os-disk-size-gb 64
  --storage-sku Premium_LRS
  --public-ip-sku Standard
)

printf 'Azure free-account VM benefits are time-limited (12 months for eligible new accounts). Public IP/network items can still create charges; review Cost Management first.\n' >&2
printf 'Command 1:'
printf ' %q' "${GROUP_CMD[@]}"
printf '\nCommand 2:'
printf ' %q' "${VM_CMD[@]}"
printf '\n'

if [[ "$APPLY" == "1" ]]; then
  [[ "${CREEPER_ALLOW_BILLABLE_PUBLIC_IP:-0}" == "1" ]] || {
    echo "refusing apply: set CREEPER_ALLOW_BILLABLE_PUBLIC_IP=1 after reviewing Azure public IP pricing" >&2
    exit 3
  }
  "${GROUP_CMD[@]}"
  "${VM_CMD[@]}"
else
  echo "dry-run only; set CREEPER_APPLY=1 and CREEPER_ALLOW_BILLABLE_PUBLIC_IP=1 to execute"
fi
