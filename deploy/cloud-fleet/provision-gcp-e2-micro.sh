#!/usr/bin/env bash
set -euo pipefail

PROJECT="${CREEPER_GCP_PROJECT:-}"
ZONE="${CREEPER_GCP_ZONE:-us-central1-a}"
NAME="${CREEPER_VM_NAME:-creeper-gcp-query-01}"
APPLY="${CREEPER_APPLY:-0}"

[[ -n "$PROJECT" ]] || {
  echo "set CREEPER_GCP_PROJECT" >&2
  exit 2
}
command -v gcloud >/dev/null 2>&1 || {
  echo "gcloud CLI is required" >&2
  exit 2
}

CMD=(
  gcloud compute instances create "$NAME"
  "--project=$PROJECT"
  "--zone=$ZONE"
  "--machine-type=e2-micro"
  "--image-family=ubuntu-2404-lts-amd64"
  "--image-project=ubuntu-os-cloud"
  "--boot-disk-size=30GB"
  "--boot-disk-type=pd-standard"
  "--network-tier=STANDARD"
  "--tags=creeper-worker"
)

printf 'About to provision GCP e2-micro. Note: the VM/free disk can qualify for Free Tier in eligible US regions, but in-use external IPv4 is separately billed.\n' >&2
printf 'Command:'
printf ' %q' "${CMD[@]}"
printf '\n'

if [[ "$APPLY" == "1" ]]; then
  [[ "${CREEPER_ALLOW_BILLABLE_IPV4:-0}" == "1" ]] || {
    echo "refusing apply: set CREEPER_ALLOW_BILLABLE_IPV4=1 after reviewing external IPv4 pricing" >&2
    exit 3
  }
  "${CMD[@]}"
else
  echo "dry-run only; set CREEPER_APPLY=1 and CREEPER_ALLOW_BILLABLE_IPV4=1 to execute"
fi
