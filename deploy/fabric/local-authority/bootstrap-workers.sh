#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "run as root" >&2
  exit 2
fi

CREDENTIALS_FILE="${FABRIC_CREDENTIALS_FILE:-/etc/creeper-fabric/workers.json}"
OUTPUT_DIR="${FABRIC_SECRET_OUTPUT_DIR:-/root/creeper-fabric-secrets}"
OCI_ID="${FABRIC_OCI_WORKER_ID:-oci-01}"
GCP_ID="${FABRIC_GCP_WORKER_ID:-gcp-01}"
CF_ID="${FABRIC_CF_WORKER_ID:-cf-thin-01}"
ROTATE="${FABRIC_ROTATE_WORKER_SECRETS:-0}"

[[ -f "${CREDENTIALS_FILE}" ]] || {
  echo "credentials file not found: ${CREDENTIALS_FILE}" >&2
  exit 2
}

for worker_id in "${OCI_ID}" "${GCP_ID}" "${CF_ID}"; do
  if [[ ! "${worker_id}" =~ ^[A-Za-z0-9._:-]+$ ]]; then
    echo "invalid worker id: ${worker_id}" >&2
    exit 2
  fi
done

install -d -m 0700 "${OUTPUT_DIR}"

python3 -   "${CREDENTIALS_FILE}" "${OUTPUT_DIR}" "${ROTATE}"   "${OCI_ID}" "${GCP_ID}" "${CF_ID}" <<'PY'
import json
import os
import secrets
import sys
import tempfile
from pathlib import Path

credentials_path = Path(sys.argv[1])
output_dir = Path(sys.argv[2])
rotate = sys.argv[3] == "1"
worker_ids = sys.argv[4:]

with credentials_path.open("r", encoding="utf-8") as handle:
    raw = json.load(handle)
if not isinstance(raw, dict):
    raise SystemExit("credentials file must contain one JSON object")

duplicates = [worker_id for worker_id in worker_ids if worker_id in raw]
if duplicates and not rotate:
    raise SystemExit(
        "worker identities already exist; set FABRIC_ROTATE_WORKER_SECRETS=1: "
        + ",".join(duplicates)
    )

generated: dict[str, str] = {}
for worker_id in worker_ids:
    secret = secrets.token_hex(32)
    raw[worker_id] = secret
    generated[worker_id] = secret

fd, temporary = tempfile.mkstemp(
    prefix=".workers.",
    dir=str(credentials_path.parent),
    text=True,
)
try:
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(raw, handle, sort_keys=True, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, 0o640)
    os.replace(temporary, credentials_path)
finally:
    if os.path.exists(temporary):
        os.unlink(temporary)

for worker_id, secret in generated.items():
    destination = output_dir / f"{worker_id}.secret"
    fd, temporary = tempfile.mkstemp(
        prefix=f".{worker_id}.",
        dir=str(output_dir),
        text=True,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(f"CREEPER_WORKER_SECRET={secret}\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, destination)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    print(destination)
PY

chown root:creeper-fabric "${CREDENTIALS_FILE}" 2>/dev/null || true
chmod 0640 "${CREDENTIALS_FILE}"
chmod 0700 "${OUTPUT_DIR}"
find "${OUTPUT_DIR}" -maxdepth 1 -type f -name '*.secret' -exec chmod 0600 {} +

systemctl restart creeper-fabric-authority.service
systemctl is-active --quiet creeper-fabric-authority.service

echo "worker credentials provisioned; transfer each .secret file only to its matching node"
