#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "run as root" >&2
  exit 2
fi
if [[ "$#" -lt 1 || "$#" -gt 2 ]]; then
  echo "usage: $0 WORKER_ID [--rotate]" >&2
  exit 2
fi

WORKER_ID="$1"
ROTATE="${2:-}"
CREDENTIALS_FILE="${FABRIC_CREDENTIALS_FILE:-/etc/creeper-fabric/workers.json}"

if [[ ! "${WORKER_ID}" =~ ^[A-Za-z0-9._:-]+$ ]]; then
  echo "invalid worker id" >&2
  exit 2
fi
if [[ ! -f "${CREDENTIALS_FILE}" ]]; then
  echo "credentials file not found: ${CREDENTIALS_FILE}" >&2
  exit 2
fi

python3 - "${CREDENTIALS_FILE}" "${WORKER_ID}" "${ROTATE}" <<'PY'
import json
import os
import secrets
import sys
import tempfile

path, worker_id, rotate = sys.argv[1:4]
with open(path, "r", encoding="utf-8") as handle:
    raw = json.load(handle)
if not isinstance(raw, dict):
    raise SystemExit("credentials file is not an object")
if worker_id in raw and rotate != "--rotate":
    raise SystemExit("worker already exists; pass --rotate to replace its secret")

secret = secrets.token_hex(32)
raw[worker_id] = secret
directory = os.path.dirname(path) or "."
fd, temporary = tempfile.mkstemp(prefix=".workers.", dir=directory, text=True)
try:
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(raw, handle, sort_keys=True, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, 0o640)
    os.replace(temporary, path)
finally:
    if os.path.exists(temporary):
        os.unlink(temporary)

print(f"CREEPER_WORKER_SECRET={secret}")
PY

chown root:creeper-fabric "${CREDENTIALS_FILE}" 2>/dev/null || true
chmod 0640 "${CREDENTIALS_FILE}"
systemctl restart creeper-fabric-authority.service
systemctl is-active --quiet creeper-fabric-authority.service
