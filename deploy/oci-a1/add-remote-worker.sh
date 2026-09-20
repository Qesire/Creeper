#!/usr/bin/env bash
set -euo pipefail

if [[ "$(id -u)" -ne 0 ]]; then
  echo "run as root: sudo bash $0 <worker-id>" >&2
  exit 2
fi
[[ $# -eq 1 ]] || {
  echo "usage: sudo bash $0 <worker-id>" >&2
  exit 2
}

WORKER_ID="$1"
CREDENTIALS=/etc/creeper/workers.json
if [[ ! "$WORKER_ID" =~ ^[A-Za-z0-9._:-]+$ ]]; then
  echo "worker id contains unsupported characters" >&2
  exit 2
fi
[[ -s "$CREDENTIALS" ]] || {
  echo "missing Fabric credentials file: $CREDENTIALS" >&2
  exit 2
}

SECRET="$(openssl rand -hex 32)"
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

python3 - "$CREDENTIALS" "$TMP" "$WORKER_ID" "$SECRET" <<'PY'
import json
from pathlib import Path
import sys

source=Path(sys.argv[1])
target=Path(sys.argv[2])
worker_id=sys.argv[3]
secret=sys.argv[4]
raw=json.loads(source.read_text(encoding="utf-8"))
if not isinstance(raw,dict):
    raise SystemExit("workers.json is not an object")
if worker_id in raw:
    raise SystemExit(f"worker id already exists: {worker_id}")
raw[worker_id]=secret
target.write_text(
    json.dumps(raw,sort_keys=True,indent=2)+"\n",
    encoding="utf-8",
)
PY

install -o root -g creeper -m 0640 "$TMP" "$CREDENTIALS"
systemctl restart creeper-fabric-authority.service

printf 'CREEPER_WORKER_ID=%s\n' "$WORKER_ID"
printf 'CREEPER_WORKER_SECRET=%s\n' "$SECRET"
echo "secret printed once; copy it to the remote worker and do not commit it"
