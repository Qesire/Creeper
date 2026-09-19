#!/usr/bin/env bash
set -euo pipefail

: "${FABRIC_PUBLIC_URL:?set FABRIC_PUBLIC_URL, e.g. https://fabric.example.com}"

AUTHORITY_CONFIG="${FABRIC_AUTHORITY_CONFIG:-/etc/creeper-fabric/authority.toml}"
CONTROL="${FABRIC_CONTROL:-/opt/creeper-fabric/.venv/bin/creeper-fabric-control}"
PUBLIC="${FABRIC_PUBLIC_URL%/}"

echo "== public authority =="
curl -fsS "${PUBLIC}/healthz"
echo
curl -fsS "${PUBLIC}/meta"
echo

if [[ -x "${CONTROL}" && -r "${AUTHORITY_CONFIG}" ]]; then
  echo "== local authority state =="
  "${CONTROL}" --config "${AUTHORITY_CONFIG}" status
else
  echo "local control unavailable; skipping SQLite-backed status" >&2
fi

for unit in creeper-fabric-authority.service cloudflared.service; do
  if systemctl cat "${unit}" >/dev/null 2>&1; then
    echo "== ${unit} =="
    systemctl is-active "${unit}"
  fi
done
