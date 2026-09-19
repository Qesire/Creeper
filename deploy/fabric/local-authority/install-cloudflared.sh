#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "run as root" >&2
  exit 2
fi

if [[ -n "${FABRIC_TUNNEL_TOKEN_FILE:-}" ]]; then
  if [[ ! -f "${FABRIC_TUNNEL_TOKEN_FILE}" ]]; then
    echo "tunnel token file not found" >&2
    exit 2
  fi
  FABRIC_TUNNEL_TOKEN="$(tr -d '\r\n' < "${FABRIC_TUNNEL_TOKEN_FILE}")"
else
  FABRIC_TUNNEL_TOKEN="${FABRIC_TUNNEL_TOKEN:-}"
fi
if [[ -z "${FABRIC_TUNNEL_TOKEN}" ]]; then
  echo "set FABRIC_TUNNEL_TOKEN_FILE (preferred) or FABRIC_TUNNEL_TOKEN" >&2
  exit 2
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends ca-certificates curl

if ! command -v cloudflared >/dev/null 2>&1; then
  install -d -m 0755 /usr/share/keyrings
  curl -fsSL https://pkg.cloudflare.com/cloudflare-main.gpg     > /usr/share/keyrings/cloudflare-main.gpg
  cat > /etc/apt/sources.list.d/cloudflared.list <<'EOF'
deb [signed-by=/usr/share/keyrings/cloudflare-main.gpg] https://pkg.cloudflare.com/cloudflared any main
EOF
  apt-get update
  apt-get install -y cloudflared
fi

if systemctl cat cloudflared.service >/dev/null 2>&1; then
  echo "cloudflared.service already exists; preserving installed tunnel token"
  systemctl enable --now cloudflared.service
else
  cloudflared service install "${FABRIC_TUNNEL_TOKEN}"
  systemctl enable --now cloudflared.service
fi

systemctl is-active --quiet cloudflared.service
echo "cloudflared is active"
if [[ -n "${FABRIC_PUBLIC_URL:-}" ]]; then
  curl -fsS "${FABRIC_PUBLIC_URL%/}/healthz"
  echo
fi
