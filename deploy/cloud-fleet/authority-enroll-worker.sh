#!/usr/bin/env bash
set -euo pipefail

if [[ "$(id -u)" -ne 0 ]]; then
  echo "run as root: sudo bash $0 <worker-id> <worker-wg-ip/32> <worker-wg-public-key>" >&2
  exit 2
fi
[[ $# -eq 3 ]] || {
  echo "usage: sudo bash $0 <worker-id> <worker-wg-ip/32> <worker-wg-public-key>" >&2
  exit 2
}

WORKER_ID="$1"
WORKER_WG_IP="$2"
WORKER_WG_PUBLIC_KEY="$3"
WG_CONFIG=/etc/wireguard/creeper.conf
ADD_WORKER=/opt/creeper/deploy/oci-a1/add-remote-worker.sh

[[ -s "$WG_CONFIG" ]] || {
  echo "missing Authority WireGuard config: $WG_CONFIG" >&2
  exit 2
}
[[ -x "$ADD_WORKER" || -f "$ADD_WORKER" ]] || {
  echo "missing worker enrollment helper: $ADD_WORKER" >&2
  exit 2
}
[[ "$WORKER_ID" =~ ^[A-Za-z0-9._:-]+$ ]] || {
  echo "invalid worker id" >&2
  exit 2
}

BARE_IP="$(python3 - "$WORKER_WG_IP" <<'PY'
import ipaddress
import sys

value=ipaddress.ip_interface(sys.argv[1])
network=ipaddress.ip_network("10.77.0.0/24")
if value.version != 4 or value.network.prefixlen != 32:
    raise SystemExit("worker WireGuard address must be IPv4 /32")
if value.ip not in network or value.ip == ipaddress.ip_address("10.77.0.1"):
    raise SystemExit("worker address must be in 10.77.0.0/24 and not 10.77.0.1")
print(value.ip)
PY
)"

if grep -Fq "PublicKey = $WORKER_WG_PUBLIC_KEY" "$WG_CONFIG"; then
  echo "WireGuard public key already exists in Authority config" >&2
  exit 2
fi
if grep -Eq "AllowedIPs[[:space:]]*=[[:space:]]*$BARE_IP/32([[:space:]]|$)" "$WG_CONFIG"; then
  echo "WireGuard address already exists in Authority config: $BARE_IP" >&2
  exit 2
fi

CREDENTIAL_OUTPUT="$(bash "$ADD_WORKER" "$WORKER_ID")"

TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT
cat "$WG_CONFIG" > "$TMP"
cat >> "$TMP" <<EOF

# Creeper worker: $WORKER_ID
[Peer]
PublicKey = $WORKER_WG_PUBLIC_KEY
AllowedIPs = $BARE_IP/32
EOF
install -o root -g root -m 0600 "$TMP" "$WG_CONFIG"

if ip link show creeper >/dev/null 2>&1; then
  wg set creeper peer "$WORKER_WG_PUBLIC_KEY" allowed-ips "$BARE_IP/32"
fi

printf '%s\n' "$CREDENTIAL_OUTPUT"
printf 'CREEPER_WORKER_WG_IP=%s/32\n' "$BARE_IP"
echo "Authority peer enrolled. Copy the HMAC secret only to the intended worker."
