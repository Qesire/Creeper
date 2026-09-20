#!/usr/bin/env bash
set -euo pipefail

if [[ "$(id -u)" -ne 0 ]]; then
  echo "run as root: sudo -E bash $0" >&2
  exit 2
fi

required=(
  CREEPER_AUTHORITY_WG_PUBLIC_KEY
  CREEPER_AUTHORITY_ENDPOINT
  CREEPER_WORKER_WG_IP
)
missing=()
for name in "${required[@]}"; do
  [[ -n "${!name:-}" ]] || missing+=("$name")
done
if (( ${#missing[@]} )); then
  echo "missing environment: ${missing[*]}" >&2
  exit 2
fi

python3 - "$CREEPER_WORKER_WG_IP" <<'PY'
import ipaddress
import sys

value=ipaddress.ip_interface(sys.argv[1])
if value.version != 4 or value.network.prefixlen != 32:
    raise SystemExit("CREEPER_WORKER_WG_IP must be an IPv4 /32")
network=ipaddress.ip_network("10.77.0.0/24")
if value.ip not in network or value.ip == ipaddress.ip_address("10.77.0.1"):
    raise SystemExit("worker WireGuard IP must be within 10.77.0.0/24 and not 10.77.0.1")
PY

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y iproute2 wireguard-tools

install -d -o root -g root -m 0700 /etc/wireguard
PRIVATE_KEY_FILE=/etc/wireguard/creeper-worker.key
PUBLIC_KEY_FILE=/etc/wireguard/creeper-worker.pub

if [[ ! -s "$PRIVATE_KEY_FILE" ]]; then
  umask 077
  wg genkey > "$PRIVATE_KEY_FILE"
fi
wg pubkey < "$PRIVATE_KEY_FILE" > "$PUBLIC_KEY_FILE"
chmod 0600 "$PRIVATE_KEY_FILE"
chmod 0644 "$PUBLIC_KEY_FILE"

PRIVATE_KEY="$(cat "$PRIVATE_KEY_FILE")"
PUBLIC_KEY="$(cat "$PUBLIC_KEY_FILE")"
MTU_LINE=""
if [[ -n "${CREEPER_WG_MTU:-}" ]]; then
  [[ "$CREEPER_WG_MTU" =~ ^[0-9]+$ ]] || {
    echo "CREEPER_WG_MTU must be an integer" >&2
    exit 2
  }
  MTU_LINE="MTU = $CREEPER_WG_MTU"
fi

cat > /etc/wireguard/creeper.conf <<EOF
[Interface]
PrivateKey = $PRIVATE_KEY
Address = $CREEPER_WORKER_WG_IP
$MTU_LINE

[Peer]
PublicKey = $CREEPER_AUTHORITY_WG_PUBLIC_KEY
Endpoint = $CREEPER_AUTHORITY_ENDPOINT
AllowedIPs = 10.77.0.1/32
PersistentKeepalive = 25
EOF
chmod 0600 /etc/wireguard/creeper.conf

printf 'CREEPER_WORKER_WG_PUBLIC_KEY=%s\n' "$PUBLIC_KEY"
printf 'CREEPER_WORKER_WG_IP=%s\n' "$CREEPER_WORKER_WG_IP"
echo "WireGuard config prepared but not started."
echo "Enroll this public key on the Authority, then run the generic Fabric worker installer."
