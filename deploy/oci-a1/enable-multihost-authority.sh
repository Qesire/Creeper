#!/usr/bin/env bash
set -euo pipefail

if [[ "$(id -u)" -ne 0 ]]; then
  echo "run as root: sudo bash $0" >&2
  exit 2
fi

FABRIC_CONFIG=/etc/creeper/fabric.toml
WG_CONFIG=/etc/wireguard/creeper.conf
[[ -s "$FABRIC_CONFIG" ]] || {
  echo "missing Fabric config: $FABRIC_CONFIG" >&2
  exit 2
}
[[ -s "$WG_CONFIG" ]] || {
  echo "install Authority WireGuard config first: $WG_CONFIG" >&2
  exit 2
}

systemctl enable --now wg-quick@creeper.service
wg show creeper >/dev/null

python3 - "$FABRIC_CONFIG" <<'PY'
from pathlib import Path
import re
import sys

path=Path(sys.argv[1])
text=path.read_text(encoding="utf-8")
text=re.sub(
    r'(?m)^host\s*=\s*"[^"]+"\s*$',
    'host = "0.0.0.0"',
    text,
    count=1,
)
text=re.sub(
    r'(?m)^require_qualified_region\s*=\s*false\s*$',
    'require_qualified_region = true\nallow_unknown_region_probe = true',
    text,
)
text=re.sub(
    r'(?m)^(require_qualified_region\s*=\s*true\s*)$'
    r'(?!\nallow_unknown_region_probe\s*=)',
    r'\1\nallow_unknown_region_probe = true',
    text,
)
temporary=path.with_suffix(path.suffix+".tmp")
temporary.write_text(text,encoding="utf-8")
temporary.replace(path)
PY

ufw default deny incoming
ufw default allow outgoing
ufw allow OpenSSH
ufw allow "${CREEPER_WG_PORT:-51820}/udp"
ufw allow in on creeper to any port 8088 proto tcp
ufw --force enable

systemctl restart creeper-fabric-authority.service
curl --fail --silent --show-error --max-time 5 http://127.0.0.1:8088/healthz >/dev/null

echo "Fabric Authority is multihost-enabled."
echo "TCP/8088 is permitted only through the WireGuard interface by this profile."
