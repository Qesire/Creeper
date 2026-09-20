#!/usr/bin/env bash
set -euo pipefail

: "${CREEPER_COORDINATOR_URL:?missing CREEPER_COORDINATOR_URL}"

if [[ "$(timedatectl show -p NTPSynchronized --value 2>/dev/null || true)" != "yes" ]]; then
  echo "system clock is not NTP-synchronized; Fabric HMAC timestamps would be unsafe" >&2
  exit 3
fi

if [[ ! -d /sys/class/net/creeper ]]; then
  echo "WireGuard interface 'creeper' is not active" >&2
  exit 3
fi

COORDINATOR_SCHEME_HOST="$(
  python3 - "$CREEPER_COORDINATOR_URL" <<'PY'
import ipaddress
import sys
from urllib.parse import urlsplit

parsed=urlsplit(sys.argv[1])
if parsed.scheme not in {"http","https"} or not parsed.hostname:
    raise SystemExit(2)
host=parsed.hostname
if parsed.scheme=="http":
    try:
        address=ipaddress.ip_address(host)
    except ValueError as exc:
        raise SystemExit(2) from exc
    if not address.is_private:
        raise SystemExit(2)
print(parsed.scheme)
print(host)
PY
)" || {
  echo "remote worker coordinator must use WireGuard-private HTTP or HTTPS" >&2
  exit 3
}
COORDINATOR_SCHEME="$(printf '%s\n' "$COORDINATOR_SCHEME_HOST" | sed -n '1p')"
COORDINATOR_HOST="$(printf '%s\n' "$COORDINATOR_SCHEME_HOST" | sed -n '2p')"

if [[ "$COORDINATOR_SCHEME" == "http" ]]; then
  ROUTE="$(ip route get "$COORDINATOR_HOST" 2>/dev/null || true)"
  if [[ ! "$ROUTE" =~ (^|[[:space:]])dev[[:space:]]+creeper([[:space:]]|$) ]]; then
    echo "private coordinator route does not traverse WireGuard interface 'creeper'" >&2
    exit 3
  fi
fi

curl --fail --silent --show-error --max-time 5 \
  "${CREEPER_COORDINATOR_URL%/}/healthz" >/dev/null

curl --fail --silent --show-error --max-time 5 \
  "${CREEPER_COORDINATOR_URL%/}/meta" | python3 -c '
import json
import sys

try:
    value=json.load(sys.stdin)
except json.JSONDecodeError as exc:
    raise SystemExit("Fabric /meta returned invalid JSON") from exc
if value.get("protocol_version")!="creeper-fabric-v2":
    raise SystemExit("Fabric /meta protocol mismatch")
'
