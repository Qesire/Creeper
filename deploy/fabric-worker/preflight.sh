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

case "$CREEPER_COORDINATOR_URL" in
  http://10.*|http://192.168.*|http://172.1[6-9].*|http://172.2[0-9].*|http://172.3[01].*)
    ;;
  https://*)
    ;;
  *)
    echo "remote worker coordinator must use WireGuard-private HTTP or HTTPS" >&2
    exit 3
    ;;
esac

curl --fail --silent --show-error --max-time 5   "${CREEPER_COORDINATOR_URL%/}/healthz" >/dev/null
