#!/usr/bin/env bash
set -euo pipefail

if [[ "$(id -u)" -ne 0 ]]; then
  echo "run as root: sudo bash $0 <staged-baseline-dir> <eed-model>" >&2
  exit 2
fi
[[ $# -eq 2 ]] || {
  echo "usage: sudo bash $0 <staged-baseline-dir> <eed-model>" >&2
  exit 2
}

APP_USER=creeper
APP_GROUP=creeper
APP_DIR=/opt/creeper
BASE_ROOT=/srv/creeper/baseline
INDEX_ROOT=/srv/creeper/data/indexes/baseline
ACTIVE_INDEX=/srv/creeper/data/indexes/baseline-fast.sqlite3
MODEL_TARGET=/srv/creeper/reference/equivalent_english_domain.json
SOURCE_DIR="$(readlink -f "$1")"
MODEL_SOURCE="$(readlink -f "$2")"
MANIFEST_SOURCE="$SOURCE_DIR/authority-manifest.json"

[[ -d "$SOURCE_DIR" ]] || { echo "missing staged baseline dir: $SOURCE_DIR" >&2; exit 2; }
[[ -s "$MANIFEST_SOURCE" ]] || { echo "missing authority-manifest.json in $SOURCE_DIR" >&2; exit 2; }
[[ -s "$MODEL_SOURCE" ]] || { echo "missing EED model: $MODEL_SOURCE" >&2; exit 2; }

BASELINE_ID="$("$APP_DIR/.venv/bin/python" - "$SOURCE_DIR" "$MANIFEST_SOURCE" "$MODEL_SOURCE" <<'PY'
from pathlib import Path
import sys

from creeper.authority.identity import AuthoritySnapshot, sha256_file

source=Path(sys.argv[1])
authority=AuthoritySnapshot.from_manifest_path(Path(sys.argv[2]))
model=Path(sys.argv[3])
for name,expected in authority.annual_file_hashes.items():
    path=source/name
    if not path.is_file() or sha256_file(path)!=expected:
        raise SystemExit(f"baseline hash mismatch: {name}")
candidate=source/"candidate_pool.txt"
if not candidate.is_file() or sha256_file(candidate)!=authority.candidate_file_hash:
    raise SystemExit("baseline hash mismatch: candidate_pool.txt")
authority.verify_model(model)
print(authority.baseline_id)
PY
)"

TARGET="$BASE_ROOT/$BASELINE_ID"
CURRENT="$BASE_ROOT/current"
if [[ -e "$CURRENT" ]]; then
  CURRENT_REAL="$(readlink -f "$CURRENT")"
  if [[ "$CURRENT_REAL" != "$TARGET" ]]; then
    echo "refusing live authority switch: current=$CURRENT_REAL requested=$TARGET" >&2
    echo "baseline migration with existing runtime state requires an explicit rebase procedure" >&2
    exit 3
  fi
fi

install -d -o "$APP_USER" -g "$APP_GROUP" -m 0750   "$BASE_ROOT" "$INDEX_ROOT" /srv/creeper/reference

# Persist the small model before moving the staging directory, because callers
# may have placed the model inside that directory. Re-runs with the already
# installed model remain idempotent.
if [[ ! -e "$MODEL_TARGET" || "$(readlink -f "$MODEL_SOURCE")" != "$(readlink -f "$MODEL_TARGET")" ]]; then
  install -o "$APP_USER" -g "$APP_GROUP" -m 0640 "$MODEL_SOURCE" "$MODEL_TARGET"
fi

if [[ "$SOURCE_DIR" != "$TARGET" ]]; then
  if [[ -e "$TARGET" ]]; then
    echo "target baseline already exists: $TARGET" >&2
    exit 3
  fi
  mv "$SOURCE_DIR" "$TARGET"
fi
chown -R "$APP_USER:$APP_GROUP" "$TARGET"
chmod 0750 "$TARGET"
find "$TARGET" -type f -exec chmod 0640 {} +

MIN_FREE_BYTES=$((25 * 1024 * 1024 * 1024))
FREE_BYTES="$(df --output=avail -B1 /srv/creeper | tail -n 1 | tr -d ' ')"
if (( FREE_BYTES < MIN_FREE_BYTES )); then
  echo "insufficient free space to build baseline index: free=$FREE_BYTES required=$MIN_FREE_BYTES" >&2
  exit 4
fi

WAS_ACTIVE=0
if systemctl is-active --quiet creeper-autopilot.service; then
  WAS_ACTIVE=1
  systemctl stop creeper-autopilot.service
fi
restart_on_error() {
  code=$?
  if (( code != 0 && WAS_ACTIVE )); then
    systemctl start creeper-autopilot.service || true
  fi
  exit "$code"
}
trap restart_on_error EXIT

INDEX="$INDEX_ROOT/$BASELINE_ID.sqlite3"
sudo -u "$APP_USER" -H "$APP_DIR/.venv/bin/python"   "$APP_DIR/scripts/build_baseline.py"   /srv/creeper "$INDEX"   --baseline-dir "$TARGET"   --authority-manifest "$TARGET/authority-manifest.json"   --batch-size 50000

rm -f "$BASE_ROOT/current.next" /srv/creeper/data/indexes/baseline-fast.sqlite3.next
ln -s "$BASELINE_ID" "$BASE_ROOT/current.next"
mv -Tf "$BASE_ROOT/current.next" "$CURRENT"
ln -s "baseline/$BASELINE_ID.sqlite3" /srv/creeper/data/indexes/baseline-fast.sqlite3.next
mv -Tf /srv/creeper/data/indexes/baseline-fast.sqlite3.next "$ACTIVE_INDEX"
chown -h "$APP_USER:$APP_GROUP" "$CURRENT" "$ACTIVE_INDEX"

sudo -u "$APP_USER" -H bash "$APP_DIR/deploy/oci-a1/verify-baseline.sh"

trap - EXIT
if (( WAS_ACTIVE )); then
  systemctl start creeper-autopilot.service
fi

echo "baseline installed on server: $TARGET"
