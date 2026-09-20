#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${CREEPER_APP_DIR:-/opt/creeper}"
BASE_ROOT="${CREEPER_BASELINE_ROOT:-/srv/creeper/baseline}"
CURRENT="$BASE_ROOT/current"
INDEX="${CREEPER_BASELINE_INDEX:-/srv/creeper/data/indexes/baseline-fast.sqlite3}"
MODEL="${CREEPER_EED_MODEL:-/srv/creeper/reference/equivalent_english_domain.json}"

[[ -e "$CURRENT" ]] || { echo "missing active baseline link: $CURRENT" >&2; exit 2; }
[[ -s "$INDEX" ]] || { echo "missing or empty baseline index: $INDEX" >&2; exit 2; }
[[ -s "$MODEL" ]] || { echo "missing or empty EED model: $MODEL" >&2; exit 2; }

CURRENT_REAL="$(readlink -f "$CURRENT")"
BASE_ROOT_REAL="$(readlink -f "$BASE_ROOT")"
case "$CURRENT_REAL/" in
  "$BASE_ROOT_REAL"/*/) ;;
  *) echo "active baseline escapes baseline root: $CURRENT_REAL" >&2; exit 2 ;;
esac
MANIFEST="$CURRENT_REAL/authority-manifest.json"
[[ -s "$MANIFEST" ]] || { echo "missing authority manifest: $MANIFEST" >&2; exit 2; }

"$APP_DIR/.venv/bin/python" - "$CURRENT_REAL" "$MANIFEST" "$MODEL" "$INDEX" <<'PY'
from pathlib import Path
import sys

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.identity import AuthoritySnapshot

baseline=Path(sys.argv[1])
manifest=Path(sys.argv[2])
model=Path(sys.argv[3])
index_path=Path(sys.argv[4])
authority=AuthoritySnapshot.from_manifest_path(manifest)
authority.verify_baseline_dir(baseline)
authority.verify_model(model)
index=BaselineIndex(index_path,authority=authority)
try:
    counts=index.counts()
finally:
    index.close()
print(f"baseline_id={authority.baseline_id}")
print(f"authority_digest={authority.authority_digest}")
print(f"baseline_eed={authority.baseline_eed}")
print(f"annual_hostnames={counts['annual_hostnames']}")
print(f"candidate_hostnames={counts['candidate_hostnames']}")
print(f"baseline_dir={baseline}")
print(f"index={index_path}")
PY
