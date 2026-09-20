#!/usr/bin/env bash
set -euo pipefail

BASELINE=/srv/creeper/data/indexes/baseline-fast.sqlite3
EED=/srv/creeper/reference/equivalent_english_domain.json

[[ -s "$BASELINE" ]] || { echo "missing or empty: $BASELINE" >&2; exit 2; }
[[ -s "$EED" ]] || { echo "missing or empty: $EED" >&2; exit 2; }

sudo systemctl enable --now creeper-fabric-authority.service
sudo systemctl enable --now creeper-fabric-worker@query.service
sudo systemctl enable --now creeper-fabric-worker@evidence.service
sudo systemctl enable --now creeper-fabric-evidence-bridge.service
sudo systemctl enable --now creeper-autopilot.service
sudo systemctl enable --now creeper-email-report.timer

systemctl --no-pager --full status \
  creeper-fabric-authority.service \
  creeper-fabric-worker@query.service \
  creeper-fabric-worker@evidence.service \
  creeper-fabric-evidence-bridge.service \
  creeper-autopilot.service \
  creeper-email-report.timer
