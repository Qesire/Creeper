#!/usr/bin/env bash
set -euo pipefail

APP_DIR=/opt/creeper

sudo -u creeper -H bash "$APP_DIR/deploy/oci-a1/verify-baseline.sh"

sudo systemctl enable --now creeper-fabric-authority.service
sudo systemctl enable --now creeper-fabric-worker@query.service
sudo systemctl enable --now creeper-fabric-worker@evidence.service
sudo systemctl enable --now creeper-fabric-evidence-bridge.service
sudo systemctl enable --now creeper-autopilot.service
sudo systemctl enable --now creeper-email-report.timer
sudo systemctl enable --now creeper-email-outbox.timer
sudo systemctl enable --now creeper-fabric-gc.timer
sudo systemctl enable --now creeper-fabric-worker-health.timer

systemctl --no-pager --full status \
  creeper-fabric-authority.service \
  creeper-fabric-worker@query.service \
  creeper-fabric-worker@evidence.service \
  creeper-fabric-evidence-bridge.service \
  creeper-autopilot.service \
  creeper-email-report.timer \
  creeper-email-outbox.timer \
  creeper-fabric-gc.timer \
  creeper-fabric-worker-health.timer
