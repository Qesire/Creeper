#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${CREEPER_REPO_URL:-https://github.com/Qesire/Creeper.git}"
GIT_REF="${CREEPER_GIT_REF:-main}"
APP_USER=creeper
APP_GROUP=creeper
APP_HOME=/var/lib/creeper
APP_DIR=/opt/creeper
ETC_DIR=/etc/creeper
DATA_DIR=/srv/creeper

required=(
  CREEPER_COORDINATOR_URL
  CREEPER_WORKER_ID
  CREEPER_WORKER_REGION
  CREEPER_WORKER_SECRET
  CREEPER_COORDINATOR_UPLOAD_BUDGET_BYTES_PER_MONTH
)
missing=()
for name in "${required[@]}"; do
  [[ -n "${!name:-}" ]] || missing+=("$name")
done
if (( ${#missing[@]} )); then
  echo "missing environment: ${missing[*]}" >&2
  exit 2
fi

if [[ ! "$CREEPER_WORKER_ID" =~ ^[A-Za-z0-9._:-]+$ ]]; then
  echo "CREEPER_WORKER_ID contains unsupported characters" >&2
  exit 2
fi

ROLE="${CREEPER_WORKER_ROLE:-query}"
case "$ROLE" in
  query)
    CAPABILITIES='["RESIDUAL_QUERY"]'
    PRODUCERS='["ResidualQueryProducer"]'
    PROVIDERS='["datacite","zenodo","harvard_dataverse","internet_archive"]'
    ;;
  evidence)
    CAPABILITIES='["EVIDENCE_QUERY","RDAP"]'
    PRODUCERS='["EvidenceQueryProducer"]'
    PROVIDERS='["internet_archive","arquivo_pt","rdap"]'
    ;;
  bulk)
    CAPABILITIES='["STREAMING_BULK","ARTIFACT_FETCH"]'
    PRODUCERS='["BulkShardProducer"]'
    PROVIDERS='[]'
    ;;
  *)
    echo "unsupported CREEPER_WORKER_ROLE: $ROLE" >&2
    exit 2
    ;;
esac

UPLOAD_BUDGET="$CREEPER_COORDINATOR_UPLOAD_BUDGET_BYTES_PER_MONTH"
UPLOAD_OVERHEAD="${CREEPER_COORDINATOR_UPLOAD_OVERHEAD_BYTES:-1536}"
if [[ ! "$UPLOAD_BUDGET" =~ ^[0-9]+$ || ! "$UPLOAD_OVERHEAD" =~ ^[0-9]+$ ]]; then
  echo "upload budget/overhead must be non-negative integers" >&2
  exit 2
fi

sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y   ca-certificates curl git wireguard-tools ufw

if ! id -u "$APP_USER" >/dev/null 2>&1; then
  sudo useradd --system --create-home --home-dir "$APP_HOME"     --shell /usr/sbin/nologin "$APP_USER"
fi
sudo install -d -o "$APP_USER" -g "$APP_GROUP" -m 0750   "$APP_DIR" "$DATA_DIR/spool" "$APP_HOME"
sudo install -d -o root -g "$APP_GROUP" -m 0750 "$ETC_DIR"

if [[ ! -d "$APP_DIR/.git" ]]; then
  sudo find "$APP_DIR" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
  sudo chown "$APP_USER:$APP_GROUP" "$APP_DIR"
  sudo -u "$APP_USER" -H git clone --filter=blob:none --branch "$GIT_REF"     "$REPO_URL" "$APP_DIR"
else
  sudo -u "$APP_USER" -H git -C "$APP_DIR" fetch --prune origin "$GIT_REF"
  sudo -u "$APP_USER" -H git -C "$APP_DIR" checkout -B deployed FETCH_HEAD
fi

UV="$APP_HOME/.local/bin/uv"
if [[ ! -x "$UV" ]]; then
  sudo -u "$APP_USER" -H sh -c 'curl -LsSf https://astral.sh/uv/install.sh | sh'
fi
sudo -u "$APP_USER" -H bash -c "cd '$APP_DIR' && '$UV' sync --locked"

ARCH="$(uname -m)"
CPU_COUNT="$(nproc)"
MEM_KIB="$(awk '/MemTotal:/ {print $2; exit}' /proc/meminfo)"
MEMORY_BYTES="$((MEM_KIB * 1024 * 3 / 5))"

cat <<EOF | sudo tee "$ETC_DIR/remote-worker.toml" >/dev/null
[worker]
coordinator_url = "$CREEPER_COORDINATOR_URL"
worker_id = "$CREEPER_WORKER_ID"
worker_instance_id = "auto"
runtime_class = "remote-free-$ROLE"
region = "$CREEPER_WORKER_REGION"
architecture = "$ARCH"
memory_bytes = $MEMORY_BYTES
cpu_count = $CPU_COUNT
network_class = "wireguard-public-egress"
capabilities = $CAPABILITIES
producers = $PRODUCERS
allowed_providers = $PROVIDERS
daily_egress_budget_bytes = 0
coordinator_upload_budget_bytes_per_month = $UPLOAD_BUDGET
coordinator_upload_overhead_bytes = $UPLOAD_OVERHEAD
spool_database = "/srv/creeper/spool/remote-worker.sqlite3"
secret_env = "CREEPER_WORKER_SECRET"
poll_seconds = 5
claim_wait_seconds = 25
lease_seconds = 300
heartbeat_seconds = 120
coordinator_timeout_seconds = 45
EOF

{
  printf 'CREEPER_WORKER_SECRET=%s\n' "$CREEPER_WORKER_SECRET"
  printf 'CREEPER_COORDINATOR_URL=%s\n' "$CREEPER_COORDINATOR_URL"
} | sudo tee "$ETC_DIR/remote-worker.env" >/dev/null
sudo chown root:"$APP_GROUP"   "$ETC_DIR/remote-worker.toml" "$ETC_DIR/remote-worker.env"
sudo chmod 0640 "$ETC_DIR/remote-worker.toml" "$ETC_DIR/remote-worker.env"

sudo chmod 0755 "$APP_DIR/deploy/fabric-worker/preflight.sh"
sudo install -o root -g root -m 0644   "$APP_DIR/deploy/fabric-worker/systemd/creeper-fabric-remote-worker.service"   /etc/systemd/system/creeper-fabric-remote-worker.service

sudo timedatectl set-ntp true || true
for _ in $(seq 1 30); do
  [[ "$(timedatectl show -p NTPSynchronized --value 2>/dev/null || true)" == "yes" ]] && break
  sleep 2
done
if [[ "$(timedatectl show -p NTPSynchronized --value 2>/dev/null || true)" != "yes" ]]; then
  echo "NTP did not synchronize; refusing to activate Fabric worker" >&2
  exit 3
fi

sudo ufw allow OpenSSH
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw --force enable
sudo systemctl daemon-reload
sudo systemctl enable creeper-fabric-remote-worker.service

if [[ -s /etc/wireguard/creeper.conf ]]; then
  sudo systemctl enable --now wg-quick@creeper.service
  sudo systemctl start creeper-fabric-remote-worker.service
else
  echo "worker installed but not started: install /etc/wireguard/creeper.conf first" >&2
  echo "then run: sudo systemctl enable --now wg-quick@creeper.service" >&2
  echo "          sudo systemctl start creeper-fabric-remote-worker.service" >&2
fi
