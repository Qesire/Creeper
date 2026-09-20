#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${CREEPER_REPO_URL:-https://github.com/Qesire/Creeper.git}"
GIT_REF="${CREEPER_GIT_REF:-main}"
APP_USER=creeper
APP_GROUP=creeper
APP_HOME=/var/lib/creeper
APP_DIR=/opt/creeper
DATA_DIR=/srv/creeper
ETC_DIR=/etc/creeper

if [[ "$(uname -m)" != "aarch64" ]]; then
  echo "warning: production profile was sized for OCI Ampere A1 (aarch64)" >&2
fi

sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  ca-certificates curl git openssl postgresql postgresql-client ufw

if ! id -u "$APP_USER" >/dev/null 2>&1; then
  sudo useradd --system --create-home --home-dir "$APP_HOME" \
    --shell /usr/sbin/nologin "$APP_USER"
fi

sudo install -d -o "$APP_USER" -g "$APP_GROUP" -m 0750 \
  "$APP_DIR" "$DATA_DIR/data" "$DATA_DIR/data/indexes" \
  "$DATA_DIR/reference" "$DATA_DIR/spool" "$DATA_DIR/backups" \
  "$APP_HOME"
sudo install -d -o root -g "$APP_GROUP" -m 0750 "$ETC_DIR"

if [[ ! -d "$APP_DIR/.git" ]]; then
  sudo find "$APP_DIR" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
  sudo chown "$APP_USER:$APP_GROUP" "$APP_DIR"
  sudo -u "$APP_USER" -H git clone --filter=blob:none --branch "$GIT_REF" \
    "$REPO_URL" "$APP_DIR"
else
  sudo -u "$APP_USER" -H git -C "$APP_DIR" fetch --prune origin "$GIT_REF"
  sudo -u "$APP_USER" -H git -C "$APP_DIR" checkout -B deployed FETCH_HEAD
fi

UV="$APP_HOME/.local/bin/uv"
if [[ ! -x "$UV" ]]; then
  sudo -u "$APP_USER" -H sh -c 'curl -LsSf https://astral.sh/uv/install.sh | sh'
fi
sudo -u "$APP_USER" -H bash -c "cd '$APP_DIR' && '$UV' sync --locked"
sudo -u "$APP_USER" -H "$UV" pip install \
  --python "$APP_DIR/.venv/bin/python" 'psycopg[binary]>=3.2,<4'
sudo -u "$APP_USER" -H bash -c \
  "cd '$APP_DIR/tools/scrapy_acquisition' && '$UV' sync --locked"

if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='creeper'" \
  | grep -q 1; then
  sudo -u postgres createuser creeper
fi
if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='creeper'" \
  | grep -q 1; then
  sudo -u postgres createdb -O creeper creeper
fi

PG_MAJOR="$(psql --version | sed -E 's/.* ([0-9]+)\..*/\1/')"
sudo install -d -m 0755 "/etc/postgresql/$PG_MAJOR/main/conf.d"
printf "listen_addresses = '127.0.0.1'\n" \
  | sudo tee "/etc/postgresql/$PG_MAJOR/main/conf.d/creeper.conf" >/dev/null
sudo systemctl restart postgresql

for name in fabric worker-query worker-evidence source-discovery producer autopilot; do
  sudo install -o root -g "$APP_GROUP" -m 0640 \
    "$APP_DIR/deploy/oci-a1/$name.toml" "$ETC_DIR/$name.toml"
done

if [[ ! -f "$ETC_DIR/workers.json" ]]; then
  query_secret="$(openssl rand -hex 32)"
  evidence_secret="$(openssl rand -hex 32)"
  printf '{"oci-a1-query":"%s","oci-a1-evidence":"%s"}\n' \
    "$query_secret" "$evidence_secret" \
    | sudo tee "$ETC_DIR/workers.json" >/dev/null
  printf 'CREEPER_WORKER_QUERY_SECRET=%s\n' "$query_secret" \
    | sudo tee "$ETC_DIR/worker-query.env" >/dev/null
  printf 'CREEPER_WORKER_EVIDENCE_SECRET=%s\n' "$evidence_secret" \
    | sudo tee "$ETC_DIR/worker-evidence.env" >/dev/null
  sudo chown root:"$APP_GROUP" "$ETC_DIR/workers.json" \
    "$ETC_DIR/worker-query.env" "$ETC_DIR/worker-evidence.env"
  sudo chmod 0640 "$ETC_DIR/workers.json" \
    "$ETC_DIR/worker-query.env" "$ETC_DIR/worker-evidence.env"
fi

if [[ ! -f "$ETC_DIR/report.env" ]]; then
  required=(
    CREEPER_REPORT_FROM CREEPER_REPORT_TO
    CREEPER_SMTP_USERNAME CREEPER_SMTP_APP_PASSWORD
  )
  missing=()
  for name in "${required[@]}"; do
    [[ -n "${!name:-}" ]] || missing+=("$name")
  done
  if (( ${#missing[@]} )); then
    echo "missing email environment: ${missing[*]}" >&2
    echo "set them and rerun; no email secret has been written" >&2
    exit 2
  fi
  {
    printf 'CREEPER_REPORT_FROM=%s\n' "$CREEPER_REPORT_FROM"
    printf 'CREEPER_REPORT_TO=%s\n' "$CREEPER_REPORT_TO"
    printf 'CREEPER_SMTP_USERNAME=%s\n' "$CREEPER_SMTP_USERNAME"
    printf 'CREEPER_SMTP_APP_PASSWORD=%s\n' "$CREEPER_SMTP_APP_PASSWORD"
  } | sudo tee "$ETC_DIR/report.env" >/dev/null
  sudo chown root:"$APP_GROUP" "$ETC_DIR/report.env"
  sudo chmod 0640 "$ETC_DIR/report.env"
fi

for unit in "$APP_DIR"/deploy/oci-a1/systemd/*; do
  sudo install -o root -g root -m 0644 "$unit" "/etc/systemd/system/$(basename "$unit")"
done

sudo timedatectl set-timezone Asia/Singapore
sudo ufw allow OpenSSH
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw --force enable

sudo install -d -m 0755 /etc/systemd/journald.conf.d
cat <<'EOF' | sudo tee /etc/systemd/journald.conf.d/creeper.conf >/dev/null
[Journal]
SystemMaxUse=1G
MaxRetentionSec=14day
EOF
sudo systemctl restart systemd-journald
sudo systemctl daemon-reload

sudo systemctl enable --now creeper-fabric-authority.service
sudo systemctl enable --now creeper-fabric-worker@query.service
sudo systemctl enable --now creeper-fabric-worker@evidence.service
sudo systemctl enable --now creeper-fabric-evidence-bridge.service
sudo systemctl enable --now creeper-email-report.timer

if [[ -f "$DATA_DIR/data/indexes/baseline-fast.sqlite3" \
   && -f "$DATA_DIR/reference/equivalent_english_domain.json" ]]; then
  sudo systemctl enable --now creeper-autopilot.service
else
  echo "Fabric is online, but autopilot is not started yet." >&2
  echo "Place baseline-fast.sqlite3 under $DATA_DIR/data/indexes/ and" >&2
  echo "equivalent_english_domain.json under $DATA_DIR/reference/," >&2
  echo "then run: sudo bash $APP_DIR/deploy/oci-a1/start-production.sh" >&2
fi

sudo -u "$APP_USER" -H "$APP_DIR/.venv/bin/creeper-fabric-email-report" \
  --config "$ETC_DIR/fabric.toml" --dry-run
