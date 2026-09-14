#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "run as root" >&2
  exit 2
fi

: "${FABRIC_BASELINE_INDEX:?set FABRIC_BASELINE_INDEX to the built baseline SQLite file}"

FABRIC_REPO_URL="${FABRIC_REPO_URL:-https://github.com/Qesire/Creeper.git}"
FABRIC_REF="${FABRIC_REF:-feat/distributed-evidence-fabric-vnext}"
FABRIC_INSTALL_ROOT="${FABRIC_INSTALL_ROOT:-/opt/creeper-fabric}"
FABRIC_STATE_DIR="${FABRIC_STATE_DIR:-/var/lib/creeper-fabric}"
FABRIC_CONFIG_DIR="${FABRIC_CONFIG_DIR:-/etc/creeper-fabric}"
FABRIC_AUTHORITY_PORT="${FABRIC_AUTHORITY_PORT:-8088}"
FABRIC_SERVICE_USER="${FABRIC_SERVICE_USER:-creeper-fabric}"
UV_VERSION="${UV_VERSION:-0.12.13}"

if [[ ! -f "${FABRIC_BASELINE_INDEX}" ]]; then
  echo "baseline index not found: ${FABRIC_BASELINE_INDEX}" >&2
  exit 2
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends ca-certificates curl git

if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf "https://astral.sh/uv/${UV_VERSION}/install.sh" |
    env UV_UNMANAGED_INSTALL=/usr/local/bin sh
fi

if ! id "${FABRIC_SERVICE_USER}" >/dev/null 2>&1; then
  useradd --system --home-dir "${FABRIC_STATE_DIR}" --shell /usr/sbin/nologin     "${FABRIC_SERVICE_USER}"
fi

mkdir -p "${FABRIC_INSTALL_ROOT}" "${FABRIC_STATE_DIR}" "${FABRIC_CONFIG_DIR}"
chown "${FABRIC_SERVICE_USER}:${FABRIC_SERVICE_USER}"   "${FABRIC_INSTALL_ROOT}" "${FABRIC_STATE_DIR}"
chmod 0750 "${FABRIC_STATE_DIR}"
chmod 0750 "${FABRIC_CONFIG_DIR}"

if [[ ! -d "${FABRIC_INSTALL_ROOT}/.git" ]]; then
  rm -rf "${FABRIC_INSTALL_ROOT}"
  git clone --branch "${FABRIC_REF}" --single-branch     "${FABRIC_REPO_URL}" "${FABRIC_INSTALL_ROOT}"
else
  git -C "${FABRIC_INSTALL_ROOT}" fetch --prune origin "${FABRIC_REF}"
  git -C "${FABRIC_INSTALL_ROOT}" checkout -B "${FABRIC_REF}" "origin/${FABRIC_REF}"
  git -C "${FABRIC_INSTALL_ROOT}" reset --hard "origin/${FABRIC_REF}"
fi
chown -R "${FABRIC_SERVICE_USER}:${FABRIC_SERVICE_USER}" "${FABRIC_INSTALL_ROOT}"

mkdir -p "${FABRIC_STATE_DIR}/home" "${FABRIC_STATE_DIR}/uv-cache"
chown -R "${FABRIC_SERVICE_USER}:${FABRIC_SERVICE_USER}" "${FABRIC_STATE_DIR}"
runuser -u "${FABRIC_SERVICE_USER}" -- env   HOME="${FABRIC_STATE_DIR}/home"   UV_CACHE_DIR="${FABRIC_STATE_DIR}/uv-cache"   uv sync --directory "${FABRIC_INSTALL_ROOT}" --frozen

CREDENTIALS_FILE="${FABRIC_CONFIG_DIR}/workers.json"
if [[ ! -e "${CREDENTIALS_FILE}" ]]; then
  printf '{}\n' > "${CREDENTIALS_FILE}"
fi
chown root:"${FABRIC_SERVICE_USER}" "${CREDENTIALS_FILE}"
chmod 0640 "${CREDENTIALS_FILE}"

cat > "${FABRIC_CONFIG_DIR}/authority.toml" <<EOF
[authority]
database = "${FABRIC_STATE_DIR}/authority.sqlite3"
baseline_index = "${FABRIC_BASELINE_INDEX}"
credentials_file = "${CREDENTIALS_FILE}"
host = "127.0.0.1"
port = ${FABRIC_AUTHORITY_PORT}
max_clock_skew_seconds = 300
reconcile_interval_seconds = 5
promotion_batch_size = 64
host_promotion_batch_size = 256
auto_promote_source_pages = false

[provider_budgets.internet_archive]
requests_per_second = 0.5
max_global_inflight = 4
require_qualified_region = true

[provider_budgets.arquivo_pt]
requests_per_second = 1.0
max_global_inflight = 4
require_qualified_region = true

[provider_budgets.web_discovery]
requests_per_second = 2.0
max_global_inflight = 8
require_qualified_region = false

[provider_budgets.web_search]
requests_per_second = 1.0
max_global_inflight = 4
require_qualified_region = false
EOF
chown root:"${FABRIC_SERVICE_USER}" "${FABRIC_CONFIG_DIR}/authority.toml"
chmod 0640 "${FABRIC_CONFIG_DIR}/authority.toml"

cat > /etc/systemd/system/creeper-fabric-authority.service <<EOF
[Unit]
Description=Creeper Fabric Local Authority
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${FABRIC_SERVICE_USER}
Group=${FABRIC_SERVICE_USER}
WorkingDirectory=${FABRIC_INSTALL_ROOT}
Environment=PYTHONUNBUFFERED=1
ExecStart=${FABRIC_INSTALL_ROOT}/.venv/bin/creeper-fabric-authority --config ${FABRIC_CONFIG_DIR}/authority.toml
Restart=on-failure
RestartSec=3
TimeoutStopSec=30
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ReadWritePaths=${FABRIC_STATE_DIR}

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now creeper-fabric-authority.service

for _ in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:${FABRIC_AUTHORITY_PORT}/healthz" >/dev/null; then
    echo "Creeper Fabric Authority is healthy on 127.0.0.1:${FABRIC_AUTHORITY_PORT}"
    exit 0
  fi
  sleep 1
done

journalctl -u creeper-fabric-authority.service -n 80 --no-pager >&2 || true
echo "Authority failed health check" >&2
exit 1
