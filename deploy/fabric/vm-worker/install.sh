#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "run as root" >&2
  exit 2
fi

: "${FABRIC_COORDINATOR_URL:?set FABRIC_COORDINATOR_URL, e.g. https://fabric.example.com}"
: "${FABRIC_WORKER_ID:?set FABRIC_WORKER_ID}"
: "${FABRIC_REGION:?set FABRIC_REGION}"

if [[ "${FABRIC_COORDINATOR_URL}" != https://* && "${FABRIC_ALLOW_INSECURE_COORDINATOR:-0}" != "1" ]]; then
  echo "production coordinator URL must use https://" >&2
  exit 2
fi

if [[ -n "${FABRIC_WORKER_SECRET_FILE:-}" ]]; then
  [[ -f "${FABRIC_WORKER_SECRET_FILE}" ]] || {
    echo "worker secret file not found" >&2
    exit 2
  }
  WORKER_SECRET="$(sed -n 's/^CREEPER_WORKER_SECRET=//p' "${FABRIC_WORKER_SECRET_FILE}" | tail -n1)"
else
  WORKER_SECRET="${CREEPER_WORKER_SECRET:-}"
fi
if [[ -z "${WORKER_SECRET}" ]]; then
  echo "set FABRIC_WORKER_SECRET_FILE (preferred) or CREEPER_WORKER_SECRET" >&2
  exit 2
fi

FABRIC_PROFILE="${FABRIC_PROFILE:-oci-explorer}"
FABRIC_REPO_URL="${FABRIC_REPO_URL:-https://github.com/Qesire/Creeper.git}"
FABRIC_REF="${FABRIC_REF:-feat/distributed-evidence-fabric-vnext}"
FABRIC_INSTALL_ROOT="${FABRIC_INSTALL_ROOT:-/opt/creeper-fabric}"
FABRIC_STATE_DIR="${FABRIC_STATE_DIR:-/var/lib/creeper-fabric}"
FABRIC_CONFIG_DIR="${FABRIC_CONFIG_DIR:-/etc/creeper-fabric}"
FABRIC_SERVICE_USER="${FABRIC_SERVICE_USER:-creeper-fabric}"
UV_VERSION="${UV_VERSION:-0.12.13}"

ARCH="$(uname -m)"
CPU_COUNT="$(nproc)"
MEMORY_BYTES="$(awk '/MemTotal:/ {print $2 * 1024}' /proc/meminfo | cut -d. -f1)"

case "${FABRIC_PROFILE}" in
  oci-explorer)
    CAPABILITIES='["ONLINE_QUERY", "WEB_DISCOVERY", "SEARCH_QUERY"]'
    ALLOWED_PROVIDERS='["internet_archive", "arquivo_pt", "web_discovery", "web_search"]'
    DAILY_EGRESS="${FABRIC_DAILY_EGRESS_BUDGET_BYTES:-0}"
    CDX_PROFILE="both"
    ;;
  gcp-metered)
    CAPABILITIES='["ONLINE_QUERY", "WEB_DISCOVERY", "SEARCH_QUERY"]'
    ALLOWED_PROVIDERS='["arquivo_pt", "web_discovery", "web_search"]'
    DAILY_EGRESS="${FABRIC_DAILY_EGRESS_BUDGET_BYTES:-268435456}"
    CDX_PROFILE="arquivo"
    ;;
  resolver)
    CAPABILITIES='["ONLINE_QUERY"]'
    ALLOWED_PROVIDERS='["internet_archive", "arquivo_pt"]'
    DAILY_EGRESS="${FABRIC_DAILY_EGRESS_BUDGET_BYTES:-0}"
    CDX_PROFILE="both"
    ;;
  bulk)
    CAPABILITIES='["STREAMING_BULK"]'
    ALLOWED_PROVIDERS='[]'
    DAILY_EGRESS="${FABRIC_DAILY_EGRESS_BUDGET_BYTES:-0}"
    CDX_PROFILE="none"
    ;;
  *)
    echo "unknown FABRIC_PROFILE: ${FABRIC_PROFILE}" >&2
    exit 2
    ;;
esac

if command -v apt-get >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends ca-certificates curl git
elif command -v dnf >/dev/null 2>&1; then
  dnf install -y ca-certificates curl git
elif command -v yum >/dev/null 2>&1; then
  yum install -y ca-certificates curl git
else
  echo "unsupported package manager; need apt, dnf, or yum" >&2
  exit 2
fi

if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf "https://astral.sh/uv/${UV_VERSION}/install.sh" |
    env UV_UNMANAGED_INSTALL=/usr/local/bin sh
fi

if ! id "${FABRIC_SERVICE_USER}" >/dev/null 2>&1; then
  NOLOGIN_SHELL="$(command -v nologin 2>/dev/null || true)"
  : "${NOLOGIN_SHELL:=/sbin/nologin}"
  useradd --system --home-dir "${FABRIC_STATE_DIR}" \
    --shell "${NOLOGIN_SHELL}" "${FABRIC_SERVICE_USER}"
fi

mkdir -p "${FABRIC_INSTALL_ROOT}" "${FABRIC_STATE_DIR}" "${FABRIC_CONFIG_DIR}"
chown "${FABRIC_SERVICE_USER}:${FABRIC_SERVICE_USER}"   "${FABRIC_INSTALL_ROOT}" "${FABRIC_STATE_DIR}"
chmod 0750 "${FABRIC_STATE_DIR}" "${FABRIC_CONFIG_DIR}"

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

cat > "${FABRIC_CONFIG_DIR}/worker.env" <<EOF
CREEPER_WORKER_SECRET=${WORKER_SECRET}
EOF
chown root:"${FABRIC_SERVICE_USER}" "${FABRIC_CONFIG_DIR}/worker.env"
chmod 0640 "${FABRIC_CONFIG_DIR}/worker.env"

cat > "${FABRIC_CONFIG_DIR}/worker.toml" <<EOF
[worker]
coordinator_url = "${FABRIC_COORDINATOR_URL}"
worker_id = "${FABRIC_WORKER_ID}"
runtime_class = "vm"
region = "${FABRIC_REGION}"
architecture = "${ARCH}"
memory_bytes = ${MEMORY_BYTES}
cpu_count = ${CPU_COUNT}
network_class = "public"
capabilities = ${CAPABILITIES}
allowed_providers = ${ALLOWED_PROVIDERS}
daily_egress_budget_bytes = ${DAILY_EGRESS}
secret_env = "CREEPER_WORKER_SECRET"
poll_seconds = 2.0
claim_wait_seconds = 20.0
lease_seconds = 300.0
EOF

if [[ "${CDX_PROFILE}" == "both" ]]; then
  cat >> "${FABRIC_CONFIG_DIR}/worker.toml" <<'EOF'

[[cdx_providers]]
name = "internet_archive"
endpoint = "https://web.archive.org/cdx/search/cdx"
dialect = "wayback"
requests_per_second = 1.0
max_inflight = 4
max_connections = 8
max_keepalive_connections = 4
keepalive_expiry_seconds = 20
row_limit = 150000
weight = 1.0

[[cdx_providers]]
name = "arquivo_pt"
endpoint = "https://arquivo.pt/wayback/cdx"
dialect = "arquivo"
requests_per_second = 1.0
max_inflight = 4
max_connections = 8
max_keepalive_connections = 4
keepalive_expiry_seconds = 20
row_limit = 100000
weight = 1.0
EOF
elif [[ "${CDX_PROFILE}" == "arquivo" ]]; then
  cat >> "${FABRIC_CONFIG_DIR}/worker.toml" <<'EOF'

[[cdx_providers]]
name = "arquivo_pt"
endpoint = "https://arquivo.pt/wayback/cdx"
dialect = "arquivo"
requests_per_second = 1.0
max_inflight = 2
max_connections = 4
max_keepalive_connections = 2
keepalive_expiry_seconds = 20
row_limit = 100000
weight = 1.0
EOF
fi

chown root:"${FABRIC_SERVICE_USER}" "${FABRIC_CONFIG_DIR}/worker.toml"
chmod 0640 "${FABRIC_CONFIG_DIR}/worker.toml"

cat > /etc/systemd/system/creeper-fabric-worker.service <<EOF
[Unit]
Description=Creeper Fabric Worker (${FABRIC_WORKER_ID})
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${FABRIC_SERVICE_USER}
Group=${FABRIC_SERVICE_USER}
WorkingDirectory=${FABRIC_INSTALL_ROOT}
Environment=PYTHONUNBUFFERED=1
EnvironmentFile=${FABRIC_CONFIG_DIR}/worker.env
ExecStart=${FABRIC_INSTALL_ROOT}/.venv/bin/creeper-fabric-worker --config ${FABRIC_CONFIG_DIR}/worker.toml
Restart=always
RestartSec=5
TimeoutStopSec=30
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full

[Install]
WantedBy=multi-user.target
EOF

curl -fsS "${FABRIC_COORDINATOR_URL%/}/healthz" >/dev/null
META_JSON="$(curl -fsS "${FABRIC_COORDINATOR_URL%/}/meta")"
AUTHORITY_TIME="$(
  printf '%s' "${META_JSON}" |
    "${FABRIC_INSTALL_ROOT}/.venv/bin/python" -c \
      'import json,sys; print(int(float(json.load(sys.stdin)["server_unix_time"])))'
)"
LOCAL_TIME="$(date +%s)"
CLOCK_SKEW=$(( LOCAL_TIME > AUTHORITY_TIME ? LOCAL_TIME - AUTHORITY_TIME : AUTHORITY_TIME - LOCAL_TIME ))
MAX_CLOCK_SKEW="${FABRIC_DEPLOY_MAX_CLOCK_SKEW_SECONDS:-240}"
if (( CLOCK_SKEW > MAX_CLOCK_SKEW )); then
  echo "clock skew too large for Fabric HMAC: ${CLOCK_SKEW}s > ${MAX_CLOCK_SKEW}s" >&2
  exit 2
fi

systemctl daemon-reload
systemctl enable --now creeper-fabric-worker.service
systemctl is-active --quiet creeper-fabric-worker.service
echo "Creeper Fabric worker ${FABRIC_WORKER_ID} is active (${FABRIC_PROFILE})"
