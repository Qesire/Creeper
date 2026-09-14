# Creeper Fabric production deployment

Creeper Fabric uses a hub-and-spoke topology. Remote workers never accept
Creeper inbound connections and never talk to one another.

```text
OCI / GCP / Cloudflare Worker
        |  outbound HTTPS/HMAC control
        v
Cloudflare public hostname
        |
Cloudflare Tunnel
        |
127.0.0.1:8088
Local Authority
```

Provider traffic is direct from each worker to Web/Search/Internet Archive/
Arquivo. The Local Authority never proxies provider bodies.

On every Linux node, first obtain a small management checkout:

```bash
git clone --depth 1 --branch feat/distributed-evidence-fabric-vnext \
  https://github.com/Qesire/Creeper.git
cd Creeper
```

The installers maintain their own runtime checkout under `/opt/creeper-fabric`.

## 1. Local Authority

Clone this derivative branch on the machine that owns the immutable baseline
index, then run:

```bash
sudo -E \
  FABRIC_BASELINE_INDEX=/absolute/path/to/baseline.sqlite3 \
  deploy/fabric/local-authority/install.sh
```

The installer:

- installs the locked Python environment with uv;
- stores mutable state under `/var/lib/creeper-fabric`;
- writes config/secrets under `/etc/creeper-fabric`;
- installs `creeper-fabric-authority.service`;
- binds Authority only to `127.0.0.1:8088`.

### Publish Authority through Cloudflare Tunnel

Create a production Tunnel in Cloudflare and configure one public hostname
(for example `fabric.example.com`) to route to
`http://127.0.0.1:8088`. Copy the tunnel token to a root-only file and run:

```bash
sudo FABRIC_TUNNEL_TOKEN_FILE=/root/cloudflared-token \
  deploy/fabric/local-authority/install-cloudflared.sh
```

Optional verification:

```bash
curl -fsS https://fabric.example.com/healthz
curl -fsS https://fabric.example.com/meta

sudo -E FABRIC_PUBLIC_URL=https://fabric.example.com \
  bash deploy/fabric/local-authority/smoke.sh
```

Do not use a Quick Tunnel for production.

## 2. Provision worker identities

Recommended first deployment: provision all expected identities in one atomic
credentials update and write root-only secret files:

```bash
sudo \
  FABRIC_OCI_WORKER_ID=oci-sg-01 \
  FABRIC_GCP_WORKER_ID=gcp-us-01 \
  FABRIC_CF_WORKER_ID=cf-thin-01 \
  bash deploy/fabric/local-authority/bootstrap-workers.sh
```

This creates files such as:

```text
/root/creeper-fabric-secrets/oci-sg-01.secret
/root/creeper-fabric-secrets/gcp-us-01.secret
/root/creeper-fabric-secrets/cf-thin-01.secret
```

Existing identities are not overwritten. Rotation requires the explicit
`FABRIC_ROTATE_WORKER_SECRETS=1` switch.

For one-off provisioning, the single-worker helper remains available:

```bash
sudo deploy/fabric/local-authority/provision-worker-secret.sh oci-extra-01 \
  > /root/oci-extra-01.secret
```

Transfer each secret file only to its matching node over an authenticated
admin channel (for example SSH/SCP). Never put worker HMAC secrets in instance
metadata, the repository, or ordinary shell history.

## 3. OCI persistent explorer

On the OCI VM:

```bash
sudo install -m 0600 /path/from/scp/oci-sg-01.secret /root/fabric-worker.secret

sudo -E \
  FABRIC_COORDINATOR_URL=https://fabric.example.com \
  FABRIC_WORKER_ID=oci-sg-01 \
  FABRIC_REGION=oci-singapore \
  FABRIC_WORKER_SECRET_FILE=/root/fabric-worker.secret \
  deploy/fabric/oci/install.sh
```

Default OCI profile:

- `ONLINE_QUERY`
- `WEB_DISCOVERY`
- `SEARCH_QUERY`
- Internet Archive + Arquivo
- no daily egress cap unless explicitly configured

The worker runs as `creeper-fabric-worker.service`.

Verify connectivity from the OCI node:

```bash
sudo bash deploy/fabric/vm-worker/smoke.sh
```

## 4. GCP metered explorer

On the GCP VM:

```bash
sudo install -m 0600 /path/from/scp/gcp-us-01.secret /root/fabric-worker.secret

sudo -E \
  FABRIC_COORDINATOR_URL=https://fabric.example.com \
  FABRIC_WORKER_ID=gcp-us-01 \
  FABRIC_REGION=gcp-us-central1 \
  FABRIC_WORKER_SECRET_FILE=/root/fabric-worker.secret \
  FABRIC_DAILY_EGRESS_BUDGET_BYTES=268435456 \
  deploy/fabric/gcp/install.sh
```

The default GCP profile is deliberately conservative: it uses Arquivo as its
archive provider and enables a 256 MiB/day raw response-byte guard. Override
the budget explicitly if required.

Google Compute Engine startup scripts may invoke the same installer, but do
not embed `CREEPER_WORKER_SECRET` directly in startup-script metadata.
Provision the root-only secret file first or retrieve it from a dedicated
secret system.

Verify connectivity from the GCP node:

```bash
sudo bash deploy/fabric/vm-worker/smoke.sh
```

## 5. Qualify each provider x region

Archive budgets are fail-closed until the specific worker region is qualified.
Run these commands on the Local Authority host after the worker service is
online:

```bash
CONTROL="/opt/creeper-fabric/.venv/bin/creeper-fabric-control"
CONFIG="/etc/creeper-fabric/authority.toml"

sudo -u creeper-fabric "$CONTROL" --config "$CONFIG" probe \
  --provider internet_archive \
  --region oci-singapore \
  --hostname example.com

sudo -u creeper-fabric "$CONTROL" --config "$CONFIG" probe \
  --provider arquivo_pt \
  --region oci-singapore \
  --hostname example.com

sudo -u creeper-fabric "$CONTROL" --config "$CONFIG" probe \
  --provider arquivo_pt \
  --region gcp-us-central1 \
  --hostname example.com
```

The probe task itself bypasses qualification but is claimable only by a worker
whose registered region exactly matches `--region`.

Inspect state:

```bash
sudo -u creeper-fabric "$CONTROL" --config "$CONFIG" status
journalctl -u creeper-fabric-worker -f
```

## 6. Submit local hostname pools

Existing local candidate pools remain a first-class input. They are streamed
locally into Authority; baseline years, already accepted HYs, and completed
coverage are subtracted before any remote task is created.

Single hostname:

```bash
sudo -u creeper-fabric "$CONTROL" --config "$CONFIG" host \
  --hostname old-host.example \
  --archive-providers internet_archive,arquivo_pt
```

One-hostname-per-line pool:

```bash
sudo -u creeper-fabric "$CONTROL" --config "$CONFIG" hosts \
  --input /absolute/path/to/candidate_pool.txt \
  --archive-providers internet_archive,arquivo_pt
```

The command computes the same provider-set/resolver fingerprint as the remote
`HistoricalQueryProducer`; do not hand-write coverage identities.

## 7. Start evidence-only exploration

A known historical root:

```bash
sudo -u creeper-fabric "$CONTROL" --config "$CONFIG" explore \
  --url https://example.org/links.html \
  --archive-providers internet_archive,arquivo_pt \
  --seed 1001
```

A centrally calibrated seeded campaign:

```bash
sudo -u creeper-fabric "$CONTROL" --config "$CONFIG" seeded-explore \
  --campaign /opt/creeper-fabric/conf/fabric-search-campaign.example.json \
  --seed 2001 \
  --slot-start 0 \
  --slot-count 4 \
  --search-endpoint https://SEARCH-PROVIDER.example/search \
  --archive-providers internet_archive,arquivo_pt
```

Replace `SEARCH-PROVIDER.example` with a search endpoint you are authorized
to automate. Search URLs/hostnames remain worker-local; workers immediately
consume discovered hostnames through their configured CDX providers and send
only HY admission/evidence traffic to Authority.

## 8. Cloudflare thin worker

The Cloudflare runtime is independent JavaScript and only executes the
positive-only exact-year thin contract.

```bash
cd deploy/fabric/cloudflare-worker
cp wrangler.jsonc.example wrangler.jsonc
# Edit COORDINATOR_URL / WORKER_ID / provider list.
set -a
. /secure/path/cf-thin.secret
set +a
./deploy.sh
```

The deploy script stores the worker HMAC secret using Wrangler secrets and
then deploys the Worker/Cron.

Verify the deployed edge runtime:

```bash
FABRIC_CLOUDFLARE_WORKER_URL=https://creeper-fabric-thin.<account>.workers.dev \
  bash deploy/fabric/cloudflare-worker/smoke.sh
```

## 9. End-to-end deployment order

For a fresh deployment, use this exact order:

1. Install Local Authority with the immutable baseline index.
2. Install Cloudflare Tunnel and verify the public `/healthz` and `/meta`.
3. Provision one HMAC secret per worker identity.
4. Install OCI/GCP workers and run the VM smoke script.
5. Submit provider x region qualification probes from Local Authority.
6. Confirm `creeper-fabric-control status` shows the workers and qualified
   provider regions.
7. Submit local hostname pools and/or evidence-only exploration tasks.
8. Deploy the Cloudflare thin worker after its worker identity/secret exists.

## Operations

```bash
systemctl status creeper-fabric-authority
journalctl -u creeper-fabric-authority -f

systemctl status creeper-fabric-worker
journalctl -u creeper-fabric-worker -f

systemctl status cloudflared
journalctl -u cloudflared -f
```

Workers require only outbound HTTPS. Do not open Creeper worker ports in OCI
security lists or GCP firewall rules.

## Upgrade

Re-run the relevant installer with the same environment. The deployment
checkout is hard-reset to `FABRIC_REF`, `uv sync --frozen` is re-run, and
the corresponding service is restarted.
