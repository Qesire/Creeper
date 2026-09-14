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

## 1. Local Authority

Clone this derivative branch on the machine that owns the immutable baseline
index, then run:

```bash
sudo -E \
  FABRIC_BASELINE_INDEX=/absolute/path/to/baseline.sqlite3 \
  bash deploy/fabric/local-authority/install.sh
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
  bash deploy/fabric/local-authority/install-cloudflared.sh
```

Optional verification:

```bash
curl -fsS https://fabric.example.com/healthz
curl -fsS https://fabric.example.com/meta
```

Do not use a Quick Tunnel for production.

## 2. Provision one worker identity

On the Local Authority machine:

```bash
sudo deploy/fabric/local-authority/provision-worker-secret.sh oci-sg-01 \
  > /root/oci-sg-01.secret
```

The script atomically updates the Authority credential file, restarts
Authority, and prints the newly generated HMAC secret once. Transfer that file
over an authenticated admin channel (for example SSH/SCP), not via instance
metadata or the repository.

## 3. OCI persistent explorer

On the OCI VM:

```bash
sudo install -m 0600 /path/from/scp/oci-sg-01.secret /root/fabric-worker.secret

sudo -E \
  FABRIC_COORDINATOR_URL=https://fabric.example.com \
  FABRIC_WORKER_ID=oci-sg-01 \
  FABRIC_REGION=oci-singapore \
  FABRIC_WORKER_SECRET_FILE=/root/fabric-worker.secret \
  bash deploy/fabric/oci/install.sh
```

Default OCI profile:

- `ONLINE_QUERY`
- `WEB_DISCOVERY`
- `SEARCH_QUERY`
- Internet Archive + Arquivo
- no daily egress cap unless explicitly configured

The worker runs as `creeper-fabric-worker.service`.

## 4. GCP metered explorer

On the GCP VM:

```bash
sudo install -m 0600 /path/from/scp/gcp-01.secret /root/fabric-worker.secret

sudo -E \
  FABRIC_COORDINATOR_URL=https://fabric.example.com \
  FABRIC_WORKER_ID=gcp-01 \
  FABRIC_REGION=gcp-us-central1 \
  FABRIC_WORKER_SECRET_FILE=/root/fabric-worker.secret \
  FABRIC_DAILY_EGRESS_BUDGET_BYTES=268435456 \
  bash deploy/fabric/gcp/install.sh
```

The default GCP profile is deliberately conservative: it uses Arquivo as its
archive provider and enables a 256 MiB/day raw response-byte guard. Override
the budget explicitly if required.

Google Compute Engine startup scripts may invoke the same installer, but do
not embed `CREEPER_WORKER_SECRET` directly in startup-script metadata.
Provision the root-only secret file first or retrieve it from a dedicated
secret system.

## 5. Cloudflare thin worker

The Cloudflare runtime is independent JavaScript and only executes the
positive-only exact-year thin contract.

```bash
cd deploy/fabric/cloudflare-worker
cp wrangler.jsonc.example wrangler.jsonc
# Edit COORDINATOR_URL / WORKER_ID / provider list.
export CREEPER_WORKER_SECRET="$(cat /secure/path/cf-thin.secret)"
bash deploy.sh
```

The deploy script stores the worker HMAC secret using Wrangler secrets and
then deploys the Worker/Cron.

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
