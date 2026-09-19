# Creeper Fabric Cloud Provisioning Handoff for Local Codex

Status: production provisioning handoff for the independent Creeper Fabric derivative.

This document is intended to be given directly to a local Codex/CLI agent that
runs on a trusted workstation where the operator is already authenticated to
Cloudflare, Google Cloud, and/or Oracle Cloud Infrastructure.

The agent must treat cloud resource creation as a two-phase operation:

1. INVENTORY / PLAN — read-only inspection only.
2. APPLY — mutate cloud resources only after the operator approves the exact
   plan (account/project/compartment, region/zone, machine type/shape, network,
   image, public IP behavior, and expected billing/free-tier status).

Do not silently choose a billable resource.

---

## 0. Repository and deployment target

Repository:

```text
https://github.com/Qesire/Creeper.git
```

Branch:

```text
feat/distributed-evidence-fabric-vnext
```

Runtime deployment assets:

```text
deploy/fabric/
```

Required final topology:

```text
Remote worker (OCI / GCP / Cloudflare)
        |
        | outbound HTTPS + Fabric HMAC
        v
fabric.<operator-domain>
        |
Cloudflare Tunnel
        |
127.0.0.1:8088
Local Creeper Fabric Authority
```

Important network invariants:

- Creeper workers do not listen for inbound Creeper traffic.
- Workers never connect to one another.
- Provider traffic (Web/Search/Internet Archive/Arquivo) goes directly from
  the worker to the provider.
- The Local Authority is loopback-only and is published through Cloudflare
  Tunnel.
- Do not open a public firewall rule for TCP/8088.
- SSH is an operator-management channel, not a Creeper runtime dependency.

---

## 1. Non-negotiable safety rules for Codex

Before any cloud mutation, print an explicit PLAN containing:

```text
provider
authenticated account / project / tenancy
target project or compartment
target region / zone / availability domain
VM name
machine type / shape
vCPU / memory
image
subnet / VPC / VCN
public IP behavior
disk size/type
service account / IAM identity
estimated billing classification
resources that will be created
resources that will be reused
resources that will NOT be modified
```

If the operator asked for a free-tier or near-zero-cost deployment:

- do not assume a machine type/shape is free;
- inspect the currently authenticated account and current provider
  documentation / pricing;
- if eligibility cannot be verified, STOP before create and report the
  uncertainty.

Never:

- print API tokens, worker HMAC secrets, private SSH keys, or tunnel tokens;
- commit secrets to Git;
- put Fabric worker HMAC secrets into public instance metadata;
- reuse one Fabric HMAC secret for multiple workers;
- delete or alter unrelated cloud resources;
- create broad inbound firewall rules;
- create a new VPC/VCN if a suitable existing network is available, unless
  the operator explicitly approves the network creation.

All secret files written locally must be mode 0600.

---

# PART A — Cloudflare Tunnel

## A1. Preferred direct-ChatGPT route

If the operator wants ChatGPT itself to manage Cloudflare:

1. Connect the Cloudflare ChatGPT plugin.
2. Re-run the request in this conversation and ask ChatGPT to create the
   Creeper Fabric tunnel.
3. If the plugin cannot perform a required mutation, use ChatGPT Work mode
   so the cloud browser can navigate the Cloudflare dashboard.

The desired Cloudflare resources are:

```text
Tunnel name: creeper-fabric-authority
Type: remotely managed
Public hostname: operator-selected, e.g. fabric.example.com
Origin service: http://127.0.0.1:8088
Catch-all ingress: http_status:404
```

Do not expose the origin directly.

## A2. Local Codex / Cloudflare API route

Required environment supplied by the operator:

```bash
export CLOUDFLARE_ACCOUNT_ID=...
export CLOUDFLARE_ZONE_ID=...
export CLOUDFLARE_API_TOKEN=...
export FABRIC_PUBLIC_HOSTNAME=fabric.example.com
```

The API token should be scoped only as needed for Tunnel management and DNS
changes.

### Inventory

Verify token/account access first. List existing tunnels and DNS records.
Do not create duplicates if an existing healthy Creeper Fabric tunnel already
matches the intended hostname/origin.

Expected public hostname must belong to the Cloudflare-managed zone.

### Create remotely-managed tunnel

Generate a random 32-byte secret locally and base64-encode it. Do not print the
secret.

Use the Cloudflare API:

```text
POST /client/v4/accounts/{ACCOUNT_ID}/cfd_tunnel
```

Body semantics:

```json
{
  "name": "creeper-fabric-authority",
  "config_src": "cloudflare",
  "tunnel_secret": "<base64 32-byte random secret>"
}
```

Capture only the returned tunnel ID in normal logs.

### Configure ingress

Replace the remotely-managed tunnel configuration:

```text
PUT /client/v4/accounts/{ACCOUNT_ID}/cfd_tunnel/{TUNNEL_ID}/configurations
```

with ingress equivalent to:

```json
{
  "config": {
    "ingress": [
      {
        "hostname": "fabric.example.com",
        "service": "http://127.0.0.1:8088"
      },
      {
        "service": "http_status:404"
      }
    ]
  }
}
```

### DNS

Create or reconcile a proxied CNAME:

```text
fabric.example.com
    -> <TUNNEL_ID>.cfargotunnel.com
```

Do not create a duplicate DNS record if it already exists correctly.

### Retrieve tunnel token

Retrieve the remotely-managed tunnel token using the Cloudflare tunnel token
endpoint. Save it to:

```text
/root/cloudflared-token
```

mode 0600 on the Local Authority machine.

Do not print the token.

### Install connector on Local Authority machine

From the Creeper repository checkout:

```bash
sudo FABRIC_TUNNEL_TOKEN_FILE=/root/cloudflared-token \
  deploy/fabric/local-authority/install-cloudflared.sh
```

Then:

```bash
sudo -E FABRIC_PUBLIC_URL="https://$FABRIC_PUBLIC_HOSTNAME" \
  deploy/fabric/local-authority/smoke.sh
```

Success criteria:

- `cloudflared.service` active;
- public `/healthz` returns success;
- public `/meta` identifies Creeper Fabric;
- Authority remains bound only to loopback.

---

# PART B — Local Authority prerequisites

The Local Authority must exist before remote workers are useful.

Required operator input:

```bash
export FABRIC_BASELINE_INDEX=/absolute/path/to/baseline.sqlite3
```

Install:

```bash
sudo -E \
  FABRIC_BASELINE_INDEX="$FABRIC_BASELINE_INDEX" \
  deploy/fabric/local-authority/install.sh
```

Verify:

```bash
systemctl is-active creeper-fabric-authority.service
curl -fsS http://127.0.0.1:8088/healthz
curl -fsS http://127.0.0.1:8088/meta
```

---

# PART C — Fabric worker identity provisioning

Create one identity per actual runtime node.

Examples:

```bash
sudo deploy/fabric/local-authority/provision-worker-secret.sh oci-sg-01 \
  > /root/oci-sg-01.secret

sudo deploy/fabric/local-authority/provision-worker-secret.sh gcp-us-01 \
  > /root/gcp-us-01.secret
```

Transfer the matching secret file to that node over an authenticated operator
channel such as SCP.

Never reuse the OCI secret for GCP or Cloudflare.

---

# PART D — GCP VM provisioning

## D1. Required local tools and authentication

Codex must first verify:

```bash
gcloud version
gcloud auth list
gcloud config list
```

Required operator variables:

```bash
export GCP_PROJECT=...
export GCP_ZONE=...
export GCP_VM_NAME=creeper-fabric-gcp-01
export GCP_MACHINE_TYPE=...
export FABRIC_PUBLIC_URL=https://fabric.example.com
export FABRIC_WORKER_ID=gcp-us-01
export FABRIC_REGION=gcp-us-central1
```

Do not guess `GCP_PROJECT`, `GCP_ZONE`, or `GCP_MACHINE_TYPE`.

## D2. Inventory / plan

Codex should inspect at minimum:

```bash
gcloud projects describe "$GCP_PROJECT"

gcloud compute zones list   --project="$GCP_PROJECT"

gcloud compute machine-types list   --project="$GCP_PROJECT"   --zones="$GCP_ZONE"

gcloud compute networks list   --project="$GCP_PROJECT"

gcloud compute instances list   --project="$GCP_PROJECT"

gcloud compute disks list   --project="$GCP_PROJECT"
```

Also inspect current billing/free-tier eligibility if that is an operator
constraint. If it cannot be verified, stop before create.

Prefer:

- current supported Debian or Ubuntu public image;
- smallest approved VM matching the operator's cost constraint;
- existing default or approved VPC;
- no inbound Creeper firewall rule;
- only the minimum management access required by the operator;
- ephemeral external IPv4 is acceptable if direct outbound Internet access is
  otherwise unavailable.

The Creeper worker itself does not need a public listening port.

## D3. Create VM

Use a `gcloud compute instances create` command with the exact approved
project/zone/machine type/image/network parameters.

Do not place the Fabric HMAC worker secret in VM metadata.

A startup script may be used for non-secret bootstrap. Google Compute Engine
runs Linux startup scripts as root, so keep the script minimal and auditable.

Preferred sequence:

1. create VM;
2. wait for RUNNING;
3. transfer the root-only Fabric worker secret;
4. SSH to VM;
5. clone the Fabric branch;
6. run `deploy/fabric/gcp/install.sh`;
7. run smoke test.

Example post-create deployment:

```bash
gcloud compute scp /root-or-local/gcp-us-01.secret \
  "$GCP_VM_NAME:/tmp/fabric-worker.secret" \
  --project="$GCP_PROJECT" \
  --zone="$GCP_ZONE"

gcloud compute ssh "$GCP_VM_NAME" \
  --project="$GCP_PROJECT" \
  --zone="$GCP_ZONE" \
  --command='
    set -euo pipefail
    sudo install -m 0600 /tmp/fabric-worker.secret /root/fabric-worker.secret
    rm -f /tmp/fabric-worker.secret
    if [[ ! -d ~/Creeper/.git ]]; then
      git clone --depth 1 --branch feat/distributed-evidence-fabric-vnext \
        https://github.com/Qesire/Creeper.git ~/Creeper
    fi
    cd ~/Creeper
    sudo -E \
      FABRIC_COORDINATOR_URL="__FABRIC_PUBLIC_URL__" \
      FABRIC_WORKER_ID="__FABRIC_WORKER_ID__" \
      FABRIC_REGION="__FABRIC_REGION__" \
      FABRIC_WORKER_SECRET_FILE=/root/fabric-worker.secret \
      FABRIC_DAILY_EGRESS_BUDGET_BYTES=268435456 \
      deploy/fabric/gcp/install.sh
  '
```

Codex must substitute the placeholders from the approved plan before
execution.

## D4. GCP success criteria

```bash
systemctl is-active creeper-fabric-worker.service
journalctl -u creeper-fabric-worker.service -n 100 --no-pager
sudo deploy/fabric/vm-worker/smoke.sh
```

Do not declare the GCP node production-ready until the Local Authority has
qualified its provider x region pair.

---

# PART E — OCI VM provisioning

## E1. Required local tools and authentication

Verify:

```bash
oci --version
oci iam region-subscription list
```

Required operator variables / chosen OCIDs:

```bash
export OCI_COMPARTMENT_ID=ocid1.compartment...
export OCI_AD=...
export OCI_SUBNET_ID=ocid1.subnet...
export OCI_IMAGE_ID=ocid1.image...
export OCI_SHAPE=...
export OCI_VM_NAME=creeper-fabric-oci-01
export OCI_SSH_PUBLIC_KEY_FILE=...
export FABRIC_PUBLIC_URL=https://fabric.example.com
export FABRIC_WORKER_ID=oci-sg-01
export FABRIC_REGION=oci-singapore
```

Do not guess compartment/subnet/image/shape.

## E2. Inventory / plan

Inspect:

```bash
oci iam availability-domain list \
  --compartment-id "$OCI_COMPARTMENT_ID"

oci compute shape list \
  --compartment-id "$OCI_COMPARTMENT_ID" \
  --availability-domain "$OCI_AD"

oci compute image list \
  --compartment-id "$OCI_COMPARTMENT_ID" \
  --operating-system "Canonical Ubuntu" \
  --sort-by TIMECREATED \
  --sort-order DESC

oci network subnet list \
  --compartment-id "$OCI_COMPARTMENT_ID"

oci compute instance list \
  --compartment-id "$OCI_COMPARTMENT_ID"
```

If the operator requires Always Free / zero-cost behavior, verify current
eligibility in the account before mutation. Do not infer it from a historical
shape name.

Prefer an existing VCN/subnet. Do not create a new VCN unless no suitable
network exists and the operator approves it.

## E3. Launch

Use `oci compute instance launch` with the approved:

- compartment;
- availability domain;
- subnet;
- shape;
- image;
- SSH public key;
- display name.

A typical command skeleton is:

```bash
oci compute instance launch \
  --compartment-id "$OCI_COMPARTMENT_ID" \
  --availability-domain "$OCI_AD" \
  --subnet-id "$OCI_SUBNET_ID" \
  --shape "$OCI_SHAPE" \
  --image-id "$OCI_IMAGE_ID" \
  --display-name "$OCI_VM_NAME" \
  --ssh-authorized-keys-file "$OCI_SSH_PUBLIC_KEY_FILE" \
  --assign-public-ip true
```

If the selected flexible shape requires a shape configuration, derive and add
`--shape-config` from the approved OCPU/memory plan. Do not invent values.

Capture the created instance OCID.

Wait for RUNNING and resolve the primary VNIC/public IP using OCI CLI.

## E4. Install worker

SCP the worker-specific secret to the instance, SSH in, clone the Fabric
branch, and run:

```bash
sudo install -m 0600 /tmp/oci-sg-01.secret /root/fabric-worker.secret
rm -f /tmp/oci-sg-01.secret

git clone --depth 1 --branch feat/distributed-evidence-fabric-vnext \
  https://github.com/Qesire/Creeper.git
cd Creeper

sudo -E \
  FABRIC_COORDINATOR_URL="$FABRIC_PUBLIC_URL" \
  FABRIC_WORKER_ID="$FABRIC_WORKER_ID" \
  FABRIC_REGION="$FABRIC_REGION" \
  FABRIC_WORKER_SECRET_FILE=/root/fabric-worker.secret \
  deploy/fabric/oci/install.sh

sudo deploy/fabric/vm-worker/smoke.sh
```

The OCI worker should have no Creeper inbound firewall rule.

---

# PART F — Provider x Region qualification

After each VM service is online, run from the Local Authority host.

Define:

```bash
CONTROL=/opt/creeper-fabric/.venv/bin/creeper-fabric-control
CONFIG=/etc/creeper-fabric/authority.toml
```

OCI example:

```bash
sudo -u creeper-fabric "$CONTROL" --config "$CONFIG" probe \
  --provider internet_archive \
  --region oci-singapore \
  --hostname example.com

sudo -u creeper-fabric "$CONTROL" --config "$CONFIG" probe \
  --provider arquivo_pt \
  --region oci-singapore \
  --hostname example.com
```

GCP example:

```bash
sudo -u creeper-fabric "$CONTROL" --config "$CONFIG" probe \
  --provider arquivo_pt \
  --region gcp-us-central1 \
  --hostname example.com
```

Then:

```bash
sudo -u creeper-fabric "$CONTROL" --config "$CONFIG" status
```

Do not start large production workloads until the intended provider x region
pairs are QUALIFIED.

---

# PART G — First workload smoke

Local candidate hostname:

```bash
sudo -u creeper-fabric "$CONTROL" --config "$CONFIG" host \
  --hostname example.com \
  --archive-providers internet_archive,arquivo_pt
```

Known historical root:

```bash
sudo -u creeper-fabric "$CONTROL" --config "$CONFIG" explore \
  --url https://example.org/links.html \
  --archive-providers internet_archive,arquivo_pt \
  --seed 1001
```

Check:

```bash
sudo -u creeper-fabric "$CONTROL" --config "$CONFIG" status
```

Success means:

- worker registers and heartbeats;
- task is claimed only by an eligible worker;
- provider permit accounting works;
- exploration hostnames are resolved remotely;
- Authority receives positive HY admission/evidence, not raw exploration
  frontier;
- tasks finish without stale lease or provider-region errors.

---

# PART H — What Codex must return to the operator

At the end, Codex must print a concise deployment report containing:

```text
Cloudflare:
  tunnel_id
  public_hostname
  connector_status
  public health/meta status

GCP:
  project
  zone
  instance_name
  instance_id
  machine_type
  external_ip (if used)
  worker_id
  Fabric service state
  provider qualification state

OCI:
  compartment
  availability_domain
  instance_name
  instance_ocid
  shape
  public_ip
  worker_id
  Fabric service state
  provider qualification state

Creeper Fabric:
  Authority health
  registered workers
  provider x region states
  failed tasks (if any)
  exact commands needed to fix remaining failures
```

Do not include any secret values in this report.

---

# Minimal prompt to give local Codex

Use this repository's
`deploy/fabric/CODEX_CLOUD_PROVISIONING_HANDOFF.md` as the authoritative
runbook.

Start with INVENTORY/PLAN only. Inspect my currently authenticated Cloudflare,
GCP and OCI environments. Do not create, delete, resize, or modify cloud
resources until you have printed an exact plan covering account/project/
compartment, region/zone/AD, network, image, VM type/shape, disk, public-IP
behavior, IAM/service-account behavior, and verified billing/free-tier
implications.

After I approve the plan, execute APPLY exactly according to the runbook.
Reuse existing suitable networks where possible. Never put Creeper Fabric HMAC
secrets into Git or public instance metadata. Do not open Creeper worker
inbound ports. Run every smoke test and provider-region qualification step.
Return the final non-secret deployment report defined by the runbook.
