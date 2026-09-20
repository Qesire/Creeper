# Creeper cloud fleet command set

Commands assume the Authority already runs the OCI A1 production profile from
`deploy/oci-a1`.

## 0. Authority: enable multihost Fabric once

Create the Authority WireGuard key if one does not already exist:

```bash
sudo install -d -m 0700 /etc/wireguard
sudo sh -c 'umask 077; test -s /etc/wireguard/creeper-authority.key || wg genkey > /etc/wireguard/creeper-authority.key'
sudo sh -c 'wg pubkey < /etc/wireguard/creeper-authority.key > /etc/wireguard/creeper-authority.pub'
sudo cat /etc/wireguard/creeper-authority.pub
```

Create `/etc/wireguard/creeper.conf`:

```bash
AUTH_PRIV="$(sudo cat /etc/wireguard/creeper-authority.key)"
sudo tee /etc/wireguard/creeper.conf >/dev/null <<EOF
[Interface]
PrivateKey = $AUTH_PRIV
Address = 10.77.0.1/24
ListenPort = 51820
EOF
sudo chmod 600 /etc/wireguard/creeper.conf
sudo bash /opt/creeper/deploy/oci-a1/enable-multihost-authority.sh
curl -fsS http://127.0.0.1:8088/healthz
```

Record:

```bash
AUTHORITY_WG_PUBLIC_KEY="$(sudo cat /etc/wireguard/creeper-authority.pub)"
AUTHORITY_PUBLIC_IP="<OCI public IPv4 or IPv6 endpoint>"
```

Do **not** open TCP/8088 publicly.

## 1. Provision a worker VM

### OCI E2.1.Micro

```bash
export OCI_COMPARTMENT_OCID='ocid1.compartment...'
export OCI_SUBNET_OCID='ocid1.subnet...'
export OCI_IMAGE_OCID='ocid1.image...'       # Ubuntu 24.04 image in the home region
export OCI_AVAILABILITY_DOMAIN='...'
export CREEPER_VM_NAME='creeper-oci-query-01'
export CREEPER_OCI_SHAPE='VM.Standard.E2.1.Micro'

bash deploy/cloud-fleet/provision-oci-free-worker.sh
# inspect output, then:
CREEPER_APPLY=1 bash deploy/cloud-fleet/provision-oci-free-worker.sh
```

### GCP e2-micro

```bash
export CREEPER_GCP_PROJECT='<project-id>'
export CREEPER_GCP_ZONE='us-central1-a'
export CREEPER_VM_NAME='creeper-gcp-query-01'

bash deploy/cloud-fleet/provision-gcp-e2-micro.sh
# External IPv4 is separately billed; only after reviewing pricing:
CREEPER_APPLY=1 CREEPER_ALLOW_BILLABLE_IPV4=1 \
  bash deploy/cloud-fleet/provision-gcp-e2-micro.sh

gcloud compute ssh "$CREEPER_VM_NAME" --zone "$CREEPER_GCP_ZONE" --project "$CREEPER_GCP_PROJECT"
```

### Azure B2ats v2

```bash
export CREEPER_AZ_RESOURCE_GROUP='creeper-workers'
export CREEPER_AZ_LOCATION='eastus'
export CREEPER_AZ_SIZE='Standard_B2ats_v2'
export CREEPER_VM_NAME='creeper-az-eastus-b2ats-01'

bash deploy/cloud-fleet/provision-azure-free-worker.sh
# Public-IP/network items can be billable:
CREEPER_APPLY=1 CREEPER_ALLOW_BILLABLE_PUBLIC_IP=1 \
  bash deploy/cloud-fleet/provision-azure-free-worker.sh

az vm show -d -g "$CREEPER_AZ_RESOURCE_GROUP" -n "$CREEPER_VM_NAME" \
  --query '{publicIps:publicIps,powerState:powerState}' -o table
```

For a second architecture pool, set `CREEPER_AZ_SIZE=Standard_B2pts_v2` after
confirming Arm availability in the chosen region.

### AWS temporary credit worker

Prepare a subnet/security group/key pair first. The worker needs outbound
Internet and SSH for bootstrap; it does not need an inbound Fabric port.

```bash
export CREEPER_AWS_REGION='us-east-1'
export CREEPER_AWS_SUBNET_ID='subnet-...'
export CREEPER_AWS_SECURITY_GROUP_ID='sg-...'
export CREEPER_AWS_KEY_NAME='<existing-keypair>'
export CREEPER_AWS_INSTANCE_TYPE='t4g.small'
export CREEPER_VM_NAME='creeper-aws-t4g-01'

bash deploy/cloud-fleet/provision-aws-credit-worker.sh
CREEPER_APPLY=1 CREEPER_ALLOW_AWS_CREDIT_SPEND=1 \
  bash deploy/cloud-fleet/provision-aws-credit-worker.sh
```

### Firewall note

For all remote workers, restrict inbound TCP/22 to your administration source
range. Do **not** open 8088 or 51820 inbound on the worker. The worker only
needs outbound Internet plus outbound UDP to the Authority's port 51820.

For custom GCP VPCs, Azure NSGs, OCI security lists, or AWS security groups,
create the SSH rule explicitly rather than relying on provider defaults.

## 2. Remote VM: prepare WireGuard identity

Choose a unique /32. Suggested allocation:

```text
10.77.0.1       Authority
10.77.0.20-39   OCI workers
10.77.0.40-79   GCP workers
10.77.0.80-119  Azure workers
10.77.0.120-159 AWS/temporary workers
```

On the remote worker:

```bash
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates git
git clone https://github.com/Qesire/Creeper.git
cd Creeper

export CREEPER_AUTHORITY_WG_PUBLIC_KEY='<authority-public-key>'
export CREEPER_AUTHORITY_ENDPOINT='<authority-public-ip>:51820'
export CREEPER_WORKER_WG_IP='10.77.0.40/32'

sudo -E bash deploy/cloud-fleet/prepare-worker-wireguard.sh
sudo cat /etc/wireguard/creeper-worker.pub
```

For an IPv6 Authority endpoint, WireGuard syntax is
`[2001:db8::1]:51820`.

## 3. Authority: enroll the worker

Copy the remote public key and run:

```bash
cd /opt/creeper
sudo bash deploy/cloud-fleet/authority-enroll-worker.sh \
  gcp-uscentral1-query-01 \
  10.77.0.40/32 \
  '<worker-wireguard-public-key>'
```

The command prints the Fabric HMAC secret **once**. Copy it directly to the
intended worker; never commit it.

Confirm the peer exists:

```bash
sudo wg show creeper
sudo grep -A3 'Creeper worker:' /etc/wireguard/creeper.conf
```

## 4. Remote VM: install the Fabric worker

### GCP query worker

```bash
cd ~/Creeper
export CREEPER_COORDINATOR_URL='http://10.77.0.1:8088'
export CREEPER_WORKER_ID='gcp-uscentral1-query-01'
export CREEPER_WORKER_REGION='gcp-uscentral1'
export CREEPER_WORKER_SECRET='<secret-from-authority>'
export CREEPER_WORKER_ROLE='query'
export CREEPER_COORDINATOR_UPLOAD_BUDGET_BYTES_PER_MONTH=700000000

bash deploy/fabric-worker/install.sh
```

### Azure bulk worker

```bash
cd ~/Creeper
export CREEPER_COORDINATOR_URL='http://10.77.0.1:8088'
export CREEPER_WORKER_ID='azure-eastus-bulk-01'
export CREEPER_WORKER_REGION='azure-eastus'
export CREEPER_WORKER_SECRET='<secret-from-authority>'
export CREEPER_WORKER_ROLE='bulk'
export CREEPER_COORDINATOR_UPLOAD_BUDGET_BYTES_PER_MONTH=50000000000

bash deploy/fabric-worker/install.sh
```

### Azure/AWS evidence worker

```bash
export CREEPER_WORKER_ROLE='evidence'
# keep the other variables provider/region-specific
bash deploy/fabric-worker/install.sh
```

## 5. Verify each connection

On the remote worker:

```bash
sudo systemctl status wg-quick@creeper --no-pager
sudo wg show creeper
ip route get 10.77.0.1
curl -fsS http://10.77.0.1:8088/healthz
curl -fsS http://10.77.0.1:8088/meta
sudo /opt/creeper/deploy/fabric-worker/preflight.sh
sudo systemctl status creeper-fabric-remote-worker --no-pager
sudo journalctl -u creeper-fabric-remote-worker -n 100 --no-pager
```

On the Authority:

```bash
sudo wg show creeper
sudo systemctl status creeper-fabric-authority --no-pager
sudo systemctl status creeper-fabric-worker-health.timer --no-pager
sudo -u creeper /var/lib/creeper/.local/bin/uv run \
  --directory /opt/creeper \
  creeper-fabric-control --config /etc/creeper/fabric.toml status
```

Run the repository connectivity smoke before production changes:

```bash
uv run python -m unittest tests.integration.test_fabric_runtime_smoke -v
```

## 6. First production rollout

Do not start with every cloud at once.

Recommended sequence:

```text
1. OCI Authority only + local bulk fallback
2. + one GCP query worker
3. + one Azure evidence worker
4. + one Azure bulk worker
5. 1k validation window
6. 10k validation window
7. only then add AWS/extra workers
```

For each new node verify:

- worker heartbeat age stays below 180 s;
- no repeated lease churn;
- spool pending count returns to zero;
- provider permits settle;
- coordinator upload use stays below its local monthly budget;
- Novel EED/1000 requests or Novel EED/GB is positive before scaling the role.
