# Creeper cloud fleet deployment pack

This directory turns the generic Fabric remote-worker profile into a provider-aware
deployment runbook. Cloud terms change, so the values below are a dated planning
snapshot (2026-09-20), not a billing guarantee. Always re-check the linked
official pricing/free-tier pages before setting `CREEPER_APPLY=1`.

## Recommended fleet

| Provider / offer | Current useful characteristics | Creeper role | Operational judgment |
| --- | --- | --- | --- |
| OCI Ampere A1 Always Free | 1,500 OCPU-hours + 9,000 GB-hours/month, equivalent to 2 OCPUs + 12 GB RAM; tenancy shares 200 GB Always Free block storage; 10 TB/month outbound; home region only | **Authority**, local bulk fallback | Primary long-lived core. Capacity can be scarce and idle Always Free compute may be reclaimed. |
| OCI E2.1.Micro Always Free | up to two AMD micro VMs; 1 GB RAM; up to 50 Mbps internet; minimum 47 GB boot volume each, counted inside the same 200 GB tenancy block quota | query, light evidence | Good only if Authority storage leaves enough block-volume quota. |
| GCP Compute Free Tier e2-micro | one e2-micro/month in us-west1/us-central1/us-east1; 1 GB RAM, fractional 0.25 vCPU; 30 GB standard PD; 1 GB/month free outbound from North America | **query**, light evidence | Compute can be Free Tier, but in-use external IPv4 is billed separately. Treat as low-cost unless an IPv6-only design is proven for every provider. |
| Azure free-account burstable VMs | eligible new accounts: 750 h/month each of B1s, B2pts v2 (Arm), B2ats v2 (AMD) for 12 months; B2pts/B2ats are 2 vCPU + 1 GiB; two 64 GB P6 managed disks free for 12 months; first 100 GB/month internet egress free | **bulk**, evidence, query | Best temporary high-value worker pool. Public IP and other networking items can still create charges. |
| AWS Free plan (new-account model) | accounts created on/after 2025-07-15 use credits for up to 6 months; eligible EC2 types include t3/t4g and larger listed types; 100 GB/month internet data transfer out is free across most AWS services | temporary bulk/evidence | Useful burst capacity, not perpetual free infrastructure. Compute/public IPv4 consume credits. |
| Koyeb Free Instance | 0.1 vCPU, 512 MB, 2 GB SSD; free instance cannot be Worker Service or use volumes and scales to zero after 1 hour without traffic | none | **Do not deploy Creeper here.** |

Official references:

- OCI Always Free resources: https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm
- OCI Free Tier overview: https://www.oracle.com/cloud/free/
- GCP Free Tier: https://docs.cloud.google.com/free/docs/free-cloud-features
- GCP network/IP pricing: https://cloud.google.com/vpc/network-pricing
- Azure free services: https://azure.microsoft.com/en-us/pricing/free-services
- Azure bandwidth pricing: https://azure.microsoft.com/en-us/pricing/details/bandwidth/
- Azure Bpsv2/Basv2 VM sizes: https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/b-family
- AWS Free Tier: https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/free-tier.html
- AWS EC2 Free Tier details: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-free-tier-usage.html
- Koyeb free instance limits: https://www.koyeb.com/docs/reference/instances

## Optional academic Azure account

Eligible full-time university students can currently use Azure for Students:
USD 100 credit for 12 months, no credit card required, with annual renewal while
student eligibility continues. Microsoft limits the offer to education,
teaching, non-commercial research, and related software development/testing;
verify that the intended use fits the current offer terms before deploying
competition workloads.

Official reference: https://azure.microsoft.com/en-us/free/students

## Role assignment

Use the network/data characteristics, not just CPU count:

```text
OCI A1 Authority
  PostgreSQL + baseline/index + EvidenceStore + ControlStore
  Fabric Authority + readiness + local bulk fallback
                |
        WireGuard 10.77.0.0/24
      /             |             \
GCP e2-micro     Azure B2ats      AWS/credit
query/light      bulk/evidence    temporary bulk
evidence
```

The worker never owns baseline/submission truth. It may disappear without losing
competition authority.

### Conservative coordinator-upload budgets

These values cap only worker -> Authority HTTP request bytes as measured by the
Creeper spool. They are **not** exact cloud billing meters and do not include all
WireGuard/IP/provider-request overhead.

| Worker class | Suggested initial `CREEPER_COORDINATOR_UPLOAD_BUDGET_BYTES_PER_MONTH` |
| --- | ---: |
| GCP e2-micro | 700000000 |
| Azure free-account VM | 50000000000 |
| AWS credit worker | 50000000000 |
| OCI worker | 100000000000 |

Start smaller for BulkShard until measured witness density is known.

## Scripts

Provider VM creation helpers are intentionally **dry-run by default**:

- `provision-oci-free-worker.sh`
- `provision-gcp-e2-micro.sh`
- `provision-azure-free-worker.sh`
- `provision-aws-credit-worker.sh`

They print the exact cloud CLI command unless `CREEPER_APPLY=1` is set.
Providers with likely billable public-IP/credit components require an additional
explicit opt-in variable.

Fabric/WireGuard enrollment is provider-neutral:

- `prepare-worker-wireguard.sh` — run on the remote VM, creates a persistent
  worker key and `/etc/wireguard/creeper.conf`, but does not start it.
- `authority-enroll-worker.sh` — run on the Authority after copying the
  worker public key; adds the HMAC worker identity and WireGuard peer.
- `../fabric-worker/install.sh` — final generic worker installation/start.

See `COMMANDS.md` for the exact order.

## Cost-safety rules

1. Never assume a VM labeled free makes its **public IPv4, disk, snapshots,
   cross-region traffic, or NAT** free.
2. Keep provider billing budgets/alerts enabled independently of Creeper's local
   coordinator-upload budget.
3. Do not attach GPUs, premium networking, extra disks, load balancers, NAT
   gateways, or reserved public addresses unless deliberately paid for.
4. Prefer worker-initiated WireGuard with `PersistentKeepalive=25`; remote
   workers do not need inbound Fabric ports.
5. Keep TCP/8088 private to the `creeper` WireGuard interface.
6. Treat GCP's free 1 GB/month outbound as a strict constraint; query is the
   default role there.
7. OCI is the only provider in this matrix used as a long-lived free Authority.
   Azure/AWS free offers are time-limited and must have calendar/budget alerts.
