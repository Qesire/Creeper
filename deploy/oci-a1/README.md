# OCI A1 single-host production profile

This is the canonical unattended deployment for Creeper when one free persistent
host must own the complete runtime and the operator wants email-only reporting.

## Fixed host profile

- Provider: Oracle Cloud Infrastructure Always Free.
- Shape: `VM.Standard.A1.Flex`.
- CPU/RAM: 2 OCPU, 12 GiB RAM.
- OS: Ubuntu 24.04 LTS aarch64.
- Storage: one 200 GB boot volume.

A single 200 GB filesystem is preferred over a 50 GB boot + 150 GB attached
volume. It stays inside the same Always Free block-volume allowance while
removing an attach/mount/fstab failure mode. Creeper still separates data by
path under `/srv/creeper`.

## Runtime ownership

```
                     localhost only
              +--------------------------+
              | PostgreSQL: creeper      |
              +------------+-------------+
                           |
              +------------v-------------+
              | Fabric Authority :8088   |
              | 127.0.0.1 only           |
              +------+-------------+-----+
                     |             |
              +------v-----+ +-----v--------+
              | query      | | evidence     |
              | worker     | | worker       |
              +------------+ +------+-------+
                                  |
                         +--------v---------+
                         | evidence bridge  |
                         +--------+---------+
                                  |
              +-------------------v-------------------+
              | existing SQLite domain authorities    |
              | control/evidence/candidates/telemetry |
              +-------------------+-------------------+
                                  |
                         +--------v---------+
                         | autopilot        |
                         | discovery        |
                         | source producer  |
                         | historical index |
                         | readiness        |
                         +------------------+
```

The source/discovery domain remains local because there is only one machine.
Residual metadata queries and ordinary CDX/RDAP completion still cross the
Fabric lease/fencing/outbox boundary. The local legacy evidence worker is
disabled, so it cannot race the Fabric evidence lane.

## Network boundary

PostgreSQL and Fabric Authority listen only on loopback. Workers are colocated
and use `http://127.0.0.1:8088`. UFW permits SSH and denies other inbound
traffic. No dashboard, metrics listener, webhook, Slack integration, or public
control API is part of this profile.

## Email is the operator surface

A systemd timer sends one summary around 20:00 Asia/Singapore every day.
Core service failures invoke the same reporter in alert mode.

The summary includes:

- Fabric pending / leased / complete / dead work;
- unconsumed ResultBatch and pending outbox counts;
- evidence-task, platform-harvest, reservoir and source-lease state counts;
- proven unique hostname-year count and evidence capsule count;
- candidate/unparsed counts;
- disk, memory and load data;
- numeric deltas since the previous successful summary email.

Mail secrets are environment-only. They never appear in TOML or Git.

Required installer environment:

```bash
export CREEPER_REPORT_FROM='sender@gmail.com'
export CREEPER_REPORT_TO='operator@example.com'
export CREEPER_SMTP_USERNAME='sender@gmail.com'
export CREEPER_SMTP_APP_PASSWORD='application-password'
```

The default SMTP endpoint is Gmail submission on port 587 with STARTTLS. OCI
blocks outbound TCP/25 by default for newer tenancies; this profile therefore
does not depend on port 25. To use another SMTP submission service, change
`[email_report]` in `fabric.toml`.


## Out-of-band host-death email

An in-guest reporter cannot send mail after the VM itself disappears. Keep the
operator surface email-only by adding one OCI Monitoring absence alarm whose
destination is an OCI Notifications email subscription.

Use the Compute metric namespace and an advanced query equivalent to:

```
CpuUtilization[1m]{resourceId = "<INSTANCE_OCID>"}.groupBy(resourceId).absent()
```

This is control-plane monitoring, not a second Creeper host. It covers VM
shutdown, metric disappearance and host-level failure while the in-guest
reporter covers application progress and systemd service failures. Do not add
synthetic workload merely to avoid Always Free idle reclamation.

## One-time data bootstrap

The production pipeline will not start until these two authority inputs exist:

```
/srv/creeper/data/indexes/baseline-fast.sqlite3
/srv/creeper/reference/equivalent_english_domain.json
```

They must be copied to the server once. After that, the workstation is not part
of the runtime topology.

## Install

During PR validation:

```bash
git clone --branch feat/distributed-fabric-v2 https://github.com/Qesire/Creeper.git
cd Creeper
export CREEPER_GIT_REF=feat/distributed-fabric-v2
# export the four mail variables above
bash deploy/oci-a1/install.sh
```

After this profile is merged, omit `CREEPER_GIT_REF`; `main` is the default.

The installer:

1. creates the locked-down `creeper` service account and filesystem;
2. installs PostgreSQL and the locked Python/Scrapy environments;
3. creates a local PostgreSQL database owned by the service account;
4. generates independent HMAC secrets for the two workers;
5. installs systemd units and UFW rules;
6. enables Authority, both Fabric workers, the evidence bridge and email timer;
7. enables autopilot only when both authority input files are present;
8. prints a dry-run email report without transmitting it.

If the two data inputs were copied later, activate the full pipeline with:

```bash
sudo bash /opt/creeper/deploy/oci-a1/start-production.sh
```

## Verification

```bash
systemctl --no-pager --full status \
  creeper-fabric-authority.service \
  creeper-fabric-worker@query.service \
  creeper-fabric-worker@evidence.service \
  creeper-fabric-evidence-bridge.service \
  creeper-autopilot.service \
  creeper-email-report.timer

sudo -u creeper /opt/creeper/.venv/bin/creeper-fabric-control \
  --config /etc/creeper/fabric.toml status

sudo -u creeper /opt/creeper/.venv/bin/creeper-fabric-email-report \
  --config /etc/creeper/fabric.toml --dry-run
```

For a real mail-path test, omit `--dry-run` while loading
`/etc/creeper/report.env` into the environment or invoke the systemd service:

```bash
sudo systemctl start creeper-email-report.service
```

## Failure semantics

- Worker crash: durable local spool replays unacknowledged ResultBatch objects.
- Authority/API crash: PostgreSQL remains authoritative; systemd restarts the
  stateless API.
- Process reincarnation: worker-instance + lease-generation fencing rejects
  stale writes.
- Lost ACK: identical batch identity is replayed idempotently.
- Provider throttle: shared provider budgets/cooldowns are enforced by
  Authority.
- Local legacy evidence duplication: prevented by
  `[evidence] enabled = false` in the autopilot profile.
- Disk/RAM pressure: autopilot resource governor throttles before hard stop.
- Service failure: systemd invokes an alert email; no operator polling is
  required for routine health visibility.


## Free-tier availability caveat

OCI documents that idle Always Free compute instances may be reclaimed when
CPU, network and (for A1) memory utilization all remain below the documented
idle thresholds over seven days. Creeper does not generate artificial keepalive
load to defeat that policy. A productive crawl/search run should normally be
non-idle, but Always Free is not an availability SLA. The daily email exposes
load and resource state so an unexpectedly idle pipeline is visible.
