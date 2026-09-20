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
Fabric lease/fencing, worker-spool and authority-inbox boundary. The local
legacy evidence worker is disabled, so it cannot race the Fabric evidence lane.
The broker outbox is explicitly disabled in this HTTP-only profile; otherwise
unused WORK/RESULT events would accumulate forever without a NATS/JetStream
publisher. Generic Fabric deployments keep that capability enabled by default.

## Network boundary

PostgreSQL and Fabric Authority listen only on loopback. Workers are colocated
and use `http://127.0.0.1:8088`. UFW permits SSH and denies other inbound
traffic. No dashboard, metrics listener, webhook, Slack integration, or public
control API is part of this profile.

## Email is the operator surface

A systemd timer sends one summary around 20:00 Asia/Singapore every day.
Core service failures invoke the same reporter in alert mode. Every generated
message is written to a local durable outbox before SMTP is attempted; a
separate 15-minute retry timer replays pending mail after transient SMTP or
network failures. Concurrent report/alert/retry processes share an advisory
outbox lock, so one queued message is not sent concurrently by two senders.

The summary includes:

- active baseline id/digest/EED denominator, raw authority record counts and runtime-index size;
- Novel EED, growth rate, five-percent target progress and readiness gates;
- Fabric pending / leased / complete / dead work;
- runtime counters/gauges, including throughput and yield telemetry already
  published by the production pipeline;
- unconsumed ResultBatch count and the broker-outbox count (expected zero in this profile);
- evidence-task, platform-harvest, reservoir and source-lease state counts;
- proven unique hostname-year count and evidence capsule count;
- candidate/unparsed counts;
- disk, memory and load data;
- numeric deltas since the previous successfully delivered summary email.

Mail secrets are environment-only. They never appear in TOML or Git.
Automatic LLM work is also disabled in this profile (`max_search_directives=0`),
so the free host has no hidden dependency on OpenCode/model credentials. Unknown
formats remain durable HOLD items until an adapter is supplied or that budget is
explicitly enabled later.

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

## Server-resident baseline authority

The production host owns both the immutable raw baseline authority and the
runtime lookup index. The workstation is used only to transfer the files once;
it is not part of the running topology.

Canonical layout after bootstrap:

```
/srv/creeper/baseline/<baseline_id>/
  1996.txt
  1997.txt
  1998.txt
  1999.txt
  2000.txt
  2001.txt
  candidate_pool.txt
  authority-manifest.json

/srv/creeper/baseline/current -> <baseline_id>
/srv/creeper/reference/equivalent_english_domain.json
/srv/creeper/data/indexes/baseline/<baseline_id>.sqlite3
/srv/creeper/data/indexes/baseline-fast.sqlite3 -> baseline/<baseline_id>.sqlite3
```

The six annual files are the competition baseline. `candidate_pool.txt` is
kept separately in the same authority package because baseline lookup and
official-candidate membership share one runtime index but retain distinct table
semantics.

Before transfer, the staged baseline directory must contain a matching
`authority-manifest.json`. The manifest binds the six annual hashes,
candidate-pool hash, EED-model hash and baseline EED denominator. The server
will refuse startup if any one of raw baseline, manifest, EED model or SQLite
index disagrees.

A practical first upload is:

```bash
# local machine
rsync -avP /path/to/<baseline_id>/ ubuntu@SERVER:~/creeper-baseline-stage/
scp /path/to/equivalent_english_domain.json ubuntu@SERVER:~/
```

The uploaded directory must itself be the baseline directory and must contain
`authority-manifest.json`. Then on the OCI host:

```bash
sudo bash /opt/creeper/deploy/oci-a1/bootstrap-baseline.sh \
  /home/ubuntu/creeper-baseline-stage \
  /home/ubuntu/equivalent_english_domain.json

sudo bash /opt/creeper/deploy/oci-a1/start-production.sh
```

`bootstrap-baseline.sh` verifies all hashes before adoption, moves the raw
baseline into `/srv/creeper/baseline/<baseline_id>`, persists the EED model,
builds/resumes the authority-bound SQLite index from the server-resident raw
files, verifies the result, then switches the `current` and
`baseline-fast.sqlite3` symlinks. It refuses to switch to a different
baseline while an existing runtime authority is active; changing baseline
identity requires an explicit runtime rebase rather than silently mixing old
results with a new denominator.

`verify-baseline.sh` is also run by the installer/start gate, so autopilot
cannot start from an unverified standalone index.

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
6. enables Authority, both Fabric workers, the evidence bridge, daily email
   timer, 15-minute durable-email replay timer, and hourly Fabric transient-state GC;
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
  creeper-email-report.timer \
  creeper-email-outbox.timer \
  creeper-fabric-gc.timer

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
- Disk/RAM pressure: autopilot throttles at 4 GiB RSS, stops its child pipeline at 5 GiB, and systemd enforces a 6 GiB cgroup ceiling; each Fabric worker advertises and is capped around a 1 GiB execution envelope, while PostgreSQL uses a small-memory profile.
- Fabric database growth: hourly GC removes replay-safe transient rows older than 24 hours (consumed/quarantined result batches, inactive permits, old nonces/egress counters, and eligible broker events) and compacts eligible COMPLETE work payloads to minimal WorkKey tombstones; task identity/state remains durable.
- Service or GC failure: systemd queues an alert email; transient SMTP failure leaves
  it durable and the retry timer replays it at least once.
- SMTP ACK ambiguity can still produce a duplicate email after a process crash;
  delivery is intentionally at-least-once rather than loss-prone exactly-once.


## Free-tier availability caveat

OCI documents that idle Always Free compute instances may be reclaimed when
CPU, network and (for A1) memory utilization all remain below the documented
idle thresholds over seven days. Creeper does not generate artificial keepalive
load to defeat that policy. A productive crawl/search run should normally be
non-idle, but Always Free is not an availability SLA. The daily email exposes
load and resource state so an unexpectedly idle pipeline is visible.
