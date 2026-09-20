# Fabric remote free-cloud worker

This profile adds replaceable workers to one central Creeper Authority. It does
not copy the baseline, PostgreSQL, EvidenceStore, ControlStore, readiness state,
or submission truth to the remote host.

## Invariants

- one globally unique `worker_id` per machine/role;
- worker traffic reaches Authority through WireGuard-private HTTP (or HTTPS);
- no public Fabric port is exposed by the worker;
- system time must be NTP-synchronized before the worker starts;
- provider I/O is still governed by the central Authority permit budget;
- provider response bytes and worker-to-Authority upload bytes are separate
  accounting domains;
- `coordinator_upload_budget_bytes_per_month` is enforced locally and
  persisted in the worker spool DB across restarts;
- the worker is disposable: losing it cannot lose authoritative competition
  state.

## Authority preparation

On the OCI Authority host:

1. configure a WireGuard hub address such as `10.77.0.1/24`;
2. run `deploy/oci-a1/enable-multihost-authority.sh`;
3. issue a unique worker HMAC secret with
   `deploy/oci-a1/add-remote-worker.sh <worker-id>`;
4. add the remote peer public key and /32 address to the Authority WireGuard
   configuration.

Never expose TCP/8088 on the public interface.

## Remote install

Copy the WireGuard configuration to `/etc/wireguard/creeper.conf`, then:

```bash
export CREEPER_COORDINATOR_URL='http://10.77.0.1:8088'
export CREEPER_WORKER_ID='gcp-uscentral1-query-01'
export CREEPER_WORKER_REGION='gcp-uscentral1'
export CREEPER_WORKER_SECRET='<secret-issued-by-authority>'
export CREEPER_WORKER_ROLE='query'

# Example for a provider with a small monthly internet-egress allowance.
export CREEPER_COORDINATOR_UPLOAD_BUDGET_BYTES_PER_MONTH=700000000

bash deploy/fabric-worker/install.sh
```

Use role `evidence` only on hosts with enough memory for CDX/RDAP responses.
Small 1 GiB micro instances should normally use role `query`.

The installer intentionally does not invent a worker ID, WireGuard private key,
or cloud-specific egress quota. Those are authority/account identities and must
be explicit.
## Cloud-specific provisioning

For current OCI/GCP/Azure/AWS server characteristics, provider-safe dry-run
provisioning helpers, WireGuard enrollment, and copy-paste command sequences,
see `deploy/cloud-fleet/README.md` and `deploy/cloud-fleet/COMMANDS.md`.

