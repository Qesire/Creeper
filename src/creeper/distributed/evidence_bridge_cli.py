"""Authority-side daemon bridging ControlStore evidence work into Fabric."""

from __future__ import annotations

import argparse
import fcntl
import os
from pathlib import Path
import signal
import time

from creeper.distributed.config import (
    load_authority_config,
    load_evidence_bridge_config,
)
from creeper.distributed.evidence_query import DistributedEvidenceBridge
from creeper.distributed.store_factory import open_authority_store
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


def run_service(config_path: Path, *, once: bool = False) -> int:
    authority_config=load_authority_config(config_path)
    bridge_config=load_evidence_bridge_config(config_path)
    root=bridge_config.runtime_data_root
    root.mkdir(parents=True,exist_ok=True)
    lock_path=root/"locks"/"fabric-evidence-bridge.lock"
    lock_path.parent.mkdir(parents=True,exist_ok=True)
    lock_fd=os.open(lock_path,os.O_RDWR|os.O_CREAT,0o600)
    try:
        fcntl.flock(lock_fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError as exc:
        os.close(lock_fd)
        raise RuntimeError(
            f"Fabric evidence bridge is already running: {lock_path}"
        ) from exc

    fabric=None
    control=None
    evidence=None
    stopping=False

    def stop_handler(_signum, _frame) -> None:
        nonlocal stopping
        stopping=True

    previous_handlers={}
    if not once:
        for signum in (signal.SIGINT,signal.SIGTERM):
            previous_handlers[signum]=signal.getsignal(signum)
            signal.signal(signum,stop_handler)

    try:
        fabric=open_authority_store(authority_config.database)
        control=ControlStore(root/"control.sqlite3")
        evidence=EvidenceStore(root/"evidence.sqlite3")
        bridge=DistributedEvidenceBridge(
            fabric,
            control,
            evidence,
            cdx_provider_configs=bridge_config.cdx_provider_configs,
            rdap_endpoint=bridge_config.rdap_endpoint,
            rdap_timeout=bridge_config.rdap_timeout,
            owner=bridge_config.owner,
            lease_seconds=bridge_config.lease_seconds,
            retry_base_seconds=bridge_config.retry_base_seconds,
            retry_max_seconds=bridge_config.retry_max_seconds,
        )
        supported=(
            ("wayback","rdap")
            if bridge_config.cdx_provider_configs
            else ("rdap",)
        )

        while True:
            before=bridge.drain(limit=bridge_config.drain_limit)
            dispatched=bridge.dispatch(
                limit=bridge_config.dispatch_limit,
                providers=supported,
            )
            after=bridge.drain(limit=bridge_config.drain_limit)
            bridge.reconcile_failed_work()
            if once or stopping:
                return (
                    before.committed
                    + after.committed
                    + dispatched
                )
            activity=(
                before.committed
                + before.failed
                + before.stale
                + after.committed
                + after.failed
                + after.stale
                + dispatched
            )
            if activity == 0:
                time.sleep(bridge_config.poll_seconds)
    finally:
        if evidence is not None:
            evidence.close()
        if control is not None:
            control.close()
        if fabric is not None:
            fabric.close()
        if not once:
            for signum,handler in previous_handlers.items():
                signal.signal(signum,handler)
        try:
            fcntl.flock(lock_fd,fcntl.LOCK_UN)
        finally:
            os.close(lock_fd)


def main(argv:list[str]|None=None)->int:
    parser=argparse.ArgumentParser(prog="creeper-fabric-evidence-bridge")
    parser.add_argument("--config",type=Path,required=True)
    parser.add_argument("--once",action="store_true")
    args=parser.parse_args(argv)
    run_service(args.config,once=args.once)
    return 0


if __name__=="__main__":
    raise SystemExit(main())
