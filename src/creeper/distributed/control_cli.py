"""Operator control CLI for Fabric v2."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from creeper.distributed.config import load_authority_config
from creeper.distributed.models import TaskClass,WorkDefinition
from creeper.distributed.store_factory import open_authority_store


def _parser()->argparse.ArgumentParser:
    parser=argparse.ArgumentParser(prog="creeper-fabric-control")
    parser.add_argument("--config",type=Path,required=True)
    sub=parser.add_subparsers(dest="command",required=True)
    sub.add_parser("status")

    gc=sub.add_parser("gc")
    gc.add_argument("--retention-hours",type=float,default=24.0)
    gc.add_argument("--limit",type=int,default=50000)

    health=sub.add_parser("worker-health")
    health.add_argument("--stale-seconds",type=float,default=180.0)
    health.add_argument("--state-file",type=Path,required=True)

    admit=sub.add_parser("admit")
    admit.add_argument("--producer",required=True)
    admit.add_argument("--task-class",choices=[v.value for v in TaskClass],required=True)
    admit.add_argument("--input-identity",required=True)
    admit.add_argument("--payload-json",default="{}")
    admit.add_argument("--partition",default="")
    admit.add_argument("--algorithm-version",required=True)
    admit.add_argument("--capability",action="append",default=[])
    admit.add_argument("--provider",action="append",default=[])
    admit.add_argument("--priority",type=float,default=0.0)
    admit.add_argument("--max-attempts",type=int,default=8)

    revoke=sub.add_parser("revoke-worker")
    revoke.add_argument("worker_id")
    return parser


def main(argv:list[str]|None=None)->int:
    args=_parser().parse_args(argv)
    config=load_authority_config(args.config)
    store=open_authority_store(
        config.database,
        emit_outbox=config.outbox_enabled,
    )
    try:
        if args.command=="status":
            print(json.dumps(store.status_snapshot(),sort_keys=True,indent=2))
            return 0
        if args.command=="revoke-worker":
            store.revoke_worker(args.worker_id)
            return 0
        if args.command=="worker-health":
            snapshot=store.status_snapshot(
                stale_after_seconds=args.stale_seconds
            )
            health=snapshot.get("worker_health",[])
            stale=sorted(
                str(item["worker_id"])
                for item in health
                if isinstance(item,dict) and bool(item.get("stale"))
            )
            prior_stale:list[str]=[]
            try:
                prior=json.loads(
                    args.state_file.read_text(encoding="utf-8")
                ).get("stale_workers",[])
                if not isinstance(prior,list) or any(
                    not isinstance(item,str) for item in prior
                ):
                    prior_stale=[]
                else:
                    prior_stale=list(prior)
            except (FileNotFoundError,OSError,json.JSONDecodeError,AttributeError):
                prior_stale=[]
            newly_stale=sorted(set(stale)-set(prior_stale))
            args.state_file.parent.mkdir(parents=True,exist_ok=True)
            temporary=args.state_file.with_suffix(args.state_file.suffix+".tmp")
            temporary.write_text(
                json.dumps(
                    {"stale_workers":stale},
                    sort_keys=True,
                    indent=2,
                )+"\n",
                encoding="utf-8",
            )
            temporary.replace(args.state_file)
            print(
                json.dumps(
                    {
                        "stale_workers":stale,
                        "newly_stale":newly_stale,
                    },
                    sort_keys=True,
                )
            )
            return 2 if newly_stale else 0
        if args.command=="gc":
            report=store.gc_transient_state(
                retention_seconds=args.retention_hours*3600.0,
                limit=args.limit,
            )
            print(json.dumps(report,sort_keys=True))
            return 0
        payload=json.loads(args.payload_json)
        if not isinstance(payload,dict):
            raise ValueError("--payload-json must decode to an object")
        work=WorkDefinition(
            producer=args.producer,
            task_class=TaskClass(args.task_class),
            input_identity=args.input_identity,
            payload=payload,
            partition=args.partition,
            algorithm_version=args.algorithm_version,
            required_capabilities=tuple(args.capability),
            required_providers=tuple(args.provider),
            priority=args.priority,
            max_attempts=args.max_attempts,
        )
        task_id,inserted=store.admit_work(work)
        print(json.dumps({"task_id":task_id,"inserted":inserted}))
        return 0
    finally:
        store.close()


if __name__=="__main__":
    raise SystemExit(main())
