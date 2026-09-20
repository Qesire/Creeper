"""Email-only production reporting for Creeper Fabric."""

from __future__ import annotations

import argparse
from datetime import datetime
from email.message import EmailMessage
import json
import os
from pathlib import Path
import shutil
import smtplib
import ssl
from typing import Any
from zoneinfo import ZoneInfo

from creeper.distributed.config import (
    load_authority_config,
    load_email_report_config,
)
from creeper.distributed.store_factory import open_authority_store
from creeper.storage.candidate_store import CandidateStore
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore
from creeper.storage.telemetry_store import RuntimeTelemetryStore


def _error(exc: BaseException) -> dict[str,str]:
    return {"error": f"{type(exc).__name__}: {exc}"}


def _fabric(config_path: Path) -> dict[str,Any]:
    store=None
    try:
        config=load_authority_config(config_path)
        store=open_authority_store(config.database)
        return dict(store.status_snapshot())
    except BaseException as exc:
        return _error(exc)
    finally:
        if store is not None:
            store.close()


def _runtime(root: Path) -> dict[str,Any]:
    result:dict[str,Any]={}
    control_path=root/"control.sqlite3"
    if control_path.exists():
        control=None
        try:
            control=ControlStore(control_path)
            result["control"]={
                "evidence_tasks": control.evidence_task_state_counts(),
                "platform_year_harvests": control.platform_year_harvest_state_counts(),
                "reservoirs": control.reservoir_state_counts(),
                "work_leases": control.work_lease_state_counts(),
            }
        except BaseException as exc:
            result["control"]=_error(exc)
        finally:
            if control is not None:
                control.close()
    else:
        result["control"]={"error": f"missing: {control_path}"}

    evidence_path=root/"evidence.sqlite3"
    if evidence_path.exists():
        evidence=None
        try:
            evidence=EvidenceStore(evidence_path)
            result["evidence"]={
                "host_years": evidence.host_year_count(),
                "capsules": evidence.count(),
                "max_host_year_sequence": evidence.max_host_year_sequence(),
            }
        except BaseException as exc:
            result["evidence"]=_error(exc)
        finally:
            if evidence is not None:
                evidence.close()
    else:
        result["evidence"]={"error": f"missing: {evidence_path}"}

    candidates_path=root/"candidates.sqlite3"
    if candidates_path.exists():
        candidates=None
        try:
            candidates=CandidateStore(candidates_path)
            result["candidates"]={
                "records": candidates.count(),
                "unparsed": candidates.unparsed_count(),
                "status_history": candidates.history_count(),
            }
        except BaseException as exc:
            result["candidates"]=_error(exc)
        finally:
            if candidates is not None:
                candidates.close()
    else:
        result["candidates"]={"error": f"missing: {candidates_path}"}
    return result


def _telemetry(root: Path) -> dict[str,Any]:
    path=root/"telemetry.sqlite3"
    if not path.exists():
        return {"error": f"missing: {path}"}
    store=None
    try:
        store=RuntimeTelemetryStore(path)
        snapshot=store.snapshot()
        return {
            "counters": snapshot.counters,
            "gauges": snapshot.gauges,
        }
    except BaseException as exc:
        return _error(exc)
    finally:
        if store is not None:
            store.close()


def _readiness(root: Path) -> dict[str,Any]:
    path=root/"readiness"/"readiness.json"
    try:
        raw=json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"error": f"missing: {path}"}
    except (OSError,json.JSONDecodeError) as exc:
        return _error(exc)
    if not isinstance(raw,dict):
        return {"error": "readiness report is not a JSON object"}
    keys=(
        "evidence_cursor",
        "latest_evidence_sequence",
        "processed_host_years",
        "novel_host_years",
        "novel_eed",
        "baseline_eed",
        "growth_rate",
        "confirmed_fraction_of_five_percent",
        "prewarm_reached",
        "formal_gate_reached",
        "submission_dispatch_ready",
        "annual",
    )
    return {key:raw[key] for key in keys if key in raw}


def _host(root: Path) -> dict[str,Any]:
    result:dict[str,Any]={}
    try:
        disk=shutil.disk_usage(root)
        result.update(
            disk_total_bytes=disk.total,
            disk_used_bytes=disk.used,
            disk_free_bytes=disk.free,
        )
    except OSError as exc:
        result.update(_error(exc))
    try:
        load1,load5,load15=os.getloadavg()
        result.update(load_1m=load1,load_5m=load5,load_15m=load15)
    except (AttributeError,OSError):
        pass
    meminfo=Path("/proc/meminfo")
    try:
        wanted={"MemTotal","MemAvailable","SwapTotal","SwapFree"}
        for line in meminfo.read_text(encoding="utf-8").splitlines():
            name,sep,value=line.partition(":")
            if sep and name in wanted:
                result[name+"_bytes"]=int(value.split()[0])*1024
    except (OSError,ValueError,IndexError):
        pass
    return result


def collect_snapshot(config_path: Path) -> dict[str,Any]:
    config=load_email_report_config(config_path)
    return {
        "fabric": _fabric(config_path),
        **_runtime(config.runtime_data_root),
        "readiness": _readiness(config.runtime_data_root),
        "telemetry": _telemetry(config.runtime_data_root),
        "host": _host(config.runtime_data_root),
    }


def _load_previous(path: Path) -> dict[str,Any] | None:
    try:
        value=json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value,dict) else None
    except (FileNotFoundError,OSError,json.JSONDecodeError):
        return None


def _save(path: Path, value: dict[str,Any]) -> None:
    path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_suffix(path.suffix+".tmp")
    temporary.write_text(json.dumps(value,sort_keys=True,indent=2)+"\n",encoding="utf-8")
    os.replace(temporary,path)


def _numbers(value: object, prefix: str="") -> dict[str,float]:
    if isinstance(value,bool):
        return {}
    if isinstance(value,(int,float)):
        return {prefix:float(value)} if prefix else {}
    if isinstance(value,str):
        try:
            return {prefix:float(value)} if prefix else {}
        except ValueError:
            return {}
    if not isinstance(value,dict):
        return {}
    result:dict[str,float]={}
    for key,item in value.items():
        name=f"{prefix}.{key}" if prefix else str(key)
        result.update(_numbers(item,name))
    return result


def _deltas(current: dict[str,Any], previous: dict[str,Any] | None) -> dict[str,float]:
    if previous is None:
        return {}
    now,before=_numbers(current),_numbers(previous)
    return {
        key:now[key]-before[key]
        for key in sorted(now.keys()&before.keys())
        if now[key] != before[key]
    }


def render_report(
    snapshot: dict[str,Any],
    *,
    generated_at: datetime,
    previous: dict[str,Any] | None=None,
    kind: str="summary",
    message: str="",
) -> str:
    lines=[
        "Creeper unattended production report",
        f"Generated: {generated_at.isoformat()}",
        f"Kind: {kind}",
    ]
    if message:
        lines.extend(("",f"Event: {message}"))
    for section in (
        "readiness",
        "fabric",
        "telemetry",
        "control",
        "evidence",
        "candidates",
        "host",
    ):
        lines.extend(("",section.upper()))
        lines.append(json.dumps(snapshot.get(section,{}),sort_keys=True,indent=2))
    lines.extend(("", "DELTA SINCE PREVIOUS SUMMARY"))
    deltas=_deltas(snapshot,previous)
    lines.append(
        json.dumps(deltas,sort_keys=True,indent=2)
        if deltas
        else "none / no previous summary"
    )
    return "\n".join(lines)+"\n"


def send_email(config, *, subject: str, body: str) -> None:
    sender,recipient,username,password=config.load_delivery_environment()
    message=EmailMessage()
    message["From"]=sender
    message["To"]=recipient
    message["Subject"]=subject
    message.set_content(body)
    with smtplib.SMTP(config.smtp_host,config.smtp_port,timeout=30) as smtp:
        if config.starttls:
            smtp.starttls(context=ssl.create_default_context())
        smtp.login(username,password)
        smtp.send_message(message)


def main(argv:list[str]|None=None)->int:
    parser=argparse.ArgumentParser(prog="creeper-fabric-email-report")
    parser.add_argument("--config",type=Path,required=True)
    parser.add_argument("--kind",choices=("summary","alert"),default="summary")
    parser.add_argument("--message",default="")
    parser.add_argument("--dry-run",action="store_true")
    args=parser.parse_args(argv)

    config=load_email_report_config(args.config)
    try:
        generated_at=datetime.now(ZoneInfo(config.timezone))
    except Exception as exc:
        parser.error(f"invalid email-report timezone: {exc}")
    snapshot=collect_snapshot(args.config)
    previous=_load_previous(config.resolved_state_file)
    body=render_report(
        snapshot,
        generated_at=generated_at,
        previous=previous,
        kind=args.kind,
        message=args.message,
    )
    readiness=snapshot.get("readiness",{})
    fabric=snapshot.get("fabric",{})
    novel_eed=readiness.get("novel_eed","?") if isinstance(readiness,dict) else "?"
    growth=readiness.get("growth_rate","?") if isinstance(readiness,dict) else "?"
    dead=fabric.get("dead","?") if isinstance(fabric,dict) else "?"
    label="ALERT" if args.kind=="alert" else "daily"
    subject=(
        f"{config.subject_prefix} {label} {generated_at:%Y-%m-%d %H:%M} "
        f"novel-eed={novel_eed} growth={growth} dead={dead}"
    )
    if args.dry_run:
        print(subject)
        print(body,end="")
        return 0
    send_email(config,subject=subject,body=body)
    if args.kind=="summary":
        _save(config.resolved_state_file,snapshot)
    return 0


if __name__=="__main__":
    raise SystemExit(main())
