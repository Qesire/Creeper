"""Preflight checks for a local V3 run."""

from __future__ import annotations

import os
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DoctorReport:
    python_ok: bool
    task_root_ok: bool
    data_root_ok: bool
    writable_data_root: bool
    free_bytes: int
    min_free_bytes: int

    @property
    def ready(self) -> bool:
        return (
            self.python_ok
            and self.task_root_ok
            and self.data_root_ok
            and self.writable_data_root
            and self.free_bytes >= self.min_free_bytes
        )


def run_doctor(task_root: Path, data_root: Path, *, min_free_bytes: int = 20 * 1024**3) -> DoctorReport:
    data_root.mkdir(parents=True, exist_ok=True)
    free_bytes = shutil.disk_usage(data_root).free
    writable = os.access(data_root, os.W_OK)
    return DoctorReport(
        python_ok=sys.version_info >= (3, 12),
        task_root_ok=(task_root / "merged260909-3").is_dir(),
        data_root_ok=data_root.is_dir(),
        writable_data_root=writable,
        free_bytes=free_bytes,
        min_free_bytes=min_free_bytes,
    )
