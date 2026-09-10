from __future__ import annotations

import asyncio
import signal
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from creeper.source_discovery.models import source_key
from creeper.source_discovery.scrapy_sidecar import ScrapyScoutLauncher, ScrapyScoutSpec


class _ImmediateProcess:
    pid = 12345
    returncode = 0

    async def wait(self) -> int:
        return 0


class _BlockingProcess:
    def __init__(self) -> None:
        self.pid = 23456
        self.returncode: int | None = None
        self.released = asyncio.Event()

    async def wait(self) -> int:
        await self.released.wait()
        assert self.returncode is not None
        return self.returncode


class ScrapySidecarAsyncTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.project = self.root / "sidecar"
        self.project.mkdir()
        (self.project / "pyproject.toml").write_text(
            "[project]\nname='sidecar-test'\nversion='0'\n",
            encoding="utf-8",
        )
        (self.project / "uv.lock").write_text("version = 1\n", encoding="utf-8")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def spec(self, name: str = "one") -> ScrapyScoutSpec:
        entrypoint = f"https://example.com/{name}/"
        return ScrapyScoutSpec(
            source_key=source_key(entrypoint),
            start_url=entrypoint,
            jobdir=self.root / f"job-{name}",
            spool_path=self.root / f"{name}.jsonl",
            max_pages=5,
            max_depth=1,
            max_seconds=2,
            max_memory_mb=128,
        )

    async def test_async_launcher_uses_locked_sidecar_in_new_process_session(self) -> None:
        launcher = ScrapyScoutLauncher(self.project, uv_executable="uv-test")
        create = AsyncMock(return_value=_ImmediateProcess())
        with patch(
            "creeper.source_discovery.scrapy_sidecar.asyncio.create_subprocess_exec",
            new=create,
        ):
            result = await launcher.run_async(self.spec())

        self.assertTrue(result.succeeded)
        self.assertEqual(create.await_count, 1)
        args, kwargs = create.await_args
        self.assertEqual(args[:4], ("uv-test", "run", "--locked", "scrapy"))
        self.assertEqual(kwargs["cwd"], self.project.resolve())
        self.assertTrue(kwargs["start_new_session"])

    async def test_cancellation_terminates_whole_sidecar_process_group(self) -> None:
        process = _BlockingProcess()
        launcher = ScrapyScoutLauncher(
            self.project,
            uv_executable="uv-test",
            termination_grace_seconds=0.1,
        )
        create = AsyncMock(return_value=process)

        def signal_group(pid: int, sig: signal.Signals) -> None:
            self.assertEqual(pid, process.pid)
            if sig is signal.SIGTERM:
                process.returncode = -int(signal.SIGTERM)
                process.released.set()

        with (
            patch(
                "creeper.source_discovery.scrapy_sidecar.asyncio.create_subprocess_exec",
                new=create,
            ),
            patch(
                "creeper.source_discovery.scrapy_sidecar.os.killpg",
                side_effect=signal_group,
            ) as killpg,
        ):
            task = asyncio.create_task(launcher.run_async(self.spec("cancel")))
            while create.await_count == 0:
                await asyncio.sleep(0)
            await asyncio.sleep(0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        killpg.assert_called()
        self.assertEqual(killpg.call_args_list[0].args, (process.pid, signal.SIGTERM))


if __name__ == "__main__":
    unittest.main()
