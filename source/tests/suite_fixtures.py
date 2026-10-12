"""Canonical synthetic fixtures. No credentials, business tasks or runtime state."""
import asyncio
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import unittest


class EntryCases(unittest.TestCase):
    """Deliberate outcomes for the runner's previously authored CLI E2E."""
    def test_pass(self):
        self.assertTrue(True)

    def test_fail(self):
        self.fail("synthetic assertion failure")

    def test_error(self):
        raise RuntimeError("synthetic method error")

    def test_skip(self):
        self.skipTest("synthetic whole-method skip")

    def test_partial(self):
        with self.subTest(kind="executed"):
            self.assertTrue(True)
        with self.subTest(kind="skipped"):
            self.skipTest("synthetic child skip")

    def test_subtests(self):
        for kind in ("passed", "failed-1", "failed-2", "error", "skipped"):
            with self.subTest(kind=kind):
                if kind.startswith("failed"):
                    self.fail("synthetic subtest failure")
                if kind == "error":
                    raise RuntimeError("synthetic child error")
                if kind == "skipped":
                    self.skipTest("synthetic child skip")

    def test_spawn_timeout(self):
        directory = Path(os.environ["JARVIS_SUITE_RUN_DIR"])
        child = subprocess.Popen([sys.executable, "-B", "-c", "import time;time.sleep(300)"],
                                 creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        (directory / "fixture-child.json").write_text(json.dumps({"pid": child.pid}))
        time.sleep(300)  # Runner deadline/cancel must reclaim this exact owned tree.


def decode(result):
    return result.structured_content or json.loads(result.content[0].text)


@asynccontextmanager
async def sdk_session(url):
    import httpx2
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client
    async with httpx2.AsyncClient(trust_env=False, timeout=httpx2.Timeout(5, read=15)) as client:
        async with streamable_http_client(url, http_client=client) as (reader, writer):
            async with ClientSession(reader, writer, read_timeout_seconds=5) as session:
                await session.initialize()
                yield session


class HttpFixture:
    """Starts only a new loopback TEST process with live creation disabled."""
    def __init__(self, root):
        self.root = root
        self.state = root / "state"
        self.process = None
        self.samples = []
        self.exit_confirmed = False
        self.log = None
        self.tree = None

    async def start(self):
        source = Path(os.environ["JARVIS_TEST_SOURCE"])
        for name in ("empty-home", "project", "state"):
            (self.root / name).mkdir()
        launcher = self.root / "launcher.json"
        launcher.write_text(json.dumps({"dispatcher_thread_id": "TEST-offline-dispatcher",
            "profile": "canonical-http-test", "expected_codex_home": str(self.root / "empty-home"),
            "runtime_environments": [], "live_creation_enabled": False, "worker_capacity": 1,
            "allowed_projects": {"TEST": str(self.root / "project")}}))
        heartbeat = self.root / "heartbeat.json"
        heartbeat.write_text(json.dumps({"db_path": str(self.root / "heartbeat.sqlite"),
            "health_path": str(self.root / "health.json"), "lock_path": str(self.root / "heartbeat.lock"),
            "heartbeat_contracts_dir": str(self.root / "contracts"),
            "native_task_launcher_config": str(launcher),
            "standard_bridge_package_root": str(source / "packages"),
            "desktop_recovery_enabled": False, "max_concurrent_runs": 1}))
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        self.url = f"http://127.0.0.1:{port}/mcp"
        env = dict(os.environ, PYTHONPATH=os.pathsep.join((str(source / "packages"),
            str(source / "packages/jarvis_runtime"))), PYTHONDONTWRITEBYTECODE="1")
        self.log = (self.root / "http-server.log").open("w", encoding="utf-8")
        from run_suite import start_owned
        self.process, self.tree = start_owned([sys.executable, "-B", "-m", "jarvis_mcp.entrypoint",
            "--config", str(heartbeat), "--launcher-config", str(launcher),
            "--state-dir", str(self.state), "--transport", "streamable-http", "--port", str(port)],
            cwd=self.root, env=env, stdout=self.log, stderr=self.log)
        try:
            limit = time.monotonic() + 10
            while time.monotonic() < limit:
                if self.process.poll() is not None:
                    raise RuntimeError("isolated HTTP child exited: " + (self.root / "http-server.log").read_text())
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=.1):
                        break
                except OSError:
                    await asyncio.sleep(.05)
            else:
                raise TimeoutError("isolated TEST HTTP readiness")
            self.sample("idle-ready")
        except BaseException:
            await self.close()
            raise

    def sample(self, phase):
        from run_suite import process_sample
        row = process_sample(self.process.pid)
        self.samples.append({"phase": phase, "monotonic": time.monotonic(), "process": row,
                             "boundary": "service launcher; full owned tree sampled by canonical runner"})
        if self.tree and not self.exit_confirmed:
            self.tree.sample(phase)

    async def close(self):
        if self.exit_confirmed:
            return
        if self.process is not None:
            self.sample("before-exit")
            if self.tree:
                cleanup = await asyncio.to_thread(self.tree.close)
                self.exit_confirmed = cleanup["confirmed"]
                self.samples.extend(self.tree.samples)
                if not self.exit_confirmed:
                    raise RuntimeError("isolated HTTP owned tree did not exit: " + json.dumps(cleanup))
            else:
                self.process.terminate()
                await asyncio.to_thread(self.process.wait, 5)
                self.exit_confirmed = False
            self.sample("after-exit")
        if self.log is not None:
            self.log.close()

    def save_evidence(self, name, value):
        directory = Path(os.environ["JARVIS_CASE_ARTIFACT_DIR"])
        directory.mkdir(parents=True, exist_ok=True)
        (directory / (name + ".json")).write_text(json.dumps(value, indent=2) + "\n")
        if (self.root / "http-server.log").exists():
            (directory / (name + ".server.log")).write_bytes((self.root / "http-server.log").read_bytes())
