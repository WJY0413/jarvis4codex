"""Real isolated HTTP/SDK E2E, zero model. Authored before fixture implementation."""
import asyncio
import json
import os
from pathlib import Path
import tempfile
import unittest

from suite_fixtures import HttpFixture, sdk_session, decode


class SharedHttpE2E(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        artifact = os.environ.get("JARVIS_CASE_ARTIFACT_DIR")
        if artifact:
            root = Path(artifact) / "http-fixture"
            root.mkdir()
        else:
            self.temp = tempfile.TemporaryDirectory(prefix="jarvis-http-suite-")
            self.addCleanup(self.temp.cleanup)
            root = Path(self.temp.name)
        self.fixture = HttpFixture(root)
        await self.fixture.start()
        self.addAsyncCleanup(self.fixture.close)

    async def test_concurrent_clients_schema_isolation_replay_conflict_and_reconnect(self):
        async with sdk_session(self.fixture.url) as first, sdk_session(self.fixture.url) as second:
            async def register(client, ident, kind):
                return decode(await client.call_tool("jarvis_loop", {
                    "action": "schema_register", "request_id": "request-" + ident,
                    "project": "TEST", "schema_id": ident, "schema_version": "1",
                    "schema": {"type": kind}}))
            a, b = await asyncio.gather(register(first, "alpha", "object"), register(second, "beta", "array"))
            self.assertEqual((a["status"], b["status"]), ("completed", "completed"))
            self.assertNotEqual(a["data"]["schema_sha256"], b["data"]["schema_sha256"])
            before = {p.name: p.read_bytes() for p in (self.fixture.state / "loops/schemas").glob("*.json")}
            replay = await register(second, "alpha", "object")
            self.assertEqual(replay["data"]["schema_sha256"], a["data"]["schema_sha256"])
            conflict = await register(first, "alpha", "string")
            self.assertEqual(conflict["status"], "invalid_request")
            self.assertEqual(before, {p.name: p.read_bytes() for p in (self.fixture.state / "loops/schemas").glob("*.json")})
        self.assertIsNone(self.fixture.process.poll(), "disconnect must not terminate shared service")
        async with sdk_session(self.fixture.url) as again:
            saved = decode(await again.call_tool("jarvis_loop", {"action": "schema_read", "project": "TEST",
                "schema_id": "alpha", "schema_version": "1", "schema_sha256": a["data"]["schema_sha256"]}))
            self.assertEqual(saved["data"]["output_schema"], {"type": "object"})
        self.assertFalse(list(self.fixture.state.glob("task-holds/*/dispatch-commits/*")))
        self.fixture.save_evidence("shared-schema", {"first": a, "second": b, "replay": replay,
            "conflict": conflict, "reconnected_read": saved, "model_turns": 0})

    async def test_unknown_close_is_durable_and_never_confirms_release(self):
        root = self.fixture.state / "task-holds" / "unknown-hold"
        root.mkdir(parents=True)
        request = {"request_id": "original", "hold_id": "unknown-hold", "mode": "recover",
                   "thread_id": "synthetic-thread", "turn_id": "synthetic-turn"}
        for name, payload in (("request.json", request), ("ack.json", {**request, "status": "failed",
            "terminal_confirmed": False, "hold_released": False, "execution_evidence": "unknown"})):
            (root / name).write_text(json.dumps(payload))
        original_ack = (root / "ack.json").read_bytes()
        async with sdk_session(self.fixture.url) as session:
            response = decode(await session.call_tool("jarvis_close", {"hold_id": "unknown-hold", "request_id": "exact-close"}))
            repeated = decode(await session.call_tool("jarvis_close", {"hold_id": "unknown-hold", "request_id": "exact-close"}))
            self.assertEqual(response["status"], "closed_unconfirmed")
            self.assertFalse(response["data"]["terminal_confirmed"])
            self.assertFalse(response["data"]["hold_released"])
            self.assertEqual(repeated["status"], "closed_unconfirmed")
        # Existing adapter contract permits only the durable stop marker on
        # request.json; identity/input fields and execution ack stay intact.
        saved_request = json.loads((root / "request.json").read_text())
        self.assertTrue(saved_request.pop("stop_requested"))
        self.assertEqual(saved_request, request)
        self.assertEqual((root / "ack.json").read_bytes(), original_ack)
        self.assertTrue((root / "cancellation.json").is_file())
        self.fixture.save_evidence("unknown-close", {"response": response, "repeat": repeated, "model_turns": 0})

    async def test_readonly_clients_resource_repeat_and_service_exit(self):
        traces = []
        for round_number in range(2):
            async def read():
                async with sdk_session(self.fixture.url) as session:
                    initialized = await session.initialize()
                    tools = await session.list_tools()
                    capabilities = decode(await session.call_tool("jarvis_read", {"subject": "capabilities"}))
                    self.assertEqual(initialized.server_info.version, "0.3.1")
                    self.assertEqual(len(tools.tools), 17)
                    self.assertEqual(capabilities["status"], "completed")
                    return {"version": initialized.server_info.version, "tools": len(tools.tools)}
            self.fixture.sample("idle-before-" + str(round_number))
            rows = await asyncio.gather(read(), read(), read())
            self.fixture.sample("after-clients-" + str(round_number))
            traces.append(rows)
        self.assertIsNone(self.fixture.process.poll())
        await self.fixture.close()
        self.assertIsNotNone(self.fixture.process.returncode)
        self.assertTrue(self.fixture.exit_confirmed)
        self.fixture.save_evidence("resource-repeat", {"rounds": traces, "resource_samples": self.fixture.samples,
            "service_exit_confirmed": True, "acceptance": "measurement_only_no_threshold", "model_turns": 0})
