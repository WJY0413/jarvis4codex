"""Isolated public-facade E2E for reusable, immutable Loop output schemas.

Uses a real LoopStore and result verifier, with only native Hold/Heartbeat/Monitor
ports replaced by fakes. It never starts a model, service or business task.
"""

import asyncio
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from jarvis_control.loop import LoopController, LoopStore
from jarvis_control.service import JarvisControl
from jarvis_mcp.server import JarvisMcpServer
from adapters.codex_app_server.task_provisioning_adapter import verify_candidate_output


class FakeProvisioner:
    def __init__(self):
        self.ready_calls = 0

    def preflight_projects(self):
        return ["TEST"]

    def creation_enabled(self):
        return True

    def ensure_hold_host_ready(self, *, required_workers):
        self.ready_calls += 1
        return {"status": "ready", "worker_capacity": required_workers}


class LoopSchemaRegistryE2E(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = LoopStore(self.root / "loops")
        self.provisioner = FakeProvisioner()
        self.control = JarvisControl(None, None, provisioner=self.provisioner,
                                     loop_controller=LoopController(self.store))
        self.control.hold = Mock(side_effect=lambda **kw: {
            "status": "accepted", "data": {"hold_id": kw["hold_id"]},
            "target_thread_id": "thread-isolated",
        })
        self.control.heartbeat = Mock(return_value={"status": "active"})
        self.control.monitor = Mock(return_value={
            "status": "completed", "target_thread_id": "thread-isolated",
            "data": {"lifecycle_status": "running", "status": "running",
                     "terminal_confirmed": False, "hold_released": False},
        })
        guards = ExitStack()
        self.addCleanup(guards.close)
        guards.enter_context(patch("subprocess.Popen", side_effect=AssertionError("native process forbidden")))
        guards.enter_context(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))

    @staticmethod
    def output_schema(finding_type="integer"):
        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["candidate_id", "request_id", "turn_number", "status", "finding"],
            "properties": {
                "candidate_id": {"type": "integer"},
                "request_id": {"type": "string"},
                "turn_number": {"type": "integer"},
                "status": {"const": "received"},
                "finding": {"type": finding_type},
            },
        }

    def register(self, version="1", *, schema=None, schema_json=None):
        return self.control.loop(
            action="schema_register", request_id=f"register-{version}",
            project="TEST", schema_id="field-note", schema_version=version,
            schema=schema, schema_json=schema_json,
        )

    def start_options(self, schema_hash):
        return {
            "request_id": "schema-loop-1", "project": "TEST", "prompt": "isolated format check",
            "title": "Schema binding", "target_thread_count": 1, "max_rounds": 1,
            "max_turns": 1, "auto_continue": False, "notifications": False,
            "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
            "threads": [{"slot": "worker-1", "acquire": "create", "title": "Schema worker",
                         "lane": {"candidate_ids": [1], "database_path": str(self.root / "synthetic.db"),
                                  "output_boundary": str(self.root / "output"),
                                  "result_verification": {"mode": "file_receipt",
                                      "receipt_paths": {"1": "receipt.json"},
                                      "terminal_statuses": ["received"],
                                      "schema_ref": {"schema_id": "field-note", "schema_version": "1",
                                                     "schema_sha256": schema_hash}}}}],
        }

    def test_create_upload_replay_conflict_and_legacy_state_preservation(self):
        legacy = {"schema": "jarvis-loop-state/v1", "loop_id": "loop-legacy", "children": []}
        self.store.save("loop-legacy", legacy)
        legacy_path = self.store._path("loop-legacy")
        previous_bytes = legacy_path.read_bytes()

        v1 = self.register(schema=self.output_schema())
        self.assertEqual(v1["status"], "completed")
        digest = v1["data"]["schema_sha256"]
        self.assertEqual(len(digest), 64)
        registry_files = list((self.root / "loops" / "schemas").glob("*.json"))
        self.assertEqual(len(registry_files), 1)
        original_registry_bytes = registry_files[0].read_bytes()

        replay = self.register(schema=self.output_schema())
        self.assertEqual(replay["status"], "completed")
        self.assertEqual(replay["data"]["schema_sha256"], digest)
        self.assertEqual(registry_files[0].read_bytes(), original_registry_bytes)
        self.assertEqual(self.register(schema=self.output_schema("string"))["status"], "invalid_request")

        uploaded = self.register("2", schema_json=json.dumps(self.output_schema("string")))
        self.assertEqual(uploaded["status"], "completed")
        self.assertNotEqual(uploaded["data"]["schema_sha256"], digest)
        self.assertEqual(len(list((self.root / "loops" / "schemas").glob("*.json"))), 2)
        self.assertEqual(self.store.load("loop-legacy"), legacy)
        self.assertEqual(legacy_path.read_bytes(), previous_bytes)

        fresh = JarvisControl(None, None, provisioner=FakeProvisioner(),
                              loop_controller=LoopController(LoopStore(self.root / "loops")))
        read = fresh.loop(action="schema_read", request_id="read-1", project="TEST",
                          schema_id="field-note", schema_version="1", schema_sha256=digest)
        self.assertEqual(read["status"], "completed")
        self.assertEqual(read["data"]["output_schema"], self.output_schema())

    def test_rejects_ambiguous_unsafe_and_wrong_project_before_host(self):
        invalid = [
            dict(schema_json='{"type":"object","type":"array"}'),
            dict(schema_json='{"type":NaN}'),
            dict(schema={"$ref": "https://example.invalid/schema"}),
            dict(schema_json=" " * 262145),
            dict(schema=self.output_schema(), schema_json=json.dumps(self.output_schema())),
        ]
        for index, value in enumerate(invalid):
            with self.subTest(index=index):
                self.assertEqual(self.register(str(index + 1), **value)["status"], "invalid_request")
        self.assertEqual(self.control.loop(action="schema_register", request_id="other",
            project="OTHER", schema_id="field-note", schema_version="1",
            schema=self.output_schema())["status"], "invalid_request")
        self.assertEqual(self.control.loop(action="schema_register", request_id="escape",
            project="TEST", schema_id="../escape", schema_version="1",
            schema=self.output_schema())["status"], "invalid_request")

        registered = self.register(schema=self.output_schema())
        bad = self.start_options("0" * 64)
        self.assertEqual(self.control.loop(action="start", **bad)["status"], "invalid_request")
        self.assertEqual(self.provisioner.ready_calls, 0)
        self.control.hold.assert_not_called()
        self.assertFalse(self.store.exists("loop-schema-loop-1"))
        self.assertEqual(registered["data"]["schema_sha256"], self.control.loop(
            action="schema_read", project="TEST", schema_id="field-note",
            schema_version="1")["data"]["schema_sha256"])

    def test_schema_ref_pin_must_be_explicit_and_well_formed_before_host(self):
        digest = self.register(schema=self.output_schema())["data"]["schema_sha256"]
        for index, pin in enumerate((None, "", 7, False, "a" * 63, "g" * 64, "0" * 64)):
            with self.subTest(pin=pin):
                options = self.start_options(digest)
                options["request_id"] = f"schema-loop-invalid-pin-{index}"
                options["threads"][0]["lane"]["result_verification"]["schema_ref"]["schema_sha256"] = pin
                ready_before = self.provisioner.ready_calls
                holds_before = self.control.hold.call_count
                self.assertEqual(self.control.loop(action="start", **options)["status"], "invalid_request")
                self.assertFalse(self.store.exists(f"loop-{options['request_id']}"))
                self.assertEqual(self.provisioner.ready_calls, ready_before)
                self.assertEqual(self.control.hold.call_count, holds_before)
        pinned = self.control.loop(action="start", **self.start_options(digest))
        self.assertIn(pinned["status"], {"running", "holding", "accepted"})
        self.assertEqual(self.control.hold.call_args.kwargs["input_binding"]
                         ["result_verification"]["schema_ref"]["schema_sha256"], digest)

    def test_selected_snapshot_reaches_hold_and_result_check_without_business_approval(self):
        schema = self.output_schema()
        digest = self.register(schema=schema)["data"]["schema_sha256"]
        started = self.control.loop(action="start", **self.start_options(digest))
        self.assertIn(started["status"], {"running", "holding", "accepted"})
        self.assertEqual(started["data"]["business_status"], "not_evaluated")
        bound = self.control.hold.call_args.kwargs["input_binding"]
        contract = bound["result_verification"]
        self.assertEqual(contract["output_schema"], schema)
        self.assertEqual(contract["schema_ref"]["schema_sha256"], digest)
        saved = self.store.load("loop-schema-loop-1")
        self.assertEqual(saved["children"][0]["lane"]["result_verification"]["output_schema"], schema)

        self.assertEqual(self.register("2", schema=self.output_schema("string"))["status"], "completed")
        self.assertEqual(self.store.load("loop-schema-loop-1")["children"][0]["lane"]["result_verification"]["output_schema"], schema)

        boundary = self.root / "output"
        boundary.mkdir()
        hold_request_id = self.control.hold.call_args.kwargs["request_id"]
        identity = {"candidate_id": 1, "request_id": hold_request_id,
                    "turn_number": 1, "status": "received"}
        (boundary / "receipt.json").write_text(json.dumps({**identity, "output_path": "output.json"}), encoding="utf-8")
        (boundary / "output.json").write_text(json.dumps({**identity, "finding": 7}), encoding="utf-8")
        request = {"request_id": hold_request_id, "input_binding": bound}
        self.assertEqual(verify_candidate_output(request, 1)["status"], "verified")
        (boundary / "output.json").write_text(json.dumps({**identity, "finding": "not an integer"}), encoding="utf-8")
        review = verify_candidate_output(request, 1)
        self.assertEqual(review["status"], "review")
        self.assertIn("output_schema mismatch", review["reason"])

        # A persisted/bound snapshot may not silently drift away from its
        # selected immutable registry hash during result verification.
        tampered = {**bound, "result_verification": {**contract,
            "output_schema": self.output_schema("string")}}
        self.assertEqual(verify_candidate_output({**request, "input_binding": tampered}, 1)["status"], "blocked")

        # Draft 2020-12 format is an annotation unless a FormatChecker is
        # explicitly installed; do not claim email syntax was asserted.
        email_schema = self.output_schema("string")
        email_schema["properties"]["finding"]["format"] = "email"
        annotated = {**contract, "output_schema": email_schema}
        annotated.pop("schema_ref")
        (boundary / "output.json").write_text(json.dumps({**identity, "finding": "not-an-email"}), encoding="utf-8")
        self.assertEqual(verify_candidate_output({**request, "input_binding": {
            **bound, "result_verification": annotated}}, 1)["status"], "verified")


class PublicMcpSchemaTransportE2E(unittest.TestCase):
    def test_uploaded_json_text_reaches_strict_parser_through_official_tool_manager(self):
        with tempfile.TemporaryDirectory() as temp:
            control = JarvisControl(None, None, provisioner=FakeProvisioner(),
                                    loop_controller=LoopController(LoopStore(Path(temp) / "loops")))
            server = JarvisMcpServer(control)

            def register(version, uploaded):
                return asyncio.run(server.mcp._tool_manager.call_tool("jarvis_loop", {
                    "action": "schema_register", "request_id": f"upload-{version}", "project": "TEST",
                    "schema_id": "uploaded", "schema_version": version, "schema_json": uploaded,
                }, None)).structured_content

            self.assertEqual(register("1", '{"type":"object"}')["status"], "completed")
            self.assertEqual(register("2", '{"type":"object","type":"array"}')["status"], "invalid_request")


if __name__ == "__main__":
    unittest.main()
