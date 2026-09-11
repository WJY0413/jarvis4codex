from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from adapters.codex_app_server.task_provisioning_adapter import (
    CodexAppServerTaskProvisioningAdapter,
    append_terminal_turn_history,
)
from jarvis_control import TaskProvisionRequest
from jarvis_control.provisioning import TaskMonitorResumeRequest, output_schema_validator, validate_saved_output


class FakeConfig:
    profile = "jarvis_test"
    expected_codex_home = "C:/test/codex-home"

    def resolve_project(self, project: str):
        self.project = project
        return project, "C:/test/project"


class TaskProvisioningAdapterContractTest(unittest.TestCase):
    def test_thread_execution_requires_exact_terminal_result_not_live_pid(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "task-holds" / "hold-1"
            root.mkdir(parents=True)
            request = {"request_id": "req-1", "hold_id": "hold-1"}
            ack = {**request, "thread_id": "thread-1", "turn_id": "turn-2", "status": "holding", "pid": 42}
            (root / "request.json").write_text(json.dumps(request))
            (root / "ack.json").write_text(json.dumps(ack))
            claim = root / ".user-host-claim"
            claim.mkdir()
            (claim / "owner.json").write_text(json.dumps({"pid": 42}))
            adapter = CodexAppServerTaskProvisioningAdapter(
                "unused", state_dir=temp, config_loader=lambda _: FakeConfig(), pid_alive=lambda _: True)
            observed = adapter.thread_execution_evidence("thread-1", "turn-2")
            self.assertEqual(observed["status"], "unknown")
            self.assertTrue(observed["owner_alive"])
            adapter._pid_alive = lambda _: False
            dead = adapter.thread_execution_evidence("thread-1", "turn-2")
            self.assertEqual(dead["status"], "unknown")
            self.assertFalse(dead["owner_alive"])
            self.assertIn("not alive", dead["reason"])
            for status in ("completed", "failed", "cancelled", "blocked", "interrupted"):
                with self.subTest(status=status):
                    (root / "result.json").write_text(json.dumps({**ack, "status": status,
                        "terminal_confirmed": True, "output_verification": {"status": "blocked" if status == "blocked" else "verified"}}))
                    observed = adapter.thread_execution_evidence("thread-1", "turn-2")
                    self.assertEqual(observed["status"], status)
                    self.assertEqual(observed["source"], "hold_terminal_result")
            self.assertEqual(adapter.thread_execution_evidence("thread-1", "different-turn")["status"], "unknown")
            self.assertIsNone(adapter.thread_execution_evidence("unmanaged", "turn-2"))
            (root / "result.json").write_text(json.dumps({**ack, "request_id": "old-request",
                "status": "completed", "terminal_confirmed": True}))
            self.assertEqual(adapter.thread_execution_evidence("thread-1", "turn-2")["status"], "unknown")

    def test_history_read_does_not_drop_batch_identity_during_first_migration(self):
        import sqlite3
        import adapters.codex_app_server.task_provisioning_adapter as adapter_module
        with tempfile.TemporaryDirectory(prefix="jarvis-qa-history-") as temp:
            path = Path(temp) / "history.sqlite"
            connection = sqlite3.connect(path)
            try:
                # WAL permits the second connection to commit while a corrected
                # reader holds its old snapshot; only this temporary DB is changed.
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute(
                    "CREATE TABLE turn_history (hold_id TEXT,turn_id TEXT,task_id TEXT,request_id TEXT,"
                    "thread_id TEXT,turn_number INTEGER,candidate_id INTEGER,status TEXT,final_answer TEXT,"
                    "has_final_answer INTEGER,completed_at TEXT,recorded_at TEXT,PRIMARY KEY(hold_id,turn_id))"
                )
                connection.execute("INSERT INTO turn_history VALUES ('h','old','t','r','thread',1,7,'completed','old',1,'a','a')")
                connection.commit()
            finally:
                connection.close()
            original_connect = sqlite3.connect
            injected = False

            class Reader:
                def __init__(self, conn):
                    object.__setattr__(self, "conn", conn)

                def __setattr__(self, name, value):
                    setattr(self.conn, name, value)

                def execute(self, sql, *args):
                    nonlocal injected
                    cursor = self.conn.execute(sql, *args)
                    if sql == "PRAGMA table_info(turn_history)" and not injected:
                        old_columns = cursor.fetchall()
                        injected = True
                        # Real second connection commits after the reader captured the
                        # old column list but before read_turn_history executes SELECT.
                        adapter_module.append_terminal_turn_history(
                            path, hold_id="h", turn_id="batch", task_id="t", request_id="r",
                            thread_id="thread", turn_number=2, candidate_id=None, candidate_ids=[8, 9],
                            output_verification={"status": "verified"}, status="completed",
                            final_answer="done", completed_at="b",
                        )
                        return old_columns
                    return cursor

                def close(self):
                    self.conn.close()

            def connect(*args, **kwargs):
                conn = original_connect(*args, **kwargs)
                return Reader(conn) if kwargs.get("uri") else conn

            with patch.object(adapter_module.sqlite3, "connect", side_effect=connect):
                rows = adapter_module.read_turn_history(path)
            raced = next((row for row in rows if row["turn_id"] == "batch"), None)
            fresh = next(row for row in adapter_module.read_turn_history(path) if row["turn_id"] == "batch")
            print("HISTORY_RACED_QA " + json.dumps(raced), flush=True)
            print("HISTORY_FRESH_QA " + json.dumps(fresh), flush=True)
            self.assertEqual(fresh.get("candidate_ids"), [8, 9])
            self.assertEqual(fresh.get("output_verification"), {"status": "verified"})
            # An old consistent snapshot may omit the newly committed batch.
            # A snapshot that returns the batch must include both new fields.
            if raced is not None:
                self.assertEqual(raced.get("candidate_ids"), [8, 9], "Concurrent read must retain batch identity")
                self.assertEqual(raced.get("output_verification"), {"status": "verified"})

    def test_batch_history_migrates_old_rows_once_and_reads_legacy_without_writes(self):
        import sqlite3
        from concurrent.futures import ThreadPoolExecutor
        from adapters.codex_app_server.task_provisioning_adapter import read_turn_history
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "legacy.sqlite"
            with sqlite3.connect(path) as db:
                db.execute("CREATE TABLE turn_history (hold_id TEXT, turn_id TEXT, task_id TEXT, request_id TEXT, "
                           "thread_id TEXT, turn_number INTEGER, candidate_id INTEGER, status TEXT, final_answer TEXT, "
                           "has_final_answer INTEGER, completed_at TEXT, recorded_at TEXT, PRIMARY KEY(hold_id,turn_id))")
                db.execute("INSERT INTO turn_history VALUES ('h','old','t','r','thread',1,7,'completed','old answer',1,'a','a')")
            db.close()
            before = path.read_bytes()
            old = read_turn_history(path)
            self.assertEqual(path.read_bytes(), before)
            self.assertNotIn("candidate_ids", old[0])
            def append(number):
                append_terminal_turn_history(path, hold_id="h", turn_id=f"new-{number}", task_id="t", request_id="r",
                    thread_id="thread", turn_number=number + 2, candidate_id=None, candidate_ids=[10 + number, 20 + number],
                    output_verification={"status": "verified", "candidate_ids": [10 + number, 20 + number]},
                    status="completed", final_answer="batch", completed_at="b")
            with ThreadPoolExecutor(max_workers=3) as pool:
                list(pool.map(append, range(3)))
            append(0)
            rows = read_turn_history(path)
            self.assertEqual(len(rows), 4)
            self.assertEqual(next(row for row in rows if row["turn_id"] == "old"), old[0])
            for row in (row for row in rows if row["turn_id"] != "old"):
                self.assertIsNone(row["candidate_id"])
                self.assertEqual(row["candidate_ids"], row["output_verification"]["candidate_ids"])

    def test_optional_schema_survives_hold_persistence_without_migration(self):
        for schema in (None, False, {"properties": {"score": {"type": "integer"}}}):
            with self.subTest(schema=schema), tempfile.TemporaryDirectory() as temp:
                binding = {"candidate_ids": [1], "lane_item_count": 1, "output_boundary": "unused",
                           "result_verification": {"receipt_paths": {"1": "1.json"}, "terminal_statuses": ["completed"]}}
                if schema is not None:
                    binding["result_verification"]["output_schema"] = schema
                adapter = CodexAppServerTaskProvisioningAdapter("unused", state_dir=Path(temp), config_loader=lambda _: FakeConfig())
                result = adapter.provision(TaskProvisionRequest(request_id="schema", project="test", title="test",
                    prompt="test", source_ref="test", input_binding=binding))
                self.assertEqual(result.status, "accepted")
                saved_path = Path(temp) / "task-holds/hold-schema/request.json"
                saved = saved_path.read_bytes()
                self.assertEqual(json.loads(saved)["input_binding"], binding)
                reloaded = CodexAppServerTaskProvisioningAdapter("unused", state_dir=Path(temp), config_loader=lambda _: FakeConfig())
                self.assertEqual(reloaded.hold_status("hold-schema")["status"], "accepted")
                self.assertEqual(saved_path.read_bytes(), saved)

    def test_output_schema_standard_keywords_local_refs_and_annotations(self):
        schema = {"$schema": "https://json-schema.org/draft/2020-12/schema",
                  "$defs": {"positive": {"$anchor": "positive", "type": "integer", "minimum": 1}},
                  "type": "object", "properties": {"score": {"$ref": "#positive"}},
                  "required": ["score"], "unevaluatedProperties": False,
                  "examples": [{"$ref": "https://example.invalid/this-is-data"}]}
        with patch("socket.socket", side_effect=AssertionError("network forbidden")):
            validate_saved_output(schema, {"score": 2})
            with self.assertRaisesRegex(ValueError, r"/score.*minimum"):
                validate_saved_output(schema, {"score": 0})
            with self.assertRaisesRegex(ValueError, "unevaluatedProperties"):
                validate_saved_output(schema, {"score": 1, "extra": True})
            validate_saved_output({"$defs": {"n": {"type": "integer"}}, "$ref": "#/$defs/n"}, 2)
            validate_saved_output({"$dynamicAnchor": "node", "type": "object",
                                   "properties": {"child": {"$dynamicRef": "#node"}}}, {"child": {}})
            validate_saved_output(True, {})
            validate_saved_output({}, {})
            with self.assertRaisesRegex(ValueError, "mismatch"):
                validate_saved_output(False, {})

    def test_output_schema_rejects_invalid_dialects_and_references_without_io(self):
        cases = [None, [], {"type": "unknown"}, {"properties": {"x": {"type": 1}}},
                 {"$schema": "http://json-schema.org/draft-07/schema#"},
                 {"$defs": {"x": {"$schema": "https://example.invalid/meta"}}},
                 {"$id": "urn:test"}, {"$defs": {"x": {"$id": "local"}}},
                 {"$ref": "#/$defs/missing"}, {"$ref": "#unknown"},
                 {"$ref": "https://example.invalid/schema"}, {"$ref": "file:///secret.json"},
                 {"$ref": "other.json#/$defs/x"}, {"$dynamicRef": "https://example.invalid/schema"},
                 {"$defs": {"unused": {"$ref": "https://example.invalid/schema"}}},
                 {"examples": [{"$ref": "https://example.invalid/schema"}], "$ref": "#/examples/0"}]
        with patch("socket.socket", side_effect=AssertionError("network forbidden")):
            for schema in cases:
                with self.subTest(schema=schema), self.assertRaises(ValueError):
                    output_schema_validator(schema)
            with self.assertRaisesRegex(ValueError, "reference evaluation failed"):
                validate_saved_output({"$ref": "#"}, {})

    def test_stop_latch_preserves_legacy_request_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "task-monitors" / "legacy"
            root.mkdir(parents=True)
            request_path = root / "request.json"
            legacy = {"request_id": "old", "monitor_id": "legacy", "mode": "recover",
                      "input_binding": {"candidate_ids": [7], "lane_item_count": 1}, "turn_id": "old-turn"}
            request_path.write_text(json.dumps(legacy), encoding="utf-8")
            request_path.with_suffix(".lock").write_text(json.dumps({"pid": 987654321}), encoding="utf-8")
            adapter = CodexAppServerTaskProvisioningAdapter("unused", state_dir=Path(temp), config_loader=lambda _: FakeConfig())
            first = adapter.request_hold_stop("legacy")
            saved = request_path.read_bytes()
            second = adapter.request_hold_stop("legacy")
            self.assertEqual(first, second)
            self.assertEqual(request_path.read_bytes(), saved)
            self.assertEqual(json.loads(saved), {**legacy, "stop_requested": True})
            self.assertFalse(request_path.with_suffix(".lock").exists())
            with self.assertRaisesRegex(RuntimeError, "not found"):
                adapter.request_hold_stop("missing")

    def test_status_distinguishes_terminal_receipt_and_host_release(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "task-holds" / "hold-test"
            root.mkdir(parents=True)
            (root / "result.json").write_text(json.dumps({"status": "blocked", "terminal_confirmed": True}), encoding="utf-8")
            claim = root / ".user-host-claim"
            claim.mkdir()
            adapter = CodexAppServerTaskProvisioningAdapter("unused", state_dir=Path(temp), config_loader=lambda _: FakeConfig())
            self.assertFalse(adapter.hold_status("hold-test")["hold_released"])
            claim.rmdir()
            self.assertTrue(adapter.hold_status("hold-test")["hold_released"])
            (root / "result.json").write_text(json.dumps({"status": "failed", "terminal_confirmed": False}), encoding="utf-8")
            self.assertFalse(adapter.hold_status("hold-test")["hold_released"])

    def test_explicit_missing_or_mismatched_existing_hold_stays_fail_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "task-holds" / "hold-test"
            root.mkdir(parents=True)
            adapter = CodexAppServerTaskProvisioningAdapter("unused", state_dir=Path(temp), config_loader=lambda _: FakeConfig())
            for existing in (None, {"status": "completed", "thread_id": "wrong-thread"}):
                if existing is not None:
                    (root / "result.json").write_text(json.dumps(existing), encoding="utf-8")
                receipt = adapter.resume_with_monitor(TaskMonitorResumeRequest(
                    request_id="resume", task_id="correct-thread", prompt="test", source_ref="test", hold_id="hold-test"))
                self.assertEqual(receipt.status, "failed")
                self.assertFalse((root / "request.json").exists())
                self.assertFalse((root / "history").exists())

    def test_hold_host_health_blocks_a_fresh_identity_with_a_dead_pid(self):
        config = FakeConfig()
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            config_path = state_dir / "launcher.json"
            (state_dir / "hold-host.json").write_text(json.dumps({
                "status": "holding",
                "pid": 1780,
                "active_count": 3,
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "profile": config.profile,
                "codex_home": config.expected_codex_home,
                "state_dir": str(state_dir.resolve()),
            }), encoding="utf-8")
            adapter = CodexAppServerTaskProvisioningAdapter(
                config_path, state_dir=state_dir, config_loader=lambda _: config,
                pid_alive=lambda _: False,
            )

            health = adapter.hold_host_health()

        self.assertEqual(health, {
            "status": "host_not_ready", "reason": "HoldHost PID is not live",
        })

    def test_ensure_hold_host_ready_preserves_requests_above_ten(self):
        initializer = Mock(return_value={"status": "ready", "phase": "started"})
        with tempfile.TemporaryDirectory() as temp:
            adapter = CodexAppServerTaskProvisioningAdapter("unused", state_dir=Path(temp), host_initializer=initializer)
            with patch.object(adapter, "hold_host_health", side_effect=[
                {"status": "host_not_ready", "reason": "missing"}, {"status": "ready"}]):
                self.assertEqual(adapter.ensure_hold_host_ready(required_workers=12)["status"], "ready")
        self.assertEqual(initializer.call_args.kwargs["workers"], 12)

    def test_ensure_hold_host_ready_starts_a_missing_host_then_rechecks_health(self):
        config = FakeConfig()
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            config_path = state_dir / "launcher.json"

            def initialize_host(**kwargs):
                self.assertEqual(kwargs["state_dir"], state_dir)
                self.assertEqual(kwargs["launcher_config"], config_path)
                self.assertEqual(kwargs["workers"], 10)
                (state_dir / "hold-host.json").write_text(json.dumps({
                    "status": "ready", "pid": 1781, "worker_capacity": 10,
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                    "host_started_at": datetime.now(timezone.utc).isoformat(),
                    "profile": config.profile, "codex_home": config.expected_codex_home,
                    "state_dir": str(state_dir.resolve()),
                }), encoding="utf-8")
                return {"status": "ready", "phase": "started"}

            initializer = Mock(side_effect=initialize_host)
            adapter = CodexAppServerTaskProvisioningAdapter(
                config_path, state_dir=state_dir, config_loader=lambda _: config,
                host_initializer=initializer, pid_alive=lambda _: True,
                pid_started_at=lambda _: datetime.now(timezone.utc),
            )

            health = adapter.ensure_hold_host_ready(required_workers=3)
            reused = adapter.ensure_hold_host_ready(required_workers=7)
            self.assertEqual(reused, {"status": "ready", "phase": "already_running"})
            self.assertFalse((state_dir / "task-holds").exists())

        self.assertEqual(health, {"status": "ready", "phase": "started"})
        initializer.assert_called_once()

    def test_ensure_hold_host_ready_does_not_start_a_second_live_host_for_more_capacity(self):
        config = FakeConfig()
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            (state_dir / "hold-host.json").write_text(json.dumps({
                "status": "holding", "active_count": 1, "pid": 1781, "worker_capacity": 1,
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "host_started_at": datetime.now(timezone.utc).isoformat(),
                "profile": config.profile, "codex_home": config.expected_codex_home,
                "state_dir": str(state_dir.resolve()),
            }), encoding="utf-8")
            initializer = Mock()
            adapter = CodexAppServerTaskProvisioningAdapter(
                state_dir / "launcher.json", state_dir=state_dir, config_loader=lambda _: config,
                host_initializer=initializer, pid_alive=lambda _: True,
                pid_started_at=lambda _: datetime.now(timezone.utc),
            )

            health = adapter.ensure_hold_host_ready(required_workers=3)

        self.assertEqual(health, {
            "status": "host_not_ready", "reason": "HoldHost worker capacity is insufficient",
        })
        initializer.assert_not_called()

    def test_hold_host_health_blocks_a_stale_identity(self):
        config = FakeConfig()
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            (state_dir / "hold-host.json").write_text(json.dumps({
                "status": "ready",
                "pid": 1780,
                "observed_at": (datetime.now(timezone.utc) - timedelta(seconds=31)).isoformat(),
                "profile": config.profile,
                "codex_home": config.expected_codex_home,
                "state_dir": str(state_dir.resolve()),
            }), encoding="utf-8")
            adapter = CodexAppServerTaskProvisioningAdapter(
                state_dir / "launcher.json", state_dir=state_dir, config_loader=lambda _: config,
                pid_alive=lambda _: True,
            )

            health = adapter.hold_host_health()

        self.assertEqual(health["status"], "host_not_ready")
        self.assertEqual(health["reason"], "HoldHost observed_at is stale")

    def test_hold_host_health_blocks_a_profile_mismatch(self):
        config = FakeConfig()
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            (state_dir / "hold-host.json").write_text(json.dumps({
                "status": "ready",
                "pid": 1780,
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "host_started_at": datetime.now(timezone.utc).isoformat(),
                "profile": "other",
                "codex_home": config.expected_codex_home,
                "state_dir": str(state_dir.resolve()),
            }), encoding="utf-8")
            adapter = CodexAppServerTaskProvisioningAdapter(
                state_dir / "launcher.json", state_dir=state_dir, config_loader=lambda _: config,
                pid_alive=lambda _: True, pid_started_at=lambda _: datetime.now(timezone.utc),
            )

            health = adapter.hold_host_health()

        self.assertEqual(health["status"], "host_not_ready")
        self.assertEqual(health["reason"], "HoldHost profile does not match")

    def test_hold_host_health_blocks_a_reused_pid_with_a_different_start_time(self):
        config = FakeConfig()
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            now = datetime.now(timezone.utc)
            (state_dir / "hold-host.json").write_text(json.dumps({
                "status": "ready", "pid": 1780, "worker_capacity": 1,
                "observed_at": now.isoformat(), "host_started_at": (now - timedelta(minutes=1)).isoformat(),
                "profile": config.profile, "codex_home": config.expected_codex_home,
                "state_dir": str(state_dir.resolve()),
            }), encoding="utf-8")
            adapter = CodexAppServerTaskProvisioningAdapter(
                state_dir / "launcher.json", state_dir=state_dir, config_loader=lambda _: config,
                pid_alive=lambda _: True, pid_started_at=lambda _: now,
            )

            health = adapter.hold_host_health()

        self.assertEqual(health, {
            "status": "host_not_ready", "reason": "HoldHost PID does not match health identity",
        })

    def test_hold_host_health_requires_the_persisted_pid_start_identity(self):
        config = FakeConfig()
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            (state_dir / "hold-host.json").write_text(json.dumps({
                "status": "ready", "pid": 1780, "worker_capacity": 1,
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "profile": config.profile, "codex_home": config.expected_codex_home,
                "state_dir": str(state_dir.resolve()),
            }), encoding="utf-8")
            adapter = CodexAppServerTaskProvisioningAdapter(
                state_dir / "launcher.json", state_dir=state_dir, config_loader=lambda _: config,
                pid_alive=lambda _: True,
            )

            health = adapter.hold_host_health()

        self.assertEqual(health, {
            "status": "host_not_ready", "reason": "HoldHost PID health identity is missing",
        })

    def test_create_queues_for_the_user_host_without_starting_an_app_server(self):
        config = FakeConfig()
        with tempfile.TemporaryDirectory() as temp:
            adapter = CodexAppServerTaskProvisioningAdapter(
                "unused.json", state_dir=Path(temp), config_loader=lambda _: config
            )
            receipt = adapter.provision(TaskProvisionRequest(
                request_id="create-1", project="Jarvis4codex", title="TEST Worker",
                prompt="hello", source_ref="mcp:test", max_turns=2,
            ))
            state = adapter.hold_status("hold-create-1")

        self.assertEqual(receipt.status, "accepted")
        self.assertEqual(receipt.phase, "queued_for_user_host")
        self.assertEqual(receipt.hold_id, "hold-create-1")
        self.assertEqual(receipt.monitor_id, "hold-create-1")
        self.assertEqual(state["status"], "accepted")
        self.assertEqual(state["phase"], "queued_for_user_host")
        self.assertEqual(config.project, "Jarvis4codex")

    def test_existing_active_hold_cannot_be_resumed_a_second_time(self):
        config = FakeConfig()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "task-holds" / "hold-create-1"
            root.mkdir(parents=True)
            (root / "ack.json").write_text(json.dumps({
                "status": "holding", "turn_count": 1, "total_turn_count": 4, "max_turns": 3,
            }), encoding="utf-8")
            adapter = CodexAppServerTaskProvisioningAdapter(
                "unused.json", state_dir=Path(temp), config_loader=lambda _: config
            )
            receipt = adapter.resume_with_monitor(TaskMonitorResumeRequest(
                request_id="resume-2", task_id="thread-created-1", prompt="继续",
                source_ref="mcp:test", hold_id="hold-create-1",
            ))

        self.assertEqual(receipt.status, "failed")
        self.assertIn("already owns", receipt.reason or "")

    def test_resume_archives_the_previous_terminal_receipt_before_queueing(self):
        config = FakeConfig()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "task-holds" / "hold-create-1"
            root.mkdir(parents=True)
            (root / "result.json").write_text(json.dumps({
                "request_id": "create-1", "status": "turn_limit_reached",
                "thread_id": "thread-created-1",
                "turn_count": 2, "total_turn_count": 2, "max_turns": 2,
            }), encoding="utf-8")
            adapter = CodexAppServerTaskProvisioningAdapter(
                "unused.json", state_dir=Path(temp), config_loader=lambda _: config
            )
            receipt = adapter.resume_with_monitor(TaskMonitorResumeRequest(
                request_id="resume-2", task_id="thread-created-1", prompt="继续",
                source_ref="mcp:test", hold_id="hold-create-1",
            ))
            queued = adapter.hold_status("hold-create-1")
            archived = root / "history" / "create-1.result.json"
            archived_exists = archived.is_file()

        self.assertEqual(receipt.status, "accepted")
        self.assertEqual(receipt.total_turn_count, 3)
        self.assertEqual(queued["request_id"], "resume-2")
        self.assertTrue(archived_exists)

    def test_status_reads_a_legacy_task_monitor_directory_without_migration(self):
        config = FakeConfig()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "task-monitors" / "monitor-legacy-1"
            root.mkdir(parents=True)
            (root / "result.json").write_text(json.dumps({
                "request_id": "legacy-1", "status": "completed",
            }), encoding="utf-8")
            adapter = CodexAppServerTaskProvisioningAdapter(
                "unused.json", state_dir=Path(temp), config_loader=lambda _: config
            )
            state = adapter.hold_status("monitor-legacy-1")

        self.assertEqual(state["request_id"], "legacy-1")

    def test_read_turn_history_filters_the_existing_sqlite_history(self):
        config = FakeConfig()
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            append_terminal_turn_history(
                state_dir / "turn-history.sqlite", task_id="loop-1", hold_id="loop-1:worker-1",
                request_id="loop-1:worker-1:acquire", thread_id="thread-1", turn_id="turn-1",
                turn_number=1, candidate_id=7, status="holding", final_answer="first",
                completed_at="2026-09-01T00:00:00+00:00",
            )
            append_terminal_turn_history(
                state_dir / "turn-history.sqlite", task_id="loop-1", hold_id="loop-1:worker-1",
                request_id="loop-1:worker-1:acquire", thread_id="thread-1", turn_id="turn-2",
                turn_number=2, candidate_id=9, status="turn_limit_reached", final_answer="second",
                completed_at="2026-09-01T00:01:00+00:00",
            )
            adapter = CodexAppServerTaskProvisioningAdapter(
                "unused.json", state_dir=state_dir, config_loader=lambda _: config
            )
            rows = adapter.read_turn_history(task_id="loop-1", turn_id="turn-2")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["candidate_id"], 9)
        self.assertEqual(rows[0]["final_answer"], "second")

    def test_pending_hold_notification_holds_returns_only_undelivered_events(self):
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            pending = state_dir / "task-holds" / "hold-pending"
            delivered = state_dir / "task-holds" / "hold-delivered"
            unverified = state_dir / "task-holds" / "hold-unverified"
            for root, hold_id in (
                (pending, "hold:pending"),
                (delivered, "hold:delivered"),
                (unverified, "hold:unverified"),
            ):
                root.mkdir(parents=True)
                (root / "request.json").write_text(json.dumps({"hold_id": hold_id}), encoding="utf-8")
                (root / "monitor-events.jsonl").write_text(json.dumps({"event_id": "terminal:turn-1"}) + "\n", encoding="utf-8")
            (delivered / "monitor-notification-deliveries.jsonl").write_text(
                json.dumps({"event_id": "terminal:turn-1", "delivery_status": "delivered", "message_id": "om-1"}) + "\n",
                encoding="utf-8",
            )
            adapter = CodexAppServerTaskProvisioningAdapter(
                "unused.json", state_dir=state_dir, config_loader=lambda _: FakeConfig()
            )

            hold_ids = adapter.pending_hold_notification_holds()

        self.assertEqual(hold_ids, ["hold:pending", "hold:unverified"])

    def test_pending_terminal_discovery_skips_milestones_and_records_legacy_delivery_in_place(self):
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            root = state_dir / "task-monitors" / "legacy-monitor"
            root.mkdir(parents=True)
            (root / "request.json").write_text(json.dumps({"hold_id": "legacy:monitor"}), encoding="utf-8")
            (root / "monitor-events.jsonl").write_text(
                "\n".join((
                    json.dumps({"event_id": "milestone:turn-1", "event_type": "milestone"}),
                    json.dumps({"event_id": "terminal:turn-1", "event_type": "terminal"}),
                )) + "\n",
                encoding="utf-8",
            )
            adapter = CodexAppServerTaskProvisioningAdapter(
                "unused.json", state_dir=state_dir, config_loader=lambda _: FakeConfig()
            )

            hold_ids = adapter.pending_hold_notification_holds(event_type="terminal")
            adapter.record_hold_notification_delivery(
                "legacy:monitor", "terminal:turn-1",
                {"delivery_status": "delivered", "message_id": "om-legacy"},
            )

            self.assertEqual(hold_ids, ["legacy:monitor"])
            self.assertTrue((root / "monitor-notification-deliveries.jsonl").is_file())
            self.assertFalse((state_dir / "task-holds" / "legacy_monitor").exists())


if __name__ == "__main__":
    unittest.main()
