from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest

from jarvis_heartbeat_service import (
    ACTIVE,
    DesktopRecovery,
    HeartbeatConfig,
    HeartbeatError,
    HeartbeatService,
    HeartbeatStore,
    NativeQuotaProbe,
    NativeThreadTerminalProbe,
    WakeController,
    WORKSPACE_ROOT,
)
from attendance_end_of_day_memo_probe import AttendanceEndOfDayMemoProbe


THREAD_ID = "11111111-1111-4111-8111-111111111111"
THREAD_ID_2 = "22222222-2222-4222-8222-222222222222"


class FakeClient:
    instances: list["FakeClient"] = []
    thread_status = "idle"

    def __init__(self, _config: object):
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.closed = False
        self.__class__.instances.append(self)

    def start(self) -> dict[str, object]:
        return {"codexHome": "C:\\Users\\22524\\.codex"}

    def request(self, method: str, params: dict[str, object]) -> dict[str, object]:
        self.calls.append((method, params))
        if method == "thread/resume":
            return {"thread": {"id": THREAD_ID}}
        if method == "thread/read":
            return {"thread": {"id": THREAD_ID, "status": self.thread_status}}
        if method == "turn/start":
            return {"turn": {"id": "turn-1", "status": "inProgress"}}
        raise AssertionError(method)

    def wait_for_turn_terminal(self, thread_id: str, turn_id: str) -> dict[str, object]:
        self.calls.append(
            (
                "wait_for_turn_terminal",
                {"threadId": thread_id, "turnId": turn_id},
            )
        )
        return {"id": turn_id, "status": "completed"}

    def run_existing_task(
        self,
        thread_id: str,
        request: dict[str, object],
        *,
        client_user_message_id: str,
        model: str | None = None,
        reasoning_effort: str | None = None,
    ) -> dict[str, object]:
        self.calls.append(
            (
                "run_existing_task",
                {
                    "thread_id": thread_id,
                    "request": request,
                    "client_user_message_id": client_user_message_id,
                    "model": model,
                    "reasoning_effort": reasoning_effort,
                },
            )
        )
        return {
            "thread_id": thread_id,
            "turn_id": "turn-inbox-parity-1",
            "turn_status": "completed",
            "final_message": "continued",
        }

    def close(self) -> None:
        self.closed = True


class FakeDesktop:
    def __init__(self, _config: object):
        pass

    def ensure_running(self) -> dict[str, object]:
        return {"desktop_status": "started", "desktop_exe": "C:\\app\\Codex.exe"}


class FakeWakeController:
    def __init__(self):
        self.calls: list[dict[str, object]] = []

    def wake(self, target_thread_id: str, prompt: str, **kwargs: object) -> dict[str, object]:
        self.calls.append(
            {"target_thread_id": target_thread_id, "prompt": prompt, **kwargs}
        )
        return {
            "outcome": "turn_completed",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "thread_status": "active",
            "turn_id": "turn-service-1",
            "desktop_status": "not_requested",
        }


class SequenceQuotaProbe:
    def __init__(self, readings: list[dict[str, object]]):
        self.readings = list(readings)
        self.calls = 0

    def read_weekly_remaining(self) -> dict[str, object]:
        self.calls += 1
        if not self.readings:
            raise AssertionError("unexpected quota probe read")
        return self.readings.pop(0)


class FakeEndOfDayMemoProbe:
    def __init__(self, result: dict[str, object]):
        self.result = result
        self.calls: list[dict[str, object]] = []

    def run(self, probe_config: dict[str, object]) -> dict[str, object]:
        self.calls.append(probe_config)
        return self.result


class FakeQuotaClient:
    response: dict[str, object] = {}

    def __init__(self, _config: object):
        self.closed = False

    def start(self) -> dict[str, object]:
        return {"codexHome": "C:\\Users\\22524\\.codex"}

    def request(self, method: str, params: dict[str, object]) -> dict[str, object]:
        if method != "account/rateLimits/read" or params != {}:
            raise AssertionError((method, params))
        return self.response

    def close(self) -> None:
        self.closed = True


class FakeThreadReadOnlyClient:
    instances: list["FakeThreadReadOnlyClient"] = []
    response: dict[str, object] = {}

    def __init__(self, _config: object):
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.closed = False
        self.__class__.instances.append(self)

    def start(self) -> dict[str, object]:
        return {"codexHome": "C:\\Users\\22524\\.codex"}

    def request(self, method: str, params: dict[str, object]) -> dict[str, object]:
        self.calls.append((method, params))
        if method != "thread/read":
            raise AssertionError(f"forbidden probe method: {method}")
        return self.response

    def close(self) -> None:
        self.closed = True


class HeartbeatTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        launcher = self.root / "launcher.json"
        launcher.write_text(
            json.dumps(
                {
                    "version": 1,
                    "dispatcher_thread_id": THREAD_ID,
                    "codex_cli": "auto",
                    "expected_codex_home": "",
                    "live_creation_enabled": False,
                    "poll_seconds": 1,
                    "request_timeout_seconds": 10,
                    "turn_completion_timeout_seconds": 10,
                    "max_attempts": 1,
                    "cooper_actor_ids": ["cooper"],
                    "allowed_projects": {"test": str(self.root)},
                }
            ),
            encoding="utf-8",
        )
        desktop = self.root / "app" / "ChatGPT.exe"
        desktop.parent.mkdir(parents=True)
        desktop.write_bytes(b"test")
        config_path = self.root / "config.json"
        config_path.write_text(
            json.dumps(
                {
                    "db_path": str(self.root / "heartbeats.sqlite"),
                    "health_path": str(self.root / "health.json"),
                    "lock_path": str(self.root / "service.lock"),
                    "native_task_launcher_config": str(launcher),
                    "poll_seconds": 1,
                    "retry_seconds": 5,
                    "max_failures": 2,
                    "min_interval_seconds": 10,
                    "max_interval_seconds": 3600,
                    "max_prompt_chars": 5000,
                    "desktop_recovery_enabled": True,
                    "desktop_exe_path": str(desktop),
                    "desktop_app_user_model_id": "OpenAI.Codex_test!App",
                    "desktop_start_timeout_seconds": 5,
                    "turn_completion_timeout_seconds": 60,
                    "max_concurrent_runs": 4,
                }
            ),
            encoding="utf-8",
        )
        self.config = HeartbeatConfig.load(config_path)
        self.store = HeartbeatStore(self.config)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def request(self, **overrides: object) -> dict[str, object]:
        # Legacy recurring-test shorthand: v2 schedules must always be bounded.
        if "max_runs" in overrides and overrides.get("max_runs") is None:
            overrides["max_runs"] = 48
        value: dict[str, object] = {
            "heartbeat_id": "test-heartbeat-v1",
            "name": "Test heartbeat",
            "target_thread_id": THREAD_ID,
            "parent_thread_id": THREAD_ID_2,
            "prompt": "Inspect current state and continue the exact approved scope.",
            "interval_seconds": 30,
            "start_immediately": True,
            "max_runs": 1,
            "expires_at": "2099-01-01T00:00:00+00:00",
            "source_event_key": "event-1",
            "confirmation_evidence": "Cooper confirmed the exact schedule.",
        }
        value.update(overrides)
        return value

    def queue_request(self, queue_path: Path, **overrides: object) -> dict[str, object]:
        value = self.request(
            heartbeat_id="queue-heartbeat-v1",
            name="Queue-aware continuation",
            prompt="Process exactly the released company and write the configured receipt fields.",
            execution_mode="native_probe",
            probe={
                "type": "master_queue_continuation",
                "master_queue_path": str(queue_path),
                "terminal_receipt": {
                    "terminal_statuses": ["completed"],
                    "required_item_fields": ["company_id", "status"],
                    "required_artifact_fields": ["result", "validation"],
                },
            },
            max_runs=10,
        )
        value.update(overrides)
        return value

    def write_queue(self, items: list[dict[str, object]]) -> Path:
        path = self.root / "master_queue.json"
        path.write_text(json.dumps({
            "schema_version": "contact-master-queue-v1",
            "lanes": [{
                "lane": "fixture_lane", "thread_id": THREAD_ID_2,
                "pending_new_master_items": items,
            }],
        }), encoding="utf-8")
        return path

    def queue_item(self, company_id: str = "UKBPLUS-901") -> dict[str, object]:
        return {
            "company_id": company_id, "company_name": "Fixture Groundcare Ltd",
            "official_website": "https://fixture.example", "rating": "B+",
            "source_csv": "C:/fixture.csv", "source_row": 1, "status": "pending",
        }

    def make_due(self, heartbeat_id: str) -> None:
        with self.store.session() as connection:
            connection.execute(
                "UPDATE heartbeats SET next_run_epoch=? WHERE heartbeat_id=?",
                (time.time() - 1, heartbeat_id),
            )

    def test_create_is_idempotent_for_identical_request(self) -> None:
        first = self.store.create(self.request())
        second = self.store.create(self.request())
        self.assertTrue(first["created"])
        self.assertFalse(second["created"])
        self.assertTrue(second["idempotent"])
        self.assertEqual(len(self.store.list()), 1)

    def test_conflicting_duplicate_id_is_rejected(self) -> None:
        self.store.create(self.request())
        with self.assertRaisesRegex(HeartbeatError, "different content"):
            self.store.create(self.request(prompt="Different prompt"))

    def test_authorized_update_reactivates_cancelled_schedule_and_preserves_history(self) -> None:
        created = self.store.create(self.request())["heartbeat"]
        self.store.set_status(
            "test-heartbeat-v1",
            "CANCELLED",
            "cancel-event",
            "Cooper confirmed cancellation.",
        )
        updated = self.store.update(self.request(
            prompt="Updated delegated controller prompt.",
            start_immediately=False,
            source_event_key="delegated-update-event",
            confirmation_evidence="Cooper explicitly authorized this exact update.",
            reactivate=True,
        ))

        self.assertEqual(updated["status"], ACTIVE)
        self.assertEqual(updated["prompt"], "Updated delegated controller prompt.")
        self.assertEqual(updated["target_thread_id"], THREAD_ID)
        self.assertEqual(updated["interval_seconds"], 30)
        self.assertEqual(updated["run_count"], 0)
        self.assertEqual(updated["created_at"], created["created_at"])
        self.assertNotEqual(updated["request_hash"], created["request_hash"])
        self.assertGreater(updated["next_run_epoch"], time.time())

    def test_create_requires_confirmation_evidence(self) -> None:
        with self.assertRaisesRegex(HeartbeatError, "confirmation_evidence"):
            self.store.create(self.request(confirmation_evidence=""))

    def test_v2_creation_requires_expiry_and_bounds_max_runs(self) -> None:
        with self.assertRaisesRegex(HeartbeatError, "expires_at is required"):
            self.store.create(self.request(expires_at=None))
        unbounded = self.request()
        unbounded["max_runs"] = None
        with self.assertRaisesRegex(HeartbeatError, "max_runs is required"):
            self.store.create(unbounded)
        with self.assertRaisesRegex(HeartbeatError, "explicit max_runs_override_evidence"):
            self.store.create(self.request(max_runs=49))
        created = self.store.create(self.request(
            heartbeat_id="override-49-v1",
            max_runs=49,
            max_runs_override_evidence="Cooper explicitly authorized 49 runs for this exact schedule.",
        ))
        self.assertEqual(created["heartbeat"]["max_runs"], 49)

    def test_required_receipt_pauses_on_decision_and_three_unchanged_rounds(self) -> None:
        guarded = replace(self.config, require_prompt_continuation_receipt=True, receipt_stale_round_limit=3)
        store = HeartbeatStore(guarded)
        heartbeat = store.create(self.request(max_runs=10))["heartbeat"]
        for _ in range(3):
            store.record_run(heartbeat, "event-1", THREAD_ID, {
                "outcome": "turn_completed", "thread_status": "idle", "desktop_status": "not_requested",
                "continuation_receipt": {"continuation_status": "CONTINUE_SAFE", "progress_fingerprint": "same"},
            })
            heartbeat = store.get("test-heartbeat-v1")
        self.assertEqual(heartbeat["status"], "PAUSED")

    def test_create_native_quota_probe_has_auditable_readback(self) -> None:
        created = self.store.create(self.request(
            heartbeat_id="quota-return-v1",
            prompt="QUOTA_RETURNED_90_NOTIFY.",
            execution_mode="native_probe",
            probe={
                "type": "codex_weekly_remaining_gte",
                "threshold_percent": 90.0,
                "double_read": True,
            },
        ))

        heartbeat = created["heartbeat"]
        self.assertEqual(heartbeat["execution_mode"], "native_probe")
        self.assertEqual(heartbeat["probe_type"], "codex_weekly_remaining_gte")
        self.assertEqual(heartbeat["probe_threshold_percent"], 90.0)
        self.assertEqual(heartbeat["trigger_state"], "monitoring")
        self.assertEqual(json.loads(heartbeat["probe_config_json"]), {
            "double_read": True,
            "threshold_percent": 90.0,
            "type": "codex_weekly_remaining_gte",
        })
        self.assertEqual(self.store.get("quota-return-v1")["request_hash"], heartbeat["request_hash"])

    def test_create_native_thread_terminal_probe_has_auditable_readback(self) -> None:
        created = self.store.create(self.request(
            heartbeat_id="thread-terminal-v1",
            prompt="Read exact thread terminal state without waking it.",
            execution_mode="native_probe",
            probe={"type": "codex_thread_terminal"},
        ))

        heartbeat = created["heartbeat"]
        self.assertEqual(heartbeat["target_thread_id"], THREAD_ID)
        self.assertEqual(heartbeat["probe_type"], "codex_thread_terminal")
        self.assertEqual(json.loads(heartbeat["probe_config_json"]), {
            "type": "codex_thread_terminal",
        })
        self.assertEqual(heartbeat["trigger_state"], "monitoring")

    def test_terminal_probe_defaults_parent_to_creation_source_thread(self) -> None:
        request = self.request(
            heartbeat_id="thread-terminal-source-default-v1",
            prompt="Read exact thread terminal state without waking it.",
            execution_mode="native_probe",
            probe={"type": "codex_thread_terminal"},
        )
        request.pop("parent_thread_id")
        request["source_thread_id"] = THREAD_ID_2

        created = self.store.create(request)

        self.assertEqual(created["heartbeat"]["parent_thread_id"], THREAD_ID_2)

    def test_terminal_continue_probe_accepts_custom_resume_target_and_prompt(self) -> None:
        created = self.store.create(self.request(
            heartbeat_id="thread-terminal-continue-custom-v1",
            prompt="default continuation",
            execution_mode="native_probe",
            probe={
                "type": "codex_thread_terminal_continue",
                "continuation_target_thread_id": THREAD_ID_2,
                "continuation_prompt": "继续",
            },
        ))

        self.assertEqual(json.loads(created["heartbeat"]["probe_config_json"]), {
            "type": "codex_thread_terminal_continue",
            "continuation_target_thread_id": THREAD_ID_2,
            "continuation_prompt": "继续",
        })

    def test_update_accepts_native_thread_terminal_probe_contract(self) -> None:
        self.store.create(self.request())

        updated = self.store.update(self.request(
            prompt="Read exact thread terminal state without waking it.",
            execution_mode="native_probe",
            probe={"type": "codex_thread_terminal"},
            source_event_key="thread-probe-update",
            confirmation_evidence="Cooper authorized the exact probe update.",
        ))

        self.assertEqual(updated["status"], ACTIVE)
        self.assertEqual(updated["target_thread_id"], THREAD_ID)
        self.assertEqual(updated["probe_type"], "codex_thread_terminal")
        self.assertEqual(json.loads(updated["probe_config_json"]), {
            "type": "codex_thread_terminal",
        })

    def test_create_attendance_end_of_day_memo_probe_has_auditable_readback(self) -> None:
        created = self.store.create(self.request(
            heartbeat_id="attendance-end-of-day-memo-v1",
            prompt="Attendance-aware end-of-day memo.",
            execution_mode="native_probe",
            probe={
                "type": "attendance_end_of_day_memo",
                "lead_minutes": 10,
                "grace_minutes": 5,
            },
            max_runs=None,
        ))

        heartbeat = created["heartbeat"]
        self.assertEqual(heartbeat["probe_type"], "attendance_end_of_day_memo")
        self.assertEqual(json.loads(heartbeat["probe_config_json"]), {
            "grace_minutes": 5,
            "lead_minutes": 10,
            "type": "attendance_end_of_day_memo",
        })
        self.assertEqual(heartbeat["trigger_state"], "monitoring")

    def test_native_quota_probe_uses_authoritative_weekly_primary_semantics(self) -> None:
        FakeQuotaClient.response = {
            "rateLimits": {
                "primary": {"usedPercent": 46, "resetsAt": 1787151326}
            }
        }
        probe = NativeQuotaProbe(self.config, client_factory=FakeQuotaClient)

        result = probe.read_weekly_remaining()

        self.assertEqual(result, {
            "used_percent": 46.0,
            "remaining_percent": 54.0,
            "resets_at": 1787151326,
        })

    def test_native_thread_terminal_probe_reads_only_latest_turn_without_wake(self) -> None:
        FakeThreadReadOnlyClient.instances.clear()
        FakeThreadReadOnlyClient.response = {
            "thread": {
                "id": THREAD_ID,
                "status": "idle",
                "turns": [
                    {"id": "turn-old", "status": "completed"},
                    {
                        "id": "turn-running",
                        "status": "inProgress",
                        "startedAt": "2026-08-14T01:00:00+00:00",
                        "items": [],
                    },
                ],
            }
        }
        probe = NativeThreadTerminalProbe(
            self.config,
            client_factory=FakeThreadReadOnlyClient,
        )

        result = probe.inspect(THREAD_ID)

        self.assertEqual(result["status"], "RUNNING")
        self.assertEqual(result["thread_id"], THREAD_ID)
        self.assertEqual(result["thread_status"], "idle")
        self.assertEqual(result["last_turn_id"], "turn-running")
        self.assertEqual(result["last_turn_status"], "inProgress")
        self.assertIsNone(result["error"])
        self.assertEqual(result["started_at"], "2026-08-14T01:00:00+00:00")
        self.assertIsNone(result["completed_at"])
        self.assertIn("duration_ms", result)
        self.assertFalse(result["has_final_answer"])
        self.assertIsNone(result["terminal_fingerprint"])
        self.assertIsNone(result["completion_observed_at"])
        client = FakeThreadReadOnlyClient.instances[-1]
        self.assertEqual(client.calls, [(
            "thread/read",
            {"threadId": THREAD_ID, "includeTurns": True},
        )])
        self.assertTrue(client.closed)

    def test_native_thread_terminal_probe_reports_completed_turn_as_unverified(self) -> None:
        FakeThreadReadOnlyClient.instances.clear()
        FakeThreadReadOnlyClient.response = {
            "thread": {
                "id": THREAD_ID,
                "status": "notLoaded",
                "turns": [{
                    "id": "turn-completed",
                    "status": "completed",
                    "startedAt": "2026-08-14T02:00:00.000+00:00",
                    "completedAt": "2026-08-14T02:00:01.500+00:00",
                    "items": [{
                        "type": "agentMessage",
                        "phase": "final_answer",
                        "text": "Technical turn finished.",
                    }],
                }],
            }
        }
        probe = NativeThreadTerminalProbe(
            self.config,
            client_factory=FakeThreadReadOnlyClient,
        )

        first = probe.inspect(THREAD_ID)
        second = probe.inspect(THREAD_ID)

        self.assertEqual(first["status"], "TURN_COMPLETED_UNVERIFIED")
        self.assertEqual(first["thread_status"], "notLoaded")
        self.assertEqual(first["last_turn_status"], "completed")
        self.assertEqual(first["duration_ms"], 1500)
        self.assertTrue(first["has_final_answer"])
        self.assertIsNotNone(first["completion_observed_at"])
        self.assertRegex(str(first["terminal_fingerprint"]), r"^[0-9a-f]{64}$")
        self.assertEqual(first["terminal_fingerprint"], second["terminal_fingerprint"])

    def test_native_thread_terminal_probe_duration_supports_epoch_seconds_and_milliseconds(self) -> None:
        probe = NativeThreadTerminalProbe(
            self.config,
            client_factory=FakeThreadReadOnlyClient,
        )
        cases = [
            (1786676043, 1786676520),
            (1786676043000, 1786676520000),
        ]
        for started_at, completed_at in cases:
            with self.subTest(started_at=started_at, completed_at=completed_at):
                FakeThreadReadOnlyClient.response = {
                    "thread": {
                        "id": THREAD_ID,
                        "status": "idle",
                        "turns": [{
                            "id": f"turn-{started_at}",
                            "status": "completed",
                            "startedAt": started_at,
                            "completedAt": completed_at,
                            "items": [{
                                "type": "agentMessage",
                                "phase": "final_answer",
                                "text": "Technical turn finished.",
                            }],
                        }],
                    }
                }

                result = probe.inspect(THREAD_ID)

                self.assertEqual(result["duration_ms"], 477000)
                self.assertEqual(result["started_at"], started_at)
                self.assertEqual(result["completed_at"], completed_at)

    def test_native_thread_terminal_probe_classifies_failed_attention_and_unknown(self) -> None:
        probe = NativeThreadTerminalProbe(
            self.config,
            client_factory=FakeThreadReadOnlyClient,
        )
        cases = [
            ("failed", {"message": "system error"}, "FAILED"),
            ("cancelled", None, "NEEDS_ATTENTION"),
        ]
        for turn_status, error, expected in cases:
            with self.subTest(turn_status=turn_status):
                FakeThreadReadOnlyClient.response = {
                    "thread": {
                        "id": THREAD_ID,
                        "status": "idle",
                        "turns": [{
                            "id": f"turn-{turn_status}",
                            "status": turn_status,
                            "error": error,
                            "completedAt": "2026-08-14T02:05:00+00:00",
                        }],
                    }
                }
                result = probe.inspect(THREAD_ID)
                self.assertEqual(result["status"], expected)
                self.assertRegex(str(result["terminal_fingerprint"]), r"^[0-9a-f]{64}$")
                self.assertIsNotNone(result["completion_observed_at"])

        FakeThreadReadOnlyClient.response = {
            "thread": {"id": THREAD_ID, "status": "idle", "turns": []}
        }
        unknown = probe.inspect(THREAD_ID)
        self.assertEqual(unknown["status"], "UNKNOWN")
        self.assertIsNone(unknown["last_turn_id"])
        self.assertIsNone(unknown["terminal_fingerprint"])

    def test_pause_resume_and_cancel_are_audited(self) -> None:
        self.store.create(self.request())
        paused = self.store.set_status(
            "test-heartbeat-v1", "PAUSED", "event-2", "Cooper confirmed pause."
        )
        self.assertEqual(paused["status"], "PAUSED")
        resumed = self.store.set_status(
            "test-heartbeat-v1", ACTIVE, "event-3", "Cooper confirmed resume."
        )
        self.assertEqual(resumed["status"], ACTIVE)
        cancelled = self.store.set_status(
            "test-heartbeat-v1", "CANCELLED", "event-4", "Cooper confirmed cancel."
        )
        self.assertEqual(cancelled["status"], "CANCELLED")
        with self.store.session() as connection:
            actions = [
                row[0]
                for row in connection.execute(
                    "SELECT action FROM heartbeat_audit ORDER BY audit_id"
                )
            ]
        self.assertEqual(
            actions,
            [
                "heartbeat_created",
                "heartbeat_paused",
                "heartbeat_active",
                "heartbeat_cancelled",
            ],
        )

    def test_wake_controller_starts_exact_thread_turn(self) -> None:
        FakeClient.instances.clear()
        FakeClient.thread_status = "idle"
        controller = WakeController(
            self.config,
            client_factory=FakeClient,
            desktop_factory=FakeDesktop,
        )
        result = controller.wake(
            THREAD_ID,
            "Exact requirement",
            source_event_key="event-wake",
            ensure_desktop=True,
        )
        self.assertEqual(result["outcome"], "turn_completed")
        self.assertEqual(result["turn_id"], "turn-1")
        self.assertEqual(result["desktop_status"], "started")
        calls = FakeClient.instances[-1].calls
        self.assertEqual(calls[0], ("thread/resume", {"threadId": THREAD_ID}))
        self.assertEqual(calls[1][0], "thread/read")
        self.assertEqual(calls[2][0], "turn/start")
        self.assertEqual(calls[2][1]["threadId"], THREAD_ID)
        self.assertEqual(
            calls[2][1]["input"],
            [{"type": "text", "text": "Exact requirement"}],
        )
        self.assertEqual(calls[3][0], "wait_for_turn_terminal")
        self.assertEqual(calls[4][0], "thread/read")
        self.assertTrue(FakeClient.instances[-1].closed)

    def test_wake_controller_does_not_overlap_active_thread(self) -> None:
        FakeClient.instances.clear()
        FakeClient.thread_status = "active"
        controller = WakeController(self.config, client_factory=FakeClient)
        result = controller.wake(
            THREAD_ID,
            "Do not overlap",
            source_event_key="event-active",
        )
        self.assertEqual(result["outcome"], "deferred_busy")
        self.assertNotIn("turn/start", [call[0] for call in FakeClient.instances[-1].calls])

    def test_wake_controller_existing_task_path_matches_inbox_transport(self) -> None:
        FakeClient.instances.clear()
        controller = WakeController(self.config, client_factory=FakeClient)
        result = controller.run_existing_task(
            THREAD_ID,
            "继续",
            source_event_key="heartbeat-terminal-package-001",
            model="gpt-5.6-terra",
            reasoning_effort="high",
            client_user_message_id="terminal-continue:monitor-1:exact-id",
        )
        self.assertEqual(result["outcome"], "turn_completed")
        self.assertEqual(result["turn_id"], "turn-inbox-parity-1")
        self.assertEqual(result["final_message"], "continued")
        calls = FakeClient.instances[-1].calls
        self.assertEqual(calls[0][0], "run_existing_task")
        self.assertEqual(calls[0][1]["thread_id"], THREAD_ID)
        self.assertEqual(calls[0][1]["request"]["prompt"], "继续")
        self.assertEqual(
            calls[0][1]["client_user_message_id"],
            "terminal-continue:monitor-1:exact-id",
        )
        self.assertTrue(FakeClient.instances[-1].closed)

    def test_desktop_detection_does_not_accept_resources_backend(self) -> None:
        expected = self.config.desktop_exe_path
        assert expected is not None
        backend = expected.parent / "resources" / "codex.exe"
        backend.parent.mkdir(exist_ok=True)
        backend.write_bytes(b"backend")
        recovery = DesktopRecovery(
            self.config,
            process_paths=lambda: [str(backend)],
            starter=lambda _path: None,
        )
        self.assertFalse(recovery.is_running(expected, [str(backend)]))
        self.assertEqual(recovery.resolve_executable([str(backend)]), expected)

    def test_service_run_once_records_turn_and_completes_max_runs(self) -> None:
        self.store.create(self.request())
        controller = FakeWakeController()
        service = HeartbeatService(
            self.config,
            store=self.store,
            controller=controller,  # type: ignore[arg-type]
        )
        result = service.run_once()
        service.shutdown(wait=True)
        self.assertEqual(result["due_count"], 1)
        heartbeat = self.store.get("test-heartbeat-v1")
        self.assertEqual(heartbeat["status"], "COMPLETED")
        self.assertEqual(heartbeat["run_count"], 1)
        self.assertEqual(heartbeat["last_turn_id"], "turn-service-1")
        self.assertTrue(self.config.health_path.is_file())

    def test_contract_ref_mode_sends_short_verified_envelope_to_thread(self) -> None:
        full_prompt = (
            "Inspect the local controller, verify all authoritative state, and report only "
            "the exact approved heartbeat result. " * 20
        )
        self.store.create(self.request(prompt=full_prompt))
        controller = FakeWakeController()
        compact_config = replace(
            self.config,
            wake_payload_mode="contract_ref",
            contracts_dir=self.root / "heartbeat_contracts",
            max_wake_envelope_chars=512,
        )
        service = HeartbeatService(
            compact_config,
            store=self.store,
            controller=controller,  # type: ignore[arg-type]
        )

        result = service.run_once()
        service.shutdown(wait=True)

        self.assertEqual(result["results"][0]["outcome"], "turn_completed")
        self.assertEqual(len(controller.calls), 1)
        envelope = str(controller.calls[0]["prompt"])
        self.assertLessEqual(len(envelope), 512)
        self.assertNotIn(full_prompt, envelope)
        lines = dict(
            line.split("=", 1)
            for line in envelope.splitlines()[1:]
        )
        self.assertEqual(envelope.splitlines()[0], "JARVIS_HEARTBEAT_CONTRACT_V1")
        self.assertEqual(lines["heartbeat_id"], "test-heartbeat-v1")
        contract_path = Path(lines["contract_path"])
        self.assertTrue(contract_path.is_file())
        self.assertEqual(
            lines["contract_sha256"],
            hashlib.sha256(contract_path.read_bytes()).hexdigest(),
        )
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
        self.assertEqual(contract["prompt"], full_prompt.strip())
        self.assertEqual(contract["target_thread_id"], THREAD_ID)
        self.assertEqual(contract["run_id"], result["results"][0]["run_id"])

    def test_below_threshold_native_probe_is_silent_zero_turn(self) -> None:
        self.store.create(self.request(
            heartbeat_id="quota-return-v1",
            prompt="QUOTA_RETURNED_90_NOTIFY.",
            execution_mode="native_probe",
            probe={
                "type": "codex_weekly_remaining_gte",
                "threshold_percent": 90.0,
                "double_read": True,
            },
            max_runs=10,
        ))
        controller = FakeWakeController()
        probe = SequenceQuotaProbe([{
            "used_percent": 46.0,
            "remaining_percent": 54.0,
            "resets_at": 1787151326,
        }])
        service = HeartbeatService(
            self.config,
            store=self.store,
            controller=controller,  # type: ignore[arg-type]
            quota_probe=probe,  # type: ignore[arg-type]
        )

        result = service.run_once()
        service.shutdown(wait=True)

        self.assertEqual(result["results"][0]["outcome"], "probe_below_threshold")
        self.assertEqual(probe.calls, 1)
        self.assertEqual(controller.calls, [])
        heartbeat = self.store.get("quota-return-v1")
        self.assertEqual(heartbeat["run_count"], 1)
        self.assertEqual(heartbeat["status"], ACTIVE)
        self.assertIsNone(heartbeat["latest_run"]["turn_id"])
        self.assertEqual(json.loads(heartbeat["latest_run"]["probe_result_json"]), {
            "reads": [{
                "remaining_percent": 54.0,
                "resets_at": 1787151326,
                "used_percent": 46.0,
            }],
            "threshold_percent": 90.0,
        })

    def test_due_attendance_memo_queues_notification_without_ai_turn(self) -> None:
        self.store.create(self.request(
            heartbeat_id="attendance-end-of-day-memo-v1",
            prompt="Attendance-aware end-of-day memo.",
            execution_mode="native_probe",
            probe={
                "type": "attendance_end_of_day_memo",
                "lead_minutes": 10,
                "grace_minutes": 5,
            },
            max_runs=None,
        ))
        controller = FakeWakeController()
        memo_probe = FakeEndOfDayMemoProbe({
            "status": "QUEUED",
            "event_key": "attendance-day-20260813-record-1",
            "outbox_id": "outbox-1",
        })
        service = HeartbeatService(
            self.config,
            store=self.store,
            controller=controller,  # type: ignore[arg-type]
            end_of_day_probe=memo_probe,  # type: ignore[arg-type]
        )

        result = service.run_once()
        service.shutdown(wait=True)

        self.assertEqual(result["results"][0]["outcome"], "probe_notification_queued")
        self.assertEqual(controller.calls, [])
        self.assertEqual(memo_probe.calls, [{
            "grace_minutes": 5,
            "lead_minutes": 10,
            "type": "attendance_end_of_day_memo",
        }])
        heartbeat = self.store.get("attendance-end-of-day-memo-v1")
        self.assertEqual(heartbeat["status"], ACTIVE)
        self.assertEqual(heartbeat["run_count"], 1)
        self.assertIsNotNone(heartbeat["next_run_epoch"])

    def test_workbench_progress_terminal_probe_completes_without_ai_turn(self) -> None:
        workbench_db = self.root / "workbench.db"
        sqlite3.connect(workbench_db).close()
        self.store.create(self.request(
            heartbeat_id="workbench-progress-v1",
            name="Workbench native progress",
            prompt="Native Workbench probe only; never start a Codex turn.",
            execution_mode="native_probe",
            probe={
                "type": "workbench_queue_progress",
                "workbench_db": str(workbench_db),
                "state_path": str(WORKSPACE_ROOT / "output" / "workbench-progress-test-state.json"),
                "health_url": "http://127.0.0.1:8000/health",
                "batch_label": "fixture batch",
                "expected_draft_ids": [1, 2],
                "overdue_grace_seconds": 300,
            },
            max_runs=10,
        ))

        class TerminalWorkbenchProbe:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def run(self, config: dict[str, object]) -> dict[str, object]:
                self.calls.append(config)
                return {
                    "status": "BATCH_COMPLETED",
                    "terminal": True,
                    "outbox_id": "outbox-workbench-test",
                    "snapshot": {"actual_sent": 2, "tracked_total": 2},
                }

        native_probe = TerminalWorkbenchProbe()
        controller = FakeWakeController()
        service = HeartbeatService(
            self.config,
            store=self.store,
            controller=controller,  # type: ignore[arg-type]
            workbench_progress_probe=native_probe,  # type: ignore[arg-type]
        )

        result = service.run_once()
        service.shutdown(wait=True)

        self.assertEqual(result["results"][0]["outcome"], "workbench_probe_completed")
        self.assertEqual(controller.calls, [])
        self.assertEqual(native_probe.calls[0]["heartbeat_id"], "workbench-progress-v1")
        self.assertEqual(self.store.get("workbench-progress-v1")["status"], "COMPLETED")

    def test_master_queue_zero_pending_is_silent(self) -> None:
        queue_path = self.write_queue([])
        self.store.create(self.queue_request(queue_path))
        controller = FakeWakeController()
        service = HeartbeatService(self.config, store=self.store, controller=controller)  # type: ignore[arg-type]
        result = service.run_once()
        service.shutdown(wait=True)
        self.assertEqual(result["results"][0]["outcome"], "queue_exhausted_silent")
        self.assertEqual(controller.calls, [])
        self.assertIsNone(self.store.get("queue-heartbeat-v1")["latest_run"]["turn_id"])

    def test_master_queue_claims_one_item_and_wakes_registered_same_thread(self) -> None:
        queue_path = self.write_queue([self.queue_item(), self.queue_item("UKBPLUS-902")])
        self.store.create(self.queue_request(queue_path))
        controller = FakeWakeController()
        service = HeartbeatService(self.config, store=self.store, controller=controller)  # type: ignore[arg-type]
        result = service.run_once()
        service.shutdown(wait=True)
        self.assertEqual(result["results"][0]["outcome"], "queue_continuation_released")
        self.assertEqual([call["target_thread_id"] for call in controller.calls], [THREAD_ID_2])
        payload = json.loads(str(controller.calls[0]["prompt"]).split("\n", 1)[1])
        self.assertEqual(payload["item"]["company_id"], "UKBPLUS-901")
        queue = json.loads(queue_path.read_text(encoding="utf-8"))
        items = queue["lanes"][0]["pending_new_master_items"]
        self.assertEqual(items[0]["status"], "claimed")
        self.assertEqual(items[0]["jarvis_claim"]["state"], "woken")
        self.assertEqual(items[1]["status"], "pending")

    def test_master_queue_claim_is_idempotent_after_wake(self) -> None:
        queue_path = self.write_queue([self.queue_item()])
        self.store.create(self.queue_request(queue_path))
        controller = FakeWakeController()
        service = HeartbeatService(self.config, store=self.store, controller=controller)  # type: ignore[arg-type]
        service.run_once()
        self.make_due("queue-heartbeat-v1")
        second = service.run_once()
        service.shutdown(wait=True)
        self.assertEqual(second["results"][0]["outcome"], "queue_no_change_silent")
        self.assertEqual(len(controller.calls), 1)

    def test_master_queue_terminal_receipt_advances_exactly_one_next_item(self) -> None:
        queue_path = self.write_queue([self.queue_item(), self.queue_item("UKBPLUS-902")])
        self.store.create(self.queue_request(queue_path))
        controller = FakeWakeController()
        service = HeartbeatService(self.config, store=self.store, controller=controller)  # type: ignore[arg-type]
        service.run_once()
        result_path = self.root / "result.json"
        validation_path = self.root / "validation.json"
        result_path.write_text("{}", encoding="utf-8")
        validation_path.write_text(json.dumps({
            "valid": True,
            "error_count": 0,
            "input": str(result_path),
            "computed_sha256": hashlib.sha256(result_path.read_bytes()).hexdigest(),
        }), encoding="utf-8")
        queue = json.loads(queue_path.read_text(encoding="utf-8"))
        first = queue["lanes"][0]["pending_new_master_items"][0]
        first.update({"status": "completed", "result": "result.json", "validation": "validation.json"})
        queue_path.write_text(json.dumps(queue), encoding="utf-8")
        self.make_due("queue-heartbeat-v1")
        second = service.run_once()
        service.shutdown(wait=True)
        self.assertEqual(second["results"][0]["outcome"], "queue_continuation_released")
        self.assertEqual([call["target_thread_id"] for call in controller.calls], [THREAD_ID_2, THREAD_ID_2])
        queue = json.loads(queue_path.read_text(encoding="utf-8"))
        items = queue["lanes"][0]["pending_new_master_items"]
        self.assertEqual(items[0]["status"], "completed")
        self.assertIn("terminal_fingerprint", items[0]["jarvis_claim"])
        self.assertEqual(items[1]["status"], "claimed")

    def test_master_queue_immediately_rolls_completed_turns_without_due_tick(self) -> None:
        queue_path = self.write_queue([self.queue_item(), self.queue_item("UKBPLUS-902")])
        queue = json.loads(queue_path.read_text(encoding="utf-8"))
        for item in queue["lanes"][0]["pending_new_master_items"]:
            item["result"] = f"results/{item['company_id']}.json"
            item["validation"] = f"validation/{item['company_id']}.json"
        queue_path.write_text(json.dumps(queue), encoding="utf-8")
        self.store.create(self.queue_request(queue_path, probe={
            "type": "master_queue_continuation",
            "master_queue_path": str(queue_path),
            "terminal_receipt": {
                "terminal_statuses": ["claimed"],
                "required_item_fields": ["company_id", "status"],
                "required_artifact_fields": ["result", "validation"],
            },
        }))

        class CompletingController(FakeWakeController):
            def wake(inner_self, target_thread_id: str, prompt: str, **kwargs: object) -> dict[str, object]:
                response = super().wake(target_thread_id, prompt, **kwargs)
                item = json.loads(prompt.split("\n", 1)[1])["item"]
                result_path = queue_path.parent / item["result"]
                validation_path = queue_path.parent / item["validation"]
                result_path.parent.mkdir(parents=True, exist_ok=True)
                validation_path.parent.mkdir(parents=True, exist_ok=True)
                result_path.write_text(json.dumps({"company_id": item["company_id"]}), encoding="utf-8")
                validation_path.write_text(json.dumps({
                    "valid": True,
                    "error_count": 0,
                    "input": str(result_path),
                    "computed_sha256": hashlib.sha256(result_path.read_bytes()).hexdigest(),
                }), encoding="utf-8")
                return response

        controller = CompletingController()
        service = HeartbeatService(self.config, store=self.store, controller=controller)  # type: ignore[arg-type]
        result = service.run_once()
        service.shutdown(wait=True)

        self.assertEqual(result["results"][0]["outcome"], "queue_continuation_released")
        self.assertEqual(len(controller.calls), 2)
        items = json.loads(queue_path.read_text(encoding="utf-8"))["lanes"][0]["pending_new_master_items"]
        self.assertEqual([item["status"] for item in items], ["completed", "completed"])

    def test_master_queue_abnormal_persistence_alerts_once(self) -> None:
        queue_path = self.write_queue([])
        self.store.create(self.queue_request(queue_path))
        controller = FakeWakeController()
        service = HeartbeatService(self.config, store=self.store, controller=controller)  # type: ignore[arg-type]
        first = service.run_once()
        self.assertEqual(first["results"][0]["outcome"], "queue_exhausted_silent")
        self.make_due("queue-heartbeat-v1")
        second = service.run_once()
        self.make_due("queue-heartbeat-v1")
        third = service.run_once()
        service.shutdown(wait=True)
        self.assertEqual(second["results"][0]["outcome"], "HEARTBEAT_ABNORMAL_PERSISTENCE")
        self.assertEqual(second["results"][0]["alert"]["code"], "HEARTBEAT_ABNORMAL_PERSISTENCE")
        self.assertEqual(third["results"][0]["outcome"], "queue_exhausted_silent")
        self.assertNotIn("alert", third["results"][0])
        self.assertEqual(controller.calls, [])

    def test_thread_terminal_probe_persists_latest_run_without_controller_wake(self) -> None:
        self.store.create(self.request(
            heartbeat_id="thread-terminal-v1",
            prompt="Read terminal state only.",
            execution_mode="native_probe",
            probe={"type": "codex_thread_terminal"},
            max_runs=2,
        ))

        class StubThreadProbe:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def inspect(self, thread_id: str) -> dict[str, object]:
                self.calls.append(thread_id)
                return {
                    "status": "TURN_COMPLETED_UNVERIFIED",
                    "thread_id": thread_id,
                    "thread_status": "idle",
                    "last_turn_id": "turn-final",
                    "last_turn_status": "completed",
                    "error": None,
                    "started_at": "2026-08-14T02:00:00+00:00",
                    "completed_at": "2026-08-14T02:00:02+00:00",
                    "duration_ms": 2000,
                    "has_final_answer": True,
                    "terminal_fingerprint": "a" * 64,
                    "completion_observed_at": "2026-08-14T02:00:03+00:00",
                }

        thread_probe = StubThreadProbe()
        controller = FakeWakeController()
        service = HeartbeatService(
            self.config,
            store=self.store,
            controller=controller,  # type: ignore[arg-type]
            thread_probe=thread_probe,  # type: ignore[arg-type]
        )

        result = service.run_once()
        service.shutdown(wait=True)

        self.assertEqual(result["results"][0]["outcome"], "parent_terminal_receipt_delivered")
        self.assertEqual(thread_probe.calls, [THREAD_ID])
        self.assertEqual(len(controller.calls), 1)
        self.assertEqual(controller.calls[0]["target_thread_id"], THREAD_ID_2)
        self.assertTrue(controller.calls[0]["prompt"].startswith("JARVIS_TERMINAL_RECEIPT_V1"))
        heartbeat = self.store.get("thread-terminal-v1")
        self.assertEqual(heartbeat["status"], ACTIVE)
        self.assertEqual(heartbeat["run_count"], 1)
        self.assertIsNone(heartbeat["latest_run"]["turn_id"])
        persisted = json.loads(heartbeat["latest_run"]["probe_result_json"])
        self.assertEqual(persisted["status"], "TURN_COMPLETED_UNVERIFIED")
        self.assertEqual(persisted["terminal_fingerprint"], "a" * 64)

    def test_prompt_monitor_is_rejected_without_contacting_child(self) -> None:
        self.store.create(self.request(
            heartbeat_id="unsafe-monitor-v1",
            name="Child monitor",
            prompt="Monitor only this child and report status.",
        ))
        controller = FakeWakeController()
        service = HeartbeatService(self.config, store=self.store, controller=controller)  # type: ignore[arg-type]

        result = service.run_once()
        service.shutdown(wait=True)

        self.assertEqual(result["results"][0]["outcome"], "rejected_unsafe_monitor_prompt")
        self.assertEqual(controller.calls, [])
        self.assertEqual(self.store.get("unsafe-monitor-v1")["latest_run"]["thread_status"], "untouched")

    def test_threshold_trigger_requires_two_reads_and_arms_exactly_once_handoff(self) -> None:
        self.store.create(self.request(
            heartbeat_id="quota-return-v1",
            prompt="Codex weekly remaining is {remaining_percent}%.",
            execution_mode="native_probe",
            probe={
                "type": "codex_weekly_remaining_gte",
                "threshold_percent": 90.0,
                "double_read": True,
            },
            max_runs=10,
        ))
        controller = FakeWakeController()
        probe = SequenceQuotaProbe([
            {"used_percent": 9.0, "remaining_percent": 91.0, "resets_at": 1},
            {"used_percent": 7.0, "remaining_percent": 93.0, "resets_at": 1},
        ])
        service = HeartbeatService(
            replace(self.config, quota_probe_double_read_delay_seconds=0),
            store=self.store,
            controller=controller,  # type: ignore[arg-type]
            quota_probe=probe,  # type: ignore[arg-type]
        )

        result = service.run_once()
        service.shutdown(wait=True)

        self.assertEqual(result["results"][0]["outcome"], "probe_trigger_handoff")
        self.assertEqual(probe.calls, 2)
        self.assertEqual(len(controller.calls), 1)
        self.assertEqual(controller.calls[0]["prompt"], "Codex weekly remaining is 93%.")
        heartbeat = self.store.get("quota-return-v1")
        self.assertEqual(heartbeat["status"], ACTIVE)
        self.assertEqual(heartbeat["trigger_state"], "handoff_pending_delivery")
        self.assertEqual(heartbeat["trigger_remaining_percent"], 93.0)
        self.assertEqual(heartbeat["trigger_run_id"], heartbeat["latest_run"]["run_id"])
        self.assertIsNone(heartbeat["next_run_epoch"])
        self.assertEqual(heartbeat["run_count"], 1)
        self.assertEqual(heartbeat["last_turn_id"], "turn-service-1")

        restarted = HeartbeatService(
            replace(self.config, quota_probe_double_read_delay_seconds=0),
            store=self.store,
            controller=controller,  # type: ignore[arg-type]
            quota_probe=SequenceQuotaProbe([]),  # type: ignore[arg-type]
        )
        second = restarted.run_once()
        restarted.shutdown(wait=True)
        self.assertEqual(second["due_count"], 0)
        self.assertEqual(len(controller.calls), 1)

    def test_first_threshold_read_must_be_confirmed_by_second_read(self) -> None:
        self.store.create(self.request(
            heartbeat_id="quota-return-v1",
            execution_mode="native_probe",
            probe={
                "type": "codex_weekly_remaining_gte",
                "threshold_percent": 90.0,
                "double_read": True,
            },
            max_runs=10,
        ))
        controller = FakeWakeController()
        probe = SequenceQuotaProbe([
            {"used_percent": 9.0, "remaining_percent": 91.0, "resets_at": 1},
            {"used_percent": 12.0, "remaining_percent": 88.0, "resets_at": 1},
        ])
        service = HeartbeatService(
            replace(self.config, quota_probe_double_read_delay_seconds=0),
            store=self.store,
            controller=controller,  # type: ignore[arg-type]
            quota_probe=probe,  # type: ignore[arg-type]
        )

        result = service.run_once()
        service.shutdown(wait=True)

        self.assertEqual(result["results"][0]["outcome"], "probe_below_threshold")
        self.assertEqual(probe.calls, 2)
        self.assertEqual(controller.calls, [])
        self.assertEqual(self.store.get("quota-return-v1")["trigger_state"], "monitoring")

    def test_quota_auth_unavailable_records_failure_and_bounded_retry_without_turn(self) -> None:
        self.store.create(self.request(
            heartbeat_id="quota-return-v1",
            execution_mode="native_probe",
            probe={
                "type": "codex_weekly_remaining_gte",
                "threshold_percent": 90.0,
                "double_read": True,
            },
            max_runs=10,
        ))

        class UnavailableProbe:
            def read_weekly_remaining(self) -> dict[str, object]:
                raise HeartbeatError("authentication required")

        controller = FakeWakeController()
        before = time.time()
        service = HeartbeatService(
            self.config,
            store=self.store,
            controller=controller,  # type: ignore[arg-type]
            quota_probe=UnavailableProbe(),  # type: ignore[arg-type]
        )
        result = service.run_once()
        service.shutdown(wait=True)

        self.assertEqual(result["results"][0]["outcome"], "failed")
        heartbeat = self.store.get("quota-return-v1")
        self.assertEqual(heartbeat["run_count"], 0)
        self.assertEqual(heartbeat["failure_count"], 1)
        self.assertGreaterEqual(float(heartbeat["next_run_epoch"]), before + 4)
        self.assertIsNone(heartbeat["latest_run"]["turn_id"])
        self.assertIn("authentication required", heartbeat["latest_run"]["error"])
        self.assertEqual(controller.calls, [])

    def test_trigger_handoff_completion_requires_delivered_message_id(self) -> None:
        self.store.create(self.request(
            heartbeat_id="quota-return-v1",
            execution_mode="native_probe",
            probe={
                "type": "codex_weekly_remaining_gte",
                "threshold_percent": 90.0,
                "double_read": True,
            },
            max_runs=10,
        ))
        self.assertTrue(self.store.arm_trigger_handoff(
            heartbeat_id="quota-return-v1",
            run_id="run-trigger-1",
            remaining_percent=93.0,
            source_event_key="event-trigger",
        ))
        self.store.finish_trigger_handoff(
            heartbeat_id="quota-return-v1",
            run_id="run-trigger-1",
            delivered_handoff_started=True,
            source_event_key="event-trigger",
        )
        with self.assertRaisesRegex(HeartbeatError, "message_id"):
            self.store.complete_delivery(
                "quota-return-v1", "", "event-delivered"
            )

        completed = self.store.complete_delivery(
            "quota-return-v1", "om_delivered_123", "event-delivered"
        )

        self.assertEqual(completed["status"], "CANCELLED")
        self.assertEqual(completed["trigger_state"], "delivered_cancelled")
        self.assertEqual(completed["delivery_message_id"], "om_delivered_123")

    def test_restart_recovers_pre_turn_trigger_handoff_to_bounded_retry(self) -> None:
        self.store.create(self.request(
            heartbeat_id="quota-return-v1",
            execution_mode="native_probe",
            probe={
                "type": "codex_weekly_remaining_gte",
                "threshold_percent": 90.0,
                "double_read": True,
            },
            max_runs=10,
        ))
        claim = self.store.claim_due()[0]
        self.assertTrue(self.store.arm_trigger_handoff(
            heartbeat_id="quota-return-v1",
            run_id=str(claim["claimed_run_id"]),
            remaining_percent=92.0,
            source_event_key="event-trigger",
        ))

        controller = FakeWakeController()
        service = HeartbeatService(
            self.config,
            store=self.store,
            controller=controller,  # type: ignore[arg-type]
            quota_probe=SequenceQuotaProbe([]),  # type: ignore[arg-type]
        )
        service.shutdown(wait=True)

        heartbeat = self.store.get("quota-return-v1")
        self.assertEqual(heartbeat["trigger_state"], "monitoring")
        self.assertIsNone(heartbeat["trigger_run_id"])
        self.assertEqual(heartbeat["latest_run"]["outcome"], "failed")
        self.assertIn("service restarted before trigger turn", heartbeat["latest_run"]["error"])
        self.assertGreater(float(heartbeat["next_run_epoch"]), time.time())
        self.assertEqual(controller.calls, [])

    def test_inflight_receipt_is_persisted_before_waiting_for_terminal_turn(self) -> None:
        self.store.create(self.request())
        observed: list[dict[str, object]] = []
        store = self.store

        class InspectingController:
            def wake(self, target_thread_id: str, prompt: str, **kwargs: object) -> dict[str, object]:
                with store.session() as connection:
                    run = connection.execute(
                        "SELECT * FROM heartbeat_runs WHERE heartbeat_id=?",
                        ("test-heartbeat-v1",),
                    ).fetchone()
                    observed.append(dict(run) if run is not None else {})
                on_turn_started = kwargs["on_turn_started"]
                assert callable(on_turn_started)
                on_turn_started(
                    turn_id="turn-inflight-1",
                    thread_status="inProgress",
                    desktop_status="not_requested",
                )
                heartbeat = store.get("test-heartbeat-v1")
                observed.append({
                    "last_turn_id": heartbeat["last_turn_id"],
                    "last_thread_status": heartbeat["last_thread_status"],
                })
                return {
                    "outcome": "turn_completed",
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "thread_status": "idle",
                    "turn_id": "turn-inflight-1",
                    "desktop_status": "not_requested",
                }

        service = HeartbeatService(
            self.config,
            store=self.store,
            controller=InspectingController(),  # type: ignore[arg-type]
        )
        result = service.run_once()
        service.shutdown(wait=True)

        self.assertEqual(observed[0]["outcome"], "inflight")
        self.assertIsNone(observed[0]["completed_at"])
        self.assertEqual(observed[1], {
            "last_turn_id": "turn-inflight-1",
            "last_thread_status": "inProgress",
        })
        with self.store.session() as connection:
            runs = connection.execute(
                "SELECT * FROM heartbeat_runs WHERE heartbeat_id=?",
                ("test-heartbeat-v1",),
            ).fetchall()
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["outcome"], "turn_completed")
        self.assertEqual(result["results"][0]["run_id"], runs[0]["run_id"])

    def test_all_due_heartbeats_are_claimed_before_first_long_wake(self) -> None:
        self.store.create(self.request(max_runs=None))
        self.store.create(self.request(
            heartbeat_id="test-heartbeat-v2",
            target_thread_id=THREAD_ID_2,
            source_event_key="event-2",
            max_runs=None,
        ))
        observed_counts: list[int] = []
        store = self.store
        both_started = threading.Barrier(2)

        class InspectingController:
            def wake(self, target_thread_id: str, prompt: str, **kwargs: object) -> dict[str, object]:
                both_started.wait(timeout=1)
                with store.session() as connection:
                    observed_counts.append(int(connection.execute(
                        "SELECT COUNT(*) FROM heartbeat_runs WHERE outcome='inflight'",
                    ).fetchone()[0]))
                return {
                    "outcome": "already_active",
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "thread_status": "active",
                    "desktop_status": "not_requested",
                }

        service = HeartbeatService(
            self.config,
            store=self.store,
            controller=InspectingController(),  # type: ignore[arg-type]
        )
        result = service.run_once()
        service.shutdown(wait=True)
        self.assertEqual(result["due_count"], 2)
        self.assertEqual(observed_counts[0], 2)

    def test_claim_due_atomically_advances_schedule_and_persists_receipt(self) -> None:
        self.store.create(self.request(max_runs=None))
        claims = self.store.claim_due()
        self.assertEqual(len(claims), 1)
        claim = claims[0]
        self.assertTrue(claim["claimed_run_id"])
        self.assertGreater(claim["next_run_epoch"], claim["scheduled_epoch"])
        with self.store.session() as connection:
            run = connection.execute(
                "SELECT * FROM heartbeat_runs WHERE run_id=?",
                (claim["claimed_run_id"],),
            ).fetchone()
            heartbeat = connection.execute(
                "SELECT * FROM heartbeats WHERE heartbeat_id=?",
                ("test-heartbeat-v1",),
            ).fetchone()
        self.assertIsNotNone(run)
        self.assertEqual(run["outcome"], "inflight")
        self.assertEqual(run["target_thread_id"], THREAD_ID)
        self.assertEqual(heartbeat["next_run_epoch"], claim["next_run_epoch"])

    def test_long_heartbeat_does_not_block_independent_fast_heartbeat(self) -> None:
        self.store.create(self.request(max_runs=None))
        self.store.create(self.request(
            heartbeat_id="test-heartbeat-v2",
            target_thread_id=THREAD_ID_2,
            source_event_key="event-2",
            max_runs=None,
        ))
        long_started = threading.Event()
        release_long = threading.Event()
        fast_completed = threading.Event()

        class LongAndFastController:
            def wake(self, target_thread_id: str, prompt: str, **kwargs: object) -> dict[str, object]:
                if target_thread_id == THREAD_ID:
                    long_started.set()
                    release_long.wait(timeout=2)
                else:
                    fast_completed.set()
                return {
                    "outcome": "turn_completed",
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "thread_status": "idle",
                    "turn_id": f"turn-{target_thread_id[:4]}",
                    "desktop_status": "not_requested",
                }

        service = HeartbeatService(
            self.config,
            store=self.store,
            controller=LongAndFastController(),  # type: ignore[arg-type]
        )
        runner = threading.Thread(target=service.run_once, daemon=True)
        runner.start()
        self.assertTrue(long_started.wait(timeout=1))
        try:
            self.assertTrue(
                fast_completed.wait(timeout=0.25),
                "independent fast heartbeat was blocked behind the long wake",
            )
        finally:
            release_long.set()
            runner.join(timeout=2)
            service.shutdown(wait=True)
        self.assertFalse(runner.is_alive())

    def test_already_active_is_busy_deferral_not_success(self) -> None:
        heartbeat = self.store.create(self.request(max_runs=None))["heartbeat"]
        before = time.time()
        self.store.record_run(
            heartbeat,
            "event-1",
            THREAD_ID,
            {
                "outcome": "already_active",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "thread_status": "active",
                "desktop_status": "not_requested",
            },
        )
        after = self.store.get("test-heartbeat-v1")
        self.assertEqual(after["run_count"], 0)
        self.assertEqual(after["failure_count"], 0)
        self.assertGreaterEqual(float(after["next_run_epoch"]), before + 4)

    def test_async_tick_returns_and_health_exposes_independent_inflight_runs(self) -> None:
        self.store.create(self.request(max_runs=None))
        long_started = threading.Event()
        release_long = threading.Event()
        fast_completed = threading.Event()

        class LongThenFastController:
            def wake(self, target_thread_id: str, prompt: str, **kwargs: object) -> dict[str, object]:
                if target_thread_id == THREAD_ID:
                    long_started.set()
                    release_long.wait(timeout=2)
                else:
                    fast_completed.set()
                return {
                    "outcome": "turn_completed",
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "thread_status": "idle",
                    "turn_id": f"turn-{target_thread_id[:4]}",
                    "desktop_status": "not_requested",
                }

        service = HeartbeatService(
            self.config,
            store=self.store,
            controller=LongThenFastController(),  # type: ignore[arg-type]
        )
        started = time.monotonic()
        first_tick = service.run_once(wait_for_completion=False)
        self.assertLess(time.monotonic() - started, 0.25)
        self.assertEqual(first_tick["due_count"], 1)
        self.assertTrue(long_started.wait(timeout=1))
        health = json.loads(self.config.health_path.read_text(encoding="utf-8"))
        self.assertEqual(health["active_inflight_count"], 1)
        self.assertEqual(health["inflight_runs"][0]["heartbeat_id"], "test-heartbeat-v1")
        self.assertEqual(health["inflight_runs"][0]["target_thread_id"], THREAD_ID)

        self.store.create(self.request(
            heartbeat_id="test-heartbeat-v2",
            target_thread_id=THREAD_ID_2,
            source_event_key="event-2",
            max_runs=None,
        ))
        service.run_once(wait_for_completion=False)
        try:
            self.assertTrue(fast_completed.wait(timeout=1))
        finally:
            release_long.set()
            service.shutdown(wait=True)

    def test_same_target_due_heartbeats_do_not_overlap(self) -> None:
        self.store.create(self.request(max_runs=None))
        self.store.create(self.request(
            heartbeat_id="test-heartbeat-v2",
            source_event_key="event-2",
            max_runs=None,
        ))
        first_started = threading.Event()
        release_first = threading.Event()

        class BlockingController:
            def wake(self, target_thread_id: str, prompt: str, **kwargs: object) -> dict[str, object]:
                first_started.set()
                release_first.wait(timeout=2)
                return {
                    "outcome": "turn_completed",
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "thread_status": "idle",
                    "turn_id": "turn-only-one",
                    "desktop_status": "not_requested",
                }

        service = HeartbeatService(
            self.config,
            store=self.store,
            controller=BlockingController(),  # type: ignore[arg-type]
        )
        tick = service.run_once(wait_for_completion=False)
        self.assertEqual(tick["due_count"], 2)
        self.assertTrue(first_started.wait(timeout=1))
        try:
            with self.store.session() as connection:
                outcomes = [row[0] for row in connection.execute(
                    "SELECT outcome FROM heartbeat_runs ORDER BY started_at,run_id"
                )]
            self.assertEqual(outcomes.count("inflight"), 1)
            self.assertEqual(outcomes.count("deferred_busy"), 1)
            self.assertEqual(self.store.get("test-heartbeat-v2")["run_count"], 0)
            self.assertEqual(self.store.get("test-heartbeat-v2")["failure_count"], 0)
        finally:
            release_first.set()
            service.shutdown(wait=True)

    def test_view_readback_isolated_per_heartbeat_inflight(self) -> None:
        first = self.store.create(self.request(max_runs=None))["heartbeat"]
        self.store.create(self.request(
            heartbeat_id="test-heartbeat-v2",
            target_thread_id=THREAD_ID_2,
            source_event_key="event-2",
            max_runs=None,
        ))
        claim = self.store.begin_run(
            first,
            "event-1",
            THREAD_ID,
            scheduled_at=datetime.now(timezone.utc).isoformat(),
        )
        self.store.record_inflight_turn(
            heartbeat_id="test-heartbeat-v1",
            run_id=str(claim["run_id"]),
            turn_id="turn-isolated",
            thread_status="inProgress",
            desktop_status="not_requested",
            source_event_key="event-1",
        )
        first_view = self.store.get("test-heartbeat-v1")
        second_view = self.store.get("test-heartbeat-v2")
        self.assertEqual(first_view["inflight_run"]["turn_id"], "turn-isolated")
        self.assertEqual(first_view["inflight_run"]["target_thread_id"], THREAD_ID)
        self.assertIsNone(second_view["inflight_run"])
        self.assertNotEqual(first_view["next_run_epoch"], None)
        self.assertNotEqual(second_view["next_run_epoch"], None)

    def test_configured_capacity_defers_without_failure(self) -> None:
        self.store.create(self.request(max_runs=None))
        self.store.create(self.request(
            heartbeat_id="test-heartbeat-v2",
            target_thread_id=THREAD_ID_2,
            source_event_key="event-2",
            max_runs=None,
        ))
        started = threading.Event()
        release = threading.Event()

        class BlockingController:
            def wake(self, target_thread_id: str, prompt: str, **kwargs: object) -> dict[str, object]:
                started.set()
                release.wait(timeout=2)
                return {
                    "outcome": "turn_completed",
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "thread_status": "idle",
                    "turn_id": "turn-capacity",
                    "desktop_status": "not_requested",
                }

        service = HeartbeatService(
            replace(self.config, max_concurrent_runs=1),
            store=self.store,
            controller=BlockingController(),  # type: ignore[arg-type]
        )
        tick = service.run_once(wait_for_completion=False)
        self.assertTrue(started.wait(timeout=1))
        try:
            self.assertEqual(tick["due_count"], 2)
            second = self.store.get("test-heartbeat-v2")
            self.assertEqual(second["latest_run"]["outcome"], "deferred_busy")
            self.assertEqual(second["latest_run"]["error"], "scheduler_capacity_busy")
            self.assertEqual(second["run_count"], 0)
            self.assertEqual(second["failure_count"], 0)
        finally:
            release.set()
            service.shutdown(wait=True)

    def test_one_timeout_does_not_block_or_corrupt_other_heartbeat(self) -> None:
        self.store.create(self.request(max_runs=None))
        self.store.create(self.request(
            heartbeat_id="test-heartbeat-v2",
            target_thread_id=THREAD_ID_2,
            source_event_key="event-2",
            max_runs=None,
        ))

        class TimeoutAndSuccessController:
            def wake(self, target_thread_id: str, prompt: str, **kwargs: object) -> dict[str, object]:
                if target_thread_id == THREAD_ID:
                    raise TimeoutError("simulated terminal readback timeout")
                return {
                    "outcome": "turn_completed",
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "thread_status": "idle",
                    "turn_id": "turn-fast-success",
                    "desktop_status": "not_requested",
                }

        service = HeartbeatService(
            self.config,
            store=self.store,
            controller=TimeoutAndSuccessController(),  # type: ignore[arg-type]
        )
        service.run_once()
        service.shutdown(wait=True)
        first = self.store.get("test-heartbeat-v1")
        second = self.store.get("test-heartbeat-v2")
        self.assertEqual(first["run_count"], 0)
        self.assertEqual(first["failure_count"], 1)
        self.assertIn("simulated terminal readback timeout", first["last_error"])
        self.assertEqual(second["run_count"], 1)
        self.assertEqual(second["failure_count"], 0)

    def test_cancel_race_during_async_wake_keeps_terminal_status(self) -> None:
        self.store.create(self.request(max_runs=None))
        started = threading.Event()
        release = threading.Event()

        class BlockingController:
            def wake(self, target_thread_id: str, prompt: str, **kwargs: object) -> dict[str, object]:
                started.set()
                release.wait(timeout=2)
                return {
                    "outcome": "turn_completed",
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "thread_status": "idle",
                    "turn_id": "turn-after-cancel",
                    "desktop_status": "not_requested",
                }

        service = HeartbeatService(
            self.config,
            store=self.store,
            controller=BlockingController(),  # type: ignore[arg-type]
        )
        service.run_once(wait_for_completion=False)
        self.assertTrue(started.wait(timeout=1))
        self.store.set_status(
            "test-heartbeat-v1", "CANCELLED", "cancel-event", "Cooper cancelled it."
        )
        release.set()
        service.shutdown(wait=True)
        after = self.store.get("test-heartbeat-v1")
        self.assertEqual(after["status"], "CANCELLED")
        self.assertEqual(after["run_count"], 0)

    def test_recurring_success_keeps_next_run_in_the_future(self) -> None:
        self.store.create(self.request(max_runs=None))
        controller = FakeWakeController()
        before = datetime.now(timezone.utc).timestamp()
        service = HeartbeatService(
            self.config,
            store=self.store,
            controller=controller,  # type: ignore[arg-type]
        )
        service.run_once()
        service.shutdown(wait=True)
        heartbeat = self.store.get("test-heartbeat-v1")
        self.assertEqual(heartbeat["status"], ACTIVE)
        self.assertGreater(float(heartbeat["next_run_epoch"]), before + 20)
        self.assertEqual(self.store.due(now_epoch=before + 1), [])

    def test_failed_run_uses_retry_and_terminal_failure_threshold(self) -> None:
        heartbeat = self.store.create(self.request(max_runs=None))["heartbeat"]
        failure = {
            "outcome": "failed",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "thread_status": "unknown",
            "desktop_status": "not_requested",
            "error": "app server unavailable",
        }
        self.store.record_run(
            heartbeat, "event-1", THREAD_ID, failure
        )
        after_one = self.store.get("test-heartbeat-v1")
        self.assertEqual(after_one["status"], ACTIVE)
        self.assertEqual(after_one["failure_count"], 1)
        self.store.record_run(
            after_one, "event-1", THREAD_ID, failure
        )
        after_two = self.store.get("test-heartbeat-v1")
        self.assertEqual(after_two["status"], "FAILED")
        self.assertEqual(after_two["failure_count"], 2)

    def test_cancelled_heartbeat_cannot_be_reactivated_by_inflight_run(self) -> None:
        heartbeat = self.store.create(self.request(max_runs=None))["heartbeat"]
        self.store.set_status(
            "test-heartbeat-v1", "CANCELLED", "cancel-event", "Cooper cancelled it."
        )
        result = {
            "outcome": "turn_completed",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "thread_status": "idle",
            "desktop_status": "not_requested",
            "turn_id": "inflight-turn",
        }
        self.store.record_run(heartbeat, "run-event", THREAD_ID, result)
        after = self.store.get("test-heartbeat-v1")
        self.assertEqual(after["status"], "CANCELLED")
        self.assertEqual(after["run_count"], 0)

    def test_integrity_check_is_clean(self) -> None:
        self.store.create(self.request())
        result = self.store.integrity()
        self.assertEqual(result["integrity_check"], "ok")
        self.assertEqual(result["foreign_key_violations"], 0)


class AttendanceEndOfDayMemoProbeTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / "coo_state" / "attendance_events").mkdir(parents=True)
        (self.root / "coo_state" / "dispatcher").mkdir(parents=True)
        (self.root / "coo_state" / "dispatcher" / "binding.json").write_text(
            json.dumps({
                "dispatcher_thread_id": THREAD_ID,
                "primary_chat_id": "chat-1",
                "live_send_enabled": True,
            }),
            encoding="utf-8",
        )
        self.event_path = self.root / "coo_state" / "attendance_events" / "20260813.json"
        self.event_path.write_text(json.dumps({
            "schema_version": "attendance-day-event-v1",
            "event_date": "20260813",
            "source_read_status": "SUCCESS",
            "work_status": "WORK_STARTED",
            "check_in_record_id": "record-1",
            "expected_off_at": "2026-08-13T19:19:07+08:00",
            "event_key": "attendance-day-20260813-record-1",
            "consumers": {},
        }), encoding="utf-8")
        self.requests: list[dict[str, object]] = []

    def tearDown(self) -> None:
        self.temp.cleanup()

    def enqueue(self, request: dict[str, object]) -> dict[str, object]:
        self.requests.append(request)
        return {
            "queued": True,
            "duplicate": False,
            "record": {"outbox_id": "outbox-1"},
        }

    def test_due_time_queues_one_checklist_and_second_poll_is_idempotent(self) -> None:
        probe = AttendanceEndOfDayMemoProbe(
            self.root,
            now_provider=lambda: datetime.fromisoformat("2026-08-13T19:09:07+08:00"),
            enqueue=self.enqueue,
        )

        first = probe.run({"type": "attendance_end_of_day_memo", "lead_minutes": 10, "grace_minutes": 5})
        second = probe.run({"type": "attendance_end_of_day_memo", "lead_minutes": 10, "grace_minutes": 5})

        self.assertEqual(first["status"], "QUEUED")
        self.assertEqual(second["status"], "ALREADY_QUEUED")
        self.assertEqual(len(self.requests), 1)
        content = str(self.requests[0]["content"])
        self.assertIn("预计 19:19 下班", content)
        self.assertIn("发信器", content)
        self.assertIn("Codex 任务", content)
        self.assertIn("日报：当前记录未提交", content)
        event = json.loads(self.event_path.read_text(encoding="utf-8"))
        self.assertEqual(event["consumers"]["end_of_day_memo"]["status"], "QUEUED")


if __name__ == "__main__":
    unittest.main()
