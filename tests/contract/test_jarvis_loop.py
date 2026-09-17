from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from adapters.codex_app_server.task_provisioning_adapter import CodexAppServerTaskProvisioningAdapter
from jarvis_control import JarvisControl, LoopController, LoopStore


class FakeRuntime:
    def __init__(self) -> None:
        self.hold_calls: list[dict] = []
        self.heartbeat_calls: list[dict] = []
        self.states = {"hold-loop-contract-1:worker-1": {"status": "holding", "thread_id": "thread-1"}}

    def hold(self, **kwargs):
        self.hold_calls.append(kwargs)
        hold_id = kwargs["hold_id"] or f"hold-{kwargs['request_id']}"
        self.states.setdefault(hold_id, {"status": "holding", "thread_id": kwargs.get("task_id") or "thread-1"})
        return {"status": "holding", "target_thread_id": self.states[hold_id]["thread_id"], "data": {"monitor_id": hold_id}}

    def monitor(self, **kwargs):
        return {"status": "completed", "data": dict(self.states[kwargs["hold_id"]])}

    def heartbeat(self, **kwargs):
        self.heartbeat_calls.append(kwargs)
        return {"status": "active" if kwargs["action"] == "create" else "completed"}


class JarvisLoopContractTest(unittest.TestCase):
    def test_thread_quota_rotates_each_seat_and_preserves_round_budget_after_reload(self):
        for quota in (1, 2):
            with self.subTest(quota=quota):
                runtime = FakeRuntime()
                started = self.controller.start(runtime, request_id=f"rotation-{quota}", project="test", title="test",
                    prompt="FIRST", continue_prompt="NEXT", turns_per_thread=quota,
                    max_rounds=3, target_thread_count=2, expires_at="2099-01-01T00:00:00+00:00")
                loop_id = started.loop_id
                for offset in range(0, 3, quota):
                    state = self.controller._store.load(loop_id)
                    for child in state["children"]:
                        runtime.states[child["hold_id"]] = {"status": "turn_limit_reached",
                            "session_turn_count": min(quota, 3 - offset), "total_turn_count": 99,
                            "hold_released": True, "terminal_confirmed": True}
                    # A new controller exercises persisted offset rather than in-memory progress.
                    result = LoopController(self.controller._store).tick(runtime, loop_id=loop_id)
                self.assertEqual(result.status, "completed")
                self.assertEqual([child["round"] for child in result.data["children"]], [3, 3])
                self.assertEqual([call["max_turns"] for call in runtime.hold_calls],
                    [min(quota, 3 - offset) for offset in range(0, 3, quota) for _ in range(2)])
                self.assertEqual(len({call["hold_id"] for call in runtime.hold_calls}), len(runtime.hold_calls))
                for call in runtime.hold_calls:
                    self.assertTrue(call["prompt"].endswith("FIRST"))
                    self.assertTrue(call["continue_prompt"].endswith("NEXT"))
                    self.assertIsNone(call["task_id"])
                before = len(runtime.hold_calls)
                self.controller.tick(runtime, loop_id=loop_id)
                self.assertEqual(len(runtime.hold_calls), before)

    def test_rotation_requires_released_success_and_respects_manual_stop_and_expiry(self):
        cases = ["failed", "cancelled", "unknown", "unreleased", "unconfirmed", "short", "manual", "stop", "expiry"]
        for case in cases:
            with self.subTest(case=case):
                runtime = FakeRuntime()
                started = self.controller.start(runtime, request_id=f"no-rotate-{case}", project="test", title="test",
                    prompt="FIRST", turns_per_thread=2, max_rounds=3, auto_continue=case != "manual",
                    target_thread_count=1, expires_at="2099-01-01T00:00:00+00:00")
                hold_id = started.data["children"][0]["hold_id"]
                runtime.states[hold_id] = {"status": case if case in {"failed", "cancelled", "unknown"} else "turn_limit_reached",
                    "session_turn_count": 1 if case == "short" else 2,
                    "hold_released": case != "unreleased", "terminal_confirmed": case != "unconfirmed"}
                if case == "stop":
                    self.controller.stop(runtime, loop_id=started.loop_id)
                if case == "expiry":
                    state = self.controller._store.load(started.loop_id)
                    state["expires_at"] = "2000-01-01T00:00:00+00:00"
                    self.controller._store.save(started.loop_id, state)
                self.controller.tick(runtime, loop_id=started.loop_id)
                self.assertEqual(len(runtime.hold_calls), 1)
                if case == "unreleased":
                    runtime.states[hold_id]["hold_released"] = True
                    self.controller.tick(runtime, loop_id=started.loop_id)
                    self.assertEqual(len(runtime.hold_calls), 2)

    def test_rotation_options_validate_and_old_state_remains_compatible(self):
        for field, values in (("turns_per_thread", (0, -1, True, "2", 1.5)), ("continue_prompt", ("", " ", 2))):
            for value in values:
                result = self.controller.start(self.runtime, request_id="bad-rotation", project="test", title="test",
                    prompt="test", max_rounds=2, target_thread_count=1,
                    expires_at="2099-01-01T00:00:00+00:00", **{field: value})
                self.assertEqual(result.status, "invalid_request")
        started = self.controller.start(self.runtime, request_id="legacy-state", project="test", title="test",
            prompt="test", max_rounds=2, target_thread_count=1, expires_at="2099-01-01T00:00:00+00:00")
        state = self.controller._store.load(started.loop_id)
        state.pop("turns_per_thread")
        state["children"][0].pop("thread_turn_limit")
        self.controller._store.save(started.loop_id, state)
        self.runtime.states[state["children"][0]["hold_id"]] = {"status": "turn_limit_reached", "total_turn_count": 2}
        result = LoopController(self.controller._store).tick(self.runtime, loop_id=started.loop_id)
        self.assertEqual(result.status, "completed")
        self.assertEqual(len(self.runtime.hold_calls), 1)

    def test_open_ended_tasks_need_no_candidate_count_or_batch(self):
        started = self.controller.start(self.runtime, request_id="open-search", project="test", title="Explore",
            prompt="Explore this coding question and record findings; no predetermined item list.",
            max_rounds=3, target_thread_count=1, expires_at="2099-01-01T00:00:00+00:00")
        self.assertEqual(started.status, "running")
        call = self.runtime.hold_calls[0]
        self.assertIsNone(call["input_binding"])
        self.assertNotIn("公司", call["prompt"])
        self.assertNotIn("binding", call["prompt"])
        self.assertEqual(call["max_turns"], 3)
        hold_id = started.data["children"][0]["hold_id"]
        self.runtime.states[hold_id] = {"status": "turn_limit_reached", "total_turn_count": 3}
        self.assertEqual(self.controller.tick(self.runtime, loop_id=started.loop_id).status, "completed")

    def test_batch_contract_rejects_invalid_size_budget_and_lane_overlap(self):
        from copy import deepcopy
        template = {"slot": "one", "lane": {"candidate_ids": [11, 22, 33], "batch_size": 2,
            "database_path": "test.sqlite", "output_boundary": "out", "result_verification": {
                "receipt_paths": {str(i): f"{i}.json" for i in (11, 22, 33)}, "terminal_statuses": ["completed"]}}}
        for size in (0, -1, True, 1.5, "2", None):
            child = deepcopy(template); child["lane"]["batch_size"] = size
            result = self.controller.start(self.runtime, request_id="bad-size", project="test", title="test", prompt="test",
                max_rounds=3, target_thread_count=1, expires_at="2099-01-01T00:00:00+00:00", threads=[child])
            self.assertEqual(result.status, "invalid_request")
            self.assertIn("batch_size", result.reason)
        for children, budget in (([template], 1), ([template, {**deepcopy(template), "slot": "two"}], 2)):
            result = self.controller.start(self.runtime, request_id="bad-binding", project="test", title="test", prompt="test",
                max_rounds=budget, target_thread_count=len(children), expires_at="2099-01-01T00:00:00+00:00", threads=children)
            self.assertEqual(result.status, "invalid_request")
        self.assertEqual(self.runtime.hold_calls, [])

    def test_invalid_output_schema_does_not_initialize_host(self):
        from unittest.mock import Mock
        provisioner = Mock()
        control = JarvisControl(object(), object(), provisioner, loop_controller=self.controller)
        receipt = control.loop(action="start", request_id="invalid-schema", project="test", title="test",
            prompt="test", target_thread_count=1, max_rounds=1, expires_at="2099-01-01T00:00:00+00:00",
            threads=[{"slot": "one", "lane": {"candidate_ids": [1], "database_path": "test.sqlite",
                "output_boundary": "outputs", "result_verification": {
                    "receipt_paths": {"1": "receipt.json"}, "terminal_statuses": ["completed"],
                    "output_schema": {"properties": {"score": {"type": "invalid"}}}}}}])
        self.assertEqual(receipt["status"], "invalid_request")
        self.assertIn("/properties/score/type", receipt["reason"])
        provisioner.ensure_hold_host_ready.assert_not_called()
        provisioner.provision.assert_not_called()

    def test_optional_output_schema_is_validated_and_persisted_before_dispatch(self):
        for index, schema in enumerate((None, {"type": "invalid"}, {"$ref": "https://example.invalid"},
                                        False, True, {"type": "object"})):
            with self.subTest(schema=schema):
                runtime = FakeRuntime()
                before = len(list(Path(self.temp.name).rglob("state.json")))
                result = self.controller.start(runtime, request_id=f"schema-{index}", project="test", title="test",
                    prompt="test", max_rounds=1, target_thread_count=1, expires_at="2099-01-01T00:00:00+00:00",
                    threads=[{"slot": "one", "lane": {"candidate_ids": [1], "database_path": "test.sqlite",
                        "output_boundary": "outputs", "result_verification": {
                            "receipt_paths": {"1": "receipt.json"}, "terminal_statuses": ["completed"],
                            "output_schema": schema}}}])
                if index < 3:
                    self.assertEqual(result.status, "invalid_request")
                    self.assertEqual(runtime.hold_calls, [])
                    self.assertEqual(runtime.heartbeat_calls, [])
                    self.assertEqual(len(list(Path(self.temp.name).rglob("state.json"))), before)
                else:
                    self.assertEqual(result.status, "running")
                    self.assertEqual(runtime.hold_calls[0]["input_binding"]["result_verification"]["output_schema"], schema)
                    saved = self.controller._store.load(result.loop_id)
                    self.assertEqual(saved["children"][0]["lane"]["result_verification"]["output_schema"], schema)

    def test_stale_tick_cannot_overwrite_a_persisted_stop(self):
        started = self.controller.start(self.runtime, request_id="stop-race", project="test", title="test", prompt="test",
                                        target_thread_count=1, max_rounds=2, expires_at="2099-01-01T00:00:00+00:00")
        stale_tick = self.controller._store.load(started.loop_id)
        self.runtime.stop_hold = lambda _: {"status": "stop_requested"}
        self.controller.stop(self.runtime, loop_id=started.loop_id)
        self.controller._store.save(started.loop_id, stale_tick)
        persisted = self.controller._store.load(started.loop_id)
        self.assertEqual(persisted["status"], "stopping")
        self.assertEqual(persisted["cleanup"]["target"], "stopped")

    def test_pilot_and_remaining_lane_use_new_requests_and_fresh_budgets(self):
        for request_id, candidates in (("pilot", [7]), ("remaining", [9, 11])):
            started = self.controller.start(self.runtime, request_id=request_id, project="test", prompt="test",
                max_rounds=len(candidates), max_turns=len(candidates), target_thread_count=1,
                expires_at="2099-01-01T00:00:00+00:00", threads=[{
                    "slot": "worker-1", "acquire": "resume", "task_id": "same-task", "lane": {
                        "candidate_ids": candidates, "database_path": "unused.sqlite", "output_boundary": "unused",
                        "result_verification": {"receipt_paths": {str(value): f"{value}.json" for value in candidates},
                                                "terminal_statuses": ["completed"]}}}])
            self.assertEqual(started.status, "running")
            hold_id = started.data["children"][0]["hold_id"]
            self.runtime.states[hold_id] = {"status": "turn_limit_reached", "total_turn_count": len(candidates),
                "thread_id": "same-task", "output_verification": {"status": "verified", "candidate_id": candidates[-1]}}
            self.assertEqual(self.controller.tick(self.runtime, loop_id=started.loop_id).status, "completed")
        self.assertEqual([call["max_turns"] for call in self.runtime.hold_calls], [1, 2])
        self.assertEqual([call["task_id"] for call in self.runtime.hold_calls], ["same-task", "same-task"])
        self.assertNotEqual(self.runtime.hold_calls[0]["request_id"], self.runtime.hold_calls[1]["request_id"])
        self.assertTrue(all(call["hold_id"] is None for call in self.runtime.hold_calls))

    def test_new_bound_lane_without_verification_is_rejected(self):
        result = self.controller.start(self.runtime, request_id="missing-contract", project="test", title="test", prompt="test",
            max_rounds=1, target_thread_count=1, expires_at="2099-01-01T00:00:00+00:00", threads=[{
                "slot": "worker-1", "lane": {"candidate_ids": [7], "database_path": "unused", "output_boundary": "unused"}}])
        self.assertEqual(result.status, "invalid_request")
        self.assertIn("result_verification", result.reason)
        self.assertEqual(self.runtime.hold_calls, [])

    def test_new_bound_loop_never_counts_turn_limit_alone_as_business_completion(self):
        started = self.controller.start(self.runtime, request_id="missing-output", project="test", title="test", prompt="test",
            max_rounds=1, target_thread_count=1, expires_at="2099-01-01T00:00:00+00:00", threads=[{
                "slot": "worker-1", "lane": {"candidate_ids": [7], "database_path": "unused", "output_boundary": "unused",
                    "result_verification": {"receipt_paths": {"7": "7.json"}, "terminal_statuses": ["completed"]}}}])
        self.runtime.states[started.data["children"][0]["hold_id"]] = {"status": "turn_limit_reached", "total_turn_count": 1}
        result = self.controller.tick(self.runtime, loop_id=started.loop_id)
        self.assertEqual(result.status, "blocked")
        self.assertEqual(result.data["children"][0]["business_status"], "unverified")

    def test_stop_round_boundary_uses_real_adapter_host_and_releases_only_its_hold(self):
        import threading
        from unittest.mock import patch
        from tests.jarvis_runtime.test_jarvis_task_hold_host import FakeClient, FakeConfig, TerminalGateClient
        from adapters.codex_app_server.jarvis_hold_host_service import JarvisHoldHost
        from jarvis_control import TaskProvisionRequest

        class Config(FakeConfig):
            def resolve_project(self, project):
                return project, str(Path(self_temp))

        class Heartbeats:
            heartbeat_available = True

            def invoke(self, request):
                return SimpleNamespace(status="completed" if request.capability == "heartbeat.cancel" else "active",
                                       request_id=request.request_id, target_thread_id=None, turn_id=None,
                                       reason=None, data={})

        self_temp = self.temp.name
        root = Path(self_temp)
        adapter = CodexAppServerTaskProvisioningAdapter(root / "unused.json", state_dir=root, config_loader=lambda _: Config())
        control = JarvisControl(Heartbeats(), object(), adapter, loop_controller=self.controller)
        started = self.controller.start(control, request_id="boundary", project="test", title="test", prompt="test",
                                        target_thread_count=1, max_rounds=2, expires_at="2099-01-01T00:00:00+00:00")
        hold_id = started.data["children"][0]["hold_id"]
        gate = TerminalGateClient(None)
        host = JarvisHoldHost(state_dir=root, launcher_config=root / "unused.json")
        with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
            "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=gate,
        ):
            runner = threading.Thread(target=host.run_once)
            runner.start()
            try:
                self.assertTrue(gate.terminal_waiting.wait(timeout=1))
                stopping = control.loop(action="stop", loop_id=started.loop_id)
                self.assertEqual(stopping["status"], "stopping")
                self.assertFalse(stopping["readback"]["terminal"])
                self.assertFalse(adapter.hold_status(hold_id)["hold_released"])
                self.assertTrue(json.loads(adapter._existing_paths(hold_id)["request"].read_text(encoding="utf-8"))["stop_requested"])
            finally:
                gate.allow_terminal.set()
                runner.join(timeout=3)
            self.assertFalse(runner.is_alive())
        stopped = control.loop(action="status", loop_id=started.loop_id)
        self.assertEqual(stopped["status"], "stopped")
        self.assertTrue(stopped["readback"]["terminal"])
        self.assertEqual(gate.started_turns, [])
        self.assertTrue(adapter.hold_status(hold_id)["hold_released"])
        self.assertEqual(stopped["data"]["cleanup"]["host"], "retained")
        self.assertEqual(control.loop(action="stop", loop_id=started.loop_id)["data"], stopped["data"])
        adapter.provision(TaskProvisionRequest(request_id="other", project="test", title="test", prompt="test", source_ref="test"))
        other = FakeClient(None)
        with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
            "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=other,
        ):
            self.assertTrue(host.run_once())
        self.assertEqual(adapter.hold_status("hold-other")["status"], "turn_limit_reached")
        self.assertEqual(len(other.created_requests), 1)
        self.assertEqual(json.loads((root / "hold-host.json").read_text(encoding="utf-8"))["active_count"], 0)

    def test_expiry_and_blocked_sibling_use_the_same_pending_cleanup(self):
        for target in ("expired", "blocked"):
            with self.subTest(target=target):
                stopped = []
                self.runtime.stop_hold = lambda hold_id: stopped.append(hold_id) or {"status": "stop_requested"}
                started = self.controller.start(self.runtime, request_id=target, project="test", title="test", prompt="test",
                                                target_thread_count=2, max_rounds=2, expires_at="2099-01-01T00:00:00+00:00")
                children = started.data["children"]
                if target == "expired":
                    state = self.controller._store.load(started.loop_id)
                    state["expires_at"] = "2000-01-01T00:00:00+00:00"
                    self.controller._store.save(started.loop_id, state)
                else:
                    self.runtime.states[children[0]["hold_id"]] = {"status": "blocked", "thread_id": "thread-1"}
                pending = self.controller.tick(self.runtime, loop_id=started.loop_id)
                if target == "blocked":
                    self.assertEqual(pending.status, "running")
                    self.assertEqual(stopped, [])
                    self.assertEqual(pending.data["children"][0]["phase"], "blocked")
                    # A seat-local block cannot stop its still-running sibling.
                    self.runtime.states[children[1]["hold_id"]] = {"status": "completed"}
                    pending = self.controller.tick(self.runtime, loop_id=started.loop_id)
                    self.assertEqual(pending.status, "blocked")
                    self.assertEqual(pending.data["business_status"], "review")
                    continue
                self.assertEqual(pending.status, "finalizing")
                self.assertEqual(pending.data["cleanup"]["target"], target)
                self.assertEqual(set(stopped), {child["hold_id"] for child in children})
                for child in children:
                    self.runtime.states[child["hold_id"]] = {"status": "cancelled", "thread_id": "thread-1", "hold_released": True}
                self.controller.reconcile(self.runtime)
                self.assertEqual(self.controller._store.load(started.loop_id)["status"], target)
                self.assertEqual(len(stopped), 2)

    def test_old_bound_loop_is_explicitly_unverified_without_rewriting_its_binding(self):
        started = self.controller.start(self.runtime, request_id="legacy-bound", project="test", title="test", prompt="test",
                                        target_thread_count=1, max_rounds=1, expires_at="2099-01-01T00:00:00+00:00")
        state = self.controller._store.load(started.loop_id)
        lane = {"candidate_ids": [7], "database_path": "legacy.sqlite", "output_boundary": "legacy-output"}
        state["children"][0]["lane"] = lane
        self.controller._store.save(started.loop_id, state)
        self.runtime.states[state["children"][0]["hold_id"]] = {"status": "turn_limit_reached", "thread_id": "thread-1"}
        result = self.controller.status(self.runtime, loop_id=started.loop_id)
        self.assertEqual(result.status, "completed")
        self.assertEqual(result.data["children"][0]["business_status"], "legacy_unverified")
        self.assertEqual(result.data["children"][0]["lane"], lane)

    def test_first_existing_task_acquire_does_not_invent_a_hold(self):
        self.controller.start(
            self.runtime, request_id="first-resume", project="Jarvis4codex", prompt="test",
            target_thread_count=1, max_rounds=1, expires_at="2099-01-01T00:00:00+00:00",
            threads=[{"slot": "worker-1", "acquire": "resume", "task_id": "existing-task"}],
        )
        self.assertIsNone(self.runtime.hold_calls[0]["hold_id"])

    def test_stop_waits_for_persistent_hold_terminal_readback(self):
        stopped_holds = []
        self.runtime.stop_hold = lambda hold_id: stopped_holds.append(hold_id) or {"status": "stop_requested"}
        started = self.controller.start(
            self.runtime, request_id="stop-boundary", project="Jarvis4codex", title="Worker", prompt="test",
            target_thread_count=1, max_rounds=2, expires_at="2099-01-01T00:00:00+00:00",
        )
        stopping = self.controller.stop(self.runtime, loop_id=started.loop_id)
        self.assertEqual(stopping.status, "stopping")
        self.assertEqual(stopped_holds, [started.data["children"][0]["hold_id"]])

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.runtime = FakeRuntime()
        self.controller = LoopController(LoopStore(Path(self.temp.name) / "loops"))

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_loop_registers_a_bounded_reconcile_heartbeat_and_cancels_it_on_terminal(self):
        started = self.controller.start(
            self.runtime, request_id="contract-1", project="Jarvis4codex", title="Worker", prompt="test task",
            business_skill="bd-search-stage6-research", target_thread_count=1, max_rounds=2, max_turns=3,
            expires_at="2099-01-01T00:00:00+00:00",
        )
        self.assertEqual(started.status, "running")
        heartbeat = self.runtime.heartbeat_calls[0]
        self.assertEqual(heartbeat["action"], "create")
        self.assertEqual(heartbeat["request_id"], "loop-contract-1:reconcile-heartbeat")
        self.assertEqual(heartbeat["source_ref"], "jarvis_loop:loop-contract-1:reconcile-heartbeat")
        heartbeat_options = dict(heartbeat["options"])
        self.assertGreater(heartbeat_options.pop("max_runs"), 1)
        self.assertEqual(heartbeat_options, {
            "heartbeat_id": "loop-contract-1:reconcile",
            "name": "Jarvis loop reconciliation loop-contract-1",
            "function": "JarvisControl.loop_tick",
            "arguments": {"loop_id": "loop-contract-1"},
            "interval_seconds": 1800,
            "expires_at": "2099-01-01T00:00:00+00:00",
            "source_event_key": "jarvis_loop:loop-contract-1:reconcile",
            "confirmation_evidence": "jarvis_loop.start:contract-1",
        })
        self.assertTrue(self.runtime.hold_calls[0]["auto_continue"])
        self.assertEqual(self.runtime.hold_calls[0]["max_turns"], 2)
        hold_id = started.data["children"][0]["hold_id"]
        self.runtime.states[hold_id] = {
            "status": "turn_limit_reached", "thread_id": "thread-1", "total_turn_count": 2,
        }
        completed = self.controller.status(self.runtime, loop_id=started.loop_id)
        self.assertEqual(completed.status, "completed")
        self.assertEqual(len(self.runtime.hold_calls), 1)
        self.assertEqual(self.runtime.heartbeat_calls[1], {
            "action": "cancel",
            "request_id": "loop-contract-1:stop",
            "source_ref": "jarvis_loop:loop-contract-1",
            "heartbeat_id": "loop-contract-1:reconcile",
        })

    def test_legacy_v1_state_without_reconcile_fields_reaches_terminal_without_migration(self):
        started = self.controller.start(
            self.runtime, request_id="legacy-reconcile", project="Jarvis4codex", title="Worker", prompt="test task",
            target_thread_count=1, max_rounds=1, expires_at="2099-01-01T00:00:00+00:00",
        )
        loop_id = str(started.loop_id)
        state = self.controller._store.load(loop_id)
        state.pop("heartbeat_id")
        state.pop("heartbeat")
        self.controller._store.save(loop_id, state)
        hold_id = str(state["children"][0]["hold_id"])
        self.runtime.states[hold_id] = {
            "status": "turn_limit_reached", "thread_id": "thread-1", "turn_id": "turn-1", "total_turn_count": 1,
        }

        completed = self.controller.tick(self.runtime, loop_id=loop_id)

        self.assertEqual(completed.status, "completed")
        self.assertEqual([call["action"] for call in self.runtime.heartbeat_calls], ["create"])
        self.assertEqual(self.controller._store.load(loop_id)["heartbeat"], {"status": "not_required"})

    def test_loop_does_not_resume_after_one_terminal_turn_when_auto_continue_is_false(self):
        started = self.controller.start(
            self.runtime, request_id="no-auto", project="Jarvis4codex", title="Worker", prompt="test task",
            business_skill="bd-search-stage6-research", target_thread_count=1, max_rounds=9,
            auto_continue=False, expires_at="2099-01-01T00:00:00+00:00",
        )
        hold_id = started.data["children"][0]["hold_id"]
        self.runtime.states[hold_id] = {"status": "completed", "thread_id": "thread-1"}

        completed = self.controller.status(self.runtime, loop_id=started.loop_id)

        self.assertEqual(completed.status, "completed")
        self.assertEqual(len(self.runtime.hold_calls), 1)
        self.assertEqual([call["action"] for call in self.runtime.heartbeat_calls], ["create", "cancel"])

    def test_loop_renders_the_worker_skill_prompt_for_create_and_resume(self):
        prompt = (
            "你是本次 Jarvis Worker。\n\n"
            "执行、续跑和回执规则，必须严格遵守 $jarvis-run-controller。\n"
            "处理任务项并完成本次工作，必须严格遵守 $bd-search-stage6-research。\n\n"
            "任务：从 1 数到 20"
        )
        started = self.controller.start(
            self.runtime, request_id="skills", project="Jarvis4codex", title="Worker", prompt="从 1 数到 20",
            business_skill="bd-search-stage6-research", controller_skill="company-run-controller",
            target_thread_count=1, max_rounds=2,
            expires_at="2099-01-01T00:00:00+00:00",
        )

        self.assertEqual(started.status, "running")
        self.assertEqual(started.data["controller_skill"], "company-run-controller")
        self.assertEqual(started.data["business_skill"], "bd-search-stage6-research")
        self.assertEqual(self.runtime.hold_calls[0]["prompt"], prompt.replace("jarvis-run-controller", "company-run-controller"))
        self.assertEqual(len(self.runtime.hold_calls), 1)

    def test_loop_keeps_explicit_prompt_free_of_lane_default_without_lane(self):
        started = self.controller.start(
            self.runtime, request_id="plain-prompt", project="Jarvis4codex", title="Worker",
            prompt="从 1 数到 20。", business_skill="counting-test", target_thread_count=1,
            max_rounds=2, expires_at="2099-01-01T00:00:00+00:00",
        )

        prompt = self.runtime.hold_calls[0]["prompt"]
        self.assertTrue(prompt.endswith("任务：从 1 数到 20。"))
        self.assertNotIn("binding 中的一个任务项", prompt)
        self.assertEqual(started.data["children"][0]["prompt"], prompt)

    def test_loop_delegates_unbound_auto_continue_to_holder(self):
        started = self.controller.start(
            self.runtime, request_id="holder-owned", project="Jarvis4codex", title="Worker",
            prompt="count", business_skill="counting-test", target_thread_count=1, max_rounds=4,
            max_turns=1, auto_continue=True, expires_at="2099-01-01T00:00:00+00:00",
        )
        call = self.runtime.hold_calls[0]
        self.assertTrue(call["auto_continue"])
        self.assertEqual(call["max_turns"], 4)

        hold_id = started.data["children"][0]["hold_id"]
        self.runtime.states[hold_id] = {
            "status": "turn_limit_reached", "thread_id": "thread-1", "total_turn_count": 4,
        }
        completed = self.controller.status(self.runtime, loop_id=started.loop_id)

        self.assertEqual(completed.status, "completed")
        self.assertEqual(len(self.runtime.hold_calls), 1)

    def test_status_closes_a_holder_owned_terminal_loop_and_cancels_its_heartbeat(self):
        started = self.controller.start(
            self.runtime, request_id="terminal-reconcile", project="Jarvis4codex", title="Worker", prompt="count",
            target_thread_count=1, max_rounds=2, expires_at="2099-01-01T00:00:00+00:00",
        )
        hold_id = started.data["children"][0]["hold_id"]
        self.runtime.states[hold_id] = {
            "status": "turn_limit_reached", "thread_id": "thread-1", "turn_id": "turn-2", "total_turn_count": 2,
        }

        final = self.controller.status(self.runtime, loop_id=started.loop_id)

        self.assertEqual(final.status, "completed")
        self.assertEqual(final.data["children"][0]["round"], 2)
        self.assertEqual([call["action"] for call in self.runtime.heartbeat_calls], ["create", "cancel"])

    def test_reconcile_heartbeat_tick_closes_a_terminal_holder_owned_loop(self):
        started = self.controller.start(
            self.runtime, request_id="heartbeat-terminal", project="Jarvis4codex", title="Worker", prompt="count",
            target_thread_count=1, max_rounds=1, expires_at="2099-01-01T00:00:00+00:00",
        )
        hold_id = started.data["children"][0]["hold_id"]
        self.runtime.states[hold_id] = {
            "status": "turn_limit_reached", "thread_id": "thread-1", "turn_id": "turn-1", "total_turn_count": 1,
        }

        final = self.controller.tick(self.runtime, loop_id=str(started.loop_id))

        self.assertEqual(final.status, "completed")
        self.assertEqual([call["action"] for call in self.runtime.heartbeat_calls], ["create", "cancel"])

    def test_status_blocks_a_terminal_hold_failure_and_cancels_its_heartbeat(self):
        started = self.controller.start(
            self.runtime, request_id="blocked-terminal", project="Jarvis4codex", title="Worker", prompt="count",
            target_thread_count=1, max_rounds=2, expires_at="2099-01-01T00:00:00+00:00",
        )
        hold_id = started.data["children"][0]["hold_id"]
        self.runtime.states[hold_id] = {"status": "failed", "thread_id": "thread-1"}

        blocked = self.controller.status(self.runtime, loop_id=started.loop_id)

        self.assertEqual(blocked.status, "blocked")
        self.assertEqual([call["action"] for call in self.runtime.heartbeat_calls], ["create", "cancel"])

    def test_loop_exposes_one_stable_lane_candidate_per_worker_turn(self):
        lane = {"candidate_ids": [7, 9], "database_path": "C:/collection.sqlite", "output_boundary": "C:/outputs/worker-1"}
        lane["result_verification"] = {"receipt_paths": {"7": "7.json", "9": "9.json"}, "terminal_statuses": ["completed"]}
        started = self.controller.start(
            self.runtime, request_id="lane", project="Jarvis4codex", title="Worker", prompt="test task",
            business_skill="marketing-collection-mining", target_thread_count=1, max_rounds=2,
            threads=[{"slot": "worker-1", "acquire": "create", "title": "Worker", "lane": lane}],
            expires_at="2099-01-01T00:00:00+00:00",
        )
        self.assertEqual(self.runtime.hold_calls[0]["input_binding"], {
            **lane, "lane_identity": "worker-1", "lane_item_count": 2,
        })
        self.assertTrue(self.runtime.hold_calls[0]["auto_continue"])
        self.assertEqual(self.runtime.hold_calls[0]["max_turns"], 2)
        self.assertEqual(started.data["children"][0]["lane"], lane)
        self.assertIn("每个 Worker 回合仅处理 binding 中的一个任务项", self.runtime.hold_calls[0]["prompt"])

    def test_loop_completes_uneven_lanes_without_an_unbound_extra_turn(self):
        lanes = [
            list(range(1, 58)),
            list(range(101, 157)),
            list(range(201, 257)),
        ]
        started = self.controller.start(
            self.runtime, request_id="uneven-lanes", project="Jarvis4codex", title="Worker", prompt="test task",
            business_skill="marketing-collection-mining", target_thread_count=3, max_rounds=57,
            threads=[
                {"slot": f"worker-{number}", "acquire": "create", "title": "Worker", "lane": {
                    "candidate_ids": lane, "database_path": "C:/collection.sqlite",
                    "output_boundary": f"C:/outputs/worker-{number}",
                    "result_verification": {"receipt_paths": {str(value): f"{value}.json" for value in lane},
                                            "terminal_statuses": ["completed"]},
                }}
                for number, lane in enumerate(lanes, 1)
            ],
            expires_at="2099-01-01T00:00:00+00:00",
        )
        for child, lane in zip(started.data["children"], lanes):
            self.runtime.states[child["hold_id"]] = {
                "status": "turn_limit_reached", "thread_id": "thread-1", "total_turn_count": len(lane),
                "output_verification": {"status": "verified", "candidate_id": lane[-1]},
            }
        result = self.controller.status(self.runtime, loop_id=started.loop_id)

        self.assertEqual(result.status, "completed")
        self.assertTrue(all(child["phase"] == "completed" for child in result.data["children"]))
        for number, lane in enumerate(lanes, 1):
            call = next(call for call in self.runtime.hold_calls if call["source_ref"].endswith(f"worker-{number}"))
            self.assertEqual(call["max_turns"], len(lane))
            self.assertEqual(call["input_binding"]["candidate_ids"], lane)

    def test_loop_rejects_lane_that_exceeds_its_round_budget(self):
        result = self.controller.start(
            self.runtime, request_id="lane-budget", project="Jarvis4codex", title="Worker", prompt="test task",
            business_skill="marketing-collection-mining", target_thread_count=1, max_rounds=1,
            threads=[{"slot": "worker-1", "acquire": "create", "title": "Worker", "lane": {
                "candidate_ids": [7, 9], "database_path": "C:/collection.sqlite",
                "output_boundary": "C:/outputs/worker-1",
            }}],
            expires_at="2099-01-01T00:00:00+00:00",
        )
        self.assertEqual(result.status, "invalid_request")
        self.assertEqual(result.reason, "max_rounds must cover every candidate in each thread lane")

    def test_loop_rejects_malformed_lane(self):
        result = self.controller.start(
            self.runtime, request_id="bad-lane", project="Jarvis4codex", title="Worker", prompt="test task",
            business_skill="marketing-collection-mining", target_thread_count=1, max_rounds=1,
            threads=[{"slot": "worker-1", "acquire": "create", "title": "Worker", "lane": {"candidate_ids": [1, 1]}}],
            expires_at="2099-01-01T00:00:00+00:00",
        )
        self.assertEqual(result.status, "invalid_request")
        self.assertEqual(result.reason, "thread lane requires unique positive candidate_ids, database_path, and output_boundary")

    def test_loop_allows_no_skills_and_injects_no_skill_rules(self):
        result = self.controller.start(
            self.runtime, request_id="no-skills", project="Jarvis4codex", title="Worker", prompt="从 1 数到 20",
            target_thread_count=1, max_rounds=1, expires_at="2099-01-01T00:00:00+00:00",
        )

        self.assertEqual(result.status, "running")
        self.assertEqual(result.data["controller_skill"], "")
        self.assertEqual(result.data["business_skill"], "")
        self.assertEqual(self.runtime.hold_calls[0]["prompt"], "你是本次 Jarvis Worker。\n\n任务：从 1 数到 20")

        overridden = self.controller.start(
            self.runtime, request_id="thread-prompt", project="Jarvis4codex", title="Worker", prompt="test task",
            business_skill="bd-search-stage6-research", target_thread_count=1, max_rounds=1,
            threads=[{"slot": "worker-1", "acquire": "create", "title": "Worker", "prompt": "override"}],
            expires_at="2099-01-01T00:00:00+00:00",
        )

        self.assertEqual(overridden.status, "invalid_request")
        self.assertEqual(overridden.reason, "thread prompt is not supported; use the loop prompt")

    def test_confirmed_defaults_create_every_omitted_thread_and_reach_hold(self):
        started = self.controller.start(
            self.runtime, request_id="defaults", project="Jarvis4codex", title="Worker", prompt="test task",
            business_skill="bd-search-stage6-research", target_thread_count=2, max_rounds=1,
            expires_at="2099-01-01T00:00:00+00:00",
        )

        self.assertEqual(started.data["interval_seconds"], 1800)
        self.assertEqual(started.data["model"], "gpt-5.6-luna")
        self.assertEqual(started.data["reasoning_effort"], "max")
        self.assertEqual(started.data["max_turns"], 999)
        self.assertTrue(started.data["auto_continue"])
        self.assertEqual(started.data["notifications"], {"milestones": [], "terminal": True})
        self.assertEqual(started.data["controller_skill"], "")
        self.assertEqual([child["acquire"] for child in started.data["children"]], ["create", "create"])
        for call in self.runtime.hold_calls:
            self.assertEqual(call["model"], "gpt-5.6-luna")
            self.assertEqual(call["reasoning_effort"], "max")
            self.assertEqual(call["max_turns"], 1)
            self.assertTrue(call["auto_continue"])
            self.assertEqual(call["notifications"], {"milestones": [], "terminal": True})

    def test_explicit_loop_fields_pass_unchanged_to_hold(self):
        notifications = {"milestones": [3], "terminal": False}
        self.controller.start(
            self.runtime, request_id="override", project="Jarvis4codex", title="Worker", prompt="test task",
            business_skill="bd-search-stage6-research", target_thread_count=1, max_rounds=1, max_turns=7,
            model="gpt-5.6-terra", reasoning_effort="high", auto_continue=False,
            notifications=notifications, expires_at="2099-01-01T00:00:00+00:00",
        )

        call = self.runtime.hold_calls[0]
        self.assertEqual(call["model"], "gpt-5.6-terra")
        self.assertEqual(call["reasoning_effort"], "high")
        self.assertEqual(call["max_turns"], 7)
        self.assertFalse(call["auto_continue"])
        self.assertEqual(call["notifications"], notifications)

    def test_control_reports_loop_unavailable_without_a_configured_controller(self):
        control = JarvisControl(object(), object())
        receipt = control.loop(action="start", request_id="no-loop")
        self.assertEqual(receipt["tool"], "jarvis_loop")
        self.assertEqual(receipt["status"], "unsupported")

    def test_loop_start_fails_closed_before_hold_loop_or_heartbeat_when_host_is_unready(self):
        class Config:
            profile = "jarvis_test"
            expected_codex_home = "C:/test/codex-home"

        state_dir = Path(self.temp.name)
        (state_dir / "hold-host.json").write_text(json.dumps({
            "status": "holding",
            "pid": 99999999,
            "active_count": 3,
            "profile": "jarvis_test",
            "codex_home": "C:/test/codex-home",
            "state_dir": str(state_dir.resolve()),
        }), encoding="utf-8")
        adapter = CodexAppServerTaskProvisioningAdapter(
            state_dir / "launcher.json", state_dir=state_dir, config_loader=lambda _: Config()
        )
        control = JarvisControl(object(), object(), adapter, loop_controller=self.controller)
        receipt = control.loop(
            action="start", request_id="dead-host", project="Jarvis4codex", title="Worker",
            prompt="hello", business_skill="bd-search-stage6-research", target_thread_count=1,
            max_rounds=1, expires_at="2099-01-01T00:00:00+00:00",
        )

        self.assertEqual(receipt["status"], "host_not_ready")
        self.assertFalse(receipt["readback"]["verified"])
        self.assertFalse((Path(self.temp.name) / "loops" / "loop-dead-host").exists())
        self.assertEqual(self.runtime.hold_calls, [])
        self.assertEqual(self.runtime.heartbeat_calls, [])

    def test_loop_start_self_heals_the_host_before_creating_workers(self):
        class SchedulerCapabilities:
            heartbeat_available = True

            def invoke(self, request):
                return SimpleNamespace(
                    status="active", request_id=request.request_id, target_thread_id=None,
                    turn_id=None, reason=None, data={"heartbeat_id": request.arguments["heartbeat_id"]},
                )

        class Config:
            profile = "jarvis_test"
            expected_codex_home = "C:/test/codex-home"

            def resolve_project(self, project):
                return project, "C:/test/project"

        state_dir = Path(self.temp.name)
        initialized = []

        def initialize_host(**kwargs):
            initialized.append(kwargs["workers"])
            (state_dir / "hold-host.json").write_text(json.dumps({
                "status": "ready", "pid": 1781, "worker_capacity": kwargs["workers"],
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "host_started_at": datetime.now(timezone.utc).isoformat(),
                "profile": "jarvis_test", "codex_home": "C:/test/codex-home",
                "state_dir": str(state_dir.resolve()),
            }), encoding="utf-8")
            return {"status": "ready", "phase": "started"}

        provisioner = CodexAppServerTaskProvisioningAdapter(
            state_dir / "launcher.json", state_dir=state_dir, config_loader=lambda _: Config(),
            host_initializer=initialize_host, pid_alive=lambda _: True,
            pid_started_at=lambda _: datetime.now(timezone.utc),
        )
        control = JarvisControl(SchedulerCapabilities(), object(), provisioner, loop_controller=self.controller)

        receipt = control.loop(
            action="start", request_id="self-heal", project="Jarvis4codex", title="Worker",
            prompt="hello", business_skill="bd-search-stage6-research", target_thread_count=2,
            max_rounds=1, expires_at="2099-01-01T00:00:00+00:00",
        )

        self.assertEqual(receipt["status"], "running")
        self.assertEqual(initialized, [2])
        self.assertEqual(len(list((state_dir / "task-holds").glob("*/request.json"))), 2)

    def test_loop_start_upgrades_an_idle_host_before_creating_workers(self):
        class SchedulerCapabilities:
            heartbeat_available = True

            def invoke(self, request):
                return SimpleNamespace(
                    status="active", request_id=request.request_id, target_thread_id=None,
                    turn_id=None, reason=None, data={"heartbeat_id": request.arguments["heartbeat_id"]},
                )

        class Config:
            profile = "jarvis_test"
            expected_codex_home = "C:/test/codex-home"

            def resolve_project(self, project):
                return project, "C:/test/project"

        state_dir = Path(self.temp.name)
        (state_dir / "hold-host.json").write_text(json.dumps({
            "status": "ready", "pid": 1780, "worker_capacity": 1, "active_count": 0,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "host_started_at": datetime.now(timezone.utc).isoformat(),
            "profile": "jarvis_test", "codex_home": "C:/test/codex-home",
            "state_dir": str(state_dir.resolve()),
        }), encoding="utf-8")
        initialized = []

        def initialize_host(**kwargs):
            initialized.append(kwargs["workers"])
            (state_dir / "hold-host.json").write_text(json.dumps({
                "status": "ready", "pid": 1781, "worker_capacity": kwargs["workers"], "active_count": 0,
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "host_started_at": datetime.now(timezone.utc).isoformat(),
                "profile": "jarvis_test", "codex_home": "C:/test/codex-home",
                "state_dir": str(state_dir.resolve()),
            }), encoding="utf-8")
            return {"status": "ready", "phase": "capacity_upgraded"}

        provisioner = CodexAppServerTaskProvisioningAdapter(
            state_dir / "launcher.json", state_dir=state_dir, config_loader=lambda _: Config(),
            host_initializer=initialize_host, pid_alive=lambda _: True,
            pid_started_at=lambda _: datetime.now(timezone.utc),
        )
        control = JarvisControl(SchedulerCapabilities(), object(), provisioner, loop_controller=self.controller)

        receipt = control.loop(
            action="start", request_id="capacity-upgrade", project="Jarvis4codex", title="Worker",
            prompt="hello", target_thread_count=3, max_rounds=1, expires_at="2099-01-01T00:00:00+00:00",
        )

        self.assertEqual(receipt["status"], "running")
        self.assertEqual(initialized, [3])
        self.assertEqual(len(list((state_dir / "task-holds").glob("*/request.json"))), 3)

    def test_loop_preflight_returns_start_contract_without_starting_any_hold(self):
        class Provisioner:
            def preflight_projects(self):
                return ["BD Search Worker", "Jarvis4codex"]

        control = JarvisControl(object(), object(), Provisioner(), loop_controller=self.controller)
        receipt = control.loop(action="preflight")

        self.assertEqual(receipt["status"], "completed")
        self.assertTrue(receipt["readback"]["verified"])
        self.assertEqual(receipt["data"]["allowed_projects"], ["BD Search Worker", "Jarvis4codex"])
        self.assertEqual(
            receipt["data"]["start_contract"]["required"],
            ["request_id", "project", "prompt", "target_thread_count", "max_rounds", "expires_at"],
        )
        self.assertEqual(receipt["data"]["start_contract"]["threads"], {
            "item": {
                "slot": "non-empty unique string",
                "acquire": "create|resume (default create)",
                "create_requires": ["title"],
                "resume_requires": ["task_id"],
                "lane": {
                    "optional": True,
                    "candidate_ids": "unique positive integers",
                    "batch_size": "optional positive integer, default 1; any size, tail batch uses remaining IDs",
                    "database_path": "non-empty path",
                    "output_boundary": "non-empty path",
                    "result_verification": "receipt_paths by candidate id and explicit terminal_statuses; optional output_schema (Draft 2020-12, document-local refs, no $id)",
                    "lane_identity": "read-only worker slot injected by Loop",
                    "lane_item_count": "read-only total candidate count injected by Loop",
                },
            },
            "count": "must equal target_thread_count when supplied",
        })
        self.assertEqual(self.runtime.hold_calls, [])
        self.assertEqual(self.runtime.heartbeat_calls, [])

    def test_invalid_existing_state_is_blocked_without_replacing_it(self):
        loop_id = "loop-corrupt"
        path = Path(self.temp.name) / "loops" / loop_id / "state.json"
        path.parent.mkdir(parents=True)
        path.write_text("{}", encoding="utf-8")

        result = self.controller.start(
            self.runtime, request_id="corrupt", project="Jarvis4codex", title="Worker", prompt="test task",
            business_skill="bd-search-stage6-research", target_thread_count=1, max_rounds=1,
            expires_at="2099-01-01T00:00:00+00:00",
        )

        self.assertEqual(result.status, "blocked")
        self.assertEqual(path.read_text(encoding="utf-8"), "{}")

    def test_mcp_source_registers_loop_without_starting_or_connecting_to_mcp(self):
        server_path = Path(__file__).parents[2] / "packages" / "jarvis_mcp" / "server.py"
        tree = ast.parse(server_path.read_text(encoding="utf-8"))
        functions = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == "jarvis_loop"]
        self.assertEqual(len(functions), 1)
        decorators = functions[0].decorator_list
        names = [keyword.value.value for decorator in decorators if isinstance(decorator, ast.Call)
                 for keyword in decorator.keywords if keyword.arg == "name" and isinstance(keyword.value, ast.Constant)]
        self.assertEqual(names, ["jarvis_loop"])
        arguments = {argument.arg for argument in functions[0].args.args}
        self.assertTrue({"model", "reasoning_effort", "notifications", "business_skill", "controller_skill"}.issubset(arguments))
        self.assertIn("prompt", arguments)
        self.assertTrue({"continue_prompt", "turns_per_thread"}.issubset(arguments))
        loop_calls = [node for node in ast.walk(functions[0]) if isinstance(node, ast.Call)
                      and isinstance(node.func, ast.Attribute) and node.func.attr == "loop"]
        self.assertEqual(len(loop_calls), 1)
        forwarded = {keyword.arg for keyword in loop_calls[0].keywords}
        self.assertTrue({"continue_prompt", "turns_per_thread"}.issubset(forwarded))
        self.assertTrue({"model", "reasoning_effort", "notifications", "prompt", "business_skill", "controller_skill"}.issubset(forwarded))


if __name__ == "__main__":
    unittest.main()
