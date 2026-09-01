from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
import tempfile
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
        hold_id = kwargs["hold_id"]
        self.states.setdefault(hold_id, {"status": "holding", "thread_id": kwargs.get("task_id") or "thread-1"})
        return {"status": "holding", "target_thread_id": self.states[hold_id]["thread_id"], "data": {"monitor_id": hold_id}}

    def monitor(self, **kwargs):
        return {"status": "completed", "data": dict(self.states[kwargs["hold_id"]])}

    def heartbeat(self, **kwargs):
        self.heartbeat_calls.append(kwargs)
        return {"status": "active" if kwargs["action"] == "create" else "completed"}


class JarvisLoopContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.runtime = FakeRuntime()
        self.controller = LoopController(LoopStore(Path(self.temp.name) / "loops"))

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_loop_delegates_continuation_to_holder_without_a_tick_heartbeat(self):
        started = self.controller.start(
            self.runtime, request_id="contract-1", project="Jarvis4codex", title="Worker", prompt="test task",
            business_skill="bd-search-stage6-research", target_thread_count=1, max_rounds=2, max_turns=3,
            expires_at="2099-01-01T00:00:00+00:00",
        )
        self.assertEqual(started.status, "running")
        self.assertEqual(self.runtime.heartbeat_calls, [])
        self.assertTrue(self.runtime.hold_calls[0]["auto_continue"])
        self.assertEqual(self.runtime.hold_calls[0]["max_turns"], 2)
        hold_id = started.data["children"][0]["hold_id"]
        self.runtime.states[hold_id] = {
            "status": "turn_limit_reached", "thread_id": "thread-1", "total_turn_count": 2,
        }
        completed = self.controller.status(self.runtime, loop_id=started.loop_id)
        self.assertEqual(completed.status, "completed")
        self.assertEqual(len(self.runtime.hold_calls), 1)

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

    def test_loop_renders_the_worker_skill_prompt_for_create_and_resume(self):
        prompt = (
            "你是本次 Jarvis Worker。\n\n"
            "执行、续跑和回执规则，必须严格遵守 $jarvis-run-controller。\n"
            "处理公司和完成本次业务工作，必须严格遵守 $bd-search-stage6-research。\n\n"
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
        self.assertNotIn("binding 中的一家公司", prompt)
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

    def test_status_closes_a_holder_owned_terminal_loop_without_heartbeat(self):
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
        self.assertEqual(self.runtime.heartbeat_calls, [])

    def test_status_blocks_a_terminal_hold_failure_without_heartbeat(self):
        started = self.controller.start(
            self.runtime, request_id="blocked-terminal", project="Jarvis4codex", title="Worker", prompt="count",
            target_thread_count=1, max_rounds=2, expires_at="2099-01-01T00:00:00+00:00",
        )
        hold_id = started.data["children"][0]["hold_id"]
        self.runtime.states[hold_id] = {"status": "failed", "thread_id": "thread-1"}

        blocked = self.controller.status(self.runtime, loop_id=started.loop_id)

        self.assertEqual(blocked.status, "blocked")
        self.assertEqual(self.runtime.heartbeat_calls, [])

    def test_loop_exposes_one_stable_lane_candidate_per_worker_turn(self):
        lane = {"candidate_ids": [7, 9], "database_path": "C:/collection.sqlite", "output_boundary": "C:/outputs/worker-1"}
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
        self.assertIn("每个 Worker 回合仅处理 binding 中的一家公司", self.runtime.hold_calls[0]["prompt"])

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
                }}
                for number, lane in enumerate(lanes, 1)
            ],
            expires_at="2099-01-01T00:00:00+00:00",
        )
        for child, lane in zip(started.data["children"], lanes):
            self.runtime.states[child["hold_id"]] = {
                "status": "turn_limit_reached", "thread_id": "thread-1", "total_turn_count": len(lane),
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
                    "database_path": "non-empty path",
                    "output_boundary": "non-empty path",
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
        self.assertNotIn("continue_prompt", arguments)
        loop_calls = [node for node in ast.walk(functions[0]) if isinstance(node, ast.Call)
                      and isinstance(node.func, ast.Attribute) and node.func.attr == "loop"]
        self.assertEqual(len(loop_calls), 1)
        forwarded = {keyword.arg for keyword in loop_calls[0].keywords}
        self.assertTrue({"model", "reasoning_effort", "notifications", "prompt", "business_skill", "controller_skill"}.issubset(forwarded))


if __name__ == "__main__":
    unittest.main()
