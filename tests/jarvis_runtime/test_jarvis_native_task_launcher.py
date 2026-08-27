from __future__ import annotations

import json
from pathlib import Path
import queue
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch


TOOLS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TOOLS_DIR))

from coo_dispatcher_store import DispatcherError, DispatcherStore, ProcessLock, iter_jsonl  # noqa: E402
from jarvis_native_task_launcher import (  # noqa: E402
    AppServerClient,
    NativeTaskError,
    NativeTaskCreationError,
    NativeTaskLauncherConfig,
    NativeTaskQueue,
    RESULT_CONTRACT_MARKER,
    parse_direct_task,
    summarize_final_message,
)


THREAD_ID = "019f7603-a0e6-7400-8383-22ea84f2e621"
COOPER_ID = "ou_cooper"


class FakeAppClient:
    def __init__(self):
        self.requests = []

    def create_task(self, request):
        self.requests.append(request)
        return {
            "thread_id": "thread-native-001",
            "turn_id": "turn-native-001",
            "turn_status": "completed",
            "final_message": "原生任务已完成，结果已生成。",
        }


class FailingAppClient:
    def create_task(self, request):
        raise NativeTaskCreationError(f"simulated failure for {request['request_id']}")


class RecoveryAppClient:
    def __init__(self):
        self.calls = []

    def run_existing_task(
        self,
        thread_id,
        request,
        *,
        client_user_message_id,
        model=None,
        reasoning_effort=None,
    ):
        self.calls.append(
            {
                "thread_id": thread_id,
                "request_id": request["request_id"],
                "client_user_message_id": client_user_message_id,
                "model": model,
                "reasoning_effort": reasoning_effort,
            }
        )
        return {
            "thread_id": thread_id,
            "turn_id": "turn-recovered-001",
            "turn_status": "completed",
            "model": model or "gpt-5.5",
            "reasoning_effort": reasoning_effort,
        }


class LauncherTests(unittest.TestCase):
    def make_queue(self, temp: str, *, live: bool = False):
        base = Path(temp)
        root = base / "dispatcher"
        store = DispatcherStore(root)
        store.bootstrap(
            THREAD_ID,
            binding_overrides={
                "cooper_open_id": COOPER_ID,
                "primary_chat_id": "oc_cooper",
                "allowed_chat_ids": ["oc_cooper"],
                "live_ingress_enabled": True,
                "live_send_enabled": True,
                "live_dispatch_enabled": True,
            },
        )
        config_path = root / "native_task_launcher.config.json"
        config_path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "dispatcher_thread_id": THREAD_ID,
                    "expected_codex_home": "",
                    "live_creation_enabled": live,
                    "max_attempts": 3,
                    "cooper_actor_ids": [COOPER_ID],
                    "allowed_projects": {"Chief of Staff": str(base)},
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        config = NativeTaskLauncherConfig(config_path)
        return NativeTaskQueue(root, config), store, config

    def direct_request(self, suffix="1"):
        return {
            "request_id": f"native:req-{suffix}",
            "correlation_id": f"native:corr-{suffix}",
            "origin": "cooper_direct",
            "route": "direct_task",
            "source_channel": "feishu",
            "source_thread_id": "oc_cooper",
            "source_message_id": f"om-{suffix}",
            "actor_id": COOPER_ID,
            "project": "Chief of Staff",
            "title": "中文原生任务",
            "prompt": "创建后向 Jarvis COO 回调，但不要执行外部写入。",
        }

    def test_direct_syntax(self):
        parsed = parse_direct_task(
            "/task Chief of Staff | 中文测试 | 只做本地只读状态检查"
        )
        self.assertEqual(parsed["project"], "Chief of Staff")
        self.assertEqual(parsed["title"], "中文测试")
        self.assertIn("本地只读", parsed["prompt"])
        self.assertIsNone(parse_direct_task("普通消息"))

    def test_concurrent_duplicate_submission_has_one_winner(self):
        with tempfile.TemporaryDirectory() as temp:
            task_queue, _, _ = self.make_queue(temp)
            request = self.direct_request()
            with ThreadPoolExecutor(max_workers=8) as pool:
                results = list(pool.map(lambda _: task_queue.submit(request), range(8)))
            self.assertEqual(sum(1 for item in results if item["queued"]), 1)
            self.assertEqual(sum(1 for item in results if item["duplicate"]), 7)
            self.assertEqual(len(list(iter_jsonl(task_queue.requests_path))), 1)

    def test_worker_cannot_self_authorize_visible_tasks(self):
        with tempfile.TemporaryDirectory() as temp:
            task_queue, _, _ = self.make_queue(temp)
            request = self.direct_request()
            request.update(
                {
                    "origin": "worker_delegated",
                    "actor_id": "worker-thread",
                    "route": "",
                    "can_request_visible_tasks": True,
                    "confirmation_id": "confirm-not-real",
                }
            )
            with self.assertRaisesRegex(NativeTaskError, "confirmed parent packet"):
                task_queue.submit(request)

    def test_dry_run_builds_native_protocol_without_creating(self):
        with tempfile.TemporaryDirectory() as temp:
            task_queue, _, _ = self.make_queue(temp, live=False)
            task_queue.submit(self.direct_request())
            plan = task_queue.process_one(FakeAppClient(), execute=False)
            self.assertTrue(plan["dry_run"])
            self.assertFalse(plan["thread_start"]["ephemeral"])
            prompt = plan["turn_start"]["input"][0]["text"]
            self.assertTrue(prompt.startswith("创建后向 Jarvis COO 回调"))
            self.assertIn(RESULT_CONTRACT_MARKER, prompt)
            self.assertIn("不要只写“已完成”", prompt)
            self.assertEqual(list(iter_jsonl(task_queue.results_path)), [])

    def test_execute_rejects_native_thread_creation(self):
        with tempfile.TemporaryDirectory() as temp:
            task_queue, store, _ = self.make_queue(temp, live=True)
            task_queue.submit(self.direct_request())
            with self.assertRaisesRegex(NativeTaskError, "creation has been removed"):
                task_queue.process_one(FakeAppClient(), execute=True)
            self.assertEqual(list(iter_jsonl(store.execution_registry_path)), [])

    def test_summary_skips_generic_completed_line_and_delivery_block(self):
        value = (
            "已完成。\n\n今天发送 230 封，队列与发件器均正常。\n"
            '<jarvis_delivery>{"attachments":[]}</jarvis_delivery>'
        )
        self.assertEqual(
            summarize_final_message(value),
            "今天发送 230 封，队列与发件器均正常。",
        )

    def test_app_server_client_exposes_task_creation_method(self):
        self.assertTrue(hasattr(AppServerClient, "create_task"))

    def test_app_server_client_creates_and_returns_the_started_turn_without_waiting_for_terminal(self):
        with tempfile.TemporaryDirectory() as temp:
            _, _, config = self.make_queue(temp)
            client = AppServerClient.__new__(AppServerClient)
            client.config = config
            calls = []

            def request(method, params):
                calls.append((method, params))
                if method == "thread/start":
                    return {"thread": {"id": "thread-created-1"}}
                raise AssertionError(method)

            client.start = lambda: None
            client.request = request
            client.select_model = lambda _model: "gpt-test"
            client._start_turn = lambda thread_id, prompt, **kwargs: {
                "thread_id": thread_id,
                "turn_id": "turn-created-1",
                "turn_status": "inProgress",
                "model": kwargs["selected_model"],
                "reasoning_effort": kwargs["selected_effort"],
            }

            result = client.create_task({
                "request_id": "create-1",
                "project_path": "C:/test/project",
                "title": "TEST Worker",
                "prompt": "hello",
            })

        self.assertEqual(result["thread_id"], "thread-created-1")
        self.assertEqual(result["turn_id"], "turn-created-1")
        self.assertEqual(result["turn_status"], "inProgress")
        self.assertEqual(calls, [
            ("thread/start", {"cwd": "C:/test/project", "ephemeral": False}),
        ])

    def test_app_server_client_sends_a_lane_binding_with_the_worker_turn(self):
        with tempfile.TemporaryDirectory() as temp:
            _, _, config = self.make_queue(temp)
            client = AppServerClient.__new__(AppServerClient)
            client.config = config
            client.start = lambda: None
            client.request = lambda method, _params: (
                {"thread": {"id": "thread-created-1"}}
                if method == "thread/start"
                else (_ for _ in ()).throw(AssertionError(method))
            )
            client.select_model = lambda _model: "gpt-test"
            observed = {}
            client._start_turn = lambda thread_id, prompt, **kwargs: observed.update(
                thread_id=thread_id, prompt=prompt, **kwargs
            ) or {
                "thread_id": thread_id,
                "turn_id": "turn-created-1",
                "turn_status": "inProgress",
                "model": kwargs["selected_model"],
                "reasoning_effort": kwargs["selected_effort"],
            }

            client.create_task({
                "request_id": "create-lane-1",
                "project_path": "C:/test/project",
                "title": "TEST Worker",
                "prompt": "固定 Worker 提示词",
                "input_binding": {
                    "candidate_ids": [7, 9],
                    "database_path": "C:/collection.sqlite",
                    "output_boundary": "C:/outputs/worker-1",
                },
            })

        self.assertEqual(observed["thread_id"], "thread-created-1")
        self.assertIn("[Jarvis lane binding v1]", observed["prompt"])
        self.assertIn('"candidate_ids":[7,9]', observed["prompt"])
        self.assertIn('"database_path":"C:/collection.sqlite"', observed["prompt"])
        self.assertIn("candidate_ids 必须且只会包含一家公司", observed["prompt"])

    def test_app_server_client_resumes_the_persisted_thread_before_starting_a_new_turn(self):
        with tempfile.TemporaryDirectory() as temp:
            _, _, config = self.make_queue(temp)
            client = AppServerClient.__new__(AppServerClient)
            client.config = config
            calls = []

            def request(method, params):
                calls.append((method, params))
                if method == "thread/resume":
                    return {"thread": {"id": "thread-existing-1"}}
                raise AssertionError(method)

            client.start = lambda: None
            client.request = request
            client.start_turn_async = lambda thread_id, prompt, **kwargs: {
                "thread_id": thread_id,
                "turn_id": "turn-resumed-1",
                "prompt": prompt,
                **kwargs,
            }

            result = client.resume_turn_async(
                "thread-existing-1",
                "继续",
                client_user_message_id="resume-1",
                model="gpt-test",
                reasoning_effort="medium",
            )

        self.assertEqual(calls, [("thread/resume", {"threadId": "thread-existing-1"})])
        self.assertEqual(result["thread_id"], "thread-existing-1")
        self.assertEqual(result["prompt"], "继续")
        self.assertEqual(result["client_user_message_id"], "resume-1")

    def test_app_server_client_rebinds_the_single_candidate_when_recovering_a_thread(self):
        with tempfile.TemporaryDirectory() as temp:
            _, _, config = self.make_queue(temp)
            client = AppServerClient.__new__(AppServerClient)
            client.config = config
            client.start = lambda: None
            client.request = lambda method, params: (
                {"thread": {"id": params["threadId"]}}
                if method == "thread/resume" else (_ for _ in ()).throw(AssertionError(method))
            )
            client.select_model = lambda _model: "gpt-test"
            observed = {}
            client._start_turn_with_model = lambda thread_id, prompt, **kwargs: observed.update(
                thread_id=thread_id, prompt=prompt, **kwargs
            ) or {"thread_id": thread_id, "turn_id": "turn-recovered-1"}

            client.run_existing_task(
                "thread-existing-1",
                {
                    "prompt": "继续",
                    "input_binding": {
                        "candidate_ids": [7],
                        "database_path": "C:/collection.sqlite",
                        "output_boundary": "C:/outputs/worker-1",
                    },
                },
                client_user_message_id="recover-1",
            )

        self.assertEqual(observed["thread_id"], "thread-existing-1")
        self.assertIn("[Jarvis lane binding v1]", observed["prompt"])
        self.assertIn('"candidate_ids":[7]', observed["prompt"])
        self.assertIn("candidate_ids 必须且只会包含一家公司", observed["prompt"])

    def test_invalid_reasoning_effort_is_rejected_before_queueing(self):
        with tempfile.TemporaryDirectory() as temp:
            task_queue, _, _ = self.make_queue(temp)
            value = self.direct_request()
            value["reasoning_effort"] = "extreme"
            with self.assertRaisesRegex(
                NativeTaskError,
                "unsupported reasoning_effort",
            ):
                task_queue.submit(value)
            self.assertEqual(list(iter_jsonl(task_queue.requests_path)), [])

    def test_wait_for_turn_terminal_matches_thread_and_turn(self):
        with tempfile.TemporaryDirectory() as temp:
            _, _, config = self.make_queue(temp)
            config.turn_completion_timeout_seconds = 10
            client = AppServerClient.__new__(AppServerClient)
            client.config = config
            client.notifications = queue.Queue()
            client.notifications.put(
                {
                    "method": "turn/completed",
                    "params": {
                        "threadId": "other-thread",
                        "turn": {"id": "turn-1", "status": "completed"},
                    },
                }
            )
            client.notifications.put(
                {
                    "method": "turn/completed",
                    "params": {
                        "threadId": "thread-1",
                        "turn": {"id": "turn-1", "status": "completed"},
                    },
                }
            )
            completed = client.wait_for_turn_terminal("thread-1", "turn-1")
            self.assertEqual(completed["status"], "completed")

    def test_turn_readback_waits_for_exact_turn_materialization(self):
        """A completed notification alone is not a completed, visible turn."""
        with tempfile.TemporaryDirectory() as temp:
            _, _, config = self.make_queue(temp)
            config.turn_readback_timeout_seconds = 1
            config.poll_seconds = 0.01
            client = AppServerClient.__new__(AppServerClient)
            client.config = config
            reads = 0

            def request(method, _params):
                nonlocal reads
                if method == "turn/start":
                    return {"turn": {"id": "turn-1"}}
                if method == "thread/read":
                    reads += 1
                    items = [] if reads == 1 else [
                        {
                            "type": "agentMessage",
                            "phase": "final_answer",
                            "text": "materialized final answer",
                        }
                    ]
                    return {"thread": {"turns": [{"id": "turn-1", "items": items}]}}
                raise AssertionError(f"unexpected request: {method}")

            client.request = request
            client.wait_for_turn_terminal = lambda _thread_id, turn_id: {
                "id": turn_id,
                "status": "completed",
            }
            with patch("jarvis_native_task_launcher.time.sleep"):
                result = client._start_turn_with_model(
                    "thread-1",
                    "say hi",
                    client_user_message_id="event-1",
                    selected_model="gpt-5.6-terra",
                )
            self.assertEqual(result["final_message"], "materialized final answer")
            self.assertEqual(reads, 2)

    def test_same_thread_uses_one_cross_process_operation_lock(self):
        with tempfile.TemporaryDirectory() as temp:
            _, _, config = self.make_queue(temp)
            first = AppServerClient.__new__(AppServerClient)
            second = AppServerClient.__new__(AppServerClient)
            first.config = config
            second.config = config
            self.assertEqual(
                first.thread_operation_lock_path("thread-owned"),
                second.thread_operation_lock_path("thread-owned"),
            )
            lock_path = first.thread_operation_lock_path("thread-owned")
            with ProcessLock(lock_path, timeout_seconds=0.01):
                with self.assertRaisesRegex(DispatcherError, "lock timeout"):
                    with ProcessLock(lock_path, timeout_seconds=0.01):
                        pass


if __name__ == "__main__":
    unittest.main()
