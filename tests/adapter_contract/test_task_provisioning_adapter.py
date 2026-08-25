from __future__ import annotations

import unittest

from adapters.codex_app_server.task_provisioning_adapter import (
    CodexAppServerTaskProvisioningAdapter,
)
from jarvis_control import TaskProvisionRequest


class FakeConfig:
    def resolve_project(self, project: str):
        self.project = project
        return project, "C:/test/project"


class FakeAppServerClient:
    def __init__(self):
        self.request_log = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def create_task(self, request):
        self.request_log.append(dict(request))
        return {
            "thread_id": "thread-created-1",
            "turn_id": "turn-created-1",
            "turn_status": "completed",
            "final_message": "hello complete",
        }


class TaskProvisioningAdapterContractTest(unittest.TestCase):
    def test_provisions_one_named_thread_and_returns_exact_creation_identity(self):
        config = FakeConfig()
        client = FakeAppServerClient()
        adapter = CodexAppServerTaskProvisioningAdapter(
            "unused.json", config_loader=lambda _: config, client_factory=lambda _: client
        )

        receipt = adapter.provision(TaskProvisionRequest(
            request_id="create-1",
            project="Jarvis4codex",
            title="TEST Worker",
            prompt="hello",
            source_ref="mcp:test",
        ))

        self.assertEqual(receipt.status, "completed")
        self.assertEqual(receipt.thread_id, "thread-created-1")
        self.assertEqual(receipt.turn_id, "turn-created-1")
        self.assertEqual(config.project, "Jarvis4codex")
        self.assertEqual(client.request_log[0]["project_path"], "C:/test/project")
        self.assertEqual(client.request_log[0]["title"], "TEST Worker")
