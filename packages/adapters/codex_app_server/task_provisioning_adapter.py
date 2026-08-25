"""App Server adapter for receipt-backed durable Jarvis task provisioning."""

from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from jarvis_control.provisioning import (
    TaskProvisionReceipt,
    TaskProvisionRequest,
    observed_now,
)


class CodexAppServerTaskProvisioningAdapter:
    """Resolve a configured project and create exactly one durable App Server thread."""

    name = "codex-app-server-provisioning"

    def __init__(
        self,
        config_path: str | Path,
        *,
        config_loader: Callable[[Path], Any] | None = None,
        client_factory: Callable[[Any], Any] | None = None,
    ) -> None:
        runtime = _runtime_symbols()
        self._config_path = Path(config_path)
        self._config_loader = config_loader or runtime["NativeTaskLauncherConfig"]
        self._client_factory = client_factory or runtime["AppServerClient"]

    def provision(self, request: TaskProvisionRequest) -> TaskProvisionReceipt:
        try:
            config = self._config_loader(self._config_path)
            project, project_path = config.resolve_project(request.project)
            app_request = {
                "request_id": request.request_id,
                "project": project,
                "project_path": str(project_path),
                "title": request.title,
                "prompt": request.prompt,
                "model": request.model,
                "reasoning_effort": request.reasoning_effort,
            }
            with self._client_factory(config) as client:
                created = client.create_task(app_request)
        except Exception as exc:
            return TaskProvisionReceipt(
                request_id=request.request_id,
                status="failed",
                observed_at=observed_now(),
                thread_id=getattr(exc, "thread_id", None),
                reason=str(exc),
            )

        thread_id = str(created.get("thread_id") or "").strip()
        turn_id = str(created.get("turn_id") or "").strip()
        output = str(created.get("final_message") or "").strip() or None
        if not thread_id or not turn_id or not output:
            return TaskProvisionReceipt(
                request_id=request.request_id,
                status="requires_readback",
                observed_at=observed_now(),
                thread_id=thread_id or None,
                turn_id=turn_id or None,
                reason="App Server create did not return an exact thread, turn, and final readback",
            )
        return TaskProvisionReceipt(
            request_id=request.request_id,
            status="completed",
            observed_at=observed_now(),
            thread_id=thread_id,
            turn_id=turn_id,
            output=output,
        )


def _runtime_symbols() -> dict[str, Any]:
    runtime_dir = Path(__file__).resolve().parents[2] / "jarvis_runtime"
    if str(runtime_dir) not in sys.path:
        sys.path.insert(0, str(runtime_dir))
    from jarvis_native_task_launcher import AppServerClient, NativeTaskLauncherConfig

    return {
        "AppServerClient": AppServerClient,
        "NativeTaskLauncherConfig": NativeTaskLauncherConfig,
    }
