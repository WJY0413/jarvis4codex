"""Concrete Codex App Server wiring for the local Jarvis MCP process."""

from __future__ import annotations

import sys
import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from jarvis_codex_bridge import ExistingThreadBridge, JarvisCapabilityPort, JsonlReceiptJournal, ThreadTerminalMonitor
from jarvis_control import JarvisControl, LoopController, LoopStore
from jarvis_codex_bridge.dispatch_owner import OwnedDispatchManager
from adapters.local_test_inbox import LocalTestInboxNotificationConfig, LocalTestInboxNotificationPort

from .task_provisioning_adapter import CodexAppServerTaskProvisioningAdapter
from adapters.feishu_outbox_notification import (
    FeishuOutboxNotificationConfig,
    FeishuOutboxNotificationPort,
)


def build_jarvis_control(
    config_path: Path,
    state_dir: Path,
    *,
    launcher_config_path: Path,
    local_heartbeat_config_path: Path | None = None,
    notification_config_path: Path | None = None,
    transport_factory: Callable[[Path], Any] | None = None,
) -> JarvisControl:
    """Wire deployed existing-thread and task-provisioning adapters into JarvisControl."""
    provisioner = CodexAppServerTaskProvisioningAdapter(launcher_config_path, state_dir=state_dir)
    transport = (transport_factory(config_path) if transport_factory else
                 _standard_transport(config_path, execution_reader=provisioner.thread_execution_evidence))
    state_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir = Path(__file__).resolve().parents[2] / "jarvis_runtime"
    if str(runtime_dir) not in sys.path:
        sys.path.insert(0, str(runtime_dir))
    from jarvis_local_heartbeat import JarvisControlHeartbeat

    bridge = ExistingThreadBridge(transport, JsonlReceiptJournal(state_dir / "resume-receipts.jsonl"))
    capabilities = JarvisCapabilityPort(
        bridge,
        ThreadTerminalMonitor(transport, state_dir / "monitor-state.json"),
        JarvisControlHeartbeat(local_heartbeat_config_path or config_path),
    )
    notifier = None
    if notification_config_path is not None:
        notification_raw = json.loads(notification_config_path.read_text(encoding="utf-8-sig"))
        if notification_raw.get("scope") == "TEST" and "storage_dir" in notification_raw:
            notifier = LocalTestInboxNotificationPort(LocalTestInboxNotificationConfig.load(
                notification_config_path.resolve(), workspace_root=state_dir.resolve().parent))
        else:
            notifier = FeishuOutboxNotificationPort(FeishuOutboxNotificationConfig.load(notification_config_path))
    dispatch = OwnedDispatchManager(bridge, state_dir / "dispatch-owners",
        transport_factory="adapters.codex_app_server.mcp_wiring:build_owned_dispatch_transport",
        transport_options={"config_path": str(config_path.resolve()), "launcher_config_path": str(launcher_config_path.resolve()),
                           "state_dir": str(state_dir.resolve())})
    control = JarvisControl(
        capabilities,
        bridge,
        provisioner=provisioner,
        loop_controller=LoopController(LoopStore(state_dir / "loops")),
        notifier=notifier,
        operation_root=state_dir / "operation-receipts",
        dispatch_manager=dispatch,
    )

    control._deployment_paths = {"transport": config_path.resolve(), "launcher": launcher_config_path.resolve()}
    if local_heartbeat_config_path is not None:
        control._deployment_paths["local_heartbeat"] = local_heartbeat_config_path.resolve()
    if notification_config_path is not None:
        control._deployment_paths["notification"] = notification_config_path.resolve()
    return control


def _standard_transport(config_path: Path, *, execution_reader: Callable[..., Any] | None = None) -> Any:
    runtime_dir = Path(__file__).resolve().parents[2] / "jarvis_runtime"
    if str(runtime_dir) not in sys.path:
        sys.path.insert(0, str(runtime_dir))
    from jarvis_heartbeat_service import (
        HeartbeatConfig,
        StandardBridgeHeartbeatTransport,
        WakeController,
        load_standard_bridge,
    )

    config = HeartbeatConfig.load(config_path)
    controller = WakeController(config)
    return StandardBridgeHeartbeatTransport(config, controller, load_standard_bridge(config),
                                            execution_reader=execution_reader)


def build_owned_dispatch_transport(options: dict[str, Any]) -> Any:
    """Fixed internal owner wiring; never chosen by a public request or model."""
    if set(options) != {"config_path", "launcher_config_path", "state_dir"}:
        raise ValueError("invalid owned dispatch transport options")
    provisioner = CodexAppServerTaskProvisioningAdapter(Path(options["launcher_config_path"]), state_dir=Path(options["state_dir"]))
    transport = _standard_transport(Path(options["config_path"]), execution_reader=provisioner.thread_execution_evidence)
    return _OwnedHoldDispatchTransport(transport, provisioner)


class _OwnedHoldDispatchTransport:
    """Keep owned dispatch on the same cancelled-thread/Holder pipe gates."""

    def __init__(self, transport, provisioner):
        self.transport, self.provisioner = transport, provisioner

    def read_thread(self, thread_id):
        return self.transport.read_thread(thread_id)

    def resume_existing(self, request):
        from jarvis_control.provisioning import TaskMonitorResumeRequest
        from jarvis_codex_bridge import StartedTurn
        receipt = self.provisioner.resume_with_monitor(TaskMonitorResumeRequest(
            request_id=request.request_id, task_id=request.thread_id, prompt=request.prompt,
            source_ref=request.source_ref, model=request.model, reasoning_effort=request.reasoning_effort,
            max_turns=1, auto_continue=False,
            notifications={"terminal": False, "milestones": []}))
        if receipt.status not in {"accepted", "holding", "running", "completed"}:
            raise RuntimeError(receipt.reason or "owned Holder dispatch was rejected")
        config = self.provisioner._config_loader(self.provisioner._config_path)
        deadline = time.monotonic() + config.turn_completion_timeout_seconds
        while time.monotonic() < deadline:
            data = self.provisioner.hold_status(receipt.hold_id)
            if data.get("terminal_confirmed") is True and data.get("hold_released") is True:
                if data.get("thread_id") != request.thread_id or not data.get("turn_id"):
                    raise RuntimeError("owned Holder terminal identity mismatch")
                native = self.transport.read_thread(request.thread_id)
                matches = [turn for turn in native.turns if turn.turn_id == data["turn_id"]]
                if (native.thread_id != request.thread_id or len(matches) != 1
                        or matches[0].status.lower() not in {"completed", "failed", "error", "interrupted", "cancelled", "canceled"}
                        or matches[0].effective_status.lower() != matches[0].status.lower()):
                    raise RuntimeError("owned Holder exact native terminal is unverified")
                return StartedTurn(request.thread_id, data["turn_id"], matches[0].status)
            if data.get("status") in {"failed", "blocked", "cancelled", "canceled"}:
                raise RuntimeError(data.get("reason") or "owned Holder outcome is unconfirmed")
            time.sleep(0.1)
        raise TimeoutError("owned Holder exact terminal readback timed out; no replacement")
