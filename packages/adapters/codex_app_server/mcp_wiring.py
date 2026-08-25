"""Concrete Codex App Server wiring for the local Jarvis MCP process."""

from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from jarvis_codex_bridge import ExistingThreadBridge, JarvisCapabilityPort, JsonlReceiptJournal, ThreadTerminalMonitor
from jarvis_control import JarvisControl

from .task_provisioning_adapter import CodexAppServerTaskProvisioningAdapter


def build_jarvis_control(
    config_path: Path,
    state_dir: Path,
    *,
    launcher_config_path: Path,
    transport_factory: Callable[[Path], Any] | None = None,
) -> JarvisControl:
    """Wire deployed existing-thread and task-provisioning adapters into JarvisControl."""
    transport = (transport_factory or _standard_transport)(config_path)
    state_dir.mkdir(parents=True, exist_ok=True)
    bridge = ExistingThreadBridge(transport, JsonlReceiptJournal(state_dir / "resume-receipts.jsonl"))
    capabilities = JarvisCapabilityPort(
        bridge,
        ThreadTerminalMonitor(transport, state_dir / "monitor-state.json"),
    )
    return JarvisControl(
        capabilities,
        bridge,
        provisioner=CodexAppServerTaskProvisioningAdapter(
            launcher_config_path,
            state_dir=state_dir,
        ),
    )


def _standard_transport(config_path: Path) -> Any:
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
    return StandardBridgeHeartbeatTransport(config, controller, load_standard_bridge(config))
