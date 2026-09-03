"""Silent local host for the scheduler-owned Jarvis heartbeat service."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time
from typing import Any, Mapping

from jarvis_control import JarvisControl

_RUNTIME_DIR = Path(__file__).resolve().parents[2] / "jarvis_runtime"
if str(_RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(_RUNTIME_DIR))

from jarvis_local_heartbeat import HeartbeatService, LocalHeartbeatConfig

from .mcp_wiring import build_jarvis_control


class JarvisControlFunctionRunner:
    """Run the bounded functions that a local heartbeat is allowed to schedule."""

    def __init__(self, control: JarvisControl) -> None:
        self._control = control

    def __call__(
        self, function_name: str, arguments: Mapping[str, Any], context: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        run_number = int(context.get("run_number") or 1)
        request_id = f"heartbeat:{context['heartbeat_id']}:{run_number}"
        source_ref = f"heartbeat:{context['heartbeat_id']}:{run_number}"
        if function_name == "JarvisControl.notify":
            return self._control.notify(
                request_id=request_id, source_ref=source_ref,
                message=str(arguments.get("message") or ""),
            )
        if function_name == "JarvisControl.resume":
            return self._control.resume(
                request_id=request_id, source_ref=source_ref,
                task_id=str(arguments.get("task_id") or ""), prompt=str(arguments.get("prompt") or ""),
                model=arguments.get("model"), reasoning_effort=arguments.get("reasoning_effort"),
                monitor_id=arguments.get("monitor_id"), max_turns=int(arguments.get("max_turns") or 1),
            )
        if function_name == "JarvisControl.monitor":
            return self._control.monitor(
                action=str(arguments.get("action") or "status"), request_id=request_id,
                source_ref=source_ref, monitor_id=str(arguments.get("monitor_id") or ""),
                observed_task_id=arguments.get("observed_task_id"),
                receipt_task_id=arguments.get("receipt_task_id"),
                resume_task_id=arguments.get("resume_task_id"), prompt=arguments.get("prompt"),
                model=arguments.get("model"), reasoning_effort=arguments.get("reasoning_effort"),
            )
        if function_name == "JarvisControl.loop_tick":
            return self._control.loop(action="tick", loop_id=str(arguments.get("loop_id") or ""))
        return {"status": "failed", "reason": f"unsupported heartbeat function: {function_name}"}

    def reconcile_terminal_holds(self) -> Mapping[str, Any]:
        """Reconcile terminal Holds, then pass their durable events to Jarvis Bridge."""
        lifecycle = self._control.loop(action="reconcile")
        notification_delivery = self._control.deliver_pending_hold_notifications(
            request_id="local-heartbeat:terminal-notification-drain",
            source_ref="local-heartbeat:terminal-notification-drain",
        )
        status = (
            "completed"
            if lifecycle.get("status") == "completed"
            and notification_delivery.get("status") == "completed"
            else str(notification_delivery.get("status") or lifecycle.get("status") or "failed")
        )
        return {
            "status": status,
            "lifecycle": lifecycle,
            "notification_delivery": notification_delivery,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True, help="existing App Server transport config")
    parser.add_argument("--local-heartbeat-config", type=Path, required=True)
    parser.add_argument("--launcher-config", type=Path, required=True)
    parser.add_argument("--notification-config", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("command", choices=("run-once", "run-forever", "health-check"))
    args = parser.parse_args()
    control = build_jarvis_control(
        args.config, args.state_dir, launcher_config_path=args.launcher_config,
        local_heartbeat_config_path=args.local_heartbeat_config,
        notification_config_path=args.notification_config,
    )
    runner = JarvisControlFunctionRunner(control)
    service = HeartbeatService(
        LocalHeartbeatConfig.load(args.local_heartbeat_config),
        function_runner=runner,
    )
    if args.command == "run-forever":
        try:
            while True:
                service.run_once()
                runner.reconcile_terminal_holds()
                time.sleep(service.config.poll_seconds)
        except KeyboardInterrupt:
            return 0
    payload = service.health() if args.command == "health-check" else service.run_once()
    if args.command == "run-once":
        payload["terminal_hold_reconciliation"] = runner.reconcile_terminal_holds()
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
