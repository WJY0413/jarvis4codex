"""Silent local host for the scheduler-owned Jarvis heartbeat service."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

from jarvis_control import JarvisControl
from jarvis_local_heartbeat import HeartbeatService, LocalHeartbeatConfig

from .mcp_wiring import build_jarvis_control


class JarvisControlFunctionRunner:
    """Run the bounded functions that a local heartbeat is allowed to schedule."""

    def __init__(self, control: JarvisControl) -> None:
        self._control = control

    def __call__(
        self, function_name: str, arguments: Mapping[str, Any], context: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        request_id = f"heartbeat:{context['heartbeat_id']}"
        source_ref = f"heartbeat:{context['heartbeat_id']}"
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
        return {"status": "failed", "reason": f"unsupported heartbeat function: {function_name}"}


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
    service = HeartbeatService(
        LocalHeartbeatConfig.load(args.local_heartbeat_config),
        function_runner=JarvisControlFunctionRunner(control),
    )
    if args.command == "run-forever":
        return service.run_forever()
    payload = service.health() if args.command == "health-check" else service.run_once()
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
