"""Normal-user Jarvis host that owns App Server hold processes.

This process is deliberately started outside the Codex MCP sandbox.  MCP writes
requests and reads receipts only; this host runs the existing hold implementation
under the normal Windows user that owns ``CODEX_HOME``.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:  # Supports both package import and the deployed direct-script entry point.
    from .jarvis_task_hold_host import hold_task
except ImportError:  # pragma: no cover - direct script execution
    from jarvis_task_hold_host import hold_task


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    temporary.replace(path)


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    value = json.loads(path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else None


class JarvisHoldHost:
    """One small seam: consume accepted local requests and hold their App Server turns."""

    def __init__(self, *, state_dir: Path, launcher_config: Path) -> None:
        self.state_dir = state_dir
        self.launcher_config = launcher_config
        self.requests_root = state_dir / "task-monitors"
        self.health_path = state_dir / "hold-host.json"

    def run_once(self) -> bool:
        self._write_health("ready")
        if not self.requests_root.is_dir():
            return False
        for root in sorted(path for path in self.requests_root.iterdir() if path.is_dir()):
            request_path = root / "request.json"
            ack_path = root / "ack.json"
            result_path = root / "result.json"
            request = _read_json(request_path)
            ack = _read_json(ack_path)
            if request is None or ack is None or result_path.exists():
                continue
            if str(ack.get("status") or "") != "accepted":
                continue
            if str(ack.get("phase") or "") != "queued_for_user_host":
                continue
            _write_json(ack_path, {
                **ack,
                "phase": "claimed_by_user_host",
                "host_pid": os.getpid(),
                "observed_at": _now(),
            })
            self._write_health("holding", request_id=str(request.get("request_id") or ""))
            hold_task(self.launcher_config, request_path, ack_path, result_path)
            self._write_health("ready")
            return True
        return False

    def run_forever(self, *, poll_seconds: float) -> None:
        while True:
            handled = self.run_once()
            if not handled:
                time.sleep(max(poll_seconds, 0.25))

    def _write_health(self, status: str, *, request_id: str | None = None) -> None:
        _write_json(self.health_path, {
            "status": status,
            "pid": os.getpid(),
            "request_id": request_id,
            "observed_at": _now(),
        })


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--launcher-config", type=Path, required=True)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    args = parser.parse_args()
    JarvisHoldHost(
        state_dir=args.state_dir,
        launcher_config=args.launcher_config,
    ).run_forever(poll_seconds=args.poll_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
