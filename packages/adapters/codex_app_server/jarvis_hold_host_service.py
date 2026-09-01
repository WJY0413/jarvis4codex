"""Normal-user Jarvis host that owns App Server hold processes.

This process is deliberately started outside the Codex MCP sandbox.  MCP writes
requests and reads receipts only; this host runs the existing hold implementation
under the normal Windows user that owns ``CODEX_HOME``.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

try:  # Supports both package import and the deployed direct-script entry point.
    from .jarvis_task_hold_host import hold_task
except ImportError:  # pragma: no cover - direct script execution
    from jarvis_task_hold_host import hold_task


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.{threading.get_ident()}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    temporary.replace(path)


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    value = json.loads(path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else None


_HOST_HEALTH_MAX_AGE = timedelta(seconds=30)


def initialize_user_host(
    *,
    state_dir: Path,
    launcher_config: Path,
    workers: int = 1,
    poll_seconds: float = 3.0,
    wait_seconds: float = 15.0,
    start_host: Callable[[], object] | None = None,
    now: Callable[[], datetime | str] = _now,
    pid_alive: Callable[[int], bool] | None = None,
) -> dict[str, str]:
    """Start one normal-user Hold Host only when its matching health is absent or stale.

    This entry is intended for a normal-user bootstrapper, never for MCP.  It
    returns only after a matching Host health record is read back.
    """
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "task-holds").mkdir(exist_ok=True)
    (state_dir / "task-monitors").mkdir(exist_ok=True)
    launcher = _read_json(launcher_config)
    if launcher is None:
        return {"status": "failed", "reason": "HoldHost launcher config is unreadable"}
    profile = str(launcher.get("profile") or "").strip()
    codex_home = str(launcher.get("expected_codex_home") or "").strip()
    if not profile or not codex_home:
        return {"status": "failed", "reason": "HoldHost launcher config requires profile and expected_codex_home"}
    if _matching_health(state_dir, profile, codex_home, now=now, pid_alive=pid_alive or _pid_is_alive):
        return {"status": "ready", "phase": "already_running"}

    lock_path = state_dir / "hold-host-bootstrap.lock"
    try:
        lock_path.mkdir(parents=True)
    except FileExistsError:
        return {"status": "blocked", "reason": "HoldHost bootstrap is already in progress"}
    try:
        if _matching_health(state_dir, profile, codex_home, now=now, pid_alive=pid_alive or _pid_is_alive):
            return {"status": "ready", "phase": "already_running"}
        if start_host is None:
            _start_hold_host(
                state_dir=state_dir,
                launcher_config=launcher_config,
                workers=workers,
                poll_seconds=poll_seconds,
            )
        else:
            start_host()
        deadline = time.monotonic() + max(wait_seconds, 0)
        while True:
            if _matching_health(state_dir, profile, codex_home, now=now, pid_alive=pid_alive or _pid_is_alive):
                return {"status": "ready", "phase": "started"}
            if time.monotonic() >= deadline:
                return {"status": "failed", "reason": "HoldHost did not report matching health before timeout"}
            time.sleep(max(poll_seconds, 0.05))
    finally:
        try:
            lock_path.rmdir()
        except OSError:
            pass


def ensure_hold_host(**kwargs: Any) -> dict[str, str]:
    """Compatibility alias for callers that use the former recovery-only name."""
    return initialize_user_host(**kwargs)


def _start_hold_host(*, state_dir: Path, launcher_config: Path, workers: int, poll_seconds: float) -> subprocess.Popen[str]:
    package_root = Path(__file__).resolve().parents[2]
    kwargs: dict[str, Any] = {
        "cwd": str(package_root),
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "text": True,
        "shell": False,
    }
    if os.name == "nt":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    else:  # pragma: no cover - Windows user-host deployment is the supported path.
        kwargs["start_new_session"] = True
    return subprocess.Popen([
        sys.executable, "-m", "adapters.codex_app_server.jarvis_hold_host_service",
        "--state-dir", str(state_dir), "--launcher-config", str(launcher_config),
        "--poll-seconds", str(max(poll_seconds, 0.25)), "--workers", str(max(workers, 1)),
    ], **kwargs)


def _matching_health(
    state_dir: Path, profile: str, codex_home: str, *, now: Callable[[], datetime | str], pid_alive: Callable[[int], bool]
) -> bool:
    health = _read_json(state_dir / "hold-host.json")
    if health is None or str(health.get("status") or "") not in {"ready", "holding"}:
        return False
    observed_at = _parse_time(health.get("observed_at"))
    current = _parse_time(now())
    if observed_at is None or current is None or current - observed_at > _HOST_HEALTH_MAX_AGE:
        return False
    try:
        pid = int(health.get("pid"))
    except (TypeError, ValueError):
        return False
    if pid <= 0 or not pid_alive(pid):
        return False
    return (
        str(health.get("profile") or "").strip() == profile
        and _same_path(health.get("codex_home"), codex_home)
        and _same_path(health.get("state_dir"), state_dir)
    )


def _pid_is_alive(pid: int) -> bool:
    if os.name == "nt":
        import ctypes

        process = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
        if not process:
            return False
        ctypes.windll.kernel32.CloseHandle(process)
        return True
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _parse_time(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _same_path(left: object, right: object) -> bool:
    try:
        return os.path.normcase(str(Path(str(left)).resolve())) == os.path.normcase(str(Path(str(right)).resolve()))
    except OSError:
        return False


class JarvisHoldHost:
    """One small seam: consume accepted local requests and hold their App Server turns."""

    def __init__(self, *, state_dir: Path, launcher_config: Path, workers: int = 1) -> None:
        self.state_dir = state_dir
        self.launcher_config = launcher_config
        launcher = _read_json(launcher_config) or {}
        self.profile = str(launcher.get("profile") or "").strip()
        self.codex_home = str(launcher.get("expected_codex_home") or "").strip()
        self.requests_roots = (state_dir / "task-holds", state_dir / "task-monitors")
        self.health_path = state_dir / "hold-host.json"
        self.workers = max(int(workers), 1)
        self._active_hold_ids: set[str] = set()
        self._health_lock = threading.Lock()

    def run_once(self) -> bool:
        self._write_health("ready")
        for requests_root in self.requests_roots:
            if not requests_root.is_dir():
                continue
            for root in sorted(path for path in requests_root.iterdir() if path.is_dir()):
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
                if not self._claim(root, ack_path, ack):
                    continue
                hold_id = str(request.get("hold_id") or request.get("monitor_id") or root.name)
                self._mark_active(hold_id)
                self._write_health("holding", request_id=str(request.get("request_id") or ""))
                try:
                    hold_task(self.launcher_config, request_path, ack_path, result_path)
                finally:
                    self._mark_inactive(hold_id)
                    self._release_claim(root)
                    self._write_health("ready")
                return True
        return False

    @staticmethod
    def _claim(root: Path, ack_path: Path, ack: dict[str, Any]) -> bool:
        claim_dir = root / ".user-host-claim"
        try:
            claim_dir.mkdir()
        except FileExistsError:
            return False
        _write_json(claim_dir / "owner.json", {"pid": os.getpid(), "observed_at": _now()})
        _write_json(ack_path, {
            **ack,
            "phase": "claimed_by_user_host",
            "host_pid": os.getpid(),
            "observed_at": _now(),
        })
        return True

    @staticmethod
    def _release_claim(root: Path) -> None:
        claim_dir = root / ".user-host-claim"
        owner = claim_dir / "owner.json"
        if owner.is_file():
            owner.unlink()
        if claim_dir.is_dir():
            claim_dir.rmdir()

    def run_forever(self, *, poll_seconds: float) -> None:
        threads = [threading.Thread(target=self._run_worker, args=(poll_seconds,), daemon=True) for _ in range(self.workers)]
        for worker in threads:
            worker.start()
        for worker in threads:
            worker.join()

    def _run_worker(self, poll_seconds: float) -> None:
        while True:
            if not self.run_once():
                time.sleep(max(poll_seconds, 0.25))

    def _mark_active(self, hold_id: str) -> None:
        with self._health_lock:
            self._active_hold_ids.add(hold_id)

    def _mark_inactive(self, hold_id: str) -> None:
        with self._health_lock:
            self._active_hold_ids.discard(hold_id)

    def _write_health(self, status: str, *, request_id: str | None = None) -> None:
        with self._health_lock:
            active_hold_ids = sorted(self._active_hold_ids)
            try:
                _write_json(self.health_path, {
                    "profile": self.profile,
                    "codex_home": self.codex_home,
                    "state_dir": str(self.state_dir.resolve()),
                    "status": "holding" if active_hold_ids else status,
                    "pid": os.getpid(),
                    "request_id": request_id,
                    "worker_capacity": self.workers,
                    "active_count": len(active_hold_ids),
                    "active_hold_ids": active_hold_ids,
                    "observed_at": _now(),
                })
            except PermissionError:
                pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--launcher-config", type=Path, required=True)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--initialize-user-host", "--ensure-running", dest="initialize_user_host", action="store_true", help="initialize profile-bound state and start one matching Hold Host only when needed")
    parser.add_argument("--wait-seconds", type=float, default=15.0)
    args = parser.parse_args()
    if args.initialize_user_host:
        receipt = initialize_user_host(
            state_dir=args.state_dir,
            launcher_config=args.launcher_config,
            workers=args.workers,
            poll_seconds=args.poll_seconds,
            wait_seconds=args.wait_seconds,
        )
        print(json.dumps(receipt, ensure_ascii=False))
        return 0 if receipt.get("status") == "ready" else 1
    JarvisHoldHost(
        state_dir=args.state_dir,
        launcher_config=args.launcher_config,
        workers=args.workers,
    ).run_forever(poll_seconds=args.poll_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
