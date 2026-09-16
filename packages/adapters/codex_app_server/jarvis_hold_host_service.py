"""Normal-user Jarvis host that owns App Server hold processes.

This process is deliberately started outside the Codex MCP sandbox.  MCP writes
requests and reads receipts only; this host runs the existing hold implementation
under the normal Windows user that owns ``CODEX_HOME``.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
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
    try:
        temporary.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
        for attempt in range(20):
            try:
                temporary.replace(path)
                break
            except PermissionError:
                if attempt == 19:
                    raise
                time.sleep(0.05)
    finally:
        temporary.unlink(missing_ok=True)


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
    workers: int = 5,
    poll_seconds: float = 3.0,
    wait_seconds: float = 15.0,
    start_host: Callable[[], object] | None = None,
    now: Callable[[], datetime | str] = _now,
    pid_alive: Callable[[int], bool] | None = None,
    pid_started_at: Callable[[int], datetime | str | None] | None = None,
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
    health_checks = {"now": now, "pid_alive": pid_alive or _pid_is_alive, "pid_started_at": pid_started_at or _pid_started_at}
    existing_matches = _matching_health(state_dir, profile, codex_home, **health_checks)
    if existing_matches and _matching_health(state_dir, profile, codex_home, required_workers=workers, **health_checks):
        return {"status": "ready", "phase": "already_running"}

    lock_path = state_dir / "hold-host-bootstrap.lock"
    try:
        lock_path.mkdir(parents=True)
    except FileExistsError:
        return {"status": "blocked", "reason": "HoldHost bootstrap is already in progress"}
    try:
        existing_matches = _matching_health(state_dir, profile, codex_home, **health_checks)
        upgraded = False
        if existing_matches:
            if _matching_health(state_dir, profile, codex_home, required_workers=workers, **health_checks):
                return {"status": "ready", "phase": "already_running"}
            health = _read_json(state_dir / "hold-host.json") or {}
            try:
                active_count = int(health.get("active_count"))
            except (TypeError, ValueError):
                return {"status": "blocked", "reason": "HoldHost capacity upgrade requires an idle Host"}
            if active_count != 0:
                return {"status": "blocked", "reason": "HoldHost capacity upgrade requires an idle Host"}
            try:
                existing_pid = int(health["pid"])
            except (KeyError, TypeError, ValueError):
                return {"status": "failed", "reason": "HoldHost capacity upgrade cannot identify the existing Host"}
            _stop_hold_host(existing_pid)
            deadline = time.monotonic() + max(wait_seconds, 0)
            while health_checks["pid_alive"](existing_pid):
                if time.monotonic() >= deadline:
                    return {"status": "failed", "reason": "HoldHost capacity upgrade did not stop the existing Host"}
                time.sleep(max(poll_seconds, 0.05))
            upgraded = True
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
            if _matching_health(state_dir, profile, codex_home, required_workers=workers, **health_checks):
                return {"status": "ready", "phase": "capacity_upgraded" if upgraded else "started"}
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
        host_args = [
            "-m", "adapters.codex_app_server.jarvis_hold_host_service",
            "--state-dir", str(state_dir), "--launcher-config", str(launcher_config),
            "--poll-seconds", str(max(poll_seconds, 0.25)), "--workers", str(max(workers, 1)),
        ]
        quote = lambda value: "'" + str(value).replace("'", "''") + "'"
        command = (
            f"$jarvisHostArgs={quote(subprocess.list2cmdline(host_args))}; "
            f"Start-Process -FilePath {quote(sys.executable)} -ArgumentList $jarvisHostArgs "
            f"-WorkingDirectory {quote(package_root)} -WindowStyle Hidden"
        )
        return subprocess.Popen([
            "powershell.exe", "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden", "-Command", command,
        ], **kwargs)
    else:  # pragma: no cover - Windows user-host deployment is the supported path.
        kwargs["start_new_session"] = True
    return subprocess.Popen([
        sys.executable, "-m", "adapters.codex_app_server.jarvis_hold_host_service",
        "--state-dir", str(state_dir), "--launcher-config", str(launcher_config),
        "--poll-seconds", str(max(poll_seconds, 0.25)), "--workers", str(max(workers, 1)),
    ], **kwargs)


def _stop_hold_host(pid: int) -> None:
    os.kill(pid, signal.SIGTERM)


def _matching_health(
    state_dir: Path, profile: str, codex_home: str, *, now: Callable[[], datetime | str], pid_alive: Callable[[int], bool],
    pid_started_at: Callable[[int], datetime | str | None],
    required_workers: int = 1,
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
    try:
        worker_capacity = max(int(health.get("worker_capacity") or 1), 1)
    except (TypeError, ValueError):
        return False
    if worker_capacity < max(required_workers, 1):
        return False
    host_started_at = _parse_time(health.get("host_started_at"))
    process_started_at = _parse_time(pid_started_at(pid))
    if host_started_at is None or process_started_at is None or process_started_at - host_started_at > timedelta(seconds=5):
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
        try:
            exit_code = ctypes.c_ulong()
            if not ctypes.windll.kernel32.GetExitCodeProcess(process, ctypes.byref(exit_code)):
                return False
            return exit_code.value == 259
        finally:
            ctypes.windll.kernel32.CloseHandle(process)
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _pid_started_at(pid: int) -> datetime | None:
    if os.name != "nt":  # pragma: no cover - Windows user-host deployment is the supported path.
        return None
    import ctypes

    class FileTime(ctypes.Structure):
        _fields_ = [("dwLowDateTime", ctypes.c_ulong), ("dwHighDateTime", ctypes.c_ulong)]

    process = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
    if not process:
        return None
    try:
        created, exited, kernel, user = FileTime(), FileTime(), FileTime(), FileTime()
        if not ctypes.windll.kernel32.GetProcessTimes(process, ctypes.byref(created), ctypes.byref(exited), ctypes.byref(kernel), ctypes.byref(user)):
            return None
        ticks = (created.dwHighDateTime << 32) | created.dwLowDateTime
        return datetime(1601, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=ticks // 10)
    finally:
        ctypes.windll.kernel32.CloseHandle(process)


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

    def __init__(self, *, state_dir: Path, launcher_config: Path, workers: int = 5) -> None:
        self.state_dir = state_dir
        self.launcher_config = launcher_config
        launcher = _read_json(launcher_config) or {}
        self.profile = str(launcher.get("profile") or "").strip()
        self.codex_home = str(launcher.get("expected_codex_home") or "").strip()
        self.started_at = _now()
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
                    if self._requeue_dead_claim(root, request_path, ack_path, request, ack):
                        return True
                    continue
                if str(ack.get("phase") or "") != "queued_for_user_host":
                    if self._requeue_dead_claim(root, request_path, ack_path, request, ack):
                        return True
                    continue
                if not self._claim(root, ack_path, ack):
                    if self._requeue_dead_claim(root, request_path, ack_path, request, ack):
                        return True
                    continue
                hold_id = str(request.get("hold_id") or request.get("monitor_id") or root.name)
                self._mark_active(hold_id)
                self._write_health("holding", request_id=str(request.get("request_id") or ""))
                try:
                    hold_task(self.launcher_config, request_path, ack_path, result_path)
                except Exception as exc:
                    failed = {
                        **(_read_json(ack_path) or ack),
                        "status": "failed", "phase": "host_execution_error",
                        "reason": f"Hold execution failed: {exc}", "observed_at": _now(),
                    }
                    _write_json(result_path, failed)
                    _write_json(ack_path, failed)
                finally:
                    self._mark_inactive(hold_id)
                    self._release_claim(root)
                    self._write_health("ready")
                return True
        return False

    @staticmethod
    def _requeue_dead_claim(
        root: Path, request_path: Path, ack_path: Path, request: dict[str, Any], ack: dict[str, Any],
    ) -> bool:
        claim_dir = root / ".user-host-claim"
        owner = _read_json(claim_dir / "owner.json")
        try:
            owner_pid = int((owner or {}).get("pid"))
        except (TypeError, ValueError):
            return False
        thread_id = str(ack.get("thread_id") or request.get("thread_id") or "").strip()
        turn_id = str(ack.get("turn_id") or request.get("turn_id") or "").strip()
        if owner_pid <= 0 or _pid_is_alive(owner_pid):
            return False
        if not thread_id or not turn_id:
            if str(ack.get("phase") or "") not in {"claimed_by_user_host", "queued_for_user_host", "thread_starting"} or str(request.get("mode") or "create") != "create":
                return False
            JarvisHoldHost._release_claim(root)
            _write_json(ack_path, {
                "request_id": str(request.get("request_id") or ""),
                "status": "failed",
                "phase": "recovery_failed",
                "hold_id": str(request.get("hold_id") or request.get("monitor_id") or root.name),
                "recovered_from_pid": owner_pid,
                "reason": "dead HoldHost claim has no durable thread identity",
                "observed_at": _now(),
            })
            return True
        recovered = {
            **request,
            "mode": "recover",
            "thread_id": thread_id,
            "turn_id": turn_id,
            "initial_turn_count": max(int(ack.get("turn_count") or 1), 1),
            "initial_total_turn_count": max(int(ack.get("total_turn_count") or ack.get("turn_count") or 1), 1),
            "max_turns": max(int(ack.get("max_turns") or request.get("max_turns") or 1), 1),
        }
        JarvisHoldHost._release_claim(root)
        _write_json(request_path, recovered)
        _write_json(ack_path, {
            "request_id": str(request.get("request_id") or ""),
            "status": "accepted",
            "phase": "queued_for_user_host",
            "hold_id": str(request.get("hold_id") or request.get("monitor_id") or root.name),
            "recovered_from_pid": owner_pid,
            "observed_at": _now(),
        })
        return True

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

    def run_forever(self, *, poll_seconds: float, stop_event: threading.Event | None = None) -> None:
        stop = stop_event or threading.Event()
        heartbeat = threading.Thread(target=self._run_health_heartbeat, args=(stop, poll_seconds), daemon=True)
        threads = [threading.Thread(target=self._run_worker, args=(poll_seconds, stop), daemon=True) for _ in range(self.workers)]
        heartbeat.start()
        for worker in threads:
            worker.start()
        for worker in threads:
            worker.join()

    def _run_health_heartbeat(self, stop_event: threading.Event, poll_seconds: float) -> None:
        while not stop_event.is_set():
            self._write_health("ready")
            stop_event.wait(min(max(poll_seconds, 0.25), 5.0))

    def _run_worker(self, poll_seconds: float, stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            try:
                handled = self.run_once()
            except Exception:
                self._write_health("ready")
                handled = False
            if not handled:
                stop_event.wait(max(poll_seconds, 0.25))

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
                    "host_started_at": self.started_at,
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
    parser.add_argument("--workers", type=int, default=5)
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
