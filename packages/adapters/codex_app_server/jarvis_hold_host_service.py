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

from jarvis_runtime.coo_dispatcher_store import ProcessLock
from jarvis_runtime.jarvis_native_task_launcher import AppServerClient, NativeTaskLauncherConfig

try:  # Supports both package import and the deployed direct-script entry point.
    from .jarvis_task_hold_host import hold_task, _write_json
except ImportError:  # pragma: no cover - direct script execution
    from jarvis_task_hold_host import hold_task, _write_json


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    value = json.loads(path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else None


_HOST_HEALTH_MAX_AGE = timedelta(seconds=30)


def _controlled_stop_intent(request: dict[str, Any]) -> bool:
    # Intent must survive exhausted ack/result writes, including the legacy form.
    return request.get("host_stop") is not None or (
        request.get("mode") == "recover" and request.get("stop_requested") is True)


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
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.restype = ctypes.c_void_p
        kernel.GetExitCodeProcess.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_ulong)]
        kernel.CloseHandle.argtypes = [ctypes.c_void_p]
        process = kernel.OpenProcess(0x1000, False, pid)
        if not process:
            # Only ERROR_INVALID_PARAMETER proves this positive PID does not exist.
            return ctypes.get_last_error() != 87
        try:
            exit_code = ctypes.c_ulong()
            if not kernel.GetExitCodeProcess(process, ctypes.byref(exit_code)):
                return True
            return exit_code.value == 259
        finally:
            kernel.CloseHandle(process)
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
                if not self._claim(root, ack_path, ack, request):
                    if self._requeue_dead_claim(root, request_path, ack_path, request, ack):
                        return True
                    continue
                hold_id = str(request.get("hold_id") or request.get("monitor_id") or root.name)
                self._mark_active(hold_id)
                self._write_health("holding", request_id=str(request.get("request_id") or ""))
                holder_returned = False
                def controlled_intent() -> bool:
                    current = _read_json(request_path) or {}
                    if current.get("request_id") != request.get("request_id"):
                        return True  # Changed ownership is never permission to release.
                    return _controlled_stop_intent(request) or _controlled_stop_intent(current)
                try:
                    hold_task(self.launcher_config, request_path, ack_path, result_path)
                    holder_returned = True
                except Exception as exc:
                    # The holder may already have persisted exact identity/terminal
                    # evidence before its ack or close failed. Never overwrite it.
                    persisted = _read_json(result_path) or {}
                    failed = {
                        "request_id": request.get("request_id"),
                        "hold_id": hold_id,
                        "thread_id": request.get("thread_id", ""),
                        **(_read_json(ack_path) or ack),
                        **persisted,
                        "host_error": str(exc), "observed_at": _now(),
                        "terminal_confirmed": persisted.get("terminal_confirmed") is True,
                    }
                    if persisted.get("terminal_confirmed") is not True:
                        failed.update(status="failed", phase="host_execution_error",
                                      reason=persisted.get("reason") or f"Hold execution failed: {exc}")
                    if controlled_intent():
                        failed.update(host_stop=True, execution_evidence="requires_owner_terminal",
                                      terminal_confirmed=False)
                    _write_json(result_path, failed)
                    _write_json(ack_path, failed)
                finally:
                    self._mark_inactive(hold_id)
                    cleanup_owner = _read_json(root / ".user-host-claim" / "owner.json")
                    try:
                        saved = _read_json(result_path) or {}
                        controlled = saved.get("host_stop") is True or (not holder_returned and controlled_intent())
                        if controlled:
                            saved = {**saved, "host_stop": True, "holder_exit_confirmed": holder_returned,
                                     "holder_owner_pid": os.getpid()}
                            if not holder_returned:
                                saved.update(terminal_confirmed=False, execution_evidence="requires_owner_terminal")
                            evidence = saved.get("owner_terminal_readback") or {}
                            if (holder_returned and saved.get("holder_client_exit_confirmed") is True
                                    and evidence.get("thread_id") == saved.get("thread_id")
                                    and evidence.get("turn_id") == saved.get("turn_id")
                                    and evidence.get("status") in {"completed", "failed", "interrupted", "cancelled", "canceled"}):
                                saved.update(terminal_confirmed=True, phase="host_stop_reconciled",
                                             terminal_evidence="holder_readback_and_owned_exit")
                            _write_json(result_path, saved)
                            _write_json(ack_path, saved)
                        if not controlled or (saved.get("terminal_confirmed") is True
                                              and holder_returned and saved.get("holder_client_exit_confirmed") is True):
                            self._release_claim(root)
                    except OSError as exc:
                        saved = _read_json(result_path)
                        if saved is not None:
                            failed_cleanup = {**saved, "cleanup_error": str(exc), "cleanup_owner": cleanup_owner}
                            _write_json(result_path, failed_cleanup)
                            if saved.get("host_stop"):
                                _write_json(ack_path, failed_cleanup)
                    finally:
                        self._write_health("ready")
                return True
        return False

    @staticmethod
    def _requeue_dead_claim(
        root: Path, request_path: Path, ack_path: Path, request: dict[str, Any], ack: dict[str, Any],
    ) -> bool:
        try:
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
                _write_json(ack_path, {
                    "request_id": str(request.get("request_id") or ""),
                    "status": "failed",
                    "phase": "recovery_failed",
                    "hold_id": str(request.get("hold_id") or request.get("monitor_id") or root.name),
                    "recovered_from_pid": owner_pid,
                    "reason": "dead HoldHost claim has no durable thread identity",
                    "observed_at": _now(),
                })
                JarvisHoldHost._release_claim(root)
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
            with ProcessLock(request_path.with_suffix(".lock"), timeout_seconds=0.1, owner_alive=_pid_is_alive):
                if (_read_json(claim_dir / "owner.json") or {}).get("pid") != owner_pid:
                    return False
                current = _read_json(request_path) or {}
                if current.get("request_id") != request.get("request_id"):
                    return False
                _write_json(request_path, {**recovered, **({"stop_requested": True} if current.get("stop_requested") else {})})
                _write_json(ack_path, {
                    "request_id": str(request.get("request_id") or ""),
                    "status": "accepted",
                    "phase": "queued_for_user_host",
                    "hold_id": str(request.get("hold_id") or request.get("monitor_id") or root.name),
                    "recovered_from_pid": owner_pid,
                    "observed_at": _now(),
                })
                JarvisHoldHost._release_claim(root)
            return True
        except (OSError, ValueError, RuntimeError):
            # Leave the dead claim for a later retry and let this Host scan other work.
            return False

    @staticmethod
    def _claim(root: Path, ack_path: Path, ack: dict[str, Any], request: dict[str, Any] | None = None) -> bool:
        claim_dir = root / ".user-host-claim"
        try:
            claim_dir.mkdir()
        except FileExistsError:
            return False
        claimed = False
        try:
            _write_json(claim_dir / "owner.json", {"pid": os.getpid(), "observed_at": _now()})
            current = _read_json(root / "request.json")
            current_ack = _read_json(ack_path) or {}
            expected = request if request is not None else current
            # A delayed scanner may acquire the directory after another worker finished.
            # Recheck before replacing the acknowledgement, not only before mkdir.
            if (current is None or expected is None or (root / "result.json").exists()
                    or current.get("request_id") != expected.get("request_id")
                    or current.get("hold_id") != expected.get("hold_id")
                    or current_ack.get("request_id", current.get("request_id")) != current.get("request_id")
                    or current_ack.get("status") != "accepted"
                    or current_ack.get("phase") != "queued_for_user_host"):
                return False
            _write_json(ack_path, {
                **current_ack,
                "phase": "claimed_by_user_host",
                "host_pid": os.getpid(),
                "observed_at": _now(),
            })
            claimed = True
            return True
        finally:
            if not claimed:
                JarvisHoldHost._release_claim(root)

    @staticmethod
    def _release_claim(root: Path) -> None:
        claim_dir = root / ".user-host-claim"
        owner = claim_dir / "owner.json"
        for attempt in range(20):
            try:
                owner.unlink(missing_ok=True)
                if claim_dir.is_dir():
                    claim_dir.rmdir()
                return
            except PermissionError:
                # Concurrent Windows readers can briefly deny deletion, as with JSON replace.
                if attempt == 19:
                    raise
                time.sleep(0.05)

    def request_host_stop(self, hold_id: str, thread_id: str, turn_id: str) -> dict[str, Any]:
        """Ask the owning Host to stop one exact holder; acceptance is not release."""
        rejected = {"status": "rejected", "hold_id": hold_id, "hold_released": False}
        matches = []
        for parent in self.requests_roots:
            if parent.is_dir():
                for root in parent.iterdir():
                    if root.is_dir() and ((_read_json(root / "request.json") or {}).get("hold_id") == hold_id):
                        matches.append(root)
        if len(matches) != 1 or not thread_id or not turn_id:
            return {**rejected, "reason": "Exact Hold/thread/turn required"}
        root = matches[0]
        try:
            with ProcessLock((root / "request.json").with_suffix(".lock"), timeout_seconds=0.1, owner_alive=_pid_is_alive):
                request = _read_json(root / "request.json") or {}
                ack = _read_json(root / "ack.json") or {}
                result = _read_json(root / "result.json") or {}
                identity = {"hold_id": hold_id, "request_id": request.get("request_id"),
                            "thread_id": thread_id, "turn_id": turn_id}
                if (not identity["request_id"] or request.get("hold_id") != hold_id
                        or any(ack.get(key) != value for key, value in identity.items())
                        or (result and any(result.get(key) != value for key, value in identity.items()))):
                    return {**rejected, "reason": "Current owner receipt identity mismatch"}
                if request.get("host_stop") not in (None, identity):
                    return {**rejected, "reason": "Conflicting existing host stop binding"}
                confirmed_repeat = (request.get("host_stop") == identity
                    and result.get("status") in {"completed", "failed", "interrupted", "cancelled", "canceled"}
                    and ack.get("status") == result.get("status")
                    and all(value.get(key) is True for value in (ack, result)
                            for key in ("host_stop", "terminal_confirmed", "holder_exit_confirmed", "holder_client_exit_confirmed"))
                    and all((value.get("owner_terminal_readback") or {}).get("thread_id") == thread_id
                            and (value.get("owner_terminal_readback") or {}).get("turn_id") == turn_id
                            for value in (ack, result)))
                if request.get("mode") == "recover" and (
                        request.get("thread_id") != thread_id
                        or (request.get("turn_id") != turn_id and not confirmed_repeat)):
                    return {**rejected, "reason": "Recover request/receipt identity conflict"}
                if result:
                    return {**result, "hold_released": (result.get("terminal_confirmed") is True
                            and not (root / ".user-host-claim").exists())}
                health = _read_json(self.health_path) or {}
                owner = _read_json(root / ".user-host-claim" / "owner.json") or {}
                if ("exact_hold_stop_v1" not in (health.get("capabilities") or [])
                        or health.get("pid") != owner.get("pid")
                        or hold_id not in (health.get("active_hold_ids") or [])
                        or not _matching_health(self.state_dir, self.profile, self.codex_home, now=_now,
                                                pid_alive=_pid_is_alive, pid_started_at=_pid_started_at)):
                    return {**rejected, "reason": "Owning Host does not advertise fresh exact-stop capability; no hot update"}
                if request.get("host_stop") is None:
                    _write_json(root / "request.json", {**request, "stop_requested": True, "host_stop": identity})
                return {"status": "stop_requested", **identity, "terminal_confirmed": False, "hold_released": False}
        except (OSError, ValueError, RuntimeError) as exc:
            return {**rejected, "reason": str(exc)}

    def reconcile_hold(self, hold_id: str) -> dict[str, Any]:
        """Read an exact orphaned turn and retry cleanup; never dispatch work.

        Explicitly scoped so a shared live Host need not be restarted. Old
        receipts without a durable turn remain unknown, even when its PID died.
        """
        unknown = {"status": "requires_readback", "hold_id": hold_id,
                   "terminal_confirmed": False, "hold_released": False}
        matches = []
        for parent in self.requests_roots:
            if parent.is_dir():
                for root in parent.iterdir():
                    if root.is_dir():
                        request = _read_json(root / "request.json") or {}
                        if (request.get("hold_id") or request.get("monitor_id")) == hold_id:
                            matches.append(root)
        if len(matches) != 1:
            return {**unknown, "reason": "Hold identity missing or ambiguous"}
        root = matches[0]
        request_path, ack_path, result_path = (root / name for name in ("request.json", "ack.json", "result.json"))
        try:
            with ProcessLock(request_path.with_suffix(".lock"), timeout_seconds=0.1, owner_alive=_pid_is_alive):
                request = _read_json(request_path) or {}
                ack, result = _read_json(ack_path) or {}, _read_json(result_path) or {}
                if not result:
                    return {**unknown, "reason": "No exception/terminal result; live execution not reconciled"}
                if _controlled_stop_intent(request) and not (result.get("host_stop") is True
                        and result.get("holder_exit_confirmed") is True
                        and result.get("holder_client_exit_confirmed") is True
                        and result.get("terminal_confirmed") is True):
                    return {**unknown, "reason": "Controlled request lacks owned terminal/exit proof; receipt fallback is not legacy evidence"}
                if result.get("execution_evidence") == "requires_owner_terminal":
                    return {**unknown, "holder_exit_confirmed": result.get("holder_exit_confirmed", False),
                            "reason": "Holder exited but execution remains unknown; external interrupted is not owner terminal evidence"}
                if result.get("host_stop") and not (result.get("holder_exit_confirmed") is True
                                                      and result.get("holder_client_exit_confirmed") is True):
                    return {**unknown, "reason": "Host-owned exit not yet confirmed"}
                request_id = request.get("request_id")
                receipts = (ack, result)
                if (not request_id or (request.get("hold_id") or request.get("monitor_id")) != hold_id
                        or any(value.get("request_id") != request_id for value in receipts)
                        or any(value.get("hold_id", hold_id) != hold_id for value in receipts)):
                    return {**unknown, "reason": "Current request/receipt identity mismatch"}
                terminal_statuses = {"completed", "failed", "interrupted", "cancelled", "canceled", "blocked", "turn_limit_reached"}
                claim = root / ".user-host-claim"
                if result.get("terminal_confirmed") is True and result.get("status") in terminal_statuses and not claim.exists():
                    return {**result, "hold_released": True}
                identities = {}
                for key in ("thread_id", "turn_id"):
                    # request/ack may name the previous terminal turn when a
                    # continuation failed before its new identity was returned.
                    if not result.get(key):
                        return {**unknown, "reason": f"Exact result {key} missing; never reuse a prior turn"}
                    values = {str(value[key]) for value in (request, ack, result) if value.get(key)}
                    if len(values) != 1:
                        return {**unknown, "reason": f"Exact {key} missing or conflicting; never guess latest turn"}
                    identities[key] = values.pop()
                owner = _read_json(claim / "owner.json") or result.get("cleanup_owner") or {}
                if claim.exists():
                    pid = owner.get("pid")
                    empty_terminal_claim = not any(claim.iterdir()) and result.get("terminal_confirmed") is True
                    if (not isinstance(pid, int) or pid <= 0) and not empty_terminal_claim:
                        return {**unknown, "reason": "Claim owner identity unavailable"}
                    if not empty_terminal_claim and _pid_is_alive(pid):
                        health = _read_json(self.health_path) or {}
                        if (health.get("pid") != pid or not _matching_health(
                                self.state_dir, self.profile, self.codex_home, now=_now,
                                pid_alive=_pid_is_alive, pid_started_at=_pid_started_at)
                                or not isinstance(health.get("active_hold_ids"), list)
                                or hold_id in health["active_hold_ids"]
                                or _parse_time(health.get("observed_at")) is None
                                or _parse_time(result.get("observed_at")) is None
                                or _parse_time(health["observed_at"]) < _parse_time(result["observed_at"])):
                            return {**unknown, "reason": "Claim owner is active or inactivity is unproven"}
                client = AppServerClient(NativeTaskLauncherConfig(self.launcher_config))
                try:
                    client.start()
                    readback = client.request("thread/read", {"threadId": identities["thread_id"], "includeTurns": True})
                finally:
                    client.close()
                thread = readback.get("thread") or {}
                turns = [turn for turn in thread.get("turns", []) if isinstance(turn, dict) and turn.get("id") == identities["turn_id"]]
                if thread.get("id") != identities["thread_id"] or len(turns) != 1 or turns[0].get("status") not in {"completed", "failed", "interrupted", "cancelled", "canceled"}:
                    return {**unknown, **identities, "reason": "Exact turn has no authoritative terminal readback"}
                # Preserve execution failure/business uncertainty rather than
                # turning a transport terminal into business success.
                reconciled = {**result, **identities, "terminal_confirmed": True,
                              "status": result.get("status") if result.get("status") in terminal_statuses else "failed",
                              "recovery": {"source": "app_server_thread_read", "turn_status": turns[0]["status"],
                                           "observed_at": _now()}}
                reconciled.pop("cleanup_error", None)
                reconciled.pop("cleanup_owner", None)
                _write_json(result_path, reconciled)
                _write_json(ack_path, {**ack, **reconciled})
                try:
                    self._release_claim(root)
                except OSError as exc:
                    reconciled = {**reconciled, "cleanup_error": str(exc)}
                    _write_json(result_path, reconciled)
                    return {**reconciled, "hold_released": False}
                return {**reconciled, "hold_released": not claim.exists()}
        except (OSError, ValueError, RuntimeError) as exc:
            return {**unknown, "reason": f"Reconciliation incomplete: {exc}"}

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
                    "capabilities": ["exact_hold_stop_v1"],
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
    parser.add_argument("--reconcile-hold", help="read exact terminal evidence and retry cleanup for one Hold; never start/resume a turn")
    parser.add_argument("--stop-hold", help="ask the owning capable Host to stop one exact holder; not a release receipt")
    parser.add_argument("--thread-id")
    parser.add_argument("--turn-id")
    args = parser.parse_args()
    if (args.thread_id or args.turn_id) and not args.stop_hold:
        parser.error("--thread-id/--turn-id require --stop-hold")
    if args.stop_hold:
        if not args.thread_id or not args.turn_id or args.reconcile_hold or args.initialize_user_host:
            parser.error("--stop-hold requires --thread-id and --turn-id and cannot initialize/reconcile")
        receipt = JarvisHoldHost(state_dir=args.state_dir, launcher_config=args.launcher_config).request_host_stop(
            args.stop_hold, args.thread_id, args.turn_id)
        print(json.dumps(receipt, ensure_ascii=False))
        return 0 if receipt.get("status") == "stop_requested" or receipt.get("hold_released") else 1
    if args.reconcile_hold:
        receipt = JarvisHoldHost(state_dir=args.state_dir, launcher_config=args.launcher_config).reconcile_hold(args.reconcile_hold)
        print(json.dumps(receipt, ensure_ascii=False))
        return 0 if receipt.get("hold_released") else 1
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
