"""Normal-user Jarvis host that owns App Server hold processes.

This process is deliberately started outside the Codex MCP sandbox.  MCP writes
requests and reads receipts only; this host runs the existing hold implementation
under the normal Windows user that owns ``CODEX_HOME``.
"""

from __future__ import annotations
from jarvis_schema import MAX_CONCURRENCY

import argparse
import errno
import hashlib
from contextlib import nullcontext
from jarvis_runtime.cancellation import generation_guard, GenerationRetired
import json
import os
import queue
import signal
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from jarvis_runtime.coo_dispatcher_store import ProcessLock
from jarvis_runtime.cancellation import cancellation_path
from jarvis_runtime.jarvis_native_task_launcher import AppServerClient, NativeTaskLauncherConfig

try:  # Supports both package import and the deployed direct-script entry point.
    from .jarvis_task_hold_host import hold_task, _write_json, _publish_generation
except ImportError:  # pragma: no cover - direct script execution
    from jarvis_task_hold_host import hold_task, _write_json, _publish_generation


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
    if type(workers) is not int or not 1 <= workers <= MAX_CONCURRENCY:
        return {"status": "rejected", "error_code": "invalid_capacity_request"}
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "task-holds").mkdir(exist_ok=True)
    (state_dir / "task-monitors").mkdir(exist_ok=True)
    launcher = _read_json(launcher_config)
    if launcher is None:
        return {"status": "failed", "reason": "HoldHost launcher config is unreadable"}
    try:
        parsed_launcher = NativeTaskLauncherConfig(launcher_config)
    except Exception as exc:
        return {"status": "failed", "reason": f"HoldHost launcher config is invalid: {exc}"}
    profile = parsed_launcher.profile
    codex_home = parsed_launcher.expected_codex_home
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
            instance = str(health.get("host_instance_id") or "")
            return request_hold_host_capacity(state_dir=state_dir, launcher_config=launcher_config,
                host_instance_id=instance, request_id=f"capacity:{instance}:{workers}", target_capacity=workers,
                wait_seconds=wait_seconds, poll_seconds=poll_seconds, **health_checks)
        from .task_provisioning_adapter import physical_memory_admission
        admission = physical_memory_admission(parsed_launcher)
        if admission["status"] != "allowed":
            return {"status": "blocked", "error_code": admission["error_code"],
                    "reason": admission["reason"], "memory_admission": admission}
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


def request_hold_host_capacity(*, state_dir: Path, launcher_config: Path, host_instance_id: str,
    request_id: str, target_capacity: int, expected_revision: int | None = None, wait_seconds: float = 15,
    poll_seconds: float = .05, now: Callable[[], datetime | str] = _now,
    pid_alive: Callable[[int], bool] | None = None, pid_started_at: Callable[[int], datetime | str | None] | None = None) -> dict[str, Any]:
    """Grow an existing capable instance only; this endpoint never bootstraps/restarts."""
    if type(target_capacity) is not int or not 1 <= target_capacity <= MAX_CONCURRENCY or not request_id or not host_instance_id:
        return {"status": "rejected", "error_code": "invalid_capacity_request"}
    try:
        health = _read_json(state_dir / "hold-host.json") or {}
        config = NativeTaskLauncherConfig(launcher_config)
        checks = {"now": now, "pid_alive": pid_alive or _pid_is_alive, "pid_started_at": pid_started_at or _pid_started_at}
        matching = _matching_health(state_dir, config.profile, config.expected_codex_home, **checks)
    except (OSError, ValueError, RuntimeError):
        matching = False
    if not matching or health.get("host_instance_id") != host_instance_id:
        return {"status": "requires_readback", "error_code": "host_identity_unverified"}
    if "hot_capacity_growth_v1" not in health.get("capabilities", []):
        return {"status": "unsupported", "error_code": "hot_capacity_unsupported"}
    receipt_path = state_dir / "hold-host-capacity-results" / (hashlib.sha256(request_id.encode()).hexdigest() + ".json")
    try:
        saved = _read_json(receipt_path)
        prior = _read_json(state_dir / "hold-host-capacity-request.json") or {}
    except (OSError, ValueError):
        return {"status": "requires_readback", "error_code": "capacity_receipt_unreadable"}
    declaration = saved.get("declaration") if saved else prior if prior.get("request_id") == request_id else None
    revision = (declaration.get("expected_revision") if isinstance(declaration, dict) else health.get("capacity_revision")) if expected_revision is None else expected_revision
    if type(revision) is not int or revision < 0:
        return {"status": "requires_readback", "error_code": "capacity_revision_unverified"}
    command = {"request_id": request_id, "host_instance_id": host_instance_id,
               "target_capacity": target_capacity, "expected_revision": revision}
    if declaration is not None and declaration != command:
        return {"status": "rejected", "error_code": "capacity_request_conflict"}
    if saved:
        ack = saved.get("result")
        if not isinstance(ack, dict) or ack.get("host_instance_id") != host_instance_id or ack.get("target_capacity") != target_capacity:
            return {"status": "requires_readback", "error_code": "capacity_receipt_unverified"}
        if ack.get("status") != "applied":
            return ack
        applied, current_revision = health.get("applied_capacity"), health.get("capacity_revision")
        ack_revision = ack.get("capacity_revision")
        if type(applied) is int and type(current_revision) is int and type(ack_revision) is int:
            if applied == target_capacity and current_revision == ack_revision:
                return {**ack, "status": "ready", "phase": "capacity_expanded"}
            if applied >= target_capacity and current_revision > ack_revision:
                return {**ack, "status": "historical_readback", "phase": "capacity_operation_completed",
                        "current_applied_capacity": applied, "current_capacity_revision": current_revision}
        return {"status": "requires_readback", "error_code": "capacity_ack_unverified"}
    if target_capacity > (health.get("applied_capacity") or health.get("worker_capacity") or 1):
        from .task_provisioning_adapter import physical_memory_admission
        admission = physical_memory_admission(config)
        if admission["status"] != "allowed":
            return {"status": "blocked", "error_code": admission["error_code"],
                    "reason": admission["reason"], "memory_admission": admission}
    gate = state_dir / "hold-host-capacity-submit.lock"
    try:
        gate.mkdir()
    except FileExistsError:
        return {"status": "requires_readback", "error_code": "capacity_update_in_progress"}
    try:
        _write_json(state_dir / "hold-host-capacity-request.json", command)
        deadline = time.monotonic() + max(wait_seconds, 0)
        while True:
            try:
                ack = _read_json(state_dir / "hold-host-capacity-result.json") or {}
                current = _read_json(state_dir / "hold-host.json") or {}
            except (OSError, ValueError):
                return {"status": "requires_readback", "error_code": "capacity_ack_unreadable"}
            bound = (ack.get("request_id") == request_id and ack.get("host_instance_id") == host_instance_id
                     and ack.get("target_capacity") == target_capacity)
            if bound and ack.get("status") != "applied":
                return ack
            if (bound and current.get("host_instance_id") == host_instance_id
                    and ack.get("status") == "applied" and ack.get("applied_capacity") == target_capacity
                    and type(ack.get("capacity_revision")) is int
                    and current.get("applied_capacity") == target_capacity
                    and current.get("capacity_revision") == ack["capacity_revision"]
                    and _matching_health(state_dir, config.profile, config.expected_codex_home, required_workers=target_capacity, **checks)):
                return {**ack, "status": "ready", "phase": "capacity_expanded"}
            if time.monotonic() >= deadline:
                return {"status": "requires_readback", "error_code": "capacity_ack_unverified",
                        "request_id": request_id, "host_instance_id": host_instance_id, "target_capacity": target_capacity}
            time.sleep(max(poll_seconds, .01))
    finally:
        gate.rmdir()



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
            "$startup=([wmiclass]'Win32_ProcessStartup').CreateInstance(); "
            "$startup.ShowWindow=0; "
            "$startup.EnvironmentVariables=@(Get-ChildItem Env: | ForEach-Object { $_.Name+'='+$_.Value }); "
            f"$jarvisHostCommand={quote(subprocess.list2cmdline([sys.executable, *host_args]))}; "
            f"$created=([wmiclass]'Win32_Process').Create($jarvisHostCommand,{quote(package_root)},$startup); "
            "if($created.ReturnValue -ne 0){exit 1}"
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
    except OSError as exc:
        if sys.platform.startswith("linux"):
            # Signal-zero permission/IO failures do not prove owner exit.
            return exc.errno != errno.ESRCH
        return False
    return True


def _strict_local_host_alive(pid: int) -> bool:
    """Positive local process proof only; access errors are never liveness proof."""
    if type(pid) is not int or pid <= 0:
        return False
    if os.name == "nt" and sys.platform == "win32":
        import ctypes
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.restype = ctypes.c_void_p
        kernel.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_ulong]
        kernel.CloseHandle.argtypes = [ctypes.c_void_p]
        process = kernel.OpenProcess(0x101000, False, pid)  # QUERY_LIMITED_INFORMATION | SYNCHRONIZE
        if not process:
            return False
        try:
            # WAIT_TIMEOUT is positive live proof; an exit code of 259 is not.
            return kernel.WaitForSingleObject(process, 0) == 0x102
        finally:
            kernel.CloseHandle(process)
    if sys.platform.startswith("linux"):
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False
    return False


def _pid_started_at(pid: int) -> datetime | None:
    if sys.platform.startswith("linux"):
        # Preserve the existing PID-reuse check with an independently read birth time.
        # /proc field 22 is starttime (clock ticks since boot); comm may contain spaces.
        try:
            process_stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
            fields = process_stat.rsplit(")", 1)[1].split()
            start_ticks = int(fields[19])
            boot_time = next(
                int(line.split()[1])
                for line in Path("/proc/stat").read_text(encoding="utf-8").splitlines()
                if line.startswith("btime ")
            )
            ticks_per_second = int(os.sysconf("SC_CLK_TCK"))
            if pid <= 0 or start_ticks < 0 or ticks_per_second <= 0:
                return None
            return datetime.fromtimestamp(
                boot_time + start_ticks / ticks_per_second, tz=timezone.utc
            )
        except (OSError, ValueError, IndexError, StopIteration):
            return None
    if os.name != "nt":  # Unsupported platforms stay fail-closed.
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

    def __init__(self, *, state_dir: Path, launcher_config: Path, workers: int = 5, recovery_backend=None,
                 worker_factory: Callable[..., Any] | None = None) -> None:
        self.state_dir = state_dir.resolve()
        self._recovery_backend = recovery_backend
        self.launcher_config = launcher_config.resolve()
        launcher = NativeTaskLauncherConfig(self.launcher_config)
        self.profile = launcher.profile
        self.codex_home = launcher.expected_codex_home
        self.codex_home_resolution = {"input": launcher.expected_codex_home_input,
            "base_dir": launcher.expected_codex_home_base_dir, "resolved": launcher.expected_codex_home}
        self.started_at = _now()
        self.instance_id = uuid.uuid4().hex
        self.requests_roots = (state_dir / "task-holds", state_dir / "task-monitors")
        self.health_path = state_dir / "hold-host.json"
        if type(workers) is not int or not 1 <= workers <= MAX_CONCURRENCY:
            raise ValueError(f"workers must be an integer from 1 to {MAX_CONCURRENCY}")
        self.workers = workers
        self._active_hold_ids: set[str] = set()
        self._health_lock = threading.RLock()
        self._capacity_lock = threading.Lock()
        self._configured_capacity = self.workers
        self._capacity_revision = 0
        self._target_capacity = self.workers
        self._capacity_error: dict[str, str] | None = None
        self._consumer_lock = threading.Lock()
        self._occupied_roots: dict[Path, dict[str, Any]] = {}
        self._worker_factory = worker_factory or threading.Thread
        self._worker_threads: list[Any] = []
        self._pool_ready = False
        self._legacy_scan_cursor = ""

    def run_once(self, *, request_root: Path | None = None) -> bool:
        self._write_health("ready")
        for requests_root in self.requests_roots if request_root is None else (request_root.parent,):
            if not requests_root.is_dir():
                continue
            roots = (request_root,) if request_root is not None else sorted(path for path in requests_root.iterdir() if path.is_dir())
            for root in roots:
                request_path = root / "request.json"
                ack_path = root / "ack.json"
                result_path = root / "result.json"
                if result_path.exists():
                    continue
                request = _read_json(request_path)
                ack = _read_json(ack_path)
                if request is None or ack is None or result_path.exists() or cancellation_path(root, request):
                    continue
                if str(ack.get("status") or "") != "accepted":
                    if self._requeue_dead_claim(root, request_path, ack_path, request, ack):
                        return True
                    continue
                if str(ack.get("phase") or "") != "queued_for_user_host":
                    if self._requeue_dead_claim(root, request_path, ack_path, request, ack):
                        return True
                    continue
                hold_id = str(request.get("hold_id") or request.get("monitor_id") or root.name)
                if not self._reserve_slot(root, request):
                    continue
                try:
                    claimed = self._claim(root, ack_path, ack, request, host_instance_id=self.instance_id)
                except Exception:
                    self._mark_inactive(hold_id, expected_request=request)
                    raise
                if not claimed:
                    self._mark_inactive(hold_id, expected_request=request)
                    if self._requeue_dead_claim(root, request_path, ack_path, request, ack):
                        return True
                    continue
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
                    _publish_generation(result_path, failed, request)
                    _publish_generation(ack_path, failed, request)
                finally:
                    self._mark_inactive(hold_id, expected_request=request)
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
                            _publish_generation(result_path, saved, request)
                            _publish_generation(ack_path, saved, request)
                        if not controlled or (saved.get("terminal_confirmed") is True
                                              and holder_returned and saved.get("holder_client_exit_confirmed") is True):
                            self._release_claim(root, expected_request=request)
                    except OSError as exc:
                        saved = _read_json(result_path)
                        if saved is not None:
                            failed_cleanup = {**saved, "cleanup_error": str(exc), "cleanup_owner": cleanup_owner}
                            _publish_generation(result_path, failed_cleanup, request)
                            if saved.get("host_stop"):
                                _publish_generation(ack_path, failed_cleanup, request)
                    finally:
                        self._write_health("ready")
                        from adapters.codex_app_server.task_provisioning_adapter import refresh_close_report
                        try:
                            with generation_guard(root, request):
                                evidence = _read_json(result_path) or {}
                                refresh_close_report(root, {**evidence, "holder_exit_confirmed": holder_returned,
                                    "hold_released": evidence.get("terminal_confirmed") is True
                                        and not (root / ".user-host-claim").exists()})
                        except GenerationRetired:
                            pass  # Old Host exit is not evidence about the new generation.
                return True
        return False

    @staticmethod
    def _requeue_dead_claim(
        root: Path, request_path: Path, ack_path: Path, request: dict[str, Any], ack: dict[str, Any],
    ) -> bool:
        try:
            if cancellation_path(root, request):
                return False  # Cancellation never requires a legacy PID death guess.
            claim_dir = root / ".user-host-claim"
            owner = _read_json(claim_dir / "owner.json")
            try:
                owner_pid = int((owner or {}).get("pid"))
            except (TypeError, ValueError):
                return False
            thread_id = str(ack.get("thread_id") or request.get("thread_id") or "").strip()
            turn_id = str(ack.get("turn_id") or request.get("turn_id") or "").strip()
            if owner_pid <= 0 or sys.platform.startswith("linux") or _pid_is_alive(owner_pid):
                return False  # Linux orphan recovery requires future namespace-aware ownership proof.
            if not thread_id or not turn_id:
                if str(ack.get("phase") or "") not in {"claimed_by_user_host", "queued_for_user_host", "thread_starting"} or str(request.get("mode") or "create") != "create":
                    return False
                _publish_generation(ack_path, {
                    "request_id": str(request.get("request_id") or ""),
                    "status": "failed",
                    "phase": "recovery_failed",
                    "hold_id": str(request.get("hold_id") or request.get("monitor_id") or root.name),
                    "recovered_from_pid": owner_pid,
                    "reason": "dead HoldHost claim has no durable thread identity",
                    "observed_at": _now(),
                }, request)
                JarvisHoldHost._release_claim(root, expected_request=request)
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
                _publish_generation(request_path, {**recovered, **({"stop_requested": True} if current.get("stop_requested") else {})}, request)
                _publish_generation(ack_path, {
                    "request_id": str(request.get("request_id") or ""),
                    "status": "accepted",
                    "phase": "queued_for_user_host",
                    "hold_id": str(request.get("hold_id") or request.get("monitor_id") or root.name),
                    "recovered_from_pid": owner_pid,
                    "observed_at": _now(),
                }, request)
                JarvisHoldHost._release_claim(root, expected_request=request)
            return True
        except (OSError, ValueError, RuntimeError):
            # Leave the dead claim for a later retry and let this Host scan other work.
            return False

    @staticmethod
    def _claim(root: Path, ack_path: Path, ack: dict[str, Any], request: dict[str, Any] | None = None, *, host_instance_id: str | None = None) -> bool:
        claim_dir = root / ".user-host-claim"
        captured = request or _read_json(root / "request.json")
        if cancellation_path(root, captured): return False
        try:
            claim_dir.mkdir()
        except FileExistsError:
            return False
        claimed = False
        created_claim = (claim_dir.stat().st_dev, claim_dir.stat().st_ino)
        try:
            _write_json(claim_dir / "owner.json", {"pid": os.getpid(), "observed_at": _now(), "host_instance_id": host_instance_id,
                        "request_id": (captured or {}).get("request_id")})
            current = _read_json(root / "request.json")
            current_ack = _read_json(ack_path) or {}
            expected = captured
            # A delayed scanner may acquire the directory after another worker finished.
            # Recheck before replacing the acknowledgement, not only before mkdir.
            if (current is None or expected is None or cancellation_path(root, current) or (root / "result.json").exists()
                    or current.get("request_id") != expected.get("request_id")
                    or current.get("hold_id") != expected.get("hold_id")
                    or current_ack.get("request_id", current.get("request_id")) != current.get("request_id")
                    or (current_ack.get("hold_id") or current_ack.get("monitor_id")) not in (
                        None, "", expected.get("hold_id") or expected.get("monitor_id") or root.name)
                    or current_ack.get("status") != "accepted"
                    or current_ack.get("phase") != "queued_for_user_host"):
                return False
            published = _publish_generation(ack_path, {
                **current_ack, "request_id": expected["request_id"],
                **({"hold_id": expected["hold_id"]} if expected.get("hold_id") else {}),
                "phase": "claimed_by_user_host",
                "host_pid": os.getpid(),
                "observed_at": _now(),
            }, captured)
            claimed = published
            return published
        finally:
            if not claimed:
                # A failed admission may remove only the exact tentative directory
                # it just created. An archived/replaced successor lease is untouched.
                from jarvis_runtime.cancellation import _hold_guard, report_guard
                gate = _hold_guard(root, captured) if root.parent.name in {"task-holds", "task-monitors"} else report_guard(root / ".dispatch-gate")
                with gate:
                    owner = _read_json(claim_dir / "owner.json") or {}
                    if (claim_dir.exists() and (claim_dir.stat().st_dev, claim_dir.stat().st_ino) == created_claim
                            and (not owner or (owner.get("pid") == os.getpid()
                                and owner.get("request_id") == (captured or {}).get("request_id")))):
                        JarvisHoldHost._release_claim(root)

    @staticmethod
    def _release_claim(root: Path, *, expected_request=None) -> bool:
        try:
            with (generation_guard(root, expected_request) if expected_request is not None else nullcontext()):
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

        except GenerationRetired:
            return False

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
            with ProcessLock((root / "request.json").with_suffix(".lock"), timeout_seconds=0.1, owner_alive=lambda pid: True):
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
                if (not owner.get("host_instance_id")
                        or owner.get("host_instance_id") != health.get("host_instance_id")
                        or "exact_hold_stop_v1" not in (health.get("capabilities") or [])
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

    def _reconcile_first_create_nondispatch(self, root: Path, hold_id: str, request: dict,
                                           ack: dict, result: dict) -> dict[str, Any] | None:
        """Recognize one local first-create model rejection; never reap a claim."""
        prior = result.get("recovery") or {}
        if not ((request.get("mode") == "create" and result.get("phase") == "model_selecting")
                or (isinstance(prior, dict) and prior.get("source") == "pre_dispatch_model_validation")):
            return None
        unknown = {"status": "requires_readback", "hold_id": hold_id, "terminal_confirmed": False,
                   "hold_released": False, "error_code": "local_nondispatch_unverified",
                   "reason": "Exact local first-create non-dispatch evidence is incomplete or changed"}
        if not (sys.platform.startswith("linux") or (os.name == "nt" and sys.platform == "win32")):
            return {**unknown, "reason": "Strict local Host liveness proof is unavailable on this platform"}
        try:
            from jarvis_runtime.cancellation import _dispatch_guard, _require_roots_open
            def digest(value):
                return hashlib.sha256(value).hexdigest()
            def encoded(value):
                # Exact bytes used by the existing atomic _write_json.
                return json.dumps(value, ensure_ascii=False).encode("utf-8")
            def is_digest(value):
                return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)
            def observe():
                records = {name: (root / (name + ".json")).read_bytes() for name in ("request", "ack", "result")}
                current, receipt, failure = (json.loads(records[name]) for name in ("request", "ack", "result"))
                if not all(isinstance(value, dict) for value in (current, receipt, failure)):
                    raise ValueError("Non-dispatch records must be objects")
                for name in ("dispatch-commits", "history"):
                    directory = root / name
                    if os.path.lexists(directory) and (directory.is_symlink() or not directory.is_dir() or any(directory.iterdir())):
                        raise ValueError("Dispatch/history absence is unverified")
                identity = {"request_id": current.get("request_id"), "hold_id": hold_id}
                keys = ("request_id", "hold_id", "status", "phase", "reason", "thread_id", "turn_id", "pid", "total_turn_count", "observed_at")
                if (not identity["request_id"] or current.get("hold_id") != hold_id
                        or any(value.get(key) != item for value in (receipt, failure) for key, item in identity.items())
                        or current.get("mode") != "create" or current.get("thread_id") not in (None, "")
                        or current.get("turn_id") not in (None, "")
                        or current.get("resume_pause") or current.get("supersedes") or current.get("stop_requested")
                        or current.get("host_stop") is not None
                        or any(type(current.get(key, 1)) is not int or current.get(key, 1) != 1
                               for key in ("initial_turn_count", "initial_total_turn_count"))
                        or failure.get("status") != "failed" or failure.get("phase") != "model_selecting"
                        or not isinstance(current.get("model"), str) or not current["model"]
                        or not str(failure.get("reason", "")).startswith(
                            f"requested model is not supported by this CLI: {current['model']}; available=")
                        or not isinstance(failure.get("thread_id"), str) or not failure["thread_id"]
                        or failure.get("turn_id") not in (None, "")
                        or type(failure.get("total_turn_count")) is not int or failure["total_turn_count"] != 1
                        or any(receipt.get(key) != failure.get(key) for key in keys)
                        or any(value.get("host_stop") is True or value.get("execution_evidence") == "requires_owner_terminal"
                               or value.get("dispatch_evidence") not in (None, "not_sent") for value in (receipt, failure))
                        or os.path.lexists(root / ".user-host-claim")
                        or os.path.lexists(root / "request.lock") or cancellation_path(root, current)):

                    raise ValueError("First-create records, cancellation, claim or dispatch history are unverified")
                failure_hash = digest(json.dumps({key: failure.get(key) for key in keys}, sort_keys=True).encode())
                saved = failure.get("recovery") or {}
                proof = saved.get("local_proof") if isinstance(saved, dict) else None
                health_bytes = self.health_path.read_bytes() if self.health_path.exists() else None
                health = json.loads(health_bytes) if health_bytes is not None else None
                if health is not None and not isinstance(health, dict):
                    raise ValueError("Host health must be an object")
                if failure.get("terminal_confirmed") is True:
                    # A completed local proof survives normal owner exit. It is
                    # never inferred from an old terminal flag or caller evidence.
                    if (not isinstance(proof, dict) or proof.get("schema") != "jarvis-first-create-nondispatch/v1"
                            or saved.get("source") != "pre_dispatch_model_validation" or saved.get("turn_started") is not False
                            or failure.get("execution_evidence") != "model_validation_rejected_before_turn_start"
                            or proof.get("request_sha256") != digest(records["request"])
                            or proof.get("failure_sha256") != failure_hash
                            or not is_digest(proof.get("initial_ack_sha256"))
                            or not is_digest(proof.get("initial_health_sha256"))
                            or not isinstance(proof.get("host_instance_id"), str) or not proof["host_instance_id"]
                            or type(proof.get("pid")) is not int or proof["pid"] != failure.get("pid")
                            or proof["pid"] <= 0 or proof.get("profile") != self.profile
                            or not _same_path(proof.get("codex_home"), self.codex_home)
                            or not _same_path(proof.get("state_dir"), self.state_dir)):
                        raise ValueError("Prior non-dispatch result is not bound to this exact local proof")
                    birth, started, failed, tick, verified, now = (_parse_time(proof.get("process_started_at")),
                        _parse_time(proof.get("host_started_at")), _parse_time(failure.get("observed_at")),
                        _parse_time(proof.get("health_observed_at")), _parse_time(saved.get("observed_at")), _parse_time(_now()))
                    target = proof.get("target_ack")
                    summary = {key: value for key, value in saved.items() if key != "local_proof"}
                    if (any(value is None for value in (birth, started, failed, tick, verified, now))
                            or not (birth <= failed and started <= failed < tick <= verified <= now)
                            or not isinstance(target, dict) or digest(encoded(target)) != proof.get("target_ack_sha256")
                            or any(target.get(key) != failure.get(key) for key in keys)
                            or target.get("terminal_confirmed") is not True
                            or target.get("execution_evidence") != failure["execution_evidence"]
                            or target.get("recovery") != summary
                            or digest(records["ack"]) not in {proof["initial_ack_sha256"], proof["target_ack_sha256"]}):
                        raise ValueError("Fixed non-dispatch proof or target acknowledgement is unverified")
                    instance, pid = proof["host_instance_id"], proof["pid"]
                    if (any(value.get("host_instance_id") not in (None, "", instance) for value in (current, receipt, failure))
                            or any(value.get("host_pid") not in (None, pid) for value in (receipt, failure))
                            or (health is not None and (health.get("host_instance_id") != instance or health.get("pid") != pid))):
                        raise ValueError("Current Host identity conflicts with the recorded operation")
                    return records, health_bytes, proof, current, receipt, failure
                if not isinstance(health, dict):
                    raise ValueError("Fresh current Host health is required for first admission")
                pid, instance = failure.get("pid"), health.get("host_instance_id")
                if (type(pid) is not int or pid <= 0 or health.get("pid") != pid
                        or not isinstance(instance, str) or not instance.strip()
                        or any(value.get("host_instance_id") not in (None, "", instance) for value in (current, receipt, failure))
                        or any(value.get("host_pid") not in (None, pid) for value in (receipt, failure))
                        or not isinstance(health.get("active_hold_ids"), list) or hold_id in health["active_hold_ids"]):
                    raise ValueError("Current Host identity or inactivity is unverified")
                birth = _parse_time(_pid_started_at(pid))
                started, observed, tick, now = (_parse_time(health.get("host_started_at")),
                    _parse_time(failure.get("observed_at")), _parse_time(health.get("observed_at")), _parse_time(_now()))
                if (any(value is None for value in (birth, started, observed, tick, now))
                        or not (birth <= observed and started <= observed < tick <= now)):
                    raise ValueError("Host/process birth, failure and current health ordering is unverified")
                if not _matching_health(self.state_dir, self.profile, self.codex_home,
                        now=lambda: now, pid_alive=_pid_is_alive, pid_started_at=_pid_started_at):
                    raise ValueError("Fresh complete current Host identity is unverified")
                # _pid_is_alive treats access errors as possibly live. Publication
                # requires a separate positive sample, including inside the gates.
                if not _strict_local_host_alive(pid):
                    raise ValueError("Strict local Host liveness proof is unavailable")
                proof = {"schema": "jarvis-first-create-nondispatch/v1", "initial_ack_sha256": digest(records["ack"]),
                         "initial_health_sha256": digest(health_bytes), "request_sha256": digest(records["request"]),
                         "failure_sha256": failure_hash, "host_instance_id": instance, "pid": pid,
                         "profile": self.profile, "codex_home": self.codex_home, "state_dir": str(self.state_dir.resolve()),
                         "process_started_at": birth.isoformat(), "host_started_at": started.isoformat(),
                         "health_observed_at": tick.isoformat()}
                return records, health_bytes, proof, current, receipt, failure
            original = observe()
            with _dispatch_guard(root, {**original[3], "thread_id": original[5]["thread_id"]}) as roots:
                _require_roots_open(roots)
                checked = observe()  # Re-sample process/health and every local predicate under the same gates.
                if checked[:3] != original[:3]:
                    raise ValueError("Local non-dispatch evidence changed before publication")
                records, _, proof, current, receipt, failure = checked
                if failure.get("terminal_confirmed") is True:
                    if digest(records["ack"]) != proof["target_ack_sha256"]:
                        _write_json(root / "ack.json", proof["target_ack"])
                    return {**failure, "hold_released": True}
                summary = {"source": "pre_dispatch_model_validation", "observed_at": _now(), "turn_started": False}
                reconciled = {**failure, "terminal_confirmed": True,
                    "execution_evidence": "model_validation_rejected_before_turn_start", "recovery": summary}
                # Freeze the exact ACK (including ACK-only fields) before either
                # write. Detailed proof remains in result, avoiding a self hash.
                target = {**receipt, **reconciled}
                proof = {**proof, "target_ack": target, "target_ack_sha256": digest(encoded(target))}
                reconciled = {**reconciled, "recovery": {**summary, "local_proof": proof}}
                # Already inside the exact thread/generation gates; publisher
                # file locks are not reentrant. Other publishers remain unchanged.
                _write_json(root / "result.json", reconciled)
                _write_json(root / "ack.json", target)
                return {**reconciled, "hold_released": not os.path.lexists(root / ".user-host-claim")}
        except (OSError, ValueError, RuntimeError, TypeError) as exc:
            return {**unknown, "reason": str(exc)}

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
        existing = _read_json(result_path) or {}
        current_request, current_ack = _read_json(request_path) or {}, _read_json(ack_path) or {}
        owned = existing.get("owner_terminal_readback") or {}
        identity = {"request_id": current_request.get("request_id"), "hold_id": hold_id,
                    "thread_id": existing.get("thread_id"), "turn_id": existing.get("turn_id")}
        owner_path = root / ".user-host-claim" / "owner.json"
        current_owner = _read_json(owner_path) if owner_path.exists() else None
        if (current_request.get("thread_id") not in (None, "", identity["thread_id"])
                or (current_owner and current_owner.get("request_id") not in (None, "", identity["request_id"]))):
            return {**unknown, "error_code": "identity_unverified",
                    "reason": "Current request thread or claim generation conflicts with owned closure proof"}
        if (all(identity.values()) and current_request.get("hold_id") == hold_id
                and all(record.get(k) == v for record in (current_ack, existing) for k, v in identity.items())
                and all(record.get(k) is True for record in (current_ack, existing)
                        for k in ("terminal_confirmed", "holder_exit_confirmed", "holder_client_exit_confirmed"))
                and existing.get("status") in {"completed", "failed", "interrupted", "cancelled", "canceled", "blocked", "turn_limit_reached"}
                and current_ack.get("status") == existing["status"]
                and (owned.get("status") == existing["status"] or
                     (owned.get("status") == "completed" and existing["status"] in {"cancelled", "canceled", "blocked", "turn_limit_reached"}))
                and owned.get("thread_id") == identity["thread_id"] and owned.get("turn_id") == identity["turn_id"]
                and owned.get("status") in {"completed", "failed", "interrupted", "cancelled", "canceled"}
                and current_ack.get("owner_terminal_readback") == owned):
            try:
                from jarvis_runtime.cancellation import _dispatch_guard
                with _dispatch_guard(root, {**current_request, "thread_id": identity["thread_id"]}):
                    if (_read_json(request_path) != current_request or _read_json(ack_path) != current_ack
                            or _read_json(result_path) != existing
                            or (_read_json(owner_path) if owner_path.exists() else None) != current_owner):
                        return {**unknown, "error_code": "identity_changed", "reason": "Owned closure proof changed before cleanup"}
                    self._release_claim(root)  # Locked exact-generation proof includes actual owned exits.
                return {**existing, "hold_released": not (root / ".user-host-claim").exists()}
            except (OSError, RuntimeError) as exc:
                return {**existing, "hold_released": False, "cleanup_error": str(exc)}
        local_non_dispatch = self._reconcile_first_create_nondispatch(root, hold_id, current_request, current_ack, existing)
        if local_non_dispatch is not None:
            return local_non_dispatch
        if not (existing.get("terminal_confirmed") is True and not (root / ".user-host-claim").exists()):
            from adapters.codex_app_server.task_provisioning_adapter import reconcile_original_generation, _OriginalRecoveryBlocked
            try:
                return reconcile_original_generation(root, getattr(self, "_recovery_backend", None))
            except _OriginalRecoveryBlocked as exc:
                return {**unknown, **exc.identity, "error_code": exc.code, "phase": "original_backend_recovery", "reason": str(exc)}
            except Exception as exc:
                return {**unknown, "error_code": "identity_changed", "reason": str(exc)}
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
                # create_task reports model_selecting before calling turn/start.
                # This exact local validation error proves non-dispatch; an empty
                # turn alone (or a transport error) never proves termination.
                model = request.get("model")
                if (request.get("mode") == "create" and model
                        and not request.get("thread_id") and not request.get("turn_id")
                        and request.get("initial_turn_count", 1) == 1
                        and request.get("initial_total_turn_count", 1) == 1
                        and result.get("status") == "failed"
                        and result.get("phase") == "model_selecting"
                        and str(result.get("reason", "")).startswith(
                            f"requested model is not supported by this CLI: {model}; available=")
                        and result.get("thread_id") and not result.get("turn_id")
                        and result.get("total_turn_count") == 1
                        and all(ack.get(key) == result.get(key) for key in (
                            "status", "phase", "reason", "thread_id", "turn_id", "pid"))
                        and not claim.exists()):
                    health = _read_json(self.health_path) or {}
                    if (health.get("pid") != result.get("pid")
                            or not _matching_health(self.state_dir, self.profile, self.codex_home,
                                now=_now, pid_alive=_pid_is_alive, pid_started_at=_pid_started_at)
                            or not isinstance(health.get("active_hold_ids"), list)
                            or hold_id in health["active_hold_ids"]
                            or _parse_time(health.get("observed_at")) is None
                            or _parse_time(result.get("observed_at")) is None
                            or _parse_time(health["observed_at"]) <= _parse_time(result["observed_at"])):
                        return {**unknown, "reason": "Pre-dispatch failure owner inactivity is unproven"}
                    reconciled = {**result, "terminal_confirmed": True,
                                  "execution_evidence": "model_validation_rejected_before_turn_start",
                                  "recovery": {"source": "pre_dispatch_model_validation",
                                               "observed_at": _now(), "turn_started": False}}
                    _publish_generation(result_path, reconciled, request)
                    _publish_generation(ack_path, {**ack, **reconciled}, request)
                    return {**reconciled, "hold_released": True}
                identities = {}
                for key in ("thread_id", "turn_id"):
                    # request/ack may name the previous terminal turn when a
                    # continuation failed before its new identity was returned.
                    if not result.get(key):
                        close = _read_json(root / "close.json") or {}
                        if key == "turn_id" and close and close.get("request_id") == request.get("request_id") and identities.get("thread_id"):
                            # Read-only diagnosis cannot assign an unrelated active writer.
                            from adapters.codex_app_server.jarvis_connection import route_for_model
                            config = NativeTaskLauncherConfig(self.launcher_config)
                            config.connection_base_url_override = route_for_model(
                                getattr(config, "jarvis_connection", None), request.get("model"), allow_start=False)
                            client = AppServerClient(config)
                            client.read_only = True
                            try:
                                client.start()
                                readback = client.request("thread/read", {"threadId": identities["thread_id"], "includeTurns": True})
                            finally:
                                client.close()
                            observed = readback.get("thread") or {}
                            matched = observed.get("id") == identities["thread_id"]
                            return {**unknown, **identities, "thread_readback_matched": matched,
                                "active_turn_ids": [turn.get("id") for turn in observed.get("turns", [])
                                    if matched and isinstance(turn, dict) and turn.get("status") == "inProgress"],
                                "reason": "Thread inspected; no durable turn ownership binding, no interrupt or release"}
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
                client.read_only = True
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
                _publish_generation(result_path, reconciled, request)
                _publish_generation(ack_path, {**ack, **reconciled}, request)
                try:
                    self._release_claim(root, expected_request=request)
                except OSError as exc:
                    reconciled = {**reconciled, "cleanup_error": str(exc)}
                    _publish_generation(result_path, reconciled, request)
                    return {**reconciled, "hold_released": False}
                return {**reconciled, "hold_released": not claim.exists()}
        except (OSError, ValueError, RuntimeError) as exc:
            return {**unknown, "reason": f"Reconciliation incomplete: {exc}"}

    def _setup_worker_pool(self, stop_event: threading.Event) -> None:
        self._pool_stop = stop_event
        self._pending: queue.Queue[Path] = queue.Queue(maxsize=self.workers)
        self._scheduled: set[Path] = set()
        self._scheduled_lock = threading.Lock()
        self._pool_ready = True


    def _accepted_roots(self) -> list[Path]:
        roots = []
        for parent in self.requests_roots:
            if not parent.is_dir():
                continue
            for root in sorted(parent.iterdir()):
                if not root.is_dir() or (root / "result.json").exists() or (root / ".user-host-claim").exists():
                    continue
                try:
                    request = _read_json(root / "request.json") or {}
                    ack = _read_json(root / "ack.json") or {}
                except (OSError, ValueError):
                    continue  # Unreadable history is never claimable backlog.
                if (request.get("request_id") and ack.get("request_id", request["request_id"]) == request["request_id"]
                        and ack.get("status") == "accepted" and ack.get("phase") == "queued_for_user_host"
                        and not cancellation_path(root, request)):
                    roots.append(root)
        return roots


    def _check_legacy_requeue(self) -> None:
        # Keep the former local recovery check visible without using history to
        # create consumers. One check per tick leaves capacity control responsive.
        candidates = []
        for parent in self.requests_roots:
            if not parent.is_dir():
                continue
            for root in parent.iterdir():
                if not root.is_dir() or root in self._occupied_roots or (root / "result.json").exists():
                    continue
                try:
                    request = _read_json(root / "request.json") or {}
                    ack = _read_json(root / "ack.json") or {}
                    if (not request.get("request_id") or not ack
                            or ack.get("request_id", request["request_id"]) != request["request_id"]
                            or cancellation_path(root, request)):
                        continue
                    if (ack.get("status") == "accepted" and ack.get("phase") == "queued_for_user_host"
                            and not (root / ".user-host-claim").exists()):
                        continue
                    candidates.append((root, request, ack))
                except (OSError, ValueError):
                    continue
        if candidates:
            candidates.sort(key=lambda entry: str(entry[0]))
            root, request, ack = next((entry for entry in candidates if str(entry[0]) > self._legacy_scan_cursor), candidates[0])
            self._legacy_scan_cursor = str(root)
            self._requeue_dead_claim(root, root / "request.json", root / "ack.json", request, ack)


    def _reserve_slot(self, root: Path, request: dict[str, Any]) -> bool:
        hold_id = str(request.get("hold_id") or request.get("monitor_id") or root.name)
        with self._health_lock:
            if hold_id in self._active_hold_ids or root in self._occupied_roots or len(self._active_hold_ids) >= self.workers:
                return False
            self._active_hold_ids.add(hold_id)
            self._occupied_roots[root] = dict(request)
            return True


    def capacity_snapshot(self) -> dict[str, Any]:
        with self._health_lock:
            occupied = len(self._active_hold_ids)
            active = 0
            for root, request in self._occupied_roots.items():
                try:
                    ack = _read_json(root / "ack.json") or {}
                except (OSError, ValueError):
                    ack = {}  # Keep its reservation; unreadable evidence never frees a seat.
                if (ack.get("request_id") == request.get("request_id")
                        and ack.get("status") in {"holding", "running", "accepted"}
                        and ack.get("phase") in {"turn_holding", "turn_started", "holding", "monitoring"}
                        and isinstance(ack.get("thread_id"), str) and ack["thread_id"]
                        and isinstance(ack.get("turn_id"), str) and ack["turn_id"]):
                    active += 1
            backlog = sum(root not in self._occupied_roots for root in self._accepted_roots())
            return {"configured_capacity": self._configured_capacity, "target_capacity": self._target_capacity,
                    "applied_capacity": self.workers, "capacity_revision": self._capacity_revision,
                    "starting_reservation_count": occupied - active, "confirmed_active_count": active,
                    "occupied_count": occupied, "backlog_count": backlog,
                    "consumer_count": sum(worker.is_alive() for worker in self._worker_threads),
                    "capacity_error": self._capacity_error}


    def apply_capacity_request(self, command: dict[str, Any]) -> dict[str, Any]:
        with self._capacity_lock:
            result = {"request_id": command.get("request_id"), "host_instance_id": self.instance_id,
                      "target_capacity": command.get("target_capacity")}
            target = command.get("target_capacity")
            if not self._pool_ready:
                return {**result, "status": "unsupported", "error_code": "hot_capacity_unsupported"}
            if command.get("host_instance_id") != self.instance_id:
                return {**result, "status": "rejected", "error_code": "stale_host_instance"}
            if (not isinstance(command.get("request_id"), str) or not command["request_id"]
                    or type(target) is not int or not 1 <= target <= MAX_CONCURRENCY):
                return {**result, "status": "rejected", "error_code": "invalid_capacity_request"}
            receipt_path = self.state_dir / "hold-host-capacity-results" / (hashlib.sha256(command["request_id"].encode()).hexdigest() + ".json")
            try:
                saved = _read_json(receipt_path)
            except (OSError, ValueError):
                return {**result, "status": "requires_readback", "error_code": "capacity_receipt_unreadable"}
            if saved:
                if saved.get("declaration") != command:
                    return {**result, "status": "rejected", "error_code": "capacity_request_conflict"}
                if not isinstance(saved.get("result"), dict):
                    return {**result, "status": "requires_readback", "error_code": "capacity_receipt_unverified"}
                return saved["result"]
            if target < self.workers:
                return {**result, "status": "unsupported", "error_code": "decrease_not_supported"}
            if target != self.workers and command.get("expected_revision") != self._capacity_revision:
                return {**result, "status": "rejected", "error_code": "capacity_conflict"}
            if target > self.workers:
                from .task_provisioning_adapter import physical_memory_admission
                admission = physical_memory_admission(NativeTaskLauncherConfig(self.launcher_config))
                if admission["status"] != "allowed":
                    result.update(status="blocked", error_code=admission["error_code"], reason=admission["reason"],
                                  memory_admission=admission, applied_capacity=self.workers, capacity_revision=self._capacity_revision)
                    _write_json(receipt_path, {"declaration": command, "result": result})
                    return result
            self._target_capacity = target
            if target > self.workers and not self._sync_consumers(target_capacity=target):
                result.update(status="requires_readback", error_code="consumer_start_failed",
                    applied_capacity=self.workers, capacity_revision=self._capacity_revision)
                self._write_health("ready")
                _write_json(receipt_path, {"declaration": command, "result": result})
                return result
            with self._health_lock:
                if target > self.workers:
                    self.workers = target
                    self._capacity_revision += 1
                    with self._pending.mutex:
                        self._pending.maxsize = target
                        self._pending.not_full.notify_all()
            result.update(status="applied", applied_capacity=self.workers, capacity_revision=self._capacity_revision)
            self._write_health("ready")
            _write_json(receipt_path, {"declaration": command, "result": result})
            return result


    def _service_capacity_request(self) -> None:
        try:
            command = _read_json(self.state_dir / "hold-host-capacity-request.json")
        except (OSError, ValueError):
            self._capacity_error = {"error_code": "capacity_control_unreadable"}
            _write_json(self.state_dir / "hold-host-capacity-result.json",
                {"host_instance_id": self.instance_id, "status": "requires_readback", **self._capacity_error})
            return
        if command:
            result = self.apply_capacity_request(command)
            _write_json(self.state_dir / "hold-host-capacity-result.json", result)


    def _sync_consumers(self, *, target_capacity: int | None = None) -> bool:
        with self._consumer_lock:
            self._worker_threads = [worker for worker in self._worker_threads if worker.is_alive()]
            with self._health_lock:
                backlog = sum(root not in self._occupied_roots for root in self._accepted_roots())
                needed = min(target_capacity or self.workers, len(self._active_hold_ids) + backlog)
            while len(self._worker_threads) < needed:
                try:
                    worker = self._worker_factory(target=self._run_worker,
                        args=(self._pending, self._scheduled, self._scheduled_lock, self._pool_stop), daemon=True)
                    worker.start()
                except (OSError, RuntimeError):
                    self._capacity_error = {"error_code": "consumer_start_failed"}
                    return False
                self._worker_threads.append(worker)
            self._capacity_error = None
            return True


    def _schedule_requests_once(self) -> None:
        self._service_capacity_request()
        self._check_legacy_requeue()
        for root in self._accepted_roots():
            with self._scheduled_lock:
                if root in self._scheduled or root in self._occupied_roots:
                    continue
                try:
                    self._pending.put_nowait(root)
                except queue.Full:
                    break  # Not enqueued: never leave a hidden scheduled marker.
                self._scheduled.add(root)
        self._sync_consumers()


    def run_forever(self, *, poll_seconds: float, stop_event: threading.Event | None = None) -> None:
        stop = stop_event or threading.Event()
        self._setup_worker_pool(stop)
        heartbeat = threading.Thread(target=self._run_health_heartbeat, args=(stop, poll_seconds), daemon=True)
        heartbeat.start()
        while not stop.is_set():
            try:
                self._schedule_requests_once()
            except OSError:
                pass
            stop.wait(max(poll_seconds, .25))
        for worker in self._worker_threads:
            worker.join()
        heartbeat.join()

    def _run_health_heartbeat(self, stop_event: threading.Event, poll_seconds: float) -> None:
        while not stop_event.is_set():
            self._write_health("ready")
            stop_event.wait(min(max(poll_seconds, 0.25), 5.0))

    def _run_worker(self, pending: queue.Queue[Path], scheduled: set[Path],
                    scheduled_lock: threading.Lock, stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            try:
                root = pending.get(timeout=0.25)
            except queue.Empty:
                continue
            try:
                self.run_once(request_root=root)
            except Exception:
                self._write_health("ready")
            finally:
                with scheduled_lock:
                    scheduled.discard(root)
                pending.task_done()

    def _mark_active(self, hold_id: str) -> None:
        with self._health_lock:
            self._active_hold_ids.add(hold_id)

    def _mark_inactive(self, hold_id: str, *, expected_request=None) -> None:
        with self._health_lock:
            matching = [root for root, request in self._occupied_roots.items()
                if str(request.get("hold_id") or request.get("monitor_id") or root.name) == hold_id
                and (expected_request is None or request.get("request_id") == expected_request.get("request_id"))]
            if expected_request is not None and not matching:
                return  # A late predecessor cannot discard its successor's seat.
            for root in matching:
                del self._occupied_roots[root]
            if not any(str(request.get("hold_id") or request.get("monitor_id") or root.name) == hold_id
                       for root, request in self._occupied_roots.items()):
                self._active_hold_ids.discard(hold_id)

    def _write_health(self, status: str, *, request_id: str | None = None) -> None:
        with self._health_lock:
            active_hold_ids = sorted(self._active_hold_ids)
            capacity = self.capacity_snapshot()
            try:
                _write_json(self.health_path, {
                    "profile": self.profile,
                    "codex_home": self.codex_home,
                    "codex_home_resolution": self.codex_home_resolution,
                    "state_dir": str(self.state_dir.resolve()),
                    "status": "holding" if active_hold_ids else status,
                    "pid": os.getpid(),
                    "host_started_at": self.started_at,
                    "host_instance_id": self.instance_id,
                    "request_id": request_id,
                    "worker_capacity": self.workers,
                    "active_count": len(active_hold_ids),
                    "active_hold_ids": active_hold_ids,
                    **capacity,
                    "capabilities": ["exact_hold_stop_v1"] + (["hot_capacity_growth_v1"] if self._pool_ready else []),
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
