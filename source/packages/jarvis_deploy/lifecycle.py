"""Conservative, local lifecycle gate for a new managed Jarvis installation.

Launch and update callers hold the *same* kernel lease for their entire operation.
The lease's availability is not shutdown evidence: a crash releases an OS lock but
leaves starting/running metadata, which cannot be reset through this API. Only a
waited, successful direct child and verified runtime receipts can record stopped.

This gate does not adopt legacy installs, inspect credentials, signal processes,
reclaim PID files, stop services, or reconcile unknown native execution. Existing
detached Hold Hosts and dispatch owners have no run-bound host-exit receipt, so
those layouts fail closed even when a PID is absent or their heartbeat is stale.
All participating launch/update paths must use this gate; it is not a sandbox
against an actor that edits deployment metadata or launches code outside it.
"""
from __future__ import annotations

from datetime import datetime, timezone
import errno
import hashlib
import json
import os
from pathlib import Path
import stat
import sqlite3
import tempfile
from typing import Any, BinaryIO
import uuid

SCHEMA = "jarvis-managed-lifecycle/v1"
METADATA_FILENAME = "lifecycle.json"
LOCK_FILENAME = ".lifecycle.lock"
_MAX_RECORD_BYTES = 2 * 1024 * 1024
_MAX_FILES = 10000
_HEARTBEAT_DB = "local-heartbeats.sqlite"
_HEARTBEAT_FILES = {_HEARTBEAT_DB, _HEARTBEAT_DB + "-wal", _HEARTBEAT_DB + "-shm"}
_TERMINAL = {"completed", "failed", "interrupted", "cancelled", "canceled", "blocked", "turn_limit_reached"}


class LifecycleError(RuntimeError):
    """Lifecycle state is unavailable, unsafe, or unproven; do not update."""


class LifecycleBusyError(LifecycleError):
    """A launch or update already holds the managed installation lease."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_path(path: Path) -> Path:
    """Reject aliases, including Windows junctions, before resolving identity."""
    path = Path(os.path.abspath(path))
    for component in (path, *path.parents):
        try:
            info = component.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise LifecycleError("Lifecycle paths must not contain symlinks or reparse points")
    return path


def _pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _json(path: Path) -> tuple[dict[str, Any], bytes]:
    _safe_path(path)
    try:
        if not stat.S_ISREG(path.stat().st_mode) or path.stat().st_size > _MAX_RECORD_BYTES:
            raise LifecycleError("Lifecycle control record is not a bounded regular file")
        with path.open("rb") as handle:
            raw = handle.read(_MAX_RECORD_BYTES + 1)
        if len(raw) > _MAX_RECORD_BYTES:
            raise LifecycleError("Lifecycle control record is too large")
        value = json.loads(raw, object_pairs_hook=_pairs,
                           parse_constant=lambda value: (_ for _ in ()).throw(ValueError("nonfinite JSON")))
        if not isinstance(value, dict):
            raise ValueError("control record must be a JSON object")
        return value, raw
    except (OSError, ValueError, UnicodeError) as exc:
        raise LifecycleError("Unreadable or malformed lifecycle control record: " + path.name) from exc


def _sync_directory(path: Path) -> None:
    if os.name != "nt":
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _lock_file(handle: BinaryIO, *, platform: str | None = None) -> None:
    """Take a nonblocking byte lock on Windows and flock on Unix."""
    if (platform or os.name) == "nt":
        import msvcrt
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        import fcntl
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_file(handle: BinaryIO, *, platform: str | None = None) -> None:
    if (platform or os.name) == "nt":
        import msvcrt
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        import fcntl
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _terminal(record: dict[str, Any]) -> bool:
    return (record.get("status") in _TERMINAL and record.get("terminal_confirmed") is True
            and record.get("external_execution_unresolved") is not True
            and record.get("execution_state") not in {"unknown", "running", "UNKNOWN"})


def _empty_heartbeat_snapshot(state_dir: Path) -> list[tuple[str, str]]:
    """Inspect a private SQLite snapshot, never open the real registry for writes.

    Any nonempty WAL fails closed, including valid, corrupt and truncated WAL:
    SQLite may silently ignore malformed or incomplete frames. A normally stopped
    scheduler must have checkpointed its WAL. Only the exact empty local scheduler
    schema is supported; nonempty or unknown scheduling remains blocked.
    """
    snapshots: dict[str, bytes] = {}
    def read(path: Path) -> bytes:
        _safe_path(path)
        info = path.stat()
        if not stat.S_ISREG(info.st_mode) or info.st_size > 32 * 1024 * 1024:
            raise LifecycleError("Local heartbeat registry is not a bounded regular file")
        with path.open("rb") as handle:
            raw = handle.read(32 * 1024 * 1024 + 1)
        if len(raw) > 32 * 1024 * 1024:
            raise LifecycleError("Local heartbeat registry exceeds inspection limit")
        return raw
    try:
        for name in sorted(_HEARTBEAT_FILES):
            path = state_dir / name
            if path.exists():
                snapshots[name] = read(path)
        if _HEARTBEAT_DB not in snapshots:
            raise LifecycleError("Local heartbeat sidecar has no main registry")
        if snapshots.get(_HEARTBEAT_DB + "-wal"):
            raise LifecycleError("Local heartbeat WAL is nonempty; checkpoint and owner reconciliation are required")
        with tempfile.TemporaryDirectory(prefix="jarvis-lifecycle-readonly-") as temporary:
            copy = Path(temporary) / _HEARTBEAT_DB
            copy.write_bytes(snapshots[_HEARTBEAT_DB])
            if _HEARTBEAT_DB + "-wal" in snapshots:
                copy.with_name(copy.name + "-wal").write_bytes(snapshots[_HEARTBEAT_DB + "-wal"])
            # A disposable SHM is reconstructed from copied WAL. Source SHM is
            # read/fingerprinted only; no source lock or sidecar is changed.
            db = sqlite3.connect(copy.as_uri() + "?mode=ro", uri=True, timeout=0)
            try:
                db.execute("PRAGMA query_only=ON")
                db.execute("PRAGMA trusted_schema=OFF")
                calls = 0
                def bound():
                    nonlocal calls
                    calls += 1
                    return int(calls > 10000)
                db.set_progress_handler(bound, 1000)
                expected = {("jarvis_local_heartbeats", "table"), ("jarvis_local_heartbeat_runs", "table")}
                schema = {(name, kind) for name, kind in db.execute("SELECT name,type FROM sqlite_schema")
                          if not name.startswith("sqlite_")}
                if schema != expected:
                    raise LifecycleError("Local heartbeat registry has an unknown schema")
                columns = {
                    "jarvis_local_heartbeats": {"heartbeat_id", "name", "function_name", "arguments_json",
                        "interval_seconds", "status", "next_run_epoch", "run_count", "max_runs", "expires_at",
                        "failure_count", "last_error", "source_event_key", "confirmation_evidence", "created_at", "updated_at"},
                    "jarvis_local_heartbeat_runs": {"run_id", "heartbeat_id", "started_at", "completed_at", "outcome", "receipt_json", "error"}}
                for table, expected_columns in columns.items():
                    if {row[1] for row in db.execute("PRAGMA table_info(" + table + ")")} != expected_columns:
                        raise LifecycleError("Local heartbeat registry columns are unrecognized")
                    if db.execute("SELECT 1 FROM " + table + " LIMIT 1").fetchone() is not None:
                        raise LifecycleError("Local heartbeat scheduling/history is nonempty and requires exact reconciliation")
                if db.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    raise LifecycleError("Local heartbeat registry is inconsistent")
            finally:
                db.close()
        for name in _HEARTBEAT_FILES:
            path = state_dir / name
            if path.exists() != (name in snapshots) or (name in snapshots and read(path) != snapshots[name]):
                raise LifecycleError("Local heartbeat registry changed during inspection")
        return [(name, hashlib.sha256(raw).hexdigest()) for name, raw in snapshots.items()]
    except (OSError, sqlite3.Error) as exc:
        raise LifecycleError("Local heartbeat registry cannot be verified read-only") from exc


def _runtime_snapshot(state_dir: Path) -> str:
    """Verify known control records; never turn a stale PID into exit proof.

    Unknown record families deliberately require a separately reviewed migration.
    Runtime prompts, logs, configuration, credentials and turn-history databases
    are not copied to deployment metadata. Only receipt identity/digests are kept.
    """
    _safe_path(state_dir)
    if not state_dir.is_dir():
        raise LifecycleError("Managed runtime state directory is missing")
    entries: list[tuple[str, str]] = []
    holds: set[str] = set()
    count = 0

    def directory(path: Path):
        nonlocal count
        _safe_path(path)
        if not path.is_dir():
            raise LifecycleError("Unexpected non-directory in runtime registry")
        children = sorted(path.iterdir(), key=lambda item: item.name)
        count += len(children)
        if count > _MAX_FILES:
            raise LifecycleError("Runtime evidence exceeds the bounded inspection limit")
        for child in children:
            _safe_path(child)
            entries.append((str(child.relative_to(state_dir)), "directory" if child.is_dir() else "file"))
        return children

    def record(path: Path):
        value, raw = _json(path)
        entries.append((str(path.relative_to(state_dir)), hashlib.sha256(raw).hexdigest()))
        return value

    parents = directory(state_dir)
    if any(path.name in _HEARTBEAT_FILES for path in parents):
        entries.extend(_empty_heartbeat_snapshot(state_dir))
    # Existing initialize_user_host creates both registries *before* Popen.
    # A slow detached Host can outlive an MCP exit without publishing health,
    # so even empty registries (or terminal Holds) are not host-exit evidence.
    if {"task-holds", "task-monitors"}.issubset({path.name for path in parents}):
        raise LifecycleError("Hold Host bootstrap footprint has no run-bound exit receipt")
    for path in parents:
        if path.name in _HEARTBEAT_FILES:
            continue
        if path.name == "hold-host.json":
            raise LifecycleError("Detached Hold Host has no run-bound exit receipt; execution remains unknown")
        if path.name not in {"task-holds", "task-monitors", "loops"}:
            raise LifecycleError("Unsupported runtime evidence requires explicit reconciliation: " + path.name)
        if path.name == "loops":
            continue
        roots = directory(path)
        if not roots:
            raise LifecycleError("Empty holder registry may be an unconfirmed detached Host startup")
        for root in roots:
            files = directory(root)
            # Claims and archived generations are not physical-exit evidence.
            if any(item.is_dir() or item.name == ".user-host-claim" for item in files):
                raise LifecycleError("Holder claim or nested generation requires exact owner reconciliation")
            allowed = {"request.json", "ack.json", "result.json", "close.json", "cancellation.json",
                       "monitor-events.jsonl", "monitor-notification-deliveries.jsonl",
                       ".dispatch-gate", ".close-report.guard", "request.lock"}
            if any(item.name not in allowed for item in files):
                raise LifecycleError("Unsupported holder evidence requires explicit reconciliation")
            request = record(root / "request.json")
            ack = record(root / "ack.json")
            result = record(root / "result.json")
            request_id = request.get("request_id")
            hold_id = request.get("hold_id") or request.get("monitor_id")
            if (not isinstance(request_id, str) or not request_id or not isinstance(hold_id, str) or not hold_id
                    or request.get("resume_pause") or hold_id in holds):
                raise LifecycleError("Holder identity is missing, duplicated or paused")
            for receipt in (ack, result):
                if (receipt.get("request_id") != request_id
                        or (receipt.get("hold_id") or receipt.get("monitor_id")) != hold_id):
                    raise LifecycleError("Holder request/receipt generation mismatch")
            if not _terminal(result):
                raise LifecycleError("Holder execution is active or unresolved")
            if result.get("local_observer_stopped") is not True:
                raise LifecycleError("Holder observer exit is not confirmed")
            # Native identity must agree; the explicit no-dispatch case is allowed.
            for key in ("thread_id", "turn_id"):
                actual = result.get(key)
                if request.get(key) not in (None, "", actual) or ack.get(key) not in (None, "", actual):
                    raise LifecycleError("Holder native execution identity mismatch")
            if result.get("turn_id") and not result.get("thread_id"):
                raise LifecycleError("Holder terminal turn lacks its native thread identity")
            if not result.get("turn_id") and result.get("non_dispatch_confirmed") is not True:
                raise LifecycleError("Holder has no exact terminal turn or explicit non-dispatch proof")
            released = result.get("hold_released") is True
            close_path = root / "close.json"
            if close_path.exists():
                close = record(close_path)
                if (close.get("request_id") != request_id or close.get("hold_id") != hold_id
                        or close.get("terminal_confirmed") is not True
                        or close.get("hold_released") is not True
                        or close.get("external_execution_unresolved") is True
                        or close.get("execution_state") not in {"terminal", "not_started"}
                        or close.get("report_status") != "completed"):
                    raise LifecycleError("Holder close receipt is unresolved or belongs to another generation")
                released = True
            if not released:
                raise LifecycleError("Exact holder release is not confirmed")
            from jarvis_runtime.holder_transport_release import holder_transport_cleanup_confirmed
            if result.get("host_stop") is True and not (
                    result.get("holder_exit_confirmed") is True and holder_transport_cleanup_confirmed(result, request)):
                raise LifecycleError("Controlled holder stop lacks owned exit proof")
            if ack.get("status") not in _TERMINAL:
                raise LifecycleError("Holder acknowledgment still reports active or unknown execution")
            if any(item.name == "request.lock" for item in files):
                raise LifecycleError("Holder still has an unresolved request lock")
            cancellation = root / "cancellation.json"
            if cancellation.exists():
                value = record(cancellation)
                if value.get("request_id") != request_id or value.get("subject_id") != hold_id:
                    raise LifecycleError("Holder cancellation identity mismatch")
            holds.add(hold_id)

    loops = state_dir / "loops"
    if loops.exists():
        for root in directory(loops):
            if root.name == "schemas":
                # Registered output schemas do not own execution, but malformed or
                # unexpected layouts still cannot be silently treated as empty.
                for item in directory(root):
                    if item.suffix != ".json":
                        raise LifecycleError("Unrecognized loop schema registry entry")
                    record(item)
                continue
            files = directory(root)
            if any(item.name not in {"state.json", "cancellation.json", ".dispatch-gate", ".close-report.guard"}
                   or item.is_dir() for item in files):
                raise LifecycleError("Unrecognized loop control evidence")
            state = record(root / "state.json")
            cleanup = state.get("cleanup")
            children = state.get("children")
            if (state.get("schema") != "jarvis-loop-state/v1" or not state.get("loop_id")
                    or not state.get("request_id") or state.get("status") not in {"completed", "stopped", "expired", "blocked"}
                    or not isinstance(cleanup, dict) or cleanup.get("status") != "completed"
                    or cleanup.get("heartbeat_cancelled") is not True or not isinstance(children, list)):
                raise LifecycleError("Loop scheduling or execution is active or unresolved")
            for child in children:
                if not isinstance(child, dict) or child.get("hold_id") not in holds:
                    raise LifecycleError("Loop child lacks an exact released holder")
                receipt = child.get("last_receipt") or {}
                data = receipt.get("data") or {}
                if (receipt.get("status") != "completed" or data.get("terminal_confirmed") is not True
                        or data.get("hold_released") is not True
                        or (data.get("lifecycle_status") or data.get("status")) not in _TERMINAL):
                    raise LifecycleError("Loop child execution lacks terminal/release evidence")
            close = state.get("close")
            if close and (not isinstance(close, dict) or close.get("report_status") != "completed"
                          or close.get("terminal_confirmed") is not True
                          or close.get("external_execution_unresolved") is True):
                raise LifecycleError("Loop close receipt remains unresolved")
            if (root / "cancellation.json").exists():
                cancellation = record(root / "cancellation.json")
                if cancellation.get("request_id") != state["request_id"]:
                    raise LifecycleError("Loop cancellation generation mismatch")
    return hashlib.sha256(json.dumps(sorted(entries), ensure_ascii=True).encode()).hexdigest()


class LifecycleLease:
    """Exclusive managed deployment lease; never delete its stable lock file.

    Caller contract: begin_launch(), spawn exactly one foreground child,
    mark_running(child.pid), wait for that exact child, finish_launch(returncode).
    Keep this context alive throughout. On spawn/wait errors leave the recorded
    state unresolved. No method in this class launches or stops a process.
    """

    def __init__(self, install_root: str | Path):
        self.install_root = _safe_path(Path(install_root))
        self.metadata_path = self.install_root / METADATA_FILENAME
        self.lock_path = self.install_root / LOCK_FILENAME
        self._handle: BinaryIO | None = None
        self._owner_pid: int | None = None
        self._run_id: str | None = None

    def __enter__(self):
        if self._handle is not None:
            raise LifecycleError("Lifecycle lease is not reentrant")
        _safe_path(self.install_root)
        _safe_path(self.lock_path)
        if not self.install_root.is_dir():
            raise LifecycleError("Managed installation root must already exist")
        handle = None
        try:
            fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
            handle = os.fdopen(fd, "r+b")
            os.set_inheritable(handle.fileno(), False)
            if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                raise LifecycleError("Lifecycle lease is not a regular file")
            _lock_file(handle)
            self._handle = handle
            self._owner_pid = os.getpid()
            self._require_held()
            return self
        except OSError as exc:
            if handle is not None:
                handle.close()
            self._handle = None
            if exc.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                raise LifecycleBusyError("Managed installation is already launching, running, or updating") from exc
            raise LifecycleError("Cannot acquire managed lifecycle lease") from exc
        except BaseException:
            if handle is not None:
                handle.close()
            self._handle = None
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        handle, self._handle = self._handle, None
        if handle is not None:
            try:
                # A forked descendant must not unlock its parent's shared flock.
                if self._owner_pid == os.getpid():
                    _unlock_file(handle)
            finally:
                handle.close()
        self._run_id = None

    def _require_held(self) -> None:
        if self._handle is None or self._handle.closed or self._owner_pid != os.getpid():
            raise LifecycleError("A lifecycle operation requires this process's held lease")
        _safe_path(self.install_root)
        _safe_path(self.lock_path)
        try:
            if not os.path.samestat(os.fstat(self._handle.fileno()), self.lock_path.stat()):
                raise LifecycleError("Lifecycle lease file changed while held")
        except OSError as exc:
            raise LifecycleError("Lifecycle lease identity is unavailable") from exc

    def _write(self, value: dict[str, Any], *, create: bool = False) -> dict[str, Any]:
        self._require_held()
        _safe_path(self.metadata_path)
        if create and self.metadata_path.exists():
            raise LifecycleError("Managed lifecycle metadata already exists; no adoption or reset")
        temporary = self.install_root / (".lifecycle-" + uuid.uuid4().hex + ".tmp")
        try:
            raw = (json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True) + "\n").encode()
            with temporary.open("xb") as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            if create:
                # Exclusive publication avoids replacing a concurrently supplied record.
                os.link(temporary, self.metadata_path)
                temporary.unlink()
            else:
                os.replace(temporary, self.metadata_path)
            _sync_directory(self.install_root)
            if _json(self.metadata_path)[0] != value:
                raise LifecycleError("Lifecycle metadata failed durable readback")
            return value
        except OSError as exc:
            raise LifecycleError("Cannot persist managed lifecycle metadata") from exc
        finally:
            temporary.unlink(missing_ok=True)

    def _read(self) -> dict[str, Any]:
        self._require_held()
        value, _ = _json(self.metadata_path)
        if (value.get("schema") != SCHEMA or value.get("managed") is not True
                or value.get("install_root") != str(self.install_root)
                or not isinstance(value.get("installation_id"), str) or not value["installation_id"]
                or value.get("state") not in {"stopped", "starting", "running", "unknown"}
                or not isinstance(value.get("state_dir"), str)):
            raise LifecycleError("Legacy, unmanaged or invalid lifecycle metadata cannot be updated")
        state = _safe_path(Path(value["state_dir"]))
        if str(state) != value["state_dir"] or not state.is_relative_to(self.install_root) or state == self.install_root:
            raise LifecycleError("Managed state directory is outside the installation")
        return value

    def initialize_managed(self, state_dir: str | Path) -> dict[str, Any]:
        """Initialize only a brand-new install and empty internal runtime directory."""
        self._require_held()
        _safe_path(self.metadata_path)
        if self.metadata_path.exists():
            raise LifecycleError("Managed lifecycle metadata already exists; no adoption or reset")
        state = _safe_path(Path(state_dir))
        if state == self.install_root or not state.is_relative_to(self.install_root):
            raise LifecycleError("New managed runtime state must be inside the installation")
        try:
            state.mkdir(parents=True, exist_ok=True)
            if next(state.iterdir(), None) is not None:
                raise LifecycleError("Existing runtime state cannot be adopted as stopped")
        except OSError as exc:
            raise LifecycleError("Cannot initialize managed runtime state directory") from exc
        return self._write({"schema": SCHEMA, "managed": True, "installation_id": str(uuid.uuid4()),
                            "install_root": str(self.install_root), "state_dir": str(state),
                            "state": "stopped", "updated_at": _now(), "run_id": None,
                            "stop_proof": {"kind": "never_started", "runtime_sha256": _runtime_snapshot(state)}}, create=True)

    def assert_update_safe(self) -> dict[str, Any]:
        """Verify stopped proof while holding the lease; has no recovery effects."""
        value = self._read()
        proof = value.get("stop_proof")
        if value["state"] != "stopped" or not isinstance(proof, dict):
            raise LifecycleError("Managed installation is not proven normally stopped; no update or rollback")
        if proof.get("kind") not in {"never_started", "normal_exit"}:
            raise LifecycleError("Managed installation has no recognized stopped proof")
        if proof["kind"] == "normal_exit" and (proof.get("exit_code") != 0
                or type(proof.get("exit_code")) is not int
                or not value.get("run_id") or proof.get("run_id") != value["run_id"]):
            raise LifecycleError("Normal-stop proof is not bound to the completed launch")
        if proof["kind"] == "never_started" and value.get("run_id") is not None:
            raise LifecycleError("Never-started proof conflicts with a recorded launch")
        if _runtime_snapshot(Path(value["state_dir"])) != proof.get("runtime_sha256"):
            raise LifecycleError("Runtime evidence changed after normal stop; execution is unproven")
        return value

    def begin_launch(self) -> dict[str, Any]:
        value = self.assert_update_safe()
        self._run_id = str(uuid.uuid4())
        value.update(state="starting", run_id=self._run_id, supervisor_pid=os.getpid(),
                     child_pid=None, updated_at=_now(), stop_proof=None)
        return self._write(value)

    def mark_running(self, child_pid: int) -> dict[str, Any]:
        value = self._read()
        if (value["state"] != "starting" or self._run_id is None or value.get("run_id") != self._run_id
                or type(child_pid) is not int or child_pid <= 0 or child_pid == os.getpid()):
            raise LifecycleError("Running receipt requires this launch's exact direct child")
        value.update(state="running", child_pid=child_pid, updated_at=_now())
        return self._write(value)

    def finish_launch(self, exit_code: int) -> dict[str, Any]:
        """Record stop only after caller waited for its exact foreground child.

        A nonzero exit or unresolved runtime returns state=unknown. It does not
        stop, reap, interrupt, recover, or otherwise change any runtime process.
        """
        value = self._read()
        if (value["state"] != "running" or self._run_id is None or value.get("run_id") != self._run_id
                or type(exit_code) is not int):
            raise LifecycleError("Completion requires this lease's running launch and actual child returncode")
        proof = None
        reason = "Direct child did not exit successfully"
        if exit_code == 0:
            try:
                digest = _runtime_snapshot(Path(value["state_dir"]))
                proof = {"kind": "normal_exit", "run_id": self._run_id,
                         "exit_code": 0, "runtime_sha256": digest}
            except (LifecycleError, OSError, ValueError, TypeError) as exc:
                reason = str(exc)
        value.update(state="stopped" if proof else "unknown", updated_at=_now(),
                     exit_code=exit_code, stop_proof=proof)
        if proof:
            value.pop("reason", None)
        else:
            value["reason"] = reason
        return self._write(value)
