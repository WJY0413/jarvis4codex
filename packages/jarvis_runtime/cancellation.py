"""Durable local scheduling cancellation. Never proves process or turn exit."""
from __future__ import annotations
from contextlib import contextmanager, ExitStack
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import time
import uuid
from urllib.parse import unquote

MARKER = "cancellation.json"


class DispatchCancelledBeforeSend(RuntimeError):
    """This exact turn command was never committed or written to the native pipe."""


class DispatchAlreadyCommitted(RuntimeError):
    """A prior commit exists; delivery and consumption must be read back, never retried."""


def _loop_root(root: Path, request: dict | None) -> Path | None:
    source = str((request or {}).get("source_ref") or "")
    def path_for(loop_id):
        safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in loop_id).strip("._") or "loop"
        return root.parents[1] / "loops" / safe
    if source.startswith("jarvis_loop_task:"):
        return path_for(unquote(source.removeprefix("jarvis_loop_task:")))
    if not source.startswith("jarvis_loop:"):
        return None
    tail = source.removeprefix("jarvis_loop:")
    parts = tail.split(":")
    matches = []
    # Legacy sources did not escape colons. Resolve their exact stored Loop/slot.
    for split in range(1, len(parts)):
        loop_id, slot = unquote(":".join(parts[:split])), ":".join(parts[split:])
        candidate = path_for(loop_id)
        state_path = candidate / "state.json"
        if not state_path.is_file(): continue
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if state.get("loop_id") == loop_id and any(str(c.get("slot")) == slot for c in state.get("children", [])):
            matches.append(candidate)
    if len(set(matches)) > 1:
        raise RuntimeError("ambiguous legacy parent Loop identity; dispatch remains blocked")
    if matches: return matches[0]
    # New requests encode the Loop ID; during first acquisition state may not exist yet.
    return path_for(unquote(parts[0]))


def _recorded_roots(root: Path, request: dict | None = None) -> list[Path]:
    parent = _loop_root(root, request)
    roots = [parent, root] if parent is not None and parent != root else [root]
    for relative in (request or {}).get("dispatch_ancestors", []):
        if not isinstance(relative, str): raise RuntimeError("invalid dispatch ancestor")
        parts = PurePosixPath(relative).parts
        if (len(parts) != 2 or parts[0] not in {"task-holds", "task-monitors", "loops"}
                or parts[1] in {".", ".."} or "\\" in relative):
            raise RuntimeError("dispatch ancestor is outside this state registry")
        ancestor = root.parents[1].joinpath(*parts)
        if ancestor.is_symlink() or ancestor.parent.is_symlink():
            raise RuntimeError("symlink dispatch ancestor rejected")
        roots.append(ancestor)
    return roots


def _thread_bindings(root: Path, thread_id: str):
    """Resolve aliases from current and legacy receipts; unknown JSON fails closed."""
    for name in ("task-holds", "task-monitors"):
        parent = root.parents[1] / name
        if parent.is_symlink():
            raise RuntimeError("symlink state registry rejected")
        if not parent.is_dir():
            continue
        for alias in parent.iterdir():
            if alias.is_symlink():
                raise RuntimeError("symlink thread alias rejected")
            if not alias.is_dir():
                continue
            records = {}
            for filename in ("request.json", "ack.json", "result.json"):
                path = alias / filename
                if not os.path.lexists(path):
                    continue
                if path.is_symlink():
                    raise RuntimeError("symlink thread binding rejected")
                value = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(value, dict):
                    raise RuntimeError("invalid thread binding record")
                records[filename] = value
            if any(value.get("thread_id") == thread_id for value in records.values()):
                yield alias, records.get("request.json", {})


def dispatch_roots(root: Path, request: dict | None = None) -> list[Path]:
    roots = _recorded_roots(root, request)
    thread_id = (request or {}).get("thread_id")
    if thread_id:
        for alias, saved in _thread_bindings(root, thread_id):
            roots.extend(_recorded_roots(alias, saved))
    return sorted(set(roots), key=lambda value: str(value.absolute()))


def cancellation_path(root: Path, request: dict | None = None) -> Path | None:
    """Malformed markers fail closed; an own marker needs no parent resolution."""
    own = root / MARKER
    if os.path.lexists(own): return own
    for candidate in dispatch_roots(root, request):
        marker = candidate / MARKER
        if os.path.lexists(marker): return marker
    return None


def require_dispatch_open(root: Path, request: dict | None = None) -> None:
    if cancellation_path(root, request) is not None:
        raise DispatchCancelledBeforeSend("Jarvis management is cancelled; use a new explicitly authorized task identity")


def _publish_cancellation(root: Path, *, scope: str, subject_id: str, request_id: str,
                      close_request_id: str, original: bytes) -> dict:
    """Publish one immutable cancellation atomically without PID-based lock recovery."""
    if scope not in {"hold", "loop"} or not subject_id or not request_id:
        raise ValueError("exact cancellation scope and identity required")
    root.mkdir(parents=True, exist_ok=True)
    target = root / MARKER
    value = {"schema": "jarvis-management-cancellation/v1", "scope": scope,
             "subject_id": subject_id, "request_id": request_id,
             "close_request_id": close_request_id, "dispatch_cancelled": True,
             "original_sha256": hashlib.sha256(original).hexdigest(),
             "requested_at": datetime.now(timezone.utc).isoformat()}
    raw = (json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n").encode()
    temporary = root / (".cancel-" + uuid.uuid4().hex + ".tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(raw); handle.flush(); os.fsync(handle.fileno())
        try:
            os.link(temporary, target)
        except FileExistsError:
            pass
        if os.name != "nt":
            fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            try: os.fsync(fd)
            finally: os.close(fd)
        if target.is_symlink():
            raise RuntimeError("cancellation marker is not a regular owned record")
        saved = json.loads(target.read_text(encoding="utf-8"))
        if any(saved.get(k) != value[k] for k in ("schema", "scope", "subject_id", "request_id", "dispatch_cancelled")):
            raise RuntimeError("existing cancellation identity differs; dispatch remains blocked")
        return saved
    finally:
        temporary.unlink(missing_ok=True)


def _thread_gate(root: Path, thread_id: str) -> Path:
    if not isinstance(thread_id, str) or not thread_id:
        raise RuntimeError("exact native thread identity required")
    key = hashlib.sha256(thread_id.encode("utf-8")).hexdigest()
    return root.parents[1] / (".dispatch-thread-" + key)


@contextmanager
def _thread_guard(root: Path, thread_ids):
    with ExitStack() as guards:
        for thread_id in sorted(set(thread_ids)):
            guards.enter_context(report_guard(_thread_gate(root, thread_id)))
        yield


@contextmanager
def _hold_guard(root: Path, request: dict | None = None):
    """Native identities first, then exact Hold/Loop; never wait on request.lock here."""
    if root.is_symlink() or root.parent.is_symlink():
        raise RuntimeError("symlink state registry rejected")
    records = [request or {}]
    for name in ("request.json", "ack.json", "result.json"):
        path = root / name
        if os.path.lexists(path):
            if path.is_symlink():
                raise RuntimeError("symlink thread binding rejected")
            value = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(value, dict):
                raise RuntimeError("invalid Hold identity record")
            records.append(value)
    identities = [value["thread_id"] for value in records if value.get("thread_id")]
    with _thread_guard(root, identities), report_guard(root / ".dispatch-gate"):
        yield


@contextmanager
def _dispatch_guard(root: Path, request: dict):
    """Re-resolve all aliases inside the native boundary, then lock complete ancestry."""
    with _thread_guard(root, [request["thread_id"]]):
        roots = dispatch_roots(root, request)
        with ExitStack() as guards:
            for scope in roots:
                guards.enter_context(report_guard(scope / ".dispatch-gate"))
            yield roots


def _require_roots_open(roots):
    if any(os.path.lexists(root / MARKER) for root in roots):
        raise DispatchCancelledBeforeSend("Jarvis management is cancelled; use a new explicitly authorized task identity")


def cancel_management(root: Path, **kwargs) -> dict:
    with _hold_guard(root):
        if kwargs.get("scope") == "hold":
            current = json.loads((root / "request.json").read_text(encoding="utf-8"))
            if current.get("request_id") != kwargs.get("request_id") or (current.get("hold_id") or current.get("monitor_id")) != kwargs.get("subject_id"):
                raise RuntimeError("Hold identity changed before cancellation commit")
        return _publish_cancellation(root, **kwargs)


def commit_dispatch(root: Path, request: dict, params: dict) -> dict:
    """Linearize one actual turn/start before cancellation; never retry a commit."""
    binding = {**request, "thread_id": params.get("threadId")}
    with _dispatch_guard(root, binding) as roots:
        current = json.loads((root / "request.json").read_text(encoding="utf-8"))
        if current.get("request_id") != request.get("request_id") or current.get("hold_id") != request.get("hold_id"):
            raise RuntimeError("dispatch request identity changed")
        if any(value.get("thread_id") and value["thread_id"] != params.get("threadId")
               for value in (current, request)):
            raise RuntimeError("dispatch native thread identity changed")
        identity = {"request_id": request["request_id"], "hold_id": request.get("hold_id"),
                    "thread_id": params.get("threadId"), "client_user_message_id": params.get("clientUserMessageId")}
        if not identity["thread_id"] or not identity["client_user_message_id"]:
            raise RuntimeError("dispatch commit requires actual thread and user-message identity")
        key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        directory = root / "dispatch-commits"
        target = directory / (key + ".json")
        if os.path.lexists(target):
            raise DispatchAlreadyCommitted("native turn command already committed; read back without replay")
        _require_roots_open(roots)
        directory.mkdir(exist_ok=True)
        value = {"schema": "jarvis-dispatch-commit/v1", **identity,
                 "params_sha256": hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest(),
                 "committed_at": datetime.now(timezone.utc).isoformat(),
                 "delivery_confirmed": False, "automatic_retry": False}
        # An existing intent is deliberately not replayed, even if the prior pipe write failed.
        with target.open("x", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False); handle.flush(); os.fsync(handle.fileno())
        if os.name != "nt":
            fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try: os.fsync(fd)
            finally: os.close(fd)
        return value


@contextmanager
def report_guard(path: Path, timeout: float = 0.2):
    """A kernel-held report lock; never inspect, signal or reap a recorded PID."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        if os.name == "nt":
            import msvcrt
            if handle.tell() == 0:
                handle.write(b"0"); handle.flush()
            def acquire():
                handle.seek(0); msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            def release():
                handle.seek(0); msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            def acquire(): fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            def release(): fcntl.flock(handle, fcntl.LOCK_UN)
        deadline = time.monotonic() + timeout
        while True:
            try:
                acquire(); break
            except (BlockingIOError, OSError):
                if time.monotonic() >= deadline:
                    raise RuntimeError("state gate is busy; protected operation did not complete")
                time.sleep(0.01)
        try: yield
        finally: release()
