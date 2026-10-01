"""Owned, deferred dispatch lifecycle over the normal durable bridge boundary.

Factories are fixed by trusted product wiring, never accepted from a public request.
A deferred owner pauses only after its ordinary starting intent has been fsynced.
Stopping that owner leaves the intent untouched for explicit conservative recovery.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any, Mapping
import uuid

from .contracts import BridgeReceipt, ResumeRequest, utc_now
from .journal import JsonlReceiptJournal, file_lock, fsync_directory, owner_alive, process_identity
from .service import ExistingThreadBridge, request_digest


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(dict(value), handle, ensure_ascii=False, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
        fsync_directory(path.parent.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def _read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("owned dispatch metadata must be an object")
    return value


class OwnedDispatchManager:
    """Spawn and control only this product's exact scope-bound dispatch owners."""

    def __init__(self, bridge: ExistingThreadBridge, state_dir: Path, *,
                 transport_factory: str, transport_options: Mapping[str, Any]):
        if not isinstance(bridge.journal, JsonlReceiptJournal):
            raise ValueError("owned dispatch requires a durable JSONL receipt journal")
        self.bridge = bridge
        self.state_dir = Path(state_dir).resolve()
        self.transport_factory = transport_factory
        self.transport_options = dict(transport_options)
        self._children: dict[str, subprocess.Popen] = {}

    def _directory(self, request_id: str) -> Path:
        if not isinstance(request_id, str) or not request_id.strip():
            raise ValueError("request_id is required")
        return self.state_dir / hashlib.sha256(request_id.encode("utf-8")).hexdigest()

    def _descriptor(self, request_id: str, owner_scope: str) -> tuple[Path, dict[str, Any]]:
        if not owner_scope.strip():
            raise ValueError("owner_scope is required")
        directory = self._directory(request_id)
        descriptor = _read(directory / "request.json")
        if descriptor["request"]["request_id"] != request_id or descriptor["owner_scope"] != owner_scope:
            raise ValueError("request and owner scope do not match this owned dispatch")
        return directory, descriptor

    def start(self, request: ResumeRequest, *, owner_scope: str,
              pause_before_dispatch: bool = True) -> dict[str, Any]:
        if not owner_scope.strip():
            raise ValueError("owner_scope is required")
        if not isinstance(pause_before_dispatch, bool):
            raise ValueError("pause_before_dispatch must be boolean")
        directory = self._directory(request.request_id)
        with file_lock(directory / "lifecycle.lock"):
            if (directory / "request.json").exists():
                _, descriptor = self._descriptor(request.request_id, owner_scope)
                if descriptor["request_digest"] != request_digest(request):
                    raise ValueError("request_id is already bound to a different dispatch")
                return self.read(request.request_id, owner_scope=owner_scope)
            previous = self.bridge.read_dispatch(request.request_id)
            if previous is not None:
                raise ValueError("request_id already has a dispatch receipt; no replacement owner may be created")
            owner_id = str(uuid.uuid4())
            descriptor = {
                "schema": "jarvis-owned-dispatch/v1", "owner_id": owner_id,
                "owner_scope": owner_scope, "request": asdict(request),
                "request_digest": request_digest(request),
                "pause_before_dispatch": pause_before_dispatch,
                "transport_factory": self.transport_factory,
                "transport_options": self.transport_options,
                "journal_path": str(self.bridge.journal.path.resolve()),
                "created_at": utc_now().isoformat(),
            }
            _write(directory / "request.json", descriptor)
            env = dict(os.environ)
            package_root = str(Path(__file__).resolve().parents[1])
            env["PYTHONPATH"] = package_root + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
            # The child command and entry point are product-owned; no shell or user command.
            log_fd = os.open(directory / "owner.log", os.O_CREAT | os.O_APPEND | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0), 0o600)
            try:
                child = subprocess.Popen(
                    [sys.executable, "-m", "jarvis_codex_bridge.dispatch_owner", str(directory / "request.json")],
                    stdin=subprocess.DEVNULL, stdout=log_fd, stderr=log_fd,
                    env=env, close_fds=True, start_new_session=(os.name != "nt"),
                )
            except Exception as exc:
                _write(directory / "state.json", {"owner_id": owner_id, "phase": "owner_failed",
                                                  "reason": str(exc), "observed_at": utc_now().isoformat()})
                raise
            finally:
                os.close(log_fd)
            self._children[request.request_id] = child
            _write(directory / "spawn.json", {"owner_id": owner_id, **process_identity(child.pid)})
        return self.read(request.request_id, owner_scope=owner_scope)

    def read(self, request_id: str, *, owner_scope: str) -> dict[str, Any]:
        directory, descriptor = self._descriptor(request_id, owner_scope)
        child = self._children.get(request_id)
        if child is not None:
            child.poll()  # Reap only the exact process this manager itself spawned.
        state = _read(directory / "state.json") if (directory / "state.json").exists() else {}
        spawn = _read(directory / "spawn.json") if (directory / "spawn.json").exists() else {}
        if state and state.get("owner_id") != descriptor["owner_id"]:
            raise ValueError("owner-state identity mismatch")
        if spawn and spawn.get("owner_id") != descriptor["owner_id"]:
            raise ValueError("spawn identity mismatch")
        identity = state.get("owner") or spawn or None
        alive = owner_alive(identity)
        receipt = self.bridge.read_dispatch(request_id)
        if receipt is not None and (receipt.owner or {}).get("owner_id") != descriptor["owner_id"]:
            raise ValueError("dispatch receipt is not owned by this owner")
        phase = state.get("phase", "owner_starting")
        status = receipt.status if receipt is not None else (state.get("result") or {}).get("status", "starting")
        if phase == "owner_failed" and receipt is None:
            status = "failed"
        return {
            "schema": "jarvis-owned-dispatch-readback/v1", "request_id": request_id,
            "owner_scope": owner_scope, "owner_id": descriptor["owner_id"],
            "thread_id": descriptor["request"]["thread_id"], "owner": identity,
            "owner_alive": alive, "owner_phase": phase, "status": status,
            "pause_before_dispatch": descriptor["pause_before_dispatch"],
            "boundary": "after_fsync_starting_before_transport_dispatch",
            "receipt": receipt.as_dict() if receipt is not None else None,
            "owner_result": state.get("result"),
            "reason": state.get("reason"),
            "receipt_ref": str(self.bridge.journal.path.resolve()),
            "owner_evidence_ref": str(directory / "state.json"),
        }

    def _decide(self, request_id: str, owner_scope: str, command: str) -> dict[str, Any]:
        directory, descriptor = self._descriptor(request_id, owner_scope)
        with file_lock(directory / "lifecycle.lock"):
            current = self.read(request_id, owner_scope=owner_scope)
            decision_path = directory / "decision.json"
            if decision_path.exists():
                decision = _read(decision_path)
                if decision.get("owner_id") != descriptor["owner_id"] or decision.get("command") != command:
                    raise ValueError("owned dispatch already has a different irreversible boundary decision")
                return current
            if (current["owner_alive"] is not True or current["owner_phase"] != "boundary_paused"
                    or current["status"] != "starting"):
                raise ValueError("only the live owned dispatch paused at its durable boundary can be controlled")
            _write(decision_path, {"owner_id": descriptor["owner_id"], "owner_scope": owner_scope,
                                   "command": command, "observed_at": utc_now().isoformat()})
        return self.read(request_id, owner_scope=owner_scope)

    def release(self, request_id: str, *, owner_scope: str) -> dict[str, Any]:
        return self._decide(request_id, owner_scope, "release")

    def stop(self, request_id: str, *, owner_scope: str) -> dict[str, Any]:
        current = self._decide(request_id, owner_scope, "stop")
        deadline = time.monotonic() + 5.0
        while current["owner_alive"] is not False and time.monotonic() < deadline:
            time.sleep(0.05)
            current = self.read(request_id, owner_scope=owner_scope)
        # Never claim exit on a command receipt alone, and never signal a shared host.
        return current

    def recover(self, request_id: str, *, owner_scope: str) -> dict[str, Any]:
        current = self.read(request_id, owner_scope=owner_scope)
        if current["receipt"] is None:
            raise ValueError("no durable starting record exists for this owned dispatch")
        recovery = self.bridge.recover_dispatch(request_id)
        result = self.read(request_id, owner_scope=owner_scope)
        result["recovery"] = recovery.as_dict()
        result["status"] = recovery.status
        result["reason"] = recovery.reason
        return result


class _StoppedAtBoundary(BaseException):
    pass


def _run_owner(descriptor_path: Path) -> int:
    descriptor = _read(descriptor_path)
    directory = descriptor_path.parent
    owner = {"owner_id": descriptor["owner_id"], "owner_scope": descriptor["owner_scope"], **process_identity()}

    def state(phase: str, *, reason: str | None = None, result: dict[str, Any] | None = None) -> None:
        _write(directory / "state.json", {"owner_id": descriptor["owner_id"], "owner": owner,
                                          "phase": phase, "reason": reason, "result": result,
                                          "observed_at": utc_now().isoformat()})

    state("preparing")
    try:
        module_name, name = descriptor["transport_factory"].split(":", 1)
        factory = getattr(importlib.import_module(module_name), name)
        transport = factory(dict(descriptor["transport_options"]))
        bridge = ExistingThreadBridge(transport, JsonlReceiptJournal(Path(descriptor["journal_path"])))
        request = ResumeRequest(**descriptor["request"])
        if request_digest(request) != descriptor["request_digest"]:
            raise ValueError("owned request digest mismatch")

        def boundary(receipt: BridgeReceipt) -> None:
            if receipt.owner != owner or receipt.status != "starting":
                raise ValueError("durable dispatch owner identity mismatch")
            if not descriptor["pause_before_dispatch"]:
                state("dispatching")
                return
            state("boundary_paused")
            while True:
                decision_path = directory / "decision.json"
                if decision_path.exists():
                    decision = _read(decision_path)
                    if decision.get("owner_id") != owner["owner_id"] or decision.get("owner_scope") != owner["owner_scope"]:
                        raise ValueError("boundary control does not belong to this dispatch owner")
                    if decision.get("command") == "stop":
                        raise _StoppedAtBoundary()
                    if decision.get("command") == "release":
                        state("dispatching")
                        return
                    raise ValueError("unknown dispatch boundary command")
                time.sleep(0.1)

        receipt = bridge.resume_existing(request, owner=owner, dispatch_boundary=boundary)
        state("owner_completed" if receipt.status == "completed" else "owner_finished",
              reason=receipt.reason, result=receipt.as_dict())
        return 0
    except _StoppedAtBoundary:
        # The normal starting receipt is deliberately retained unchanged.
        state("owner_stopped", reason="owned process stopped after durable intent and before transport dispatch")
        return 0
    except Exception as exc:
        state("owner_failed", reason=str(exc))
        return 1


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("owned dispatch expects one product-created descriptor path")
    raise SystemExit(_run_owner(Path(sys.argv[1])))
