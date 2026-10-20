"""Opt-in, one-turn control ledger for an already pinned Unix owner.

This is NOT a legacy stdio migration, a server launcher, or a warm-resume API.
An unanswered dispatch/interrupt is never replayed. Kernel flock ownership is
held for the entire controller lifetime; elapsed time and PID absence cannot
steal it. The ledger contains control metadata, not prompts or credentials.
"""
from __future__ import annotations

import argparse
import copy
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import uuid

from jarvis_codex_bridge.journal import fsync_directory
from jarvis_runtime.cancellation import generation_guard
from adapters.codex_app_server.native_execution_evidence import classify_native_turn

SCHEMA = "jarvis-owner-control-ledger/v1"
REQUEST_SCHEMA = "jarvis-owner-control-request/v1"
IDENTITY = ("request_id", "hold_id", "attempt", "thread_id", "turn_id")
TERMINAL = {"completed", "failed", "interrupted"}


class ControlUnavailable(RuntimeError):
    """No mutation was authorized by this control operation."""


class ControlBusy(ControlUnavailable):
    pass


class ControlReplayBlocked(ControlUnavailable):
    pass


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                    allow_nan=False).encode()).hexdigest()


def read_json(path):
    path = Path(path)
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd, "r", encoding="utf-8") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ControlUnavailable("control metadata is not a regular file")
        value = json.load(stream)
    if not isinstance(value, dict):
        raise ControlUnavailable("control metadata is not an object")
    return value


def durable_json(path, value):
    """Atomic replace plus file and directory fsync, including the first record."""
    path = Path(path)
    if path.is_symlink():
        raise ControlUnavailable("symlink control metadata rejected")
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, sort_keys=True, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def control_requested(request):
    # A malformed present opt-in may not silently downgrade to legacy behavior.
    return "owner_control" in request


def control_attempt(request):
    option = request.get("owner_control")
    if (not isinstance(option, dict) or set(option) != {"schema", "attempt"}
            or option.get("schema") != REQUEST_SCHEMA
            or type(option.get("attempt")) is not int or option["attempt"] < 1):
        raise ControlUnavailable("owner_control requires an explicit positive attempt")
    return option["attempt"]


def owner_build_version(raw):
    """Only the server-produced UA main prefix, never a client suffix/fallback.

    Originator is variable. A missing, ambiguous, noncanonical or prerelease
    prefix stays unknown; currently only audited 0.159.2 grants history semantics.
    """
    if not isinstance(raw, str):
        return None
    match = re.fullmatch(r"([A-Za-z0-9_.-]+)/((?:0|[1-9][0-9]*)\."
                         r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*))"
                         r" \([^\r\n]*\)(?: [^\r\n]*)?", raw)
    return match.group(2) if match else None


def _pin_valid(pin):
    return (isinstance(pin, dict)
            and set(pin) == {"schema", "process", "socket", "instance_id"}
            and pin.get("schema") == "jarvis-unix-owner/v1"
            and isinstance(pin.get("process"), dict) and isinstance(pin.get("socket"), dict)
            and pin.get("instance_id") == digest({"process": pin["process"], "socket": pin["socket"]}))


def _claim_snapshot(root):
    directory = root / ".user-host-claim"
    if not os.path.lexists(directory):
        return None
    if directory.is_symlink():
        raise ControlUnavailable("symlink claim rejected")
    info = directory.stat()
    owner = read_json(directory / "owner.json")
    return {"dev": info.st_dev, "ino": info.st_ino, "owner_digest": digest(owner),
            "request_id": owner.get("request_id")}


class ControlLease:
    """One non-stealable controller, with durable monotonic fencing generation."""

    def __init__(self, root, *, existing_only=False):
        self.root = Path(root).absolute()
        self.path = self.root / "owner-control.json"
        self.lock_path = self.root / "owner-control.lock"
        self.fd = None
        self.state = None
        self.token = None
        self.existing_only = existing_only

    def __enter__(self):
        if os.name != "posix":
            raise ControlUnavailable("Unix owner control requires POSIX flock")
        if self.root.is_symlink() or not self.root.is_dir():
            raise ControlUnavailable("exact existing Hold directory required")
        import fcntl
        self.fd = os.open(self.lock_path, (0 if self.existing_only else os.O_CREAT) | os.O_RDWR |
                          getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            if not stat.S_ISREG(os.fstat(self.fd).st_mode):
                raise ControlUnavailable("control lock is not a regular file")
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException as exc:
            os.close(self.fd)
            self.fd = None
            if isinstance(exc, BlockingIOError):
                raise ControlBusy("exact task still has a live control lease") from exc
            raise
        return self

    def __exit__(self, *_):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def _lock_check(self):
        if self.fd is None:
            raise ControlUnavailable("controller lease is closed")
        opened, current = os.fstat(self.fd), self.lock_path.lstat()
        if ((opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino)
                or not stat.S_ISREG(current.st_mode)):
            raise ControlUnavailable("control lock identity changed")

    def _request_check(self, state):
        request = read_json(self.root / "request.json")
        identity = state["identity"]
        if (request.get("request_id") != identity["request_id"]
                or request.get("hold_id") != identity["hold_id"]
                or control_attempt(request) != identity["attempt"]
                or any(request.get(k) not in (None, "", identity[k])
                       for k in ("thread_id", "turn_id"))):
            raise ControlUnavailable("current Hold generation/attempt/target changed")
        binding = request.get("owner_transport_binding")
        if binding is not None and (not isinstance(binding, dict)
                or binding.get("owner_instance_id") != state["owner_pin"]["instance_id"]
                or any(binding.get(k) != identity[k] for k in ("request_id", "hold_id"))):
            raise ControlUnavailable("current Hold owner pin changed")
        stop = request.get("host_stop")
        if stop is not None and (not isinstance(stop, dict)
                or any(stop.get(k) != identity[k] for k in
                       ("request_id", "hold_id", "thread_id", "turn_id"))):
            raise ControlUnavailable("current stop target conflicts with control ledger")
        return request

    def check(self):
        self._lock_check()
        current = read_json(self.path)
        if current.get("fence") != self.token or current != self.state:
            raise ControlUnavailable("stale controller token or changed ledger")
        self._request_check(current)

    def _save(self):
        self._lock_check()
        durable_json(self.path, self.state)

    def _claim(self):
        prior = self.state.get("fence") or {}
        generation = prior.get("generation", 0)
        if type(generation) is not int or generation < 0:
            raise ControlUnavailable("invalid controller generation")
        self.token = {"generation": generation + 1, "nonce": uuid.uuid4().hex}
        self.state["fence"] = self.token
        self._save()
        self.check()

    def create(self, request, config):
        self._lock_check()
        if self.path.exists():
            raise ControlReplayBlocked("control ledger already exists; query-stop only, never redispatch")
        attempt = control_attempt(request)
        pin = json.loads(config.unix_owner_pin_json or "null")
        if (not _pin_valid(pin) or not config.expected_codex_home
                or config.unix_initial_dispatch is not True
                or getattr(config, "jarvis_connection", None)
                or request.get("mode", "create") != "create"
                or request.get("max_turns", 1) != 1 or request.get("auto_continue", False) is not False
                or request.get("prediction_options")
                or request.get("thread_id") or request.get("turn_id")
                or not all(isinstance(request.get(k), str) and request[k]
                           for k in ("request_id", "hold_id"))):
            raise ControlUnavailable("control ledger admission requires a new pinned single finite turn")
        # Same gate as Host claim admission: no ledger-without-claim / late
        # replacement-claim race between direct Holder and Host initialization.
        with generation_guard(self.root, request):
            if self.path.exists():
                raise ControlReplayBlocked("control ledger already exists; never redispatch")
            claim = _claim_snapshot(self.root)
            if claim is not None and claim["request_id"] != request["request_id"]:
                raise ControlUnavailable("claim does not belong to the exact request")
            self.state = {"schema": SCHEMA, "identity": {
                "request_id": request["request_id"], "hold_id": request["hold_id"],
                "attempt": attempt, "thread_id": "", "turn_id": ""},
                "owner_pin": pin, "expected_codex_home": config.expected_codex_home,
                "claim": claim, "dispatch": {"state": "reserved"},
                "stop": None, "terminal": None, "release": None, "connection": None,
                "attachment_close": None}
            self._request_check(self.state)
            self._claim()

    def load(self):
        self._lock_check()
        if not self.path.is_file():
            raise ControlUnavailable("no original pinned control ledger; legacy stdio cannot be attached")
        self.state = read_json(self.path)
        identity = self.state.get("identity")
        if (self.state.get("schema") != SCHEMA or not _pin_valid(self.state.get("owner_pin"))
                or not isinstance(identity, dict) or set(identity) != set(IDENTITY)
                or not all(isinstance(identity.get(k), str) and identity[k]
                           for k in ("request_id", "hold_id"))
                or type(identity.get("attempt")) is not int or identity["attempt"] < 1
                or not all(isinstance(identity.get(k), str) for k in ("thread_id", "turn_id"))
                or not isinstance(self.state.get("expected_codex_home"), str)
                or not self.state["expected_codex_home"]):
            raise ControlUnavailable("invalid original control ledger")
        self._request_check(self.state)
        fence = self.state.get("fence")
        if (not isinstance(fence, dict) or set(fence) != {"generation", "nonce"}
                or type(fence["generation"]) is not int or fence["generation"] < 1
                or not re.fullmatch(r"[a-f0-9]{32}", str(fence["nonce"]))):
            raise ControlUnavailable("invalid original controller fence")
        self.token = fence
        self.check()

    def recover(self):
        self.load()
        self._claim()

    def bind(self, *, thread_id=None, turn_id=None):
        self.check()
        for key, value in (("thread_id", thread_id), ("turn_id", turn_id)):
            if value is None:
                continue
            previous = self.state["identity"][key]
            if not isinstance(value, str) or not value or previous not in ("", value):
                raise ControlUnavailable("immutable native control identity conflict")
            self.state["identity"][key] = value
        if turn_id is not None:
            if self.state["dispatch"]["state"] != "sent_unknown":
                raise ControlUnavailable("turn acknowledgement has no durable dispatch")
            self.state["dispatch"]["state"] = "acknowledged"
        self._save()

    def rpc_guard(self, method, params):
        self.check()
        if method == "thread/start":
            if self.state["dispatch"]["state"] != "reserved":
                raise ControlReplayBlocked("thread creation is already committed; never replay")
            self.state["dispatch"] = {"state": "thread_start_unknown"}
            self._save()  # Must precede the first byte of thread/start.
        elif method == "turn/start":
            if (self.state["dispatch"]["state"] != "sent_unknown"
                    or self.state["dispatch"].get("params_digest") != digest(params)):
                raise ControlUnavailable("turn/start lacks exact durable dispatch intent")
        elif method == "turn/interrupt":
            identity, stop = self.state["identity"], self.state["stop"]
            if (not stop or stop["state"] != "sent_unknown"
                    or params != {"threadId": identity["thread_id"], "turnId": identity["turn_id"]}):
                raise ControlUnavailable("interrupt lacks exact durable stop intent")
        elif method not in {"initialize", "initialized", "thread/read", "thread/turns/list",
                            "thread/items/list", "thread/loaded/list", "model/list"}:
            raise ControlUnavailable("control ledger forbids resume and other mutating RPCs")

    @contextmanager
    def wire_scope(self, method, params):
        """Linearize the last identity check and wire send with generation changes."""
        self.check()
        request = self._request_check(self.state)
        with generation_guard(self.root, request):
            self.check()
            yield

    def dispatch(self, params):
        self.check()
        identity = self.state["identity"]
        if (self.state["dispatch"]["state"] != "thread_start_unknown"
                or not identity["thread_id"] or identity["turn_id"]
                or params.get("threadId") != identity["thread_id"]
                or params.get("clientUserMessageId") != identity["request_id"]):
            raise ControlReplayBlocked("exact dispatch identity unavailable or already committed")
        self.state["dispatch"] = {"state": "sent_unknown", "params_digest": digest(params)}
        self._save()

    def connection(self, client):
        self.check()
        evidence = client.unix_connection_evidence()
        health = client.transport_health()
        initial = client.initialize_result
        if (evidence.get("schema") != "jarvis-unix-connection/v1"
                or evidence.get("owner_instance_id") != self.state["owner_pin"]["instance_id"]
                or type(evidence.get("epoch")) is not int or evidence["epoch"] < 1
                or evidence.get("connected") is not True
                or health.get("schema") != "jarvis-client-transport-health/v1"
                or health.get("mode") != "unix_owner" or health.get("connected") is not True
                or health.get("failure") is not None
                or not isinstance(initial, dict)
                or Path(str(initial.get("codexHome") or "")).resolve() !=
                   Path(self.state["expected_codex_home"]).resolve()):
            raise ControlUnavailable("original pinned owner/initialize epoch is not reachable")
        raw = initial.get("userAgent")
        current = {"owner_instance_id": evidence["owner_instance_id"],
            "controller_generation": self.token["generation"], "controller_nonce": self.token["nonce"],
            "epoch": evidence["epoch"], "raw_user_agent": raw,
            "owner_reported_build_version": owner_build_version(raw)}
        previous = self.state.get("connection")
        # Version changes within the exact pin are not silently accepted.
        if previous and previous.get("raw_user_agent") != raw:
            previous_version = previous.get("owner_reported_build_version")
            if previous_version != current["owner_reported_build_version"]:
                raise ControlUnavailable("same owner reported a different build version")
        self.state["connection"] = current
        self._save()
        return current

    def event_terminal(self, client, thread_id, native_turn):
        connection = self.connection(client)
        identity = self.state["identity"]
        if (thread_id != identity["thread_id"] or not identity["turn_id"]
                or native_turn.get("id") != identity["turn_id"]
                or native_turn.get("status") not in TERMINAL
                or client.transport_health().get("contiguous") is not True):
            raise ControlUnavailable("exact contiguous owner terminal event required")
        self._terminal("owner_event", native_turn, connection)

    def _terminal(self, kind, native_turn, connection):
        self.check()
        selected = {key: native_turn.get(key) for key in
                    ("id", "status", "startedAt", "completedAt", "error")}
        value = {"kind": kind, "identity": copy.deepcopy(self.state["identity"]),
                 "native_turn": selected, "connection": copy.deepcopy(connection)}
        old = self.state["terminal"]
        if old is not None and old["native_turn"]["status"] != selected["status"]:
            raise ControlUnavailable("conflicting exact terminal observations")
        self.state["terminal"] = old or value
        self._save()

    def stop_intent(self):
        self.check()
        if not self.state["identity"]["turn_id"]:
            raise ControlUnavailable("dispatch outcome unknown: exact turn id was not durably acknowledged")
        if self.state["stop"] is None:
            self.state["stop"] = {"state": "intent", "identity": copy.deepcopy(self.state["identity"])}
            self._save()

    def interrupt(self, client):
        self.check()
        self.stop_intent()
        if self.state["stop"]["state"] != "intent":
            return False  # Unknown/acknowledged prior send is query-only forever.
        self.connection(client)
        self.state["stop"]["state"] = "sent_unknown"
        self._save()  # ACK loss or crash from here onward must never resend.
        identity = self.state["identity"]
        client.request("turn/interrupt", {"threadId": identity["thread_id"], "turnId": identity["turn_id"]})
        self.check()
        self.state["stop"]["state"] = "acknowledged"
        self._save()
        return True

    def query(self, client, *, max_pages=20):
        self.check()
        identity = self.state["identity"]
        if not identity["thread_id"] or not identity["turn_id"]:
            raise ControlUnavailable("dispatch outcome unknown: exact original thread/turn id unavailable")
        connection = self.connection(client)
        metadata = client.request("thread/read", {"threadId": identity["thread_id"], "includeTurns": False})
        self.check()
        if (metadata.get("thread") or {}).get("id") != identity["thread_id"]:
            raise ControlUnavailable("original thread read returned a different identity")
        cursor, seen_cursors, exact = None, set(), None
        for _ in range(max_pages):
            self.check()
            params = {"threadId": identity["thread_id"], "itemsView": "notLoaded",
                      "sortDirection": "desc", "limit": 50}
            if cursor is not None:
                params["cursor"] = cursor
            page = client.request("thread/turns/list", params)
            self.check()
            if self.connection(client) != connection:
                raise ControlUnavailable("owner connection epoch changed during readback")
            turns = page.get("data")
            if not isinstance(turns, list) or len(turns) > 50:
                raise ControlUnavailable("malformed or oversized original turn page")
            for turn in turns:
                if not isinstance(turn, dict) or not isinstance(turn.get("id"), str):
                    raise ControlUnavailable("malformed original turn identity")
                if turn["id"] == identity["turn_id"]:
                    if exact is not None:
                        raise ControlUnavailable("duplicate exact turn in readback")
                    exact = turn
            cursor = page.get("nextCursor")
            if cursor is None:
                break
            if not isinstance(cursor, str) or not cursor or cursor in seen_cursors:
                raise ControlUnavailable("invalid/repeated original turn cursor")
            seen_cursors.add(cursor)
        else:
            raise ControlUnavailable("original turn scan incomplete; bounded page limit reached")
        authority = classify_native_turn(identity["thread_id"], identity["turn_id"], exact,
            observed_thread_id=identity["thread_id"],
            protocol_version=connection["owner_reported_build_version"])
        if authority["terminal_confirmed"]:
            self._terminal("owner_history", exact, connection)
        return authority

    def _terminal_valid(self):
        terminal = self.state.get("terminal")
        if not isinstance(terminal, dict) or terminal.get("identity") != self.state["identity"]:
            return False
        connection, native = terminal.get("connection") or {}, terminal.get("native_turn") or {}
        if (connection.get("owner_instance_id") != self.state["owner_pin"]["instance_id"]
                or type(connection.get("epoch")) is not int or connection["epoch"] < 1
                or type(connection.get("controller_generation")) is not int
                or not 1 <= connection["controller_generation"] <= self.token["generation"]
                or not re.fullmatch(r"[a-f0-9]{32}", str(connection.get("controller_nonce", "")))
                or native.get("id") != self.state["identity"]["turn_id"]
                or not native.get("id") or native.get("status") not in TERMINAL):
            return False
        if terminal.get("kind") == "owner_event":
            return True  # Written only by the exact, contiguous owner-event callback.
        if terminal.get("kind") != "owner_history":
            return False
        version = owner_build_version(connection.get("raw_user_agent"))
        if version != connection.get("owner_reported_build_version"):
            return False
        identity = self.state["identity"]
        return classify_native_turn(identity["thread_id"], identity["turn_id"], native,
            observed_thread_id=identity["thread_id"], protocol_version=version)["terminal_confirmed"]

    def observer_closed(self, client):
        """A local detach certificate, explicitly not a remote process exit."""
        self.check()
        evidence, health = client.unix_connection_evidence(), client.transport_health()
        connection = self.state.get("connection")
        if (not isinstance(connection, dict)
                or connection.get("controller_generation") != self.token["generation"]
                or connection.get("controller_nonce") != self.token["nonce"]
                or evidence.get("schema") != "jarvis-unix-connection/v1"
                or evidence.get("owner_instance_id") != connection.get("owner_instance_id")
                or evidence.get("epoch") != connection.get("epoch")
                or evidence.get("connected") is not False
                or health.get("schema") != "jarvis-client-transport-health/v1"
                or health.get("mode") != "unix_owner"
                or health.get("local_connection_closed") is not True):
            raise ControlUnavailable("exact local recovery attachment close is unproven")
        self.state["attachment_close"] = {**connection, "local_connection_closed": True,
                                         "owner_process_exit_confirmed": False}
        self._save()

    def _close_valid(self):
        close, connection = self.state.get("attachment_close"), self.state.get("connection")
        return (isinstance(close, dict) and isinstance(connection, dict)
                and close == {**connection, "local_connection_closed": True,
                              "owner_process_exit_confirmed": False})

    def settle(self):
        """Release this local finite-turn claim, never claim remote owner exit.

        The stable intent is durable before deleting the exact original claim.
        A crash after unlink/rmdir is resumed by inspecting that same identity;
        another claim or generation is never removed.
        """
        self.check()
        if not self._terminal_valid() or not self._close_valid():
            raise ControlUnavailable("exact terminal and local attachment close proofs required; claim retained")
        request = self._request_check(self.state)
        with generation_guard(self.root, request):
            self.check()
            claim, release = self.state["claim"], self.state["release"]
            directory = self.root / ".user-host-claim"
            if release is None:
                if _claim_snapshot(self.root) != claim:
                    raise ControlUnavailable("original claim changed or disappeared before release intent")
                release = {"id": digest({"identity": self.state["identity"],
                    "owner_instance_id": self.state["owner_pin"]["instance_id"]}),
                    "state": "intent", "claim": claim}
                self.state["release"] = release
                self._save()
            if release.get("claim") != claim:
                raise ControlUnavailable("release intent does not match original claim")
            if os.path.lexists(directory):
                if claim is None or directory.is_symlink():
                    raise ControlUnavailable("unexpected claim exists; no release")
                info = directory.stat()
                if (info.st_dev, info.st_ino) != (claim["dev"], claim["ino"]):
                    raise ControlUnavailable("replacement claim is not releasable")
                owner = directory / "owner.json"
                if owner.exists():
                    if digest(read_json(owner)) != claim["owner_digest"]:
                        raise ControlUnavailable("replacement claim owner is not releasable")
                    owner.unlink()
                    fsync_directory(directory)
                directory.rmdir()
                fsync_directory(self.root)
            self.state["release"]["state"] = "released"
            self._save()
            receipt = self.result()
            # Metadata-only control receipt. Do not fabricate holder/process exit
            # or overwrite task output / business acceptance with a stop ACK.
            durable_json(self.root / "owner-control-receipt.json", receipt)
            return receipt

    def result(self):
        identity = self.state["identity"]
        terminal = self.state.get("terminal")
        try:
            self.check()
            current_binding = True
        except Exception:
            current_binding = False
        confirmed = current_binding and self._terminal_valid()
        # Historical release is not evidence that a replacement claim is absent
        # now. Even a dangling symlink is not an absent claim.
        released = (confirmed and self._close_valid()
                    and (self.state.get("release") or {}).get("state") == "released"
                    and not os.path.lexists(self.root / ".user-host-claim"))
        return {"schema": "jarvis-owner-control-receipt/v1", **identity,
            "owner_instance_id": self.state["owner_pin"]["instance_id"],
            "controller_generation": self.token["generation"],
            "dispatch_state": self.state["dispatch"]["state"],
            "stop_state": (self.state.get("stop") or {}).get("state"),
            "status": terminal["native_turn"]["status"] if confirmed else "unknown",
            "terminal_confirmed": confirmed, "current_binding_confirmed": current_binding,
            "hold_released": released,
            "release_id": (self.state.get("release") or {}).get("id"),
            "owner_process_exit_confirmed": False, "automatic_redispatch_allowed": False,
            "scope": "exact_single_turn_and_local_claim"}


def recovery_client(state):
    """Construct from the frozen original pin only; no caller-supplied new owner."""
    from types import SimpleNamespace
    from jarvis_runtime.jarvis_native_task_launcher import AppServerClient
    config = SimpleNamespace(unix_owner_pin_json=json.dumps(state["owner_pin"]),
        unix_initial_dispatch=False, expected_codex_home=state["expected_codex_home"],
        request_timeout_seconds=10.0, default_model=None, default_reasoning_effort=None,
        poll_seconds=.25, profile="owner-query-stop", codex_cli="unused")
    return AppServerClient(config)


def control_status(root):
    """No network or ledger writes: current exact control/claim evidence only."""
    try:
        with ControlLease(root, existing_only=True) as lease:
            lease.load()
            return lease.result()
    except Exception as exc:
        return {"status": "unknown", "reason": str(exc), "terminal_confirmed": False,
                "hold_released": False, "automatic_redispatch_allowed": False}


def query_stop(root, action, *, client_factory=recovery_client, quarantine_config=None):
    # Deny before ledger recovery/stop intent/claim settlement, not merely on wire.
    from jarvis_runtime.business_quarantine_gate import control_scope
    root = Path(root)
    request = json.loads((root / "request.json").read_text()) if (root / "request.json").is_file() else {}
    with control_scope(quarantine_config, root.parents[1], request):
        return _query_stop_allowed(root, action, client_factory=client_factory)


def _query_stop_allowed(root, action, *, client_factory=recovery_client):
    if action not in {"query", "stop"}:
        raise ValueError("only query and stop are supported; never resume/start/archive")
    if not (Path(root) / "owner-control.json").is_file():
        raise ControlUnavailable("no original pinned control ledger; legacy stdio cannot be attached")
    client = None
    with ControlLease(root, existing_only=True) as lease:
        lease.recover()
        try:
            if lease.state["release"] is not None:
                return lease.settle()  # Complete the durable idempotent local release.
            if action == "stop":
                lease.stop_intent()  # Durable even when the original owner is unreachable.
            if not lease.state["identity"]["turn_id"]:
                raise ControlUnavailable("dispatch outcome unknown: exact original turn id unavailable")
            client = client_factory(copy.deepcopy(lease.state))
            client.owner_control_rpc_guard = lease.rpc_guard
            client.owner_control_wire_scope = lease.wire_scope
            client.start()
            lease.connection(client)
            authority = lease.query(client)
            if action == "stop" and authority["terminal_confirmed"] is not True:
                lease.interrupt(client)
                lease.query(client)  # ACK may mean natural completion, not cancellation.
            if lease._terminal_valid():
                client.close()
                if client.transport_health().get("local_connection_closed") is not True:
                    raise ControlUnavailable("local recovery observer has not closed; claim retained")
                lease.observer_closed(client)
                client = None
                return lease.settle()
            return {**lease.result(), "reason": "exact native terminal is unproven; claim retained"}
        except Exception as exc:
            return {**lease.result(), "reason": str(exc), "error_kind": getattr(exc, "kind", type(exc).__name__)}
        finally:
            if client is not None:
                client.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("query", "stop"))
    parser.add_argument("--hold-root", type=Path, required=True)
    parser.add_argument("--business-quarantine-file", type=Path)
    args = parser.parse_args(argv)
    try:
        from types import SimpleNamespace
        config = SimpleNamespace(business_quarantine_file=str(args.business_quarantine_file)) if args.business_quarantine_file else None
        result = query_stop(args.hold_root, args.action, quarantine_config=config)
    except Exception as exc:
        result = {"status": "unavailable", "reason": str(exc), "terminal_confirmed": False,
                  "hold_released": False, "automatic_redispatch_allowed": False}
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("hold_released") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
