"""Review-bound production entrypoint for an isolated sustained research run.

Import has no runtime effects. ``run`` creates one *fresh* private HoldHost and
facade in this process; it never adopts, repairs, restarts, or stops an old Host.
The reviewed manifest and a separately supplied digest are required. They are an
integrity boundary, not a substitute for the operator's launch authorization.
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import threading
import time
from typing import Any, Mapping

from adapters.codex_app_server.jarvis_hold_host_service import JarvisHoldHost
from adapters.codex_app_server.mcp_wiring import build_jarvis_control, _standard_transport
from adapters.codex_app_server.prediction import official_prediction_config
from jarvis_control.file_task import strict_json
from jarvis_control.sustained_research import (
    DEFAULT_DEADLINE, OCCUPIED, RUN_TERMINAL, JarvisControlResearchPort,
    JurisdictionClaim, LaneConfig, ResearchError, ResearchStore, RunConfig,
    SustainedResearchController, ReadOnlyPreflightUnavailable,
)
from jarvis_runtime.jarvis_native_task_launcher import AppServerClient, NativeTaskLauncherConfig, NativeTaskError
from jarvis_runtime.cancellation import StateGateBusy
from jarvis_runtime.owner_transport_health import registered_owner_guard
from jarvis_runtime.shared_transport_recovery import SharedTransportGate, collect_recovery_proof

SCHEMA = "jarvis-sustained-runner/v1"
GENERATION_SCHEMA = "jarvis-scheduling-generation-run/v1"
CONSTRAINTS = {"model_provider": "openai", "sandbox": "read-only", "approval_policy": "never"}
SOURCE_PACKAGES = (
    "adapters", "jarvis_codex_bridge", "jarvis_contracts", "jarvis_control",
    "jarvis_deploy", "jarvis_engine", "jarvis_harness_sdk", "jarvis_mcp",
    "jarvis_monitor", "jarvis_runtime", "jarvis_schema",
)


def _require(value, message):
    if not value:
        raise ResearchError(message)


def canonical_sha256(value):
    # Exactly the legacy gate import's digest convention, including ensure_ascii.
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _digest(raw):
    return hashlib.sha256(raw).hexdigest()


def _path(value):
    _require(isinstance(value, str) and value, "explicit absolute path required")
    path = Path(value)
    _require(path.is_absolute() and path.resolve() == path and not path.is_symlink(), "path must be absolute and must not traverse links")
    return path


def _json_file(path):
    _require(path.is_file() and path.stat().st_size <= 8 * 1024 * 1024, "missing or oversized declaration")
    result = strict_json(path.read_text(encoding="utf-8"))
    _require(isinstance(result, dict), "declaration must be an object")
    return result


def source_sha256():
    """Pin every own package, including deploy, but never site-packages dependencies."""
    root = Path(__file__).resolve().parents[1]
    files = {}
    for name in SOURCE_PACKAGES:
        package = root / name
        _require(package.is_dir() and package.resolve() == package and not package.is_symlink(),
                 "required Jarvis source package missing or linked: " + name)
        _require(not any(p.is_symlink() for p in package.rglob("*") if "__pycache__" not in p.parts),
                 "Jarvis source files must not traverse links: " + name)
        paths = [p for p in sorted(package.rglob("*.py")) if "__pycache__" not in p.parts]
        _require(paths, "required Jarvis source package is empty: " + name)
        _require(all(p.is_file() and not p.is_symlink() and p.resolve() == p for p in paths),
                 "Jarvis source files must not traverse links: " + name)
        files.update({str(p.relative_to(root)).replace(os.sep, "/"): _digest(p.read_bytes())
                      for p in paths})
    return canonical_sha256(files)


class ReviewedManifest:
    def __init__(self, path, approved_sha256, *, verify_runtime=True):
        self.path = _path(str(path))
        raw = self.path.read_bytes()
        _require(isinstance(approved_sha256, str) and re.fullmatch(r"[a-f0-9]{64}", approved_sha256), "independently approved manifest digest required")
        _require(_digest(raw) == approved_sha256, "reviewed manifest digest mismatch")
        self.digest = approved_sha256
        self.data = _json_file(self.path)
        _require(self.data.get("schema") == SCHEMA, "unsupported runner manifest")
        self.state_dir = _path(self.data["state_dir"])
        self.launcher_path = _path(self.data["launcher"]["path"])
        self.transport_path = _path(self.data["transport"]["path"])
        config = dict(self.data["run"])
        if "lanes" in config:
            config["lanes"] = tuple(LaneConfig(**x) for x in config["lanes"])
        self.run_config = RunConfig(**config)
        self.run_config.validate()
        self.generation_route = self.data.get("generation_run")
        self.generation = None
        if "generation_run" in self.data:
            _require(isinstance(self.generation_route, dict)
                     and self.generation_route.get("schema") == GENERATION_SCHEMA,
                     "explicit scheduling generation run binding required")
            _require(not ({"legacy", "formal_predecessor", "partial_predecessor"} & self.data.keys()),
                     "generation route cannot mix legacy or predecessor routes")
            _require(self.run_config.hard_total in {25, 99} and self.run_config.reserved_external == 0
                     and self.run_config.concurrency_policy == "generation_batch",
                     "reviewed generation launch requires exact local 25 or amended99 / zero external generation_batch")
        else:
            _require(self.run_config.hard_total in {25, 35} and self.run_config.reserved_external == 10,
                     "reviewed launch profile requires hard_total=25 or authorized35 and reserved_external=10")
        _require(self.run_config.deadline == DEFAULT_DEADLINE, "deadline must remain 2026-10-20T16:00:00Z")
        _require([(x.name, x.target) for x in self.run_config.lanes] == [("base", 20), ("task", 5)],
                 "reviewed lane targets must remain ordinary 20 / TASK 5")
        _require(isinstance(self.data.get("frozen_inputs"), dict) and bool(self.data["frozen_inputs"]),
                 "review-pinned operational handoff/inventory/fence inputs required")
        if self.run_config.hard_total == 35:
            authorization = self.data.get("capacity_authorization") or {}
            _require(authorization.get("sha256") == self.run_config.expanded_capacity_authorization
                     and self.data["frozen_inputs"].get(authorization.get("path")) == authorization.get("sha256"),
                     "expanded capacity requires the exact pinned authorization manifest")
        _require(getattr(self.run_config, "concurrency_policy", "healthy_ramp") != "healthy_batch"
                 or self.run_config.hard_total == 35, "healthy batch requires the reviewed expanded-capacity launch")
        self.claims = tuple(JurisdictionClaim(**x) for x in self.data["claims"])
        for claim in self.claims:
            claim.validate()
        self.poll_seconds = self.data.get("poll_seconds", 1.0)
        self.preflight_ttl = self.data.get("preflight_ttl_seconds", 60.0)
        _require(type(self.poll_seconds) in (int, float) and 0.25 <= self.poll_seconds <= 60, "invalid runner poll interval")
        _require(type(self.preflight_ttl) in (int, float) and 1 <= self.preflight_ttl <= 60, "invalid preflight cache interval")
        if self.generation_route is not None:
            self.verify_generation_frozen()
        else:
            legacy = self.data["legacy"]
            _require(legacy.get("admissions_paused") is True and legacy.get("external_reserved") == 10,
                     "reviewed legacy pause and unresolved exposure declaration required")
            _require(isinstance(legacy.get("reservations"), dict) and len(legacy["reservations"]) == 10,
                     "every one of the ten unresolved legacy owners must remain reserved")
            _require(canonical_sha256({"gate": legacy["gate"], "reservations": legacy["reservations"]}) == legacy["snapshot_sha256"],
                     "legacy snapshot digest mismatch")
            _require(bool(legacy.get("source_semantics_sha256")) and isinstance(legacy.get("pause_markers"), list)
                     and len(legacy["pause_markers"]) == 2
                     and {x.get("lane") for x in legacy["pause_markers"]} == {"base", "task"},
                     "semantic source digest and both supported lane pause-marker pins required")
        _require(not ("formal_predecessor" in self.data and "partial_predecessor" in self.data),
                 "choose exactly one predecessor route")
        self.predecessor = None
        if verify_runtime:
            self.verify_static()

    @property
    def database(self):
        if self.generation_route is not None:
            return _path(self.generation_route["database"])
        partial = self.data.get("partial_predecessor")
        return _path(partial["database"]) if partial else self.state_dir / "research.sqlite3"

    @property
    def require_existing_gate(self):
        return self.generation_route is not None or "partial_predecessor" in self.data

    @property
    def worker_capacity(self):
        return self.run_config.hard_total if self.generation_route is not None else 50

    def open_store(self):
        if self.generation_route is not None:
            self.verify_generation_frozen()
            route = self.generation_route
            return ResearchStore(self.database, namespace=route["namespace"],
                shared_union={"schema": GENERATION_SCHEMA, "generation_id": route["generation_id"]},
                business_quarantine_file=route["business_quarantine"]["path"],
                scheduling_generation_id=route["generation_id"])
        partial = self.data.get("partial_predecessor")
        raw_launcher = json.loads(self.launcher_path.read_text())
        quarantine_file = raw_launcher.get("business_quarantine_file")
        return ResearchStore(self.database, business_quarantine_file=quarantine_file, scheduling_generation_id=raw_launcher.get("scheduling_generation_id"), **({"namespace": partial["namespace"], "shared_union": partial["union"]} if partial else {}))

    def verify_static(self):
        _require(_digest(self.path.read_bytes()) == self.digest, "reviewed manifest changed")
        _require(source_sha256() == self.data["source_code_sha256"], "reviewed package code changed")
        for path, digest in self.data["frozen_inputs"].items():
            _require(isinstance(digest, str) and re.fullmatch(r"[a-f0-9]{64}", digest)
                     and _digest(_path(path).read_bytes()) == digest, "reviewed handoff/inventory/fence input changed")
        if self.run_config.hard_total == 35:
            authorization = _json_file(_path(self.data["capacity_authorization"]["path"]))
            expected = dict(schema="jarvis-sustained-capacity-authorization/v1", run_id=self.run_config.run_id,
                            deadline=self.run_config.deadline, provider_key=self.run_config.provider_key,
                            hard_total=35, reserved_external=10, lane_targets={"base": 20, "task": 5})
            _require(all(authorization.get(k) == v for k, v in expected.items())
                     and isinstance(authorization.get("authorization_reference"), str) and authorization["authorization_reference"],
                     "expanded capacity approval scope mismatch")
        for key, path in (("launcher", self.launcher_path), ("transport", self.transport_path)):
            _require(_digest(path.read_bytes()) == self.data[key]["sha256"], "reviewed deployment configuration changed")
        launcher = NativeTaskLauncherConfig(self.launcher_path)
        _require(launcher.worker_capacity == self.worker_capacity,
                 "launcher capacity must match the reviewed route")
        _require(launcher.live_creation_enabled and launcher.profile and launcher.expected_codex_home,
                 "reviewed launcher must explicitly enable creation and identify profile/home")
        _require(launcher.runtime_constraints == CONSTRAINTS and launcher.runtime_environments == [],
                 "explicit read-only OpenAI runtime constraints and empty environments required")
        _require(not launcher.jarvis_connection, "research runner cannot start a gateway or change routes")
        transport = _json_file(self.transport_path)
        configured_launcher = Path(transport.get("native_task_launcher_config", ""))
        if not configured_launcher.is_absolute():
            configured_launcher = self.transport_path.parent / configured_launcher
        _require(configured_launcher.resolve() == self.launcher_path and transport.get("desktop_recovery_enabled") is False,
                 "transport must bind the same launcher and explicitly disable desktop recovery")
        launcher.resolve_project(self.run_config.project)
        self.launcher = launcher
        if self.generation_route is not None:
            self.verify_generation_frozen()
        if "formal_predecessor" in self.data and self.predecessor is None:
            from jarvis_control.formal_predecessor import FormalPredecessor
            self.predecessor = FormalPredecessor(self)
        if "partial_predecessor" in self.data and self.predecessor is None:
            if self.data["partial_predecessor"].get("schema") == "jarvis-partial-predecessor/v4":
                from jarvis_control.retained_partial_predecessor import RetainedPartialPredecessor
                self.predecessor = RetainedPartialPredecessor(self)
            else:
                from jarvis_control.partial_predecessor import PartialPredecessor
                self.predecessor = PartialPredecessor(self)
        return launcher

    def verify_generation_frozen(self, *, require_fresh=False):
        """Read-only authority proof; never create a DB, namespace or provider gate."""
        from jarvis_runtime import business_quarantine as quarantine
        from jarvis_runtime.scheduling_budget_generation import generation_for, FILENAME
        route = self.generation_route
        _require(_digest(self.path.read_bytes()) == self.digest, "reviewed manifest changed")
        _require(set(route) == {"schema", "database", "database_identity", "namespace",
                 "generation_id", "business_quarantine", "scheduling_generation", "quarantine_authority"},
                 "complete explicit generation route pins required")
        namespace = route["namespace"]
        _require(isinstance(namespace, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,39}", namespace),
                 "fresh explicit generation namespace required")
        for key in ("business_quarantine", "scheduling_generation"):
            pin = route[key]
            _require(isinstance(pin, dict) and set(pin) == {"path", "sha256"}
                     and isinstance(pin["sha256"], str) and re.fullmatch(r"[a-f0-9]{64}", pin["sha256"])
                     and self.data["frozen_inputs"].get(pin["path"]) == pin["sha256"],
                     "exact frozen generation authority file pins required")
            _require(_digest(_path(pin["path"]).read_bytes()) == pin["sha256"],
                     "reviewed generation authority file changed")
        qpath = _path(route["business_quarantine"]["path"])
        _require(qpath == self.database.parent / quarantine.FILENAME
                 and _path(route["scheduling_generation"]["path"]) == self.database.parent / FILENAME,
                 "generation route requires original authority paths")
        policy = quarantine.Policy(qpath, expected_generation_id=route["generation_id"])
        gen = generation_for(policy)
        _require(gen is not None and gen.active and gen.document["generation_id"] == route["generation_id"],
                 "active exact scheduling generation required")
        document = gen.document
        _require(document["database"] == str(self.database)
                 and quarantine.digest(document["database_identity"]) == quarantine.digest(route["database_identity"])
                 and quarantine.digest(document["quarantine_authority"]) == quarantine.digest(route["quarantine_authority"]),
                 "original database inode and quarantine authority binding required")
        _require(len(document["original_runs"]) == 5 and len(document["archived_reservations"]) == 27
                 and len(document["excluded_scopes"]) == 52,
                 "exact five original runs, twenty-seven archives and fifty-two denied scopes required")
        _require(namespace not in gen.original_namespaces and self.run_config.run_id not in gen.old_run_ids
                 and self.run_config.owner_id not in {r["owner_id"] for r in document["original_runs"].values()},
                 "original namespace, run or owner cannot become a fresh generation")
        for claim in self.claims:
            policy.require_allowed(asdict(claim), require_scope=True)
            _require(claim.checkpoint is None, "generation launch cannot adopt an old checkpoint")
        _require(_digest(self.launcher_path.read_bytes()) == self.data["launcher"]["sha256"],
                 "reviewed deployment configuration changed")
        launcher = _json_file(self.launcher_path)
        _require(launcher.get("business_quarantine_file") == str(qpath)
                 and launcher.get("scheduling_generation_id") == route["generation_id"],
                 "launcher must explicitly pin the original quarantine and scheduling generation")
        with quarantine._database(self.database, route["database_identity"]) as db:
            for table, pin in document["original_runs"].items():
                rows = db.execute('SELECT data FROM main."' + table + '" WHERE id=1').fetchall()
                _require(len(rows) == 1, "original paused run missing")
                state = strict_json(rows[0][0])
                _require(state.get("status") == "paused" and quarantine.digest(state) == pin["state_sha256"],
                         "exact original paused run changed")
            tables = {row[0] for row in db.execute("SELECT name FROM main.sqlite_master WHERE type='table'")}
            if require_fresh:
                _require(not any("jarvis_research_" + kind + "__" + namespace in tables
                                 for kind in ("run", "checkpoints", "native_scans")),
                         "existing generation namespace cannot be adopted or redispatched")
                _require("jarvis_research_union" not in tables or not db.execute(
                    "SELECT 1 FROM main.jarvis_research_union LIMIT 1").fetchone(),
                    "existing generation controller requires inspection, not relaunch")
            budget = gen.require_budget(db)
            _require(self.run_config.hard_total == budget["hard_total"],
                     "reviewed Runner capacity amendment is missing or changed")
            _require(budget["management_cleanup_applied"] and budget["archived_native_unknown"] == 27,
                     "verified applied management archive for the exact twenty-seven required")
        self.generation = gen
        return {"schema": GENERATION_SCHEMA, "generation_id": route["generation_id"],
                "original_runs_paused": 5, "archived_native_unknown": 27,
                "denied_scopes": 52, "current_occupancy": budget["current_occupancy"],
                "provider_gate_reset": False}

    def verify_legacy_frozen(self):
        """Semantic JSON row comparison under one read transaction; never writes.

        Identifiers are constrained and values parameterized. JSON reserialization,
        SQLite checkpointing and identical writes do not invalidate a frozen gate.
        All source rows (gate and ownership) are independently review-pinned.
        """
        legacy = self.data["legacy"]
        source = _path(legacy["source_database"])
        _require(source != self.database and source.is_file(), "existing distinct legacy database required")
        for marker in legacy["pause_markers"]:
            _require(_digest(_path(marker["path"]).read_bytes()) == marker["sha256"], "supported legacy pause marker changed")
        with sqlite3.connect(source.as_uri() + "?mode=ro", uri=True, timeout=10) as db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN")
            gates = db.execute("SELECT data FROM gate WHERE id=1").fetchall()
            _require(len(gates) == 1, "exact legacy gate missing")
            gate = strict_json(gates[0]["data"])
            semantic = {"gate": gate}
            for table, columns in (
                ("recovery_capacity", ("id", "reserved", "hard_limit")),
                ("recovery_policy", ("id", "generation", "base_limit", "task_limit", "release_proof")),
                ("recovery_occupancy", ("request_id", "role", "owner", "created")),
            ):
                actual = tuple(row[1] for row in db.execute(f"PRAGMA table_info({table})"))
                _require(actual == columns, "legacy source schema changed")
                semantic[table] = [dict(row) for row in db.execute(f"SELECT * FROM {table} ORDER BY 1")]
            _require(semantic["recovery_capacity"] == [{"id": 1, "reserved": 10, "hard_limit": 25}],
                     "legacy unresolved capacity changed")
            policy = semantic["recovery_policy"]
            _require(len(policy) == 1 and all(policy[0].get(k) == v for k, v in
                     {"id": 1, "generation": 2, "base_limit": 20, "task_limit": 5}.items())
                     and policy[0].get("release_proof"), "legacy corrected policy unverified")
            _require(not semantic["recovery_occupancy"], "legacy new admission occupancy is not frozen")
            _require(canonical_sha256(gate) == canonical_sha256(legacy["gate"]), "legacy gate changed")
            _require(canonical_sha256(semantic) == legacy["source_semantics_sha256"],
                     "legacy budget or ownership changed; admissions blocked")

    def verify_predecessors_frozen(self):
        if self.generation_route is not None:
            return self.verify_generation_frozen()
        self.verify_legacy_frozen()
        _require(not ({"formal_predecessor", "partial_predecessor"} & self.data.keys()) or self.predecessor is not None,
                 "formal predecessor must be validated; legacy fallback forbidden")
        return self.predecessor.verify() if self.predecessor is not None else None



def _persist_diagnostic(path, data):
    if path is None:
        return
    temporary = path.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(data, stream, sort_keys=True, allow_nan=False)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


class NativeReadOnlyClient:
    """One serialized observer. Read errors retain unknown; dead clients never restart."""
    METHODS = {"model/list", "thread/items/list"}

    def __init__(self, config, *, diagnostic_path=None, clock=time.monotonic):
        self.client = AppServerClient(config)
        self.client.read_only = True
        self.lock = threading.Lock()
        self.started = self.failed = self.degraded = False
        self.exact_read_required = False
        self.diagnostic_path, self.clock = diagnostic_path, clock
        self.retry_at = 0.0
        self.error_count = 0
        self.last_error = None
        self.cancelled = threading.Event()

    def _connection_failure(self):
        health = getattr(self.client, "transport_health", None)
        if callable(health):
            try:
                state = health()
                if (not isinstance(state, dict)
                        or state.get("schema") != "jarvis-client-transport-health/v1"
                        or type(state.get("connected")) is not bool):
                    return "transport_health_invalid"
                failure = state.get("failure")
                if failure is not None and not isinstance(failure, str):
                    return "transport_health_invalid"
                return failure or (None if state["connected"] else "transport_disconnected")
            except Exception:
                return "transport_health_unavailable"
            # Never fall back to process polling after an explicit transport
            # health failure. An attached Unix owner is not our Popen child.
        # Compatibility with existing legacy adapters/test doubles only.
        protocol = getattr(self.client, "_stdout_protocol_failure", None)
        if protocol is not None and protocol.is_set():
            return "malformed_protocol"
        eof = getattr(self.client, "_stdout_eof", None)
        if eof is not None and eof.is_set():
            return "stdout_eof"
        process = getattr(self.client, "process", None)
        if self.started and hasattr(self.client, "process") and (process is None or process.poll() is not None):
            return "process_exit"
        return None

    def refresh_connection_health(self):
        with self.lock:
            origin = self._connection_failure() if self.started and not self.failed else None
            if origin:
                self._record(ResearchError("observer connection unavailable"), "health", origin, True)

    def _record(self, exc, method, origin, permanent):
        self.failed = self.failed or permanent
        self.degraded = True
        self.exact_read_required = self.exact_read_required or method != "model/list"
        self.error_count += 1
        self.retry_at = self.clock() + min(5.0, .1 * 2 ** min(self.error_count - 1, 6))
        self.last_error = {"type": type(exc).__name__, "method": method,
                           "origin": origin, "at": time.time()}
        self._publish()

    def _publish(self):
        _persist_diagnostic(self.diagnostic_path, {"schema": "jarvis-observer-health/v1",
            "failed": self.failed, "degraded": self.degraded, "exact_read_required": self.exact_read_required,
            "error_count": self.error_count,
            "last_error": self.last_error, "read_only": True, "automatic_restart": False})

    def _start_once(self):
        _require(not self.failed, "observer unavailable; automatic restart is disabled")
        if not self.started:
            self.started = True  # Even an uncertain start is never repeated.
            try:
                initialized = self.client.start()
                home = initialized.get("codexHome")
                _require(home and Path(home).resolve() == Path(self.client.config.expected_codex_home).resolve(),
                         "observer Codex Home readback mismatch")
            except Exception as exc:
                self._record(exc, "initialize", "observer_start", True)
                raise ResearchError("observer initialization failed; automatic restart is disabled") from None

    def _call(self, method, operation, *, deadline=None, exact=False):
        with self.lock:
            _require(not self.failed, "observer unavailable; automatic restart is disabled")
            if self.cancelled.is_set():
                raise ReadOnlyPreflightUnavailable("observer read wait cancelled")
            self._start_once()
            for attempt in range(2):  # At most one bounded same-client read retry.
                if self.cancelled.is_set():
                    raise ReadOnlyPreflightUnavailable("observer read wait cancelled")
                origin = self._connection_failure()
                if origin:
                    exc = ResearchError("observer connection unavailable; automatic restart is disabled")
                    self._record(exc, method, origin, True)
                    raise exc
                delay = max(0.0, self.retry_at - self.clock())
                if delay:
                    if attempt == 0:
                        raise ReadOnlyPreflightUnavailable("observer read backoff pending")
                    if deadline is not None and self.clock() + delay >= deadline:
                        raise ReadOnlyPreflightUnavailable("observer read backoff exceeds deadline")
                    if self.cancelled.wait(delay):
                        raise ReadOnlyPreflightUnavailable("observer read wait cancelled")
                try:
                    result = operation()
                except Exception as exc:
                    origin = self._connection_failure()
                    permanent = bool(origin) or isinstance(exc, (EOFError, BrokenPipeError, ConnectionError))
                    transient = isinstance(exc, (NativeTaskError, TimeoutError, StateGateBusy))
                    category = ("observer_connection" if permanent else "local_state_gate"
                                if isinstance(exc, StateGateBusy) else "native_read" if transient else "observer_protocol")
                    self._record(exc, method, origin or category, permanent or not transient)
                    if self.failed:
                        raise ResearchError("observer permanent failure; automatic restart is disabled") from None
                    if attempt == 1:
                        raise ReadOnlyPreflightUnavailable("observer read unavailable; exact result unknown") from None
                    continue
                # EOF racing a successful RPC must never clear its failure epoch.
                origin = self._connection_failure()
                if origin:
                    exc = ResearchError("observer connection ended during read")
                    self._record(exc, method, origin, True)
                    raise exc
                if self.degraded and exact:
                    self.degraded = False
                    self.exact_read_required = False
                    self.retry_at = 0.0
                    self.error_count = 0
                    self._publish()  # Keep the sanitized last error for diagnosis.
                return result

    def catalog_scan_validated(self):
        """Called only after the complete model catalog pagination validates."""
        with self.lock:
            origin = self._connection_failure()
            if origin:
                self._record(ResearchError("observer connection ended during catalog scan"), "model/list", origin, True)
            _require(not self.failed, "observer unavailable; automatic restart is disabled")
            if self.degraded and not self.exact_read_required:
                self.degraded = False
                self.retry_at = 0.0
                self.error_count = 0
                self._publish()

    def request(self, method, params):
        _require(method in self.METHODS, "unsupported observer method")
        return self._call(method, lambda: self.client.request(method, dict(params), _terminal_probe=True))

    def read_exact_turn(self, thread_id, turn_id, *, deadline):
        data = self._call("read_exact_turn", lambda: self.client.read_exact_turn(
            thread_id, turn_id, deadline=deadline, _terminal_probe=True), deadline=deadline, exact=True)
        return data, self.client.cli_version

    def close(self):
        # Only supported owned finish calls this; never a recovery mechanism.
        self.cancelled.set()
        self.client.close()


class RuntimeBindings:
    def __init__(self, manifest, control, host, reader, *, clock=time.time):
        self.manifest, self.control, self.host, self.reader, self.clock = manifest, control, host, reader, clock
        self._cache = {}
        self._models = None
        self._models_at = 0.0
        self._preflight_lock = threading.Lock()
        self._proof_at = {}

    def _models_read(self):
        now = self.clock()
        if self._models is not None and 0 <= now - self._models_at < self.manifest.preflight_ttl:
            return self._models
        cursor, seen, models = None, set(), {}
        for _ in range(100):
            params = {"limit": 100, "includeHidden": True}
            if cursor is not None:
                params["cursor"] = cursor
            page = self.reader.request("model/list", params)
            _require(isinstance(page, dict) and isinstance(page.get("data"), list) and "nextCursor" in page,
                     "complete paginated model/list response required")
            for item in page["data"]:
                _require(isinstance(item, dict), "invalid model declaration")
                model = item.get("model") or item.get("id")
                _require(isinstance(model, str) and model and model not in models, "duplicate or absent model identity")
                # Catalogs often omit provider. The frozen launcher explicitly
                # selects OpenAI and each actual owning runtime verifies it again.
                provider = item.get("modelProvider", item.get("provider"))
                _require(provider in (None, "openai"), "model catalog provider mismatch")
                models[model] = item
            cursor = page["nextCursor"]
            if cursor is None:
                self.reader.catalog_scan_validated()
                self._models, self._models_at = models, now
                return models
            _require(isinstance(cursor, str) and cursor and cursor not in seen, "model/list cursor loop")
            seen.add(cursor)
        raise ResearchError("model/list pagination bound exceeded")

    async def mode_preflight(self, lane):
        if self.manifest.generation_route is not None and not any(
                claim.lane == lane["name"] for claim in self.manifest.claims):
            raise ReadOnlyPreflightUnavailable("no declared claim in this generation lane")
        try:
            return await asyncio.to_thread(self._mode_preflight, lane)
        except StateGateBusy as exc:
            _persist_diagnostic(self.manifest.state_dir / "preflight-diagnostic.json",
                {"schema": "jarvis-preflight-diagnostic/v1", "type": type(exc).__name__,
                 "origin": "local_state_gate", "at": time.time()})
            raise ReadOnlyPreflightUnavailable("local ownership read temporarily busy") from None

    def _mode_preflight(self, lane):
        with self._preflight_lock:
            now, key = self.clock(), (lane["name"], lane["model"], lane["reasoning_effort"], lane["task_mode"])
            # Frozen predecessor budget/ownership is cheap local evidence and
            # is rechecked every tick, outside the network preflight cache.
            self.manifest.verify_predecessors_frozen()
            self.reader.refresh_connection_health()
            _require(not self.reader.failed, "owned observer connection failed; admissions paused, no automatic restart")
            if self.reader.degraded:
                if self.reader.exact_read_required:
                    raise ReadOnlyPreflightUnavailable("observer read health degraded; exact fresh read required")
                self._models = None  # Catalog-only failure can recover before a first turn exists.
                self._cache.pop(key, None)
            cached = self._cache.get(key)
            if cached and 0 <= now - cached[0] < self.manifest.preflight_ttl:
                if cached[1] is None:
                    raise ResearchError("cached read-only preflight failed; no model fallback")
                return dict(cached[1])
            try:
                self.manifest.verify_static()
                ready = self.control._provisioner.hold_host_health(required_workers=self.manifest.worker_capacity)
                _require(ready.get("status") == "ready", "owned Host readiness unverified")
                health = _json_file(self.manifest.state_dir / "hold-host.json")
                _require(health.get("host_instance_id") == self.host.instance_id and health.get("pid") == os.getpid(),
                         "Host ownership changed; automatic recovery disabled")
                models = self._models_read()
                if self.reader.degraded:
                    raise ReadOnlyPreflightUnavailable("exact fresh read required after observer error")
                _require(lane["model"] in models, "exact lane model is unavailable; no fallback")
                efforts = models[lane["model"]].get("supportedReasoningEfforts")
                _require(isinstance(efforts, list) and lane["reasoning_effort"] in [x.get("reasoningEffort") for x in efforts if isinstance(x, dict)],
                         "requested lane reasoning effort unavailable")
                if lane["task_mode"]:
                    cfg = official_prediction_config(Path(self.manifest.launcher.expected_codex_home))
                    _require(cfg.get("is_enabled") is True and cfg.get("prompt") and cfg.get("prompt_version")
                             and cfg.get("prediction_reasoning_effort") and lane["model"] not in cfg.get("unsupported_models", []),
                             "official TASK prediction configuration unavailable for exact lane model")
                result = {"ready": True, "model": lane["model"], "provider": "openai", "task_supported": lane["task_mode"]}
                self._cache[key] = (now, result)
                return dict(result)
            except StateGateBusy:
                raise
            except Exception:
                if self.reader.degraded and not self.reader.failed:
                    raise ReadOnlyPreflightUnavailable("read-only observer temporarily unavailable") from None
                self._cache[key] = (now, None)
                raise ResearchError("read-only model/configuration/ownership preflight failed; no fallback") from None

    def _guard(self, claim, current):
        return registered_owner_guard(request_id=current["request_id"], thread_id=current["thread_id"],
            turn_id=current["turn_id"], host_instance_id=self.host.instance_id,
            owner=self.manifest.run_config.owner_id, lane=claim["lane"])

    def owner_observer(self, claim, current):
        with self._guard(claim, current) as evidence:
            return evidence

    async def transport_recovery(self, claim, current, gate):
        return await asyncio.to_thread(self._transport_recovery, claim, current, gate)

    def _transport_recovery(self, claim, current, gate):
        snapshot = gate.snapshot()
        if not snapshot["open"] or snapshot["probe"] != current["request_id"]:
            return False
        key = (current["request_id"], snapshot["generation"])
        now = self.clock()
        if now - self._proof_at.get(key, float("-inf")) < 5:
            return False
        self._proof_at[key] = now
        with self._guard(claim, current) as evidence:
            pass  # Never hold owner cancellation/event locks during network reads.
        proof = collect_recovery_proof(self.reader.request, evidence, snapshot["generation"], clock=self.clock)
        return gate.recover(proof, lambda: self._guard(claim, current))

    def prediction_observer(self, claim, current):
        root = self.manifest.state_dir / "task-holds" / claim["hold_id"]
        _require(root.resolve() == root, "prediction Hold path traverses a link")
        request = _json_file(root / "request.json")
        _require(request.get("request_id") == current["request_id"] and request.get("hold_id") == claim["hold_id"], "prediction request ownership changed")
        envelope = _json_file(root / "prediction-first.json")
        rid = current["request_id"] + ":prediction:first"
        original = _json_file(root / "predictions" / (_digest(rid.encode()) + ".json"))
        binding = original.get("binding") or {}
        lane = next(x for x in self.manifest.run_config.lanes if x.name == claim["lane"])
        _require(binding.get("thread_id") == claim["thread_id"] and binding.get("model") == lane.model
                 and binding.get("source_turn_id") in (None, claim["previous_turn_id"]), "original prediction binding mismatch")
        saved, copied = original.get("result"), envelope.get("prediction")
        _require(isinstance(saved, dict) and isinstance(copied, dict) and all(copied.get(k) == v for k, v in saved.items()),
                 "original helper receipt disagrees with Hold envelope")
        _require(saved.get("request_id") == rid and saved.get("source_turn_id") == claim["previous_turn_id"], "original helper source mismatch")
        return envelope

    def non_dispatch_observer(self, claim, current, hold, prediction):
        """Prove a typed pre-helper rejection without claiming a native terminal."""
        root = self.manifest.state_dir / "task-holds" / claim["hold_id"]
        _require(root.resolve() == root, "no-dispatch Hold path traverses a link")
        expected = {"request_id": current["request_id"], "hold_id": claim["hold_id"]}
        with self.host._health_lock:
            _require(root not in self.host._occupied_roots and claim["hold_id"] not in self.host._active_hold_ids,
                     "owned Host still occupies this generation")
            request_before = (root / "request.json").read_bytes()
            request = strict_json(request_before)
            result = _json_file(root / "result.json")
            _require(all(request.get(k) == result.get(k) == hold.get(k) == v for k, v in expected.items()),
                     "no-dispatch generation changed")
            _require(request.get("thread_id") == claim["thread_id"] and request.get("model") == current["arguments"]["model"],
                     "no-dispatch source/model changed")
            _require(result.get("local_observer_stopped") is True and not result.get("turn_id"),
                     "local observer exit or no research turn unproven")
            original = self.prediction_observer(claim, current)
            p = original.get("prediction") or {}
            _require(p == (prediction.get("prediction") if "prediction" in prediction else prediction),
                     "prediction receipt changed during no-dispatch verification")
            witness = p.get("pre_helper_rejection")
            _require(isinstance(witness, dict) and witness == result.get("pre_helper_rejection"),
                     "original typed rejection witness missing or changed")
            required = {"schema": "jarvis-prediction-pre-helper-rejection/v1",
                "request_id": current["request_id"] + ":prediction:first",
                "source_thread_id": claim["thread_id"], "source_turn_id": claim["previous_turn_id"],
                "model": current["arguments"]["model"], "helper_fork_attempted": False,
                "helper_dispatch_attempted": False, "research_dispatch_attempted": False}
            _require(all(witness.get(k) == v and type(witness.get(k)) is type(v) for k, v in required.items()),
                     "typed rejection binding mismatch")
            _require(witness.get("rejection") in {"disabled", "unsupported", "failed"}
                     and isinstance(witness.get("config_sha256"), str)
                     and re.fullmatch(r"[a-f0-9]{64}", witness["config_sha256"])
                     and not any(p.get(k) for k in ("parent_thread_id", "prediction_thread_id", "prediction_turn_id")),
                     "helper dispatch absence unproven")
            for path in (root / ".user-host-claim", root / "request.lock"):
                _require(not path.exists() and not path.is_symlink(), "owner claim or request lock remains")
            for path in (root / "dispatch-commits").glob("*.json"):
                _require(not path.is_symlink(), "dispatch journal traverses a link")
                _require(_json_file(path).get("request_id") != current["request_id"], "research dispatch commitment exists")
            _require((root / "request.json").read_bytes() == request_before
                     and _json_file(root / "result.json") == result, "no-dispatch evidence changed while checking")
        digest = hashlib.sha256(json.dumps(witness, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
        return {"schema": "jarvis-sustained-no-dispatch/v1", **expected,
            "thread_id": claim["thread_id"], "source_turn_id": claim["previous_turn_id"],
            "model": current["arguments"]["model"], "host_instance_id": self.host.instance_id,
            "owner_stopped": True, "no_owner_claim": True, "no_request_lock": True,
            "no_dispatch_journal": True, "pre_helper_rejection_sha256": digest}


class SustainedRuntime:
    """Owned lifetime supervisor. Unknown execution retains the non-daemon Host."""
    def __init__(self, manifest, *, clock=time.time):
        self.manifest, self.clock = manifest, clock
        self.host = self.control = self.reader = self.controller = None
        self.stop_event = threading.Event()
        self.host_thread = None
        self.host_failure = None
        self.last_error_type = None
        self._decoupled_observation = False
        self._observation_tasks = {}
        self._diagnostic_task = None

    def prepare(self):
        m = self.manifest
        m.verify_static()
        formal = m.verify_predecessors_frozen()
        if m.generation_route is not None:
            m.verify_generation_frozen(require_fresh=True)
        _require(m.state_dir.parent.is_dir() and not m.state_dir.exists(),
                 "run requires a fresh private state directory; never adopt or autoheal existing ownership")
        m.state_dir.mkdir(mode=0o700)  # Atomic no-overwrite ownership claim.
        (m.state_dir / "runner-manifest.json").write_text(json.dumps({"schema": SCHEMA, "manifest_sha256": m.digest}), encoding="utf-8")
        store = m.open_store()
        store.initialize(m.run_config, m.claims, require_fresh=m.generation_route is not None)
        gate = SharedTransportGate(m.database, clock=self.clock, require_existing=m.require_existing_gate)
        if m.require_existing_gate:
            # The namespaced controller and provider share this exact live DB.
            # Never import, clone, clear pending/probe, or replace a budget here.
            m.verify_predecessors_frozen()
        elif formal is None:
            legacy = m.data["legacy"]
            gate.import_legacy_snapshot(legacy["gate"], legacy["reservations"], expected_sha256=legacy["snapshot_sha256"])
        else:
            gate.import_formal_snapshot(formal, expected_sha256=canonical_sha256(formal))
            # Historical evidence stays original predecessor provenance. Never
            # insert it as a newly produced successor checkpoint/native receipt.
            (m.state_dir / "predecessor-provenance.json").write_text(json.dumps({
                "schema": "jarvis-successor-provenance/v1", "predecessor": m.data["formal_predecessor"],
                "successor_manifest_sha256": m.digest}), encoding="utf-8")
        heartbeat_path = m.state_dir / "local-heartbeat.config.json"
        heartbeat_path.write_text(json.dumps({"db_path": str(m.state_dir / "heartbeats.sqlite3"),
            "health_path": str(m.state_dir / "heartbeat-health.json")}), encoding="utf-8")
        self.reader = NativeReadOnlyClient(m.launcher, diagnostic_path=m.state_dir / "observer-health.json")
        def transport_factory(path):
            transport = _standard_transport(path, execution_reader=lambda thread, turn:
                self.control._provisioner.thread_execution_evidence(thread, turn))
            transport.exact_reader = self.reader.read_exact_turn
            return transport
        self.control = build_jarvis_control(m.transport_path, m.state_dir, launcher_config_path=m.launcher_path,
            local_heartbeat_config_path=heartbeat_path, transport_factory=transport_factory)
        # Fail closed forever if the same-process Host is not ready; the facade
        # must never fall back to initialize_user_host's external-process route.
        self.control._provisioner._host_initializer = lambda **_: {"status": "host_not_ready", "reason": "runner-owned Host is not ready; automatic startup disabled"}
        self.host = JarvisHoldHost(state_dir=m.state_dir, launcher_config=m.launcher_path, workers=m.worker_capacity)
        bindings = RuntimeBindings(m, self.control, self.host, self.reader, clock=self.clock)
        self.bindings = bindings
        controls = self._successor_call
        port = JarvisControlResearchPort({"base": controls, "task": controls},
            mode_preflight=bindings.mode_preflight, owner_observer=bindings.owner_observer,
            prediction_observer=bindings.prediction_observer, transport_recovery=bindings.transport_recovery,
            non_dispatch_observer=bindings.non_dispatch_observer)
        self.controller = SustainedResearchController(store, gate, port, clock=self.clock)
        self.host_thread = threading.Thread(target=self._host_main, name="jarvis-sustained-owned-host", daemon=False)
        self.host_thread.start()
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if self.host_failure:
                break
            if self.control._provisioner.hold_host_health(required_workers=m.worker_capacity).get("status") == "ready":
                health = _json_file(m.state_dir / "hold-host.json")
                if health.get("host_instance_id") == self.host.instance_id and health.get("pid") == os.getpid():
                    return self
            time.sleep(.05)
        self.controller.control("pause")
        raise ResearchError("new owned Host did not publish initial exact readiness; retained without dispatch")

    async def _successor_call(self, tool, arguments):
        """No excluded native identity may be adopted or accepted as fresh."""
        excluded = self.manifest.predecessor.excluded if self.manifest.predecessor is not None else set()
        if self.manifest.generation is not None:
            excluded = excluded | set(self.manifest.generation.document["denied_identities"]["thread_id"])
        _require(all(arguments.get(k) not in excluded for k in ("thread_id", "task_id", "threadId")),
                 "excluded predecessor native identity cannot be reused")
        if tool != "jarvis_read":
            # Also recheck immediately before each irreversible dispatch; startup
            # readiness and a cached network preflight are not a handoff fence.
            await asyncio.to_thread(self.manifest.verify_predecessors_frozen)
        names = {"jarvis_create": "create", "jarvis_resume": "resume", "jarvis_task": "task", "jarvis_read": "read"}
        def native_call():
            if tool != 'jarvis_read':
                self.controller.require_current_dispatch(arguments['request_id'])
            return getattr(self.control, names[tool])(**arguments)
        result = await asyncio.to_thread(native_call)
        for value in (result, result.get("data") or {}):
            _require(all(value.get(k) not in excluded for k in ("thread_id", "threadId", "task_id")),
                     "excluded native identity in successor receipt; ownership remains unknown")
        return result

    def _host_main(self):
        try:
            self.host.run_forever(poll_seconds=.25, stop_event=self.stop_event)
            if not self.stop_event.is_set():
                self.host_failure = "UnexpectedHostExit"
        except BaseException as exc:
            self.host_failure = type(exc).__name__
        if self.host_failure:
            try:
                _persist_diagnostic(self.manifest.state_dir / "host-failure.json",
                    {"schema": "jarvis-owned-host-failure/v1", "type": self.host_failure,
                     "phase": "run_forever", "at": time.time(), "host_instance_id": self.host.instance_id,
                     "automatic_restart": False})
            except Exception:
                self.last_error_type = "HostDiagnosticPersistenceError"
        # The keeper is deliberately non-daemon. A Host exception must not let
        # Python tear down possibly still-owned workers/native clients.
        self.stop_event.wait()

    def safe_to_finish(self):
        if (any(not t.done() for t in self._observation_tasks.values())
                or self._diagnostic_task is not None and not self._diagnostic_task.done()):
            return False
        state = self.controller.store.read()
        if state["status"] not in RUN_TERMINAL or any(x["status"] in OCCUPIED or x["current"] for x in state["claims"]):
            return False
        for claim in state["claims"]:
            for record in claim["history"]:
                if record.get("outcome") == "non_dispatch_settled":
                    witness = record.get("non_dispatch_witness") or {}
                    if record.get("non_dispatch_confirmed") is not True or not witness:
                        return False
                elif record.get("outcome") != "not_sent" and not (record.get("terminal_confirmed") is True and record.get("hold_released") is True):
                    return False
        with self.host._health_lock:
            if self.host._occupied_roots or self.host._active_hold_ids:
                return False
        for dirname in ("task-holds", "task-monitors"):
            for root in (self.manifest.state_dir / dirname).glob("*"):
                if not root.is_dir():
                    continue
                if (root / ".user-host-claim").exists():
                    return False
                if (root / "request.json").exists():
                    status = self.control._provisioner.hold_status(root.name)
                    if not (status.get("terminal_confirmed") is True and status.get("hold_released") is True):
                        claim = next((x for x in state["claims"] if x["hold_id"] == root.name), None)
                        record = claim["history"][-1] if claim and claim["history"] else {}
                        if record.get("outcome") != "non_dispatch_settled":
                            return False
                        saved = record.get("non_dispatch_witness") or {}
                        current = {"request_id": record["request_id"], "arguments": {"model": saved.get("model")}}
                        try:
                            prediction = self.bindings.prediction_observer(claim, current)
                            witness = self.bindings.non_dispatch_observer(claim, current, status, prediction)
                        except Exception:
                            return False
                        if witness != saved or witness["pre_helper_rejection_sha256"] != record.get("witness_digest"):
                            return False
        return True

    def finish(self):
        _require(self.safe_to_finish(), "owned lifetime retained until exact terminal and release")
        self.stop_event.set()
        self.host_thread.join()
        self.reader.close()

    async def step(self):
        self.reader.refresh_connection_health()
        if self.host_failure or self.reader.failed:
            self.controller.control("pause")
        if not self._decoupled_observation:
            return await self.controller.tick()
        self._poll_owned_tasks()
        # Starting readers does not wait for full terminal pagination. Every
        # reservation is still counted from durable exact state and the same gate.
        await asyncio.sleep(0)
        return await self.controller.admission_tick()

    def _poll_owned_tasks(self):
        for key, task in list(self._observation_tasks.items()):
            if task.done():
                del self._observation_tasks[key]
                if task.cancelled():
                    raise ResearchError('owned read task cancelled; admission paused, no release')
                task.result()  # Supervisor records/pauses on any unexpected failure.
        if self._diagnostic_task is not None and self._diagnostic_task.done():
            task, self._diagnostic_task = self._diagnostic_task, None
            if task.cancelled():
                raise ResearchError('owned diagnostic cancelled; admission paused, no release')
            task.result()
        state = self.controller.store.read()
        bound = state['config']['hard_total'] - state['config']['reserved_external']
        occupied_claims = {key[0] for key in self._observation_tasks}
        for claim in state['claims']:
            current = claim.get('current')
            if (current is None or claim['status'] not in OCCUPIED - {'prepared'}
                    or claim['claim_id'] in occupied_claims):
                continue
            if len(self._observation_tasks) >= bound:
                break
            key = (claim['claim_id'], current['request_id'])
            self._observation_tasks[key] = asyncio.create_task(self.controller.observe_request(*key))
            occupied_claims.add(claim['claim_id'])
        if self._diagnostic_task is None:
            self._diagnostic_task = asyncio.create_task(self.controller._diagnose_stalls())

    async def supervise(self):
        self._decoupled_observation = True
        while True:
            try:
                result = await self.step()
                if result["status"] in RUN_TERMINAL and self.safe_to_finish():
                    self.finish()
                    return result
            except StateGateBusy as exc:
                self.last_error_type = type(exc).__name__
                _persist_diagnostic(self.manifest.state_dir / "runner-diagnostic.json",
                    {"schema": "jarvis-runner-diagnostic/v1", "type": self.last_error_type,
                     "origin": "local_state_gate", "at": time.time(), "research_redispatched": False})
            except Exception as exc:
                self.last_error_type = type(exc).__name__  # Never log raw auth/config exceptions.
                _persist_diagnostic(self.manifest.state_dir / "runner-diagnostic.json",
                    {"schema": "jarvis-runner-diagnostic/v1", "type": self.last_error_type,
                     "origin": "supervisor_tick", "at": time.time(), "research_redispatched": False})
                try:
                    self.controller.control("pause")
                except Exception:
                    pass
                # No finally-stop, no restart, no redispatch, no timeout release.
            await asyncio.sleep(self.manifest.poll_seconds)


def open_control(manifest):
    """Control/status only: does not build a facade, client, Host or new state."""
    _require(manifest.database.is_file(), "existing runner database required")
    record = _json_file(manifest.state_dir / "runner-manifest.json")
    _require(record.get("manifest_sha256") == manifest.digest, "runner manifest identity mismatch")
    store = manifest.open_store()
    _require(store.read()["config"] == json.loads(json.dumps(asdict(manifest.run_config))), "persisted run/deadline mismatch")
    gate = SharedTransportGate(manifest.database, require_existing=manifest.require_existing_gate)
    return SustainedResearchController(store, gate, None)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("run", "validate", "status", "pause", "resume", "drain", "code-sha256"))
    parser.add_argument("--manifest")
    parser.add_argument("--approved-manifest-sha256")
    parser.add_argument("--lane", choices=("base", "task"))
    args = parser.parse_args(argv)
    if args.action == "code-sha256":
        print(source_sha256())
        return 0
    _require(args.manifest and args.approved_manifest_sha256, "manifest and independently approved digest are required")
    manifest = ReviewedManifest(args.manifest, args.approved_manifest_sha256, verify_runtime=args.action in {"run", "validate"})
    if args.action == "validate":
        _require(args.lane is None, "validate cannot select a single lane")
        evidence = manifest.verify_predecessors_frozen()
        print(json.dumps({"status": "validated", "manifest_sha256": manifest.digest,
            "source_code_sha256": manifest.data['source_code_sha256'], "claims": len(manifest.claims),
            "run_initialized": False, "native_preflight_executed": False,
            "private_state_absent": not manifest.state_dir.exists(), "predecessor": evidence}, ensure_ascii=False))
        return 0
    if args.action != "run":
        controller = open_control(manifest)
        result = controller.status() if args.action == "status" else controller.control(args.action, args.lane)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    _require(args.lane is None, "run cannot select a single lane")
    runtime = SustainedRuntime(manifest)
    try:
        runtime.prepare()
    except Exception:
        if runtime.host_thread is None:
            if runtime.reader is not None:
                runtime.reader.close()
            raise
        # A started Host always remains supervised, even if initial readiness
        # could not be proved. It is never replaced by another Host instance.
        runtime.last_error_type = "InitialReadinessFailure"
    def request_drain(_signum, _frame):
        try:
            runtime.controller.control("drain")
        except Exception:
            runtime.last_error_type = "DrainPersistenceFailure"
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, request_drain)
    result = asyncio.run(runtime.supervise())
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
