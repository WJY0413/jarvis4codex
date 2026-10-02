"""Product-level contract and exact source-bound routing operations.

This module implements ordinary controller effects, not an acceptance suite.
Callers own their acceptance conditions and never select the lower adapter.
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import inspect
import json
import os
from pathlib import Path
from typing import Any

from jarvis_runtime.coo_dispatcher_store import ProcessLock

CONTRACT = "jarvis-controller-contract/v1"
BUILD = "Jarvis 0.2.6 Linux (dot.1)"
ACTIVE = {"active", "running", "inprogress", "in_progress", "pending", "unknown"}


def now():
    return datetime.now(timezone.utc).isoformat()


def encoded(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def digest(value: bytes):
    return hashlib.sha256(value).hexdigest()


def source_identity():
    root = Path(__file__).resolve().parents[1]
    files = {str(p.relative_to(root)): digest(p.read_bytes()) for p in sorted(root.rglob("*.py")) if "__pycache__" not in p.parts}
    return {"build": BUILD, "code_sha256": digest(encoded(files)), "files": files}


class OperationReceipts:
    def __init__(self, root: Path):
        self.root = root

    def _path(self, request_id):
        if not isinstance(request_id, str) or not request_id.strip() or len(request_id) > 200:
            raise ValueError("bounded non-empty request_id required")
        return self.root / (digest(request_id.encode()) + ".json")

    def lock(self, request_id):
        return ProcessLock(self._path(request_id).with_suffix(".lock"), timeout_seconds=1, stale_seconds=86400)

    def read(self, request_id):
        path = self._path(request_id)
        if not path.exists():
            return None
        raw = path.read_bytes(); value = json.loads(raw)
        if value.get("request_id") != request_id:
            raise ValueError("receipt identity mismatch")
        return value

    def save(self, value):
        path = self._path(value["request_id"])
        path.parent.mkdir(parents=True, exist_ok=True)
        raw = encoded(value); tmp = path.with_suffix(".tmp")
        with tmp.open("wb") as handle:
            handle.write(raw); handle.flush(); os.fsync(handle.fileno())
        os.replace(tmp, path)
        if os.name != "nt":
            fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try: os.fsync(fd)
            finally: os.close(fd)
        with (path.parent / "operations.jsonl").open("ab") as handle:
            handle.write(raw); handle.flush(); os.fsync(handle.fileno())
        observed = path.read_bytes()
        if observed != raw:
            raise RuntimeError("durable operation receipt readback mismatch")
        return digest(observed)


class ControllerOperations:
    def __init__(self, control, root: Path):
        self.control = control
        self.receipts = OperationReceipts(root)

    def register_test_target(self, *, run_id, role, request_id, provision):
        if not isinstance(run_id, str) or not run_id.strip() or not isinstance(role, str) or not role.strip():
            raise ValueError("TEST target registration requires run_id and role")
        key = "target:" + run_id + ":" + role
        with self.receipts.lock(key):
            existing = self.receipts.read(key)
            if existing:
                if existing.get("provision_request_id") != request_id:
                    raise ValueError("TEST run role already bound to another target")
                return existing
            value = {"schema": "jarvis-operation-receipt/v1", "kind": "target", "request_id": key,
                     "scope": "TEST", "run_id": run_id, "role": role, "provision_request_id": request_id,
                     "hold_id": (provision.get("data") or {}).get("hold_id"), "status": provision["status"],
                     "provision_receipt": provision, "observed_at": now()}
            self.receipts.save(value); return value

    def ensure_test_target(self, run_id, thread_id):
        if not run_id or not thread_id:
            raise ValueError("run_id and exact TEST target identity are required")
        for path in self.receipts.root.glob("*.json"):
            value = json.loads(path.read_bytes())
            if value.get("kind") != "target" or value.get("scope") != "TEST" or value.get("run_id") != run_id:
                continue
            hold = self.control.read(subject="hold", hold_id=value.get("hold_id"))
            data = hold.get("data") or {}
            if (hold.get("status") == "completed" and data.get("thread_id") == thread_id
                    and data.get("request_id") == value["provision_request_id"]
                    and data.get("terminal_confirmed") is True and data.get("hold_released") is True):
                return {"registration": value, "hold_readback": hold}
        raise ValueError("target is not a released, identity-proven TEST role of this run")

    def _envelope(self, value):
        status = value["status"]
        terminal = status in {"completed", "failed", "interrupted", "blocked", "requires_readback"}
        return self.control._receipt("jarvis_" + value["kind"], status,
            request_id=value["request_id"], target_thread_id=value.get("target_thread_id"),
            turn_id=value.get("turn_id"), reason=value.get("reason"), data=value,
            readback={"verified": True, "terminal": terminal, "receipt_sha256": digest(encoded(value)),
                      "receipt_ref": value["request_id"]})

    def contract_check(self, request_id, contract_id, expected_code_sha256, expected_config_sha256):
        with self.receipts.lock(request_id):
            previous = self.receipts.read(request_id)
            declaration = {"contract_id": contract_id, "expected_code_sha256": expected_code_sha256, "expected_config_sha256": expected_config_sha256}
            if previous:
                if previous.get("declaration") != declaration:
                    raise ValueError("request_id already bound to another contract check")
                return self._envelope(previous)
            checks = []
            def check(name, observed, expected, passed):
                checks.append({"name": name, "observed": observed, "expected": expected, "passed": bool(passed)})
            check("declared_contract", contract_id, CONTRACT, contract_id == CONTRACT)
            identity = source_identity()
            check("frozen_source_identity", identity["code_sha256"], expected_code_sha256,
                  isinstance(expected_code_sha256, str) and identity["code_sha256"] == expected_code_sha256)
            configuration_hashes = {key: digest(path.read_bytes()) for key, path in self.control._deployment_paths.items()}
            check("frozen_deployment_identity", digest(encoded(configuration_hashes)), expected_config_sha256,
                  bool(configuration_hashes) and digest(encoded(configuration_hashes)) == expected_config_sha256)
            try:
                registry = self.control._registry_reader()
                names = [tool["name"] for tool in registry]
                required = {"jarvis_contract_check", "jarvis_receipt", "jarvis_create", "jarvis_read", "jarvis_callback", "jarvis_relay", "jarvis_close", "jarvis_dispatch", "jarvis_capacity", "jarvis_notify", "jarvis_read_delivery", "jarvis_resume"}
                check("registered_public_effects", names, sorted(required), len(names) == len(set(names)) and required.issubset(names))
                check("registry_input_contracts", [t["name"] for t in registry if t.get("inputSchema", {}).get("type") != "object"], [],
                      all(t.get("inputSchema", {}).get("type") == "object" for t in registry))
            except Exception as exc:
                registry = []; check("registered_public_effects", type(exc).__name__ + ": " + str(exc), "readable registry", False)
            try:
                snapshot = self.control._provisioner.contract_snapshot()
                check("configured_target_projects", snapshot["projects"], "nonempty existing allowlisted projects", bool(snapshot["projects"]) and snapshot["projects_exist"])
                check("declared_execution_configuration", snapshot["configuration_valid"], True, snapshot["configuration_valid"])
            except Exception as exc:
                snapshot = {}; check("declared_execution_configuration", type(exc).__name__ + ": " + str(exc), "valid effective deployment", False)
            capabilities = self.control.read(subject="capabilities")
            check("capability_receipt_schema", capabilities.get("schema"), "jarvis-mcp-receipt/v1", capabilities.get("schema") == "jarvis-mcp-receipt/v1")
            check("core_control_ports", {name: callable(getattr(self.control, name, None)) for name in ["create", "read", "resume", "jarvis_close"]}, "all callable",
                  all(callable(getattr(self.control, name, None)) for name in ["create", "read", "resume", "jarvis_close"]))
            value = {"schema": "jarvis-operation-receipt/v1", "kind": "contract_check", "request_id": request_id,
                     "declaration": declaration, "status": "completed" if all(x["passed"] for x in checks) else "failed",
                     "observed_at": now(), "checks": checks, "source_identity": identity,
                     "registry": registry, "configuration_snapshot": snapshot, "configuration_hashes": configuration_hashes, "capability_readback": capabilities,
                     "meaning": "terminal evaluation of controller contract and exact current deployment, not evidence that later effects or suite cases passed"}
            self.receipts.save(value)
            return self._envelope(self.receipts.read(request_id))

    def _source(self, thread_id, turn_id):
        state = self.control._bridge.observe_thread(thread_id)
        if state.thread_id != thread_id:
            raise ValueError("source target readback identity mismatch")
        matches = [turn for turn in state.turns if turn.turn_id == turn_id]
        if len(matches) != 1 or matches[0].effective_status.lower() != "completed" or matches[0].error:
            raise ValueError("exact source turn is not verified completed")
        finals = [item.get("text") for item in matches[0].items if item.get("type") == "agentMessage" and item.get("phase") == "final_answer"]
        if len(finals) != 1 or not isinstance(finals[0], str) or not finals[0]:
            raise ValueError("source needs exact non-empty final output readback")
        return finals[0], asdict(state)

    def capacity(self, *, request_id, run_id, project, inputs):
        declaration = {"run_id": run_id, "project": project, "inputs": inputs, "capacity": len(inputs)}
        with self.receipts.lock(request_id):
            previous = self.receipts.read(request_id)
            if previous:
                if previous.get("kind") != "capacity" or previous.get("declaration") != declaration:
                    raise ValueError("capacity request_id already bound to different inputs or capacity")
                return self._read_capacity(previous)
            value = {"schema": "jarvis-operation-receipt/v1", "kind": "capacity", "request_id": request_id,
                     "declaration": declaration, "status": "starting", "children": [], "observed_at": now()}
            self.receipts.save(value)
            health = self.control._provisioner.ensure_hold_host_ready(required_workers=len(inputs))
            if health.get("status") != "ready":
                value.update(status="blocked", reason=health.get("reason"), host_readback=health)
                self.receipts.save(value); return self._envelope(value)
            try:
                for n, prompt in enumerate(inputs, 1):
                    child = self.control.create(request_id=f"{request_id}:{n}", project=project, prompt=prompt,
                        source_ref=f"TEST:{run_id}:capacity", run_id=run_id, role=f"capacity-{request_id}-{n}", test_only=True)
                    value["children"].append({"slot": n, "request_id": f"{request_id}:{n}",
                        "hold_id": (child.get("data") or {}).get("hold_id"), "receipt": child})
                    self.receipts.save(value)
                    if child["status"] not in {"accepted", "holding", "running", "completed"}:
                        value.update(status="blocked", reason="batch stopped at first unaccepted action; no replacement")
                        self.receipts.save(value); return self._envelope(value)
                value.update(status="accepted"); self.receipts.save(value)
                return self._envelope(value)
            except Exception as exc:
                value.update(status="requires_readback", reason=str(exc)); self.receipts.save(value)
                return self._envelope(value)

    def _read_capacity(self, value):
        if value["status"] != "accepted":
            if value["status"] == "starting":
                value.update(status="requires_readback", reason="partial capacity dispatch intent; no automatic re-submission")
                self.receipts.save(value)
            return self._envelope(value)
        observations = []
        try:
            for child in value["children"]:
                readback = self.control.read(subject="hold", hold_id=child["hold_id"])
                data = readback.get("data") or {}
                observation = {"request_id": child["request_id"], "hold_readback": readback}
                if data.get("request_id") != child["request_id"]:
                    raise ValueError("capacity child request identity mismatch")
                if data.get("status") in {"failed", "interrupted", "blocked", "cancelled", "canceled"}:
                    value.update(status="interrupted" if data["status"] == "interrupted" else "failed", reason=data.get("reason"))
                    observations.append(observation); break
                if (data.get("status") in {"completed", "turn_limit_reached"} and data.get("terminal_confirmed") is True
                        and data.get("hold_released") is True and data.get("local_observer_stopped") is True):
                    final, native = self._source(data.get("thread_id"), data.get("turn_id"))
                    observation.update(thread_id=data["thread_id"], turn_id=data["turn_id"], output=final, target_readback=native, terminal=True)
                observations.append(observation)
            if value["status"] == "accepted" and len(observations) == value["declaration"]["capacity"] and all(x.get("terminal") for x in observations):
                identities = [(x["thread_id"], x["turn_id"]) for x in observations]
                if len(set(identities)) != len(identities) or len({x[0] for x in identities}) != len(identities):
                    raise ValueError("duplicate or ambiguous capacity target identities")
                value.update(status="completed", completed_at=now())
            value["observations"] = observations; self.receipts.save(value)
        except Exception as exc:
            value.update(status="requires_readback", reason=str(exc), observations=observations); self.receipts.save(value)
        return self._envelope(value)

    def route(self, *, kind, request_id, run_id, source_thread_id, source_turn_id, target_thread_id, source_ref, target_input=None):
        if kind not in {"callback", "relay"}:
            raise ValueError("unsupported route effect")
        declaration = {"kind": kind, "run_id": run_id, "source_thread_id": source_thread_id, "source_turn_id": source_turn_id,
                       "target_thread_id": target_thread_id, "source_ref": source_ref, "target_input": target_input}
        with self.receipts.lock(request_id):
            old = self.receipts.read(request_id)
            if old:
                if old.get("declaration") != declaration:
                    raise ValueError("request_id already bound to another route")
                return self._read_route(old)
            self.ensure_test_target(run_id, source_thread_id)
            self.ensure_test_target(run_id, target_thread_id)
            output, source = self._source(source_thread_id, source_turn_id)
            target = self.control._bridge.observe_thread(target_thread_id)
            if target.thread_id != target_thread_id or target.effective_status.lower() in ACTIVE:
                raise ValueError("callback/relay target is busy or eligibility unknown")
            if kind == "callback":
                if not isinstance(target_input, str) or not target_input.strip():
                    raise ValueError("callback target_input is required")
                prompt = json.dumps({"event": "source_completed", "source_thread_id": source_thread_id,
                                     "source_turn_id": source_turn_id, "output": output}, ensure_ascii=False) + "\n" + target_input
            else:
                prompt = output  # No prefix, normalization, trimming or reconstruction.
            hold_id = "hold-" + request_id
            value = {"schema": "jarvis-operation-receipt/v1", "kind": kind, "request_id": request_id,
                     "declaration": declaration, "observed_at": now(), "status": "starting",
                     "source_readback": source, "source_output": output, "source_output_sha256": digest(output.encode()),
                     "target_thread_id": target_thread_id, "target_eligibility": asdict(target), "target_input": prompt, "hold_id": hold_id}
            self.receipts.save(value)
            try:
                result = self.control.resume(request_id=request_id, task_id=target_thread_id, prompt=prompt,
                    source_ref=source_ref, max_turns=1, auto_continue=False, notifications={"terminal": False, "milestones": []})
                actual_hold = (result.get("data") or {}).get("hold_id")
                value.update(status=result["status"], resume_receipt=result, hold_id=actual_hold or hold_id,
                             reason=result.get("reason"))
                self.receipts.save(value)
                return self._read_route(value)
            except Exception as exc:
                value.update(status="requires_readback", reason=str(exc))
                self.receipts.save(value); return self._envelope(value)

    def _read_route(self, value):
        if value["status"] in {"completed", "failed", "interrupted", "blocked", "requires_readback"}:
            return self._envelope(value)
        try:
            hold = self.control.read(subject="hold", hold_id=value["hold_id"])
            data = hold.get("data") or {}
            status = data.get("status")
            if status in {"failed", "interrupted", "blocked", "cancelled", "canceled"}:
                value.update(status="interrupted" if status == "interrupted" else "failed", hold_readback=hold, reason=data.get("reason"))
            elif (status in {"completed", "turn_limit_reached"} and data.get("terminal_confirmed") is True
                  and data.get("hold_released") is True and data.get("local_observer_stopped") is True):
                tid, turn_id = data.get("thread_id"), data.get("turn_id")
                if tid != value["target_thread_id"] or not turn_id:
                    raise ValueError("route terminal target/action mismatch")
                final, native = self._source(tid, turn_id)
                turn = next(t for t in native["turns"] if t["turn_id"] == turn_id)
                inputs = [i for i in turn["items"] if i.get("type") == "userMessage"]
                text = "".join(part.get("text", "") for i in inputs for part in i.get("content", []) if part.get("type") == "text")
                if text != value["target_input"]:
                    raise ValueError("exact target input body was altered")
                if value["kind"] == "relay" and final != value["source_output"]:
                    raise ValueError("relay output bytes differ from source")
                value.update(status="completed", turn_id=turn_id, output=final, target_readback=native, hold_readback=hold,
                             output_sha256=digest(final.encode()), completed_at=now())
            elif value["status"] == "starting":
                value.update(status="requires_readback", reason="route starting outcome is not established; no replacement issued", hold_readback=hold)
            else:
                value.update(hold_readback=hold)
            self.receipts.save(value)
        except Exception as exc:
            value.update(status="requires_readback", reason=str(exc)); self.receipts.save(value)
        return self._envelope(value)

    def read(self, request_id):
        with self.receipts.lock(request_id):
            value = self.receipts.read(request_id)
            if value is None:
                return self.control._receipt("jarvis_receipt", "blocked", request_id=request_id, reason="operation receipt not found")
            if value["kind"] in {"callback", "relay"}:
                return self._read_route(value)
            if value["kind"] == "capacity":
                return self._read_capacity(value)
            return self._envelope(value)
