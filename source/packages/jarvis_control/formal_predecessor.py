"""Read-only, review-bound evidence for a fresh-identity formal successor.

No Host, native client, controller writer, or recovery operation is constructed.
The predecessor stays paused with its original Host retained. This is deliberately
not a resume/import of its controller or native identities.
"""
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sqlite3

from jarvis_control.file_task import strict_json
from jarvis_control.sustained_research import OCCUPIED, ResearchError, ResearchStore, TERMINAL
from adapters.codex_app_server.native_execution_evidence import OWNER_TERMINAL_EVIDENCE, classify_owner_terminal_receipt


def _require(value, reason):
    if not value:
        raise ResearchError(reason)


def _stored_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def _path(value):
    path = Path(value)
    _require(path.is_absolute() and path.resolve() == path and not path.is_symlink(), "predecessor path must be absolute without links")
    return path


def read_formal_snapshot(database, provider="openai"):
    """One SQLite read transaction, including WAL; ignores only tick revision."""
    path = _path(database)
    _require(path.is_file(), "formal predecessor database missing")
    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=10) as db:
        db.execute("BEGIN")
        rows = db.execute("SELECT id,data FROM jarvis_research_run").fetchall()
        _require(len(rows) == 1 and rows[0][0] == 1, "exact predecessor run required")
        state = strict_json(rows[0][1])
        ResearchStore._validate_state(state)
        state.pop("revision")  # Paused v1 ticks still advance this counter.
        tables = {x[0] for x in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        checkpoints = [dict(claim_id=c, attempt=a, digest=h, data=strict_json(v)) for c, a, h, v in
                       db.execute("SELECT claim_id,attempt,digest,data FROM jarvis_research_checkpoints ORDER BY claim_id,attempt")]
        scans = [dict(claim_id=c, attempt=a, request_id=r, digest=h, data=strict_json(v)) for c, a, r, h, v in
                 db.execute("SELECT claim_id,attempt,request_id,digest,data FROM jarvis_research_native_scans ORDER BY claim_id,attempt")] if "jarvis_research_native_scans" in tables else []
        _require(all(_stored_hash(x["data"]) == x["digest"] for x in checkpoints + scans), "predecessor checkpoint/receipt digest mismatch")
        gates = db.execute("SELECT provider,data FROM jarvis_transport_gate").fetchall()
        _require(len(gates) == 1 and gates[0][0] == provider, "exact predecessor provider required")
        receipts = [dict(kind=k, receipt_id=r, data=strict_json(v)) for p, k, r, v in
                    db.execute("SELECT provider,kind,receipt_id,data FROM jarvis_transport_receipts ORDER BY provider,kind,receipt_id") if p == provider]
        _require(db.execute("SELECT COUNT(*) FROM jarvis_transport_receipts WHERE provider<>?", (provider,)).fetchone()[0] == 0,
                 "unexpected predecessor provider receipts")
        return dict(schema="jarvis-formal-predecessor-snapshot/v1", state=state, checkpoints=checkpoints,
                    native_scans=scans, transport=dict(provider=provider, gate=strict_json(gates[0][1]), receipts=receipts))


def verify_retired_non_dispatch_roots(registry, state, receipts):
    """Prove every extra retired root with original finite non-dispatch authority.

    This does not create a native terminal witness or excuse the current Hold's
    normal close. The immutable retirement receipt is also pinned by the union.
    """
    from jarvis_control.failed_before_send_consumer import certificate, SCHEMA, POLICY
    registry = _path(registry)
    _require(isinstance(receipts, list), 'retirement receipt list required')
    roots, threads = set(), set()
    claims = {c['claim_id']: c for c in state['claims']}
    for record in receipts:
        _require(record.get('schema') == SCHEMA and record.get('native_terminal_fabricated') is False
                 and record.get('original_request_redispatched') is False
                 and record.get('physical_owner_exit_confirmed') is None,
                 'exact truthful identity-retirement receipt required')
        old, history, plan = record['old_claim'], record['history'], record['plan']
        claim = claims.get(old['claim_id'])
        _require(claim is not None and claim['hold_id'] == record['new_hold_id'] != old['hold_id']
                 and claim.get('thread_id') != old['thread_id']
                 and all(_stored_hash(claim[k]) == _stored_hash(old[k]) for k in ('country','jurisdiction_code','lane')),
                 'retired identity must differ from exact current jurisdiction Hold')
        prefix = old['history'] + [history]
        _require(_stored_hash(claim['history'][:len(prefix)]) == _stored_hash(prefix)
                 and history.get('request_id') == old['current']['request_id']
                 and history.get('hold_id') == old['hold_id'] and history.get('thread_id') == old['thread_id']
                 and history.get('turn_id') is None and history.get('terminal_confirmed') is False
                 and history.get('native_terminal_confirmed') is False
                 and history.get('non_dispatch_settled') is True and history.get('hold_released') is True,
                 'original non-dispatch history must remain an unchanged prefix')
        root = registry / 'task-holds' / old['hold_id']
        _require(str(root) == plan['target']['certificate_root']
                 and plan.get('registry') == str(registry)
                 and record['new_hold_id'] == plan['target']['new_hold_id'],
                 'retired original registry binding mismatch')
        proof, hashes = certificate(root, old)
        authority = history.get('finite_non_dispatch_authority') or {}
        _require(authority.get('source') == 'finite_terminal_settlement_v2'
                 and authority.get('kind') == 'failed_before_send'
                 and authority.get('successor_policy') == POLICY
                 and authority.get('certificate_root') == str(root)
                 and authority.get('certificate_sha256') == hashes['certificate_sha256']
                 and all(plan['target'].get(k) == v for k,v in hashes.items())
                 and _stored_hash(plan['target']['proof']) == _stored_hash(proof),
                 'retired certificate/result/archive dependency changed')
        _require(str(root) not in roots, 'duplicate retired management root')
        roots.add(str(root)); threads.add(old['thread_id'])
    return {'roots': roots, 'threads': threads}


def verify_normal_close_fences(registry, state, fences, pinned, *, retirement_receipts=None):
    """Shared exact normal-close authority check; no state or lifecycle writes."""
    from adapters.codex_app_server import jarvis_hold_host_service as hosts
    registry = _path(registry)
    now = hosts._parse_time(hosts._now())
    _require(state.get('status') == 'paused' and all(c.get('current') is None
        and c.get('status') not in OCCUPIED and c.get('actual_observation') is None for c in state['claims']),
        'retired namespace must remain paused with no unresolved ownership')
    _require(isinstance(fences, list) and len(fences) == len({f['claim_id'] for f in fences})
        and {f['claim_id'] for f in fences} == {c['claim_id'] for c in state['claims'] if c.get('thread_id')},
        'exact close/fence coverage required for every created predecessor claim')
    claims = {c["claim_id"]: c for c in state["claims"]}
    for fence in fences:
        claim = claims[fence["claim_id"]]
        root = _path(fence["root"])
        _require(root.parent.name in {"task-holds", "task-monitors"} and root.parent.parent == registry and root.name == claim["hold_id"],
                 "close proof must name the original registry Hold")
        records = {name: pinned(dict(path=str(root / (name + ".json")), sha256=fence[name + "_sha256"]))
                   for name in ("request", "result", "close", "cancellation")}
        request, result, close, cancellation = (records[x] for x in ("request", "result", "close", "cancellation"))
        latest = next((x for x in reversed(claim["history"]) if x.get("outcome") != "not_sent"), None)
        identity = dict(request_id=latest["request_id"], hold_id=claim["hold_id"], thread_id=claim["thread_id"], turn_id=latest["turn_id"])
        _require(all(result.get(k) == v and close.get(k) == v for k, v in identity.items())
                 and all(request.get(k) == identity[k] for k in ("request_id", "hold_id")), "close/owner generation mismatch")
        _require(result.get("terminal_confirmed") is True and result.get("status") in TERMINAL
                 and result.get("terminal_evidence") in OWNER_TERMINAL_EVIDENCE
                 and close.get("schema") == "jarvis-close-report/v1" and close.get("report_status") == "completed"
                 and close.get("execution_state") == "terminal" and close.get("terminal_confirmed") is True
                 and close.get("hold_released") is True and close.get("scheduling_closed") is True
                 and close.get("management_closed") is True and close.get("external_execution_unresolved") is False
                 and not close.get("non_dispatch_confirmed"),
                 "exact native terminal and supported close/release required")
        model = next(x["model"] for x in state["config"]["lanes"] if x["name"] == claim["lane"])
        readback = {}
        if result["terminal_evidence"] == "owner_exact_terminal_readback":
            _require("native_readback" in fence, "owner fallback requires pinned complete native readback proof")
            readback = pinned(fence["native_readback"])
            read_at = hosts._parse_time(readback.get("read_at"))
            _require(readback.get("schema") == "jarvis-predecessor-native-readback/v1"
                     and readback.get("source") == "native_exact_turn_read"
                     and readback.get("identity") == identity and readback.get("model") == model
                     and readback.get("owner_result_sha256") == fence["result_sha256"]
                     and read_at is not None and read_at <= now, "original exact-read provenance mismatch")
        terminal = classify_owner_terminal_receipt(result, **identity, model=model,
            native_turn=readback.get("turn"), observed_thread_id=readback.get("observed_thread_id"),
            protocol_version=readback.get("protocol_version"))
        _require(terminal["terminal_confirmed"] is True, "original owner terminal witness failed shared authority classification")
        original = pinned(fence["original_request"])
        _require(_stored_hash(pinned(fence["original_result"])) == _stored_hash(result) and fence["original_result"]["sha256"] == fence["result_sha256"],
                 "supported close must preserve original owner result bytes")
        _require(_stored_hash(request) == _stored_hash(original) or _stored_hash(request) == _stored_hash({**original, "stop_requested": True}), "unsupported predecessor request change")
        _require(cancellation.get("schema") == "jarvis-management-cancellation/v1" and cancellation.get("scope") == "hold"
                 and cancellation.get("subject_id") == identity["hold_id"] and cancellation.get("request_id") == identity["request_id"]
                 and cancellation.get("dispatch_cancelled") is True and cancellation.get("close_request_id") == close.get("close_request_id")
                 and cancellation.get("original_sha256") == fence["original_request"]["sha256"]
                 and close.get("cancellation") == cancellation and all(original.get(k) == identity[k] for k in ("request_id", "hold_id")),
                 "durable exact-generation cancellation proof missing")
        _require(not (root / ".user-host-claim").exists() and not (root / "request.lock").exists(), "predecessor owner/dispatch lock remains")
    # A new/aliased management root must never hide outside the reviewed list.
    retired = verify_retired_non_dispatch_roots(registry, state, retirement_receipts or [])
    expected = {str(_path(x["root"])) for x in fences} | retired["roots"]
    actual = {str(p.parent) for name in ("task-holds", "task-monitors") for p in (registry / name).glob("*/request.json")}
    _require(actual == expected, "unreviewed predecessor management root")
    return {'closed': len(fences)}


class FormalPredecessor:
    def __init__(self, manifest):
        self.manifest = manifest
        self.spec = manifest.data["formal_predecessor"]
        _require(self.spec.get("schema") == "jarvis-formal-predecessor/v1", "unsupported formal predecessor")
        _require(isinstance(self.spec.get("host_instance_id"), str) and self.spec["host_instance_id"]
                 and type(self.spec.get("host_pid")) is int and self.spec["host_pid"] > 0,
                 "exact retained predecessor Host identity required")
        self.root = _path(self.spec["state_dir"])
        _require(self.root != manifest.state_dir and self.root not in manifest.state_dir.parents
                 and manifest.state_dir not in self.root.parents, "distinct disjoint successor state required")
        self.original = self._pinned(self.spec["manifest"])
        self.snapshot = self._pinned(self.spec["snapshot"])
        _require(self.original.get("schema") == "jarvis-sustained-runner/v1" and self.original.get("state_dir") == str(self.root)
                 and self.original.get("legacy") == manifest.data["legacy"], "predecessor registry/legacy provenance mismatch")
        for path, digest in self.original["frozen_inputs"].items():
            _require(manifest.data["frozen_inputs"].get(path) == digest, "predecessor frozen evidence must remain pinned")
        self.inherited_threads = set()
        def collect(value):
            if isinstance(value, dict):
                for key, item in value.items():
                    if key in {"thread_id", "threadId", "prediction_thread_id", "parent_thread_id"} and isinstance(item, str) and item:
                        self.inherited_threads.add(item)
                    elif key == "excluded_native_thread_ids" and isinstance(item, list):
                        self.inherited_threads.update(x for x in item if isinstance(x, str) and x)
                    collect(item)
            elif isinstance(value, list):
                for item in value:
                    collect(item)
        for path, digest in self.original["frozen_inputs"].items():
            if Path(path).suffix == ".json":
                collect(self._pinned(dict(path=path, sha256=digest)))
        self._validate_snapshot()

    def _pinned(self, binding):
        path, digest = _path(binding["path"]), binding["sha256"]
        _require(self.manifest.data["frozen_inputs"].get(str(path)) == digest, "predecessor evidence is not review-pinned")
        raw = path.read_bytes()
        _require(hashlib.sha256(raw).hexdigest() == digest, "predecessor pinned evidence changed")
        return strict_json(raw.decode("utf-8"))

    def _validate_snapshot(self):
        snapshot, config = self.snapshot, asdict(self.manifest.run_config)
        _require(snapshot.get("schema") == "jarvis-formal-predecessor-snapshot/v1", "unsupported predecessor snapshot")
        state = snapshot["state"]
        ResearchStore._validate_state(state)
        _require(state["config"] == self.original["run"], "original predecessor declaration mismatch")
        _require(state["status"] == "paused" and not state.get("closed_at") and not state.get("deadline_snapshot"),
                 "predecessor must be supported-paused before deadline; never drain for restart")
        _require(state["config"]["run_id"] != config["run_id"] and state["config"]["owner_id"] != config["owner_id"],
                 "successor requires distinct run and owner identities")
        for key in config:
            if key not in {"run_id", "owner_id"}:
                if (config["hard_total"] == 35 and state["config"]["hard_total"] == 25
                        and key in {"hard_total", "expanded_capacity_authorization"}):
                    continue  # ReviewedManifest separately binds exact35 approval.
                previous = state["config"].get(key, "healthy_ramp" if key == "concurrency_policy" else None)
                if (key == "concurrency_policy" and config[key] == "healthy_batch" and previous == "healthy_ramp"
                        and config["hard_total"] == 35):
                    continue  # Explicit approved successor declaration only.
                _require(json.loads(json.dumps(config[key])) == previous, "successor policy/deadline must remain unchanged")
        old = {x["claim_id"]: x for x in state["claims"]}
        new = {x.claim_id: asdict(x) for x in self.manifest.claims}
        _require(old.keys() == new.keys(), "successor must preserve finite predecessor claim scope")
        required_threads, request_ids = set(self.inherited_threads), set()
        for key, claim in old.items():
            _require(claim["status"] not in OCCUPIED and claim.get("current") is None and claim.get("actual_observation") is None,
                     "predecessor has occupied, unknown or in-flight work")
            _require(all(claim.get(k) == v for k, v in new[key].items()), "successor checkpoint/jurisdiction declaration changed")
            if claim["thread_id"]:
                required_threads.add(claim["thread_id"])
            for record in claim["history"]:
                request_ids.add(record["request_id"])
                if record.get("outcome") == "not_sent":
                    _require(not record.get("turn_id") and record.get("sent_at") is None, "unsubmitted predecessor history is ambiguous")
                else:
                    _require(record.get("terminal_confirmed") is True and record.get("hold_released") is True
                             and record.get("turn_id"), "predecessor history lacks exact terminal/release")
                if record.get("thread_id"):
                    required_threads.add(record["thread_id"])
        gate = snapshot["transport"]["gate"]
        external = self.manifest.data["legacy"]["reservations"]
        _require(not gate["pending"] and gate["probe"] is None, "predecessor pending/probe must settle before successor")
        _require(set(gate["reservations"]) == set(external) | request_ids, "unexplained predecessor reservations")
        for rid, row in gate["reservations"].items():
            if rid in external:
                _require(all(row.get(k) == external[rid].get(k) for k in ("owner", "lane", "status", "cost")), "external ten reservation changed")
            else:
                _require(row.get("status") == "finished" and row.get("owner") == state["config"]["owner_id"], "predecessor owner reservation unresolved")
        exclusions = self.spec["external_native_exclusions"]
        _require(set(exclusions) == set(external), "all ten external native exclusions required")
        proof_binding = self.spec["external_exclusion_proof"]
        _require(self.original["frozen_inputs"].get(proof_binding["path"]) == proof_binding["sha256"], "external exclusion proof must be original predecessor evidence")
        proof = self._pinned(proof_binding)
        targets = proof.get("targets", [])
        _require(proof.get("schema") == "jarvis-task-successor-exclusions/v1" and len(targets) == 10
                 and {x.get("request_id") for x in targets} == set(external)
                 and all(exclusions[x["request_id"]] == {k:x.get(k) for k in ("thread_id", "turn_id")} for x in targets),
                 "external native exclusions differ from original reviewed evidence")
        for row in exclusions.values():
            _require(isinstance(row.get("thread_id"), str) and row["thread_id"] and isinstance(row.get("turn_id"), str) and row["turn_id"],
                     "exact excluded external native identity required")
            required_threads.add(row["thread_id"])
        prior = self.original.get("formal_predecessor", {})
        required_threads.update(prior.get("excluded_native_thread_ids", []))
        self.excluded = frozenset(self.spec["excluded_native_thread_ids"])
        _require(len(self.excluded) == len(self.spec["excluded_native_thread_ids"])
                 and all(isinstance(x, str) and x for x in self.excluded) and required_threads <= self.excluded,
                 "predecessor native identity exclusion omitted")
        fences = self.spec["close_fences"]
        _require(len(fences) == len({x["claim_id"] for x in fences})
                 and {x["claim_id"] for x in fences} == {c["claim_id"] for c in old.values() if c["thread_id"]},
                 "exact close/fence coverage required for every created predecessor claim")

    def verify(self):
        """Check original paused semantics and immutable exact fences, read-only."""
        _require(self._pinned(self.spec["manifest"]) == self.original and self._pinned(self.spec["snapshot"]) == self.snapshot,
                 "formal predecessor declaration changed")
        _require(_stored_hash(read_formal_snapshot(self.root / "research.sqlite3")) == _stored_hash(self.snapshot),
                 "formal predecessor budget/checkpoint/ownership changed")
        binding = strict_json((self.root / "runner-manifest.json").read_text())
        _require(binding.get("manifest_sha256") == self.spec["manifest"]["sha256"], "predecessor runtime manifest binding mismatch")
        from adapters.codex_app_server import jarvis_hold_host_service as hosts
        health = strict_json((self.root / "hold-host.json").read_text())
        retained = self._pinned(self.spec["retained_host_proof"])
        _require(retained == dict(schema="jarvis-retained-predecessor-host/v1", state_dir=str(self.root),
                 host_instance_id=self.spec["host_instance_id"], pid=self.spec["host_pid"], action="retain", stop_requested=False),
                 "reviewed original Host retention proof required")
        observed, now = hosts._parse_time(health.get("observed_at")), hosts._parse_time(hosts._now())
        # A new exec namespace may not see the old PID. A fresh exact publication
        # and reviewed retention intent are not represented as process liveness.
        _require(health.get("host_instance_id") == self.spec["host_instance_id"] and health.get("pid") == self.spec["host_pid"]
                 and type(health.get("pid")) is int and health["pid"] > 0 and type(health.get("active_count")) is int
                 and health.get("active_count") == 0 and health.get("active_hold_ids") == []
                 and health.get("state_dir") == str(self.root) and health.get("profile") == self.manifest.launcher.profile
                 and health.get("codex_home") == self.manifest.launcher.expected_codex_home and health.get("status") == "ready"
                 and health.get("worker_capacity") == 50 and observed is not None and now is not None
                 and 0 <= (now - observed).total_seconds() <= hosts._HOST_HEALTH_MAX_AGE.total_seconds(),
                 "original retained Host publication must remain exact and idle; never replace it")
        verify_normal_close_fences(self.root, self.snapshot['state'], self.spec['close_fences'], self._pinned)
        return self.snapshot["transport"]
