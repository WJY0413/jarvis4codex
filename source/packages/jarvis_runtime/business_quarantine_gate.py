"""Test-route quarantine guards. No process, terminal, claim or budget mutation."""
from contextlib import contextmanager
from pathlib import Path
import json
import sqlite3
import time

from jarvis_runtime.business_quarantine import policy_for


def policy(config=None, registry=None):
    path = getattr(config, "business_quarantine_file", None)
    return policy_for(Path(registry) if registry is not None else Path(path).parent if path else Path.cwd(),
                      explicit_path=path, expected_generation_id=getattr(config, "scheduling_generation_id", None))


def identity(request):
    request = dict(request or {})
    scope = (request.get("input_binding") or {}).get("business_scope") or {}
    # A structured scope carries classification only; cannot replace exact IDs.
    return {**{k: scope.get(k) for k in ("country", "jurisdiction_code", "claim_id", "lane", "attempt", "generation_id")},
            **{k: request[k] for k in ("country", "jurisdiction_code", "claim_id", "lane", "attempt", "generation_id",
               "request_id", "hold_id", "thread_id", "turn_id") if request.get(k) is not None}}


def _exact_admission(p, target, db):
    p.require_budget(db, request_id=target.get("request_id"), lane=target.get("lane"))
    tables = [r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
              if r[0] == "jarvis_research_run" or r[0].startswith("jarvis_research_run__")]
    matches = []
    for table in tables:
        # sqlite catalog data is quoted as an identifier, never interpolated raw.
        for raw, in db.execute('SELECT data FROM "' + table.replace('"', '""') + '"'):
            state = json.loads(raw)
            for claim in state["claims"]:
                if (claim.get("current") or {}).get("request_id") == target.get("request_id"):
                    matches.append((state, claim, table))
    if len(matches) != 1:
        raise RuntimeError("quarantine route requires one exact original-budget admission")
    state, claim, table = matches[0]
    if state["status"] != "running" or claim["status"] not in {"dispatching", "queued", "active"}:
        raise RuntimeError("quarantine route admission is not dispatchable")
    for key in ("hold_id", "country", "jurisdiction_code", "claim_id", "lane", "attempt"):
        left, right = target.get(key), claim.get(key)
        if key in {"country", "jurisdiction_code"} and isinstance(left, str) and isinstance(right, str):
            left, right = left.strip().casefold(), right.strip().casefold()
        if left != right:
            raise RuntimeError("quarantine route admission identity mismatch: " + key)
    if claim.get("thread_id") and target.get("thread_id") != claim["thread_id"]:
        raise RuntimeError("quarantine route native thread differs from admitted claim")
    from jarvis_runtime.scheduling_budget_generation import generation_for
    generation = generation_for(p)
    if generation is not None and generation.active:
        if target.get("generation_id") != generation.document["generation_id"] or state["config"].get("concurrency_policy") != "generation_batch":
            raise RuntimeError("dispatch must belong to the current explicit scheduling generation")
        from jarvis_runtime.scheduling_budget_capacity import effective_limit
        if state["config"].get("hard_total") != effective_limit(generation.document, db):
            raise RuntimeError("dispatch capacity amendment is missing or changed")
        namespace = table.removeprefix("jarvis_research_run__") if "__" in table else None
        registered = db.execute("SELECT data FROM main.jarvis_research_union WHERE namespace=?", (namespace,)).fetchone()
        record = json.loads(registered[0]) if registered else {}
        if (record.get("binding") != {"schema": "jarvis-scheduling-generation-run/v1", "generation_id": generation.document["generation_id"]}
                or record.get("run_id") != state["config"]["run_id"] or record.get("owner_id") != state["config"]["owner_id"]
                or record.get("declaration_sha256") != state["declaration_sha256"]
                or target.get("request_id") not in (state.get("generation_batch") or {}).get("requests", [])):
            raise RuntimeError("current generation/controller/pair admission is unproven")
        from jarvis_runtime.shared_transport_recovery import SharedTransportGate
        gate = object.__new__(SharedTransportGate)
        gate.provider_key, gate.clock = "openai", time.time
        if not gate.dispatch_allowed_in_transaction(db, target["request_id"]):
            raise RuntimeError("provider dispatch generation/cooldown changed; reservation retained")

@contextmanager
def dispatch_scope(config=None, registry=None, request=None, *, native_params=None):
    p = policy(config, registry)
    if p is None:
        yield
        return
    # DB writer first, then manifest lock: same order as reset/deactivate and
    # controller reservation. This short wire boundary performs no DB writes,
    # and does not wait for the owner response inside the transaction.
    database = Path(p.data["database"])
    with sqlite3.connect(database.as_uri() + "?mode=rw", uri=True, timeout=30) as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("PRAGMA query_only=ON")
        with p.read_scope():
            target = identity(request)
            p.require_allowed(target, require_scope=True)
            params = native_params or {}
            if params.get("threadId"):
                p.require_allowed({"thread_id": params["threadId"]})
                if target.get("thread_id") and params["threadId"] != target["thread_id"]:
                    raise RuntimeError("quarantine dispatch target changed")
            _exact_admission(p, target, db)
            yield


def require_not_abandoned(config=None, registry=None, request=None):
    p = policy(config, registry)
    if p is not None:
        with p.read_scope():
            p.require_allowed(identity(request))


@contextmanager
def control_scope(config=None, registry=None, request=None):
    p = policy(config, registry)
    if p is None:
        yield
        return
    with p.read_scope():
        p.require_allowed(identity(request))
        yield


def management_view(config=None, registry=None, request=None):
    p = policy(config, registry)
    if p is None:
        return None
    with p.read_scope():
        if p.blocked(identity(request)):
            budget = p.require_budget()
            if budget.get('management_cleanup_applied') is True:
                return {'management_status': 'archived_not_managed', 'automatic_dispatch_allowed': False,
                        'automatic_takeover_allowed': False, 'abandonment_proves_terminal': False,
                        'management_occupancy_retained': False, 'native_state_unknown': True,
                        'archive': budget['management_archive']}
            return {"management_status": "abandoned_quarantined" if p.data["mode"] == "quarantined" else "restored_paused",
                    "automatic_dispatch_allowed": False, "automatic_takeover_allowed": False,
                    "abandonment_proves_terminal": False, "exposure_retained": True}
    return None
