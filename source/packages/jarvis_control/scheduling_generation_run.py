"""Fresh namespace admission for an explicitly authorized scheduling generation.

This is not a provider reset. The original SharedTransportGate remains the only
start/token/cooldown/probe gate and no original task state is rewritten.
"""
import json
from dataclasses import asdict

SCHEMA = 'jarvis-scheduling-generation-run/v1'


def generation(store):
    from jarvis_runtime.scheduling_budget_generation import generation_for
    from jarvis_control.sustained_research import _require
    policy = store.quarantine()
    _require(policy is not None, 'generation run requires the original quarantine policy')
    result = generation_for(policy)
    _require(result is not None and result.active, 'active scheduling generation required')
    _require(store.shared_union == {'schema': SCHEMA, 'generation_id': result.document['generation_id']},
             'exact scheduling generation binding required')
    _require(store.scheduling_generation_id == result.document['generation_id'],
             'configuration must explicitly pin scheduling generation')
    return result


def verify_original_runs(db, gen):
    from jarvis_control.sustained_research import _require, strict_json
    from jarvis_runtime.business_quarantine import digest
    for table, pin in gen.document['original_runs'].items():
        row = db.execute('SELECT data FROM "'+table.replace('"','""')+'" WHERE id=1').fetchone()
        _require(row is not None, 'original run disappeared')
        old = strict_json(row[0])
        _require(old['status'] == 'paused' and digest(old['config']) == pin['config_sha256']
                 and old.get('declaration_sha256') == pin['declaration_sha256'],
                 'original run changed or resumed; no new generation admission')


def register(store, db, config, claims):
    from jarvis_control.sustained_research import _require, _digest, _json
    gen = generation(store)
    _require(store.namespace not in gen.original_namespaces and config.run_id not in gen.old_run_ids,
             'old namespace/run cannot become a fresh generation')
    _require(config.owner_id not in {p['owner_id'] for p in gen.document['original_runs'].values()},
             'original owner identity cannot be repurposed')
    from jarvis_runtime.scheduling_budget_capacity import effective_limit
    _require(config.hard_total == effective_limit(gen.document, db) and config.reserved_external == 0
             and config.concurrency_policy == 'generation_batch', 'fresh generation requires exact authoritative local capacity and zero external reservation')
    for claim in claims:
        store.quarantine().require_allowed(asdict(claim), require_scope=True)
        _require(claim.checkpoint is None, 'fresh generation cannot adopt old task checkpoint')
    verify_original_runs(db, gen)
    gen.require_budget(db)
    entry = {'namespace': store.namespace, 'run_id': config.run_id, 'owner_id': config.owner_id,
             'declaration_sha256': _digest({'config': asdict(config), 'claims': [asdict(c) for c in claims]}),
             'binding': store.shared_union}
    db.execute('CREATE TABLE IF NOT EXISTS jarvis_research_union(namespace TEXT PRIMARY KEY,data TEXT NOT NULL)')
    rows = db.execute('SELECT namespace,data FROM jarvis_research_union').fetchall()
    _require(not rows or (len(rows) == 1 and rows[0][0] == store.namespace and json.loads(rows[0][1]) == entry),
             'one controller namespace per scheduling generation; existing registration is not replaced')
    if not rows:
        _require(not any(rid not in gen.document['original_reservations'] for rid in
                         json.loads(db.execute('SELECT data FROM main.jarvis_transport_gate WHERE provider=?', ('openai',)).fetchone()[0])['reservations']),
                 'unregistered new request exists; cannot register controller')
        db.execute('INSERT INTO jarvis_research_union VALUES(?,?)', (store.namespace, _json(entry)))


def check(store, db, state):
    from jarvis_control.sustained_research import _require, strict_json
    gen = generation(store); verify_original_runs(db, gen)
    from jarvis_runtime.scheduling_budget_capacity import effective_limit
    _require(state['config']['hard_total'] == effective_limit(gen.document, db),
             'controller capacity amendment is missing or changed')
    record = db.execute('SELECT data FROM jarvis_research_union WHERE namespace=?', (store.namespace,)).fetchone()
    _require(record is not None, 'generation controller registration missing')
    registered = strict_json(record[0])
    _require(registered['binding'] == store.shared_union and registered['run_id'] == state['config']['run_id']
             and registered['owner_id'] == state['config']['owner_id']
             and registered['declaration_sha256'] == state['declaration_sha256'], 'generation controller identity changed')
    gate = strict_json(db.execute('SELECT data FROM main.jarvis_transport_gate WHERE provider=?', ('openai',)).fetchone()[0])
    own = {r['request_id']: c['lane'] for c in state['claims'] for r in c['history'] + ([c['current']] if c.get('current') else [])}
    from jarvis_runtime.management_archive import virtual_gate
    historical_gate, _ = virtual_gate(gen.document, db, gate)
    _require(set(historical_gate['reservations']) == set(gen.document['original_reservations']) | set(own),
             'unregistered provider request appeared; keep unknown without dispatch')
    for rid, lane in own.items():
        row = gate['reservations'][rid]
        _require(row['owner'] == state['config']['owner_id'] and row['lane'] == lane,
                 'new generation request ownership mismatch')
    for claim in state['claims']:
        _require(not gen.blocked({**claim, **(claim.get('current') or {})}), 'historical identity/scope cannot dispatch')
    budget = gen.require_budget(db)
    return {**budget, 'potential': budget['total_potential_exposure'], 'current': budget['current_occupancy'],
            'external_reserved': 0, 'predecessor_reserved': 0, 'sibling_reserved': budget['current_occupancy'],
            'shared_pending': dict(gate['pending'])}
