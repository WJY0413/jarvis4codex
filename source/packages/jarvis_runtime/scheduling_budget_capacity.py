"""Audited same-generation local capacity amendment, never provider recovery.

The immutable generation and management archive retain their original bytes.
Only one dedicated SQLite record is written, under the original writer/manifest
lock order. This does not dispatch, settle UNKNOWN, change lanes or provider
controls. Missing authority remains the original 25; malformed authority fails
closed. Plans and audit history contain hashes, never copied request contents.
"""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
import json
from pathlib import Path
import re
import sqlite3

from jarvis_runtime import business_quarantine as quarantine
from jarvis_runtime import scheduling_budget_generation as budget

SCHEMA = 'jarvis-scheduling-capacity-change/v1'
RECORD_SCHEMA = 'jarvis-scheduling-capacity-record/v1'
TABLE = 'jarvis_scheduling_capacity'
CapacityError = quarantine.QuarantineError
_require = quarantine._require
_CREATE = ('CREATE TABLE jarvis_scheduling_capacity('
           'generation_id TEXT NOT NULL PRIMARY KEY,data TEXT NOT NULL)')
_BINDINGS = {'generation_file', 'generation_sha256', 'generation_id', 'database',
             'database_identity', 'quarantine_file', 'quarantine_sha256',
             'quarantine_authority', 'lane_targets'}
_PLAN_FIELDS = _BINDINGS | {'schema', 'authorization_ref', 'from_hard_total',
    'hard_total', 'revision', 'previous_record_sha256', 'source_snapshot', 'plan_sha256'}
_RECORD_FIELDS = {'schema', 'generation_id', 'hard_total', 'changes', 'record_sha256'}
_SOURCE_FIELDS = {'schema_sha256', 'tables_sha256', 'target_evidence'}
_DIGEST = re.compile(r'[0-9a-f]{64}')
_AUTHORITY = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:/-]{0,159}')


def _encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':'), allow_nan=False)


def _seal(value, key):
    value[key] = quarantine.digest({k: v for k, v in value.items() if k != key})
    return value


def _generation(path):
    path = quarantine._path(path)
    document, sha = quarantine._read(path)
    return budget._validate(document, path), sha


def _table_exists(db):
    rows = list(db.execute('SELECT type,sql FROM main.sqlite_master WHERE name=?', (TABLE,)))
    if not rows:
        return False
    _require(rows == [('table', _CREATE)], 'unexpected scheduling capacity table schema')
    columns = list(db.execute('PRAGMA main.table_xinfo(jarvis_scheduling_capacity)'))
    _require(columns == [(0, 'generation_id', 'TEXT', 1, None, 1, 0),
                         (1, 'data', 'TEXT', 1, None, 0, 0)],
             'unexpected scheduling capacity columns')
    objects = list(db.execute('SELECT type,name,sql FROM main.sqlite_master '
                              'WHERE tbl_name=? AND name!=? ORDER BY name', (TABLE, TABLE)))
    _require(objects == [('index', 'sqlite_autoindex_jarvis_scheduling_capacity_1', None)],
             'unexpected scheduling capacity index or write trigger')
    _require(not list(db.execute('PRAGMA main.foreign_key_list(jarvis_scheduling_capacity)')),
             'scheduling capacity foreign keys refused')
    return True


def _bindings(document, generation_sha):
    qpath = quarantine._path(document['quarantine_file'])
    q, qsha = quarantine._read(qpath)
    quarantine._validate(q, qpath)
    _require(budget._binding(q) == document['quarantine_authority'],
             'scheduling capacity quarantine authority changed')
    return dict(generation_file=str(Path(document['database']).parent / budget.FILENAME),
        generation_sha256=generation_sha, generation_id=document['generation_id'],
        database=document['database'], database_identity=deepcopy(document['database_identity']),
        quarantine_file=str(qpath), quarantine_sha256=qsha,
        quarantine_authority=deepcopy(document['quarantine_authority']),
        lane_targets=deepcopy(budget.LANES))


def _check_plan(plan, bindings):
    _require(isinstance(plan, dict) and set(plan) == _PLAN_FIELDS and plan['schema'] == SCHEMA,
             'exact prepared scheduling capacity plan required')
    _require(plan['plan_sha256'] == quarantine.digest(
        {k: v for k, v in plan.items() if k != 'plan_sha256'}), 'scheduling capacity plan hash mismatch')
    _require(all(plan[k] == bindings[k] for k in _BINDINGS),
             'scheduling capacity generation bytes, database or authority binding changed')
    _require(isinstance(plan['authorization_ref'], str) and _AUTHORITY.fullmatch(plan['authorization_ref']),
             'specific scheduling capacity authorization reference required')
    _require(type(plan['from_hard_total']) is int and type(plan['hard_total']) is int
             and (plan['from_hard_total'], plan['hard_total']) in ((25, 99), (99, 25)),
             'only explicit scheduling capacity 25 to 99 or 99 to 25 changes allowed')
    _require(type(plan['revision']) is int and plan['revision'] >= 1
             and (plan['previous_record_sha256'] is None or isinstance(plan['previous_record_sha256'], str)
                  and _DIGEST.fullmatch(plan['previous_record_sha256'])), 'invalid scheduling capacity audit predecessor')
    source = plan['source_snapshot']
    _require(isinstance(source, dict) and set(source) == _SOURCE_FIELDS
             and isinstance(source['schema_sha256'], str) and _DIGEST.fullmatch(source['schema_sha256'])
             and isinstance(source['tables_sha256'], dict) and bool(source['tables_sha256'])
             and all(isinstance(k, str) and k != TABLE and isinstance(v, str) and _DIGEST.fullmatch(v)
                     for k, v in source['tables_sha256'].items())
             and isinstance(source['target_evidence'], dict) and bool(source['target_evidence']),
             'exact original database and evidence CAS hashes required')
    for rid, pin in source['target_evidence'].items():
        _require(isinstance(rid, str) and bool(rid) and isinstance(pin, dict)
                 and set(pin) == {'request_sha256', 'ack_sha256', 'claim_sha256'}
                 and all(isinstance(v, str) and _DIGEST.fullmatch(v) for v in pin.values()),
                 'invalid scheduling capacity target evidence digest')


def _make_record(changes):
    latest = changes[-1]
    return _seal(dict(schema=RECORD_SCHEMA, generation_id=latest['generation_id'],
                      hard_total=latest['hard_total'], changes=deepcopy(changes)), 'record_sha256')


def _record(db, document, bindings):
    if not _table_exists(db):
        return None, None
    rows = list(db.execute('SELECT generation_id,data FROM main.jarvis_scheduling_capacity'))
    _require(len(rows) == 1 and rows[0][0] == document['generation_id'] and isinstance(rows[0][1], str),
             'exact one same-generation scheduling capacity record required')
    raw = rows[0][1]
    _require(len(raw.encode()) <= 4_194_304, 'scheduling capacity audit exceeds bounded record size')
    record = quarantine._json(raw)
    _require(isinstance(record, dict) and set(record) == _RECORD_FIELDS
             and record['schema'] == RECORD_SCHEMA and record['generation_id'] == document['generation_id']
             and type(record['hard_total']) is int and record['hard_total'] in (25, 99)
             and isinstance(record['changes'], list) and bool(record['changes']),
             'invalid scheduling capacity record')
    previous, limit = None, 25
    for revision, plan in enumerate(record['changes'], 1):
        _check_plan(plan, bindings)
        _require(plan['revision'] == revision and plan['from_hard_total'] == limit
                 and plan['previous_record_sha256'] == previous,
                 'scheduling capacity audit chain changed')
        prefix = _make_record(record['changes'][:revision])
        previous, limit = prefix['record_sha256'], prefix['hard_total']
    _require(record == prefix, 'scheduling capacity record hash or effective limit mismatch')
    return record, raw


def effective_limit(document, db):
    """Read the original database's audited ceiling; absence alone means 25."""
    _require(isinstance(document, dict), 'original scheduling generation required')
    path = Path(document['database']).parent / budget.FILENAME
    current, sha = _generation(path)
    _require(current == document, 'scheduling capacity requires current generation authority')
    budget._anchor_connection(db, document['database'], document['database_identity'])
    record, _ = _record(db, document, _bindings(document, sha))
    return record['hard_total'] if record is not None else budget.LIMIT


def _cell(value):
    if isinstance(value, bytes):
        return ['blob', value.hex()]
    return [type(value).__name__, value]


def _database_snapshot(db):
    # Exact raw row types and values, including whitespace in JSON TEXT. Exclude
    # only our dedicated table and its autoindex, never provider or run data.
    schema = list(db.execute('SELECT type,name,tbl_name,sql FROM main.sqlite_master '
                             'WHERE tbl_name!=? ORDER BY type,name', (TABLE,)))
    tables = {}
    for kind, name, _, _ in schema:
        if kind != 'table':
            continue
        quoted = '"' + name.replace('"', '""') + '"'
        rows = sorted(_encoded([_cell(v) for v in row]) for row in db.execute('SELECT * FROM main.' + quoted))
        tables[name] = quarantine.digest(rows)
    return {'schema_sha256': quarantine.digest(schema), 'tables_sha256': tables}


def _source_snapshot(document, db):
    from jarvis_runtime import management_archive as archive
    states = archive._paused(db, document)
    actual = quarantine._gate(db)
    historical, _ = archive.virtual_gate(document, db, actual)
    _require(historical['reservations'] == document['original_reservations'],
             'capacity change refuses new reservation, UNKNOWN or changed original status')
    q, _ = budget._quarantine(document['quarantine_file'])
    pins = {}
    for entry in q['entries']:
        table = 'jarvis_research_run' + ('__' + entry['namespace'] if entry['namespace'] else '')
        claims = [c for c in states[table]['claims'] if c.get('claim_id') == entry['claim_id']]
        _require(len(claims) == 1, 'capacity target claim missing or ambiguous')
        pin = {'claim_sha256': quarantine.digest(claims[0])}
        for filename, field in (('request.json', 'request_sha256'), ('ack.json', 'ack_sha256')):
            _, pin[field] = quarantine._read(Path(entry['root']) / filename)
        pins[entry['request_id']] = pin
    _require(pins == document['source_snapshot']['target_snapshot']['target_evidence'],
             'original capacity target request, ACK or claim evidence changed')
    return _database_snapshot(db) | {'target_evidence': pins}


def _prepare(path, db, *, hard_total, authorization_ref):
    document, sha = _generation(path)
    _require(document['mode'] == 'active', 'active original scheduling generation required')
    budget._anchor_connection(db, document['database'], document['database_identity'])
    bindings = _bindings(document, sha)
    record, _ = _record(db, document, bindings)
    plan = dict(schema=SCHEMA, **bindings, authorization_ref=authorization_ref,
                from_hard_total=record['hard_total'] if record else budget.LIMIT,
                hard_total=hard_total, revision=len(record['changes']) + 1 if record else 1,
                previous_record_sha256=record['record_sha256'] if record else None,
                source_snapshot=_source_snapshot(document, db))
    _seal(plan, 'plan_sha256')
    _check_plan(plan, bindings)
    return plan


def plan_capacity_change(generation_file, *, hard_total=99, authorization_ref):
    """Read-only exact CAS plan; no sidecar, table, lock or DB is created."""
    path = quarantine._path(generation_file)
    initial, _ = _generation(path)
    with quarantine.manifest_lock(initial['quarantine_file']), quarantine.manifest_lock(path):
        with quarantine._database(initial['database'], initial['database_identity']) as db:
            return _prepare(path, db, hard_total=hard_total, authorization_ref=authorization_ref)


@contextmanager
def _writer(document):
    path = quarantine._path(document['database'])
    _require(quarantine._inode(path) == document['database_identity'], 'original capacity database inode changed')
    db, commit_started = None, False
    try:
        db = sqlite3.connect(path.as_uri() + '?mode=rw', uri=True, timeout=10)
        budget._anchor_connection(db, str(path), document['database_identity'])
        db.execute('BEGIN IMMEDIATE')
        budget._anchor_connection(db, str(path), document['database_identity'])
        yield db
        budget._anchor_connection(db, str(path), document['database_identity'])
        commit_started = True
        db.commit()
    except sqlite3.Error as exc:
        raise CapacityError('capacity commit outcome uncertain; inspect authoritative DB before retry'
                            if commit_started else 'capacity transaction rolled back; no amendment applied') from exc
    finally:
        if db is not None:
            db.close()


def apply_capacity_change(plan):
    """Atomically CAS only our single audit row; no dispatch or provider writes."""
    _require(isinstance(plan, dict) and 'generation_file' in plan, 'exact prepared capacity plan required')
    path = quarantine._path(plan['generation_file'])
    initial, sha = _generation(path)
    _check_plan(plan, _bindings(initial, sha))
    with _writer(initial) as db:
        with quarantine.manifest_lock(initial['quarantine_file']), quarantine.manifest_lock(path):
            document, sha = _generation(path)
            bindings = _bindings(document, sha)
            _require(document == initial and document['mode'] == 'active', 'capacity generation CAS changed')
            _check_plan(plan, bindings)
            record, raw = _record(db, document, bindings)
            if record is not None and record['changes'][-1] == plan:
                _require(_source_snapshot(document, db) == plan['source_snapshot'],
                         'capacity source CAS changed after prior apply; inspect rather than retry')
                return _result(record, 'unchanged')
            prepared = _prepare(path, db, hard_total=plan['hard_total'],
                                authorization_ref=plan['authorization_ref'])
            _require(prepared == plan, 'scheduling capacity source CAS changed')
            changes = (record['changes'] if record else []) + [plan]
            changed_record = _make_record(changes)
            encoded = _encoded(changed_record)
            _require(len(encoded.encode()) <= 4_194_304, 'scheduling capacity audit exceeds bounded record size')
            if record is None:
                db.execute(_CREATE)
                db.execute('INSERT INTO main.jarvis_scheduling_capacity VALUES(?,?)',
                           (document['generation_id'], encoded))
            else:
                changed = db.execute('UPDATE main.jarvis_scheduling_capacity SET data=? '
                    'WHERE generation_id=? AND data=?', (encoded, document['generation_id'], raw))
                _require(changed.rowcount == 1, 'scheduling capacity record CAS changed')
            _require(_record(db, document, bindings)[0] == changed_record,
                     'scheduling capacity atomic readback mismatch')
            _require(_source_snapshot(document, db) == plan['source_snapshot']
                     and _generation(path) == (document, sha)
                     and _bindings(document, sha) == bindings,
                     'capacity evidence or original tables changed; transaction refused')
            result = _result(changed_record, 'applied')
    return result


def _result(record, status):
    return dict(status=status, generation_id=record['generation_id'], hard_total=record['hard_total'],
        lane_targets=deepcopy(budget.LANES), revision=len(record['changes']),
        record_sha256=record['record_sha256'], authorization_ref=record['changes'][-1]['authorization_ref'],
        local_capacity_only=True, provider_gate_reset=False, automatic_dispatch_restored=False,
        native_terminal_proven=False)
