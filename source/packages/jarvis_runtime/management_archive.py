"""Reversible removal of reviewed local management indexes, not native settlement.

Only the existing local SQLite gate and its dedicated audit table are written.
The immutable PRIVATE backup retains all 411 original records and the unmodified
statuses of the 27 unresolved records. No network, provider, native or service
operation is performed. A backup alone is never evidence of an applied cleanup.
"""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat

from jarvis_runtime import business_quarantine as quarantine

SCHEMA = 'jarvis-management-archive/v1'
TABLE = 'jarvis_management_archive'
ArchiveError = quarantine.QuarantineError
_require = quarantine._require
_FIELDS = {'schema', 'generation_file', 'generation_sha256', 'generation_id',
    'database', 'database_identity', 'quarantine_file', 'quarantine_sha256',
    'quarantine_authority', 'original_runs', 'original_gate_raw',
    'original_gate_sha256', 'after_gate_raw', 'after_gate_sha256',
    'archived_reservations', 'pending_removed', 'target_snapshot',
    'backup_path', 'plan_sha256'}


def _encoded(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=False,
                       separators=(',', ':'), allow_nan=False) + '\n').encode()


def _sha(raw):
    return hashlib.sha256(raw.encode() if isinstance(raw, str) else raw).hexdigest()


def _generation(path):
    # Lazy import so generation's reporting integration can import this module.
    from jarvis_runtime import scheduling_budget_generation as budget
    path = quarantine._path(path)
    document, sha = quarantine._read(path)
    return budget._validate(document, path), sha


def _raw_gate(db):
    rows = list(db.execute('SELECT data FROM main.jarvis_transport_gate WHERE provider=?', ('openai',)))
    _require(len(rows) == 1 and isinstance(rows[0][0], str), 'one original local gate required')
    return rows[0][0]


def _anchor(db, document):
    from jarvis_runtime import scheduling_budget_generation as budget
    budget._anchor_connection(db, document['database'], document['database_identity'])


def _paused(db, document):
    from jarvis_runtime import scheduling_budget_generation as budget
    states, pins, _, _ = budget._original_runs(db)
    _require(len(pins) == 5 and pins == document['original_runs'],
             'exact five original paused runs required; changed or new namespace refused')
    tables = {row[0] for row in db.execute("SELECT name FROM main.sqlite_master WHERE type='table'")}
    _require('jarvis_research_union' not in tables or not db.execute(
        'SELECT 1 FROM main.jarvis_research_union LIMIT 1').fetchone(),
        'management cleanup or restore requires no research union')
    return states


def _table_exists(db):
    found = db.execute('SELECT type FROM main.sqlite_master WHERE name=?', (TABLE,)).fetchone()
    if found is None:
        return False
    _require(found == ('table',), 'management archive audit must be an original SQLite table')
    columns = list(db.execute('PRAGMA main.table_info(jarvis_management_archive)'))
    _require([(row[1], row[2], row[3], row[5]) for row in columns] == [
        ('generation_id', 'TEXT', 1, 1), ('status', 'TEXT', 1, 0),
        ('data', 'TEXT', 1, 0), ('backup_sha256', 'TEXT', 1, 0),
        ('backup_path', 'TEXT', 1, 0)], 'unexpected management archive audit schema')
    return True


def _no_write_triggers(db):
    _require(not db.execute(
        "SELECT 1 FROM main.sqlite_master WHERE type='trigger' AND tbl_name IN (?,?) LIMIT 1",
        ('jarvis_transport_gate', TABLE)).fetchone(), 'unexpected gate or archive write trigger refused')


def _record(db, generation_id):
    if not _table_exists(db):
        return None
    rows = list(db.execute('SELECT generation_id,status,data,backup_sha256,backup_path '
                           'FROM main.jarvis_management_archive'))
    _require(len(rows) <= 1 and all(row[0] == generation_id for row in rows),
             'foreign or multiple management archive generations refused')
    return rows[0] if rows else None


def _backup_path(document):
    return Path(document['database']).parent / ('management-archive-' + document['generation_id'] + '.json')


def _check_plan(plan, generation):
    _require(isinstance(plan, dict) and set(plan) == _FIELDS and plan['schema'] == SCHEMA,
             'exact prepared management cleanup plan required')
    _require(plan['plan_sha256'] == quarantine.digest({k: v for k, v in plan.items() if k != 'plan_sha256'}),
             'management cleanup plan content hash mismatch')
    for key in ('generation_id', 'database', 'database_identity', 'quarantine_file',
                'quarantine_authority', 'original_runs', 'archived_reservations'):
        _require(plan[key] == generation[key], 'management archive generation binding changed: ' + key)
    _require(plan['generation_file'] == str(Path(generation['database']).parent / 'scheduling-budget-generation.json')
             and plan['backup_path'] == str(_backup_path(generation)), 'management archive path binding changed')
    _require(len(generation['original_runs']) == 5 and len(generation['original_reservations']) == 411,
             'exact five original runs and 411 original reservations required')
    original = quarantine._json(plan['original_gate_raw'])
    after = quarantine._json(plan['after_gate_raw'])
    _require(isinstance(original, dict) and isinstance(original.get('pending'), dict)
             and original.get('reservations') == generation['original_reservations'],
             'full original 411 reservation snapshot required')
    archived = plan['archived_reservations']
    _require(original.get('probe') not in archived,
             'original provider probe points to an archived request; refuse rather than clear provider control')
    _require(len(archived) == 27 and archived == {rid: row for rid, row in original['reservations'].items()
                                               if row['status'] != 'finished'},
             'exact original 27 unresolved statuses must remain unchanged')
    for rid, cost in original['pending'].items():
        _require(rid in archived and type(cost) is int and cost == archived[rid]['cost'],
                 'unknown or unrelated original pending exposure')
    expected = deepcopy(original)
    for rid in archived:
        expected['reservations'].pop(rid)
        expected['pending'].pop(rid, None)
    _require(after == expected and plan['pending_removed'] == original['pending'],
             'only original unresolved reservations and related pending references may be removed')
    _require(plan['original_gate_sha256'] == _sha(plan['original_gate_raw'])
             and plan['after_gate_sha256'] == _sha(plan['after_gate_raw']), 'gate raw snapshot hash mismatch')
    _require(all(isinstance(plan[key], str) and len(plan[key]) == 64
                 for key in ('generation_sha256', 'quarantine_sha256')), 'manifest byte hashes required')
    return original, after


def _prepare(path, db):
    generation, generation_sha = _generation(path)
    _require(generation['mode'] == 'active', 'active original scheduling generation required')
    _anchor(db, generation)
    _paused(db, generation)
    qpath = quarantine._path(generation['quarantine_file'])
    q, qsha = quarantine._read(qpath)
    quarantine._validate(q, qpath)
    gate, target_snapshot = quarantine._snapshot(db, q['entries'])
    _require(gate['reservations'] == generation['original_reservations'],
             'original reservation snapshot changed; new or UNKNOWN reservation refused')
    _require(target_snapshot['target_evidence'] == generation['source_snapshot']['target_snapshot']['target_evidence'],
             'original request, ACK or claim evidence changed')
    raw = _raw_gate(db)
    after = deepcopy(gate)
    archived = generation['archived_reservations']
    removed = {rid: after['pending'][rid] for rid in archived if rid in after['pending']}
    for rid in archived:
        after['reservations'].pop(rid)
        after['pending'].pop(rid, None)
    after_raw = _encoded(after).decode().rstrip('\n')
    plan = dict(schema=SCHEMA, generation_file=str(path), generation_sha256=generation_sha,
        generation_id=generation['generation_id'], database=generation['database'],
        database_identity=deepcopy(generation['database_identity']), quarantine_file=str(qpath),
        quarantine_sha256=qsha, quarantine_authority=deepcopy(generation['quarantine_authority']),
        original_runs=deepcopy(generation['original_runs']), original_gate_raw=raw,
        original_gate_sha256=_sha(raw), after_gate_raw=after_raw, after_gate_sha256=_sha(after_raw),
        archived_reservations=deepcopy(archived), pending_removed=removed,
        target_snapshot=target_snapshot, backup_path=str(_backup_path(generation)))
    plan['plan_sha256'] = quarantine.digest(plan)
    _check_plan(plan, generation)
    return plan


def plan_cleanup(generation_file):
    """Read-only exact CAS plan; creates no directory, lock, DB or backup."""
    path = quarantine._path(generation_file)
    initial, _ = _generation(path)
    with quarantine.manifest_lock(initial['quarantine_file']), quarantine.manifest_lock(path):
        with quarantine._database(initial['database'], initial['database_identity']) as db:
            _require(_record(db, initial['generation_id']) is None,
                     'management cleanup already has an authoritative audit; inspect rather than re-plan')
            _require(not os.path.lexists(_backup_path(initial)),
                     'backup exists without authoritative DB archive; not applied, inspect before retry')
            return _prepare(path, db)


@contextmanager
def _writer(document):
    database = quarantine._path(document['database'])
    _require(quarantine._inode(database) == document['database_identity'], 'original database inode changed')
    db = None
    commit_started = False
    try:
        db = sqlite3.connect(database.as_uri() + '?mode=rw', uri=True, timeout=10)
        _anchor(db, document)
        db.execute('BEGIN IMMEDIATE')
        _anchor(db, document)
        yield db
        _anchor(db, document)
        commit_started = True
        db.commit()
    except sqlite3.Error as exc:
        raise ArchiveError('management archive commit outcome uncertain; inspect authoritative DB before retry'
                           if commit_started else 'management archive transaction rolled back; any backup is not proof of apply') from exc
    finally:
        if db is not None:
            db.close()


def _write_backup(plan):
    """Create once, PRIVATE 0600, durable before touching the SQLite transaction."""
    path = quarantine._path(plan['backup_path'], exists=False)
    _require(not os.path.lexists(path), 'existing or partial management backup is never overwritten')
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(_encoded(plan))
        stream.flush()
        os.fsync(stream.fileno())
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return _sha(_encoded(plan))


def _verified_record(db, generation):
    record = _record(db, generation['generation_id'])
    path = _backup_path(generation)
    if record is None:
        _require(not os.path.lexists(path),
                 'backup exists without authoritative DB archive; not applied, inspect before retry')
        return None
    _, status, raw, backup_sha, backup_path = record
    _require(status in ('applied', 'restored'), 'unknown management archive audit status')
    plan = quarantine._json(raw)
    original, after = _check_plan(plan, generation)
    _require(backup_path == str(path), 'database archive backup path changed')
    backup, observed_sha = quarantine._read(path)
    _require(backup == plan and observed_sha == backup_sha == _sha(_encoded(plan)),
             'PRIVATE backup hash or authoritative archive mismatch')
    mode = path.stat().st_mode
    _require(stat.S_IMODE(mode) & 0o077 == 0, 'management backup must remain PRIVATE')
    return plan, status, original, after, backup_sha


def _audit(plan, status, gate, backup_sha):
    active = sum(row['status'] != 'finished' for row in gate['reservations'].values())
    historical = sum(rid in gate['reservations'] and gate['reservations'][rid]['status'] != 'finished'
                     for rid in plan['archived_reservations'])
    return {'status': status, 'generation_id': plan['generation_id'],
        'state': status, 'archive_path': plan['backup_path'], 'backup_sha256': backup_sha,
        'archive_sha256': backup_sha, 'archived_request_ids': sorted(plan['archived_reservations']),
        'effective_original_request_ids': sorted(set(quarantine._json(plan['original_gate_raw'])['reservations'])
                                               - set(plan['archived_reservations'])) if status == 'applied'
                                          else sorted(quarantine._json(plan['original_gate_raw'])['reservations']),
        'current_management_active': active - historical,
        'historical_management_active': historical,
        'archived_native_unknown': 27 if status == 'applied' else 0,
        'local_pending_count': len(gate['pending']), 'original_reservation_count': 411,
        'local_management_only': True, 'native_terminal_proven': False,
        'provider_gate_reset': False, 'automatic_dispatch_restored': False}


def _validate_live(gate, plan, status, original, after):
    if status == 'restored':
        _require(all(gate['reservations'].get(rid) == row for rid, row in original['reservations'].items()),
                 'restored original reservation snapshot changed')
    else:
        _require(not set(plan['archived_reservations']) & (set(gate['reservations']) | set(gate['pending'])),
                 'archived original reservation or pending reference reappeared')
        _require(all(gate['reservations'].get(rid) == row for rid, row in after['reservations'].items()),
                 'retained original reservation snapshot changed')


def management_snapshot(document, db_connection, gate=None):
    """Verified audit for generation integration; None only if never archived.

    Later new reservations/provider activity remain visible and counted. Only
    the exact reviewed historical rows are supplied by the archive; no new
    UNKNOWN can be disguised as archived. Callers must not treat the audit as
    native terminal evidence or provider recovery permission.
    """
    path = Path(document['database']).parent / 'scheduling-budget-generation.json'
    current, _ = _generation(path)
    _require(current == document, 'management snapshot requires current generation authority')
    _anchor(db_connection, document)
    actual = quarantine._gate(db_connection)
    _require(gate is None or gate == actual, 'caller gate differs from original authoritative local gate')
    verified = _verified_record(db_connection, document)
    if verified is None:
        from jarvis_runtime import scheduling_budget_generation as budget
        budget._pin_reservations(actual['reservations'], document['original_reservations'])
        return None
    plan, status, original, after, backup_sha = verified
    _validate_live(actual, plan, status, original, after)
    return _audit(plan, status, actual, backup_sha)


def virtual_gate(document, db_connection, gate):
    """Return (historically complete gate, audit) for original identity checks.

    Virtual historical rows must not be counted as active management records.
    Current local pending and all provider controls are copied without changes.
    """
    audit = management_snapshot(document, db_connection, gate)
    result = deepcopy(gate)
    if audit is not None and audit['status'] == 'applied':
        result['reservations'].update(deepcopy(document['archived_reservations']))
    return result, audit



def require_request_not_archived(db_connection, request_id):
    """Read-only lower-gate tombstone seam, including after an explicit restore.

    Only the connected database's fixed generation sidecar and fixed backup
    path are inspected. Missing/corrupt archive authority fails closed. This
    protects patched local writers, not an old binary or a provider queue.
    """
    _require(isinstance(db_connection, sqlite3.Connection)
             and isinstance(request_id, str) and bool(request_id),
             'original SQLite connection and nonempty request identity required')
    main = next((row[2] for row in db_connection.execute('PRAGMA database_list') if row[1] == 'main'), None)
    _require(isinstance(main, str) and bool(main), 'original file-backed gate database required')
    database = quarantine._path(main)
    path = database.parent / 'scheduling-budget-generation.json'
    has_table = _table_exists(db_connection)
    if not os.path.lexists(path):
        _require(not has_table, 'management archive exists without original generation authority')
        return True
    generation, _ = _generation(path)
    _anchor(db_connection, generation)
    gate = quarantine._gate(db_connection)
    verified = _verified_record(db_connection, generation)
    if verified is None:
        from jarvis_runtime import scheduling_budget_generation as budget
        budget._pin_reservations(gate['reservations'], generation['original_reservations'])
        return True
    plan, status, original, after, _ = verified
    _validate_live(gate, plan, status, original, after)
    _require(request_id not in generation['denied_request_ids'],
             'historical archived request is permanently denied; never redispatch')
    return True


def apply_cleanup(plan):
    """Exact CAS, immutable backup, and one SQLite transaction for audit + gate."""
    _require(isinstance(plan, dict) and 'generation_file' in plan, 'exact cleanup plan required')
    path = quarantine._path(plan['generation_file'])
    generation, _ = _generation(path)
    _check_plan(plan, generation)
    with _writer(generation) as db:
        with quarantine.manifest_lock(generation['quarantine_file']), quarantine.manifest_lock(path):
            current, sha = _generation(path)
            _require(current == generation and sha == plan['generation_sha256'],
                     'management cleanup generation CAS changed')
            verified = _verified_record(db, current)
            if verified is not None:
                stored, status, original, after, backup_sha = verified
                _require(stored == plan and status == 'applied', 'different or restored cleanup cannot be reapplied')
                gate = quarantine._gate(db)
                _validate_live(gate, plan, status, original, after)
                return _audit(plan, 'applied', gate, backup_sha) | {'status': 'unchanged'}
            _no_write_triggers(db)
            _require(_prepare(path, db) == plan, 'management cleanup source CAS changed')
            backup_sha = _write_backup(plan)
            # Recheck external evidence after durable backup and before any DB write.
            _require(_prepare(path, db) == plan, 'management cleanup evidence changed; backup retained, DB not applied')
            db.execute('CREATE TABLE IF NOT EXISTS main.jarvis_management_archive('
                'generation_id TEXT NOT NULL PRIMARY KEY,status TEXT NOT NULL,'
                'data TEXT NOT NULL,backup_sha256 TEXT NOT NULL,backup_path TEXT NOT NULL)')
            _table_exists(db)
            db.execute('INSERT INTO main.jarvis_management_archive VALUES(?,?,?,?,?)',
                       (plan['generation_id'], 'applied', _encoded(plan).decode(), backup_sha, plan['backup_path']))
            changed = db.execute('UPDATE main.jarvis_transport_gate SET data=? WHERE provider=? AND data=?',
                                 (plan['after_gate_raw'], 'openai', plan['original_gate_raw']))
            _require(changed.rowcount == 1, 'management cleanup exact raw gate CAS changed')
            _require(_raw_gate(db) == plan['after_gate_raw'], 'management cleanup gate readback mismatch')
            verified = _verified_record(db, current)
            _require(verified is not None and verified[0] == plan and verified[1] == 'applied',
                     'management cleanup atomic audit readback mismatch')
            result = _audit(plan, 'applied', quarantine._gate(db), backup_sha)
    return result


def restore(generation_file, *, expected_generation_id):
    """Restore exact original raw gate only while original paused evidence holds.

    No audit, backup, generation deny, run, claim or original file is deleted.
    Any new reservation (even finished), namespace, union, provider-control
    change, or changed original evidence blocks restoration instead of losing it.
    """
    path = quarantine._path(generation_file)
    initial, _ = _generation(path)
    _require(initial['generation_id'] == expected_generation_id, 'expected management generation identity changed')
    with _writer(initial) as db:
        with quarantine.manifest_lock(initial['quarantine_file']), quarantine.manifest_lock(path):
            generation, _ = _generation(path)
            _require(generation == initial, 'management restore generation CAS changed')
            _paused(db, generation)
            _no_write_triggers(db)
            verified = _verified_record(db, generation)
            _require(verified is not None, 'no authoritative applied management archive to restore')
            plan, status, original, _, backup_sha = verified
            if status == 'restored':
                _require(_raw_gate(db) == plan['original_gate_raw'], 'restored gate changed; inspect before retry')
                return _audit(plan, 'restored', original, backup_sha) | {'status': 'unchanged'}
            _require(_raw_gate(db) == plan['after_gate_raw'],
                     'management restore requires exact after gate; new UNKNOWN or provider changes refused')
            # Validate original request/ACK/claim evidence using a read-only virtual
            # gate is unnecessary here: _paused pins every original run, while the
            # independent 51 files were never modified and are checked explicitly.
            q, qsha = quarantine._read(generation['quarantine_file'])
            _require(qsha == plan['quarantine_sha256'], 'management restore quarantine bytes changed')
            for entry in q['entries']:
                pin = plan['target_snapshot']['target_evidence'][entry['request_id']]
                for filename, field in (('request.json', 'request_sha256'), ('ack.json', 'ack_sha256')):
                    _, sha = quarantine._read(Path(entry['root']) / filename)
                    _require(sha == pin[field], 'management restore original file evidence changed')
            changed = db.execute('UPDATE main.jarvis_transport_gate SET data=? WHERE provider=? AND data=?',
                                 (plan['original_gate_raw'], 'openai', plan['after_gate_raw']))
            _require(changed.rowcount == 1, 'management restore gate CAS changed')
            db.execute("UPDATE main.jarvis_management_archive SET status='restored' WHERE generation_id=? AND status='applied'",
                       (expected_generation_id,))
            _require(_raw_gate(db) == plan['original_gate_raw'] and _verified_record(db, generation)[1] == 'restored',
                     'management restore atomic readback mismatch')
            result = _audit(plan, 'restored', original, backup_sha)
    return result
