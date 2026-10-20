"""Independent scheduling accounting reset, never native settlement or provider reset.

No original DB data is changed here. Twenty-seven unresolved reservations retain
all authoritative identities and statuses, but are excluded from one immutable
scheduling generation. Every old request and every original declared scope stays
denied. Provider pending starts, tokens, cooldown and native evidence are untouched.
"""
from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager, nullcontext
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile

from jarvis_runtime import business_quarantine as quarantine

SCHEMA = 'jarvis-scheduling-budget-generation/v1'
FILENAME = 'scheduling-budget-generation.json'
LIMIT = 25
LANES = {'base': 20, 'task': 5}
GenerationError = quarantine.QuarantineError
_require = quarantine._require
_DIGEST = re.compile(r'[0-9a-f]{64}')
_BINDING_KEYS = ('database', 'database_identity', 'target_set_sha256',
                 'baseline_reservations_sha256')
_FIELDS = {'schema', 'mode', 'generation_id', 'database', 'database_identity',
           'quarantine_file', 'quarantine_authority', 'archived_reservations',
           'original_reservations', 'denied_request_ids', 'denied_identities', 'excluded_scopes',
           'original_runs', 'hard_total', 'lane_targets', 'source_snapshot',
           'manifest_sha256'}


def _binding(document):
    return {key: deepcopy(document[key]) for key in _BINDING_KEYS}


def _immutable(document):
    return {key: value for key, value in document.items()
            if key not in {'mode', 'generation_id', 'manifest_sha256'}}


def _seal(document):
    document['generation_id'] = 'budget-' + quarantine.digest(_immutable(document))
    document['manifest_sha256'] = quarantine.digest(
        {key: value for key, value in document.items() if key != 'manifest_sha256'})
    return document


def _quarantine(path):
    path = quarantine._path(path)
    document, sha = quarantine._read(path)
    return quarantine._validate(document, path), sha


def _scope(value):
    _require(isinstance(value, Mapping) and value.get('country') == 'Netherlands'
             and isinstance(value.get('jurisdiction_code'), str)
             and re.fullmatch(r'GM[0-9]{4}', value['jurisdiction_code']),
             'all original scopes require canonical Netherlands/GM-four-digit identity')
    return ('Netherlands', value['jurisdiction_code'])


def _walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def _original_runs(db):
    tables = sorted(row[0] for row in db.execute(
        "SELECT name FROM main.sqlite_master WHERE type='table'")
        if re.fullmatch(r'jarvis_research_run(?:__[a-z][a-z0-9_]{0,39})?', row[0]))
    _require(tables, 'authoritative research runs missing')
    states, pins, scopes = {}, {}, set()
    identities = {key: set() for key in quarantine.IDENTIFIERS}
    for table in tables:
        rows = list(db.execute(f'SELECT id,data FROM main."{table}"'))
        _require(len(rows) == 1 and rows[0][0] == 1, 'one original run per namespace required')
        state = quarantine._json(rows[0][1])
        _require(isinstance(state, dict) and state.get('status') == 'paused'
                 and isinstance(state.get('claims'), list)
                 and isinstance(state.get('config'), dict)
                 and isinstance(state['config'].get('owner_id'), str)
                 and bool(state['config']['owner_id']), 'all original research runs must be paused and owner-bound')
        declaration = state.get('declaration')
        if declaration is not None:
            _require(isinstance(declaration, dict) and isinstance(declaration.get('claims'), list)
                     and declaration.get('config') == state['config']
                     and quarantine.digest(declaration) == state.get('declaration_sha256'),
                     'original run declaration integrity changed')
            for claim in declaration['claims']:
                scopes.add(_scope(claim))
        for claim in state['claims']:
            scopes.add(_scope(claim))
        # Includes declared/queued scopes and inherited exclusions, not only
        # the seventeen current quarantine claims or the unresolved claims.
        for value in _walk(state):
            if 'country' in value and 'jurisdiction_code' in value:
                scopes.add(_scope(value))
            for key in quarantine.IDENTIFIERS:
                identifier = value.get(key)
                if identifier is not None:
                    _require(isinstance(identifier, str) and bool(identifier), 'invalid original ' + key)
                    identities[key].add(identifier)
            for identifier in value.get('excluded_native_thread_ids', []):
                _require(isinstance(identifier, str) and bool(identifier), 'invalid excluded original thread identity')
                identities['thread_id'].add(identifier)
        namespace = table.removeprefix('jarvis_research_run__') if '__' in table else None
        pins[table] = {'namespace': namespace, 'owner_id': state['config']['owner_id'],
                      'run_id': state['config'].get('run_id'),
                      'declaration_sha256': state.get('declaration_sha256'),
                      'config_sha256': quarantine.digest(state['config']),
                      'state_sha256': quarantine.digest(state)}
        states[table] = state
    _require(scopes, 'original jurisdiction scope is empty')
    return states, pins, scopes, identities


def _pin_reservations(reservations, baseline):
    _require(set(baseline) <= set(reservations), 'original reservation disappeared; deny remains in force')
    for rid, pin in baseline.items():
        actual = reservations[rid]
        _require(all(actual.get(key) == pin.get(key) for key in quarantine.RESERVATION_KEYS),
                 'original reservation identity changed')
        _require(pin['status'] != 'finished' or actual['status'] == 'finished',
                 'finished original reservation reopened')


def _prepare(quarantine_file, db_connection=None):
    q, qsha = _quarantine(quarantine_file)
    database = quarantine._path(q['database'])
    if db_connection is not None:
        _anchor_connection(db_connection, str(database), q['database_identity'])
    context = quarantine._database(database, q['database_identity']) if db_connection is None else nullcontext(db_connection)
    with context as db:
        gate, target_snapshot = quarantine._snapshot(db, q['entries'])
        _pin_reservations(gate['reservations'], q['baseline_reservations'])
        archived = {rid: deepcopy(row) for rid, row in gate['reservations'].items()
                    if row['status'] != 'finished'}
        _require(len(archived) == 27 and set(archived) == set(q['baseline_reservations']),
                 'exact original twenty-seven unresolved reservation identities required')
        states, runs, scopes, identities = _original_runs(db)
        identities['request_id'].update(gate['reservations'])
        raw_gate = db.execute('SELECT data FROM main.jarvis_transport_gate WHERE provider=?',
                              ('openai',)).fetchone()[0]
    document = {'schema': SCHEMA, 'mode': 'active', 'database': str(database),
                'database_identity': deepcopy(q['database_identity']),
                'quarantine_file': str(quarantine_file), 'quarantine_authority': _binding(q),
                'archived_reservations': archived,
                'original_reservations': deepcopy(gate['reservations']),
                'denied_request_ids': sorted(identities['request_id']),
                'denied_identities': {key: sorted(values) for key, values in identities.items()},
                'excluded_scopes': [dict(country=country, jurisdiction_code=code)
                                    for country, code in sorted(scopes)],
                'original_runs': runs, 'hard_total': LIMIT, 'lane_targets': deepcopy(LANES),
                'source_snapshot': {'quarantine_sha256': qsha,
                                    'runs_sha256': quarantine.digest(states),
                                    'gate_raw_sha256': hashlib.sha256(raw_gate.encode()).hexdigest(),
                                    'target_snapshot': target_snapshot}}
    return _seal(document)


def _validate(document, path):
    _require(isinstance(document, dict) and set(document) == _FIELDS
             and document.get('schema') == SCHEMA
             and document.get('mode') in {'active', 'deactivated'}, 'unsupported scheduling generation')
    _require(document.get('manifest_sha256') == quarantine.digest(
        {key: value for key, value in document.items() if key != 'manifest_sha256'}),
        'scheduling generation content hash mismatch')
    _require(document.get('generation_id') == 'budget-' + quarantine.digest(_immutable(document)),
             'immutable scheduling generation identity changed')
    database = quarantine._path(document['database'])
    _require(path == database.parent / FILENAME and quarantine._inode(database) == document['database_identity'],
             'scheduling generation must retain original database path and inode')
    qpath = quarantine._path(document['quarantine_file'])
    _require(qpath == database.parent / quarantine.FILENAME, 'original quarantine path changed')
    q, _ = _quarantine(qpath)
    _require(document['quarantine_authority'] == _binding(q), 'original quarantine authority changed')
    _require(document['hard_total'] == LIMIT and document['lane_targets'] == LANES,
             'current scheduling limits must remain 25 / 20 base / 5 task')
    original, archived = document['original_reservations'], document['archived_reservations']
    _require(isinstance(original, dict) and isinstance(archived, dict) and len(archived) == 27
             and set(archived) == set(q['baseline_reservations']), 'exact archived twenty-seven required')
    for rid, row in original.items():
        _require(isinstance(rid, str) and bool(rid), 'invalid original request identity')
        quarantine._reservation(row)
    _require(archived == {rid: row for rid, row in original.items() if row['status'] != 'finished'},
             'archive must contain all and only original unresolved reservations')
    _pin_reservations(original, q['baseline_reservations'])
    ids = document['denied_request_ids']
    _require(isinstance(ids, list) and all(isinstance(rid, str) and rid for rid in ids)
             and ids == sorted(set(ids)) and set(original) <= set(ids), 'complete original request denies required')
    identities = document['denied_identities']
    _require(isinstance(identities, dict) and set(identities) == set(quarantine.IDENTIFIERS)
             and identities['request_id'] == ids, 'all original identity deny sets required')
    for key, values in identities.items():
        _require(isinstance(values, list) and all(isinstance(value, str) and value for value in values)
                 and values == sorted(set(values)) and {entry[key] for entry in q['entries']} <= set(values),
                 'complete original identity deny set required: ' + key)
    scopes = document['excluded_scopes']
    _require(isinstance(scopes, list) and bool(scopes), 'all original jurisdiction denies required')
    scope_keys = [_scope(scope) for scope in scopes]
    _require(scope_keys == sorted(set(scope_keys))
             and all(set(scope) == {'country', 'jurisdiction_code'} for scope in scopes)
             and {_scope(e) for e in q['entries']} <= set(scope_keys), 'original scope deny set changed')
    runs = document['original_runs']
    _require(isinstance(runs, dict) and bool(runs), 'original namespace pins required')
    for table, run in runs.items():
        _require(isinstance(table, str) and re.fullmatch(r'jarvis_research_run(?:__[a-z][a-z0-9_]{0,39})?', table)
                 and isinstance(run, dict) and set(run) == {'namespace', 'owner_id', 'run_id',
                     'declaration_sha256', 'config_sha256', 'state_sha256'}
                 and run['namespace'] == (table.split('__', 1)[1] if '__' in table else None)
                 and isinstance(run['owner_id'], str) and bool(run['owner_id'])
                 and (run['run_id'] is None or isinstance(run['run_id'], str) and bool(run['run_id']))
                 and all(isinstance(run[key], str) and _DIGEST.fullmatch(run[key])
                         for key in ('config_sha256', 'state_sha256')), 'invalid original namespace binding')
    source = document['source_snapshot']
    _require(isinstance(source, dict) and set(source) == {'quarantine_sha256', 'runs_sha256',
             'gate_raw_sha256', 'target_snapshot'} and isinstance(source['target_snapshot'], dict)
             and all(isinstance(source[key], str) and _DIGEST.fullmatch(source[key])
                     for key in ('quarantine_sha256', 'runs_sha256', 'gate_raw_sha256')),
             'original reset CAS snapshot required')
    return document


def plan_reset(quarantine_file):
    """Read-only exact snapshot. Does not create a lock, directory, DB or sidecar."""
    path = quarantine._path(quarantine_file)
    with quarantine.manifest_lock(path):
        document = _prepare(path)
        _validate(document, path.parent / FILENAME)
        return document


def _write(path, document, *, replace=False):
    _require(not list(path.parent.glob('.scheduling-budget-generation-*.tmp')),
             'uncertain prior generation publication requires inspection')
    fd, name = tempfile.mkstemp(prefix='.scheduling-budget-generation-', suffix='.tmp', dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump(document, stream, ensure_ascii=False, sort_keys=True, allow_nan=False)
            stream.write('\n'); stream.flush(); os.fsync(stream.fileno())
        if replace:
            os.replace(temporary, path)
        else:
            os.link(temporary, path)  # Never replace another/partial/uncertain generation.
            temporary.unlink()
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        if temporary.exists():
            temporary.unlink()


def _anchor_connection(db, database, identity):
    _require(isinstance(db, sqlite3.Connection), 'anchored SQLite connection required')
    main = next((row[2] for row in db.execute('PRAGMA database_list') if row[1] == 'main'), None)
    _require(main == database and quarantine._inode(main) == identity,
             'fresh or foreign generation budget database rejected')


@contextmanager
def _writer_barrier(database, identity):
    """Serialize with existing reservation writers without changing any DB data.

    Lock order is always SQLite writer reservation, quarantine, generation. The
    original path must already exist; query_only is enabled immediately after
    taking BEGIN IMMEDIATE. No schema, journal mode, or business row is changed.
    """
    path = quarantine._path(database)
    _require(quarantine._inode(path) == identity, 'original database inode changed')
    db = None
    yielded = False
    try:
        db = sqlite3.connect(path.as_uri() + '?mode=rw', uri=True, timeout=10)
        _anchor_connection(db, str(path), identity)
        db.execute('BEGIN IMMEDIATE')
        db.execute('PRAGMA query_only=ON')
        _anchor_connection(db, str(path), identity)
        yielded = True
        yield db
        _anchor_connection(db, str(path), identity)
        db.commit()
    except sqlite3.Error as exc:
        reason = ('database transaction failed after operation began; publication outcome uncertain, inspect sidecar before retry'
                  if yielded else 'original database writer barrier unavailable; no reset or reversal performed')
        raise GenerationError(reason) from exc
    finally:
        if db is not None:
            db.close()


def apply_reset(document):
    """CAS paused evidence under the existing writer order; publish only sidecar."""
    _require(isinstance(document, dict), 'exact prepared reset plan required')
    database = quarantine._path(document.get('database'))
    path = database.parent / FILENAME
    _validate(document, path)
    _require(document['mode'] == 'active', 'reset requires active prepared generation')
    qpath = quarantine._path(document['quarantine_file'])
    with _writer_barrier(database, document['database_identity']) as db:
        with quarantine.manifest_lock(qpath), quarantine.manifest_lock(path, exclusive=True):
            if os.path.lexists(path):
                existing = _validate(quarantine._read(path)[0], path)
                _require(existing == document, 'different or partial scheduling generation already exists')
                _pin_reservations(quarantine._gate(db)['reservations'], document['original_reservations'])
                return {'status': 'unchanged', 'path': str(path), 'generation_id': document['generation_id']}
            _require(_prepare(qpath, db) == document, 'original paused reset evidence CAS changed')
            _write(path, document)
            _require(quarantine._read(path)[0] == document,
                     'generation publication differs from exact plan; inspect uncertain outcome')
            try:
                unchanged = _prepare(qpath, db) == document
            except (GenerationError, OSError) as exc:
                raise GenerationError('evidence changed during generation publication; file retained, inspect uncertain outcome') from exc
            _require(unchanged,
                     'evidence changed during generation publication; file retained, inspect uncertain outcome')
            return {'status': 'applied', 'path': str(path), 'generation_id': document['generation_id']}


class Generation:
    """Read-only budget projection anchored to the same original main database."""
    def __init__(self, path):
        self.path = quarantine._path(path)
        self.document = _validate(quarantine._read(self.path)[0], self.path)
        self._generation_id = self.document['generation_id']

    @property
    def data(self):
        return deepcopy(self.document)

    @property
    def active(self):
        return self.document['mode'] == 'active'

    @property
    def old_run_ids(self):
        return frozenset(run['run_id'] for run in self.document['original_runs'].values() if run['run_id'])

    @property
    def original_namespaces(self):
        return frozenset(run['namespace'] for run in self.document['original_runs'].values())

    def _refresh(self):
        current = _validate(quarantine._read(self.path)[0], self.path)
        _require(current['generation_id'] == self._generation_id, 'scheduling generation authority changed')
        _require(self.document['mode'] != 'deactivated' or current['mode'] == 'deactivated',
                 'deactivated generation cannot be reactivated')
        self.document = current

    def _anchor(self, db):
        _anchor_connection(db, self.document['database'], self.document['database_identity'])

    def _evaluate(self, gate, request_id, prospective, lane, *, db_connection, enforce=True):
        d = self.document
        from jarvis_runtime.management_archive import virtual_gate
        historical_gate, management_archive = virtual_gate(d, db_connection, gate)
        reservations = gate['reservations']
        _pin_reservations(historical_gate['reservations'], d['original_reservations'])
        cleaned = management_archive is not None and management_archive.get('state') == 'applied'
        archived_unknown = management_archive['archived_native_unknown'] if cleaned else 0
        unresolved = {rid: row for rid, row in reservations.items() if row['status'] != 'finished'}
        archive = set(d['archived_reservations'])
        current = {rid: row for rid, row in unresolved.items() if rid not in archive}
        current_lanes = {name: sum(row['lane'] == name for row in current.values()) for name in LANES}
        historical = len(archive & set(unresolved))
        q, _ = _quarantine(d['quarantine_file'])
        external = set(q['external_request_ids'])
        legacy_lanes = {name: sum(row['lane'] == name for rid, row in unresolved.items() if rid not in external)
                        for name in LANES}
        lanes = current_lanes if self.active else legacy_lanes
        occupancy = len(current) if self.active else len(unresolved)
        from jarvis_runtime.scheduling_budget_capacity import effective_limit
        hard_total = effective_limit(d, db_connection) if self.active else q['hard_total']
        _require(type(prospective) is bool and (lane is None or lane in LANES), 'invalid admission budget request')
        increment = int(prospective)
        if request_id is not None:
            _require(isinstance(request_id, str) and request_id not in d['denied_request_ids'],
                     'historical request is abandoned and must never be replayed')
            _require(request_id in unresolved, 'native dispatch lacks existing unresolved reservation')
            _require(lane is None or unresolved[request_id]['lane'] == lane, 'reservation lane changed')
            _require(not prospective, 'existing reservation must not be counted prospectively')
        _require(not prospective or lane in LANES, 'prospective admission requires lane')
        if enforce:
            _require(occupancy + increment <= hard_total, 'current scheduling potential exposure exceeds limit')
            _require(all(count + (increment if name == lane else 0) <= LANES[name]
                         for name, count in lanes.items()), 'current scheduling lane potential exposure exceeded')
        return {'generation_id': d['generation_id'], 'mode': d['mode'],
                'current_occupancy': len(current), 'historical_unknown': historical,
                'historical_management_active': historical, 'current_management_active': len(current),
                'management_cleanup_applied': cleaned, 'management_archive': management_archive,
                'archived_native_unknown': archived_unknown,
                'native_potential_exposure': len(unresolved) + archived_unknown,
                'potential_exposure_scope': 'managed_active_only',
                'total_potential_exposure': len(unresolved), 'potential_exposure': len(unresolved),
                'current_lanes': current_lanes, 'lanes': lanes, 'hard_total': hard_total,
                'available': max(0, hard_total - occupancy), 'archived_reservations': len(archive),
                'quarantined_potential_exposure': sum(e['request_id'] in unresolved for e in q['entries']),
                'external_potential_exposure': len(external & set(unresolved)),
                'historical_management_status': 'archived_not_managed' if cleaned else 'abandoned_unknown',
                'abandonment_proves_terminal': False, 'provider_gate_reset': False}

    def budget(self, gate, *, db_connection=None, database_path=None,
               request_id=None, prospective=False, lane=None):
        """Accept a gate only after comparing it with its anchored DB snapshot."""
        _require((db_connection is None) != (database_path is None),
                 'exactly one original database connection or path required')
        with quarantine.manifest_lock(self.path):
            self._refresh()
            if db_connection is not None:
                self._anchor(db_connection)
                actual = quarantine._gate(db_connection)
                _require(gate == actual, 'caller gate differs from anchored authoritative gate')
                return self._evaluate(actual, request_id, prospective, lane, db_connection=db_connection)
            path = quarantine._path(database_path)
            _require(str(path) == self.document['database'], 'fresh or foreign generation budget database rejected')
            with quarantine._database(path, self.document['database_identity']) as db:
                actual = quarantine._gate(db)
                _require(gate == actual, 'caller gate differs from anchored authoritative gate')
                return self._evaluate(actual, request_id, prospective, lane, db_connection=db)

    def require_budget(self, db_connection=None, request_id=None, prospective=False, lane=None):
        with quarantine.manifest_lock(self.path):
            self._refresh()
            if db_connection is not None:
                self._anchor(db_connection)
                return self._evaluate(quarantine._gate(db_connection), request_id, prospective, lane, db_connection=db_connection)
            with quarantine._database(self.document['database'], self.document['database_identity']) as db:
                return self._evaluate(quarantine._gate(db), request_id, prospective, lane, db_connection=db)

    def blocked(self, identity):
        _require(isinstance(identity, Mapping), 'structured identity required')
        with quarantine.manifest_lock(self.path):
            self._refresh()
            values = [identity]
            if isinstance(identity.get('current'), Mapping):
                values.append(identity['current'])
            denied = {key: set(values) for key, values in self.document['denied_identities'].items()}
            scopes = {quarantine._scope(scope) for scope in self.document['excluded_scopes']}
            for value in values:
                _require(all(value.get(key) is None or isinstance(value[key], str)
                             for key in quarantine.IDENTIFIERS), 'invalid structured historical identity')
                if any(value.get(key) in ids for key, ids in denied.items()) or quarantine._scope(value) in scopes:
                    return True
            # Preserve quarantine's non-request identities as well.
            q, _ = _quarantine(self.document['quarantine_file'])
            return any(value.get(key) == entry[key] for value in values
                       for entry in q['entries'] for key in quarantine.IDENTIFIERS)

    def require_allowed(self, identity, require_scope=False):
        _require(not self.blocked(identity), 'historical generation identity or scope is abandoned; dispatch forbidden')
        if require_scope:
            _scope(identity)
        return True

    def snapshot(self, db_connection=None):
        """Read-only reporting remains usable even if observed capacity is exceeded."""
        with quarantine.manifest_lock(self.path):
            self._refresh()
            if db_connection is not None:
                self._anchor(db_connection)
                return self._evaluate(quarantine._gate(db_connection), None, False, None, db_connection=db_connection, enforce=False)
            with quarantine._database(self.document['database'], self.document['database_identity']) as db:
                return self._evaluate(quarantine._gate(db), None, False, None, db_connection=db, enforce=False)

    def view(self, claims=None):
        result = self.snapshot()
        if claims is not None:
            archived = [c for c in claims if self.blocked(c)]
            result['historical_abandoned_unknown'] = [] if result['management_cleanup_applied'] else deepcopy(archived)
            result['archived_record_count'] = len(archived) if result['management_cleanup_applied'] else 0
            result['active'] = [deepcopy(c) for c in claims if not self.blocked(c)]
        return result


def generation_for(quarantine_policy):
    """Stable optional lookup. No generation file means unchanged legacy behavior."""
    _require(isinstance(quarantine_policy, quarantine.Policy), 'original quarantine policy required')
    qpath = quarantine._path(quarantine_policy.path)
    q, _ = _quarantine(qpath)
    _require(_binding(q) == quarantine_policy._binding, 'original quarantine authority changed')
    path = qpath.parent / FILENAME
    expected = getattr(quarantine_policy, 'expected_generation_id', None)
    observed = getattr(quarantine_policy, '_observed_generation_id', None)
    _require(expected is None or isinstance(expected, str) and bool(expected), 'invalid expected generation identity')
    if not os.path.lexists(path):
        _require(expected is None and observed is None, 'required scheduling generation disappeared; downgrade forbidden')
        _require(not list(path.parent.glob('.scheduling-budget-generation-*.tmp')),
                 'uncertain prior generation publication requires inspection')
        return None
    generation = Generation(path)
    _require(generation.document['quarantine_file'] == str(qpath), 'foreign generation quarantine rejected')
    actual = generation.document['generation_id']
    _require((expected is None or actual == expected) and (observed is None or actual == observed),
             'expected scheduling generation identity changed; downgrade forbidden')
    quarantine_policy._observed_generation_id = actual
    return generation


def deactivate(generation_file, *, expected_generation_id):
    """Explicit serialized historical-accounting reversal; never remove denies."""
    path = quarantine._path(generation_file)
    initial = _validate(quarantine._read(path)[0], path)
    qpath = quarantine._path(initial['quarantine_file'])
    with _writer_barrier(initial['database'], initial['database_identity']) as db:
        with quarantine.manifest_lock(qpath), quarantine.manifest_lock(path, exclusive=True):
            document = _validate(quarantine._read(path)[0], path)
            _require(document == initial and document['generation_id'] == expected_generation_id,
                     'generation deactivation CAS changed')
            gate = quarantine._gate(db)
            from jarvis_runtime.management_archive import management_snapshot
            archive = management_snapshot(document, db, gate)
            _require(archive is None or archive.get('state') != 'applied',
                     'management cleanup must be explicitly restored before budget deactivation')
            _pin_reservations(gate['reservations'], document['original_reservations'])
            _require(not any(rid not in document['archived_reservations'] and row['status'] != 'finished'
                             for rid, row in gate['reservations'].items()),
                     'deactivation blocked by current nonfinished reservation; unknown remains charged')
            # A queued/running successor cannot race a rollback even before it has
            # reserved a provider start. Original and successor namespaces pause.
            _original_runs(db)
            tables = {row[0] for row in db.execute("SELECT name FROM main.sqlite_master WHERE type='table'")}
            _require('jarvis_research_union' not in tables or not db.execute(
                'SELECT 1 FROM main.jarvis_research_union LIMIT 1').fetchone(),
                'deactivation requires no active research union')
            if document['mode'] == 'deactivated':
                return {'status': 'unchanged', 'path': str(path), 'generation_id': expected_generation_id,
                        'automatic_dispatch_restored': False}
            changed = deepcopy(document); changed['mode'] = 'deactivated'; _seal(changed)
            _require(changed['generation_id'] == expected_generation_id, 'generation identity cannot change')
            _write(path, changed, replace=True)
            _require(quarantine._read(path)[0] == changed,
                     'generation deactivation publication differs; inspect uncertain outcome')
            return {'status': 'deactivated', 'path': str(path), 'generation_id': expected_generation_id,
                    'automatic_dispatch_restored': False}
