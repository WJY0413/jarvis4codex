"""Reversible business projection; never a lifecycle release or owner settlement.

Only the independent manifest and its kernel-lock file are writable. All database,
request, ACK, claim, checkpoint and result evidence is read-only. Restoring the
view retains every deny and every unresolved authoritative reservation.
"""
from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
from copy import deepcopy
import argparse
try:
    import fcntl
except ImportError:  # Legacy no-policy imports remain available off Linux.
    fcntl = None
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import tempfile

SCHEMA = 'jarvis-business-quarantine/v1'
FILENAME = 'business-quarantine.json'
ENTRY_KEYS = frozenset(('request_id', 'hold_id', 'thread_id', 'turn_id', 'attempt',
    'root', 'namespace', 'country', 'jurisdiction_code', 'lane', 'claim_id', 'controller_turn_id'))
IDENTIFIERS = ('request_id', 'hold_id', 'thread_id', 'claim_id')
RESERVATION_KEYS = ('owner', 'lane', 'cost', 'generation')
STATUSES = {'pending', 'started', 'unknown', 'finished'}


class QuarantineError(ValueError):
    """The policy is unavailable, inconsistent, or denies this admission."""


def _require(condition, reason):
    if not condition:
        raise QuarantineError(reason)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def _json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            _require(key not in result, 'duplicate JSON key')
            result[key] = value
        return result
    def invalid(_):
        raise QuarantineError('non-finite JSON value')
    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid)
    except (ValueError, TypeError) as exc:
        raise QuarantineError('invalid quarantine JSON') from exc


def _path(value, *, exists=True, directory=False):
    try:
        p = Path(value)
        _require(p.is_absolute() and str(p) == os.fspath(value) and p.resolve() == p
                 and not p.is_symlink(), 'exact absolute canonical path required')
        if exists:
            mode = p.stat().st_mode
            _require(stat.S_ISDIR(mode) if directory else stat.S_ISREG(mode),
                     'existing regular file/directory required')
        return p
    except (OSError, TypeError) as exc:
        raise QuarantineError('quarantine path unavailable') from exc


def _read(path):
    p = _path(path)
    fd = os.open(p, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        meta = os.fstat(stream.fileno())
        _require(stat.S_ISREG(meta.st_mode) and meta.st_nlink == 1 and meta.st_size <= 4_194_304,
                 'bounded unlinked regular evidence required')
        raw = stream.read()
    return _json(raw), hashlib.sha256(raw).hexdigest()


def _inode(path):
    s = _path(path).stat()
    return {'device': s.st_dev, 'inode': s.st_ino}


def _scope(value):
    return tuple(value[k].strip().casefold() for k in ('country', 'jurisdiction_code')) \
        if all(isinstance(value.get(k), str) and value[k].strip()
               for k in ('country', 'jurisdiction_code')) else None


def _entries(entries):
    _require(isinstance(entries, list) and len(entries) == 17, 'exact seventeen approved targets required')
    seen = {k: set() for k in (*IDENTIFIERS, 'root', 'scope')}
    for entry in entries:
        _require(isinstance(entry, dict) and set(entry) == ENTRY_KEYS, 'exact structured target fields required')
        _require(all(isinstance(entry[k], str) and entry[k].strip() for k in
                     (*IDENTIFIERS, 'turn_id', 'country', 'jurisdiction_code')), 'incomplete target identity')
        _require(entry['controller_turn_id'] is None or isinstance(entry['controller_turn_id'], str)
                 and bool(entry['controller_turn_id']), 'invalid original controller turn')
        _require(entry['controller_turn_id'] in (None, entry['turn_id']), 'controller/ACK turn mismatch')
        _require(type(entry['attempt']) is int and entry['attempt'] > 0, 'positive original attempt required')
        _require(entry['lane'] in ('base', 'task'), 'invalid target lane')
        _require(entry['country'] == 'Netherlands'
                 and re.fullmatch(r'GM[0-9]{4}', entry['jurisdiction_code']),
                 'canonical Netherlands/GM-four-digit original target scope required')
        _require(entry['namespace'] is None or isinstance(entry['namespace'], str)
                 and re.fullmatch(r'[a-z][a-z0-9_]{0,39}', entry['namespace']), 'invalid namespace')
        root = _path(entry['root'], directory=True)
        _require(root.name == entry['hold_id'], 'root/hold identity mismatch')
        for key in seen:
            value = _scope(entry) if key == 'scope' else entry[key]
            _require(value not in seen[key], 'duplicate target ' + key)
            seen[key].add(value)
    return entries


def _reservation(value):
    _require(isinstance(value, dict) and value.get('status') in STATUSES
             and isinstance(value.get('owner'), str) and value['owner']
             and value.get('lane') in ('base', 'task')
             and type(value.get('cost')) is int and value['cost'] in (1, 2)
             and type(value.get('generation')) is int and value['generation'] >= 0,
             'unknown or malformed authoritative reservation')


def _validate(document, path):
    _require(isinstance(document, dict) and document.get('schema') == SCHEMA
             and document.get('mode') in ('quarantined', 'restored_paused'), 'unsupported quarantine manifest')
    _require(document.get('manifest_sha256') == digest({k: v for k, v in document.items()
                                                      if k != 'manifest_sha256'}), 'manifest content hash mismatch')
    database = _path(document.get('database'))
    _require(path == database.parent / FILENAME, 'manifest must remain beside original database')
    _require(_inode(database) == document.get('database_identity'), 'original database inode changed')
    entries = _entries(document.get('entries'))
    _require(document.get('target_set_sha256') == digest(entries), 'target set hash mismatch')
    baseline = document.get('baseline_reservations')
    _require(isinstance(baseline, dict) and len(baseline) == 27
             and digest(baseline) == document.get('baseline_reservations_sha256'), 'exact original 27 reservation pins required')
    for value in baseline.values():
        _reservation(value)
    targets = {e['request_id'] for e in entries}
    external = document.get('external_request_ids')
    _require(isinstance(external, list) and len(external) == 10 and len(set(external)) == 10
             and targets <= set(baseline) and set(external) == set(baseline) - targets,
             'exact external ten reservations required')
    _require(document.get('hard_total') == 35 and document.get('lane_targets') == {'base': 20, 'task': 5},
             'reviewed exposure policy changed')
    _require(isinstance(document.get('source_snapshot'), dict), 'original CAS snapshot required')
    return document


@contextmanager
def manifest_lock(path, *, exclusive=False):
    """Shared readers and exclusive apply/restore serialize on one persistent inode."""
    _require(fcntl is not None, 'kernel flock unavailable on this platform; quarantine operation refused')
    p = _path(path, exists=False)
    lock = p.with_name(p.name + '.lock')
    _path(lock, exists=False)
    try:
        create = os.O_CREAT if exclusive and not os.path.lexists(p) else 0
        fd = os.open(lock, (os.O_RDWR | create if exclusive else os.O_RDONLY) | os.O_NOFOLLOW, 0o600)
    except OSError as exc:
        raise QuarantineError('quarantine lock unavailable; shared readers never create it') from exc
    try:
        st = os.fstat(fd)
        _require(stat.S_ISREG(st.st_mode) and st.st_nlink == 1, 'invalid quarantine lock')
        fcntl.flock(fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        _require((lock.stat().st_dev, lock.stat().st_ino) == (st.st_dev, st.st_ino), 'quarantine lock inode changed')
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


@contextmanager
def _database(path, identity=None):
    p = _path(path)
    before = _inode(p)
    _require(identity is None or before == identity, 'original database inode changed')
    try:
        with sqlite3.connect(p.as_uri() + '?mode=ro', uri=True, timeout=10) as db:
            db.execute('PRAGMA query_only=ON')
            db.execute('BEGIN')
            _require(_inode(p) == before, 'original database replaced during read')
            yield db
            _require(_inode(p) == before, 'original database replaced during read')
    except sqlite3.Error as exc:
        raise QuarantineError('authoritative database read failed') from exc


def _gate(db):
    try:
        row = db.execute('SELECT data FROM main.jarvis_transport_gate WHERE provider=?', ('openai',)).fetchone()
        _require(row is not None, 'authoritative gate missing')
        gate = _json(row[0])
        _require(isinstance(gate, dict) and isinstance(gate.get('reservations'), dict)
                 and isinstance(gate.get('pending'), dict), 'authoritative gate malformed')
        for rid, reservation in gate['reservations'].items():
            _require(isinstance(rid, str) and rid, 'invalid authoritative request identity')
            _reservation(reservation)
        for rid, cost in gate['pending'].items():
            _require(rid in gate['reservations'] and gate['reservations'][rid]['status'] != 'finished'
                     and type(cost) is int and cost == gate['reservations'][rid]['cost'], 'unknown pending exposure')
        return gate
    except sqlite3.Error as exc:
        raise QuarantineError('authoritative gate unavailable') from exc


class Policy:
    def __init__(self, path, *, expected_generation_id=None):
        self.expected_generation_id = expected_generation_id
        self.path = _path(path)
        self.document = _validate(_read(self.path)[0], self.path)
        self._binding = {k: deepcopy(self.document[k]) for k in ('database', 'database_identity',
            'target_set_sha256', 'baseline_reservations_sha256')}
        self.require_budget()

    @property
    def data(self):
        return deepcopy(self.document)

    @property
    def entries(self):
        return deepcopy(self.document['entries'])

    @property
    def mode(self):
        return self.document['mode']

    def _refresh(self):
        current = _validate(_read(self.path)[0], self.path)
        _require(all(current.get(k) == v for k, v in self._binding.items()), 'quarantine authority changed')
        self.document = current

    @contextmanager
    def read_scope(self):
        with manifest_lock(self.path):
            self._refresh()
            self.require_budget()
            yield self

    def _matches(self, identity):
        _require(isinstance(identity, Mapping), 'structured identity required')
        from jarvis_runtime.scheduling_budget_generation import generation_for
        generation = generation_for(self)
        if generation is not None and generation.blocked(identity):
            return True
        values = [identity]
        if isinstance(identity.get('current'), Mapping):
            values.append(identity['current'])
        for entry in self.document['entries']:
            if any(value.get(k) == entry[k] for value in values for k in IDENTIFIERS):
                return True
            if _scope(identity) == _scope(entry):
                return True
        return False

    def blocked(self, identity_or_claim):
        self._refresh()
        self.require_budget()
        return self._matches(identity_or_claim)

    def require_allowed(self, identity, require_scope=False):
        self._refresh()
        self.require_budget()
        _require(not self._matches(identity), 'business identity is quarantined; restore-view does not permit dispatch')
        _require(not require_scope or identity.get('country') == 'Netherlands'
                 and isinstance(identity.get('jurisdiction_code'), str)
                 and re.fullmatch(r'GM[0-9]{4}', identity['jurisdiction_code']),
                 'canonical Netherlands/GM-four-digit structured scope required')
        return True

    def require_budget(self, db_connection=None, request_id=None, prospective=False, lane=None):
        """Require original shared authority; never accept an unanchored gate dict."""
        self._refresh()
        d = self.document
        from jarvis_runtime.scheduling_budget_generation import generation_for
        generation = generation_for(self)
        if generation is not None:
            return generation.require_budget(db_connection=db_connection, request_id=request_id,
                                             prospective=prospective, lane=lane)
        if db_connection is None:
            with _database(d['database'], d['database_identity']) as db:
                return self._budget(_gate(db), request_id, prospective, lane)
        _require(isinstance(db_connection, sqlite3.Connection), 'anchored SQLite connection required')
        main = next((row[2] for row in db_connection.execute('PRAGMA database_list') if row[1] == 'main'), None)
        _require(main == d['database'] and _inode(main) == d['database_identity'], 'fresh or foreign budget database rejected')
        return self._budget(_gate(db_connection), request_id, prospective, lane)

    def _budget(self, gate, request_id, prospective, lane):
        d = self.document
        reservations, baseline = gate['reservations'], d['baseline_reservations']
        _require(set(baseline) <= set(reservations), 'original reservation disappeared; deny remains in force')
        for rid, pin in baseline.items():
            actual = reservations[rid]
            _require(all(actual.get(k) == pin.get(k) for k in RESERVATION_KEYS), 'original reservation identity changed')
            _require(pin['status'] != 'finished' or actual['status'] == 'finished', 'finished baseline reservation reopened')
        unresolved = {rid: r for rid, r in reservations.items() if r['status'] != 'finished'}
        external = set(d['external_request_ids'])
        lanes = {name: sum(r['lane'] == name for rid, r in unresolved.items() if rid not in external)
                 for name in ('base', 'task')}
        _require(type(prospective) is bool and (lane is None or lane in lanes), 'invalid admission budget request')
        increment = int(prospective)
        if request_id is not None:
            _require(isinstance(request_id, str) and request_id in unresolved, 'native dispatch lacks existing unresolved reservation')
            _require(lane is None or unresolved[request_id]['lane'] == lane, 'reservation lane changed')
            _require(not prospective, 'existing reservation must not be counted prospectively')
        _require(not prospective or lane in lanes, 'prospective admission requires lane')
        _require(len(unresolved) + increment <= d['hard_total'], 'shared potential exposure exceeds 35')
        _require(all(count + (increment if name == lane else 0) <= d['lane_targets'][name]
                     for name, count in lanes.items()), 'shared lane potential exposure exceeded')
        return {'potential_exposure': len(unresolved), 'hard_total': d['hard_total'],
                'available': d['hard_total'] - len(unresolved), 'lanes': lanes,
                'quarantined_potential_exposure': sum(e['request_id'] in unresolved for e in d['entries']),
                'external_potential_exposure': len(external & set(unresolved))}

    def view(self, claims):
        with self.read_scope():
            result = {'active': [], 'quarantined': [], 'restored_paused': []}
            budget = self.require_budget()
            cleaned = budget.get('management_cleanup_applied') is True
            archived_count = 0
            for claim in claims:
                matched = self._matches(claim)
                if cleaned and matched:
                    archived_count += 1
                    continue  # Archive is accessed explicitly, never an active management list.
                key = self.mode if matched else 'active'
                result[key].append(deepcopy(claim))
            return {**result, 'mode': 'management_archived' if cleaned else self.mode,
                    'archived_record_count': archived_count, 'automatic_dispatch_restored': False,
                    'budget': budget}


def policy_for(registry, explicit_path=None, *, expected_generation_id=None):
    """Default absence is legacy-compatible; explicit policy absence is an error."""
    if explicit_path is not None:
        return Policy(explicit_path, expected_generation_id=expected_generation_id)
    root = _path(registry, exists=False)
    if root.exists():
        _path(root, directory=True)
    path = root / FILENAME
    if not os.path.lexists(path):
        _require(expected_generation_id is None, 'pinned scheduling generation requires quarantine policy')
        return None
    return Policy(path, expected_generation_id=expected_generation_id)


def _snapshot(db, entries):
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    runs = sorted(t for t in tables if re.fullmatch(r'jarvis_research_run(?:__[a-z][a-z0-9_]{0,39})?', t))
    _require(runs, 'authoritative research runs missing')
    states = {}
    for table in runs:
        row = db.execute(f'SELECT data FROM {table} WHERE id=1').fetchone()
        _require(row is not None, 'authoritative research run missing')
        state = _json(row[0])
        _require(state.get('status') == 'paused' and isinstance(state.get('claims'), list), 'all research runs must be paused')
        states[table] = state
    union = list(db.execute('SELECT namespace,data FROM jarvis_research_union ORDER BY namespace')) \
        if 'jarvis_research_union' in tables else []
    _require(not union, 'active research union must be absent')
    pins = {}
    for entry in entries:
        table = 'jarvis_research_run' + ('__' + entry['namespace'] if entry['namespace'] else '')
        state = states.get(table)
        _require(state is not None, 'target original namespace missing')
        claims = [c for c in state['claims'] if c.get('claim_id') == entry['claim_id']]
        _require(len(claims) == 1, 'target original claim missing or ambiguous')
        c = claims[0]; current = c.get('current') or {}
        _require(all(c.get(k) == entry[k] for k in ('claim_id', 'country', 'jurisdiction_code', 'lane',
                    'hold_id', 'thread_id', 'attempt')) and current.get('request_id') == entry['request_id']
                 and current.get('turn_id') == entry['controller_turn_id'], 'original target generation changed')
        root = _path(entry['root'], directory=True)
        request, request_sha = _read(root / 'request.json')
        ack, ack_sha = _read(root / 'ack.json')
        _require(all(request.get(k) == entry[k] for k in ('request_id', 'hold_id')),
                 'original request identity changed')
        _require(all(ack.get(k) == entry[k] for k in ('request_id', 'hold_id', 'thread_id', 'turn_id')),
                 'original ACK identity changed')
        pins[entry['request_id']] = {'request_sha256': request_sha, 'ack_sha256': ack_sha,
                                    'claim_sha256': digest(c)}
    gate = _gate(db)
    _require(sum(r['status'] != 'finished' for r in gate['reservations'].values()) == 27,
             'exact original 27 unresolved reservations required at apply')
    for entry in entries:
        r = gate['reservations'].get(entry['request_id'])
        _require(r is not None and r['lane'] == entry['lane'] and r['status'] != 'finished',
                 'target lacks original unresolved gate reservation')
        table = 'jarvis_research_run' + ('__' + entry['namespace'] if entry['namespace'] else '')
        _require(r['owner'] == states[table].get('config', {}).get('owner_id'), 'target gate owner changed')
    return gate, {'runs_sha256': digest(states), 'gate_sha256': digest(gate),
                  'union_sha256': digest(union), 'target_evidence': pins}


def plan(database, targets):
    """Prepare only; caller must supply the explicitly approved structured allowlist."""
    database = _path(database)
    entries = sorted(deepcopy(_entries(targets)), key=lambda e: e['request_id'])
    identity = _inode(database)
    with _database(database, identity) as db:
        gate, snapshot = _snapshot(db, entries)
    baseline = deepcopy({rid: r for rid, r in gate['reservations'].items() if r['status'] != 'finished'})
    document = {'schema': SCHEMA, 'mode': 'quarantined', 'database': str(database),
        'database_identity': identity, 'entries': entries, 'target_set_sha256': digest(entries),
        'baseline_reservations': baseline, 'baseline_reservations_sha256': digest(baseline),
        'external_request_ids': sorted(set(baseline) - {e['request_id'] for e in entries}),
        'hard_total': 35, 'lane_targets': {'base': 20, 'task': 5}, 'source_snapshot': snapshot}
    document['manifest_sha256'] = digest(document)
    _validate(document, database.parent / FILENAME)
    checker = object.__new__(Policy)
    checker.document = document
    checker._budget(gate, None, False, None)
    return document


def _write(path, document, *, replace=False):
    # Unknown leftover publication attempts require inspection, never overwrite them.
    _require(not list(path.parent.glob('.business-quarantine-*.tmp')), 'uncertain prior publication requires inspection')
    fd, name = tempfile.mkstemp(prefix='.business-quarantine-', suffix='.tmp', dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump(document, stream, sort_keys=True, ensure_ascii=False, allow_nan=False)
            stream.write('\n'); stream.flush(); os.fsync(stream.fileno())
        if replace:
            os.replace(temporary, path)
        else:
            os.link(temporary, path)  # Atomic publication without replacing unknown state.
            temporary.unlink()
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        if temporary.exists():
            temporary.unlink()


def apply(document):
    """CAS the read-only original evidence, then publish only one independent file."""
    _require(isinstance(document, dict), 'exact prepared plan required')
    database = _path(document.get('database'))
    path = database.parent / FILENAME
    _validate(document, path)
    _require(document['mode'] == 'quarantined', 'apply requires quarantined plan')
    with manifest_lock(path, exclusive=True):
        if os.path.lexists(path):
            existing = _validate(_read(path)[0], path)
            _require(existing == document, 'different or partial quarantine already exists')
            Policy(path).require_budget()
            return {'status': 'unchanged', 'path': str(path)}
        _require(plan(database, document['entries']) == document, 'original paused evidence CAS changed')
        _write(path, document)
        _require(plan(database, document['entries']) == document,
                 'evidence changed during publication; deny file retained, inspect uncertain outcome')
        Policy(path).require_budget()
        return {'status': 'applied', 'path': str(path)}


def restore_view(database, targets):
    """A view-only reversal. Never re-enable dispatch, settle a task, or release a seat."""
    database = _path(database)
    entries = sorted(deepcopy(_entries(targets)), key=lambda e: e['request_id'])
    path = database.parent / FILENAME
    with manifest_lock(path, exclusive=True):
        policy = Policy(path)
        _require(policy.document['entries'] == entries, 'restore target set differs from exact allowlist')
        if policy.mode == 'restored_paused':
            return {'status': 'unchanged', 'path': str(path), 'automatic_dispatch_restored': False}
        document = deepcopy(policy.document)
        document['mode'] = 'restored_paused'
        document['manifest_sha256'] = digest({k: v for k, v in document.items() if k != 'manifest_sha256'})
        _write(path, document, replace=True)
        return {'status': 'restored_paused', 'path': str(path), 'automatic_dispatch_restored': False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('plan', 'apply', 'view', 'restore-view'))
    parser.add_argument('--database', required=True)
    parser.add_argument('--targets', required=True, help='Explicit approved JSON array, never inferred')
    parser.add_argument('--plan', help='Previously reviewed plan file required for apply')
    args = parser.parse_args(argv)
    targets = _read(_path(args.targets))[0]
    if args.operation == 'plan':
        result = plan(args.database, targets)
    elif args.operation == 'apply':
        _require(args.plan, 'apply requires explicit reviewed --plan')
        document = _read(_path(args.plan))[0]
        _require(document.get('database') == str(_path(args.database))
                 and document.get('entries') == sorted(_entries(targets), key=lambda e: e['request_id']),
                 'plan database or approved target set differs')
        result = apply(document)
    elif args.operation == 'restore-view':
        result = restore_view(args.database, targets)
    else:
        policy = Policy(_path(args.database).parent / FILENAME)
        _require(policy.entries == sorted(_entries(targets), key=lambda e: e['request_id']), 'view allowlist differs')
        result = policy.view(targets)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == '__main__':
    main()
