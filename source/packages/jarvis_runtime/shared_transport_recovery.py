"""Provider-scoped admission and owner-evidenced transport recovery.

This module never settles a business task or releases lifecycle occupancy. Unknown
starts never expire. Observer thread history is not an authority for active owner
execution (0.159.2 can synthesize ``interrupted`` in another app-server).
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, asdict
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import threading
import time
from types import SimpleNamespace
from typing import Callable
from urllib.parse import urlparse

from jarvis_runtime.native_error_origin import (has_structured_error, transport_error_facts,
    facts_block_transport)


class RecoveryRejected(ValueError):
    """Insufficient evidence or a changed admission generation; no recovery."""


def _require(value, message):
    if not value:
        raise RecoveryRejected(message)


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def _number(value):
    return type(value) in (int, float) and math.isfinite(value)


@dataclass(frozen=True)
class OwnerExecutionEvidence:
    request_id: str
    thread_id: str
    turn_id: str
    model: str
    host_instance_id: str
    owner: str
    lane: str
    started_at: float
    observed_at: float
    failure_epoch: int
    status: str
    provider_error: bool = False
    cancelled: bool = False
    stopping: bool = False
    witness_source: str = 'owner_native_event_stream'
    progress_at: float | None = None
    progress_sequence: int = 0

    tool_error: bool = False
    unknown_error: bool = False
    error_origins: list | None = None

    def validate(self, now, max_age):
        self.validate_activity(now, max_age)
        _require(not self.provider_error and not self.tool_error and not self.unknown_error,
                 'owner transport error; activity is not proof of health')

    def validate_activity(self, now, max_age):
        """Exact owner execution only; never certifies transport health or release."""
        _require(self.witness_source == 'owner_native_event_stream', 'owner event authority required')
        _require(all(isinstance(x, str) and x for x in (self.request_id, self.thread_id, self.turn_id,
                    self.model, self.host_instance_id, self.owner)), 'incomplete owner identity')
        _require(self.lane in ('base', 'task'), 'invalid lane')
        _require(self.status == 'inProgress', 'owner is not actively executing')
        _require(not self.cancelled and not self.stopping, 'owner cancel/stop')
        _require(type(self.progress_sequence) is int and self.progress_sequence >= 0
                 and (self.progress_at is None or _number(self.progress_at) and self.progress_at <= now), 'invalid owner progress')
        _require(type(self.failure_epoch) is int and self.failure_epoch >= 0, 'invalid error epoch')
        _require(_number(self.started_at) and _number(self.observed_at)
                 and self.started_at <= self.observed_at <= now
                 and now - self.observed_at <= max_age, 'stale owner evidence')

    def identity(self):
        return (self.request_id, self.thread_id, self.turn_id, self.model,
                self.host_instance_id, self.owner, self.lane, self.started_at, self.failure_epoch)


@dataclass(frozen=True)
class TransportRecoveryProof:
    owner: OwnerExecutionEvidence
    generation: int
    observed_at: float
    web_items: tuple[tuple[str, str], ...]
    scan_complete: bool


@dataclass(frozen=True)
class TerminalTransportRecoveryProof:
    request_id: str
    hold_id: str
    thread_id: str
    turn_id: str
    model: str
    provider: str
    owner: str
    lane: str
    generation: int
    observed_at: float
    owner_terminal_sha256: str
    native_scan_sha256: str
    checkpoint_sha256: str
    web_items: tuple[tuple[str, str], ...]


def collect_terminal_recovery_proof(*, request_id, hold_id, owner, lane, model,
                                    generation, hold, turn, exact, checkpoint, clock=time.time):
    """Successful business final from the original owner plus a complete scan.

    This never replaces owner/release authority with an observer projection. The
    caller must reread the unchanged original owner receipt before entering its
    checkpoint transaction. Search/error checks use the same strict collector
    rules as mid-turn recovery; setup/helper text cannot certify recovery.
    """
    from adapters.codex_app_server.native_execution_evidence import classify_owner_terminal_receipt
    from jarvis_runtime.native_scan_receipt import validate_public_scan
    _require(type(generation) is int and generation >= 0, 'invalid terminal generation')
    _require(isinstance(checkpoint, dict) and isinstance(checkpoint.get('checkpoint'), dict)
             and checkpoint.get('outcome') in ('partial', 'blocked', 'exhaustion_candidate')
             and isinstance(checkpoint.get('next_actions'), list), 'research checkpoint required')
    data, readback = exact.get('data') or {}, exact.get('readback') or {}
    thread_id, turn_id = hold.get('thread_id'), hold.get('turn_id')
    _require(exact.get('status') == 'completed' and readback.get('verified') is True
             and readback.get('terminal') is True and data.get('thread_id') == thread_id
             and data.get('turn_id') == turn_id and _hash(data.get('turns')) == _hash([turn]), 'exact final binding required')
    authority = classify_owner_terminal_receipt(hold, request_id=request_id, hold_id=hold_id,
        thread_id=thread_id, turn_id=turn_id, model=model, native_turn=turn, observed_thread_id=data.get('thread_id'),
        protocol_version=(data.get('native_terminal_evidence') or {}).get('protocol_version'))
    _require(authority['terminal_confirmed'] is True and authority['status'] == 'completed'
             and hold.get('hold_released') is True and hold.get('error') is None
             and turn.get('native_error') is None and turn.get('error') is None
             and turn.get('execution_status') not in ('failed', 'blocked'), 'successful original owner final required')
    native = data.get('native_terminal_evidence') or {}
    _require(native.get('terminal_confirmed') is not True or native.get('status') == 'completed',
             'native terminal contradicts successful owner')
    scan = validate_public_scan(turn.get('native_scan_receipt'), thread_id=thread_id, turn=turn, model_provider='openai')
    final = turn.get('items') or []
    _require(len(final) == 1 and final[0].get('phase') == 'final_answer'
             and _hash(json.loads(final[0]['text'])) == _hash(checkpoint), 'checkpoint is not the exact business final')
    good = []
    for page in scan['items']['pages']:
        for projected in page['entries']:
            if projected.get('omitted') is True:
                continue
            entry = projected['envelope']; item = entry['item']
            facts = transport_error_facts(entry)
            _require(not facts_block_transport(facts), 'terminal provider or unknown-origin error')
            if facts:
                # A known website/tool failure is not a provider failure, but
                # that errored search itself cannot be positive success evidence.
                continue
            # Exact turn IDs and exhausted pagination bind this search to the
            # original business turn; no live owner started_at is needed after exit.
            if successful_native_search(entry, owner=SimpleNamespace(started_at=0.0), now=clock()):
                good.append((item['id'], _hash(entry)))
    _require(good, 'terminal final lacks successful native search')
    return TerminalTransportRecoveryProof(request_id, hold_id, thread_id, turn_id, model, 'openai',
        owner, lane, generation, clock(), _hash({k:hold.get(k) for k in ('request_id', 'hold_id',
            'thread_id', 'turn_id', 'status', 'terminal_confirmed', 'terminal_evidence',
            'hold_released', 'runtime_context', 'error')}), _hash(scan), _hash(checkpoint), tuple(good))


_structured_error = has_structured_error  # Backward-compatible shared predicate.


def successful_native_search(entry, *, owner, now):
    """Only completed native search with structured positive citations qualifies.

    Mere item/action presence, model self-report, null status, empty results, and
    open-page failures are never positive evidence. Unknown structured errors are
    rejected conservatively by the full-scan collector, without scanning page text.
    """
    item = entry['item']
    if item.get('type') != 'webSearch' or (item.get('action') or {}).get('type') != 'search':
        return False
    start, end = entry.get('startedAtMs'), entry.get('completedAtMs')
    if not (_number(start) and _number(end) and owner.started_at * 1000 <= start <= end <= now * 1000):
        return False
    if item.get('status') not in (None, 'completed', 'success', 'succeeded'):
        return False
    results = item.get('results')
    if not isinstance(results, list) or not results:
        return False
    return any(isinstance(r, dict) and r.get('type') == 'text_result'
               and all(isinstance(r.get(k), str) and r[k].strip() for k in ('ref_id', 'title', 'domain', 'url'))
               and urlparse(r['url']).scheme in ('http', 'https') and urlparse(r['url']).netloc
               for r in results)


def collect_recovery_proof(request: Callable, owner: OwnerExecutionEvidence, generation: int,
                           *, clock=time.time, max_age=10.0):
    """Read complete exact-turn native envelopes; never consult observer status.

    ``owner`` must come from the live owning event recorder, not acceptance ACKs.
    request(method, params) must be a trusted read-only native client. The caller
    subsequently supplies the recorder's lock-held guard to recover().
    """
    owner.validate(clock(), max_age)
    _require(type(generation) is int and generation >= 0, 'invalid generation')
    cursor = None
    cursors, ids, good = set(), set(), []
    while True:
        params = dict(threadId=owner.thread_id, turnId=owner.turn_id, sortDirection='asc', limit=100)
        if cursor is not None:
            params['cursor'] = cursor
        page = request('thread/items/list', params)
        _require(isinstance(page.get('data'), list) and 'nextCursor' in page, 'incomplete native page')
        for entry in page['data']:
            _require(isinstance(entry, dict) and entry.get('turnId') == owner.turn_id, 'wrong native turn')
            _require(not transport_error_facts(entry), 'native provider/tool/unknown error')
            item = entry.get('item')
            _require(isinstance(item.get('id'), str) and item['id'] and item['id'] not in ids, 'duplicate/missing native item')
            ids.add(item['id'])
            if successful_native_search(entry, owner=owner, now=clock()):
                good.append((item['id'], _hash(entry)))
        cursor = page['nextCursor']
        if cursor is None:
            break
        _require(isinstance(cursor, str) and cursor and cursor not in cursors, 'invalid native cursor')
        cursors.add(cursor)
    _require(good, 'no completed successful native search')
    return TransportRecoveryProof(owner, generation, clock(), tuple(good), True)


class SharedTransportGate:
    """One provider gate, separate from controller queues and lifecycle slots.

    Each method uses a short BEGIN IMMEDIATE. No network work under its DB lock.
    A failed reservation does not seal a lane or spend tokens. TASK cost2 may wait
    without preventing cost1 requests. Tokens are never refunded.
    """
    def __init__(self, db_path, provider_key='openai', *, clock=time.time, burst=2,
                 tokens_per_second=0.5, min_spacing=2.0, cooldown_seconds=120.0, require_existing=False):
        _require(provider_key and burst == 2 and _number(tokens_per_second) and 0 < tokens_per_second <= 2/3
                 and _number(min_spacing) and min_spacing >= 0 and _number(cooldown_seconds) and cooldown_seconds > 0,
                 'invalid transport policy')
        self.path, self.provider_key, self.clock = Path(db_path), provider_key, clock
        self.policy = dict(burst=burst, tokens_per_second=tokens_per_second,
                           min_spacing=min_spacing, cooldown_seconds=cooldown_seconds)
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            if require_existing:
                tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                _require({'jarvis_transport_gate', 'jarvis_transport_receipts'} <= tables,
                         'existing shared provider schema required')
                _require(db.execute('SELECT 1 FROM jarvis_transport_gate WHERE provider=?', (provider_key,)).fetchone(),
                         'existing shared provider row required; initialization forbidden')
            else:
                db.execute('CREATE TABLE IF NOT EXISTS jarvis_transport_gate(provider TEXT PRIMARY KEY,data TEXT NOT NULL)')
                db.execute('CREATE TABLE IF NOT EXISTS jarvis_transport_receipts(provider TEXT,kind TEXT,receipt_id TEXT,data TEXT,PRIMARY KEY(provider,kind,receipt_id))')
                now = clock()
                initial = dict(policy=self.policy, generation=0, until=0.0, open=False, probe=None,
                               probe_generation=None, failures=0, last_failure=None, pending={}, reservations={},
                               tokens=float(burst), token_at=now, next_at=0.0)
                db.execute('INSERT OR IGNORE INTO jarvis_transport_gate VALUES(?,?)', (provider_key, json.dumps(initial)))
            _require(self._load(db)['policy'] == self.policy, 'existing transport policy mismatch')

    def _db(self):
        return sqlite3.connect(self.path, timeout=30)

    def _load(self, db):
        return json.loads(db.execute('SELECT data FROM jarvis_transport_gate WHERE provider=?', (self.provider_key,)).fetchone()[0])

    def _save(self, db, state):
        db.execute('UPDATE jarvis_transport_gate SET data=? WHERE provider=?', (json.dumps(state), self.provider_key))

    def snapshot(self):
        with self._db() as db:
            return self._load(db)

    def import_formal_snapshot(self, snapshot, *, expected_sha256):
        """Copy a frozen formal predecessor exactly into a pristine provider.

        Caller proves predecessor/run eligibility and freezes its complete snapshot
        in one read transaction. This method neither adopts owners nor settles work.
        Extra gate keys and receipt payloads are retained without transformation.
        A separate durable marker forbids replay even for an empty generation zero.
        """
        _require(isinstance(snapshot, dict) and set(snapshot) == {'provider', 'gate', 'receipts'}, 'invalid formal snapshot')
        # Detach caller-owned containers before digest/validation and transaction entry.
        snapshot = json.loads(json.dumps(snapshot, allow_nan=False))
        _require(_hash(snapshot) == expected_sha256, 'formal snapshot hash mismatch')
        _require(snapshot['provider'] == self.provider_key, 'formal provider mismatch')
        state, receipts = snapshot['gate'], snapshot['receipts']
        required = {'policy', 'generation', 'until', 'open', 'probe', 'probe_generation',
                    'failures', 'last_failure', 'pending', 'reservations', 'tokens', 'token_at', 'next_at'}
        _require(isinstance(state, dict) and required <= set(state), 'incomplete formal gate')
        _require(_hash(state['policy']) == _hash(self.policy), 'formal policy mismatch')
        for key in ('generation', 'failures'):
            _require(type(state[key]) is int and state[key] >= 0, 'invalid formal counter')
        for key in ('until', 'token_at', 'next_at'):
            _require(_number(state[key]) and state[key] >= 0, 'invalid formal timestamp')
        _require(state['last_failure'] is None or _number(state['last_failure']) and state['last_failure'] >= 0, 'invalid formal failure time')
        _require(type(state['open']) is bool and _number(state['tokens']) and 0 <= state['tokens'] <= self.policy['burst'], 'invalid formal budget')
        _require(state['probe_generation'] is None or type(state['probe_generation']) is int and 0 <= state['probe_generation'] <= state['generation'], 'invalid probe generation')
        if 'effective_tokens_per_second' in state:
            _require(_number(state['effective_tokens_per_second']) and 0 < state['effective_tokens_per_second'] <= self.policy['tokens_per_second'], 'invalid effective rate')
        pending, reservations = state['pending'], state['reservations']
        _require(isinstance(pending, dict) and isinstance(reservations, dict), 'invalid formal reservations')
        _require(all(type(cost) is int and cost in (1, 2) for cost in pending.values()) and sum(pending.values()) <= 2, 'invalid pending budget')
        _require(set(pending) <= set(reservations), 'unbound pending budget')
        for rid, row in reservations.items():
            _require(isinstance(rid, str) and rid and isinstance(row, dict), 'invalid reservation identity')
            _require(isinstance(row.get('owner'), str) and row['owner'] and row.get('lane') in ('base', 'task'), 'invalid reservation owner')
            _require(type(row.get('cost')) is int and row['cost'] in (1, 2) and (row['lane'] == 'task' or row['cost'] == 1), 'invalid reservation cost')
            _require(type(row.get('generation')) is int and 0 <= row['generation'] <= state['generation'], 'invalid reservation generation')
            _require(row.get('status') in ('pending', 'started', 'unknown', 'finished'), 'invalid reservation status')
            _require((row['status'] != 'pending' or rid in pending)
                     and (rid not in pending or row['status'] in ('pending', 'unknown') and pending[rid] == row['cost']), 'inconsistent pending reservation')
        probe = state['probe']
        _require(probe is None or isinstance(probe, str) and probe in reservations
                 and reservations[probe]['status'] != 'finished' and state['open']
                 and state['probe_generation'] is not None, 'invalid formal probe')
        _require(isinstance(receipts, list), 'invalid formal receipts')
        keys = []
        for receipt in receipts:
            _require(isinstance(receipt, dict) and set(receipt) == {'kind', 'receipt_id', 'data'}, 'invalid receipt envelope')
            _require(all(isinstance(receipt[k], str) and receipt[k] for k in ('kind', 'receipt_id'))
                     and isinstance(receipt['data'], dict), 'invalid receipt payload')
            keys.append((receipt['kind'], receipt['receipt_id']))
        _require(keys == sorted(set(keys)), 'receipts must be sorted and unique')
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('CREATE TABLE IF NOT EXISTS jarvis_transport_imports(provider TEXT PRIMARY KEY,snapshot_sha256 TEXT NOT NULL)')
            current = self._load(db)
            _require(set(current) == required and current['policy'] == self.policy
                     and current['generation'] == 0 and current['failures'] == 0
                     and not current['open'] and current['probe'] is None and current['probe_generation'] is None
                     and current['last_failure'] is None and not current['pending'] and not current['reservations']
                     and current['tokens'] == self.policy['burst'] and current['until'] == current['next_at'] == 0
                     and not db.execute('SELECT 1 FROM jarvis_transport_receipts WHERE provider=?', (self.provider_key,)).fetchone()
                     and not db.execute('SELECT 1 FROM jarvis_transport_imports WHERE provider=?', (self.provider_key,)).fetchone(), 'formal gate is not pristine')
            self._save(db, state)
            db.executemany('INSERT INTO jarvis_transport_receipts VALUES(?,?,?,?)',
                           [(self.provider_key, r['kind'], r['receipt_id'], json.dumps(r['data'], allow_nan=False)) for r in receipts])
            db.execute('INSERT INTO jarvis_transport_imports VALUES(?,?)', (self.provider_key, expected_sha256))
        return state

    def import_legacy_snapshot(self, snapshot, reservations, *, expected_sha256):
        """Explicit frozen-source migration, never a fresh budget reset.

        Operator must first pause/drain legacy dispatch and freeze its shared gate
        and occupancy in one source transaction. ``snapshot`` is the exact legacy
        gate JSON. ``reservations`` binds EVERY legacy occupancy/pending/probe ID
        to owner/lane/status. Missing/unknown ownership is a blocker, not permission
        to discard it. This API never touches the legacy database. It only imports
        into a pristine formal provider row; caller must verify source still frozen.
        SHA256 binds BOTH snapshot and reservation mapping, not an unreviewed latest.
        """
        _require(_hash({'gate':snapshot,'reservations':reservations})==expected_sha256, 'legacy snapshot hash mismatch')
        _require(isinstance(reservations,dict) and isinstance(snapshot.get('pending'),dict), 'invalid legacy snapshot')
        _require(not snapshot.get('blocked') and not snapshot.get('sealed') and not snapshot.get('task_budget_waiter'), 'unresolved legacy control barrier; migration blocked')
        _require(snapshot.get('used')==sum(snapshot['pending'].values()), 'unexplained legacy start budget')
        required=set(snapshot['pending']) | ({snapshot['probe']} if snapshot.get('probe') else set())
        _require(required<=set(reservations), 'missing legacy owner bindings')
        for rid,row in reservations.items():
            _require(isinstance(rid,str) and rid and row.get('owner') and row.get('lane') in ('base','task')
                     and row.get('status') in ('pending','started','unknown'), 'invalid legacy reservation')
            _require((rid in snapshot['pending']) == (row['status']=='pending' or row['status']=='unknown' and row.get('pending_start') is True), 'legacy pending/start mismatch')
        _require(_number(snapshot['tokens']) and 0<=snapshot['tokens']<=self.policy['burst'], 'legacy token budget outside policy')
        for field in ('until','token_at','next_at'):_require(_number(snapshot[field]), 'invalid legacy deadline')
        _require(type(snapshot['generation']) is int and snapshot['generation']>=0, 'invalid legacy generation')
        imported={key:snapshot[key] for key in ('generation','until','open','probe','probe_generation','failures','last_failure','pending','tokens','token_at','next_at')}
        imported['policy']=self.policy
        _require(type(snapshot.get('ceiling')) is int and 2<=snapshot['ceiling']<=25, 'missing legacy rate ceiling')
        imported['effective_tokens_per_second']=min(self.policy['tokens_per_second'],min(40,2*snapshot['ceiling'])/60)
        imported['legacy_ceiling']=snapshot['ceiling']
        imported['reservations']={rid:dict(owner=row['owner'],lane=row['lane'],status=row['status'],
                    cost=snapshot['pending'].get(rid,row.get('cost',1)),generation=row.get('generation',snapshot['generation'])) for rid,row in reservations.items()}
        imported['legacy_snapshot_sha256']=expected_sha256
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE');current=self._load(db)
            _require(current['generation']==0 and not current['reservations'] and not current['open']
                     and not current.get('legacy_snapshot_sha256') and not db.execute('SELECT 1 FROM jarvis_transport_receipts WHERE provider=?',(self.provider_key,)).fetchone(), 'formal gate is not pristine')
            self._save(db,imported)
        return self.snapshot()

    def try_reserve(self, request_id, owner, lane, cost=1):
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            return self.try_reserve_in_transaction(db, request_id, owner, lane, cost)

    def try_reserve_in_transaction(self, db, request_id, owner, lane, cost=1):
        _require(db.in_transaction, 'caller must hold BEGIN IMMEDIATE')
        _require(all(isinstance(v, str) and v for v in (request_id, owner)), 'request/owner required')
        _require(lane in ('base', 'task') and type(cost) is int and cost in (1,2) and (lane == 'task' or cost == 1), 'invalid lane/start cost')
        from jarvis_runtime.management_archive import require_request_not_archived
        require_request_not_archived(db, request_id)
        s=self._load(db);now=self.clock()
        _require(request_id not in s['reservations'], 'request already reserved; never redispatch')
        tokens = min(self.policy['burst'], s['tokens'] + max(0, now-s['token_at']) * s.get('effective_tokens_per_second',self.policy['tokens_per_second']))
        if (now < max(s['until'],s['next_at']) or sum(s['pending'].values())+cost>2
                or tokens+1e-9<cost or (s['open'] and s['probe'] is not None)):
            return False
        s['tokens']=tokens-cost;s['token_at']=now;s['next_at']=now+self.policy['min_spacing']*cost
        s['pending'][request_id]=cost
        s['reservations'][request_id]=dict(owner=owner,lane=lane,cost=cost,generation=s['generation'],status='pending')
        if s['open']:
            s['probe']=request_id;s['probe_generation']=s['generation']
        self._save(db,s)
        return True

    def dispatch_allowed_in_transaction(self, db, request_id):
        """Recheck an already charged start after intervening read/preflight work.

        The reservation owns its spacing/token debit. Do not debit again or test
        next_at (which includes this reservation's own spacing). A newer provider
        failure must prevent an old prepared/dispatching generation from sending.
        """
        _require(db.in_transaction, 'caller must hold BEGIN IMMEDIATE')
        s = self._load(db); row = s['reservations'].get(request_id)
        _require(row and row['status'] == 'pending' and type(row.get('cost')) is int and row['cost'] in (1, 2)
                 and type(s['pending'].get(request_id)) is int and s['pending'][request_id] == row['cost']
                 and type(row.get('generation')) is int and type(s.get('generation')) is int
                 and type(s.get('open')) is bool and _number(s.get('until')),
                 'exact pending reservation/generation is malformed; retain without dispatch')
        return (row['generation'] == s['generation'] and self.clock() >= s['until']
                and (not s['open'] or s['probe'] == request_id
                     and type(s.get('probe_generation')) is int and s['probe_generation'] == s['generation']))

    def release_unsubmitted_in_transaction(self, db, request_id):
        """Only for caller's atomically proven never-sent prepared intent.

        Caller must check its intent is not dispatching under this same DB lock.
        This method never refunds tokens or clears the open provider breaker.
        """
        _require(db.in_transaction, 'caller must hold BEGIN IMMEDIATE')
        s=self._load(db);row=s['reservations'].get(request_id)
        _require(row and row['status']=='pending', 'not an unsubmitted reservation')
        row['status']='finished';s['pending'].pop(request_id,None)
        if s['probe']==request_id:s['probe']=None
        self._save(db,s)

    def mark_started(self, request_id):
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE');s=self._load(db)
            _require(request_id in s['reservations'], 'unknown reservation')
            row=s['reservations'][request_id]
            if row['status']=='started':return
            _require(row['status'] in ('pending','unknown'), 'reservation already terminal')
            s['pending'].pop(request_id,None);row['status']='started';self._save(db,s)

    def retain_unknown(self, request_id):
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE');s=self._load(db)
            _require(request_id in s['reservations'] and s['reservations'][request_id]['status']!='finished', 'unknown reservation')
            # Do not re-add budget already authoritatively proven started.
            s['reservations'][request_id]['status']='unknown';self._save(db,s)

    def finish(self, request_id):
        """Caller must prove authoritative terminal; this is not health recovery."""
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self.finish_in_transaction(db,request_id)

    def finish_in_transaction(self, db, request_id):
        self.finish_existing_in_transaction(db, self.provider_key, request_id)

    @classmethod
    def finish_existing_in_transaction(cls, db, provider_key, request_id):
        """Existing-only finish seam for an already proof-checked atomic handoff.

        No constructor, nested transaction, schema creation or budget reset. The
        caller still supplies exact lifecycle/non-dispatch authority as before.
        """
        _require(db.in_transaction, 'caller must hold BEGIN IMMEDIATE')
        _require(isinstance(provider_key, str) and provider_key, 'provider required')
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        _require('jarvis_transport_gate' in tables, 'existing shared provider schema required')
        raw = db.execute('SELECT data FROM jarvis_transport_gate WHERE provider=?', (provider_key,)).fetchone()
        _require(raw is not None, 'existing shared provider row required')
        state = json.loads(raw[0])
        _require(request_id in state['reservations'], 'unknown reservation')
        state['pending'].pop(request_id, None); state['reservations'][request_id]['status'] = 'finished'
        if state['probe'] == request_id: state['probe'] = None
        db.execute('UPDATE jarvis_transport_gate SET data=? WHERE provider=?', (json.dumps(state), provider_key))

    def recover_terminal_in_transaction(self, db, proof, *, max_age=10.0):
        """Same-generation business success CAS, before atomic terminal finish.

        A failed CAS is normal: business settlement continues while a later
        provider failure keeps the breaker closed to admission.
        Never refunds tokens, changes cooldown, or releases another reservation.
        """
        _require(db.in_transaction, 'terminal recovery requires checkpoint transaction')
        _require(isinstance(proof, TerminalTransportRecoveryProof) and proof.web_items,
                 'complete terminal recovery proof required')
        _require(all(isinstance(v, str) and v for v in (proof.request_id, proof.hold_id,
            proof.thread_id, proof.turn_id, proof.model, proof.owner)) and proof.lane in ('base', 'task')
            and proof.provider == self.provider_key and type(proof.generation) is int
            and proof.generation >= 0, 'invalid terminal proof identity')
        _require(all(isinstance(v, str) and len(v) == 64 and all(c in '0123456789abcdef' for c in v)
            for v in (proof.owner_terminal_sha256, proof.native_scan_sha256, proof.checkpoint_sha256)),
            'terminal proof digests required')
        now = self.clock()
        _require(_number(proof.observed_at) and 0 <= now-proof.observed_at <= max_age, 'stale terminal proof')
        receipt_id = _hash((proof.request_id, proof.generation))
        old = db.execute('SELECT data FROM jarvis_transport_receipts WHERE provider=? AND kind=? AND receipt_id=?',
                         (self.provider_key, 'terminal_success', receipt_id)).fetchone()
        proof_data = asdict(proof)
        # Fresh collection time is not semantic evidence and cannot create a
        # different terminal operation. A later failure always remains intact.
        semantic = {k:v for k,v in proof_data.items() if k != 'observed_at'}
        proof_hash = _hash(semantic)
        if old:
            _require(json.loads(old[0])['proof_hash'] == proof_hash, 'terminal success receipt collision')
            return {'status': 'already_applied', 'receipt_id': receipt_id}
        state = self._load(db); row = state['reservations'].get(proof.request_id)
        _require(row and (row['owner'], row['lane']) == (proof.owner, proof.lane), 'terminal reservation owner mismatch')
        if (not state['open'] or state['probe'] != proof.request_id
                or not (type(row['generation']) is int and type(state['generation']) is int
                        and type(state['probe_generation']) is int and row['generation'] == proof.generation
                        == state['probe_generation'] == state['generation'])
                or row['status'] == 'finished'):
            return {'status': 'not_applied', 'reason': 'probe_or_generation_changed'}
        if now < state['until']:
            return {'status': 'not_applied', 'reason': 'cooldown_not_expired'}
        # A fully settled probe proves endpoint health independently of an older
        # unresolved delivery. Keep every pending cost/reservation unchanged; the
        # normal shared pending budget and occupancy still constrain admission.
        state.update(open=False, probe=None, failures=0, last_failure=None)
        self._save(db, state)
        db.execute('INSERT INTO jarvis_transport_receipts VALUES(?,?,?,?)',
            (self.provider_key, 'terminal_success', receipt_id,
             json.dumps({'proof_hash': proof_hash, 'proof': proof_data, 'applied_at': now})))
        return {'status': 'recovered', 'receipt_id': receipt_id}

    def record_provider_failure(self, event_id, request_id=None, retry_after_until=None, kind='provider_429'):
        """Only structured provider errors, not business errors or source HTTP text."""
        _require(isinstance(event_id,str) and event_id and kind in ('provider_429','provider_unavailable'), 'invalid provider failure')
        if retry_after_until is not None:_require(_number(retry_after_until), 'invalid Retry-After')
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE');s=self._load(db);now=self.clock()
            payload=dict(request_id=request_id,retry_after_until=retry_after_until,kind=kind)
            old=db.execute('SELECT data FROM jarvis_transport_receipts WHERE provider=? AND kind=? AND receipt_id=?',(self.provider_key,'failure',event_id)).fetchone()
            if old:
                _require(json.loads(old[0])==payload, 'failure receipt collision')
                return s
            s['generation']+=1;s['failures']+=1;s['last_failure']=now;s['open']=True
            delay=min(3600,self.policy['cooldown_seconds']*2**min(s['failures']-1,5))
            s['until']=max(s['until'],now+delay,retry_after_until or 0)
            s['tokens']=0.0;s['token_at']=now
            # A probe with unknown delivery remains assigned until terminal proof.
            # A different/late failure fences any old success via generation.
            db.execute('INSERT INTO jarvis_transport_receipts VALUES(?,?,?,?)',(self.provider_key,'failure',event_id,json.dumps(payload)))
            self._save(db,s);return s

    def recover(self, proof: TransportRecoveryProof, owner_guard, *, max_age=10.0):
        """Close only matching transport breaker while holding authoritative owner guard.

        owner_guard() yields current OwnerExecutionEvidence and must serialize with
        owner error/cancel/stop recording. Lock order: SQLite then owner guard; the owner recorder never acquires SQLite. A
        subsequent provider error reopens normally; a preceding one wins generation.
        """
        _require(isinstance(proof,TransportRecoveryProof) and proof.scan_complete and proof.web_items, 'incomplete recovery proof')
        receipt_id=_hash((proof.owner.request_id,proof.generation));proof_data=asdict(proof);proof_hash=_hash(proof_data)
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            with owner_guard() as current:
                s=self._load(db);now=self.clock()
                old=db.execute('SELECT data FROM jarvis_transport_receipts WHERE provider=? AND kind=? AND receipt_id=?',(self.provider_key,'success',receipt_id)).fetchone()
                if old:
                    _require(json.loads(old[0])['proof_hash']==proof_hash, 'success receipt collision')
                    return {'status':'already_applied','receipt_id':receipt_id}
                _require(isinstance(current,OwnerExecutionEvidence), 'missing owner authority')
                current.validate(now,max_age)
                _require(current.identity()==proof.owner.identity(), 'owner identity/error epoch changed')
                _require(0<=now-proof.observed_at<=max_age, 'stale native proof')
                rid=current.request_id;row=s['reservations'].get(rid)
                _require(row and (row['owner'],row['lane'],row['status'])==(current.owner,current.lane,'started'), 'not exact proven started reservation')
                _require(s['open'] and s['probe']==rid and s['generation']==s['probe_generation']==proof.generation, 'probe/failure generation changed')
                _require(now>=s['until'] and not s['pending'], 'cooldown or unknown starts remain')
                s.update(open=False,probe=None,failures=0,last_failure=None)
                self._save(db,s)
                db.execute('INSERT INTO jarvis_transport_receipts VALUES(?,?,?,?)',(self.provider_key,'success',receipt_id,json.dumps({'proof_hash':proof_hash,'proof':proof_data,'applied_at':now})))
                return {'status':'recovered','receipt_id':receipt_id,'business_completed':False,'occupancy_released':False}
