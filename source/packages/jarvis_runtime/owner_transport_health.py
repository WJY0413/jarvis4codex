"""Exact owning-client event receipts, separate from acceptance and settlement.

No observer client may instantiate this recorder as proof of existing execution.
It does not change Host lifecycle, native requests, or terminal interpretation.
"""
from __future__ import annotations
from collections import deque
from contextlib import contextmanager
from dataclasses import asdict
import json
import os
from pathlib import Path
import threading
import time
import uuid

from jarvis_runtime.native_error_origin import notification_error_facts, error_fact
from jarvis_runtime.cancellation import report_guard, cancellation_path, StateGateBusy
from jarvis_runtime.shared_transport_recovery import OwnerExecutionEvidence, RecoveryRejected, _require, _structured_error


# Only protocol-known, content-only deltas may avoid the critical fence.
_PURE_DELTA_METHODS = frozenset(('item/agentMessage/delta', 'item/reasoning/textDelta',
    'item/reasoning/summaryTextDelta', 'item/plan/delta'))
_KNOWN_METHODS = _PURE_DELTA_METHODS | frozenset(('error', 'turn/started', 'turn/completed',
    'item/started', 'item/completed', 'thread/tokenUsage/updated', 'turn/diff/updated',
    'item/reasoning/summaryPartAdded', 'thread/status/changed', 'account/rateLimits/updated'))


_REGISTRY = {}
_REGISTRY_LOCK = threading.RLock()


@contextmanager
def registered_owner_guard(*, request_id, thread_id, turn_id, host_instance_id, owner, lane):
    """Exact same-process owner registry, never reconstructed from an ACK/file flag."""
    key=(request_id,thread_id,turn_id,host_instance_id)
    with _REGISTRY_LOCK:
        recorder=_REGISTRY.get(key)
    _require(recorder is not None, 'exact live owner recorder unavailable')
    with recorder._lock:
        _require(not recorder.disabled and recorder.is_connected(), 'owner stream unavailable')
        recorder._drain_events(strict=True)
        with recorder._events_lock:
            epoch=recorder._critical_epoch
            _require(epoch==recorder._applied_critical_epoch, 'owner events pending; retry fresh observation')
        with owner_transport_health_guard(recorder.root,request_id=request_id,thread_id=thread_id,
                 turn_id=turn_id,model=recorder._state['model'],host_instance_id=host_instance_id,owner=owner,lane=lane) as evidence:
            # This is a fresh read under the same lock used by the live owner reader.
            from dataclasses import replace
            yield replace(evidence,observed_at=recorder.clock())
            _require(not recorder.disabled and recorder.is_connected(), 'owner evidence invalidated during transaction')
            with recorder._events_lock:
                changed=recorder._critical_epoch!=epoch
            if changed:
                # Persist after the file guard exits on the next observation. Drain
                # now so errors are latched before any caller can obtain evidence.
                recorder._drain_events(strict=False)
                raise RecoveryRejected('owner events changed during observation; retry fresh evidence')


class OwnerTransportHealthRecorder:
    def __init__(self, hold_root, request_id, *, clock=time.time, is_connected=lambda: False):
        self.root=Path(hold_root);self.request_id=request_id;self.clock=clock
        self._lock=threading.RLock();self._state=None
        # This separate lock protects only bounded in-memory operations; it is
        # never held across file I/O, SQLite, callbacks, or a yielded guard.
        self._events_lock=threading.Lock();self._pending=deque();self._pending_limit=512
        self._arrival_epoch=0;self._applied_epoch=0;self._dirty=False
        self._critical_epoch=0;self._applied_critical_epoch=0;self._bound_scope=None
        self.disabled=False;self.is_connected=is_connected

    def bind(self, thread_id, turn_id, runtime_context):
        """Bind owner native dispatch result/model; binding alone is never active."""
        with self._lock:
            _require(runtime_context.get('thread_id')==thread_id and runtime_context.get('model'), 'missing exact runtime model')
            _require(runtime_context.get('constraints_verified') is True, 'runtime constraints not verified')
            request=json.loads((self.root/'request.json').read_text())
            host=json.loads((self.root/'.user-host-claim/owner.json').read_text())
            _require(request['request_id']==host['request_id']==self.request_id, 'owner request mismatch')
            _require(runtime_context['model']==request.get('model'), 'requested/native model mismatch')
            self._state=dict(schema='jarvis-owner-transport-health/v1',request_id=self.request_id,
                thread_id=thread_id,turn_id=turn_id,model=runtime_context['model'],host_instance_id=host['host_instance_id'],
                started_at=self.clock(),observed_at=self.clock(),failure_epoch=0,status='unknown',
                provider_error=False,cancelled=False,stopping=False,witness_source='owner_native_event_stream',
                progress_at=None,progress_sequence=0,tool_error=False,unknown_error=False,error_origins=[])
            with self._events_lock:
                self._bound_scope=(thread_id,turn_id)
            self._dirty=True;self._drain_events(strict=True)
            with _REGISTRY_LOCK:
                _REGISTRY[(self.request_id,thread_id,turn_id,host['host_instance_id'])]=self

    def observe(self, notification):
        """Queue compact native facts; ordinary observation contention is transient.

        The reader never waits on the owner guard or filesystem lock. Overflow or
        an actual I/O/protocol failure remains fail-closed; no event is silently
        dropped to claim health. No message text/tool content is queued.
        """
        if self.disabled:return
        malformed=not isinstance(notification,dict)
        notification=notification if isinstance(notification,dict) else {}
        params=notification.get('params',{})
        malformed=malformed or not isinstance(params,dict)
        params=params if isinstance(params,dict) else {}
        turn=params.get('turn',{});item=params.get('item',{})
        malformed=malformed or not isinstance(turn,dict) or not isinstance(item,dict)
        turn=turn if isinstance(turn,dict) else {};item=item if isinstance(item,dict) else {}
        method=notification.get('method')
        malformed=malformed or not isinstance(method,str)
        method=method if isinstance(method,str) else None
        event=dict(method=method,thread_id=params.get('threadId'),
                   turn_id=params.get('turnId') or turn.get('id'),at=self.clock(),
                   errors=notification_error_facts(method,params,turn,item),
                   status=turn.get('status'))
        if malformed or event['method'] not in _KNOWN_METHODS:
            event['errors'].append(error_fact('notification.unknown',None))
        with self._events_lock:
            # Adjacent exact-scope delta notifications carry progress only. Keep
            # their latest time/count and advance every arrival epoch. Only
            # critical events invalidate a held transport guard. Never coalesce errors or lifecycle events.
            method=event['method'] or ''
            delta=(method in _PURE_DELTA_METHODS and not event['errors'] and event['status'] is None
                   and self._bound_scope is not None
                   and (event['thread_id'],event['turn_id'])==self._bound_scope)
            if not delta:self._critical_epoch+=1
            event['critical_epoch']=self._critical_epoch
            previous=self._pending[-1][1] if self._pending else None
            coalesce=(delta and previous is not None and not previous['errors']
                      and previous['method']==event['method']
                      and event['thread_id'] is not None and event['turn_id'] is not None
                      and (previous['thread_id'],previous['turn_id'])==(event['thread_id'],event['turn_id']))
            if not coalesce and len(self._pending)>=self._pending_limit:
                self._critical_epoch+=1
                self.disabled=True
                return
            self._arrival_epoch+=1
            if coalesce:
                event['progress_count']=previous.get('progress_count',1)+1
                self._pending[-1]=(self._arrival_epoch,event)
            else:
                self._pending.append((self._arrival_epoch,event))
        if not self._lock.acquire(blocking=False):return
        try:
            if self._state is not None:self._drain_events(strict=False)
        finally:
            self._lock.release()

    def _drain_events(self, *, strict):
        """Caller holds _lock. Drain one bounded batch; never drop newer arrivals."""
        if self._state is None:return
        with self._events_lock:
            batch=list(self._pending);self._pending.clear()
        for epoch,event in batch:
            self._dirty=self._apply(event) or self._dirty
            self._applied_epoch=epoch
            self._applied_critical_epoch=event['critical_epoch']
        if self._dirty:
            try:
                self._persist()
                self._dirty=False
            except StateGateBusy:
                # A contended audit file is not a broken owning event stream.
                # Keep dirty state; registry guards reject until it can be flushed.
                if strict:raise RecoveryRejected('owner receipt busy; retry fresh observation')

    def _apply(self,event):
        s=self._state
        is_error=bool(event['errors'])
        if is_error:
            # Missing error scope is uncertain, never silently counted as healthy.
            if event['thread_id'] not in (None,s['thread_id']) or event['turn_id'] not in (None,s['turn_id']):return False
        elif event['thread_id']!=s['thread_id'] or event['turn_id']!=s['turn_id']:
            return False
        s['observed_at']=event['at']
        method=event['method'] or ''
        exact_scope=event['thread_id']==s['thread_id'] and event['turn_id']==s['turn_id']
        if exact_scope and (method in ('turn/started','turn/completed','item/started','item/completed')
                or method.startswith('item/') and 'delta' in method.lower()):
            s['progress_at']=event['at'];s['progress_sequence']+=event.get('progress_count',1)
        if is_error:
            s['failure_epoch']+=1
            for fact in event['errors']:
                origin=fact['origin']
                s['provider_error']=s['provider_error'] or origin=='native_request'
                s['tool_error']=s['tool_error'] or origin in ('web_tool','native_tool')
                s['unknown_error']=s['unknown_error'] or origin=='unknown'
                # Bounded distinct provenance plus first/last epoch, no raw content.
                records=s['error_origins']
                match=next((r for r in records if all(r.get(k)==v for k,v in fact.items())),None)
                if match is not None:
                    match['last_epoch']=s['failure_epoch'];match['count']+=1
                elif len(records)<32:
                    records.append(dict(fact,first_epoch=s['failure_epoch'],last_epoch=s['failure_epoch'],count=1))
                else:
                    s['unknown_error']=True
        # Execution activity and transport health are different evidence. An error
        # never erases prior activity or clears a latch; only terminal events end it.
        if exact_scope and event['method']=='turn/completed':
            s['status']=event['status'] if event['status'] in ('completed','failed','interrupted') else 'unknown'
        elif exact_scope and event['method'] in ('turn/started','item/started','item/completed') and s['status'] not in ('completed','failed','interrupted'):
            s['status']='inProgress';s['started_at']=min(s['started_at'],event['at'])
        return True

    def stop(self, *, cancelled=False):
        # Called from native reader EOF as well as the Hold. Never delay delivery.
        with self._events_lock:
            self._critical_epoch+=1
            self.disabled=True
        if not self._lock.acquire(blocking=False):return
        try:
            if self._state is not None:
                self._drain_events(strict=False)
                self._state.update(stopping=True,cancelled=cancelled,observed_at=self.clock())
                self._persist()
        finally:
            with _REGISTRY_LOCK:
                for key,value in list(_REGISTRY.items()):
                    if value is self:_REGISTRY.pop(key,None)
            self._lock.release()

    def invalidate(self):
        """A failed optional recorder never blocks native execution delivery."""
        with self._events_lock:
            self._critical_epoch+=1
            self.disabled=True
        if not self._lock.acquire(blocking=False):return
        try:
            if self._state is not None:
                self._state.update(status='unknown',stopping=True,observed_at=self.clock())
                self._persist()
        finally:
            self._lock.release()

    def _persist(self):
        with report_guard(self.root/'.transport-health.lock',timeout=0):
            current=json.loads((self.root/'request.json').read_text())
            if current.get('request_id')!=self.request_id:return
            target=self.root/'owner-transport-health.json'
            temp=self.root/('.transport-health-'+uuid.uuid4().hex+'.tmp')
            try:
                temp.write_text(json.dumps(self._state),encoding='utf-8');os.replace(temp,target)
            finally:
                temp.unlink(missing_ok=True)


@contextmanager
def owner_transport_health_guard(hold_root, *, request_id, thread_id, turn_id, model,
                                 host_instance_id, owner, lane):
    """Cross-process guard serializes with owner event/cancel/stop receipt updates.

    Identity comes from the controller's persisted admission, never latest-thread
    selection. This only supplies transport authority, never lifecycle release.
    """
    root=Path(hold_root)
    with report_guard(root/'.transport-health.lock',timeout=30):
        request=json.loads((root/'request.json').read_text())
        host=json.loads((root/'.user-host-claim/owner.json').read_text())
        data=json.loads((root/'owner-transport-health.json').read_text())
        _require(data.get('schema')=='jarvis-owner-transport-health/v1', 'invalid owner event schema')
        expected=dict(request_id=request_id,thread_id=thread_id,turn_id=turn_id,model=model,host_instance_id=host_instance_id)
        _require(all(data.get(k)==v for k,v in expected.items()), 'owner event identity changed')
        _require(request.get('request_id')==host.get('request_id')==request_id and request.get('model')==model
                 and host.get('host_instance_id')==host_instance_id, 'current owner changed')
        _require(request.get('mode')!='recover', 'observer recovery cannot prove owner activity')
        _require(not request.get('host_stop') and not request.get('stop_requested')
                 and not request.get('resume_pause') and cancellation_path(root,request) is None, 'owner cancelled/paused/stopping')
        fields={k:data[k] for k in OwnerExecutionEvidence.__dataclass_fields__ if k not in ('owner','lane') and k in data}
        yield OwnerExecutionEvidence(**fields,owner=owner,lane=lane)
