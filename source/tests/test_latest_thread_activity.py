"""Offline regression for activity after disconnect; never runs a native process."""
import asyncio
from dataclasses import replace
import json
from pathlib import Path
import queue
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import patch

from jarvis_codex_bridge import ExistingThreadBridge, JarvisCapabilityPort, ReceiptRoute, ResumeRequest, ThreadState, TurnState
from jarvis_codex_bridge.monitor import ThreadTerminalMonitor
from jarvis_control.service import JarvisControl
from jarvis_mcp.server import JarvisMcpServer
from jarvis_runtime import jarvis_heartbeat_service as heartbeat
from jarvis_runtime.jarvis_native_task_launcher import AppServerClient


def turn(ident='turn-8', status='inProgress'):
    return {'id': ident, 'status': status, 'error': None, 'items': [], 'itemsView': 'notLoaded'}


class LatestThreadActivityTests(unittest.TestCase):
    def test_public_monitor_observe_consumes_terminal_change_and_status_is_read_only(self):
        active = ThreadState('thread', 'idle', turns=(TurnState('turn', 'inProgress'),))
        completed = ThreadState('thread', 'idle', turns=(TurnState('turn', 'completed'),))
        states = iter((active, completed, completed))
        class NoResume:
            def resume_existing(self, *_): raise AssertionError('duplicate terminal resume dispatched')
        class HoldStatus:
            def hold_status(self, _): return {'status': 'running', 'terminal_confirmed': False}
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'monitor-state.json'
            monitor = ThreadTerminalMonitor(types.SimpleNamespace(read_thread=lambda _: next(states, completed)), path)
            control = JarvisControl(JarvisCapabilityPort(NoResume(), monitor), NoResume(), provisioner=HoldStatus())
            common = dict(monitor_id='monitor', observed_task_id='thread', receipt_task_id='receipt', source_ref='isolated')
            first = control.monitor(action='observe', request_id='observe-active', **common)
            consumed = control.monitor(action='observe', request_id='observe-terminal', **common)
            before_status = path.read_bytes()
            status = control.monitor(action='status', request_id='read-only-status', hold_id='known', source_ref='isolated')
            self.assertEqual(path.read_bytes(), before_status)
            later = control.monitor(action='terminal_resume', request_id='later-resume',
                                    resume_task_id='thread', prompt='continue', **common)
            self.assertEqual([first['status'], consumed['status'], later['status']],
                             ['baseline_active', 'terminal_changed', 'no_change'])
            self.assertEqual(status['status'], 'completed')
            for missing in ({'monitor_id': None}, {'observed_task_id': None},
                            {'receipt_task_id': None}, {'resume_task_id': None}, {'prompt': None}):
                with self.subTest(missing=missing):
                    args = dict(common, resume_task_id='thread', prompt='continue')
                    args.update(missing)
                    self.assertEqual(control.monitor(action='terminal_resume', request_id='missing-field', **args)
                                     ['status'], 'invalid_request')

    def native(self, pages=None, *, metadata_id='thread', native_status='idle', updated_at=8, legacy_turn_id='turn-5', legacy_turn_status='completed'):
        client = object.__new__(AppServerClient)
        client.config = types.SimpleNamespace(request_timeout_seconds=.2, turn_readback_timeout_seconds=.2)
        client.response_queue = queue.Queue(); client.stderr_lines = []; client._request_id = 0
        client._write_lock = threading.Lock(); client.before_turn_dispatch = None
        client.calls = []; client.closed = False
        client.start = lambda **kw: None
        client.close = lambda: setattr(client, 'closed', True)
        pages = iter(pages if pages is not None else [{'data': [turn()]}])
        class Sink:
            def write(self, raw):
                req = json.loads(raw); client.calls.append(req)
                if req['method'] == 'thread/read':
                    result = {'thread': {'id': metadata_id, 'status': {'type': native_status}, 'updatedAt': updated_at,
                              'turns': [{'id': legacy_turn_id, 'status': legacy_turn_status, 'items': []}]}}
                elif req['method'] == 'thread/turns/list':
                    result = next(pages)
                    if isinstance(result, Exception): raise result
                else: raise AssertionError('Forbidden RPC: ' + req['method'])
                client.response_queue.put({'id': req['id'], 'result': result})
            def flush(self): pass
        client.process = types.SimpleNamespace(stdin=Sink(), poll=lambda: None)
        return client

    def transport(self, client, owner=None):
        transport = object.__new__(heartbeat.StandardBridgeHeartbeatTransport)
        transport.launcher_config = client.config
        transport.bridge = types.SimpleNamespace(ThreadState=ThreadState, TurnState=TurnState)
        transport.execution_reader = lambda *_: owner
        return transport

    def read(self, client, owner=None):
        with patch.object(heartbeat, 'AppServerClient', lambda _: client):
            state = self.transport(client, owner).read_thread('thread')
        self.assertTrue(client.closed)
        return state

    def test_old_turn_5_terminal_new_turn_8_active_cannot_be_idle(self):
        owner = {'thread_id': 'thread', 'turn_id': 'turn-5', 'status': 'completed',
                 'source': 'hold_evidence', 'terminal_confirmed': True, 'hold_released': True}
        client = self.native([{'data': [turn(), turn('turn-7', 'completed')]},])
        state = self.read(client, owner)
        self.assertEqual(state.turns[-1].turn_id, 'turn-5')  # Legacy output remains intact.
        self.assertEqual(state.execution_status, 'completed')
        self.assertEqual(state.execution_evidence, owner)
        self.assertEqual(state.effective_status, 'inProgress')
        self.assertEqual(state.remote_activity['latest_turn_id'], 'turn-8')
        self.assertEqual(state.remote_activity['active_turn_ids'], ['turn-8'])
        self.assertFalse(state.remote_activity['no_active_turns'])
        self.assertTrue(state.remote_activity['scan_complete'])
        self.assertNotIn('hold_released', state.remote_activity)
        self.assertNotIn('items', state.remote_activity)

    def test_same_turn_status_change_is_fresh_and_repeated_query_is_no_change(self):
        active = self.read(self.native([{'data': [turn()]}], legacy_turn_id='turn-8', legacy_turn_status='inProgress'))
        completed = self.read(self.native([{'data': [turn(status='completed')]}], legacy_turn_id='turn-8'))
        states = iter([active, active, completed, completed])
        with tempfile.TemporaryDirectory() as temp:
            monitor = ThreadTerminalMonitor(types.SimpleNamespace(read_thread=lambda _: next(states)), Path(temp)/'baseline.json')
            route = ReceiptRoute('thread', 'receipt')
            receipts = [monitor.observe('monitor', route) for _ in range(4)]
        self.assertEqual([r.state for r in receipts], ['baseline_active', 'no_change', 'terminal_changed', 'no_change'])
        self.assertNotEqual(receipts[0].fingerprint, receipts[2].fingerprint)

    def test_pagination_incomplete_cannot_claim_no_active_or_authorize_preflight(self):
        for pages in ([{'data': [turn('old', 'completed')], 'nextCursor': 'repeat'},
                       {'data': [], 'nextCursor': 'repeat'}],
                      [{'data': [turn('old', 'completed')], 'nextCursor': 'next'}, RuntimeError('disconnected')]):
            with self.subTest(pages=pages):
                state = self.read(self.native(pages))
                self.assertFalse(state.remote_activity['scan_complete'])
                self.assertIsNone(state.remote_activity['no_active_turns'])
                self.assertEqual(state.effective_status, 'unknown')
                request = ResumeRequest('request', 'thread', 'prompt', 'source')
                self.assertEqual(ExistingThreadBridge._preflight(state, request)[0], 'requires_readback')

    def test_duplicate_conflicting_turns_cannot_claim_idle(self):
        state = self.read(self.native([{'data': [turn('same', 'completed')], 'nextCursor': 'next'},
                                      {'data': [turn('same', 'inProgress')]}]))
        self.assertFalse(state.remote_activity['scan_complete'])
        self.assertEqual(state.remote_activity['status'], 'unknown')
        self.assertIsNone(state.remote_activity['no_active_turns'])

    def test_duplicate_same_turn_is_not_counted_twice(self):
        state = self.read(self.native([{'data': [turn()], 'nextCursor': 'next'}, {'data': [turn()]}]))
        self.assertEqual(state.remote_activity['active_turn_ids'], ['turn-8'])
        self.assertEqual(state.remote_activity['active_turn_count'], 1)

    def test_active_on_earlier_page_remains_active_when_scan_incomplete(self):
        state = self.read(self.native([{'data': [turn()], 'nextCursor': 'repeat'},
                                      {'data': [], 'nextCursor': 'repeat'}]))
        self.assertEqual(state.effective_status, 'inProgress')
        self.assertFalse(state.remote_activity['scan_complete'])
        self.assertIsNone(state.remote_activity['active_turn_count'])
        self.assertFalse(state.remote_activity['no_active_turns'])
        request = ResumeRequest('request', 'thread', 'prompt', 'source')
        self.assertEqual(ExistingThreadBridge._preflight(state, request)[0], 'busy')

    def test_turn_reading_is_metadata_only_bounded_and_identity_checked(self):
        client = self.native([{'data': [turn('old', 'completed')], 'nextCursor': 'next'}, {'data': [turn()]}])
        state = self.read(client)
        self.assertEqual(state.remote_activity['status'], 'active')
        for req in client.calls:
            self.assertNotEqual(req['method'], 'thread/items/list')
            if req['method'] == 'thread/turns/list':
                self.assertEqual(req['params']['itemsView'], 'notLoaded')
                self.assertEqual(req['params']['sortDirection'], 'desc')
                self.assertLessEqual(req['params']['limit'], 50)
        self.assertTrue(state.remote_activity['observed_at'])
        self.assertEqual(state.remote_activity['updated_at'], 8)

    def test_public_read_preserves_owner_and_exposes_separate_activity(self):
        owner = {'thread_id': 'thread', 'turn_id': 'turn-5', 'status': 'completed', 'source': 'hold_terminal_result'}
        state = self.read(self.native(), owner)
        control = JarvisControl(None, ExistingThreadBridge(types.SimpleNamespace(read_thread=lambda _: state), None))
        server = JarvisMcpServer(control)
        got = asyncio.run(server.mcp._tool_manager.call_tool('jarvis_read',
              {'subject': 'thread', 'task_id': 'thread'}, None)).structured_content
        self.assertEqual(got['data']['remote_activity']['status'], 'active')
        self.assertEqual(got['data']['execution_status'], 'inProgress')
        self.assertEqual(got['data']['recorded_execution_status'], 'completed')
        self.assertEqual(got['data']['execution_source'], 'native_activity')
        self.assertEqual(got['data']['recorded_execution_source'], 'hold_terminal_result')
        self.assertEqual(got['data']['execution_evidence'], owner)

    def test_complete_idle_does_not_fabricate_delivery_or_owner_release(self):
        state = self.read(self.native([{'data': [dict(turn(status='interrupted'), completedAt=8)]}]))
        self.assertEqual(state.remote_activity['status'], 'idle')
        self.assertTrue(state.remote_activity['no_active_turns'])
        self.assertIsNone(state.execution_evidence)
        self.assertNotIn('delivery_completed', state.remote_activity)
        self.assertNotIn('hold_released', state.remote_activity)

    def test_newer_stopped_turn_does_not_reuse_old_owner_delivery_completion(self):
        owner = {'thread_id': 'thread', 'turn_id': 'turn-5', 'status': 'completed', 'source': 'hold_evidence'}
        state = self.read(self.native([{'data': [turn(status='completed')]}]), owner)
        self.assertEqual(state.remote_activity['status'], 'idle')
        self.assertTrue(state.remote_activity['no_active_turns'])
        self.assertEqual(state.execution_evidence, owner)
        self.assertEqual(state.effective_status, 'unknown')
        self.assertEqual(ExistingThreadBridge._preflight(state, ResumeRequest('r', 'thread', 'p', 's'))[0], 'requires_readback')

    def test_expired_scan_never_dispatches_and_returns_unknown(self):
        client = self.native()
        data = client.read_thread_activity('thread', deadline=time.monotonic()-1)
        self.assertEqual(client.calls, [])
        self.assertEqual(data['status'], 'unknown')
        self.assertFalse(data['scan_complete'])
        self.assertIsNone(data['no_active_turns'])

    def test_explicit_page_bound_and_active_output_bound(self):
        client = self.native([{'data': [turn('old', 'completed')], 'nextCursor': 'next'}])
        data = client.read_thread_activity('thread', deadline=time.monotonic()+1, max_pages=1)
        self.assertFalse(data['scan_complete'])
        self.assertIsNone(data['active_turn_count'])
        self.assertIn('bound', data['reason'])
        client = self.native([{'data': [turn(str(i)) for i in range(50)], 'nextCursor': 'next'},
                              {'data': [turn(str(i)) for i in range(50,75)]}])
        data = client.read_thread_activity('thread', deadline=time.monotonic()+1)
        self.assertTrue(data['scan_complete'])
        self.assertEqual(len(data['active_turn_ids']), 50)
        self.assertFalse(data['active_turn_ids_complete'])
        self.assertEqual(data['active_turn_count'], 75)

    def test_remote_identity_conflict_and_invalid_turn_shape_are_unknown(self):
        for client in (self.native(metadata_id='wrong'), self.native([{'data': [dict(turn(), items=[{'payload': 'forbidden'}])]}])):
            with self.subTest(client=client):
                data = client.read_thread_activity('thread', deadline=time.monotonic()+1)
                self.assertEqual(data['status'], 'unknown')
                self.assertFalse(data['scan_complete'])
                self.assertIsNone(data['no_active_turns'])

    def test_query_timestamp_and_native_update_do_not_create_dispatch_progress(self):
        first = self.read(self.native())
        repeated = replace(first, remote_activity={**first.remote_activity, 'observed_at': 'a later query'})
        updated = replace(first, remote_activity={**first.remote_activity, 'updated_at': 9})
        self.assertEqual(ThreadTerminalMonitor._fingerprint(first), ThreadTerminalMonitor._fingerprint(repeated))
        self.assertEqual(ThreadTerminalMonitor._fingerprint(first), ThreadTerminalMonitor._fingerprint(updated))
        self.assertEqual(updated.remote_activity['updated_at'], 9)

    def test_legacy_thread_state_without_activity_keeps_original_behavior(self):
        state = ThreadState('thread', 'idle', (TurnState('old', 'completed'),), execution_status='completed')
        self.assertEqual(state.effective_status, 'completed')
        self.assertIsNone(state.remote_activity)

    def test_same_stopped_turn_conflicting_status_does_not_reuse_old_delivery(self):
        state = self.read(self.native([{'data': [dict(turn(status='interrupted'), completedAt=8)]}], legacy_turn_id='turn-8'))
        self.assertTrue(state.remote_activity['no_active_turns'])
        self.assertEqual(state.remote_activity['latest_turn_status'], 'interrupted')
        self.assertEqual(state.execution_status, 'completed')
        self.assertEqual(state.effective_status, 'unknown')

    def test_changed_metadata_during_scan_cannot_claim_no_active(self):
        client = self.native([{'data': [turn(status='completed')]}])
        request = client.request
        count = 0
        def read(method, params, **kwargs):
            nonlocal count
            result = request(method, params, **kwargs)
            if method == 'thread/read':
                count += 1
                if count == 2:
                    result['thread']['updatedAt'] = 9
            return result
        client.request = read
        data = client.read_thread_activity('thread', deadline=time.monotonic()+1)
        self.assertFalse(data['scan_complete'])
        self.assertEqual(data['status'], 'unknown')
        self.assertIsNone(data['no_active_turns'])

    def test_native_active_without_matching_active_turn_has_unknown_exact_count(self):
        state = self.read(self.native([{'data': [turn(status='completed')]}], native_status='active'))
        self.assertEqual(state.remote_activity['status'], 'active')
        self.assertFalse(state.remote_activity['no_active_turns'])
        self.assertTrue(state.remote_activity['scan_complete'])
        self.assertIsNone(state.remote_activity['active_turn_count'])
        self.assertFalse(state.remote_activity['active_turn_ids_complete'])

    def test_empty_activity_cannot_inherit_legacy_delivery_completion(self):
        state = self.read(self.native([{'data': []}]))
        self.assertTrue(state.remote_activity['no_active_turns'])
        self.assertIsNone(state.remote_activity['latest_turn_id'])
        self.assertEqual(state.execution_status, 'completed')
        self.assertEqual(state.effective_status, 'unknown')
        self.assertEqual(ExistingThreadBridge._preflight(state, ResumeRequest('r', 'thread', 'p', 's'))[0], 'requires_readback')

    def test_completed_turn_updated_at_only_does_not_trigger_terminal_continuation(self):
        state = self.read(self.native([{'data': [turn(status='completed')]}], legacy_turn_id='turn-8'))
        updated = replace(state, remote_activity={**state.remote_activity, 'updated_at': 9})
        states = iter([state, updated, updated])
        with tempfile.TemporaryDirectory() as temp:
            monitor = ThreadTerminalMonitor(types.SimpleNamespace(read_thread=lambda _: next(states)), Path(temp)/'baseline.json')
            route = ReceiptRoute('thread', 'receipt')
            receipts = [monitor.observe('monitor', route) for _ in range(3)]
        self.assertEqual([r.state for r in receipts], ['baseline_terminal', 'no_change', 'no_change'])
        self.assertTrue(all(r.state != 'terminal_changed' for r in receipts))
        self.assertEqual(updated.remote_activity['updated_at'], 9)

    def test_terminal_baseline_survives_disconnect_without_repeating_completion(self):
        terminal = self.read(self.native([{'data': [turn(status='completed')]}], legacy_turn_id='turn-8'))
        unknown = replace(terminal, remote_activity={**terminal.remote_activity, 'status': 'unknown',
                         'scan_complete': False, 'no_active_turns': None})
        states = iter([terminal, unknown, terminal, terminal])
        with tempfile.TemporaryDirectory() as temp:
            monitor = ThreadTerminalMonitor(types.SimpleNamespace(read_thread=lambda _: next(states)), Path(temp)/'baseline.json')
            receipts = [monitor.observe('monitor', ReceiptRoute('thread', 'receipt')) for _ in range(4)]
        self.assertEqual(receipts[0].state, 'baseline_terminal')
        self.assertEqual(receipts[2].state, 'no_change')
        self.assertEqual(receipts[3].state, 'no_change')
        self.assertFalse(any(r.state == 'terminal_changed' for r in receipts))

    def test_initial_unknown_then_old_terminal_is_baseline_without_completion_event(self):
        terminal = self.read(self.native([{'data': [turn(status='completed')]}], legacy_turn_id='turn-8'))
        unknown = replace(terminal, remote_activity={**terminal.remote_activity, 'status': 'unknown',
                         'scan_complete': False, 'no_active_turns': None})
        states = iter([unknown, terminal, terminal])
        with tempfile.TemporaryDirectory() as temp:
            monitor = ThreadTerminalMonitor(types.SimpleNamespace(read_thread=lambda _: next(states)), Path(temp)/'baseline.json')
            receipts = [monitor.observe('monitor', ReceiptRoute('thread', 'receipt')) for _ in range(3)]
        self.assertEqual([r.state for r in receipts], ['baseline_active', 'baseline_terminal', 'no_change'])
        self.assertFalse(any(r.state == 'terminal_changed' for r in receipts))

    def test_fresh_remote_active_survives_unknown_then_completes_once(self):
        active = self.read(self.native())  # Legacy snapshot still points to completed turn-5.
        unknown = replace(active, remote_activity={**active.remote_activity, 'status': 'unknown',
                          'scan_complete': False, 'no_active_turns': None})
        completed = self.read(self.native([{'data': [turn(status='completed')]}], legacy_turn_id='turn-8'))
        states = iter([active, unknown, completed, completed])
        with tempfile.TemporaryDirectory() as temp:
            monitor = ThreadTerminalMonitor(types.SimpleNamespace(read_thread=lambda _: next(states)), Path(temp)/'baseline.json')
            receipts = [monitor.observe('monitor', ReceiptRoute('thread', 'receipt')) for _ in range(4)]
        self.assertEqual(receipts[0].state, 'baseline_active')
        self.assertEqual(receipts[2].state, 'terminal_changed')
        self.assertEqual(receipts[3].state, 'no_change')
        self.assertEqual(sum(r.state == 'terminal_changed' for r in receipts), 1)

    def test_old_v3_monitor_entry_rebaselines_safely_and_dedup_survives_restart(self):
        terminal = self.read(self.native([{'data': [turn(status='completed')]}], legacy_turn_id='turn-8'))
        unknown = replace(terminal, remote_activity={**terminal.remote_activity, 'status': 'unknown',
                         'scan_complete': False, 'no_active_turns': None})
        route = ReceiptRoute('thread', 'receipt')
        identity = json.dumps(['monitor', 'thread', 'receipt'], ensure_ascii=False, separators=(',', ':'))
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)/'baseline.json'
            path.write_text(json.dumps({'version': 3, 'monitors': {
                identity: {'fingerprint': 'old-v3-before-activity-projection', 'event': 'terminal_changed'},
                'unrelated': {'fingerprint': 'untouched', 'event': 'baseline_active'}}}))
            def observe(state):
                # A fresh instance each time exercises only persisted migration evidence.
                return ThreadTerminalMonitor(types.SimpleNamespace(read_thread=lambda _: state), path).observe('monitor', route)
            receipts = [observe(terminal), observe(unknown), observe(terminal), observe(terminal)]
            saved = json.loads(path.read_text())
        self.assertEqual(receipts[0].state, 'baseline_terminal')
        self.assertEqual(receipts[2].state, 'no_change')
        self.assertEqual(receipts[3].state, 'no_change')
        self.assertFalse(any(r.state == 'terminal_changed' for r in receipts))
        self.assertEqual(saved['version'], 3)
        self.assertTrue(saved['monitors'][identity]['terminal_key'])
        self.assertIsNone(saved['monitors'][identity]['active_turn_id'])
        self.assertEqual(saved['monitors']['unrelated'], {'fingerprint': 'untouched', 'event': 'baseline_active'})

    def test_interrupted_observer_with_null_completion_does_not_prove_no_active_turn(self):
        # Actual recovery boundary: an independent observer labels an owned live
        # remote turn interrupted, while completedAt is null. Do not take over.
        state = self.read(self.native([{'data': [dict(turn(status='interrupted'), completedAt=None)]}]))
        self.assertTrue(state.remote_activity['scan_complete'])
        self.assertEqual(state.remote_activity['latest_turn_status'], 'interrupted')
        self.assertIsNone(state.remote_activity['latest_turn_completed_at'])
        self.assertEqual(state.remote_activity['status'], 'unknown')
        self.assertIsNone(state.remote_activity['no_active_turns'])
        self.assertIsNone(state.remote_activity['active_turn_count'])
        self.assertFalse(state.remote_activity['active_turn_ids_complete'])
        self.assertEqual(state.effective_status, 'unknown')
        self.assertEqual(ExistingThreadBridge._preflight(state, ResumeRequest('r', 'thread', 'p', 's'))[0], 'requires_readback')
        self.assertNotIn('hold_released', state.remote_activity)

    def test_native_activity_unknown_cannot_claim_idle_from_old_terminal(self):
        state = self.read(self.native([{'data': [turn(status='completed')]}], native_status='notLoaded'))
        self.assertEqual(state.remote_activity['status'], 'unknown')
        self.assertIsNone(state.remote_activity['no_active_turns'])
        self.assertEqual(state.effective_status, 'unknown')


if __name__ == '__main__': unittest.main()
