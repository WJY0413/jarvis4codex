"""Offline real Jarvis MCP/facade/bridge/RPC chain, never a formal Jarvistest.

Only process admission/closure and pipe delivery are fake. Native requests cross
AppServerClient.request and _write, and valid wire fixtures use alpha.7 schemas.
No model, Host, production configuration, database, or network is touched.
"""
import asyncio
import gc
import json
from pathlib import Path
import queue
import threading
import time
import types
import unittest
import weakref
from unittest.mock import patch

import jsonschema
from jarvis_codex_bridge import ExistingThreadBridge, ThreadState, TurnState
from jarvis_control.service import JarvisControl
from jarvis_mcp.server import JarvisMcpServer
from jarvis_runtime import jarvis_heartbeat_service as heartbeat
from jarvis_runtime.jarvis_native_task_launcher import AppServerClient, NativeTaskError

SCHEMA_ROOT = Path(__file__).resolve().parents[1] / 'evidence/alpha7-schema'
SCHEMAS = {name: json.loads((SCHEMA_ROOT / (name + '.json')).read_text()) for name in
           ('ThreadReadParams', 'ThreadReadResponse', 'ThreadTurnsListParams',
            'ThreadTurnsListResponse', 'ThreadItemsListParams', 'ThreadItemsListResponse')}
PREFIX = {'thread/read': 'ThreadRead', 'thread/turns/list': 'ThreadTurnsList',
          'thread/items/list': 'ThreadItemsList'}


def native_thread(thread='thread-exact'):
    return {'id': thread, 'cliVersion': '0.159.0-alpha.7', 'createdAt': 0,
            'cwd': '/test', 'ephemeral': False, 'modelProvider': 'openai', 'preview': '',
            'projectId': None, 'sessionId': 'session', 'source': 'cli',
            'status': {'type': 'idle'}, 'turns': [], 'updatedAt': 0}


def turn(turn_id='turn-exact', status='completed', error=None):
    return {'id': turn_id, 'status': status, 'error': error, 'items': [], 'itemsView': 'notLoaded'}


def entry(text='  {"ok":true}\n', phase='final_answer', turn_id='turn-exact', item_id='item-final'):
    return {'turnId': turn_id,
            'item': {'id': item_id, 'type': 'agentMessage', 'phase': phase, 'text': text}}


class ExactTurnReadTests(unittest.TestCase):
    def chain(self, *, turns=None, items=None, metadata=None, evidence=None, validate=False, hook=None):
        native = object.__new__(AppServerClient)
        native.config = types.SimpleNamespace(request_timeout_seconds=.2, turn_readback_timeout_seconds=.2)
        native.response_queue = queue.Queue(); native.stderr_lines = []; native._request_id = 0
        native._write_lock = threading.Lock(); native.before_turn_dispatch = None
        native.calls = []; native.deadlines = []; native.started = []; native.closed = False
        native.start = lambda **kw: native.started.append(kw.get('deadline'))
        native.close = lambda: setattr(native, 'closed', True)
        turn_pages = iter(turns if turns is not None else [{'data': [turn()]}])
        item_pages = iter(items if items is not None else [{'data': [entry()]}])
        outer = self
        class ProtocolSink:
            def write(self, raw):
                request = json.loads(raw); native.calls.append(request)
                method = request['method']; params = request['params']
                if hook is not None:
                    hook(method, native)
                if validate:
                    jsonschema.validate(params, SCHEMAS[PREFIX[method] + 'Params'])
                if method == 'thread/read':
                    outer.assertFalse(params['includeTurns'])
                    result = metadata if metadata is not None else {'thread': native_thread()}
                elif method == 'thread/turns/list':
                    outer.assertEqual(params['itemsView'], 'notLoaded')
                    result = next(turn_pages)
                elif method == 'thread/items/list':
                    outer.assertEqual(params['turnId'], 'turn-exact')
                    result = next(item_pages)
                else:
                    raise AssertionError('Undeclared native operation: ' + method)
                if callable(result):
                    result = result()
                if validate:
                    jsonschema.validate(result, SCHEMAS[PREFIX[method] + 'Response'])
                if 'rpc_error' in result:
                    native.response_queue.put({'id': request['id'], 'error': result['rpc_error']})
                else:
                    native.response_queue.put({'id': request['id'], 'result': result})
            def flush(self): pass
        native.process = types.SimpleNamespace(stdin=ProtocolSink(), poll=lambda: None)
        real_request = native.request
        def request(method, params, **kw):
            native.deadlines.append(kw.get('deadline'))
            return real_request(method, params, **kw)
        native.request = request
        transport = object.__new__(heartbeat.StandardBridgeHeartbeatTransport)
        transport.launcher_config = native.config
        transport.execution_reader = evidence
        transport.bridge = types.SimpleNamespace(ThreadState=ThreadState, TurnState=TurnState)
        bridge = ExistingThreadBridge(transport, None)
        control = JarvisControl(None, bridge)
        server = JarvisMcpServer(control)
        return server, native, transport

    def call(self, server, native, **kw):
        with patch.object(heartbeat, 'AppServerClient', lambda config: native):
            result = asyncio.run(server.mcp._tool_manager.call_tool('jarvis_read',
                {'subject': 'thread', 'task_id': 'thread-exact', 'turn_id': 'turn-exact', **kw}, None))
        self.assertTrue(native.closed)
        return result.structured_content

    def test_real_mcp_rpc_chain_and_alpha7_schema_preserve_raw_final(self):
        server, native, _ = self.chain(validate=True)
        got = self.call(server, native)
        self.assertEqual(got['status'], 'completed')
        self.assertEqual(got['target_thread_id'], 'thread-exact')
        self.assertEqual(got['turn_id'], 'turn-exact')
        selected = got['data']['turns'][0]
        self.assertEqual(selected['status'], 'completed')
        self.assertEqual(selected['items'][0]['text'], '  {"ok":true}\n')
        self.assertEqual(selected['items_projection'], 'final_answer')
        self.assertFalse(selected['items_complete']); self.assertTrue(selected['items_scan_complete'])
        self.assertTrue(got['readback']['final_answer_nonempty'])
        self.assertEqual([x['method'] for x in native.calls],
                         ['thread/read', 'thread/turns/list', 'thread/items/list'])
        self.assertEqual(len(set(native.deadlines + native.started)), 1)

    def test_exact_turn_is_not_newest_turn(self):
        server, native, _ = self.chain(turns=[{'data': [turn('newer', 'inProgress')], 'nextCursor': 'turn-2'},
                                              {'data': [turn()]}])
        got = self.call(server, native)
        self.assertEqual(got['data']['turns'][0]['turn_id'], 'turn-exact')
        self.assertEqual(got['data']['turns'][0]['status'], 'completed')
        self.assertEqual(native.calls[2]['params']['cursor'], 'turn-2')

    def test_empty_turn_page_with_cursor_continues(self):
        server, native, _ = self.chain(turns=[{'data': [], 'nextCursor': 'turn-2'}, {'data': [turn()]}])
        self.assertEqual(self.call(server, native)['status'], 'completed')

    def test_missing_turn_fails_without_items_or_fallback(self):
        server, native, _ = self.chain(turns=[{'data': [turn('wrong')]}])
        got = self.call(server, native)
        self.assertEqual(got['status'], 'failed'); self.assertIsNone(got['data'])
        self.assertIn('not found', got['reason'])
        self.assertEqual(len(native.calls), 2)

    def test_wrong_thread_fails_before_paging(self):
        server, native, _ = self.chain(metadata={'thread': native_thread('wrong')})
        got = self.call(server, native)
        self.assertEqual(got['status'], 'failed'); self.assertIn('identity mismatch', got['reason'])
        self.assertEqual(len(native.calls), 1)

    def test_wrong_item_turn_fails_without_partial_counts(self):
        server, native, _ = self.chain(items=[{'data': [entry(turn_id='wrong')]}])
        got = self.call(server, native)
        self.assertEqual(got['status'], 'failed'); self.assertIsNone(got['data'])

    def test_final_projection_counts_all_pages_including_empty_pages(self):
        web = {'turnId': 'turn-exact', 'item': {'id': 'web-1', 'type': 'webSearch', 'query': 'public', 'results': []}}
        server, native, _ = self.chain(items=[{'data': [entry(), web], 'nextCursor': 'items-2'},
            {'data': [], 'nextCursor': 'items-3'},
            {'data': [entry('old', item_id='old-final'), entry('comment', 'commentary', item_id='comment')]}], validate=True)
        selected = self.call(server, native)['data']['turns'][0]
        self.assertEqual(selected['item_count'], 4); self.assertEqual(selected['web_search_count'], 1)
        self.assertEqual(len(selected['items']), 1); self.assertEqual(selected['items'][0]['id'], 'item-final')
        self.assertEqual(len(native.calls), 5)

    def test_commentary_never_becomes_strict_final(self):
        server, native, _ = self.chain(items=[{'data': [entry('comment', 'commentary')]}])
        got = self.call(server, native)
        selected = got['data']['turns'][0]
        self.assertEqual(selected['items'], []); self.assertFalse(selected['final_answer_present'])
        self.assertTrue(selected['items_scan_complete']); self.assertFalse(got['readback']['final_answer_nonempty'])
        self.assertEqual(selected['status'], 'completed')

    def test_empty_items_do_not_invent_final(self):
        server, native, _ = self.chain(items=[{'data': []}])
        selected = self.call(server, native)['data']['turns'][0]
        self.assertEqual(selected['item_count'], 0); self.assertFalse(selected['final_answer_present'])

    def test_blank_newest_final_does_not_fall_back_to_older_final(self):
        server, native, _ = self.chain(items=[{'data': [entry(' \n'), entry('old', item_id='older')]}])
        got = self.call(server, native)
        self.assertEqual(got['data']['turns'][0]['items'][0]['text'], ' \n')
        self.assertFalse(got['readback']['final_answer_nonempty'])

    def test_running_turn_with_final_text_is_still_running(self):
        server, native, _ = self.chain(turns=[{'data': [turn(status='inProgress')]}])
        got = self.call(server, native)
        self.assertEqual(got['data']['turns'][0]['status'], 'inProgress')
        self.assertFalse(got['readback']['terminal'])

    def test_native_failed_preserves_error_object_and_legacy_error_string(self):
        error = {'message': 'network_error', 'additionalDetails': 'upstream gone',
                 'codexErrorInfo': {'responseStreamDisconnected': {'httpStatusCode': 502}}}
        server, native, _ = self.chain(turns=[{'data': [turn(status='failed', error=error)]}], validate=True)
        got = self.call(server, native)
        selected = got['data']['turns'][0]
        self.assertEqual(selected['status'], 'failed'); self.assertEqual(selected['native_error'], error)
        self.assertEqual(selected['error'], str(error)); self.assertTrue(got['readback']['native_terminal'])

    def test_unconfirmed_interrupted_remains_unknown_execution(self):
        server, native, _ = self.chain(turns=[{'data': [turn(status='interrupted')]}])
        got = self.call(server, native)
        self.assertEqual(got['data']['turns'][0]['status'], 'interrupted')
        self.assertEqual(got['data']['execution_status'], 'unknown'); self.assertFalse(got['readback']['terminal'])

    def test_exact_owner_terminal_evidence_confirms_matching_interruption(self):
        evidence = {'thread_id': 'thread-exact', 'turn_id': 'turn-exact', 'status': 'interrupted',
                    'source': 'hold_terminal_result', 'terminal_confirmed': True}
        calls = []
        server, native, _ = self.chain(turns=[{'data': [turn(status='interrupted')]}],
            evidence=lambda t, u: calls.append((t, u)) or evidence)
        got = self.call(server, native)
        self.assertEqual(calls, [('thread-exact', 'turn-exact')]); self.assertTrue(got['readback']['terminal'])
        self.assertEqual(got['data']['execution_evidence'], evidence)

    def test_stale_owner_completed_cannot_override_fresh_running(self):
        evidence = {'thread_id': 'thread-exact', 'turn_id': 'turn-exact', 'status': 'completed',
                    'source': 'hold_terminal_result', 'terminal_confirmed': True}
        server, native, _ = self.chain(turns=[{'data': [turn(status='inProgress')]}], evidence=lambda *a: evidence)
        got = self.call(server, native)
        self.assertEqual(got['data']['turns'][0]['status'], 'inProgress')
        self.assertEqual(got['data']['execution_status'], 'unknown'); self.assertFalse(got['readback']['terminal'])

    def test_owner_wrong_turn_fails_closed(self):
        server, native, _ = self.chain(evidence=lambda *a: {'thread_id': 'thread-exact', 'turn_id': 'wrong', 'status': 'completed'})
        got = self.call(server, native)
        self.assertEqual(got['status'], 'failed'); self.assertIsNone(got['data'])
        self.assertIn('owner evidence identity mismatch', got['reason'])

    def test_owner_reader_error_is_explicit_unknown_not_native_rewrite(self):
        def broken(*a): raise OSError('cannot read receipt')
        server, native, _ = self.chain(evidence=broken)
        got = self.call(server, native)
        self.assertEqual(got['data']['turns'][0]['status'], 'completed')
        self.assertEqual(got['data']['execution_status'], 'unknown'); self.assertFalse(got['readback']['terminal'])
        self.assertIn('cannot read receipt', got['data']['execution_evidence']['reason'])

    def test_rpc_error_does_not_fall_back_to_full_history(self):
        server, native, _ = self.chain(items=[{'rpc_error': {'code': -32601, 'message': 'unsupported'}}])
        got = self.call(server, native)
        self.assertEqual(got['status'], 'failed'); self.assertIsNone(got['data'])
        self.assertEqual(len(native.calls), 3)

    def test_invalid_and_repeated_cursors_fail_closed_in_both_streams(self):
        for stream in ['turns', 'items']:
            for cursor in ['', 42, 'repeat']:
                with self.subTest(stream=stream, cursor=cursor):
                    pages = [{'data': [], 'nextCursor': cursor}]
                    if cursor == 'repeat': pages.append({'data': [], 'nextCursor': cursor})
                    server, native, _ = self.chain(**{stream: pages})
                    got = self.call(server, native)
                    self.assertEqual(got['status'], 'failed'); self.assertIsNone(got['data'])
                    self.assertIn('cursor', got['reason'])

    def test_invalid_protocol_shapes_fail_closed(self):
        bad_turns = [{'data': {}}, {'data': [dict(turn(), itemsView='summary')]},
                     {'data': [dict(turn(), status='unknown')]}, {'data': [None]},
                     {'data': [turn(), turn()]}, {'data': [dict(turn(), error={'message': 'conflict'})]}]
        bad_items = [{'data': {}}, {'data': [{'turnId': 'turn-exact', 'item': None}]},
                     {'data': [entry(text=42)]}, {'data': [dict(entry(), turnId=None)]}]
        for stream, pages in [('turns', bad_turns), ('items', bad_items)]:
            for page in pages:
                with self.subTest(stream=stream, page=page):
                    server, native, _ = self.chain(**{stream: [page]})
                    got = self.call(server, native)
                    self.assertEqual(got['status'], 'failed'); self.assertIsNone(got['data'])

    def test_expired_exact_read_never_dispatches(self):
        server, native, _ = self.chain()
        with self.assertRaisesRegex(NativeTaskError, 'deadline exhausted'):
            native.read_exact_turn('thread-exact', 'turn-exact', deadline=time.monotonic() - 1)
        self.assertEqual(native.calls, [])

    def test_partial_scan_timeout_discards_final_and_counts(self):
        def stall(method, native):
            if method == 'thread/items/list' and len(native.calls) == 4: time.sleep(.03)
        server, native, _ = self.chain(items=[{'data': [entry()], 'nextCursor': 'second'}, {'data': []}], hook=stall)
        native.config.turn_readback_timeout_seconds = .015
        got = self.call(server, native)
        self.assertEqual(got['status'], 'failed'); self.assertIsNone(got['data'])
        self.assertRegex(got['reason'], 'deadline exhausted|timed out'); self.assertFalse(got['readback']['verified'])

    def test_large_tool_pages_released_before_next_rpc_and_not_returned(self):
        refs = []
        class LargeItem(dict): pass
        def page(index):
            item = LargeItem(id='large-'+str(index), type='mcpToolCall', server='test', tool='test',
                             status='completed', arguments={'large': 'x' * 4_000_000})
            refs.append(weakref.ref(item))
            return {'data': [{'turnId': 'turn-exact', 'item': item}], 'nextCursor': str(index+1)}
        def hook(method, native):
            if method == 'thread/items/list' and refs:
                gc.collect(); self.assertIsNone(refs[-1]())
        server, native, _ = self.chain(items=[lambda: page(1), lambda: page(2), {'data': [entry()]}], hook=hook)
        got = self.call(server, native)
        self.assertEqual(got['data']['turns'][0]['item_count'], 3)
        self.assertLess(len(json.dumps(got)), 2000); gc.collect()
        self.assertTrue(all(ref() is None for ref in refs))

    def test_full_history_default_keeps_old_shape_and_all_items(self):
        tool = {'type': 'webSearch', 'id': 'web', 'query': 'public', 'results': ['raw']}
        legacy = ThreadState('thread-exact', 'idle', turns=(TurnState('old', 'completed', items=(tool,)),
            TurnState('new', 'inProgress')), read_source='legacy', execution_status='inProgress', execution_source='transport')
        class LegacyTransport:
            def read_thread(self, thread_id): return legacy
        control = JarvisControl(None, ExistingThreadBridge(LegacyTransport(), None))
        server = JarvisMcpServer(control)
        got = asyncio.run(server.mcp._tool_manager.call_tool('jarvis_read',
              {'subject': 'thread', 'task_id': 'thread-exact'}, None)).structured_content
        self.assertEqual(got['status'], 'completed'); self.assertIsNone(got['turn_id'])
        self.assertEqual(got['data']['turns'][0]['items'], [tool]); self.assertEqual(len(got['data']['turns']), 2)
        self.assertNotIn('items_projection', got['data']['turns'][0]); self.assertEqual(got['readback'], {'verified': False})

    def test_transport_without_exact_capability_never_falls_back(self):
        class LegacyTransport:
            def read_thread(self, thread_id): raise AssertionError('full-history fallback attempted')
        control = JarvisControl(None, ExistingThreadBridge(LegacyTransport(), None))
        got = control.read(subject='thread', task_id='thread-exact', turn_id='turn-exact')
        self.assertEqual(got['status'], 'unsupported'); self.assertIsNone(got['data'])

    def test_blank_turn_and_conflicting_thread_rejected_before_transport(self):
        class Bridge:
            def observe_turn(self, *a): raise AssertionError('invalid request dispatched')
        control = JarvisControl(None, Bridge())
        for args in [{'turn_id': ''}, {'turn_id': ' '}, {'turn_id': 'turn-exact', 'thread_id': 'wrong'}]:
            with self.subTest(args=args):
                self.assertEqual(control.read(subject='thread', task_id='thread-exact', **args)['status'], 'invalid_request')

    def test_start_forwards_shared_deadline_and_expiry_closes_without_start(self):
        native = object.__new__(AppServerClient); seen = []; native.close = lambda: seen.append('closed')
        native._start_impl = lambda **kw: seen.append(kw['deadline']) or {}
        deadline = time.monotonic() + 1
        self.assertEqual(native.start(deadline=deadline), {}); self.assertEqual(seen, [deadline])
        with self.assertRaisesRegex(NativeTaskError, 'deadline exhausted'):
            native.start(deadline=time.monotonic() - 1)
        self.assertEqual(seen, [deadline, 'closed'])


if __name__ == '__main__': unittest.main()
