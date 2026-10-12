"""Offline owner-path engineering checks, not original Jarvistest acceptance.

Only fake process/stdio responses and a deterministic monotonic clock are used.
The native wait, monitor, holder, history, verification and Host claim paths run.
All mutable state is in independent TemporaryDirectory fixtures.
"""
import gc
import json
from pathlib import Path
import queue
import tempfile
import threading
from types import SimpleNamespace
import unittest
import weakref
from unittest.mock import patch

from jarvis_runtime import jarvis_native_task_launcher as native
from jarvis_runtime.jarvis_native_task_launcher import AppServerClient, NativeTaskCreationError, NativeTaskError, OwnerExactTerminalReadback
from jarvis_monitor import HoldTurnMonitor, HoldTurnRequest, NotificationPolicy
from adapters.codex_app_server.jarvis_hold_host_service import JarvisHoldHost
from adapters.codex_app_server.task_provisioning_adapter import read_turn_history
from jarvis_control.file_task import FileTaskClient, FileTaskError


def turn(ident='turn-exact', status='completed', error=None):
    return {'id': ident, 'status': status, 'error': error, 'items': [], 'itemsView': 'notLoaded'}


def entry(text='  {"ok": true}\n', ident='turn-exact', item_id='final', phase='final_answer'):
    return {'turnId': ident, 'item': {'id': item_id, 'type': 'agentMessage', 'phase': phase, 'text': text}}


def event(ident='turn-exact', thread='thread-exact', **fields):
    return {'method': 'turn/completed', 'params': {'threadId': thread, 'turn': {'id': ident, 'status': 'completed', **fields}}}


def put(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


class Clock:
    def __init__(self): self.now = 0.0
    def monotonic(self): return self.now


class AdvancingQueue(queue.Queue):
    def __init__(self, clock, *, storm=False, gate_at=None):
        super().__init__(); self.clock = clock; self.storm = storm
        self.gate_at = gate_at; self.paused = threading.Event(); self.resume = threading.Event()
        self.on_empty = None
    def get(self, block=True, timeout=None):
        try: return super().get(False)
        except queue.Empty:
            if not block: raise
        self.clock.now += timeout or 0
        if self.on_empty: self.on_empty()
        if self.gate_at is not None and self.clock.now >= self.gate_at:
            self.paused.set()
            if not self.resume.wait(5): raise AssertionError('Offline fixture gate was not released')
            self.gate_at = None
            try: return super().get(False)
            except queue.Empty: pass
        if self.storm: return event('unrelated-turn', thread='unrelated-thread')
        raise queue.Empty


class Process:
    def __init__(self, sink): self.stdin = sink; self.exitcode = None
    def poll(self): return self.exitcode
    def terminate(self): self.exitcode = 0
    def kill(self): self.exitcode = -9
    def wait(self, timeout=None): self.exitcode = 0; return 0


class Harness:
    def __init__(self, clock=None, *, mode='completed', pages=None, turns=None, hook=None, storm=False, gate_at=None):
        self.clock = clock or Clock(); self.mode = mode; self.hook = hook
        self.pages = pages or [{'data': [entry()]}]; self.turn_pages = turns or [{'data': [turn()]}]
        self.page_index = self.turn_index = 0; self.calls = []; self.deadlines = []; self.client = None
        self.notifications = AdvancingQueue(self.clock, storm=storm, gate_at=gate_at)
        self.responses = AdvancingQueue(self.clock)
    def install(self, client):
        self.client = client; client.notifications = self.notifications; client.response_queue = self.responses
        owner = self
        class Sink:
            def write(self, raw):
                call = json.loads(raw); owner.calls.append(call)
                probe = getattr(client, '_terminal_probe_pending_id', None) == call['id']
                owner.deadlines.append((call['method'], owner.clock.now))
                if owner.hook: owner.hook(call, owner)
                method = call['method']; params = call['params']
                if method == 'thread/resume': result = {'thread': {'id': 'thread-exact'}, 'model': 'test'}
                elif method == 'turn/start': result = {'turn': {'id': 'turn-exact', 'status': 'inProgress'}}
                elif method == 'turn/interrupt':
                    result = {'rpc_error': {'message': 'already finished'}} if owner.mode == 'stop_error' else {}
                elif method == 'thread/read':
                    if probe and owner.mode == 'rpc_timeout': return
                    result = {'thread': {'id': 'wrong' if probe and owner.mode == 'wrong_thread' else 'thread-exact'}}
                elif method == 'thread/turns/list':
                    if owner.mode == 'running': result = {'data': [turn(status='inProgress')]}
                    elif owner.mode in {'failed', 'interrupted'}:
                        result = {'data': [turn(status=owner.mode, error={'message': 'native failed'} if owner.mode == 'failed' else None)]}
                    elif owner.mode == 'missing_turn': result = {'data': [turn('new-turn', 'completed')]}
                    elif owner.mode == 'malformed_turn': result = {'data': [dict(turn(), itemsView='full')]}
                    else:
                        result = owner.turn_pages[min(owner.turn_index, len(owner.turn_pages)-1)]; owner.turn_index += 1
                elif method == 'thread/items/list':
                    if not probe: result = {'data': [entry()]}
                    elif owner.mode == 'wrong_item': result = {'data': [entry(ident='wrong')]}
                    elif owner.mode == 'malformed_item': result = {'data': [{'turnId': 'turn-exact', 'item': None}]}
                    elif owner.mode == 'blank_final': result = {'data': [entry(' \n')]}
                    elif owner.mode == 'commentary': result = {'data': [entry('comment', phase='commentary')]}
                    elif owner.mode == 'partial': result = {'data': [entry()], 'nextCursor': 'repeated'}
                    elif owner.mode == 'failed' or owner.mode == 'interrupted': result = {'data': []}
                    else:
                        result = owner.pages[min(owner.page_index, len(owner.pages)-1)]; owner.page_index += 1
                else: raise AssertionError('Unexpected RPC: ' + method)
                if callable(result): result = result()
                if 'rpc_error' in result: client._queue_response({'id': call['id'], 'error': result['rpc_error']})
                else: client._queue_response({'id': call['id'], 'result': result})
            def flush(self): pass
            def close(self): pass
        client.process = Process(Sink())
        return {}
    def make_client(self, cls=AppServerClient):
        config = SimpleNamespace(codex_cli='offline', request_timeout_seconds=60, turn_readback_timeout_seconds=30,
            turn_completion_timeout_seconds=95, poll_seconds=.25)
        with patch.object(native, '_resolve_codex_command', return_value=(['offline'], 'offline')):
            if cls is AppServerClient: client = cls(config)
            else:
                client = object.__new__(cls); AppServerClient.__init__(client, config)
        self.install(client)
        return client
    @property
    def methods(self): return [c['method'] for c in self.calls]


class OwnerTerminalFallbackTests(unittest.TestCase):
    def observe(self, harness, *, strict=False, control=None, verify=None):
        client = harness.make_client()
        request = HoldTurnRequest('hold-exact', 'thread-exact', 'turn-exact', 1, 1, False, 'continue', NotificationPolicy(),
            verify_output=verify, control_poll=control, receive_final_answer=strict if callable(strict) else None)
        with patch.object(native.time, 'monotonic', harness.clock.monotonic):
            return HoldTurnMonitor().observe(client, request)

    def host_fixture(self, *, mode='completed', pages=None, turns=None, hook=None, gate_at=None, storm=False):
        tmp = tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        state = Path(tmp.name); root = state/'task-holds/hold-exact'
        config = state/'launcher.json'
        put(config, {'dispatcher_thread_id': 'offline', 'codex_cli': '/bin/false', 'default_model': 'test',
            'allowed_projects': {'offline': str(state)}, 'poll_seconds': .25, 'turn_readback_timeout_seconds': 30})
        request = {'request_id': 'request-exact', 'hold_id': 'hold-exact', 'mode': 'resume',
            'thread_id': 'thread-exact', 'prompt': 'offline', 'model': 'test',
            'max_turns': 1, 'auto_continue': False, 'input_binding': {}, 'notifications': {}}
        put(root/'request.json', request)
        put(root/'ack.json', {'request_id': 'request-exact', 'hold_id': 'hold-exact', 'status': 'accepted', 'phase': 'queued_for_user_host'})
        host = JarvisHoldHost(state_dir=state, launcher_config=config)
        harness = Harness(mode=mode, pages=pages, turns=turns, hook=hook, gate_at=gate_at, storm=storm)
        return root, host, harness

    def patches(self, harness):
        return (patch.object(native, '_resolve_codex_command', return_value=(['offline'], 'offline')),
                patch.object(AppServerClient, '_start_impl', lambda client, **kw:
                    {} if client.process is not None and client.process.poll() is None else harness.install(client)),
                patch.object(native.time, 'monotonic', harness.clock.monotonic))

    def run_host(self, root, host, harness):
        a,b,c = self.patches(harness)
        with a,b,c: self.assertTrue(host.run_once(request_root=root))
        return json.loads((root/'result.json').read_text())

    def test_lost_notification_real_wait_monitor_owner_history_and_host_release(self):
        root,host,h = self.host_fixture(pages=[{'data': [entry()], 'nextCursor': 'older'},
            {'data': [entry('old', item_id='old')]}])
        result = self.run_host(root,host,h)
        self.assertEqual(result['status'], 'turn_limit_reached')
        self.assertEqual(result['terminal_evidence'], 'owner_exact_terminal_readback')
        self.assertEqual(result['final_message'], '{"ok": true}')
        self.assertTrue(result['terminal_confirmed']); self.assertTrue(result['local_observer_stopped'])
        self.assertFalse((root/'.user-host-claim').exists())
        history = read_turn_history(root/'turn-history.sqlite', hold_id='hold-exact')
        self.assertEqual(len(history), 1); self.assertEqual(history[0]['turn_id'], 'turn-exact')
        self.assertEqual(h.methods, ['thread/resume', 'turn/start', 'thread/read', 'thread/turns/list', 'thread/items/list', 'thread/items/list'])
        self.assertIsNone(h.client.process)
        self.assertEqual(json.loads((host.health_path).read_text())['active_count'], 0)

    def test_newer_turn_and_unrelated_notification_storm_never_substitute(self):
        h = Harness(storm=True, turns=[{'data': [turn('new-turn', 'inProgress')], 'nextCursor': 'older'}, {'data': [turn()]}])
        decision = self.observe(h)
        self.assertEqual(decision.expected_turn_id, 'turn-exact'); self.assertEqual(decision.terminal_evidence, 'owner_exact_terminal_readback')
        self.assertEqual(h.methods, ['thread/read', 'thread/turns/list', 'thread/turns/list', 'thread/items/list'])
        self.assertGreaterEqual(h.clock.now, 30)

    def test_lost_notification_real_pocket_intake_receipt_verification_history_release(self):
        root,host,h=self.host_fixture()
        boundary=root/'output'; boundary.mkdir()
        request=json.loads((root/'request.json').read_text())
        request['input_binding']={'candidate_ids':[1049],'lane_item_count':1,'batch_size':1,
            'output_boundary':str(boundary),'result_verification':{'mode':'final_answer_json',
                'receipt_paths':{'1049':'receipt-1049.json'},'terminal_statuses':['received'],
                'output_schema':{'type':'object','properties':{'ok':{'type':'boolean'}},
                    'required':['ok'],'additionalProperties':False}}}
        put(root/'request.json',request)
        result=self.run_host(root,host,h)
        self.assertEqual(result['final_message'],'  {"ok": true}\n')
        self.assertEqual(result['output_verification']['status'],'verified')
        self.assertTrue(result['output_verification']['review_needed'])
        self.assertEqual(result['output_verification']['verification_status'],'not_verified')
        receipt=json.loads((boundary/'receipt-1049.json').read_text())
        self.assertEqual(receipt['thread_id'],'thread-exact'); self.assertEqual(receipt['turn_id'],'turn-exact')
        self.assertEqual(receipt['candidate_id'],1049); self.assertEqual(receipt['schema_issues'],[])
        self.assertEqual(Path(receipt['raw_path']).read_text(),'  {"ok": true}\n')
        self.assertEqual(json.loads(Path(receipt['output_path']).read_text()),{'ok':True})
        history=read_turn_history(root/'turn-history.sqlite',hold_id='hold-exact')
        self.assertEqual(len(history),1); self.assertEqual(history[0]['candidate_id'],1049)
        self.assertEqual(history[0]['final_answer'],'  {"ok": true}\n')
        self.assertTrue(result['terminal_confirmed']); self.assertFalse((root/'.user-host-claim').exists())
        self.assertEqual(h.methods,['thread/resume','turn/start','thread/read','thread/turns/list','thread/items/list'])

    def test_partial_invalid_and_timeout_hold_have_no_result_or_release(self):
        for mode in ['wrong_thread', 'missing_turn', 'malformed_turn', 'wrong_item', 'malformed_item', 'partial', 'rpc_timeout', 'scan_timeout', 'blank_final', 'commentary', 'running']:
            with self.subTest(mode=mode):
                def slow_page(call,h):
                    if (mode=='scan_timeout' and call['method']=='thread/items/list'
                            and h.page_index==1): h.clock.now += 6
                root,host,h = self.host_fixture(mode=mode, gate_at=45,hook=slow_page,
                    pages=[{'data':[entry()],'nextCursor':'older'},{'data':[]}])
                errors = []
                def run():
                    try: host.run_once(request_root=root)
                    except BaseException as exc: errors.append(exc)
                a,b,c = self.patches(h)
                with a,b,c:
                    worker = threading.Thread(target=run); worker.start()
                    try:
                        self.assertTrue(h.notifications.paused.wait(3), errors)
                        self.assertFalse((root/'result.json').exists())
                        self.assertFalse((root/'turn-history.sqlite').exists())
                        self.assertTrue((root/'.user-host-claim/owner.json').exists())
                        self.assertEqual(json.loads((root/'ack.json').read_text())['status'], 'holding')
                        self.assertEqual(h.client._held_terminal_readback_diagnostic['state'], 'holding')
                        if mode == 'running': self.assertNotIn('thread/items/list', h.methods)
                    finally:
                        h.notifications.put(event()); h.notifications.resume.set(); worker.join(3)
                    self.assertFalse(worker.is_alive()); self.assertEqual(errors, [])
                self.assertEqual(h.methods.count('turn/start'), 1); self.assertNotIn('turn/interrupt', h.methods)

    def test_running_probe_is_metadata_only_and_limited_to_30_second_intervals(self):
        h = Harness(mode='running'); client = h.make_client()
        with patch.object(native.time, 'monotonic', h.clock.monotonic):
            with self.assertRaises(NativeTaskCreationError):
                client.wait_for_turn_terminal('thread-exact', 'turn-exact', terminal_readback=True, control_poll=lambda: None)
        self.assertEqual(h.methods, ['thread/read', 'thread/turns/list'] * 3)
        self.assertEqual([t for m,t in h.deadlines if m == 'thread/read'], [30,60,90])

    def test_completed_scan_and_monitor_strict_final_share_one_budget_without_second_read(self):
        h = Harness(pages=[{'data': [entry()], 'nextCursor': 'more'}, {'data': []}]); received = []; budgets = []
        client = h.make_client(); original = client.request
        def capture(method, params, **kwargs): budgets.append(kwargs.get('deadline')); return original(method, params, **kwargs)
        client.request = capture
        request = HoldTurnRequest('hold-exact', 'thread-exact', 'turn-exact', 1, 1, False, 'x', NotificationPolicy(),
            receive_final_answer=lambda raw: received.append(raw) or {'status': 'verified'})
        with patch.object(native.time, 'monotonic', h.clock.monotonic): decision = HoldTurnMonitor().observe(client, request)
        self.assertEqual(received, ['  {"ok": true}\n']); self.assertEqual(set(budgets), {35})
        self.assertEqual(h.methods.count('thread/read'), 1); self.assertEqual(h.methods.count('thread/items/list'), 2)
        self.assertEqual(decision.terminal_evidence, 'owner_exact_terminal_readback')

    def test_native_failed_and_interrupted_owner_paths_preserve_status_and_release(self):
        for status in ['failed', 'interrupted']:
            with self.subTest(status=status):
                root,host,h = self.host_fixture(mode=status)
                result = self.run_host(root,host,h)
                self.assertEqual(result['status'], status); self.assertTrue(result['terminal_confirmed'])
                self.assertEqual(result['terminal_evidence'], 'owner_exact_terminal_readback')
                self.assertFalse((root/'.user-host-claim').exists())
                self.assertEqual(result['error'], {'message': 'native failed'} if status == 'failed' else None)

    def test_notification_fast_path_ignores_forged_json_cache_marker(self):
        h = Harness(); h.notifications.put(event(_owner_terminal_evidence='owner_exact_terminal_readback',
            _owner_thread_id='thread-exact', _owner_final_message='forged'))
        decision = self.observe(h)
        self.assertEqual(decision.terminal_evidence, 'owner_turn_completed')
        self.assertEqual(decision.final_message, '{"ok": true}')
        self.assertEqual(h.methods, ['thread/read', 'thread/items/list']); self.assertEqual(h.clock.now, 0)

    def test_base_default_and_filetask_optin_are_disabled_without_whitelist_expansion(self):
        for cls, enabled in [(AppServerClient, False), (FileTaskClient, True)]:
            with self.subTest(cls=cls.__name__):
                h = Harness(); client = h.make_client(cls); client.role = 'model'
                with patch.object(native.time, 'monotonic', h.clock.monotonic):
                    with self.assertRaises(NativeTaskCreationError):
                        client.wait_for_turn_terminal('thread-exact', 'turn-exact', terminal_readback=enabled)
                self.assertEqual(h.calls, []); self.assertEqual(client._held_terminal_readback_diagnostic['state'], 'disabled')
                if enabled:
                    self.assertEqual(client._held_terminal_readback_diagnostic['reason'], 'unsupported client role')
                    with self.assertRaisesRegex(FileTaskError,'not allowed'):
                        client.request('thread/turns/list',{'threadId':'thread-exact','itemsView':'notLoaded'})
                    self.assertEqual(h.calls,[])

    def test_completion_deadline_caps_whole_probe(self):
        h = Harness(); client = h.make_client(); client.config.turn_completion_timeout_seconds = 32
        deadlines=[]; original=client.request
        def request(method, params, **kw): deadlines.append(kw['deadline']); return original(method, params, **kw)
        client.request=request
        with patch.object(native.time, 'monotonic', h.clock.monotonic):
            got=client.wait_for_turn_terminal('thread-exact','turn-exact',terminal_readback=True,control_poll=lambda:None)
        self.assertIsInstance(got, OwnerExactTerminalReadback); self.assertEqual(set(deadlines), {32})

    def test_last_control_poll_crossing_deadline_cannot_return_terminal(self):
        h = Harness(); client=h.make_client(); after_items=0
        def control():
            nonlocal after_items
            if h.methods and h.methods[-1] == 'thread/items/list':
                after_items += 1
                if after_items == 2: h.clock.now += 5
        client.config.turn_completion_timeout_seconds = 40
        with patch.object(native.time,'monotonic',h.clock.monotonic):
            with self.assertRaises(NativeTaskCreationError):
                client.wait_for_turn_terminal('thread-exact','turn-exact',terminal_readback=True,control_poll=control)
        self.assertIn('deadline exhausted after control', client._held_terminal_readback_diagnostic['reason'])

    def test_owner_stop_rpc_error_is_not_swallowed_as_readback_error_or_success(self):
        def stop(call, h):
            if call['method']=='thread/items/list':
                request=json.loads((root/'request.json').read_text())
                request['host_stop']={'request_id':'request-exact','hold_id':'hold-exact','thread_id':'thread-exact','turn_id':'turn-exact'}
                request['stop_requested']=True
                put(root/'request.json',request)
        root,host,h=self.host_fixture(mode='stop_error',hook=stop)
        result=self.run_host(root,host,h)
        self.assertEqual(result['status'],'failed'); self.assertFalse(result['terminal_confirmed'])
        self.assertTrue(result['host_stop']); self.assertEqual(h.methods.count('turn/interrupt'),1)
        self.assertTrue((root/'.user-host-claim').exists()); self.assertIn('already finished',result['reason'])

    def test_stop_after_readback_retains_cancelled_semantics_and_owned_exit_gate(self):
        def stop(call,h):
            if call['method']=='thread/items/list':
                request=json.loads((root/'request.json').read_text())
                request['host_stop']={'request_id':'request-exact','hold_id':'hold-exact','thread_id':'thread-exact','turn_id':'turn-exact'}
                request['stop_requested']=True
                put(root/'request.json',request)
        root,host,h=self.host_fixture(hook=stop)
        result=self.run_host(root,host,h)
        self.assertEqual(result['status'],'cancelled'); self.assertTrue(result['terminal_confirmed'])
        self.assertTrue(result['holder_client_exit_confirmed']); self.assertTrue(result['holder_exit_confirmed'])
        self.assertFalse((root/'.user-host-claim').exists()); self.assertEqual(h.methods.count('turn/interrupt'),1)

    def test_duplicate_late_notification_does_not_duplicate_owner_result_or_history(self):
        def late(call,h):
            if call['method']=='thread/items/list': h.notifications.put(event()); h.notifications.put(event())
        root,host,h=self.host_fixture(hook=late)
        result=self.run_host(root,host,h)
        before=(root/'result.json').read_bytes()
        self.assertFalse(host.run_once(request_root=root))
        self.assertEqual((root/'result.json').read_bytes(),before)
        self.assertEqual(len(read_turn_history(root/'turn-history.sqlite',hold_id='hold-exact')),1)

    def test_large_item_pages_are_released_before_next_rpc_and_control_callback(self):
        refs=[]
        class LargeItem(dict): pass
        def large():
            item=LargeItem(id='tool',type='mcpToolCall',arguments={'large':'x'*4_000_000})
            refs.append(weakref.ref(item))
            return {'data':[{'turnId':'turn-exact','item':item}],'nextCursor':'more'}
        def check(call,h):
            if call['method']=='thread/items/list' and refs:
                gc.collect(); self.assertIsNone(refs[-1]())
        h=Harness(pages=[large,{'data':[entry()]}],hook=check)
        def control():
            if refs:
                gc.collect(); self.assertIsNone(refs[-1]())
        decision=self.observe(h,control=control)
        self.assertLess(len(str(decision)),1000); self.assertTrue(all(ref() is None for ref in refs))
        self.assertEqual(set(h.client._held_terminal_readback_diagnostic),{'state','reason'})

    def test_one_unanswered_probe_and_exact_late_reply_do_not_accumulate_or_drop_other_ids(self):
        h=Harness(mode='rpc_timeout'); client=h.make_client()
        client.config.turn_completion_timeout_seconds=94
        with patch.object(native.time,'monotonic',h.clock.monotonic):
            with self.assertRaises(NativeTaskCreationError):
                client.wait_for_turn_terminal('thread-exact','turn-exact',terminal_readback=True,control_poll=lambda:None)
        self.assertEqual(len(h.calls),1)
        pending=client._terminal_probe_pending_id; self.assertTrue(client._terminal_probe_expired)
        class LargeItem(dict): pass
        item=LargeItem(blob='x'*4_000_000); ref=weakref.ref(item)
        unrelated={'id':777,'result':{'keep':True}}; client._queue_response(unrelated)
        client._queue_response({'id':pending,'result':item}); del item; gc.collect()
        self.assertIsNone(ref()); self.assertIsNone(client._terminal_probe_pending_id)
        self.assertEqual(client.response_queue.get_nowait(),unrelated)
        self.assertEqual(client.response_queue.qsize(),0)
        self.assertIn('unanswered',client._held_terminal_readback_diagnostic['reason'])
        h.mode='completed'; client.config.turn_completion_timeout_seconds=40
        with patch.object(native.time,'monotonic',h.clock.monotonic):
            got=client.wait_for_turn_terminal('thread-exact','turn-exact',terminal_readback=True,control_poll=lambda:None)
        self.assertIsInstance(got,OwnerExactTerminalReadback)
        self.assertEqual(len(h.calls),4); self.assertIsNone(client._terminal_probe_pending_id)

    def test_timeout_enqueue_race_preserves_unrelated_reply(self):
        h=Harness(mode='rpc_timeout'); client=h.make_client()
        def race():
            client._queue_response({'id':999,'result':{'keep':True}})
            client._queue_response({'id':client._terminal_probe_pending_id,'result':{'large':'x'*1000}})
        h.responses.on_empty=race
        with patch.object(native.time,'monotonic',h.clock.monotonic):
            with self.assertRaises(NativeTaskError):
                client.request('thread/read',{'threadId':'thread-exact','includeTurns':False},deadline=1,_terminal_probe=True)
        self.assertIsNone(client._terminal_probe_pending_id)
        self.assertEqual(client.response_queue.qsize(),1)
        self.assertEqual(client.response_queue.get_nowait()['id'],999)

    def test_permanently_missing_probe_reply_does_not_block_event_fast_path(self):
        h=Harness(mode='rpc_timeout'); client=h.make_client()
        def event_after_probe_timeout():
            if h.clock.now >= 70: h.notifications.put(event())
        h.notifications.on_empty=event_after_probe_timeout
        with patch.object(native.time,'monotonic',h.clock.monotonic):
            got=client.wait_for_turn_terminal('thread-exact','turn-exact',wait_forever=True,
                terminal_readback=True,control_poll=lambda:None)
        self.assertEqual(got['id'],'turn-exact'); self.assertNotIsInstance(got,OwnerExactTerminalReadback)
        self.assertEqual(h.methods,['thread/read']); self.assertIsNotNone(client._terminal_probe_pending_id)

    def test_forever_pending_probe_then_event_completes_real_owner_chain(self):
        root,host,h=self.host_fixture(mode='rpc_timeout')
        def event_after_timeout():
            if h.clock.now >= 70: h.notifications.put(event())
        h.notifications.on_empty=event_after_timeout
        result=self.run_host(root,host,h)
        self.assertEqual(result['terminal_evidence'],'owner_turn_completed')
        self.assertTrue(result['terminal_confirmed']); self.assertTrue(result['local_observer_stopped'])
        self.assertFalse((root/'.user-host-claim').exists())
        self.assertEqual(h.methods,['thread/resume','turn/start','thread/read','thread/read','thread/items/list'])
        self.assertEqual(h.client.response_queue.qsize(),0)

    def test_control_exception_between_metadata_and_items_is_propagated(self):
        h=Harness(); client=h.make_client()
        def control():
            if h.methods==['thread/read','thread/turns/list']: raise RuntimeError('control ownership conflict')
        with patch.object(native.time,'monotonic',h.clock.monotonic):
            with self.assertRaisesRegex(RuntimeError,'ownership conflict'):
                client.wait_for_turn_terminal('thread-exact','turn-exact',terminal_readback=True,control_poll=control)
        self.assertNotIn('thread/items/list',h.methods)


if __name__=='__main__': unittest.main()
