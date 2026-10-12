"""Offline engineering regressions; these do not substitute for Jarvistest."""
import gc
import json
import queue
import threading
import time
import types
import unittest
import weakref
import io
import tempfile
from pathlib import Path
from unittest.mock import Mock
from unittest.mock import patch

from jarvis_runtime.jarvis_native_task_launcher import AppServerClient, NativeTaskError


class PhysicalAdmissionTests(unittest.TestCase):
    """Explicit >=95% user requirement through existing adapter/native seams."""

    def sample(self, available):
        return {'source': 'TEST-physical', 'total_bytes': 10000, 'available_bytes': available}

    def test_boundary_configurable_policy_unknown_and_live_os_sample(self):
        from adapters.codex_app_server import task_provisioning_adapter as adapter
        config = types.SimpleNamespace()
        with patch.object(adapter, 'physical_memory_sample', return_value=self.sample(500), create=True):
            at = adapter.physical_memory_admission(config)
        self.assertEqual(at['status'], 'blocked')
        self.assertEqual(at['threshold_percent'], 95)
        self.assertEqual(at['used_percent'], 95)
        with patch.object(adapter, 'physical_memory_sample', return_value=self.sample(501), create=True):
            self.assertEqual(adapter.physical_memory_admission(config)['status'], 'allowed')
        config.memory_admission_max_used_percent = 94
        with patch.object(adapter, 'physical_memory_sample', return_value=self.sample(600), create=True):
            self.assertEqual(adapter.physical_memory_admission(config)['status'], 'blocked')
        for invalid in (True, '95', 0, 101, float('nan'), float('inf')):
            config.memory_admission_max_used_percent = invalid
            rejected = adapter.physical_memory_admission(config)
            self.assertEqual(rejected['error_code'], 'memory_policy_invalid')
            self.assertEqual(rejected['status'], 'blocked')
        config.memory_admission_max_used_percent = 95
        with patch.object(adapter, 'physical_memory_sample', side_effect=OSError('TEST-sample-unavailable'), create=True):
            unknown = adapter.physical_memory_admission(config)
        self.assertEqual(unknown['error_code'], 'physical_memory_unknown')
        self.assertIn('TEST-sample-unavailable', unknown['reason'])
        live = adapter.physical_memory_sample()
        self.assertGreater(live['total_bytes'], 0)
        self.assertGreaterEqual(live['available_bytes'], 0)
        self.assertLessEqual(live['available_bytes'], live['total_bytes'])
        print('PHYSICAL_MEMORY_SAMPLE_RECEIPT ' + json.dumps(live, sort_keys=True))

    def test_sdk_create_ready_resamples_95_94_95_without_queue_or_host_effects(self):
        from adapters.codex_app_server import task_provisioning_adapter as adapter
        from jarvis_control import TaskProvisionRequest
        with tempfile.TemporaryDirectory() as temp:
            config = types.SimpleNamespace(worker_capacity=1, resolve_project=lambda _: ('TEST', temp))
            starter = Mock(side_effect=AssertionError('no host start while blocked'))
            port = adapter.CodexAppServerTaskProvisioningAdapter(Path(temp)/'unused', state_dir=temp,
                config_loader=lambda _: config, host_initializer=starter)
            with patch.object(adapter, 'physical_memory_sample', return_value=self.sample(500), create=True):
                refused = port.ensure_hold_host_ready(required_workers=1)
                create = port.provision(TaskProvisionRequest(request_id='TEST-high', project='TEST', title='TEST', prompt='TEST', source_ref='TEST'))
            self.assertEqual(refused['status'], 'blocked')
            self.assertEqual(create.status, 'blocked')
            self.assertEqual(list(Path(temp).iterdir()), [])
            starter.assert_not_called()
            with patch.object(adapter, 'physical_memory_sample', return_value=self.sample(600), create=True):
                low = port.provision(TaskProvisionRequest(request_id='TEST-low', project='TEST', title='TEST', prompt='TEST', source_ref='TEST'))
            self.assertEqual(low.status, 'accepted')
            original = {str(p): p.read_bytes() for p in Path(temp).rglob('*') if p.is_file()}
            with patch.object(adapter, 'physical_memory_sample', return_value=self.sample(500), create=True):
                second = port.provision(TaskProvisionRequest(request_id='TEST-high-again', project='TEST', title='TEST', prompt='TEST', source_ref='TEST'))
            self.assertEqual(second.status, 'blocked')
            self.assertEqual({str(p):p.read_bytes() for p in Path(temp).rglob('*') if p.is_file()}, original)
            print('MEMORY_ADMISSION_SEQUENCE_RECEIPT ' + json.dumps({'statuses':[create.status,low.status,second.status], 'host_starts':starter.call_count, 'existing_unchanged':True}))

    def test_actual_dispatch_guard_precedes_commit_and_readonly_observer_cannot_write(self):
        from adapters.codex_app_server import task_provisioning_adapter as adapter
        client = object.__new__(AppServerClient)
        client.config = types.SimpleNamespace(memory_admission_max_used_percent=95)
        stream = io.StringIO()
        client.process = types.SimpleNamespace(poll=lambda: None, stdin=stream)
        client._write_lock = threading.Lock()
        client.before_turn_dispatch = Mock()
        with patch.object(adapter, 'physical_memory_sample', return_value=self.sample(500), create=True):
            with self.assertRaisesRegex(NativeTaskError, 'memory'):
                client._write({'method':'turn/start', 'params':{'threadId':'TEST', 'model':'TEST'}})
            client._write({'method':'thread/read', 'params':{'threadId':'TEST'}})
        client.before_turn_dispatch.assert_not_called()
        self.assertNotIn('turn/start', stream.getvalue())
        client.read_only = True
        with patch.object(adapter, 'physical_memory_sample', return_value=self.sample(600), create=True):
            with self.assertRaisesRegex(NativeTaskError, 'read-only'):
                client._write({'method':'turn/start', 'params':{'threadId':'TEST'}})
        client.before_turn_dispatch.assert_not_called()

    def test_resume_resamples_before_queue_and_same_request_replay_stays_readonly(self):
        from adapters.codex_app_server import task_provisioning_adapter as adapter
        from jarvis_control.provisioning import TaskMonitorResumeRequest
        with tempfile.TemporaryDirectory() as temp:
            port = adapter.CodexAppServerTaskProvisioningAdapter(Path(temp)/'unused', state_dir=temp,
                config_loader=lambda _: types.SimpleNamespace())
            request = TaskMonitorResumeRequest(request_id='TEST-resume', task_id='TEST-thread', prompt='TEST', source_ref='TEST')
            with patch.object(adapter, 'physical_memory_sample', return_value=self.sample(500), create=True):
                high = port.resume_with_monitor(request)
            self.assertEqual(high.status, 'blocked')
            self.assertEqual(list(Path(temp).iterdir()), [])
            # First check permits; pressure rises at the lock's actual admission.
            with patch.object(adapter, 'physical_memory_sample', side_effect=[self.sample(600),self.sample(500)], create=True):
                late = port.resume_with_monitor(request)
            self.assertEqual(late.status, 'blocked')
            self.assertEqual(list(Path(temp).rglob('request.json')), [])
            with patch.object(adapter, 'physical_memory_sample', return_value=self.sample(600), create=True):
                low = port.resume_with_monitor(request)
            self.assertEqual(low.status, 'accepted')
            before = {str(p):p.read_bytes() for p in Path(temp).rglob('*') if p.is_file()}
            with patch.object(adapter, 'physical_memory_sample', side_effect=AssertionError('same request replay must not sample'), create=True):
                replay = port.resume_with_monitor(request)
            self.assertEqual(replay.request_id, request.request_id)
            self.assertEqual({str(p):p.read_bytes() for p in Path(temp).rglob('*') if p.is_file()}, before)

    def test_native_spawn_admission_and_internal_readonly_spawn_under_bad_policy(self):
        from adapters.codex_app_server import task_provisioning_adapter as adapter
        from jarvis_runtime import jarvis_native_task_launcher as native
        config = types.SimpleNamespace(codex_cli='TEST', expected_codex_home='', memory_admission_max_used_percent=95)
        with patch.object(native, '_resolve_codex_command', return_value=(['TEST'], 'TEST')):
            client = AppServerClient(config)
        with patch.object(adapter, 'physical_memory_sample', return_value=self.sample(500), create=True), patch.object(native.subprocess, 'Popen') as spawn:
            with self.assertRaisesRegex(NativeTaskError, 'memory'):
                client.start()
            spawn.assert_not_called()
        config.memory_admission_max_used_percent = 'bad'
        client.read_only = True
        process = Mock(); process.poll.return_value = None
        with patch.object(adapter, 'physical_memory_sample', side_effect=AssertionError('readonly spawn must not sample'), create=True), \
                patch.object(native, 'child_environment', return_value={}), \
                patch.object(native, 'acquire_instance_lock', return_value=None), \
                patch.object(native.subprocess, 'Popen', return_value=process) as spawn, \
                patch.object(native.threading, 'Thread'), patch.object(client, 'request', return_value={}), patch.object(client, 'notify'):
            client.start()
        spawn.assert_called_once()
        client.process = None

    def test_linux_sampler_requires_memavailable_and_does_not_guess_free_memory(self):
        from adapters.codex_app_server import task_provisioning_adapter as adapter
        with patch.object(adapter.sys, 'platform', 'linux'), patch.object(Path, 'read_text', return_value='MemTotal: 10000 kB\nMemAvailable: 500 kB\nMemFree: 1 kB\n'):
            sample = adapter.physical_memory_sample()
        self.assertEqual(sample['total_bytes'], 10000*1024)
        self.assertEqual(sample['available_bytes'], 500*1024)
        with patch.object(adapter.sys, 'platform', 'linux'), patch.object(Path, 'read_text', return_value='MemTotal: 10000 kB\nMemFree: 500 kB\n'):
            with self.assertRaisesRegex((ValueError, RuntimeError), 'MemAvailable'):
                adapter.physical_memory_sample()

    def test_capacity_application_rechecks_and_replay_read_close_bypass_invalid_policy(self):
        from adapters.codex_app_server import task_provisioning_adapter as adapter
        from adapters.codex_app_server import jarvis_hold_host_service as service
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config_path = root/'launcher.json'
            config_path.write_text(json.dumps({'dispatcher_thread_id':'TEST','expected_codex_home':temp,'profile':'TEST','allowed_projects':{'TEST':temp},'memory_admission_max_used_percent':95}))
            host = service.JarvisHoldHost(state_dir=root, launcher_config=config_path, workers=1)
            host._pool_ready = True
            host._pending = queue.Queue(maxsize=1)
            host._sync_consumers = Mock(return_value=True)
            command = {'request_id':'TEST-grow-high','host_instance_id':host.instance_id,'target_capacity':2,'expected_revision':0}
            with patch.object(adapter, 'physical_memory_sample', return_value=self.sample(500), create=True):
                high = host.apply_capacity_request(command)
            self.assertEqual(high['status'], 'blocked')
            self.assertEqual(host.workers, 1)
            host._sync_consumers.assert_not_called()
            with patch.object(adapter, 'physical_memory_sample', return_value=self.sample(600), create=True):
                low = host.apply_capacity_request({**command,'request_id':'TEST-grow-low'})
            self.assertEqual(low['status'], 'applied')
            self.assertEqual(host.workers, 2)
            with patch.object(adapter, 'physical_memory_sample', side_effect=AssertionError('replay does not sample'), create=True):
                self.assertEqual(host.apply_capacity_request({**command,'request_id':'TEST-grow-low'}), low)
            config = json.loads(config_path.read_text()); config['memory_admission_max_used_percent'] = 'bad'
            config_path.write_text(json.dumps(config))
            hold = root/'task-holds'/'TEST-close'; hold.mkdir(parents=True)
            (hold/'request.json').write_text(json.dumps({'request_id':'TEST-close','hold_id':'TEST-close'}))
            (hold/'ack.json').write_text(json.dumps({'request_id':'TEST-close','status':'unknown'}))
            port = adapter.CodexAppServerTaskProvisioningAdapter(config_path, state_dir=root)
            with patch.object(adapter, 'physical_memory_sample', side_effect=AssertionError('read/close does not sample'), create=True):
                self.assertEqual(port.hold_status('TEST-close')['status'], 'unknown')
                close = port.close_hold('TEST-close')
            self.assertEqual(close['status'], 'closed_unconfirmed')
            self.assertFalse(close['terminal_confirmed'])
            self.assertFalse(close['hold_released'])


def message(text, phase='final_answer', turn='turn-1'):
    return {'turnId': turn, 'item': {'id':'item-1', 'type':'agentMessage', 'phase':phase, 'text':text}}


class ReadbackTests(unittest.TestCase):
    def client(self, pages):
        client=object.__new__(AppServerClient)
        client.config=types.SimpleNamespace(turn_readback_timeout_seconds=0.1,poll_seconds=0.001)
        client.calls=[]
        client.metadata_calls=[]
        client.deadlines=[]
        def request(method, params, **kwargs):
            client.deadlines.append(kwargs.get('deadline'))
            if method == 'thread/read':
                client.metadata_calls.append((method,params))
                return {'thread':{'id':params['threadId']}}
            client.calls.append((method,params))
            return pages.pop(0)
        client.request=request
        return client

    def test_reads_exact_turn_only(self):
        c=self.client([{'data':[message(' done ')]}])
        self.assertEqual(c.wait_for_turn_readback('thread-1','turn-1'),'done')
        self.assertEqual(c.metadata_calls,[('thread/read',{'threadId':'thread-1','includeTurns':False})])
        self.assertEqual(len(set(c.deadlines)),1)
        self.assertEqual(c.calls,[('thread/items/list',{'threadId':'thread-1','turnId':'turn-1','sortDirection':'desc','limit':50})])

    def test_pocket_preserves_whitespace(self):
        c=self.client([{'data':[message('  {"ok": true}\n')]}])
        self.assertEqual(c.wait_for_turn_readback('thread-1','turn-1',require_final_answer=True),'  {"ok": true}\n')

    def test_newest_final_preferred_to_newest_commentary_across_pages(self):
        c=self.client([{'data':[message('comment','commentary')],'nextCursor':'page-2'}, {'data':[message('real final')]}])
        self.assertEqual(c.wait_for_turn_readback('thread-1','turn-1'),'real final')
        self.assertEqual(c.calls[1][1]['cursor'],'page-2')

    def test_legacy_commentary_only_fallback(self):
        c=self.client([{'data':[message('new','commentary'),message('old','commentary')]}])
        self.assertEqual(c.wait_for_turn_readback('thread-1','turn-1'),'new')

    def test_require_final_does_not_accept_commentary(self):
        c=self.client([{'data':[message('comment','commentary')]}])
        self.assertEqual(c._read_final_message_pagewise('thread-1','turn-1',require_final_answer=True,deadline=time.monotonic()+1),'')

    def test_wrong_turn_fails_closed(self):
        c=self.client([{'data':[message('other',turn='turn-other')]}])
        with self.assertRaisesRegex(NativeTaskError,'identity mismatch'):
            c.wait_for_turn_readback('thread-1','turn-1')

    def test_invalid_shape_fails_closed(self):
        c=self.client([{'data':{}}])
        with self.assertRaisesRegex(NativeTaskError,'missing data'):
            c.wait_for_turn_readback('thread-1','turn-1')

    def test_cursor_cycle_fails_closed(self):
        c=self.client([{'data':[],'nextCursor':'same'},{'data':[],'nextCursor':'same'}])
        with self.assertRaisesRegex(NativeTaskError,'repeated cursor'):
            c.wait_for_turn_readback('thread-1','turn-1')

    def test_partial_scan_cannot_fall_back_to_commentary(self):
        c=self.client([{'data':[message('comment','commentary')],'nextCursor':'more'}])
        self.assertEqual(c._read_final_message_pagewise('thread-1','turn-1',require_final_answer=False,deadline=time.monotonic()-1),'')

    def test_blank_delayed_readback_retries(self):
        c=self.client([{'data':[]},{'data':[message('later')]}])
        self.assertEqual(c.wait_for_turn_readback('thread-1','turn-1'),'later')
        self.assertEqual(len(c.calls),2)

    def test_wrong_thread_metadata_fails_closed(self):
        c=self.client([])
        c.request=lambda *args,**kwargs:{'thread':{'id':'different-thread'}}
        with self.assertRaisesRegex(NativeTaskError,'Exact-thread metadata'):
            c.wait_for_turn_readback('thread-1','turn-1')

    def test_already_expired_scan_does_not_dispatch(self):
        c=self.client([])
        self.assertEqual(c._read_final_message_pagewise('thread-1','turn-1',require_final_answer=False,deadline=time.monotonic()-1),'')
        self.assertEqual(c.calls,[])

    def test_rpc_timeout_is_capped_by_readback_deadline(self):
        c=object.__new__(AppServerClient)
        c.config=types.SimpleNamespace(request_timeout_seconds=60)
        c._request_id=0;c.stderr_lines=[];c._write=lambda value:None
        class NoReply:
            def __init__(self):self.timeout=None
            def get(self,timeout):self.timeout=timeout;raise queue.Empty
        c.response_queue=NoReply()
        deadline=time.monotonic()+0.01
        with self.assertRaisesRegex(NativeTaskError,'timed out'):
            c.request('thread/items/list',{},deadline=deadline)
        self.assertGreaterEqual(c.response_queue.timeout,0)
        self.assertLessEqual(c.response_queue.timeout,0.01)

    def test_expired_rpc_never_writes(self):
        c=object.__new__(AppServerClient)
        c.config=types.SimpleNamespace(request_timeout_seconds=60)
        c._write=lambda value:self.fail('expired request was written')
        with self.assertRaisesRegex(NativeTaskError,'deadline exhausted'):
            c.request('thread/items/list',{},deadline=time.monotonic()-1)

    def test_resume_does_not_load_historical_turns(self):
        c=self.client([{'thread':{'id':'thread-1','turns':[]}}])
        c.start=lambda:None
        c._runtime_thread_params=lambda model:{'model':model}
        c._verify_runtime_context=lambda response,**kwargs:None
        c.start_turn_async=lambda *args,**kwargs:{'turn_id':'turn-1'}
        self.assertEqual(c.resume_turn_async('thread-1','public test',client_user_message_id='msg-1',model='gpt-6-luna'),{'turn_id':'turn-1'})
        self.assertTrue(c.calls[0][1]['excludeTurns'])


class ReaderRetentionTests(unittest.TestCase):
    def test_consumed_message_not_retained_by_waiting_reader(self):
        class WeakDict(dict): pass
        class BlockingStream:
            def __init__(self):
                self.once=False;self.blocked=threading.Event();self.release=threading.Event()
            def __iter__(self):return self
            def __next__(self):
                if not self.once:
                    self.once=True;return '{"id":1,"result":{}}\n'
                self.blocked.set();self.release.wait(3);raise StopIteration
        stream=BlockingStream();refs=[]
        def decode(raw):
            obj=WeakDict(id=1,result={'payload':'x'*65536})
            refs.append(weakref.ref(obj));return obj
        c=object.__new__(AppServerClient)
        c.process=types.SimpleNamespace(stdout=stream)
        c.response_queue=queue.Queue();c.notifications=queue.Queue()
        with patch('jarvis_runtime.jarvis_native_task_launcher.json.loads',side_effect=decode):
            reader=threading.Thread(target=c._read_stdout)
            reader.start()
            try:
                got=c.response_queue.get(timeout=1);del got
                self.assertTrue(stream.blocked.wait(1));gc.collect()
                self.assertTrue(refs[0]() is None, 'reader still owns the last consumed JSON response')
            finally:
                stream.release.set();reader.join(2)
            self.assertFalse(reader.is_alive())
            self.assertIsNone(c.notifications.get(timeout=1))


if __name__=='__main__':unittest.main()
