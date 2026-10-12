"""Cold-caller metadata protocol fixtures; no real process, Host, or model."""
import asyncio
import json
import queue
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from jarvis_codex_bridge import ExistingThreadBridge, ThreadState, TurnState
from jarvis_control.service import JarvisControl
from jarvis_mcp.server import JarvisMcpServer
from jarvis_runtime import jarvis_heartbeat_service as heartbeat
from jarvis_runtime.jarvis_native_task_launcher import AppServerClient, NativeRpcError
from adapters.codex_app_server.task_provisioning_adapter import _validate_original_evidence, _OriginalRecoveryBlocked


def turn(ident='turn-8',status='inProgress'):
    return {'id':ident,'status':status,'error':None,'items':[],'itemsView':'notLoaded'}


class LoadLatestActivityTests(unittest.TestCase):
    def setUp(self):
        guard=patch('subprocess.Popen',side_effect=AssertionError('real process forbidden'))
        guard.start();self.addCleanup(guard.stop)

    def native(self, *, initial='not_found', load_error=None, load_id='thread', read_id='thread', load_status='idle',
               still_not_loaded=False, native_status='active', pages=None, after_load_error=None):
        client=object.__new__(AppServerClient)
        client.config=SimpleNamespace(request_timeout_seconds=.2,turn_readback_timeout_seconds=.2,
            default_model='must-not-send',service_tier='must-not-send',environments=['must-not-send'])
        client.response_queue=queue.Queue();client.stderr_lines=[];client._request_id=0
        client._write_lock=threading.Lock();client.before_turn_dispatch=None
        client.calls=[];client.loaded=initial=='ready';client.closed=False
        client.runtime_context={'sentinel':'unchanged'}
        client.start=lambda **kw:None;client.close=lambda:setattr(client,'closed',True)
        sequence=pages or [{'data':[turn(),turn('turn-5','completed')]}]
        client.page_index=0
        class Sink:
            def write(self,raw):
                call=json.loads(raw);client.calls.append(call);method=call['method'];params=call['params']
                error=None
                if method=='thread/resume':
                    self_outer.assertEqual(params,{'threadId':'thread','excludeTurns':True})
                    if load_error:error={'code':-32000,'message':load_error}
                    else:
                        client.loaded=True;result={'thread':{'id':load_id,'turns':[]}}
                        if load_status is not None:result['thread']['status']={'type':load_status}
                elif method=='thread/read':
                    self_outer.assertEqual(params['threadId'],'thread')
                    if client.loaded and after_load_error:
                        error={'code':-32000,'message':after_load_error}
                    elif not client.loaded and initial in {'not_found','not_loaded_error','permission'}:
                        message={'not_found':'thread not found: thread','not_loaded_error':'thread not loaded: thread',
                                 'permission':'permission denied'}[initial]
                        error={'code':-32000,'message':message}
                    else:
                        status='notLoaded' if not client.loaded or still_not_loaded else native_status
                        result={'thread':{'id':read_id,'status':{'type':status},'updatedAt':8}}
                        if params.get('includeTurns') is True:
                            result['thread']['turns']=[{'id':'turn-5','status':'completed','items':[]}]
                elif method=='thread/turns/list':
                    self_outer.assertEqual(params['itemsView'],'notLoaded');self_outer.assertEqual(params['limit'],50)
                    result=sequence[min(client.page_index,len(sequence)-1)];client.page_index+=1
                    if isinstance(result,Exception):raise result
                else:raise AssertionError('forbidden RPC: '+method)
                client.response_queue.put({'id':call['id'],'error':error} if error else {'id':call['id'],'result':result})
            def flush(self):pass
        self_outer=self;client.process=SimpleNamespace(stdin=Sink(),poll=lambda:None)
        return client

    def read(self,client,**kwargs):
        result=client.read_thread_activity('thread',deadline=time.monotonic()+1,**kwargs)
        self.assertTrue(all(c['method'] in {'thread/read','thread/resume','thread/turns/list'} for c in client.calls))
        self.assertEqual(client.runtime_context,{'sentinel':'unchanged'})
        return result

    def test_explicit_metadata_load_only(self):
        for status,expected in [('idle',True),('active',True),('notLoaded',False),('systemError',None),(None,None)]:
            with self.subTest(load_status=status):
                client=self.native(load_status=status)
                loaded=client.load_thread_for_read('thread',deadline=time.monotonic()+1)
                self.assertIs(loaded['loaded'],expected);self.assertTrue(loaded['load_acknowledged'])
                self.assertIsNone(loaded['owner_reattached'])
                self.assertEqual([c['method'] for c in client.calls],['thread/resume'])
                self.assertEqual(client.runtime_context,{'sentinel':'unchanged'})
        for status,expected in [('notLoaded',False),('systemError',None),(None,None)]:
            with self.subTest(ack_then_read_failure=status):
                client=self.native(load_status=status,after_load_error='permission denied after load')
                got=self.read(client,load_for_read=True)
                self.assertIs(got['caller_read']['loaded'],expected)
                self.assertFalse(got['caller_read']['readable'])
                self.assertEqual(sum(c['method']=='thread/resume' for c in client.calls),1)

    def test_notloaded_load_once(self):
        for initial in ('not_loaded_status','not_loaded_error'):
            with self.subTest(initial=initial):
                client=self.native(initial=initial);got=self.read(client,load_for_read=True)
                self.assertEqual(got['status'],'active');self.assertEqual(got['latest_turn_id'],'turn-8')
                self.assertEqual(sum(c['method']=='thread/resume' for c in client.calls),1)
                self.assertTrue(got['caller_read']['readable']);self.assertTrue(got['caller_read']['loaded'])

    def test_explicit_load_resolves_cold_notfound(self):
        passive=self.read(self.native())
        self.assertEqual(passive['status'],'unknown');self.assertFalse(passive['caller_read']['load_attempted'])
        client=self.native();got=self.read(client,load_for_read=True)
        self.assertEqual(got['status'],'active');self.assertFalse(got['no_active_turns'])
        self.assertEqual(got['caller_read']['source'],'current_caller_metadata')
        self.assertIsNone(got['caller_read']['owner_reattached']);self.assertIsNone(got['caller_read']['physical_release'])

    def test_readable_no_load(self):
        client=self.native(initial='ready');got=self.read(client,load_for_read=True)
        self.assertTrue(got['caller_read']['readable']);self.assertTrue(got['caller_read']['loaded'])
        self.assertFalse(got['caller_read']['load_attempted'])
        self.assertFalse(any(c['method']=='thread/resume' for c in client.calls))

    def test_rollout_missing(self):
        client=self.native(load_error='no rollout found for thread thread');got=self.read(client,load_for_read=True)
        self.assertEqual(got['caller_read']['error']['code'],'rollout_unavailable')
        self.assertIsNone(got['no_active_turns']);self.assertFalse(got['scan_complete'])
        self.assertEqual([c['method'] for c in client.calls],['thread/read','thread/resume'])

    def test_placement_unsupported(self):
        client=self.native(load_error='placement is unsupported');got=self.read(client,load_for_read=True)
        self.assertEqual(got['caller_read']['error']['code'],'placement_unsupported')
        self.assertFalse(got['caller_read']['readable']);self.assertIsNone(got['caller_read']['physical_release'])
        self.assertEqual(sum(c['method']=='thread/resume' for c in client.calls),1)

    def test_other_error_no_retry(self):
        client=self.native(initial='permission');got=self.read(client,load_for_read=True)
        self.assertEqual(got['status'],'unknown');self.assertFalse(got['caller_read']['load_attempted'])
        self.assertEqual([c['method'] for c in client.calls],['thread/read'])
        diagnostic=client.thread_read_diagnostic(NativeRpcError('thread/read',{'code':{'unbounded':'x'*10000},'message':'permission denied'}),'thread',phase='metadata')
        self.assertIsNone(diagnostic['native_code']);self.assertLess(len(json.dumps(diagnostic)),1024)

    def test_load_identity_conflict(self):
        for where in ('load','read'):
            with self.subTest(where=where):
                client=self.native(load_id='other' if where=='load' else 'thread',read_id='other' if where=='read' else 'thread')
                got=self.read(client,load_for_read=True)
                self.assertEqual(got['caller_read']['error']['code'],'identity_unverified')
                self.assertEqual(got['status'],'unknown');self.assertIsNone(got['no_active_turns'])
                self.assertFalse(any(c['method']=='thread/turns/list' for c in client.calls))

    def test_incomplete_after_load_no_idle(self):
        client=self.native(native_status='idle',pages=[{'data':[turn('turn-8','completed')],'nextCursor':'page'},RuntimeError('page lost')])
        got=self.read(client,load_for_read=True)
        self.assertTrue(got['caller_read']['loaded']);self.assertTrue(got['caller_read']['readable'])
        self.assertFalse(got['scan_complete']);self.assertIsNone(got['no_active_turns'])
        self.assertEqual(got['status'],'unknown')

    def test_still_notloaded_no_second_load(self):
        client=self.native(still_not_loaded=True);got=self.read(client,load_for_read=True)
        self.assertEqual(got['status'],'unknown');self.assertFalse(got['caller_read']['loaded'])
        self.assertEqual(sum(c['method']=='thread/resume' for c in client.calls),1)
        self.assertFalse(any(c['method']=='thread/turns/list' for c in client.calls))

    def test_same_seen_no_duplicate_progress(self):
        client=self.native(initial='ready');baseline=self.read(client)
        same=self.read(client,last_seen=baseline)
        self.assertEqual(same['progress']['state'],'unchanged')
        changed=self.read(client,last_seen={**baseline,'latest_turn_id':'turn-5','latest_turn_status':'completed'})
        self.assertEqual(changed['progress']['state'],'changed')
        self.assertIsNone(changed['progress']['exact_turns_since_seen'])
        updated=self.read(client,last_seen={**baseline,'updated_at':7})
        self.assertEqual(updated['progress']['state'],'unchanged');self.assertTrue(updated['progress']['metadata_updated'])
        malformed=self.read(client,last_seen={'thread_id':'thread'})
        self.assertEqual(malformed['progress']['state'],'unknown')
        for bad in ({**baseline,'latest_turn_id':{}},{**baseline,'latest_turn_status':[]}):
            with self.subTest(marker_shape=bad):
                self.assertEqual(self.read(client,last_seen=bad)['progress']['state'],'unknown')
        missing=self.read(self.native(initial='ready',native_status='idle',pages=[RuntimeError('page missing')]),last_seen=baseline)
        self.assertEqual(missing['progress']['state'],'unknown')
        empty=self.read(self.native(initial='ready',native_status='idle',pages=[{'data':[]}]),last_seen=baseline)
        self.assertEqual(empty['progress']['state'],'unknown')
        positive=self.read(self.native(initial='ready',pages=[{'data':[turn()],'nextCursor':'page'},RuntimeError('later page missing')]),last_seen={**baseline,'latest_turn_id':'turn-5','latest_turn_status':'completed'})
        self.assertEqual(positive['progress']['state'],'changed');self.assertEqual(positive['status'],'active')

    def test_public_read_preserves_owner_new_active(self):
        owner={'thread_id':'thread','turn_id':'turn-5','status':'completed','source':'hold_terminal_result','hold_released':False}
        client=self.native();transport=object.__new__(heartbeat.StandardBridgeHeartbeatTransport)
        transport.launcher_config=client.config;transport.bridge=SimpleNamespace(ThreadState=ThreadState,TurnState=TurnState)
        transport.execution_reader=lambda *_:owner
        with patch.object(heartbeat,'AppServerClient',lambda _:client):
            state=transport.read_thread('thread',metadata_only=True,load_for_read=True)
        control=JarvisControl(None,ExistingThreadBridge(SimpleNamespace(read_thread=lambda _:state),None))
        server=JarvisMcpServer(control)
        got=asyncio.run(server.mcp._tool_manager.call_tool('jarvis_read',{'subject':'thread','task_id':'thread'},None)).structured_content
        self.assertEqual(got['data']['execution_status'],'inProgress')
        self.assertEqual(got['data']['execution_evidence'],owner)
        self.assertEqual(got['data']['turns'],[])
        self.assertIsNone(got['data']['remote_activity']['caller_read']['owner_reattached'])
        self.assertFalse(any(c['method']=='thread/read' and c['params'].get('includeTurns') is True for c in client.calls))
        self.assertTrue(client.closed)
        client=self.native(native_status='idle',pages=[{'data':[turn(status='completed')]}])
        with patch.object(heartbeat,'AppServerClient',lambda _:client):
            idle=transport.read_thread('thread',metadata_only=True,load_for_read=True)
        self.assertTrue(idle.remote_activity['no_active_turns'])
        self.assertEqual(idle.effective_status,'unknown');self.assertEqual(idle.execution_evidence,owner)
        self.assertIsNone(idle.remote_activity['caller_read']['owner_reattached'])
        self.assertIsNone(idle.remote_activity['caller_read']['physical_release'])
        with self.assertRaises(_OriginalRecoveryBlocked):
            _validate_original_evidence({'thread_id':'thread'},idle.remote_activity)
        for initial in ('not_loaded_error','not_found','permission'):
            with self.subTest(default_history_read=initial):
                client=self.native(initial=initial)
                with patch.object(heartbeat,'AppServerClient',lambda _:client):
                    observed=transport.read_thread('thread')
                loads=sum(c['method']=='thread/resume' for c in client.calls)
                self.assertEqual(loads,1 if initial=='not_loaded_error' else 0)
                self.assertEqual(observed.execution_evidence,owner)
                self.assertEqual(observed.turns,())
                if initial=='not_loaded_error':
                    self.assertEqual(observed.effective_status,'inProgress')
                    self.assertFalse(any(c['method']=='thread/read' and c['params'].get('includeTurns') is True for c in client.calls[1:]))
                else:
                    self.assertEqual(observed.effective_status,'unknown')
                    self.assertEqual(len(client.calls),1)


if __name__=='__main__':unittest.main()
