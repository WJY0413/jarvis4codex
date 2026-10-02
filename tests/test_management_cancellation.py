"""Offline product regressions. No Codex process, model, network, or live state."""
import json
import io
import threading
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from urllib.parse import quote

from jarvis_runtime.cancellation import cancel_management, cancellation_path, require_dispatch_open, report_guard, commit_dispatch, DispatchCancelledBeforeSend, DispatchAlreadyCommitted
from adapters.codex_app_server.task_provisioning_adapter import CodexAppServerTaskProvisioningAdapter
from adapters.codex_app_server.jarvis_hold_host_service import JarvisHoldHost
from adapters.codex_app_server.jarvis_task_hold_host import hold_task
from jarvis_control.loop import LoopController, LoopStore
from jarvis_control.service import JarvisControl
from jarvis_control.provisioning import TaskMonitorResumeRequest
from jarvis_runtime.jarvis_native_task_launcher import AppServerClient
from jarvis_monitor.hold_turn_monitor import HoldTurnDecision


def put(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


class ManagementCancellation(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = Path(self.tmp.name)
        self.config = SimpleNamespace(resolve_project=lambda p: (p, self.state), live_creation_enabled=True)
        self.adapter = CodexAppServerTaskProvisioningAdapter(self.state/'absent-config.json', state_dir=self.state,
            config_loader=lambda p: self.config)
        self.host = patch('adapters.codex_app_server.jarvis_hold_host_service.JarvisHoldHost')
        self.host_mock = self.host.start()
        self.addCleanup(self.host.stop)
        self.host_mock.return_value.request_host_stop.return_value = {'status':'rejected', 'reason':'owner identity unavailable'}

    def fixture(self, ident='h1', queued=False, claim=True):
        root=self.state/'task-holds'/ident
        request={'request_id':'request-'+ident, 'hold_id':ident, 'mode':'create', 'prompt':'fixture',
                 'max_turns':5,'auto_continue':True,'input_binding':{},'notifications':{}}
        ack={'request_id':request['request_id'],'hold_id':ident,'status':'accepted' if queued else 'holding',
             'phase':'queued_for_user_host' if queued else 'turn_holding'}
        if not queued: ack.update(thread_id='thread-'+ident,turn_id='turn-'+ident,pid=8)
        put(root/'request.json',request);put(root/'ack.json',ack)
        if claim:put(root/'.user-host-claim/owner.json',{'pid':8,'observed_at':'old','host_instance_id':'old-owner'})
        return root,request,ack

    def close(self, ident='h1', request_id='close-1'):
        return self.adapter.close_hold(ident,request_id=request_id)

    def test_unknown_owner_closes_management_without_fabricating_terminal(self):
        root,request,ack=self.fixture()
        original_ack=(root/'ack.json').read_bytes();owner=(root/'.user-host-claim/owner.json').read_bytes()
        with patch('os.kill',side_effect=AssertionError('PID signalling forbidden')):
            answer=self.close()
        self.assertEqual(answer['status'],'closed_unconfirmed')
        self.assertTrue(answer['close_report']['scheduling_closed'])
        self.assertFalse(answer['close_report']['terminal_confirmed'])
        self.assertFalse(answer['hold_released'])
        self.assertEqual((root/'ack.json').read_bytes(),original_ack)
        self.assertEqual((root/'.user-host-claim/owner.json').read_bytes(),owner)
        self.assertFalse((root/'result.json').exists())
        self.assertEqual(self.adapter.hold_status('h1')['management_status'],'closed_unconfirmed')

    def test_current_owner_stop_remains_closing(self):
        self.fixture()
        self.host_mock.return_value.request_host_stop.return_value={'status':'stop_requested'}
        answer=self.close()
        self.assertEqual(answer['status'],'closing')
        self.assertEqual(answer['close_report']['execution_state'],'running')
        self.assertEqual(self.adapter.hold_status('h1')['management_status'],'closing')
        self.assertFalse(answer['close_report']['terminal_confirmed'])

    def test_real_terminal_and_released_remains_closed(self):
        root,request,ack=self.fixture(claim=False)
        put(root/'result.json',{**ack,'status':'interrupted','terminal_confirmed':True})
        answer=self.close()
        self.assertEqual(answer['status'],'closed')
        self.assertTrue(answer['close_report']['terminal_confirmed'])
        self.assertTrue(answer['hold_released'])
        self.assertEqual(self.adapter.hold_status('h1')['management_status'],'closed')

    def test_repeated_close_keeps_first_durable_identity(self):
        root,_,_=self.fixture();self.close()
        first=(root/'cancellation.json').read_bytes()
        self.close(request_id='close-2')
        self.assertEqual((root/'cancellation.json').read_bytes(),first)
        self.assertEqual(json.loads(first)['close_request_id'],'close-1')

    def test_stale_pid_lock_is_not_reaped_and_cancellation_still_succeeds(self):
        root,_,_=self.fixture();put(root/'request.lock',{'pid':8,'acquired_at':'old'})
        before=(root/'request.lock').read_bytes()
        answer=self.close()
        self.assertEqual(answer['status'],'closed_unconfirmed')
        self.assertEqual((root/'request.lock').read_bytes(),before)
        self.assertIn('legacy_stop_delivery_error',answer['close_report']['last_action'])

    def test_report_lock_does_not_prevent_durable_cancellation(self):
        root,_,_=self.fixture()
        with report_guard(root/'.close-report.guard'):
            answer=self.close()
        self.assertEqual(answer['status'],'closed_unconfirmed')
        self.assertTrue((root/'cancellation.json').is_file())
        self.assertFalse(answer['terminal_confirmed'])

    def test_identity_conflict_does_not_cancel_unrelated_request(self):
        root,_,ack=self.fixture();put(root/'ack.json',{**ack,'request_id':'other'})
        with self.assertRaises(RuntimeError):self.close()
        self.assertFalse((root/'cancellation.json').exists())

    def test_host_scan_claim_and_requeue_never_dispatch_cancelled_work(self):
        root,request,ack=self.fixture();self.close()
        host=object.__new__(JarvisHoldHost)
        host.requests_roots=(self.state/'task-holds',self.state/'task-monitors');host._write_health=Mock()
        with patch('adapters.codex_app_server.jarvis_hold_host_service.hold_task',side_effect=AssertionError('dispatch forbidden')), \
             patch('adapters.codex_app_server.jarvis_hold_host_service._pid_is_alive',side_effect=AssertionError('PID lookup forbidden')):
            self.assertFalse(host.run_once())
            self.assertFalse(JarvisHoldHost._claim(root,root/'ack.json',ack,request))
            self.assertFalse(JarvisHoldHost._requeue_dead_claim(root,root/'request.json',root/'ack.json',request,ack))
        self.assertTrue((root/'.user-host-claim').exists())

    def test_malformed_cancellation_fails_closed(self):
        root,request,_=self.fixture();(root/'cancellation.json').write_text('{')
        with self.assertRaises(RuntimeError):require_dispatch_open(root,request)

    def test_cancelled_hold_cannot_be_reused_but_new_identity_can(self):
        root,_,_=self.fixture();self.close();saved=(root/'request.json').read_bytes()
        base=dict(request_id='new',project='P',title='T',prompt='new',source_ref='user',model=None,
                  reasoning_effort=None,max_turns=1,auto_continue=False,continue_prompt='continue',notifications={},input_binding={})
        denied=self.adapter.provision(SimpleNamespace(hold_id='h1',**base))
        allowed=self.adapter.provision(SimpleNamespace(hold_id='h2',**base))
        self.assertEqual(denied.status,'failed');self.assertEqual(allowed.status,'accepted')
        self.assertEqual((root/'request.json').read_bytes(),saved)

    def test_new_hold_alias_cannot_resume_cancelled_native_thread(self):
        self.fixture();self.close()
        answer=self.adapter.resume_with_monitor(TaskMonitorResumeRequest(request_id='later',task_id='thread-h1',prompt='TEST only continuation',source_ref='TEST:cancel-alias',hold_id='alias',input_binding={}))
        self.assertEqual(answer.status,'failed')
        self.assertIn('cancelled',answer.reason)

    def test_queued_holder_checks_cancellation_before_creating_a_client(self):
        root,_,_=self.fixture(queued=True,claim=False);self.close()
        with patch('adapters.codex_app_server.jarvis_task_hold_host.AppServerClient',side_effect=AssertionError('client creation forbidden')):
            status=hold_task(self.state/'absent-config',root/'request.json',root/'ack.json',root/'result.json')
        self.assertEqual(status,0)
        self.assertEqual(json.loads((root/'result.json').read_text())['phase'],'stopped_before_dispatch')

    def test_parent_loop_marker_blocks_child_without_pid_or_owner(self):
        root,request,_=self.fixture();loop_id='loop-test:colon'
        safe='loop-test_colon';parent=self.state/'loops'/safe
        cancel_management(parent,scope='loop',subject_id=loop_id,request_id='looprequest',close_request_id='close-loop',original=b'{}')
        request['source_ref']='jarvis_loop:'+quote(loop_id,safe='')+':slot1'
        self.assertEqual(cancellation_path(root,request),parent/'cancellation.json')
        with self.assertRaises(RuntimeError):require_dispatch_open(root,request)

    def test_loop_close_cancels_all_children_despite_monitor_and_heartbeat_errors(self):
        for h in ('a','b'):self.fixture(h)
        store=LoopStore(self.state/'loops');controller=LoopController(store)
        state={'schema':'jarvis-loop-state/v1','loop_id':'loop-x','request_id':'loop-r','status':'running',
            'heartbeat_id':'hb','heartbeat':{'status':'active'},'children':[{'hold_id':h,'slot':h,'last_receipt':{'status':'holding'}} for h in ('a','b')]}
        store.create('loop-x',state)
        runtime=SimpleNamespace(close_hold=self.adapter.close_hold,heartbeat=Mock(side_effect=RuntimeError('heartbeat unavailable')),
            monitor=Mock(side_effect=RuntimeError('monitor unavailable')),hold=Mock(side_effect=AssertionError('new work forbidden')))
        result=controller.close(runtime,loop_id='loop-x',request_id='close-loop')
        self.assertEqual(result.status,'closed_unconfirmed')
        self.assertTrue(result.data['close']['scheduling_closed'])
        self.assertFalse(result.data['close']['terminal_confirmed'])
        self.assertFalse(result.data['cleanup']['heartbeat_cancelled'])
        for h in ('a','b'):self.assertTrue((self.state/'task-holds'/h/'cancellation.json').exists())
        self.assertEqual(controller.tick(runtime,loop_id='loop-x').status,'closed_unconfirmed')
        runtime.hold.assert_not_called()

    def test_loop_live_child_remains_closing_and_readback_can_continue(self):
        self.fixture('a')
        self.host_mock.return_value.request_host_stop.return_value={'status':'stop_requested'}
        store=LoopStore(self.state/'loops');controller=LoopController(store)
        state={'schema':'jarvis-loop-state/v1','loop_id':'loop-live','request_id':'lr','status':'running',
            'heartbeat_id':None,'heartbeat':None,'children':[{'hold_id':'a','slot':'a','last_receipt':{'status':'holding'}}]}
        store.create('loop-live',state)
        runtime=SimpleNamespace(close_hold=self.adapter.close_hold,monitor=lambda **kw:{'status':'completed','data':{'status':'holding'}},
            hold=Mock(side_effect=AssertionError('new work forbidden')))
        result=controller.close(runtime,loop_id='loop-live')
        self.assertEqual(result.status,'stopping')
        self.assertEqual(result.data['close']['execution_state'],'running')
        self.assertTrue(result.data['close']['scheduling_closed'])
        self.assertIn('loop-live',store.active_loop_ids())
        self.assertEqual(controller.tick(runtime,loop_id='loop-live').status,'stopping')
        with self.assertRaises(RuntimeError):controller._acquire(runtime,state,{'slot':'a'})
        runtime.hold.assert_not_called()

    def test_existing_unconfirmed_loop_readback_gets_the_cancellation_record(self):
        store=LoopStore(self.state/'loops');controller=LoopController(store)
        state={'schema':'jarvis-loop-state/v1','loop_id':'loop-old','request_id':'lr','status':'closed_unconfirmed',
            'children':[],'close':{'report_status':'unresolved','close_request_id':'old-close'}}
        store.create('loop-old',state)
        result=controller.close(SimpleNamespace(),loop_id='loop-old',request_id='new-close')
        self.assertEqual(result.status,'closed_unconfirmed')
        self.assertEqual(result.data['close']['cancellation']['close_request_id'],'old-close')
        self.assertEqual(controller.status(SimpleNamespace(),loop_id='loop-old').data['close']['cancellation']['schema'],'jarvis-management-cancellation/v1')

    def test_public_close_does_not_call_management_cancel_a_terminal(self):
        self.fixture()
        control=JarvisControl(SimpleNamespace(),SimpleNamespace(),provisioner=self.adapter)
        receipt=control.jarvis_close(hold_id='h1',request_id='public-close')
        self.assertEqual(receipt['status'],'closed_unconfirmed')
        self.assertFalse(receipt['readback']['terminal'])

    def test_owning_holder_interrupts_only_its_actual_turn_once(self):
        root,request,_=self.fixture(queued=True,claim=False)
        calls=[]
        class Client:
            process=None
            def __init__(self,config):pass
            def create_task(self,request,on_phase):return {'thread_id':'owned-thread','turn_id':'owned-turn'}
            def request(self,method,params):calls.append((method,params));return {}
            def close(self):pass
        class Monitor:
            def observe(_,client,request):
                cancel_management(root,scope='hold',subject_id='h1',request_id='request-h1',close_request_id='close',original=(root/'request.json').read_bytes())
                request.control_poll();request.control_poll()
                return HoldTurnDecision('STOP','monitor:h1:owned-turn:1','h1','owned-turn','interrupted','','explicit cancellation',None,())
        with patch('adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig',return_value=SimpleNamespace(jarvis_connection=None)), \
             patch('adapters.codex_app_server.jarvis_task_hold_host.AppServerClient',Client):
            result=hold_task(self.state/'absent-config',root/'request.json',root/'ack.json',root/'result.json',monitor=Monitor())
        self.assertEqual(result,0)
        self.assertEqual(calls,[('turn/interrupt',{'threadId':'owned-thread','turnId':'owned-turn'})])


    def test_actual_host_stop_does_not_reap_an_unknown_pid_lock(self):
        root,_,_=self.fixture();put(root/'request.lock',{'pid':123456,'acquired_at':'old'})
        original=(root/'request.lock').read_bytes()
        host=object.__new__(JarvisHoldHost)
        host.requests_roots=(self.state/'task-holds',self.state/'task-monitors')
        with patch('adapters.codex_app_server.jarvis_hold_host_service._pid_is_alive',side_effect=AssertionError('must not inspect PID')):
            answer=host.request_host_stop('h1','thread-h1','turn-h1')
        self.assertEqual(answer['status'],'rejected')
        self.assertEqual((root/'request.lock').read_bytes(),original)

    def test_result_only_native_identity_still_blocks_resume_alias(self):
        root,_,ack=self.fixture(claim=False)
        put(root/'result.json',{**ack,'status':'failed','terminal_confirmed':False})
        (root/'ack.json').unlink();self.close()
        reply=self.adapter.resume_with_monitor(TaskMonitorResumeRequest(request_id='alias',task_id='thread-h1',prompt='TEST only continuation',source_ref='TEST:result-only-alias',input_binding={}))
        self.assertEqual(reply.status,'failed');self.assertIn('cancelled',reply.reason)

    def test_legacy_colon_loop_binding_observes_exact_parent_cancellation(self):
        root,request,_=self.fixture()
        parent=self.state/'loops/loop-a_b'
        put(parent/'state.json',{'loop_id':'loop-a:b','children':[{'slot':'s'}]})
        cancel_management(parent,scope='loop',subject_id='loop-a:b',request_id='lr',close_request_id='lc',original=b'{}')
        for source in ('jarvis_loop:loop-a:b:s','jarvis_loop_task:loop-a:b'):
            request['source_ref']=source
            self.assertEqual(cancellation_path(root,request),parent/'cancellation.json')

    def test_ambiguous_legacy_loop_binding_cannot_dispatch(self):
        root,request,_=self.fixture();request['source_ref']='jarvis_loop:loop-a:b:s'
        put(self.state/'loops/loop-a/state.json',{'loop_id':'loop-a','children':[{'slot':'b:s'}]})
        put(self.state/'loops/loop-a_b/state.json',{'loop_id':'loop-a:b','children':[{'slot':'s'}]})
        with self.assertRaises(RuntimeError):require_dispatch_open(root,request)

    def test_loop_path_alias_cannot_cancel_another_logical_id(self):
        store=LoopStore(self.state/'loops');controller=LoopController(store)
        store.create('loop-a:b',{'schema':'jarvis-loop-state/v1','loop_id':'loop-a:b','request_id':'r','status':'running','children':[]})
        answer=controller.close(SimpleNamespace(),loop_id='loop-a/b')
        self.assertEqual(answer.status,'failed')
        self.assertFalse((self.state/'loops/loop-a_b/cancellation.json').exists())

    def pipe_client(self, callback):
        client=object.__new__(AppServerClient)
        client.process=SimpleNamespace(poll=lambda:None,stdin=io.StringIO())
        client._write_lock=threading.Lock();client.before_turn_dispatch=callback
        return client

    def test_cancel_between_setup_and_actual_native_write_blocks_first_or_next_turn(self):
        root,request,_=self.fixture()
        client=self.pipe_client(lambda params:commit_dispatch(root,request,params))
        self.close()
        for command in ('first-command','next-command'):
            with self.assertRaises(DispatchCancelledBeforeSend):
                client._write({'id':1,'method':'turn/start','params':{'threadId':'thread-h1','clientUserMessageId':command}})
        self.assertEqual(client.process.stdin.getvalue(),'')
        self.assertFalse(list((root/'dispatch-commits').glob('*.json')))

    def test_only_a_durable_prior_commit_may_write_after_cancel(self):
        root,request,_=self.fixture()
        commits=[]
        def before(params):
            commits.append(commit_dispatch(root,request,params))
            cancel_management(root,scope='hold',subject_id='h1',request_id='request-h1',close_request_id='close',original=(root/'request.json').read_bytes())
        client=self.pipe_client(before)
        client._write({'id':1,'method':'turn/start','params':{'threadId':'thread-h1','clientUserMessageId':'command'}})
        marker=json.loads((root/'cancellation.json').read_text())
        self.assertLessEqual(commits[0]['committed_at'],marker['requested_at'])
        self.assertFalse(commits[0]['delivery_confirmed'])
        self.assertEqual(len(client.process.stdin.getvalue().splitlines()),1)

    def test_existing_commit_is_not_replayed_or_relabelled_unsent_after_cancel(self):
        root,request,_=self.fixture()
        client=self.pipe_client(lambda params:commit_dispatch(root,request,params))
        command={'id':1,'method':'turn/start','params':{'threadId':'thread-h1','clientUserMessageId':'once'}}
        client._write(command);self.close()
        with self.assertRaises(DispatchAlreadyCommitted):client._write(command)
        self.assertEqual(len(client.process.stdin.getvalue().splitlines()),1)

    def test_resume_admitted_before_close_inherits_the_old_owner_gate(self):
        old,_,_=self.fixture()
        incoming=SimpleNamespace(request_id='later',task_id='thread-h1',hold_id=None,monitor_id=None,input_binding={},
            max_turns=1,prompt='new',source_ref='user',model=None,reasoning_effort=None,auto_continue=False,continue_prompt='continue',notifications={})
        reply=self.adapter.resume_with_monitor(incoming)
        self.assertEqual(reply.status,'accepted')
        root=self.state/'task-holds'/reply.hold_id
        request=json.loads((root/'request.json').read_text())
        self.assertIn('task-holds/h1',request['dispatch_ancestors'])
        self.close('h1')
        client=self.pipe_client(lambda params:commit_dispatch(root,request,params))
        with self.assertRaises(DispatchCancelledBeforeSend):
            client._write({'id':1,'method':'turn/start','params':{'threadId':'thread-h1','clientUserMessageId':'later'}})
        self.assertEqual(client.process.stdin.getvalue(),'')

    def test_busy_dispatch_gate_does_not_claim_cancellation_was_persisted(self):
        root,_,_=self.fixture()
        control=JarvisControl(SimpleNamespace(),SimpleNamespace(),provisioner=self.adapter)
        with report_guard(root/'.dispatch-gate'):
            answer=control.jarvis_close(hold_id='h1',request_id='close-busy')
        self.assertEqual(answer['status'],'failed')
        self.assertNotIn('cancellation is durable',answer['reason'])
        self.assertFalse((root/'cancellation.json').exists())


if __name__=='__main__':unittest.main()
