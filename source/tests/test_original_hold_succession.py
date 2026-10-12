"""Synthetic-only original Hold recovery contracts; never a production unlock."""
import io
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from adapters.codex_app_server import task_provisioning_adapter as provisioning
from adapters.codex_app_server import jarvis_task_hold_host as holder
from adapters.codex_app_server.jarvis_hold_host_service import JarvisHoldHost
from jarvis_control.provisioning import TaskMonitorResumeRequest
from jarvis_runtime.cancellation import cancel_management, commit_dispatch, DispatchAlreadyCommitted
from jarvis_runtime.jarvis_native_task_launcher import AppServerClient


def put(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


class OriginalBackendFixture:
    """Trusted constructor dependency with controlled evidence, not caller data."""
    def __init__(self):
        self.inspected=[]; self.paused=[]; self.barrier=None; self.on_pause=None; self.on_inspect=None
        self.revision='revision-1'; self.unknown=False; self.pending_settled=True; self.durable=True
    def recovery_capabilities(self):
        return {'owner_independent_execution_v1', 'compare_pause_and_fence_v1', 'prior_dispatch_settlement_v1',
                'durable_operation_fencing_v1'}
    def inspect_execution(self, identity):
        self.inspected.append(dict(identity))
        if self.barrier: self.barrier.wait(3)
        if self.on_inspect:self.on_inspect()
        return {**identity, 'source':'original_backend', 'owner_independent':True,
                'query_complete':True, 'execution_state':'unknown' if self.unknown else 'stopped',
                'no_active_turns':None if self.unknown else True, 'evidence_token':self.revision,
                'pending_dispatch_settled':self.pending_settled,
                **({'observer_status':'interrupted','completedAt':None} if self.unknown else {})}
    def pause_if_unchanged(self, identity, token):
        self.paused.append(dict(identity))
        if self.on_pause: self.on_pause()
        if token != self.revision: return {**identity,'execution_state':'unknown'}
        return {**identity,'source':'original_backend','owner_independent':True,'query_complete':True,
                'execution_state':'stopped','no_active_turns':True,'evidence_token':token,
                'original_generation_fenced':True,'pending_dispatch_settled':self.pending_settled,
                'durable_operation_fence':self.durable,
                'operation_request_id':identity.get('operation_request_id')}


class OriginalHoldSuccessionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.state=Path(self.tmp.name); self.root=self.state/'task-holds'/'hold'
        self.request={'request_id':'old','hold_id':'hold','thread_id':'thread','mode':'resume',
                      'prompt':'synthetic','source_ref':'TEST','max_turns':5,'auto_continue':False,
                      'input_binding':{},'notifications':{}}
        self.ack={**{k:self.request[k] for k in ('request_id','hold_id','thread_id')},
                  'turn_id':'turn-old','status':'holding','phase':'turn_holding','total_turn_count':2,'max_turns':5}
        put(self.root/'request.json',self.request); put(self.root/'ack.json',self.ack)
        put(self.root/'.user-host-claim'/'owner.json',{'pid':987654,'host_instance_id':'old-host'})
        self.owner=(self.root/'.user-host-claim'/'owner.json').read_bytes()
        self.config=SimpleNamespace(resolve_project=lambda p:(p,self.state),live_creation_enabled=True)
        self.backend=OriginalBackendFixture()
    def adapter(self, backend=None):
        return provisioning.CodexAppServerTaskProvisioningAdapter(self.state/'absent-config',state_dir=self.state,
             config_loader=lambda _:self.config,recovery_backend=backend)
    def incoming(self, request_id='new', **kwargs):
        return TaskMonitorResumeRequest(request_id=request_id,task_id='thread',hold_id='hold',prompt=kwargs.pop('prompt','synthetic resume'),source_ref='TEST',**kwargs)
    def original_bytes(self):
        return {name:(self.root/name).read_bytes() for name in ('request.json','ack.json')}

    def test_independent_interrupted_null_never_transfers_original_owner(self):
        self.backend.unknown=True; before=self.original_bytes()
        reply=self.adapter(self.backend).resume_with_monitor(self.incoming())
        self.assertEqual(reply.status,'requires_readback')
        self.assertEqual(reply.error_code,'execution_unverified')
        self.assertEqual(self.backend.paused,[])
        self.assertEqual(self.original_bytes(),before)
        self.assertEqual((self.root/'.user-host-claim'/'owner.json').read_bytes(),self.owner)

    def test_missing_original_backend_is_structured_and_has_no_new_call_or_claim(self):
        before=self.original_bytes()
        with patch('subprocess.Popen',side_effect=AssertionError('no new Host')), \
             patch.object(provisioning,'_initialize_user_host',side_effect=AssertionError('no new Host')):
            reply=self.adapter().resume_with_monitor(self.incoming(input_binding={'authoritative_evidence':{'no_active_turns':True}}))
        self.assertEqual(reply.status,'requires_readback')
        self.assertEqual(reply.error_code,'capability_missing')
        self.assertIn('owner_independent',reply.reason)
        self.assertEqual(self.original_bytes(),before)
        self.assertEqual((self.root/'.user-host-claim'/'owner.json').read_bytes(),self.owner)
        self.assertFalse((self.root/'history').exists())

    def test_stopped_authoritative_generation_resumes_same_thread_and_preserves_old_lease(self):
        self.request.pop('thread_id');self.request['mode']='create';put(self.root/'request.json',self.request)
        with patch('os.kill',side_effect=AssertionError('old owner must not revive')):
            reply=self.adapter(self.backend).resume_with_monitor(self.incoming())
        self.assertEqual(reply.status,'accepted', reply.reason)
        current=json.loads((self.root/'request.json').read_text())
        self.assertEqual(current['request_id'],'new');self.assertEqual(current['thread_id'],'thread')
        history=self.root/'history'/'old'
        self.assertEqual(json.loads((history/'request.json').read_text())['request_id'],'old')
        self.assertEqual((history/'.user-host-claim'/'owner.json').read_bytes(),self.owner)
        self.assertFalse(current['predecessor_hold_released'])
        self.assertEqual(len(self.backend.paused),1)
        self.assertFalse(self.adapter(self.backend).hold_status('hold')['hold_released'])

    def test_query_to_handoff_identity_or_backend_revision_change_is_rejected(self):
        for change in ('local_identity','backend_revision'):
            with self.subTest(change=change):
                self.setUp()
                if change=='local_identity':
                    self.backend.on_pause=lambda:put(self.root/'request.json',{**self.request,'request_id':'unrelated'})
                else: self.backend.on_pause=lambda:setattr(self.backend,'revision','changed')
                reply=self.adapter(self.backend).resume_with_monitor(self.incoming())
                self.assertEqual(reply.status,'requires_readback')
                self.assertNotEqual(json.loads((self.root/'request.json').read_text())['request_id'],'new')
                self.assertFalse((self.root/'history').exists())

    def test_two_resume_requests_admit_only_one_and_native_commit_is_once(self):
        self.backend.barrier=threading.Barrier(2); adapter=self.adapter(self.backend); replies=[]
        threads=[threading.Thread(target=lambda name=name:replies.append(adapter.resume_with_monitor(self.incoming(name)))) for name in ('new-a','new-b')]
        for thread in threads:thread.start()
        for thread in threads:thread.join(4);self.assertFalse(thread.is_alive())
        self.assertEqual(sum(r.status=='accepted' for r in replies),1)
        self.assertEqual(len(self.backend.paused),1)
        request=json.loads((self.root/'request.json').read_text())
        native=object.__new__(AppServerClient);native.process=SimpleNamespace(poll=lambda:None,stdin=io.StringIO())
        native.config=self.config  # Constructor contract; keep live admission and commit-once gates.
        native._write_lock=threading.Lock();native.before_turn_dispatch=lambda params:commit_dispatch(self.root,request,params)
        command={'id':1,'method':'turn/start','params':{'threadId':'thread','clientUserMessageId':request['request_id']}}
        native._write(command)
        with self.assertRaises(DispatchAlreadyCommitted):native._write(command)
        self.assertEqual(len(native.process.stdin.getvalue().splitlines()),1)

    def test_delayed_old_report_and_final_receipt_cannot_overwrite_successor(self):
        request={**self.request,'mode':'resume'};put(self.root/'request.json',request)
        new_request={**request,'request_id':'new'};new_ack={**self.ack,'request_id':'new'}
        new_result={**new_ack,'status':'completed','terminal_confirmed':True}
        class Client:
            runtime_context={};process=None
            def __init__(self,*_):self.before_turn_dispatch=None
            def resume_turn_async(_self,*args,**kwargs):
                put(self.root/'request.json',new_request);put(self.root/'ack.json',new_ack);put(self.root/'result.json',new_result)
                kwargs['on_phase']('turn_started',{'thread_id':'thread','turn_id':'turn-old'})
                return {'turn_id':'turn-old'}
            def close(self):pass
        with patch.object(holder,'NativeTaskLauncherConfig',lambda _:SimpleNamespace(jarvis_connection=None)), \
             patch.object(holder,'route_for_model',lambda *a,**k:None),patch.object(holder,'AppServerClient',Client):
            holder.hold_task(self.state/'absent-config',self.root/'request.json',self.root/'ack.json',self.root/'result.json')
        self.assertEqual(json.loads((self.root/'ack.json').read_text()),new_ack)
        self.assertEqual(json.loads((self.root/'result.json').read_text()),new_result)
        self.assertFalse(holder._publish_generation(self.root/'result.json',{'request_id':'old','hold_id':'hold','status':'failed'},request))
        self.assertEqual(json.loads((self.root/'result.json').read_text()),new_result)

    def test_user_cancel_is_not_recoverable_pause_and_cannot_be_bypassed(self):
        cancel_management(self.root,scope='hold',subject_id='hold',request_id='old',close_request_id='cancel',original=(self.root/'request.json').read_bytes())
        before=self.original_bytes();reply=self.adapter(self.backend).resume_with_monitor(self.incoming())
        self.assertNotEqual(reply.status,'accepted');self.assertEqual(self.backend.inspected,[])
        self.assertEqual(self.backend.paused,[]);self.assertEqual(self.original_bytes(),before)
        self.assertTrue((self.root/'cancellation.json').exists())

    def test_confirmed_stop_is_logically_paused_but_does_not_release_physical_hold(self):
        self.request.pop('thread_id');self.request['mode']='create';put(self.root/'request.json',self.request)
        host=object.__new__(JarvisHoldHost);host.requests_roots=(self.state/'task-holds',self.state/'task-monitors')
        host._recovery_backend=self.backend
        reply=host.reconcile_hold('hold')
        self.assertEqual(reply['status'],'paused');self.assertFalse(reply['logical_running'])
        self.assertFalse(reply['hold_released'])
        self.assertEqual((self.root/'.user-host-claim'/'owner.json').read_bytes(),self.owner)
        status=self.adapter(self.backend).hold_status('hold')
        self.assertEqual(status['status'],'paused');self.assertFalse(status['logical_running']);self.assertFalse(status['hold_released'])

    def test_unsettled_old_committed_dispatch_never_admits_successor(self):
        commit_dispatch(self.root,self.request,{'threadId':'thread','clientUserMessageId':'old-pending'})
        self.backend.pending_settled=False
        before=self.original_bytes();reply=self.adapter(self.backend).resume_with_monitor(self.incoming())
        self.assertEqual(reply.status,'requires_readback')
        self.assertEqual(reply.error_code,'old_generation_dispatch_unresolved')
        self.assertEqual(self.backend.paused,[]);self.assertEqual(self.original_bytes(),before)
        self.assertFalse((self.root/'history').exists())

    def test_transient_stopped_observation_is_not_a_durable_transfer_fence(self):
        self.backend.durable=False;before=self.original_bytes()
        reply=self.adapter(self.backend).resume_with_monitor(self.incoming())
        self.assertEqual(reply.status,'requires_readback')
        self.assertEqual(reply.error_code,'recovery_fence_unverified')
        self.assertEqual(self.original_bytes(),before)
        self.assertFalse((self.root/'history').exists())

    def test_legacy_request_lock_needs_safe_retirement_capability_not_pid_revival(self):
        put(self.root/'request.lock',{'pid':987654,'acquired_at':'old'})
        before=(self.root/'request.lock').read_bytes()
        reply=self.adapter(self.backend).resume_with_monitor(self.incoming())
        self.assertEqual(reply.status,'requires_readback');self.assertEqual(reply.error_code,'capability_missing')
        self.assertIn('legacy_request_lock',reply.reason)
        self.assertEqual((self.root/'request.lock').read_bytes(),before)
        self.assertEqual(self.backend.paused,[])

    def test_old_commit_appearing_after_query_invalidates_admission_basis(self):
        self.backend.on_inspect=lambda:commit_dispatch(self.root,self.request,{'threadId':'thread','clientUserMessageId':'after-query'})
        reply=self.adapter(self.backend).resume_with_monitor(self.incoming())
        self.assertEqual(reply.status,'requires_readback');self.assertEqual(reply.error_code,'identity_changed')
        self.assertEqual(self.backend.paused,[])
        self.assertEqual(json.loads((self.root/'request.json').read_text())['request_id'],'old')
        self.assertFalse((self.root/'history').exists())

    def test_old_host_final_publish_release_and_close_report_do_not_touch_successor(self):
        (self.root/'.user-host-claim'/'owner.json').unlink();(self.root/'.user-host-claim').rmdir()
        put(self.root/'ack.json',{**self.ack,'status':'accepted','phase':'queued_for_user_host'})
        host=object.__new__(JarvisHoldHost)
        host.requests_roots=(self.state/'task-holds',self.state/'task-monitors')
        host.instance_id='old-host';host.launcher_config=self.state/'absent-config'
        host._write_health=Mock();host._mark_active=Mock();host._mark_inactive=Mock()
        host._reserve_slot=Mock(return_value=True)  # This fixture isolates late publication, not capacity accounting.
        expected={}
        def delayed(*args):
            old_claim=self.root/'.user-host-claim';old_claim.rename(self.root/'retired-old-claim')
            put(self.root/'.user-host-claim'/'owner.json',{'pid':123456,'host_instance_id':'new-host','request_id':'new'})
            put(self.root/'request.json',{**self.request,'request_id':'new'})
            put(self.root/'ack.json',{**self.ack,'request_id':'new'})
            put(self.root/'result.json',{**self.ack,'request_id':'new','status':'completed','terminal_confirmed':True})
            put(self.root/'close.json',{'request_id':'new','report_status':'pending'})
            for name in ('request.json','ack.json','result.json','close.json','.user-host-claim/owner.json'):
                expected[name]=(self.root/name).read_bytes()
            raise RuntimeError('old holder returned after successor admission')
        with patch('adapters.codex_app_server.jarvis_hold_host_service.hold_task',side_effect=delayed):
            self.assertTrue(host.run_once())
        for name,raw in expected.items():self.assertEqual((self.root/name).read_bytes(),raw,name)

    def test_reconcile_owner_lost_has_no_observer_release_without_original_capability(self):
        result={**self.ack,'status':'failed','terminal_confirmed':False,'execution_evidence':'unknown'}
        put(self.root/'result.json',result)
        before={name:(self.root/name).read_bytes() for name in ('request.json','ack.json','result.json','.user-host-claim/owner.json')}
        host=object.__new__(JarvisHoldHost);host.requests_roots=(self.state/'task-holds',self.state/'task-monitors')
        with patch('adapters.codex_app_server.jarvis_hold_host_service.AppServerClient',side_effect=AssertionError('independent observer forbidden')):
            reply=host.reconcile_hold('hold')
        self.assertEqual(reply['status'],'requires_readback');self.assertEqual(reply['error_code'],'capability_missing')
        self.assertFalse(reply['hold_released']);self.assertFalse(reply['terminal_confirmed'])
        for name,raw in before.items():self.assertEqual((self.root/name).read_bytes(),raw,name)
        self.assertFalse((self.root/'request.lock').exists())

    def test_owned_terminal_and_actual_holder_client_exit_keeps_legacy_cleanup(self):
        terminal={**self.ack,'status':'interrupted','terminal_confirmed':True,'host_stop':True,
                  'holder_exit_confirmed':True,'holder_client_exit_confirmed':True,
                  'owner_terminal_readback':{'thread_id':'thread','turn_id':'turn-old','status':'interrupted'}}
        put(self.root/'ack.json',terminal);put(self.root/'result.json',terminal)
        host=object.__new__(JarvisHoldHost);host.requests_roots=(self.state/'task-holds',self.state/'task-monitors')
        with patch('adapters.codex_app_server.jarvis_hold_host_service.AppServerClient',side_effect=AssertionError('owner already proved closure')):
            reply=host.reconcile_hold('hold')
        self.assertEqual(reply['status'],'interrupted');self.assertTrue(reply['terminal_confirmed'])
        self.assertTrue(reply['hold_released']);self.assertFalse((self.root/'.user-host-claim').exists())

    def test_retired_resume_request_retries_read_history_without_new_admission(self):
        adapter=self.adapter(self.backend)
        self.assertEqual(adapter.resume_with_monitor(self.incoming('first')).status,'accepted')
        current=json.loads((self.root/'request.json').read_text())
        put(self.root/'ack.json',{'request_id':'first','hold_id':'hold','thread_id':'thread','turn_id':'first-turn',
             'status':'holding','total_turn_count':3,'max_turns':5})
        self.assertEqual(adapter.resume_with_monitor(self.incoming('second')).status,'accepted')
        before=self.original_bytes();queries=len(self.backend.inspected);pauses=len(self.backend.paused)
        replay=adapter.resume_with_monitor(self.incoming('first'))
        self.assertEqual(replay.status,'requires_readback');self.assertEqual(replay.phase,'historical_readback')
        self.assertEqual(replay.request_id,'first');self.assertEqual(replay.turn_id,'first-turn')
        self.assertEqual(self.original_bytes(),before)
        self.assertEqual(len(self.backend.inspected),queries);self.assertEqual(len(self.backend.paused),pauses)

    def test_interrupted_local_transfer_retries_finish_only_the_same_operation(self):
        for point in ('archive_mkdir','claim_archived','new_request_written'):
            with self.subTest(point=point):
                self.setUp();adapter=self.adapter(self.backend);fired=False
                mkdir=Path.mkdir;rename=Path.rename;write=provisioning._persist_transfer_json
                def injected_mkdir(path,*args,**kwargs):
                    nonlocal fired
                    value=mkdir(path,*args,**kwargs)
                    if point=='archive_mkdir' and path==self.root/'history'/'old' and not fired:
                        fired=True;raise OSError('injected after archive mkdir')
                    return value
                def injected_rename(path,*args,**kwargs):
                    nonlocal fired
                    value=rename(path,*args,**kwargs)
                    if point=='claim_archived' and path==self.root/'.user-host-claim' and not fired:
                        fired=True;raise OSError('injected after claim archival')
                    return value
                def injected_write(path,value):
                    nonlocal fired
                    answer=write(path,value)
                    if point=='new_request_written' and path==self.root/'request.json' and value.get('request_id')=='new' and not fired:
                        fired=True;raise OSError('injected before new ack')
                    return answer
                with patch.object(Path,'mkdir',injected_mkdir),patch.object(Path,'rename',injected_rename),patch.object(provisioning,'_persist_transfer_json',injected_write):
                    first=adapter.resume_with_monitor(self.incoming())
                self.assertTrue(fired);self.assertNotEqual(first.status,'accepted')
                queries,pauses=len(self.backend.inspected),len(self.backend.paused)
                restarted=self.adapter(self.backend)
                final=restarted.resume_with_monitor(self.incoming())
                self.assertEqual(final.status,'accepted')
                self.assertEqual(json.loads((self.root/'request.json').read_text())['request_id'],'new')
                self.assertEqual(json.loads((self.root/'ack.json').read_text())['request_id'],'new')
                self.assertEqual(len(self.backend.inspected),queries);self.assertEqual(len(self.backend.paused),pauses)
                self.assertEqual((self.root/'history'/'old'/'.user-host-claim'/'owner.json').read_bytes(),self.owner)
                before=self.original_bytes();conflict=restarted.resume_with_monitor(self.incoming(prompt='conflicting prompt'))
                self.assertNotEqual(conflict.status,'accepted');self.assertEqual(self.original_bytes(),before)

    def test_owned_exit_proof_never_clears_conflicting_thread_or_new_owner(self):
        for conflict in ('request_thread','owner_request'):
            with self.subTest(conflict=conflict):
                self.setUp()
                terminal={**self.ack,'status':'interrupted','terminal_confirmed':True,
                          'holder_exit_confirmed':True,'holder_client_exit_confirmed':True,
                          'owner_terminal_readback':{'thread_id':'thread','turn_id':'turn-old','status':'interrupted'}}
                put(self.root/'ack.json',terminal);put(self.root/'result.json',terminal)
                if conflict=='request_thread':put(self.root/'request.json',{**self.request,'thread_id':'another-thread'})
                else:put(self.root/'.user-host-claim'/'owner.json',{'pid':987654,'request_id':'another-owner'})
                before=(self.root/'.user-host-claim'/'owner.json').read_bytes()
                host=object.__new__(JarvisHoldHost);host.requests_roots=(self.state/'task-holds',self.state/'task-monitors')
                answer=host.reconcile_hold('hold')
                self.assertEqual(answer['status'],'requires_readback');self.assertEqual(answer['error_code'],'identity_unverified')
                self.assertFalse(answer['hold_released']);self.assertEqual((self.root/'.user-host-claim'/'owner.json').read_bytes(),before)

    def test_original_recovery_rejects_a_claim_from_another_generation_before_query(self):
        put(self.root/'.user-host-claim'/'owner.json',{'request_id':'another-owner','pid':987654})
        before=self.original_bytes()
        reply=self.adapter(self.backend).resume_with_monitor(self.incoming())
        self.assertEqual(reply.status,'requires_readback');self.assertEqual(reply.error_code,'identity_unverified')
        self.assertEqual(self.backend.inspected,[]);self.assertEqual(self.backend.paused,[])
        self.assertEqual(self.original_bytes(),before)

    def test_partial_transfer_retry_rejects_corruption_new_claim_cancel_or_missing_durable_contract(self):
        for conflict in ('archive_content','new_claim','cancel','durable_capability'):
            with self.subTest(conflict=conflict):
                self.setUp();rename=Path.rename;fired=False
                def interrupt(path,*args,**kwargs):
                    nonlocal fired
                    answer=rename(path,*args,**kwargs)
                    if path==self.root/'.user-host-claim' and not fired:
                        fired=True;raise OSError('interrupt after claim archive')
                    return answer
                with patch.object(Path,'rename',interrupt):
                    self.assertNotEqual(self.adapter(self.backend).resume_with_monitor(self.incoming()).status,'accepted')
                self.assertTrue(fired)
                if conflict=='archive_content':put(self.root/'history'/'old'/'ack.json',{'request_id':'foreign'})
                elif conflict=='new_claim':put(self.root/'.user-host-claim'/'owner.json',{'request_id':'foreign','pid':123456})
                elif conflict=='cancel':cancel_management(self.root,scope='hold',subject_id='hold',request_id='old',close_request_id='cancel',original=(self.root/'request.json').read_bytes())
                else:self.backend.recovery_capabilities=lambda:{'owner_independent_execution_v1','compare_pause_and_fence_v1','prior_dispatch_settlement_v1'}
                before=self.original_bytes();queries,pauses=len(self.backend.inspected),len(self.backend.paused)
                answer=self.adapter(self.backend).resume_with_monitor(self.incoming())
                self.assertNotEqual(answer.status,'accepted')
                self.assertEqual(self.original_bytes(),before)
                self.assertEqual(len(self.backend.inspected),queries);self.assertEqual(len(self.backend.paused),pauses)
                self.assertEqual((self.root/'history'/'old'/'.user-host-claim'/'owner.json').read_bytes(),self.owner)

    def test_partial_transfer_changed_target_bytes_are_rejected(self):
        for field in ('request_prompt','request_thread','ack_hold'):
            with self.subTest(field=field):
                self.setUp();mkdir=Path.mkdir;fired=False
                def interrupt(path,*args,**kwargs):
                    nonlocal fired
                    value=mkdir(path,*args,**kwargs)
                    if path==self.root/'history'/'old' and not fired:
                        fired=True;raise OSError('interrupt after archive mkdir')
                    return value
                with patch.object(Path,'mkdir',interrupt):
                    self.assertNotEqual(self.adapter(self.backend).resume_with_monitor(self.incoming()).status,'accepted')
                self.assertTrue(fired)
                paused=json.loads((self.root/'request.json').read_text())
                transfer=paused['resume_pause']['transfer']
                if field=='request_prompt':transfer['target_request']['prompt']='corrupted target'
                elif field=='request_thread':transfer['target_request']['thread_id']='another-thread'
                else:transfer['target_ack']['hold_id']='another-hold'
                put(self.root/'request.json',paused)
                before=self.original_bytes();queries,pauses=len(self.backend.inspected),len(self.backend.paused)
                answer=self.adapter(self.backend).resume_with_monitor(self.incoming())
                self.assertEqual(answer.status,'requires_readback')
                self.assertEqual(self.original_bytes(),before)
                self.assertEqual(len(self.backend.inspected),queries);self.assertEqual(len(self.backend.paused),pauses)

    def test_same_resume_request_retries_observe_original_admission(self):
        adapter=self.adapter(self.backend);first=adapter.resume_with_monitor(self.incoming());self.assertEqual(first.status,'accepted')
        before=self.original_bytes();second=adapter.resume_with_monitor(self.incoming())
        self.assertEqual(second.status,'accepted');self.assertEqual(self.original_bytes(),before)
        self.assertEqual(len(self.backend.inspected),1);self.assertEqual(len(self.backend.paused),1)


if __name__=='__main__':unittest.main()
