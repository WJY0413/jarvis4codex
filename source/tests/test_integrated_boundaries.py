"""Cross-slice cancellation/generation/read-only/capacity contracts; synthetic only."""
import errno,json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
import test_hot_capacity as cap
import test_original_hold_succession as succession
import test_load_latest_activity as load
from adapters.codex_app_server import jarvis_hold_host_service as host_module
from adapters.codex_app_server import task_provisioning_adapter as provisioning
from jarvis_runtime.cancellation import cancel_management
from jarvis_control.provisioning import TaskMonitorResumeRequest

class IntegratedBoundaries(unittest.TestCase):
    def fixture(self,cls):
        fixture=cls();fixture.setUp();self.addCleanup(fixture.doCleanups);return fixture
    def test_growth_preserves_missing_legacy_ack_ids_but_rejects_explicit_conflicting_hold(self):
        f=self.fixture(cap.HotCapacityTests);host=f.host();host.apply_capacity_request(f.command(host))
        for index,conflict in enumerate((False,True)):
            with self.subTest(conflict=conflict):
                root,req,ack=f.root(index)
                if conflict:ack['hold_id']='TEST-other-hold'
                else:ack.pop('hold_id');ack.pop('request_id')
                cap.put(root/'ack.json',ack)
                original=(root/'ack.json').read_bytes()
                claimed=host._claim(root,root/'ack.json',ack,req,host_instance_id=host.instance_id)
                self.assertIs(claimed,not conflict)
                if conflict:self.assertEqual((root/'ack.json').read_bytes(),original);self.assertFalse((root/'.user-host-claim').exists())
                else:self.assertEqual(json.loads((root/'ack.json').read_text())['hold_id'],req['hold_id'])
    def test_growth_never_requeues_cancelled_hold_or_touches_original_claim(self):
        f=self.fixture(cap.HotCapacityTests);host=f.host();root,req,ack=f.root(1)
        cancel_management(root,scope='hold',subject_id=req['hold_id'],request_id=req['request_id'],close_request_id='TEST-close',original=(root/'request.json').read_bytes())
        before={str(p):p.read_bytes() for p in root.rglob('*') if p.is_file()}
        self.assertEqual(host.apply_capacity_request(f.command(host))['status'],'applied')
        with patch.object(host_module,'hold_task',side_effect=AssertionError('no cancelled dispatch')):
            host._schedule_requests_once();self.assertFalse(host.run_once(request_root=root))
        self.assertEqual(f.created,[]);self.assertEqual(host.capacity_snapshot()['backlog_count'],0)
        for path,raw in before.items():self.assertEqual(Path(path).read_bytes(),raw)
    def test_cancel_racing_growth_preserves_original_and_denies_dispatch(self):
        f=self.fixture(cap.HotCapacityTests);host=f.host();f.busy(host);root,req,ack=f.root(31)
        original=host._sync_consumers
        def sync(**kw):
            cancel_management(root,scope='hold',subject_id=req['hold_id'],request_id=req['request_id'],close_request_id='TEST-close',original=(root/'request.json').read_bytes())
            return original(**kw)
        with patch.object(host,'_sync_consumers',side_effect=sync),patch.object(host_module,'hold_task',side_effect=AssertionError('no dispatch')):
            host.apply_capacity_request(f.command(host));self.assertFalse(host.run_once(request_root=root))
        self.assertEqual(host.capacity_snapshot()['occupied_count'],30);self.assertEqual(host.capacity_snapshot()['backlog_count'],0)
    def test_accepted_start_reservations_cannot_borrow_the_growth_seats_twice(self):
        f=self.fixture(cap.HotCapacityTests);host=f.host();f.busy(host,starting=True)
        host.apply_capacity_request(f.command(host))
        for i in range(30,45):
            root,req,ack=f.root(i);self.assertTrue(host._reserve_slot(root,req));self.assertFalse(host._reserve_slot(root,req))
        root,req,ack=f.root(99);self.assertFalse(host._reserve_slot(root,req))
        self.assertEqual(host.capacity_snapshot()['starting_reservation_count'],45)
        self.assertEqual(host.capacity_snapshot()['confirmed_active_count'],0)
    def test_late_old_generation_finally_preserves_new_claim_and_reservation(self):
        f=self.fixture(cap.HotCapacityTests);host=f.host();root,old,ack=f.root(1)
        host.apply_capacity_request(f.command(host));new={**old,'request_id':'TEST-new-generation'}
        def held(config,request_path,ack_path,result_path):
            cap.put(root/'history'/old['request_id']/'request.json',old)
            cap.put(request_path,new);cap.put(ack_path,{**ack,'request_id':new['request_id']})
            cap.put(root/'.user-host-claim/owner.json',{'pid':987654,'host_instance_id':host.instance_id,'request_id':new['request_id']})
            host._occupied_roots[root]=dict(new)
            self.assertFalse(host_module._publish_generation(result_path,{'request_id':old['request_id'],'hold_id':old['hold_id'],'status':'completed'},old))
        with patch.object(host_module,'hold_task',side_effect=held):self.assertTrue(host.run_once(request_root=root))
        self.assertEqual(json.loads((root/'.user-host-claim/owner.json').read_text())['request_id'],new['request_id'])
        self.assertEqual(host.capacity_snapshot()['occupied_count'],1)
        self.assertEqual(host._occupied_roots[root]['request_id'],new['request_id'])
        self.assertFalse((root/'result.json').exists())
    def test_partial_transfer_remains_blocked_after_growth_without_second_pause_or_consumer(self):
        f=self.fixture(succession.OriginalHoldSuccessionTests)
        config=f.state/'launcher.json';cap.put(config,{'dispatcher_thread_id':'TEST','profile':'TEST','expected_codex_home':str(f.state),'allowed_projects':{'test':str(f.state)}})
        host=host_module.JarvisHoldHost(state_dir=f.state,launcher_config=config,workers=30,worker_factory=lambda **kw:cap.FakeWorker(**kw))
        import threading
        host._setup_worker_pool(threading.Event());adapter=f.adapter(f.backend)
        original=provisioning._persist_transfer_json
        def fault(path,value):
            if path.name=='ack.json' and value.get('request_id')=='new':raise OSError('synthetic transfer interruption')
            return original(path,value)
        with patch.object(provisioning,'_persist_transfer_json',side_effect=fault):
            self.assertEqual(adapter.resume_with_monitor(f.incoming()).status,'failed')
        current=json.loads((f.root/'request.json').read_text());pause=current.get('resume_pause')
        if pause is None:pause=json.loads((f.root/'history/old/pause.json').read_text())
        pause['transfer']['target_request']['thread_id']='forged-thread'
        current['resume_pause']=pause;cap.put(f.root/'request.json',current)
        before={str(p):p.read_bytes() for p in f.root.rglob('*') if p.is_file()}
        self.assertEqual(host.apply_capacity_request({'request_id':'TEST-grow','host_instance_id':host.instance_id,'target_capacity':45,'expected_revision':0})['status'],'applied')
        reply=adapter.resume_with_monitor(f.incoming());self.assertEqual(reply.status,'requires_readback')
        self.assertEqual(reply.error_code,'identity_unverified');self.assertEqual(len(f.backend.paused),1)
        host._schedule_requests_once();self.assertEqual(host.capacity_snapshot()['backlog_count'],0)
        self.assertEqual(host._worker_threads,[])
        for path,raw in before.items():self.assertEqual(Path(path).read_bytes(),raw)
    def test_metadata_loaded_idle_cannot_upgrade_original_recovery_authority_after_growth(self):
        f=self.fixture(succession.OriginalHoldSuccessionTests);cfg=f.state/'launcher.json'
        cap.put(cfg,{'dispatcher_thread_id':'TEST','profile':'TEST','expected_codex_home':str(f.state),'allowed_projects':{'test':str(f.state)}})
        host=host_module.JarvisHoldHost(state_dir=f.state,launcher_config=cfg,workers=30,worker_factory=lambda **kw:cap.FakeWorker(**kw))
        import threading
        host._setup_worker_pool(threading.Event());host.apply_capacity_request({'request_id':'TEST-grow','host_instance_id':host.instance_id,'target_capacity':45,'expected_revision':0})
        reader=self.fixture(load.LoadLatestActivityTests);client=reader.native(initial='not_loaded_error',native_status='idle',pages=[{'data':[load.turn('turn-8','completed')]}])
        activity=reader.read(client,load_for_read=True);self.assertIs(activity['caller_read']['loaded'],True)
        before=f.original_bytes();reply=f.adapter().resume_with_monitor(f.incoming(input_binding={'authoritative_evidence':activity}))
        self.assertEqual(reply.status,'requires_readback');self.assertEqual(reply.error_code,'capability_missing')
        self.assertEqual(f.original_bytes(),before);self.assertEqual((f.root/'.user-host-claim/owner.json').read_bytes(),f.owner)
    def test_permission_unknown_growth_does_not_reap_original_owner_or_enable_recovery(self):
        f=self.fixture(succession.OriginalHoldSuccessionTests);cfg=f.state/'launcher.json'
        cap.put(f.root/'result.json',{**f.ack,'status':'failed','terminal_confirmed':False,'execution_evidence':'unknown'})
        cap.put(cfg,{'dispatcher_thread_id':'TEST','profile':'TEST','expected_codex_home':str(f.state),'allowed_projects':{'test':str(f.state)}})
        host=host_module.JarvisHoldHost(state_dir=f.state,launcher_config=cfg,workers=30,worker_factory=lambda **kw:cap.FakeWorker(**kw))
        import threading
        host._setup_worker_pool(threading.Event())
        # This scenario simulates POSIX permission failure, not a real Windows
        # PID. Keep the proxy module-local so Path and other users retain their
        # real platform, and do not substitute a liveness result.
        posix_os=Mock(wraps=host_module.os);posix_os.name='posix'
        posix_os.kill.side_effect=PermissionError(errno.EPERM,'synthetic permission')
        linux_sys=Mock(wraps=host_module.sys);linux_sys.platform='linux'
        with patch.object(host_module,'os',posix_os),patch.object(host_module,'sys',linux_sys):
            self.assertTrue(host_module._pid_is_alive(987654));host.apply_capacity_request({'request_id':'TEST-grow','host_instance_id':host.instance_id,'target_capacity':45,'expected_revision':0})
            reply=host.reconcile_hold('hold')
        posix_os.kill.assert_any_call(987654,0)
        self.assertEqual(reply['error_code'],'capability_missing');self.assertFalse(reply['hold_released'])
        self.assertEqual((f.root/'.user-host-claim/owner.json').read_bytes(),f.owner)

    def test_wrong_generation_close_report_cannot_supply_completed_or_release_flags(self):
        f=self.fixture(cap.HotCapacityTests);root,req,ack=f.root(1)
        adapter=provisioning.CodexAppServerTaskProvisioningAdapter(f.config,state_dir=f.state)
        wrong={'request_id':'TEST-other-generation','hold_id':req['hold_id'],'report_status':'completed',
               'terminal_confirmed':True,'hold_released':True,'execution_state':'terminal'}
        with patch.object(provisioning,'refresh_close_report',return_value=wrong):reply=adapter.close_hold(req['hold_id'])
        self.assertEqual(reply['request_id'],req['request_id']);self.assertEqual(reply['status'],'closed_unconfirmed')
        self.assertIs(reply['terminal_confirmed'],False);self.assertIs(reply['hold_released'],False)
