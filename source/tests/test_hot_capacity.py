"""Isolated hot-growth contracts: fake consumers only, no process or model."""
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from adapters.codex_app_server import jarvis_hold_host_service as service
from adapters.codex_app_server.task_provisioning_adapter import CodexAppServerTaskProvisioningAdapter


def put(path,value):
    path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value))


class FakeWorker:
    def __init__(self,**kwargs):self.kwargs=kwargs;self.started=False
    def start(self):self.started=True
    def is_alive(self):return self.started
    def join(self,*_):pass


class HotCapacityTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.state=Path(self.tmp.name);self.config=self.state/'launcher.json'
        put(self.config,{'dispatcher_thread_id':'test','profile':'synthetic','expected_codex_home':str(self.state),
             'worker_capacity':30,'default_model':'fake','allowed_projects':{'test':str(self.state)}})
        guard=patch('subprocess.Popen',side_effect=AssertionError('no real process'))
        guard.start();self.addCleanup(guard.stop)
        self.created=[]
        def factory(**kwargs):
            worker=FakeWorker(**kwargs);self.created.append(worker);return worker
        self.factory=factory

    def host(self):
        host=service.JarvisHoldHost(state_dir=self.state,launcher_config=self.config,workers=30,worker_factory=self.factory)
        host._setup_worker_pool(threading.Event());host._write_health('ready')
        return host

    def root(self,index,*,active=False):
        root=self.state/'task-holds'/f'h{index:03}'
        req={'request_id':f'r{index}','hold_id':root.name,'mode':'create'}
        ack={'request_id':req['request_id'],'hold_id':root.name,'status':'accepted','phase':'queued_for_user_host'}
        if active:ack.update(status='holding',phase='turn_holding',thread_id=f't{index}',turn_id=f'u{index}')
        put(root/'request.json',req);put(root/'ack.json',ack)
        return root,req,ack

    def busy(self,host,count=30,*,starting=False):
        for i in range(count):
            root,req,ack=self.root(i,active=not starting)
            if starting:ack.update(phase='claimed_by_user_host');put(root/'ack.json',ack)
            put(root/'.user-host-claim'/'owner.json',{'host_instance_id':host.instance_id,'request_id':req['request_id'],'pid':987654})
            self.assertTrue(host._reserve_slot(root,req))
        # Existing consumers are already busy; no real Thread is created.
        host._worker_threads=[FakeWorker() for _ in range(count)]
        for worker in host._worker_threads:worker.start()

    def command(self,host,*,request_id='grow',target=45,revision=0,instance=None):
        return {'request_id':request_id,'host_instance_id':instance or host.instance_id,
                'target_capacity':target,'expected_revision':revision}

    def test_busy_thirty_grows_to_forty_five_without_restart_or_touching_originals(self):
        host=self.host();self.busy(host)
        before={str(p):p.read_bytes() for p in (self.state/'task-holds').rglob('*') if p.is_file()}
        with patch.object(service,'_stop_hold_host',side_effect=AssertionError('no restart')):
            result=host.apply_capacity_request(self.command(host))
        self.assertEqual(result['status'],'applied');self.assertEqual(host.workers,45)
        self.assertEqual(result['host_instance_id'],host.instance_id)
        for path,raw in before.items():self.assertEqual(Path(path).read_bytes(),raw)
        self.assertEqual(self.created,[])

    def test_only_fifteen_more_consumers_and_unique_claims_from_existing_backlog(self):
        host=self.host();self.busy(host)
        for i in range(30,60):self.root(i)
        host.apply_capacity_request(self.command(host));host._schedule_requests_once()
        self.assertEqual(len(self.created),15)
        claimed=[]
        for _ in range(15):
            root=host._pending.get_nowait();req=json.loads((root/'request.json').read_text());ack=json.loads((root/'ack.json').read_text())
            self.assertTrue(host._reserve_slot(root,req))
            self.assertTrue(host._claim(root,root/'ack.json',ack,req,host_instance_id=host.instance_id));claimed.append(root)
        host._schedule_requests_once()
        self.assertEqual(len(set(claimed)),15);self.assertEqual(host.capacity_snapshot()['occupied_count'],45)
        root,req,_=self.root(99);self.assertFalse(host._reserve_slot(root,req))

    def test_empty_pool_grows_capacity_without_consumer_or_native_work(self):
        host=self.host()
        terminal,_,_=self.root(1);put(terminal/'result.json',{'status':'completed'})
        unknown,req,ack=self.root(2);put(unknown/'ack.json',{**ack,'status':'failed','phase':'unknown'})
        foreign,req,ack=self.root(3);put(foreign/'.user-host-claim'/'owner.json',{'host_instance_id':'other','request_id':req['request_id'],'pid':987654})
        reply=host.apply_capacity_request(self.command(host));host._schedule_requests_once()
        self.assertEqual(reply['status'],'applied');self.assertEqual(self.created,[])
        self.assertEqual(host.capacity_snapshot()['backlog_count'],0)
        self.assertEqual(len(list((self.state/'task-holds').glob('*/request.json'))),3)

    def test_full_queue_still_services_capacity_and_does_not_hide_unscheduled_root(self):
        host=self.host();self.busy(host)
        for i in range(30,62):self.root(i)
        host._schedule_requests_once();self.assertTrue(host._pending.full())
        root=self.state/'task-holds'/'h061';self.assertNotIn(root,host._scheduled)
        put(self.state/'hold-host-capacity-request.json',self.command(host))
        host._service_capacity_request();host._schedule_requests_once()
        self.assertEqual(json.loads((self.state/'hold-host-capacity-result.json').read_text())['status'],'applied')
        self.assertIn(root,host._scheduled)

    def test_absolute_target_replay_never_adds_fifteen_again(self):
        host=self.host();first=host.apply_capacity_request(self.command(host))
        same=host.apply_capacity_request(self.command(host));self.assertEqual(same,first)
        self.assertEqual(host.workers,45);self.assertEqual(host.capacity_snapshot()['capacity_revision'],1)

    def test_stale_instance_and_revision_conflicts_are_readable_and_do_not_apply(self):
        host=self.host()
        stale=host.apply_capacity_request(self.command(host,instance='old-host'))
        self.assertEqual(stale['error_code'],'stale_host_instance');self.assertEqual(host.workers,30)
        self.assertEqual(host.apply_capacity_request(self.command(host))['status'],'applied')
        conflict=host.apply_capacity_request(self.command(host,request_id='other',target=60))
        self.assertEqual(conflict['error_code'],'capacity_conflict');self.assertEqual(host.workers,45)

    def test_starting_reservations_occupy_seats_while_accepted_backlog_is_separate(self):
        host=self.host();self.busy(host,starting=True)
        for i in range(30,45):self.root(i)
        data=host.capacity_snapshot()
        self.assertEqual(data['starting_reservation_count'],30);self.assertEqual(data['confirmed_active_count'],0)
        self.assertEqual(data['occupied_count'],30);self.assertEqual(data['backlog_count'],15)
        root,req,_=self.root(99);self.assertFalse(host._reserve_slot(root,req))

    def test_decrease_is_unsupported_and_never_kills_active_work(self):
        host=self.host();self.busy(host);host.apply_capacity_request(self.command(host))
        with patch('os.kill',side_effect=AssertionError('no killing')):
            reply=host.apply_capacity_request(self.command(host,request_id='shrink',target=30,revision=1))
        self.assertEqual(reply['error_code'],'decrease_not_supported');self.assertEqual(host.workers,45)
        self.assertEqual(host.capacity_snapshot()['occupied_count'],30)

    def test_hot_entry_requires_same_instance_applied_health_and_ack(self):
        host=self.host();original=service._write_json
        def write(path,value):
            original(path,value)
            if path.name=='hold-host-capacity-request.json':host._service_capacity_request()
        with patch.object(service,'_write_json',write),patch.object(service,'_stop_hold_host',side_effect=AssertionError('no restart')):
            result=service.request_hold_host_capacity(state_dir=self.state,launcher_config=self.config,
                host_instance_id=host.instance_id,request_id='grow',target_capacity=45,wait_seconds=0,
                pid_alive=lambda _:True,pid_started_at=lambda _:'2000-01-01T00:00:00+00:00')
        self.assertEqual(result['status'],'ready');self.assertEqual(result['applied_capacity'],45)
        self.assertEqual(result['capacity_revision'],1);self.assertEqual(result['host_instance_id'],host.instance_id)

    def test_missing_or_legacy_host_hot_entry_never_bootstraps(self):
        for legacy in (False,True):
            with self.subTest(legacy=legacy):
                self.setUp()
                if legacy:
                    host=self.host();health=json.loads(host.health_path.read_text());health['capabilities']=['exact_hold_stop_v1'];put(host.health_path,health)
                with patch.object(service,'_start_hold_host',side_effect=AssertionError('no new host')),patch.object(service,'_stop_hold_host',side_effect=AssertionError('no restart')):
                    reply=service.request_hold_host_capacity(state_dir=self.state,launcher_config=self.config,
                        host_instance_id=host.instance_id if legacy else 'missing',request_id='grow',target_capacity=45,wait_seconds=0,
                        pid_alive=lambda _:True,pid_started_at=lambda _:'2000-01-01T00:00:00+00:00')
                self.assertIn(reply['status'],{'unsupported','requires_readback'})
                self.assertFalse((self.state/'hold-host-capacity-request.json').exists())

    def test_timeout_or_wrong_ack_cannot_use_configuration_as_applied_capacity(self):
        host=self.host()
        reply=service.request_hold_host_capacity(state_dir=self.state,launcher_config=self.config,
            host_instance_id=host.instance_id,request_id='grow',target_capacity=45,wait_seconds=0,
            pid_alive=lambda _:True,pid_started_at=lambda _:'2000-01-01T00:00:00+00:00')
        self.assertEqual(reply['status'],'requires_readback');self.assertEqual(host.workers,30)
        self.assertEqual(json.loads(self.config.read_text())['worker_capacity'],30)

    def test_concurrent_same_target_has_one_revision_and_no_duplicate_consumers(self):
        host=self.host();self.busy(host)
        for i in range(30,60):self.root(i)
        gate=threading.Barrier(2);replies=[]
        def resize(name):
            gate.wait(2);replies.append(host.apply_capacity_request(self.command(host,request_id=name)))
        threads=[threading.Thread(target=resize,args=(name,)) for name in ('a','b')]
        for thread in threads:thread.start()
        for thread in threads:thread.join(3);self.assertFalse(thread.is_alive())
        self.assertEqual([r['status'] for r in replies],['applied','applied'])
        self.assertEqual(host.capacity_snapshot()['capacity_revision'],1)
        self.assertEqual(host.workers,45);self.assertEqual(len(self.created),15)
        host._schedule_requests_once();self.assertEqual(len(self.created),15)
        claimed=[]
        for _ in range(15):
            root=host._pending.get_nowait();req=json.loads((root/'request.json').read_text());ack=json.loads((root/'ack.json').read_text())
            self.assertTrue(host._reserve_slot(root,req))
            self.assertTrue(host._claim(root,root/'ack.json',ack,req,host_instance_id=host.instance_id));claimed.append(root)
        self.assertEqual(len(set(claimed)),15);self.assertEqual(host.capacity_snapshot()['occupied_count'],45)

    def test_claim_failure_before_dispatch_does_not_leak_a_starting_reservation(self):
        host=self.host();self.busy(host);host.apply_capacity_request(self.command(host));root,req,ack=self.root(31)
        before={str(p):p.read_bytes() for i in range(30) for p in (self.state/'task-holds'/f'h{i:03}').rglob('*') if p.is_file()}
        with patch.object(host,'_claim',side_effect=OSError('claim failed before dispatch')):
            with self.assertRaises(OSError):host.run_once(request_root=root)
        self.assertEqual(host.capacity_snapshot()['occupied_count'],30)
        for path,raw in before.items():self.assertEqual(Path(path).read_bytes(),raw)

    def test_legacy_busy_ensure_returns_unsupported_without_cold_bootstrap(self):
        host=self.host();self.busy(host)
        health=json.loads(host.health_path.read_text());health['capabilities']=['exact_hold_stop_v1'];put(host.health_path,health)
        initializer=lambda **kw:service.initialize_user_host(**kw,pid_alive=lambda _:True,pid_started_at=lambda _:'2000-01-01T00:00:00+00:00')
        adapter=CodexAppServerTaskProvisioningAdapter(self.config,state_dir=self.state,
            host_initializer=initializer,pid_alive=lambda _:True,pid_started_at=lambda _:'2000-01-01T00:00:00+00:00')
        with patch.object(service,'_stop_hold_host',side_effect=AssertionError('no restart')),patch.object(service,'_start_hold_host',side_effect=AssertionError('no bootstrap')):
            result=adapter.ensure_hold_host_ready(required_workers=45)
        self.assertEqual(result['status'],'unsupported')

    def test_public_default_revision_replay_and_retired_target_only_read_original_operation(self):
        host=self.host();original=service._write_json;submits=[]
        def write(path,value):
            original(path,value)
            if path.name=='hold-host-capacity-request.json':submits.append(value);host._service_capacity_request()
        kwargs={'state_dir':self.state,'launcher_config':self.config,'host_instance_id':host.instance_id,
                'request_id':'grow','target_capacity':45,'wait_seconds':0,
                'pid_alive':lambda _:True,'pid_started_at':lambda _:'2000-01-01T00:00:00+00:00'}
        with patch.object(service,'_write_json',write):
            self.assertEqual(service.request_hold_host_capacity(**kwargs)['status'],'ready')
            self.assertEqual(service.request_hold_host_capacity(**kwargs)['status'],'ready')
            host.apply_capacity_request(self.command(host,request_id='later',target=60,revision=1))
            prior=service.request_hold_host_capacity(**kwargs)
        self.assertEqual(prior['status'],'historical_readback');self.assertEqual(host.workers,60)
        self.assertEqual(len(submits),1)

    def test_consumer_resource_failure_isolated_from_original_thirty_and_not_acknowledged_as_forty_five(self):
        for fail_at in (1,3):
            with self.subTest(fail_at=fail_at):
                self.setUp();host=self.host();self.busy(host)
                for i in range(30,45):self.root(i)
                before={str(p):p.read_bytes() for i in range(30) for p in (self.state/'task-holds'/f'h{i:03}').rglob('*') if p.is_file()}
                attempts=[]
                def fail_factory(**kwargs):
                    worker=FakeWorker(**kwargs);attempts.append(worker)
                    if len(attempts)==fail_at:
                        worker.start=lambda:(_ for _ in ()).throw(RuntimeError('synthetic thread limit'))
                    else:
                        def start():
                            # Staged consumers cannot borrow the fifteen uncommitted seats.
                            root=self.state/'task-holds'/'h030';req=json.loads((root/'request.json').read_text())
                            self.assertFalse(host._reserve_slot(root,req));worker.started=True
                        worker.start=start
                    return worker
                host._worker_factory=fail_factory
                reply=host.apply_capacity_request(self.command(host))
                self.assertEqual(reply['status'],'requires_readback');self.assertEqual(reply['error_code'],'consumer_start_failed')
                self.assertEqual(host.capacity_snapshot()['applied_capacity'],30)
                self.assertEqual(host.capacity_snapshot()['occupied_count'],30)
                self.assertEqual(host.capacity_snapshot()['consumer_count'],30+fail_at-1)
                self.assertTrue(all(worker.is_alive() for worker in attempts[:-1]))
                self.assertEqual(host.apply_capacity_request(self.command(host)),reply)
                self.assertEqual(len(attempts),fail_at)
                class Stop:
                    stopped=False
                    def is_set(self):return self.stopped
                    def wait(self,*_):self.stopped=True
                with patch.object(service.threading,'Thread',side_effect=lambda **kw:FakeWorker(**kw)),patch('os.kill',side_effect=AssertionError('no killing')):
                    host.run_forever(poll_seconds=.01,stop_event=Stop())
                self.assertEqual(host.capacity_snapshot()['occupied_count'],30)
                self.assertTrue(all(worker.is_alive() for worker in attempts[:-1]))
                for path,raw in before.items():self.assertEqual(Path(path).read_bytes(),raw)

    def test_malformed_capacity_control_and_ack_are_isolated_without_fabricating_free_seats(self):
        host=self.host();self.busy(host)
        (self.state/'hold-host-capacity-request.json').write_text('{bad')
        (self.state/'task-holds'/'h000'/'ack.json').write_text('{bad')
        malformed,_,_=self.root(99);(malformed/'ack.json').write_text('{bad')
        host._service_capacity_request();host._write_health('holding')
        data=json.loads(host.health_path.read_text())
        self.assertEqual(data['occupied_count'],30);self.assertEqual(data['starting_reservation_count'],1)
        self.assertEqual(data['confirmed_active_count'],29)
        result=json.loads((self.state/'hold-host-capacity-result.json').read_text())
        self.assertEqual(result['status'],'requires_readback')

    def test_legacy_requeue_check_stays_visible_without_growing_or_claiming_rejected_history(self):
        host=self.host();roots=[]
        for i in range(2):
            root,req,ack=self.root(i);put(root/'ack.json',{**ack,'status':'holding','phase':'thread_starting'})
            put(root/'.user-host-claim'/'owner.json',{'pid':987654,'host_instance_id':'historical'});roots.append(root)
        before={str(p):p.read_bytes() for root in roots for p in root.rglob('*') if p.is_file()}
        with patch.object(host,'_requeue_dead_claim',return_value=False) as recovery,patch.object(host,'_claim',side_effect=AssertionError('no claim')):
            host._schedule_requests_once();host._schedule_requests_once()
        self.assertEqual([call.args[0] for call in recovery.call_args_list],roots)
        self.assertEqual(host.capacity_snapshot()['backlog_count'],0);self.assertEqual(self.created,[])
        self.assertEqual(host.capacity_snapshot()['occupied_count'],0)
        for path,raw in before.items():self.assertEqual(Path(path).read_bytes(),raw)


if __name__=='__main__':unittest.main()
