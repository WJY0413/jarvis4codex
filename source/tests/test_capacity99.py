"""User-requested 1..99 ceiling; no models, sockets, live state or processes.

Failure cases specified before implementation: 99 rejected by old 64/10 caps;
0/100/bool/float accepted or coerced; MCP disagrees with internal validation;
defaults silently increase; a full pool over-admits or a failed task leaks a
reservation; legacy 10x10 changes; custom seats bypass uniqueness or slot grid;
campaign preparation and task binding disagree; existing pause/cancel changes.
"""
import asyncio
from contextlib import ExitStack
import copy
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from adapters.codex_app_server import jarvis_hold_host_service as host_module
from adapters.codex_app_server.task_provisioning_adapter import CodexAppServerTaskProvisioningAdapter
from jarvis_control.loop import _validate_start
from jarvis_control.service import JarvisControl
from jarvis_mcp.server import JarvisMcpServer
from jarvis_runtime.jarvis_native_task_launcher import NativeTaskLauncherConfig, NativeTaskError

VALID=(1,10,20,30,99)
INVALID=(0,100,True,1.5,'20')

class Capacity99(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.config=self.root/'launcher.json'
        self.raw={'dispatcher_thread_id':'TEST','profile':'TEST','expected_codex_home':str(self.root),'allowed_projects':{'TEST':str(self.root)}}
        self.config.write_text(json.dumps(self.raw))
        self.guards=ExitStack();self.addCleanup(self.guards.close)
        for name in ('subprocess.Popen','socket.socket.connect','socket.socket.connect_ex'):
            self.guards.enter_context(patch(name,side_effect=AssertionError('live execution forbidden')))

    def loop_request(self,n):
        return dict(request_id='TEST',project='TEST',prompt='offline',target_thread_count=n,max_rounds=1,expires_at='2099-01-01T00:00:00Z')

    def test_loop_and_config_boundaries_keep_default_one(self):
        self.assertEqual(NativeTaskLauncherConfig(self.config).worker_capacity,1)
        for n in VALID:
            with self.subTest(valid=n):
                self.config.write_text(json.dumps({**self.raw,'worker_capacity':n}))
                self.assertEqual(NativeTaskLauncherConfig(self.config).worker_capacity,n)
                self.assertEqual(len(_validate_start(self.loop_request(n))['threads']),n)
        for n in INVALID:
            with self.subTest(invalid=n):
                self.config.write_text(json.dumps({**self.raw,'worker_capacity':n}))
                with self.assertRaises(NativeTaskError):NativeTaskLauncherConfig(self.config)
                with self.assertRaises(ValueError):_validate_start(self.loop_request(n))

    def test_host_bounds_queue_reservation_release_and_hot_replay(self):
        import threading
        for n in VALID:
            with self.subTest(capacity=n):
                root=self.root/str(n);root.mkdir()
                host=host_module.JarvisHoldHost(state_dir=root,launcher_config=self.config,workers=n)
                host._setup_worker_pool(threading.Event())
                for i in range(n):
                    key=root/f'h{i}';req={'request_id':f'r{i}','hold_id':f'h{i}'}
                    self.assertTrue(host._reserve_slot(key,req));self.assertFalse(host._reserve_slot(key,req))
                self.assertFalse(host._reserve_slot(root/'overflow',{'request_id':'overflow'}))
                self.assertEqual(host.capacity_snapshot()['occupied_count'],n)
        for n in INVALID:
            with self.subTest(invalid=n):
                with self.assertRaises(ValueError):host_module.JarvisHoldHost(state_dir=self.root,launcher_config=self.config,workers=n)
        host=host_module.JarvisHoldHost(state_dir=self.root,launcher_config=self.config,workers=1)
        host._setup_worker_pool(threading.Event())
        self.assertEqual(host.workers,1)
        for n in (10,20,30,99):
            command=dict(request_id=f'TEST-{n}',host_instance_id=host.instance_id,target_capacity=n,expected_revision=host._capacity_revision)
            reply=host.apply_capacity_request(command)
            self.assertEqual(reply['status'],'applied');self.assertEqual(host.workers,n)
            self.assertEqual(host.apply_capacity_request(command),reply)
            self.assertEqual(host._worker_threads,[])
        for n in INVALID:
            reply=host.apply_capacity_request(dict(request_id='bad',host_instance_id=host.instance_id,target_capacity=n,expected_revision=host._capacity_revision))
            self.assertEqual(reply['error_code'],'invalid_capacity_request')
            public=host_module.request_hold_host_capacity(state_dir=self.root,launcher_config=self.config,host_instance_id=host.instance_id,request_id='bad',target_capacity=n,wait_seconds=0)
            self.assertEqual(public['error_code'],'invalid_capacity_request')

    def test_mcp_schema_and_facade_agree(self):
        server=JarvisMcpServer(SimpleNamespace())
        tools={x.name:x.parameters for x in server.mcp._tool_manager.list_tools()}
        schema=tools['jarvis_loop']['properties']['target_thread_count']
        branch=next(x for x in schema['anyOf'] if x.get('type')=='integer')
        self.assertEqual((branch['minimum'],branch['maximum']),(1,99))
        inputs=tools['jarvis_capacity']['properties']['inputs']
        self.assertEqual((inputs['minItems'],inputs['maxItems']),(1,99))
        control=JarvisControl(capabilities=None,bridge=None,provisioner=SimpleNamespace(creation_enabled=lambda:True))
        control._operations=SimpleNamespace(capacity=lambda **kw:{'status':'offline_accepted','count':len(kw['inputs'])})
        for n in VALID:self.assertEqual(control.capacity(request_id='TEST',run_id='TEST',project='TEST',inputs=['offline']*n)['count'],n)
        for n in (0,100):self.assertEqual(control.capacity(request_id='TEST',run_id='TEST',project='TEST',inputs=['offline']*n)['status'],'invalid_request')

    def test_invalid_bootstrap_and_adapter_requests_have_no_side_effect(self):
        for n in INVALID:
            with self.subTest(value=n):
                root=self.root/f'invalid-{n}'
                with patch.object(host_module,'_start_hold_host',side_effect=AssertionError('no bootstrap')):
                    result=host_module.initialize_user_host(state_dir=root,launcher_config=self.config,workers=n,wait_seconds=0)
                self.assertEqual(result['error_code'],'invalid_capacity_request');self.assertFalse(root.exists())
                adapter=CodexAppServerTaskProvisioningAdapter(self.config,state_dir=root,host_initializer=lambda **_:self.fail('no initializer'))
                result=adapter.ensure_hold_host_ready(required_workers=n)
                self.assertEqual(result['error_code'],'invalid_capacity_request');self.assertFalse(root.exists())


@unittest.skipIf(os.name=='nt','ContactTask is a Linux-only secure workflow; exercise real flock on Linux')
class ContactCapacity99(unittest.TestCase):
    def setUp(self):
        from jarvis_control import contact_task as module
        self.module=module
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.guards=ExitStack();self.addCleanup(self.guards.close)
        for name in ('subprocess.Popen','socket.socket.connect','socket.socket.connect_ex'):
            self.guards.enter_context(patch(name,side_effect=AssertionError('live execution forbidden')))

    # 30x1 requirement failures, specified before product edits: ignored rounds,
    # invalid round count, missing/duplicate slots or companies, lost hash binding,
    # prepare accepting a layout which verify_binding/run still rejects.
    def allocation(self,seats=10,capacity=10,explicit=True,rounds=None):
        data={'schema':'jarvis-dot-secondary-allocation/v1','run_id':'TEST','max_concurrency':capacity,'production_write':False,'secondary_research':True,
            'assignments':[{'company_id':f'c{s}-{r}','official_domain':f'c{s}-{r}.example','seat_id':f'seat-{s:02}','round':r,'eligible':True,'no_go':False,'secondary_research':True}
                for s in range(1,seats+1) for r in range(1,(10 if rounds is None else rounds)+1)]}
        if explicit:data['seat_count']=seats
        if rounds is not None:data['rounds_per_seat']=rounds
        return data

    def test_explicit_rounds_reject_invalid_values_and_incomplete_grid(self):
        for value in (0,11,100,True,1.5,'1',None):
            data=self.allocation(30,30,rounds=1);data['rounds_per_seat']=value
            with self.subTest(invalid=value),self.assertRaises(self.module.FileTaskError):self.module.validate_secondary_allocation(data)
        for seats in (20,30,99):
            self.module.validate_secondary_allocation(self.allocation(seats,seats,rounds=1))
        for fault in ('missing_seat','missing_round','duplicate_company','duplicate_domain','duplicate_seat','wrong_round'):
            data=self.allocation(30,30,rounds=1)
            if fault=='missing_seat':data['assignments'].pop()
            elif fault=='missing_round':data['rounds_per_seat']=2
            elif fault=='wrong_round':data['assignments'][0]['round']=2
            else:
                key={'duplicate_company':'company_id','duplicate_domain':'official_domain','duplicate_seat':'seat_id'}[fault]
                data['assignments'][1][key]=data['assignments'][0][key]
            with self.subTest(fault=fault),self.assertRaises(self.module.FileTaskError):self.module.validate_secondary_allocation(data)

    def test_legacy_and_explicit_layout_and_limits(self):
        self.module.validate_secondary_allocation(self.allocation(explicit=False))
        for n in VALID:self.module.validate_secondary_allocation(self.allocation(seats=n,capacity=n))
        for key in ('max_concurrency','seat_count'):
            for n in INVALID:
                with self.subTest(field=key,value=n):
                    data=self.allocation();data[key]=n
                    with self.assertRaises(self.module.FileTaskError):self.module.validate_secondary_allocation(data)

    def test_dedupe_exact_grid_and_security_flags_still_required(self):
        for field in ('company_id','official_domain','seat_id'):
            data=self.allocation(20,20);data['assignments'][10][field]=data['assignments'][0][field]
            with self.subTest(field=field),self.assertRaises(self.module.FileTaskError):self.module.validate_secondary_allocation(data)
        for mutation in ('missing','bad_round','bad_seat','production','secondary','round_bool'):
            data=self.allocation()
            if mutation=='missing':data['assignments'].pop()
            elif mutation=='bad_round':data['assignments'][0]['round']=11
            elif mutation=='round_bool':data['assignments'][0]['round']=True
            elif mutation=='bad_seat':data['assignments'][0]['seat_id']='seat-99'
            elif mutation=='production':data['production_write']=True
            else:data['secondary_research']=False
            with self.subTest(mutation=mutation),self.assertRaises(self.module.FileTaskError):self.module.validate_secondary_allocation(data)

    def test_real_linux_slots_capacity_and_release_on_error(self):
        for n in VALID:
            with self.subTest(capacity=n):
                root=self.root/str(n);root.mkdir()
                def task(seat):
                    value=self.module.ContactTask.__new__(self.module.ContactTask)
                    value.root=root;value.capacity=n;value.p={'seat_id':seat};return value
                with ExitStack() as occupied:
                    for i in range(n):occupied.enter_context(task(f'seat-{i}').execution_slot())
                    with self.assertRaises(self.module.FileTaskError):
                        with task('overflow').execution_slot():self.fail('over-admission')
                with self.assertRaisesRegex(RuntimeError,'synthetic'):
                    with task('released').execution_slot():raise RuntimeError('synthetic')
                with task('reused').execution_slot():pass
                self.assertFalse(list((root/'.contact-capacity').glob('*.lock')))

    def test_campaign_rejects_invalid_before_import_or_state_write(self):
        from jarvis_control.contact_campaign import prepare
        allocation=self.root/'allocation.json';config=self.root/'config.json';config.write_text('{}')
        for mutation in ('duplicate','capacity','seat_count'):
            data=self.allocation(20,20)
            if mutation=='duplicate':data['assignments'][1]['official_domain']=data['assignments'][0]['official_domain']
            elif mutation=='capacity':data['max_concurrency']=100
            else:data['seat_count']=100
            allocation.write_text(json.dumps(data))
            with self.assertRaises(self.module.FileTaskError):prepare(self.root,allocation,config)
            self.assertFalse((self.root/'validation-campaign.json').exists())

    def test_prepare_and_binding_agree_for_legacy_and_thirty_seats(self):
        from jarvis_control import contact_campaign
        m=self.module
        for seats,explicit,rounds in ((10,False,None),(30,True,10),(30,True,1),(20,True,1),(99,True,1)):
            root=self.root/f'{seats}-{rounds}';root.mkdir()
            data=self.allocation(seats,seats,explicit,rounds)
            data.update(source_library_id='TEST',source_snapshot_sha256='0'*64,authorization_message_ids=['TEST'],expires_at_epoch=4102444800)
            for row in data['assignments']:
                row.update(company_name=row['company_id'],website='https://'+row['official_domain'],rating='A',country_code='GB',search_language_mode='en',bizseek_enabled=False,source_company_id=row['company_id'])
            allocation=root/'allocation.json';allocation.write_text(json.dumps(data))
            config=root/'config.json';config.write_text(json.dumps({'contact_source_hashes':{},'contact_v2_contract':str(root/'synthetic-contract'),'contact_skill_dir':str(root/'synthetic-skill')}))
            fake=SimpleNamespace()
            def seal(item):return {**item,'input_sha256':'synthetic-offline'}
            fake.V2=SimpleNamespace(seal_work_item=seal,validate_work_item=lambda _:[])
            fake.load_json=lambda p:json.loads(Path(p).read_text())
            fake.digest=lambda value:m.sha(m.json_bytes(value))
            def prepare(assignment,workspace,run_id,export):
                item=fake.load_json(assignment)
                m.save(workspace/'task.json',{'company':{'entity_id':item['entity']['entity_id']},'rating':'A'})
                m.save(workspace/'binding.json',{'work_item':item,'assignment_path':str(assignment),'assignment_sha256':fake.digest(item),'run_id':run_id})
                return {'status':'synthetic_prepared','model_turns_started':0}
            fake.prepare=prepare
            with patch.dict(os.environ),patch.object(contact_campaign,'importlib',SimpleNamespace(import_module=lambda _:fake)),patch('sys.path',list(__import__('sys').path)):
                receipt=contact_campaign.prepare(root,allocation,config)
            self.assertEqual(receipt['company_count'],seats*(rounds or 10))
            self.assertEqual(len(list(root.glob('seat-*.packet.json'))),seats*(rounds or 10))
            packet=m.strict_json((root/f'seat-{seats:02}-r01.packet.json').read_bytes())
            task=m.ContactTask.__new__(m.ContactTask)
            task.root=root;task.p=packet;task.config_path=config;task.config_hash=m.sha(config.read_bytes());task.code_hash='TEST';task.source_hashes={}
            task.workspace=root/packet['company_workspace'];task.binding=m.strict_json((task.workspace/'binding.json').read_bytes());task.task=m.strict_json((task.workspace/'task.json').read_bytes())
            task.company_id=task.task['company']['entity_id'];task.v24=fake
            with patch.object(m,'source_identity',return_value={'code_sha256':'TEST'}):task.verify_binding()
            self.assertEqual(task.capacity,seats)
            if rounds==1:
                task.p['round']=2
                with self.assertRaises(m.FileTaskError),patch.object(m,'source_identity',return_value={'code_sha256':'TEST'}):task.verify_binding()
                task.p['round']=1
                original_allocation=allocation.read_bytes();allocation.write_bytes(original_allocation+b' ')
                with self.assertRaisesRegex(m.FileTaskError,'allocation changed'),patch.object(m,'source_identity',return_value={'code_sha256':'TEST'}):task.verify_binding()
                allocation.write_bytes(original_allocation)
                task.state=task.workspace/'jarvis-contact';task.receipt_path=task.state/'receipt.json'
                task.packet_bytes=m.json_bytes(packet);task.config=SimpleNamespace(live_creation_enabled=True)
                with patch.object(m,'source_identity',return_value={'code_sha256':'TEST'}),patch.object(m,'ContactClient',side_effect=RuntimeError('OFFLINE_MODEL_BOUNDARY')) as client:
                    with self.assertRaisesRegex(RuntimeError,'OFFLINE_MODEL_BOUNDARY'):task.run()
                    client.assert_called_once()
                self.assertEqual(m.strict_json(task.receipt_path.read_bytes())['error'],'OFFLINE_MODEL_BOUNDARY')
                self.assertFalse((task.workspace/'.jarvis-contact.lock').exists())
            with self.assertRaises(m.FileTaskError),patch.dict(os.environ),patch.object(contact_campaign,'importlib',SimpleNamespace(import_module=lambda _:fake)),patch('sys.path',list(__import__('sys').path)):
                contact_campaign.prepare(root,allocation,config)

if __name__=='__main__':unittest.main()
