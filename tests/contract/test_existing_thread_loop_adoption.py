from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from adapters.codex_app_server.task_provisioning_adapter import CodexAppServerTaskProvisioningAdapter
from jarvis_control import LoopController, LoopStore
from jarvis_control.provisioning import TaskMonitorResumeRequest


class ExistingThreadLoopAdoptionTest(unittest.TestCase):
    def test_first_adoption_uses_real_adapter_without_existing_hold(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            adapter = CodexAppServerTaskProvisioningAdapter(root / 'launcher.json', state_dir=root / 'holds', config_loader=lambda _: SimpleNamespace())
            calls = []
            class Runtime:
                def hold(self, **kwargs):
                    calls.append(kwargs)
                    result = adapter.resume_with_monitor(TaskMonitorResumeRequest(
                        request_id=kwargs['request_id'], task_id=kwargs['task_id'], prompt=kwargs['prompt'], source_ref=kwargs['source_ref'],
                        hold_id=kwargs['hold_id'], max_turns=kwargs['max_turns'], auto_continue=kwargs['auto_continue']))
                    return {'status': result.status, 'target_thread_id': result.thread_id, 'data': result.as_dict()}
                def monitor(self, **kwargs):
                    return {'status': 'completed', 'data': adapter.hold_status(kwargs['hold_id'])}
                def heartbeat(self, **kwargs):
                    return {'status': 'active'}
            result = LoopController(LoopStore(root / 'loops')).start(Runtime(), request_id='adopt', project='Star fire', prompt='one company',
                target_thread_count=1, max_rounds=1, max_turns=1, auto_continue=True,
                threads=[{'slot': 'A', 'acquire': 'resume', 'task_id': 'existing-thread'}], expires_at='2099-01-01T00:00:00+00:00')
            self.assertEqual(result.status, 'running')
            self.assertIsNone(calls[0]['hold_id'])
            self.assertEqual(result.data['children'][0]['thread_id'], 'existing-thread')
            self.assertTrue(result.data['children'][0]['hold_id'])
            self.assertEqual(calls[0]['max_turns'], 1)
            missing = adapter.resume_with_monitor(TaskMonitorResumeRequest(request_id='bad', task_id='existing-thread', prompt='one', source_ref='test', hold_id='actually-missing'))
            self.assertEqual(missing.status, 'failed')
            self.assertIn('hold state was not found', missing.reason)
