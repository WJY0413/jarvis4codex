import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from adapters.codex_app_server.jarvis_task_hold_host import _write_json

class ReceiptWriteTest(unittest.TestCase):
    def test_transient_windows_reader_lock_retries_and_publishes(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'ack.json'
            path.write_text('{"old": true}')
            replace = Path.replace
            attempts = []
            def locked_then_replace(source, target):
                attempts.append(str(source))
                if len(attempts) == 1:
                    raise PermissionError('sharing violation')
                return replace(source, target)
            with patch.object(Path, 'replace', locked_then_replace), patch('adapters.codex_app_server.jarvis_task_hold_host.time.sleep'):
                _write_json(path, {'status': 'holding'})
            self.assertEqual(json.loads(path.read_text()), {'status': 'holding'})
            self.assertEqual(len(attempts), 2)
            self.assertEqual(list(Path(temp).glob('*.tmp')), [])

    def test_persistent_permission_failure_preserves_previous_receipt(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'ack.json'
            path.write_text('{"old": true}')
            with patch.object(Path, 'replace', side_effect=PermissionError('denied')) as replace, patch('adapters.codex_app_server.jarvis_task_hold_host.time.sleep'):
                with self.assertRaises(PermissionError):
                    _write_json(path, {'status': 'holding'})
            self.assertEqual(json.loads(path.read_text()), {'old': True})
            self.assertEqual(replace.call_count, 20)
            self.assertEqual(list(Path(temp).glob('*.tmp')), [])

    def test_host_records_unexpected_holder_failure_instead_of_stalling_claim(self):
        from adapters.codex_app_server.jarvis_hold_host_service import JarvisHoldHost
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = root / 'config.json'
            config.write_text(json.dumps({'profile': 'test', 'expected_codex_home': str(root)}))
            item = root / 'task-holds/item'
            item.mkdir(parents=True)
            (item / 'request.json').write_text(json.dumps({'request_id': 'r', 'hold_id': 'h'}))
            (item / 'ack.json').write_text(json.dumps({'request_id': 'r', 'status': 'accepted', 'phase': 'queued_for_user_host'}))
            with patch('adapters.codex_app_server.jarvis_hold_host_service.hold_task', side_effect=RuntimeError('receipt failed')):
                self.assertTrue(JarvisHoldHost(state_dir=root, launcher_config=config).run_once())
            result = json.loads((item / 'result.json').read_text())
            self.assertEqual(result['status'], 'failed')
            self.assertIn('receipt failed', result['reason'])
            self.assertFalse((item / '.user-host-claim').exists())
