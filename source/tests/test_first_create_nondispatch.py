"""Exact local first-create rejection evidence; no process or network calls."""
import errno
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from adapters.codex_app_server import jarvis_hold_host_service as service


def put(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


class FirstCreateNonDispatchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.config = self.state / 'launcher.json'
        put(self.config, {'dispatcher_thread_id': 'TEST-dispatcher', 'profile': 'TEST-profile',
            'expected_codex_home': str(self.state), 'worker_capacity': 5,
            'allowed_projects': {'test': str(self.state)}})
        self.host = service.JarvisHoldHost(state_dir=self.state, launcher_config=self.config)
        self.root = self.state / 'task-holds' / 'TEST-first-create'
        self.request = {'hold_id': self.root.name, 'request_id': 'TEST-first', 'mode': 'create', 'model': 'TEST-model'}
        self.result = {'hold_id': self.root.name, 'request_id': 'TEST-first', 'status': 'failed',
            'phase': 'model_selecting', 'thread_id': 'TEST-empty-thread', 'turn_id': '',
            'total_turn_count': 1, 'pid': 424242, 'terminal_confirmed': False,
            'reason': 'requested model is not supported by this CLI: TEST-model; available=[]',
            'observed_at': '2026-10-03T00:00:10+00:00'}
        for name, data in [('request.json', self.request), ('ack.json', self.result), ('result.json', self.result)]:
            put(self.root / name, data)
        self.health = {'pid': 424242, 'host_instance_id': 'TEST-existing-host',
            'status': 'ready', 'host_started_at': '2026-10-03T00:00:00+00:00',
            'observed_at': '2026-10-03T00:00:20+00:00', 'active_hold_ids': [],
            'profile': 'TEST-profile', 'codex_home': str(self.state),
            'state_dir': str(self.state.resolve()), 'worker_capacity': 5}
        put(self.host.health_path, self.health)
        for guard in [patch('subprocess.Popen', side_effect=AssertionError('no real process')),
                      patch.object(service, 'AppServerClient', side_effect=AssertionError('no observer')),
                      patch.object(service, '_now', return_value='2026-10-03T00:00:25+00:00'),
                      patch.object(service, '_pid_started_at', return_value='2026-10-03T00:00:00+00:00'),
                      patch('os.kill', return_value=None)]:
            guard.start()
            self.addCleanup(guard.stop)
        if os.name == 'nt':
            for guard in [patch.object(service, '_pid_is_alive', return_value=True),
                          patch.object(service, '_strict_local_host_alive', return_value=True, create=True)]:
                guard.start()
                self.addCleanup(guard.stop)

    def records(self):
        return {str(p.relative_to(self.state)): p.read_bytes() for p in self.state.rglob('*')
                if p.is_file() and p.name in {'request.json', 'ack.json', 'result.json', 'owner.json', 'hold-host.json'}}

    def refused(self):
        before = self.records()
        reply = self.host.reconcile_hold(self.root.name)
        self.assertEqual(reply['status'], 'requires_readback')
        self.assertFalse(reply['terminal_confirmed'])
        self.assertFalse(reply['hold_released'])
        self.assertEqual(self.records(), before)

    def test_exact_local_rejection_is_failed_non_dispatch_and_replay_is_read_only(self):
        reply = self.host.reconcile_hold(self.root.name)
        self.assertEqual(reply['status'], 'failed', reply)
        self.assertEqual(reply['phase'], 'model_selecting')
        self.assertEqual(reply['reason'], self.result['reason'])
        self.assertEqual(reply['turn_id'], '')
        self.assertTrue(reply['terminal_confirmed'])
        self.assertTrue(reply['hold_released'])
        self.assertEqual(reply['execution_evidence'], 'model_validation_rejected_before_turn_start')
        self.assertFalse(reply['recovery']['turn_started'])
        before = self.records()
        again = self.host.reconcile_hold(self.root.name)
        self.assertEqual(again, reply)
        self.assertEqual(self.records(), before)
        self.assertFalse((self.root / '.user-host-claim').exists())

    def test_first_create_shape_and_native_or_generation_conflicts_are_refused(self):
        cases = [('resume', 'request', {'mode': 'resume'}), ('prior', 'request', {'initial_total_turn_count': 2}),
                 ('native_request', 'request', {'thread_id': 'TEST-empty-thread'}),
                 ('turn', 'result', {'turn_id': 'TEST-turn'}), ('wrong_phase', 'result', {'phase': 'turn_starting'}),
                 ('transport', 'result', {'reason': 'model/list timeout'}),
                 ('ack_request', 'ack', {'request_id': 'TEST-other'}), ('ack_thread', 'ack', {'thread_id': 'TEST-other'}),
                 ('ack_instance', 'ack', {'host_instance_id': 'TEST-other'}),
                 ('result_instance', 'result', {'host_instance_id': 'TEST-other'}),
                 ('pause', 'request', {'resume_pause': {'request_id': 'TEST-first'}})]
        for label, name, change in cases:
            with self.subTest(case=label):
                self.setUp()
                value = self.request if name == 'request' else self.result
                put(self.root / (name + '.json'), {**value, **change})
                self.refused()

    def test_host_identity_missing_or_conflicting_and_time_order_are_refused(self):
        cases = [{'host_instance_id': ''}, {'pid': 989898}, {'profile': 'TEST-other'}, {'codex_home': str(self.state / 'other')},
                 {'state_dir': str(self.state / 'other')}, {'active_hold_ids': [self.root.name]},
                 {'active_hold_ids': None}, {'observed_at': '2026-10-03T00:00:10+00:00'},
                 {'observed_at': '2026-10-03T00:00:26+00:00'}, {'host_started_at': '2026-10-03T00:00:11+00:00'},
                 {'host_started_at': None}]
        for change in cases:
            with self.subTest(change=change):
                put(self.host.health_path, {**self.health, **change})
                self.refused()
        put(self.host.health_path, self.health)
        for birth in (None, '2026-10-03T00:00:11+00:00'):
            with self.subTest(birth=birth), patch.object(service, '_pid_started_at', return_value=birth):
                self.refused()

    def test_unknown_pid_permissions_never_become_positive_local_proof(self):
        for error in [PermissionError(errno.EPERM, 'TEST-permission'), PermissionError(errno.EACCES, 'TEST-access'),
                      OSError(errno.EIO, 'TEST-io'), OSError('TEST-unknown')]:
            with self.subTest(errno=error.errno), patch.object(service, '_strict_local_host_alive', side_effect=error,
                                                               create=True) if os.name == 'nt' else patch('os.kill', side_effect=error):
                self.refused()

    def test_non_linux_does_not_use_signal_zero_as_a_liveness_probe(self):
        from types import SimpleNamespace
        for platform in ('win32', 'darwin'):
            with self.subTest(platform=platform), patch.object(service, 'sys', SimpleNamespace(platform=platform)), \
                    patch('os.kill', side_effect=AssertionError('POSIX probe forbidden here')) as kill:
                if platform == 'win32' and os.name == 'nt':
                    self.assertEqual(self.host.reconcile_hold(self.root.name)['status'], 'failed')
                else:
                    self.refused()
                kill.assert_not_called()

    def test_claim_commit_and_cancellation_refuse_without_mutating_the_generation(self):
        for case in ('claim', 'commit', 'cancel', 'legacy_lock', 'commits_broken_link', 'history_broken_link',
                     'commits_empty_link', 'history_empty_link', 'commits_file', 'history_file'):
            with self.subTest(case=case):
                self.setUp()
                if case == 'claim': put(self.root / '.user-host-claim' / 'owner.json', {'request_id': 'TEST-first'})
                elif case == 'commit': put(self.root / 'dispatch-commits' / 'TEST.json', {'request_id': 'TEST-first'})
                elif case == 'cancel': put(self.root / 'cancellation.json', {'request_id': 'TEST-first'})
                elif case == 'legacy_lock': put(self.root / 'request.lock', {'pid': 424242})
                else:
                    path = self.root / ('dispatch-commits' if case.startswith('commits') else 'history')
                    if case.endswith('file'): path.write_text('TEST-unreadable-history-type')
                    else:
                        target = self.state / 'TEST-history-target'
                        if case.endswith('empty_link'): target.mkdir()
                        try:
                            path.symlink_to(target, target_is_directory=True)
                        except OSError as exc:
                            if os.name == 'nt' and getattr(exc, 'winerror', None) == 1314:
                                self.skipTest('Windows account cannot create symlinks; security predicate is not bypassed')
                            raise
                self.refused()

    def test_new_commit_or_cancel_after_prior_success_does_not_replay_old_release(self):
        for case in ('commit', 'cancel', 'ack', 'claim'):
            with self.subTest(case=case):
                self.setUp()
                self.assertEqual(self.host.reconcile_hold(self.root.name)['status'], 'failed')
                if case == 'commit': put(self.root / 'dispatch-commits' / 'TEST-late.json', {'request_id': 'TEST-first'})
                elif case == 'cancel': put(self.root / 'cancellation.json', {'request_id': 'TEST-first'})
                elif case == 'ack': put(self.root / 'ack.json', {**json.loads((self.root / 'ack.json').read_text()), 'thread_id': 'TEST-other'})
                else: put(self.root / '.user-host-claim' / 'owner.json', {'request_id': 'TEST-new'})
                self.refused()

    def test_gate_resamples_changed_claim_or_current_generation_before_publish(self):
        from contextlib import contextmanager
        from jarvis_runtime import cancellation
        original = cancellation._dispatch_guard
        for case in ('claim', 'request', 'health'):
            with self.subTest(case=case):
                self.setUp()
                @contextmanager
                def changed(root, request):
                    with original(root, request) as roots:
                        if case == 'claim': put(root / '.user-host-claim' / 'owner.json', {'request_id': 'TEST-new'})
                        elif case == 'request': put(root / 'request.json', {**self.request, 'request_id': 'TEST-new'})
                        else: put(self.host.health_path, {**self.health, 'host_instance_id': 'TEST-new-host'})
                        yield roots
                before_result = (self.root / 'result.json').read_bytes()
                before_ack = (self.root / 'ack.json').read_bytes()
                with patch.object(cancellation, '_dispatch_guard', changed):
                    answer = self.host.reconcile_hold(self.root.name)
                self.assertEqual(answer['status'], 'requires_readback')
                self.assertFalse(answer['hold_released'])
                self.assertEqual((self.root / 'result.json').read_bytes(), before_result)
                self.assertEqual((self.root / 'ack.json').read_bytes(), before_ack)

    def test_pid_is_resampled_inside_gate_and_permission_change_refuses(self):
        probe = (patch.object(service, '_strict_local_host_alive', side_effect=[True, False], create=True)
                 if os.name == 'nt' else
                 patch('os.kill', side_effect=[None, None, None, PermissionError(errno.EPERM, 'TEST-changed-permission')]))
        with probe:
            self.refused()

    def test_ack_write_failure_retries_only_the_same_fixed_local_proof(self):
        writer = service._write_json
        attempts = []
        def fail_ack(path, value):
            attempts.append(path.name)
            if path == self.root / 'ack.json':
                raise PermissionError('TEST-ack-write-denied')
            return writer(path, value)
        with patch.object(service, '_write_json', fail_ack):
            answer = self.host.reconcile_hold(self.root.name)
        self.assertEqual(answer['status'], 'requires_readback')
        self.assertIn('TEST-ack-write-denied', answer['reason'])
        fixed_result = (self.root / 'result.json').read_bytes()
        restarted = service.JarvisHoldHost(state_dir=self.state, launcher_config=self.config)
        self.assertNotEqual(restarted.instance_id, self.host.instance_id)
        retried = restarted.reconcile_hold(self.root.name)
        self.assertEqual(retried['status'], 'failed')
        self.assertEqual((self.root / 'result.json').read_bytes(), fixed_result)
        proof = json.loads(fixed_result)['recovery']['local_proof']
        self.assertEqual(json.loads((self.root / 'ack.json').read_text()), proof['target_ack'])
        self.assertEqual(hashlib.sha256((self.root / 'ack.json').read_bytes()).hexdigest(), proof['target_ack_sha256'])
        before = self.records()
        self.assertEqual(restarted.reconcile_hold(self.root.name), retried)
        self.assertEqual(self.records(), before)

    def test_half_written_ack_cannot_complete_after_evidence_or_original_ack_changes(self):
        for case in ('claim', 'commit', 'cancel', 'ack_bytes', 'proof_shape'):
            with self.subTest(case=case):
                self.setUp()
                writer = service._write_json
                def fail_ack(path, value):
                    if path == self.root / 'ack.json':
                        raise PermissionError('TEST-ack-write-denied')
                    return writer(path, value)
                with patch.object(service, '_write_json', fail_ack):
                    self.assertEqual(self.host.reconcile_hold(self.root.name)['status'], 'requires_readback')
                if case == 'claim': put(self.root / '.user-host-claim' / 'owner.json', {'request_id': 'TEST-new'})
                elif case == 'commit': put(self.root / 'dispatch-commits' / 'TEST-late.json', {'request_id': 'TEST-first'})
                elif case == 'cancel': put(self.root / 'cancellation.json', {'request_id': 'TEST-first'})
                elif case == 'ack_bytes': put(self.root / 'ack.json', {**self.result, 'unexpected_extra': 'TEST-modified'})
                else:
                    result = json.loads((self.root / 'result.json').read_text())
                    result['recovery']['local_proof'] = ['TEST-malformed']
                    put(self.root / 'result.json', result)
                self.host = service.JarvisHoldHost(state_dir=self.state, launcher_config=self.config)
                self.refused()

    def test_completed_local_proof_remains_readable_after_original_host_exit(self):
        for case in ('health_missing', 'pid_exited'):
            with self.subTest(case=case):
                self.setUp()
                first = self.host.reconcile_hold(self.root.name)
                self.assertEqual(first['status'], 'failed')
                if case == 'health_missing': self.host.health_path.unlink()
                restarted = service.JarvisHoldHost(state_dir=self.state, launcher_config=self.config)
                before = self.records()
                with patch('os.kill', side_effect=ProcessLookupError(errno.ESRCH, 'TEST-owner-exited')) as kill:
                    self.assertEqual(restarted.reconcile_hold(self.root.name), first)
                    kill.assert_not_called()
                self.assertEqual(self.records(), before)

    def test_half_committed_local_proof_finishes_ack_after_original_host_exit(self):
        for case in ('health_missing', 'pid_exited'):
            with self.subTest(case=case):
                self.setUp()
                writer = service._write_json
                def fail_ack(path, value):
                    if path == self.root / 'ack.json': raise PermissionError('TEST-ack-write-denied')
                    return writer(path, value)
                with patch.object(service, '_write_json', fail_ack):
                    self.assertEqual(self.host.reconcile_hold(self.root.name)['status'], 'requires_readback')
                fixed_result = (self.root / 'result.json').read_bytes()
                if case == 'health_missing': self.host.health_path.unlink()
                restarted = service.JarvisHoldHost(state_dir=self.state, launcher_config=self.config)
                with patch('os.kill', side_effect=ProcessLookupError(errno.ESRCH, 'TEST-owner-exited')) as kill:
                    reply = restarted.reconcile_hold(self.root.name)
                    self.assertEqual(reply['status'], 'failed')
                    self.assertTrue(reply['hold_released'])
                    kill.assert_not_called()
                self.assertEqual((self.root / 'result.json').read_bytes(), fixed_result)
                proof = json.loads(fixed_result)['recovery']['local_proof']
                self.assertEqual(json.loads((self.root / 'ack.json').read_text()), proof['target_ack'])
                self.assertEqual(hashlib.sha256((self.root / 'ack.json').read_bytes()).hexdigest(), proof['target_ack_sha256'])

    def test_original_ack_only_metadata_is_preserved_in_the_fixed_target(self):
        put(self.root / 'ack.json', {**self.result, 'synthetic_ack_only': {'route': 'TEST-route'}})
        first = self.host.reconcile_hold(self.root.name)
        self.assertEqual(first['status'], 'failed')
        ack = json.loads((self.root / 'ack.json').read_text())
        self.assertEqual(ack['synthetic_ack_only'], {'route': 'TEST-route'})
        self.assertEqual(ack['request_id'], first['request_id'])
        self.assertEqual(ack['status'], 'failed')
        before = self.records()
        self.assertEqual(self.host.reconcile_hold(self.root.name), first)
        self.assertEqual(self.records(), before)


@unittest.skipUnless(os.name == 'nt', 'real Windows process proof')
class WindowsRealFirstCreateProof(unittest.TestCase):
    def test_live_process_exact_proof_then_read_only_replay_after_exit(self):
        with tempfile.TemporaryDirectory(prefix='jarvis30-first-create-') as temporary:
            state = Path(temporary)
            config = state / 'launcher.json'
            put(config, {'dispatcher_thread_id': 'TEST-dispatcher', 'profile': 'TEST-profile',
                         'expected_codex_home': str(state), 'allowed_projects': {'TEST': str(state)}})
            host = service.JarvisHoldHost(state_dir=state, launcher_config=config)
            root = state / 'task-holds' / 'TEST-first-create'
            process = subprocess.Popen([sys.executable, '-B', '-c', 'import time; time.sleep(15)'],
                                       stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                       stderr=subprocess.DEVNULL, creationflags=subprocess.CREATE_NO_WINDOW)
            try:
                born = service._pid_started_at(process.pid)
                self.assertIsNotNone(born)
                started = datetime.now(timezone.utc).isoformat()
                request = {'hold_id': root.name, 'request_id': 'TEST-first', 'mode': 'create', 'model': 'TEST-model'}
                time.sleep(0.02)
                failed_at = datetime.now(timezone.utc).isoformat()
                result = {'hold_id': root.name, 'request_id': 'TEST-first', 'status': 'failed',
                          'phase': 'model_selecting', 'thread_id': 'TEST-empty-thread', 'turn_id': '',
                          'total_turn_count': 1, 'pid': process.pid, 'terminal_confirmed': False,
                          'reason': 'requested model is not supported by this CLI: TEST-model; available=[]',
                          'observed_at': failed_at}
                for name, value in (('request', request), ('ack', result), ('result', result)):
                    put(root / (name + '.json'), value)
                time.sleep(0.02)
                put(host.health_path, {'pid': process.pid, 'host_instance_id': 'TEST-live-host',
                    'status': 'ready', 'host_started_at': started,
                    'observed_at': datetime.now(timezone.utc).isoformat(), 'active_hold_ids': [],
                    'profile': 'TEST-profile', 'codex_home': str(state),
                    'state_dir': str(state.resolve()), 'worker_capacity': 1})
                with patch('subprocess.Popen', side_effect=AssertionError('no second process')), \
                        patch.object(service, 'AppServerClient', side_effect=AssertionError('no observer')):
                    first = host.reconcile_hold(root.name)
                self.assertEqual(first['status'], 'failed', first)
                self.assertTrue(first['terminal_confirmed'])
                self.assertTrue(first['hold_released'])
                self.assertEqual(first['turn_id'], '')
                self.assertFalse(first['recovery']['turn_started'])
                frozen = tuple((root / (name + '.json')).read_bytes() for name in ('request', 'ack', 'result'))
                process.terminate()
                process.wait(timeout=5)
                replay = service.JarvisHoldHost(state_dir=state, launcher_config=config).reconcile_hold(root.name)
                self.assertEqual(replay, first)
                self.assertEqual(frozen, tuple((root / (name + '.json')).read_bytes() for name in ('request', 'ack', 'result')))
            finally:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=5)


if __name__ == '__main__':
    unittest.main()
