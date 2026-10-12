"""Linux signal-zero uncertainty: fake OS results, no process/network calls."""
import errno
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from adapters.codex_app_server import jarvis_hold_host_service as service
from jarvis_runtime.coo_dispatcher_store import DispatcherError, ProcessLock


@unittest.skipUnless(sys.platform.startswith('linux'),'Linux-only classification')
class LinuxPidErrnoTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        guard=patch('subprocess.Popen',side_effect=AssertionError('no real process'))
        guard.start();self.addCleanup(guard.stop)

    def test_signal_zero_success_and_only_esrch_proves_absence(self):
        cases=[(None,True),(ProcessLookupError(errno.ESRCH,'synthetic missing'),False),
               (PermissionError(errno.EPERM,'synthetic permission'),True),
               (PermissionError(errno.EACCES,'synthetic access'),True),
               (OSError(errno.EIO,'synthetic IO'),True),(OSError(errno.EINVAL,'synthetic invalid'),True),
               (OSError('synthetic unclassified'),True)]
        for error,expected in cases:
            with self.subTest(errno=getattr(error,'errno',None),success=error is None):
                with patch('os.kill',side_effect=error) as kill:
                    self.assertIs(service._pid_is_alive(987654),expected)
                    kill.assert_called_once_with(987654,0)

    def test_unknown_owner_cannot_enter_existing_cleanup_gate_or_remove_its_claim(self):
        for error in (PermissionError(errno.EPERM,'synthetic permission'),PermissionError(errno.EACCES,'synthetic access'),
                      OSError(errno.EIO,'synthetic IO'),OSError('synthetic unclassified')):
            with self.subTest(errno=error.errno):
                root=self.root/str(len(list(self.root.iterdir())));root.mkdir()
                lock=root/'request.lock';lock.write_text(json.dumps({'pid':987654,'acquired_at':'2000-01-01T00:00:00Z'}))
                claim=root/'.user-host-claim';claim.mkdir();owner=claim/'owner.json'
                owner.write_text(json.dumps({'pid':987654,'request_id':'TEST-old','host_instance_id':'TEST-owner'}))
                original_lock=lock.read_bytes();original_owner=owner.read_bytes()
                with patch('os.kill',side_effect=error),patch.object(service.JarvisHoldHost,'_release_claim') as release:
                    with self.assertRaises(DispatcherError):
                        with ProcessLock(lock,timeout_seconds=0,owner_alive=service._pid_is_alive):
                            service.JarvisHoldHost._release_claim(root)
                    release.assert_not_called()
                self.assertEqual(lock.read_bytes(),original_lock);self.assertEqual(owner.read_bytes(),original_owner)

    def test_esrch_only_reclaims_the_dead_lock_not_a_hold_without_other_proof(self):
        lock=self.root/'request.lock';lock.write_text(json.dumps({'pid':987654}))
        claim=self.root/'.user-host-claim';claim.mkdir();owner=claim/'owner.json';owner.write_text('{"request_id":"TEST-old"}')
        before=owner.read_bytes()
        with patch('os.kill',side_effect=ProcessLookupError(errno.ESRCH,'synthetic missing')):
            with ProcessLock(lock,timeout_seconds=0,owner_alive=service._pid_is_alive):
                self.assertTrue(lock.is_file())
        self.assertFalse(lock.exists());self.assertEqual(owner.read_bytes(),before)

    def test_permission_unknown_matching_host_does_not_trigger_a_cold_replacement(self):
        config=self.root/'launcher.json';config.write_text(json.dumps({'dispatcher_thread_id':'TEST-dispatcher',
            'profile':'TEST-profile','expected_codex_home':str(self.root),'worker_capacity':5,
            'allowed_projects':{'test':str(self.root)}}))
        host=service.JarvisHoldHost(state_dir=self.root,launcher_config=config,workers=5);host._write_health('ready')
        for code in (errno.EPERM,errno.EACCES):
            with self.subTest(errno=code):
                replacements=[]
                with patch('os.kill',side_effect=PermissionError(code,'synthetic permission')):
                    reply=service.initialize_user_host(state_dir=self.root,launcher_config=config,workers=5,
                        start_host=lambda:replacements.append('unexpected'),wait_seconds=0,
                        pid_started_at=lambda _:'2000-01-01T00:00:00Z')
                self.assertEqual(reply['status'],'ready');self.assertEqual(reply['phase'],'already_running')
                self.assertEqual(replacements,[])


if __name__=='__main__':unittest.main()
