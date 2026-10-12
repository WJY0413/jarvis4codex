"""Module-local capability simulation; no real subprocess or storage operations."""
import importlib.util
import os
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

SOURCE = Path(__file__).resolve().parents[1] / 'packages/adapters/local_test_inbox_receiver.py'
FLAGS = ('O_DIRECTORY', 'O_NOFOLLOW', 'O_CLOEXEC', 'O_NONBLOCK')

class OsCapabilities:
    def __init__(self, missing=(), no_dir_fd=()):
        self.missing = set(missing)
        # Explicit synthetic Linux API. Never inherit the host's capabilities:
        # on Windows they are absent, even though this is a mock-only case.
        self.posix = dict(O_DIRECTORY=0o200000, O_NOFOLLOW=0o400000,
                          O_CLOEXEC=0o2000000, O_NONBLOCK=0o4000,
                          O_RDONLY=0, O_WRONLY=1, O_CREAT=0o100, O_EXCL=0o200,
                          geteuid=lambda: 1000)
        for name in ('open', 'mkdir', 'unlink', 'link', 'listdir', 'write', 'fsync', 'close'):
            setattr(self, name, Mock(name=name))
        self.listdir.return_value = []
        self.supports_dir_fd = {getattr(self, name) for name in ('open', 'mkdir', 'unlink', 'link')
            if name not in no_dir_fd}
    def __getattr__(self, name):
        if name in self.missing:
            raise AttributeError(name)
        if name in self.posix:
            return self.posix[name]
        return getattr(os, name)

class SecureInboxCapabilityFakeTests(unittest.TestCase):
    def load(self, proxy):
        spec = importlib.util.spec_from_file_location('synthetic_inbox_capability', SOURCE)
        module = importlib.util.module_from_spec(spec)
        with patch.dict('sys.modules', {'os': proxy}):
            spec.loader.exec_module(module)
        return module

    def assert_closed(self, module, proxy):
        self.assertIs(getattr(module, '_SECURE_STORAGE_SUPPORTED', None), False)
        self.assertIsNone(module._DIR_FLAGS)
        self.assertIsNone(module._FILE_FLAGS)
        with self.assertRaisesRegex(OSError, 'secure TEST inbox storage is unavailable'):
            with module.workspace_fd(PurePosixPath('/synthetic')):
                self.fail('unsupported workspace opened')
        with self.assertRaisesRegex(OSError, 'secure TEST inbox storage is unavailable'):
            module.read_record(19, 'record.json', 100)
        with self.assertRaisesRegex(OSError, 'secure TEST inbox storage is unavailable'):
            module.publish_record(19, 'record.json', {'synthetic': True})
        for name in ('open', 'mkdir', 'unlink', 'link', 'listdir', 'write', 'fsync', 'close'):
            getattr(proxy, name).assert_not_called()

    def test_missing_posix_flags_imports_but_all_storage_entries_fail_closed(self):
        for flag in FLAGS:
            with self.subTest(flag=flag):
                proxy = OsCapabilities(missing=(flag,))
                self.assert_closed(self.load(proxy), proxy)

    def test_missing_dir_fd_or_uid_capability_fails_before_any_storage_operation(self):
        for missing in ('open', 'mkdir', 'unlink', 'link', 'geteuid'):
            with self.subTest(missing=missing):
                proxy = OsCapabilities(missing=('geteuid',) if missing == 'geteuid' else (),
                    no_dir_fd=(missing,) if missing != 'geteuid' else ())
                self.assert_closed(self.load(proxy), proxy)

    def test_supported_linux_flags_and_secure_open_are_not_weakened(self):
        proxy = OsCapabilities()
        module = self.load(proxy)
        self.assertIs(getattr(module, '_SECURE_STORAGE_SUPPORTED', None), True)
        self.assertEqual(module._DIR_FLAGS, proxy.O_RDONLY | proxy.O_DIRECTORY | proxy.O_NOFOLLOW | proxy.O_CLOEXEC)
        self.assertEqual(module._FILE_FLAGS, proxy.O_RDONLY | proxy.O_NOFOLLOW | proxy.O_CLOEXEC | proxy.O_NONBLOCK)
        proxy.open.side_effect = OSError('synthetic secure open')
        with self.assertRaisesRegex(OSError, 'synthetic secure open'):
            module.read_record(19, 'record.json', 100)
        proxy.open.assert_called_once_with('record.json', module._FILE_FLAGS, dir_fd=19)
        proxy.open.reset_mock()
        with self.assertRaisesRegex(OSError, 'synthetic secure open'):
            with module.workspace_fd(PurePosixPath('/synthetic')):
                self.fail('synthetic open must fail')
        proxy.open.assert_called_once_with('/', module._DIR_FLAGS)
        proxy.open.reset_mock()
        with patch.object(module.uuid, 'uuid4', return_value=SimpleNamespace(hex='0' * 32)):
            with self.assertRaisesRegex(OSError, 'synthetic secure open'):
                module.publish_record(19, 'record.json', {'synthetic': True})
        proxy.open.assert_called_once_with('.pending-' + '0' * 32,
            proxy.O_WRONLY | proxy.O_CREAT | proxy.O_EXCL | proxy.O_NOFOLLOW | proxy.O_CLOEXEC, 0o600, dir_fd=19)
        proxy.write.assert_not_called()
        proxy.fsync.assert_not_called()

if __name__ == '__main__':
    unittest.main()
