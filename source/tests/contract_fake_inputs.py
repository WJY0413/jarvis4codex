"""Constructor-only config doubles for fixed legacy tests; no authority or native client."""
from functools import wraps
import json,os,sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

def config_double(path):
    path=Path(path);raw=json.loads(path.read_text()) if path.is_file() else {}
    home=raw.get('expected_codex_home') or str(path.parent/'TEST-home')
    return SimpleNamespace(profile=raw.get('profile') or 'jarvis_test',expected_codex_home=home,
        expected_codex_home_input=home,expected_codex_home_base_dir=str(path.parent),jarvis_connection={},
        default_model=None,default_reasoning_effort=None,runtime_constraints=None,runtime_environments=None,
        worker_capacity=raw.get('worker_capacity',5))

def host_config_fixture(cls):
    original=cls.setUp
    @wraps(original)
    def set_up(self):
        guard=patch('adapters.codex_app_server.jarvis_hold_host_service.NativeTaskLauncherConfig',side_effect=config_double)
        guard.start();self.addCleanup(guard.stop);original(self)
    cls.setUp=set_up;return cls

class WindowsCommandOs:
    name='nt'
    def __getattr__(self,name):return getattr(os,name)

class WindowsLogicSys:
    platform='win32'
    def __getattr__(self,name):return getattr(sys,name)

def windows_requeue_logic(function):
    @wraps(function)
    def run(*args,**kwargs):
        with patch('adapters.codex_app_server.jarvis_hold_host_service.sys',WindowsLogicSys()):
            return function(*args,**kwargs)
    return run
