"""Offline integration of the real role guard with strict paginated readback.
No Codex/app-server or model process, network call or production state.
"""
import json
from pathlib import Path
import queue
import tempfile
import threading
import time
import types
import unittest

from jarvis_control.file_task import FileTaskClient, FileTaskError


class FileTaskReadbackGuardTests(unittest.TestCase):
    def make_client(self, directory, role, *, final=True):
        c=object.__new__(FileTaskClient)
        c.role=role
        c.evidence=Path(directory)/'protocol.jsonl'
        c.config=types.SimpleNamespace(request_timeout_seconds=.1,turn_readback_timeout_seconds=.01,poll_seconds=.001)
        c.response_queue=queue.Queue();c.notifications=queue.Queue();c.stderr_lines=[]
        c._request_id=0;c._write_lock=threading.Lock();c.before_turn_dispatch=None
        writes=[]
        class ProtocolSink:
            def write(self,raw):
                call=json.loads(raw);writes.append(call)
                if call['method']=='thread/read':
                    self_outer.assertFalse(call['params']['includeTurns'])
                    result={'thread':{'id':'thread-exact'}}
                elif call['method']=='thread/items/list':
                    self_outer.assertEqual(call['params']['threadId'],'thread-exact')
                    self_outer.assertEqual(call['params']['turnId'],'turn-exact')
                    result={'data':[{'turnId':'turn-exact','item':{'type':'agentMessage','id':'item-test','phase':'final_answer' if final else 'commentary','text':'  {"ok": true}\n'}}]}
                else:
                    raise AssertionError('Unexpected fake transport method: '+call['method'])
                c.response_queue.put({'id':call['id'],'result':result})
            def flush(self):pass
        self_outer=self
        c.process=types.SimpleNamespace(stdin=ProtocolSink(),poll=lambda:None)
        return c,writes

    def test_model_role_strict_final_crosses_real_write_guard(self):
        with tempfile.TemporaryDirectory() as d:
            c,writes=self.make_client(d,'model')
            got=c.wait_for_turn_readback('thread-exact','turn-exact',require_final_answer=True)
            self.assertEqual(got,'  {"ok": true}\n')
            self.assertEqual([w['method'] for w in writes],['thread/read','thread/items/list'])
            self.assertEqual(len(c.evidence.read_text().splitlines()),2)

    def test_model_role_commentary_is_not_a_strict_final(self):
        with tempfile.TemporaryDirectory() as d:
            c,writes=self.make_client(d,'model',final=False)
            self.assertEqual(c._read_final_message_pagewise('thread-exact','turn-exact',require_final_answer=True,deadline=time.monotonic()+1),'')
            self.assertTrue(any(w['method']=='thread/items/list' for w in writes))

    def test_files_role_still_denies_items_list_before_transport(self):
        with tempfile.TemporaryDirectory() as d:
            c,writes=self.make_client(d,'files')
            with self.assertRaisesRegex(FileTaskError,'not allowed'):
                c.request('thread/items/list',{'threadId':'thread-exact','turnId':'turn-exact'})
            self.assertEqual(writes,[])
            self.assertFalse(c.evidence.exists())

    def test_unknown_role_does_not_inherit_new_items_permission(self):
        with tempfile.TemporaryDirectory() as d:
            c,writes=self.make_client(d,'unknown-role')
            with self.assertRaisesRegex(FileTaskError,'not allowed'):
                c.request('thread/items/list',{'threadId':'thread-exact','turnId':'turn-exact'})
            self.assertEqual(writes,[])
            self.assertFalse(c.evidence.exists())

    def test_model_role_still_denies_filesystem_and_other_rpc(self):
        with tempfile.TemporaryDirectory() as d:
            c,writes=self.make_client(d,'model')
            for method in ['fs/readFile','fs/writeFile','thread/fork','turn/interrupt']:
                with self.subTest(method=method), self.assertRaisesRegex(FileTaskError,'not allowed'):
                    c.request(method,{})
            self.assertEqual(writes,[])
            self.assertFalse(c.evidence.exists())


if __name__=='__main__':unittest.main()
