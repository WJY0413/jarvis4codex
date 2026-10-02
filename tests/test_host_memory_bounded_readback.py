"""Offline engineering regressions; these do not substitute for Jarvistest."""
import gc
import json
import queue
import threading
import time
import types
import unittest
import weakref
from unittest.mock import patch

from jarvis_runtime.jarvis_native_task_launcher import AppServerClient, NativeTaskError


def message(text, phase='final_answer', turn='turn-1'):
    return {'turnId': turn, 'item': {'id':'item-1', 'type':'agentMessage', 'phase':phase, 'text':text}}


class ReadbackTests(unittest.TestCase):
    def client(self, pages):
        client=object.__new__(AppServerClient)
        client.config=types.SimpleNamespace(turn_readback_timeout_seconds=0.1,poll_seconds=0.001)
        client.calls=[]
        client.metadata_calls=[]
        client.deadlines=[]
        def request(method, params, **kwargs):
            client.deadlines.append(kwargs.get('deadline'))
            if method == 'thread/read':
                client.metadata_calls.append((method,params))
                return {'thread':{'id':params['threadId']}}
            client.calls.append((method,params))
            return pages.pop(0)
        client.request=request
        return client

    def test_reads_exact_turn_only(self):
        c=self.client([{'data':[message(' done ')]}])
        self.assertEqual(c.wait_for_turn_readback('thread-1','turn-1'),'done')
        self.assertEqual(c.metadata_calls,[('thread/read',{'threadId':'thread-1','includeTurns':False})])
        self.assertEqual(len(set(c.deadlines)),1)
        self.assertEqual(c.calls,[('thread/items/list',{'threadId':'thread-1','turnId':'turn-1','sortDirection':'desc','limit':50})])

    def test_pocket_preserves_whitespace(self):
        c=self.client([{'data':[message('  {"ok": true}\n')]}])
        self.assertEqual(c.wait_for_turn_readback('thread-1','turn-1',require_final_answer=True),'  {"ok": true}\n')

    def test_newest_final_preferred_to_newest_commentary_across_pages(self):
        c=self.client([{'data':[message('comment','commentary')],'nextCursor':'page-2'}, {'data':[message('real final')]}])
        self.assertEqual(c.wait_for_turn_readback('thread-1','turn-1'),'real final')
        self.assertEqual(c.calls[1][1]['cursor'],'page-2')

    def test_legacy_commentary_only_fallback(self):
        c=self.client([{'data':[message('new','commentary'),message('old','commentary')]}])
        self.assertEqual(c.wait_for_turn_readback('thread-1','turn-1'),'new')

    def test_require_final_does_not_accept_commentary(self):
        c=self.client([{'data':[message('comment','commentary')]}])
        self.assertEqual(c._read_final_message_pagewise('thread-1','turn-1',require_final_answer=True,deadline=time.monotonic()+1),'')

    def test_wrong_turn_fails_closed(self):
        c=self.client([{'data':[message('other',turn='turn-other')]}])
        with self.assertRaisesRegex(NativeTaskError,'identity mismatch'):
            c.wait_for_turn_readback('thread-1','turn-1')

    def test_invalid_shape_fails_closed(self):
        c=self.client([{'data':{}}])
        with self.assertRaisesRegex(NativeTaskError,'missing data'):
            c.wait_for_turn_readback('thread-1','turn-1')

    def test_cursor_cycle_fails_closed(self):
        c=self.client([{'data':[],'nextCursor':'same'},{'data':[],'nextCursor':'same'}])
        with self.assertRaisesRegex(NativeTaskError,'repeated cursor'):
            c.wait_for_turn_readback('thread-1','turn-1')

    def test_partial_scan_cannot_fall_back_to_commentary(self):
        c=self.client([{'data':[message('comment','commentary')],'nextCursor':'more'}])
        self.assertEqual(c._read_final_message_pagewise('thread-1','turn-1',require_final_answer=False,deadline=time.monotonic()-1),'')

    def test_blank_delayed_readback_retries(self):
        c=self.client([{'data':[]},{'data':[message('later')]}])
        self.assertEqual(c.wait_for_turn_readback('thread-1','turn-1'),'later')
        self.assertEqual(len(c.calls),2)

    def test_wrong_thread_metadata_fails_closed(self):
        c=self.client([])
        c.request=lambda *args,**kwargs:{'thread':{'id':'different-thread'}}
        with self.assertRaisesRegex(NativeTaskError,'Exact-thread metadata'):
            c.wait_for_turn_readback('thread-1','turn-1')

    def test_already_expired_scan_does_not_dispatch(self):
        c=self.client([])
        self.assertEqual(c._read_final_message_pagewise('thread-1','turn-1',require_final_answer=False,deadline=time.monotonic()-1),'')
        self.assertEqual(c.calls,[])

    def test_rpc_timeout_is_capped_by_readback_deadline(self):
        c=object.__new__(AppServerClient)
        c.config=types.SimpleNamespace(request_timeout_seconds=60)
        c._request_id=0;c.stderr_lines=[];c._write=lambda value:None
        class NoReply:
            def __init__(self):self.timeout=None
            def get(self,timeout):self.timeout=timeout;raise queue.Empty
        c.response_queue=NoReply()
        deadline=time.monotonic()+0.01
        with self.assertRaisesRegex(NativeTaskError,'timed out'):
            c.request('thread/items/list',{},deadline=deadline)
        self.assertGreaterEqual(c.response_queue.timeout,0)
        self.assertLessEqual(c.response_queue.timeout,0.01)

    def test_expired_rpc_never_writes(self):
        c=object.__new__(AppServerClient)
        c.config=types.SimpleNamespace(request_timeout_seconds=60)
        c._write=lambda value:self.fail('expired request was written')
        with self.assertRaisesRegex(NativeTaskError,'deadline exhausted'):
            c.request('thread/items/list',{},deadline=time.monotonic()-1)

    def test_resume_does_not_load_historical_turns(self):
        c=self.client([{'thread':{'id':'thread-1','turns':[]}}])
        c.start=lambda:None
        c._runtime_thread_params=lambda model:{'model':model}
        c._verify_runtime_context=lambda response,**kwargs:None
        c.start_turn_async=lambda *args,**kwargs:{'turn_id':'turn-1'}
        self.assertEqual(c.resume_turn_async('thread-1','public test',client_user_message_id='msg-1',model='gpt-6-luna'),{'turn_id':'turn-1'})
        self.assertTrue(c.calls[0][1]['excludeTurns'])


class ReaderRetentionTests(unittest.TestCase):
    def test_consumed_message_not_retained_by_waiting_reader(self):
        class WeakDict(dict): pass
        class BlockingStream:
            def __init__(self):
                self.once=False;self.blocked=threading.Event();self.release=threading.Event()
            def __iter__(self):return self
            def __next__(self):
                if not self.once:
                    self.once=True;return '{"id":1,"result":{}}\n'
                self.blocked.set();self.release.wait(3);raise StopIteration
        stream=BlockingStream();refs=[]
        def decode(raw):
            obj=WeakDict(id=1,result={'payload':'x'*65536})
            refs.append(weakref.ref(obj));return obj
        c=object.__new__(AppServerClient)
        c.process=types.SimpleNamespace(stdout=stream)
        c.response_queue=queue.Queue();c.notifications=queue.Queue()
        with patch('jarvis_runtime.jarvis_native_task_launcher.json.loads',side_effect=decode):
            reader=threading.Thread(target=c._read_stdout)
            reader.start()
            try:
                got=c.response_queue.get(timeout=1);del got
                self.assertTrue(stream.blocked.wait(1));gc.collect()
                self.assertTrue(refs[0]() is None, 'reader still owns the last consumed JSON response')
            finally:
                stream.release.set();reader.join(2)
            self.assertFalse(reader.is_alive())
            self.assertIsNone(c.notifications.get(timeout=1))


if __name__=='__main__':unittest.main()
