import tempfile
import unittest
from pathlib import Path
from adapters.codex_app_server.monitor_adapter import CodexAppServerMonitorAdapter

class Turn: 
    def __init__(self): self.turn_id="turn-1"; self.status="completed"
class State:
    thread_id="child-1"; status="idle"; turns=(Turn(),)
class Transport:
    def read_thread(self, thread_id): return State()
class Controller:
    def __init__(self): self.call=None
    def run_existing_task(self,*args,**kwargs): self.call=(args,kwargs); return {"outcome":"turn_completed","turn_id":"turn-2"}
class Dispatcher:
    def __init__(self,path): self.delivery_log_path=path
    def enqueue_outbox(self,request): self.request=request; return {"queued":True,"record":{"outbox_id":"outbox-1"}}

class MonitorAdapterContractTest(unittest.TestCase):
    def test_read_resume_outbox_and_delivery_readback(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/"delivery.jsonl"; dispatcher=Dispatcher(path); controller=Controller(); adapter=CodexAppServerMonitorAdapter(Transport(),controller,dispatcher)
            self.assertEqual(adapter.read_thread("child-1")["turns"][0]["id"],"turn-1")
            adapter.resume_thread("parent-1","custom",client_user_message_id="stable-1",source_event_key="event-1",model="m",reasoning_effort="high")
            self.assertEqual(controller.call[1]["client_user_message_id"],"stable-1")
            self.assertEqual(adapter.enqueue_bot_notification(monitor_id="monitor-1",observed_thread_id="child-1",notification_text="done",source_event_key="event-1")["outbox_id"],"outbox-1")
            path.write_text('{"outbox_id":"outbox-1","delivery_status":"delivered"}\n',encoding="utf-8")
            self.assertEqual(adapter.read_bot_delivery("outbox-1")["delivery_status"],"delivered")
