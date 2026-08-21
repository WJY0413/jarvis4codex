"""Concrete App Server and Jarvis Bot adapter for the Monitor SDK seam."""
from __future__ import annotations
from typing import Any
from coo_dispatcher_store import iter_jsonl

class CodexAppServerMonitorAdapter:
    name = "codex-app-server-monitor"
    def __init__(self, transport: Any, controller: Any, dispatcher: Any): self.transport,self.controller,self.dispatcher=transport,controller,dispatcher
    def read_thread(self, thread_id: str) -> dict[str, object]:
        state=self.transport.read_thread(thread_id)
        return {"id":state.thread_id,"status":state.status,"turns":[{"id":item.turn_id,"status":item.status} for item in state.turns]}
    def resume_thread(self, thread_id: str, user_message_text: str, *, client_user_message_id: str, source_event_key: str, model: str|None, reasoning_effort: str|None) -> dict[str,object]:
        return self.controller.run_existing_task(thread_id,user_message_text,source_event_key=source_event_key,model=model,reasoning_effort=reasoning_effort,client_user_message_id=client_user_message_id)
    def enqueue_bot_notification(self, *, monitor_id: str, observed_thread_id: str, notification_text: str, source_event_key: str) -> dict[str,object]:
        result=self.dispatcher.enqueue_outbox({"task_id":monitor_id,"source_thread_id":observed_thread_id,"status":"completed","recipient":"cooper","content":notification_text,"message_kind":"monitor_completion","interaction_mode":"progress","source_event_key":source_event_key})
        record=result.get("record") if isinstance(result,dict) else {}; return {"outbox_id":record.get("outbox_id") if isinstance(record,dict) else None,"queued":bool(result.get("queued"))}
    def read_bot_delivery(self, outbox_id: str) -> dict[str,object]:
        rows=[row for row in iter_jsonl(self.dispatcher.delivery_log_path) if row.get("outbox_id")==outbox_id]
        return rows[-1] if rows else {"delivery_status":"queued"}
