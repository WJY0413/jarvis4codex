#!/usr/bin/env python3
"""Jarvis Monitor runtime: bounded thread observation with explicit outputs."""
from __future__ import annotations

import argparse, json, time
from pathlib import Path
from typing import Any

from jarvis_monitor import MonitorService, MonitorStore
from jarvis_heartbeat_service import HeartbeatConfig, WakeController, load_standard_bridge, StandardBridgeHeartbeatTransport
from coo_dispatcher_store import DispatcherStore


class RuntimeMonitorAdapter:
    def __init__(self, heartbeat: HeartbeatConfig, dispatcher_root: Path):
        self.heartbeat=heartbeat; self.controller=WakeController(heartbeat); self.bridge=load_standard_bridge(heartbeat)
        self.transport=StandardBridgeHeartbeatTransport(heartbeat,self.controller,self.bridge); self.dispatcher=DispatcherStore(dispatcher_root)
    def read_thread(self, thread_id: str) -> dict[str,object]:
        state=self.transport.read_thread(thread_id)
        return {"id":state.thread_id,"status":state.status,"turns":[{"id":t.turn_id,"status":t.status} for t in state.turns]}
    def resume_thread(self, thread_id: str, user_message_text: str, *, client_user_message_id: str, source_event_key: str, model: str|None, reasoning_effort: str|None) -> dict[str,object]:
        return self.controller.run_existing_task(thread_id,user_message_text,source_event_key=source_event_key,model=model,reasoning_effort=reasoning_effort,client_user_message_id=client_user_message_id)
    def enqueue_bot_notification(self, *, monitor_id: str, observed_thread_id: str, notification_text: str, source_event_key: str) -> dict[str,object]:
        result=self.dispatcher.enqueue_outbox({"task_id":monitor_id,"source_thread_id":observed_thread_id,"status":"completed","recipient":"cooper","content":notification_text,"message_kind":"monitor_completion","interaction_mode":"progress","source_event_key":source_event_key})
        record=result.get("record") if isinstance(result,dict) else {}
        return {"outbox_id":record.get("outbox_id") if isinstance(record,dict) else None, "queued":bool(result.get("queued"))}


def parser() -> argparse.ArgumentParser:
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--config",type=Path,required=True); p.add_argument("--dispatcher-root",type=Path,required=True)
    sub=p.add_subparsers(dest="command",required=True); start=sub.add_parser("monitor-start"); start.add_argument("--request",type=Path,required=True)
    status=sub.add_parser("monitor-status"); status.add_argument("--monitor-id",required=True)
    sub.add_parser("monitor-run-once"); serve=sub.add_parser("monitor-serve"); serve.add_argument("--max-ticks",type=int)
    return p


def main() -> int:
    args=parser().parse_args(); heartbeat=HeartbeatConfig.load(args.config); db=heartbeat.db_path.with_name("jarvis-monitors.sqlite")
    service=MonitorService(MonitorStore(db),RuntimeMonitorAdapter(heartbeat,args.dispatcher_root))
    if args.command=="monitor-start": print(json.dumps({"ok":True,"monitor":service.store.start(json.loads(args.request.read_text(encoding="utf-8-sig")))},ensure_ascii=False)); return 0
    if args.command=="monitor-status": print(json.dumps({"ok":True,"monitor":service.store.get(args.monitor_id)},ensure_ascii=False)); return 0
    if args.command=="monitor-run-once": print(json.dumps({"ok":True,"results":service.run_once()},ensure_ascii=False)); return 0
    ticks=0
    while args.max_ticks is None or ticks<args.max_ticks:
        service.run_once(); ticks+=1; time.sleep(1)
    return 0


if __name__=="__main__": raise SystemExit(main())
