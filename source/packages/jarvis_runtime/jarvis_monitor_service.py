#!/usr/bin/env python3
"""Jarvis Monitor runtime: bounded thread observation with explicit outputs."""
from __future__ import annotations

import argparse, json, time
from pathlib import Path
from typing import Any

from jarvis_monitor import MonitorService, MonitorStore
from jarvis_heartbeat_service import HeartbeatConfig, WakeController, load_standard_bridge, StandardBridgeHeartbeatTransport
from coo_dispatcher_store import DispatcherStore
from adapters.codex_app_server.monitor_adapter import CodexAppServerMonitorAdapter


def parser() -> argparse.ArgumentParser:
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--config",type=Path,required=True); p.add_argument("--dispatcher-root",type=Path,required=True)
    sub=p.add_subparsers(dest="command",required=True); start=sub.add_parser("monitor-start"); start.add_argument("--request",type=Path,required=True)
    status=sub.add_parser("monitor-status"); status.add_argument("--monitor-id",required=True)
    sub.add_parser("monitor-run-once"); serve=sub.add_parser("monitor-serve"); serve.add_argument("--max-ticks",type=int)
    return p


def main() -> int:
    args=parser().parse_args(); heartbeat=HeartbeatConfig.load(args.config); db=heartbeat.db_path.with_name("jarvis-monitors.sqlite")
    controller=WakeController(heartbeat); bridge=load_standard_bridge(heartbeat)
    transport=StandardBridgeHeartbeatTransport(heartbeat,controller,bridge)
    service=MonitorService(MonitorStore(db),CodexAppServerMonitorAdapter(transport,controller,DispatcherStore(args.dispatcher_root)))
    if args.command=="monitor-start": print(json.dumps({"ok":True,"monitor":service.store.start(json.loads(args.request.read_text(encoding="utf-8-sig")))},ensure_ascii=False)); return 0
    if args.command=="monitor-status": print(json.dumps({"ok":True,"monitor":service.store.get(args.monitor_id)},ensure_ascii=False)); return 0
    if args.command=="monitor-run-once": print(json.dumps({"ok":True,"results":service.run_once()},ensure_ascii=False)); return 0
    ticks=0
    while args.max_ticks is None or ticks<args.max_ticks:
        service.run_once(); ticks+=1; time.sleep(1)
    return 0


if __name__=="__main__": raise SystemExit(main())
