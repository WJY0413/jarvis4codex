"""Harness-neutral Monitor engine.

The runtime adapter supplies thread read/resume and Jarvis Bot outbox delivery.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol


class MonitorError(RuntimeError): pass


def now() -> str: return datetime.now(timezone.utc).isoformat()
def packed(value: object) -> str: return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class MonitorAdapter(Protocol):
    def read_thread(self, thread_id: str) -> dict[str, object]: ...
    def resume_thread(self, thread_id: str, user_message_text: str, *, client_user_message_id: str, source_event_key: str, model: str | None, reasoning_effort: str | None) -> dict[str, object]: ...
    def enqueue_bot_notification(self, *, monitor_id: str, observed_thread_id: str, notification_text: str, source_event_key: str) -> dict[str, object]: ...


class MonitorStore:
    def __init__(self, db_path: Path):
        self.db_path = db_path; db_path.parent.mkdir(parents=True, exist_ok=True)
        with self.session() as c:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS monitors (monitor_id TEXT PRIMARY KEY, monitor_key TEXT UNIQUE, monitor_name TEXT, observed_thread_id TEXT NOT NULL, source_thread_id TEXT, source_event_key TEXT NOT NULL, outputs_json TEXT NOT NULL, interval_seconds INTEGER NOT NULL, expires_at TEXT NOT NULL, model TEXT, reasoning_effort TEXT, monitor_status TEXT NOT NULL, last_fingerprint TEXT, next_observation_at TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, error TEXT);
            CREATE TABLE IF NOT EXISTS monitor_observations (observation_id INTEGER PRIMARY KEY, monitor_id TEXT NOT NULL, observed_at TEXT NOT NULL, event_type TEXT NOT NULL, fingerprint TEXT NOT NULL, thread_status TEXT, turn_id TEXT, turn_status TEXT);
            CREATE TABLE IF NOT EXISTS monitor_deliveries (delivery_id TEXT PRIMARY KEY, monitor_id TEXT NOT NULL, fingerprint TEXT NOT NULL, output_index INTEGER NOT NULL, output_type TEXT NOT NULL, target_thread_id TEXT, client_user_message_id TEXT, outbox_id TEXT, delivery_status TEXT NOT NULL, turn_id TEXT, error TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, UNIQUE(monitor_id,fingerprint,output_index));""")
    def db(self):
        c=sqlite3.connect(self.db_path); c.row_factory=sqlite3.Row; return c
    @contextmanager
    def session(self):
        connection=self.db()
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()
    def get(self, monitor_id: str) -> dict[str, object]:
        with self.session() as c: row=c.execute("SELECT * FROM monitors WHERE monitor_id=?",(monitor_id,)).fetchone()
        if not row: raise MonitorError(f"monitor not found: {monitor_id}")
        value=dict(row); value["outputs"]=json.loads(str(value.pop("outputs_json"))); return value
    def start(self, request: dict[str, object]) -> dict[str, object]:
        observed=str(request.get("observed_thread_id") or "").strip()
        outputs=request.get("outputs") or []
        if not observed: raise MonitorError("observed_thread_id is required")
        if not isinstance(outputs,list) or not all(isinstance(x,dict) for x in outputs): raise MonitorError("outputs must be a list of objects")
        interval=int(request.get("interval_seconds") or 5); minutes=int(request.get("expires_in_minutes") or 60)
        if not 3<=interval<=60: raise MonitorError("interval_seconds must be between 3 and 60")
        if not 1<=minutes<=1440: raise MonitorError("expires_in_minutes must be between 1 and 1440")
        source_thread=str(request.get("source_thread_id") or "").strip() or None
        for output in outputs:
            kind=str(output.get("type") or "")
            if kind not in {"resume_thread","resume_source_thread","notify_jarvis_bot"}: raise MonitorError("unknown output type")
            if kind=="resume_thread" and not str(output.get("target_thread_id") or "").strip(): raise MonitorError("resume_thread requires target_thread_id")
            if kind=="resume_source_thread" and not source_thread: raise MonitorError("resume_source_thread requires source_thread_id")
        key=str(request.get("monitor_key") or "").strip() or packed({"observed":observed,"outputs":outputs,"source":source_thread})
        monitor_id="monitor-"+hashlib.sha256(key.encode()).hexdigest()[:24]; created=now()
        with self.session() as c:
            if c.execute("SELECT 1 FROM monitors WHERE monitor_id=?",(monitor_id,)).fetchone(): return self.get(monitor_id)
            c.execute("INSERT INTO monitors VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(monitor_id,str(request.get("monitor_key") or "") or None,str(request.get("monitor_name") or "") or None,observed,source_thread,str(request.get("source_event_key") or f"monitor:{monitor_id}"),packed(outputs),interval,(datetime.now(timezone.utc)+timedelta(minutes=minutes)).isoformat(),str(request.get("model") or "") or None,str(request.get("reasoning_effort") or "") or None,"ACTIVE",None,created,created,created,None))
        return self.get(monitor_id)
    def due(self) -> list[dict[str, object]]:
        with self.session() as c: ids=[r[0] for r in c.execute("SELECT monitor_id FROM monitors WHERE monitor_status='ACTIVE' AND next_observation_at<=?",(now(),))]
        return [self.get(x) for x in ids]
    def observation(self, monitor_id: str, event: str, fingerprint: str, thread: str, turn_id: str|None, turn: str|None, interval: int):
        at=now()
        with self.session() as c:
            c.execute("INSERT INTO monitor_observations(monitor_id,observed_at,event_type,fingerprint,thread_status,turn_id,turn_status) VALUES (?,?,?,?,?,?,?)",(monitor_id,at,event,fingerprint,thread,turn_id,turn))
            c.execute("UPDATE monitors SET last_fingerprint=?,next_observation_at=?,updated_at=? WHERE monitor_id=?",(fingerprint,(datetime.now(timezone.utc)+timedelta(seconds=interval)).isoformat(),at,monitor_id))
    def status(self, monitor_id: str, status: str, error: str|None=None):
        with self.session() as c: c.execute("UPDATE monitors SET monitor_status=?,error=?,updated_at=? WHERE monitor_id=?",(status,error,now(),monitor_id))
    def delivery(self, monitor: dict[str,object], fp: str, index: int, output: dict[str,object], target: str|None, client_id: str|None) -> dict[str,object]:
        delivery_id=f"delivery-{monitor['monitor_id']}-{fp[:12]}-{index}"
        with self.session() as c:
            row=c.execute("SELECT * FROM monitor_deliveries WHERE monitor_id=? AND fingerprint=? AND output_index=?",(monitor["monitor_id"],fp,index)).fetchone()
            if row:return dict(row)
            c.execute("INSERT INTO monitor_deliveries VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",(delivery_id,monitor["monitor_id"],fp,index,output["type"],target,client_id,None,"PENDING",None,None,now(),now()))
        return self.delivery(monitor,fp,index,output,target,client_id)
    def complete_delivery(self, delivery: dict[str,object], status: str, **values: str|None):
        with self.session() as c: c.execute("UPDATE monitor_deliveries SET delivery_status=?,turn_id=?,outbox_id=?,error=?,updated_at=? WHERE delivery_id=?",(status,values.get("turn_id"),values.get("outbox_id"),values.get("error"),now(),delivery["delivery_id"]))


class MonitorService:
    def __init__(self, store: MonitorStore, adapter: MonitorAdapter): self.store,self.adapter=store,adapter
    def run_once(self) -> list[dict[str,object]]:
        results=[]
        for monitor in self.store.due():
            mid=str(monitor["monitor_id"])
            if datetime.now(timezone.utc)>=datetime.fromisoformat(str(monitor["expires_at"])): self.store.status(mid,"EXPIRED"); results.append({"monitor_id":mid,"outcome":"expired"}); continue
            try:
                state=self.adapter.read_thread(str(monitor["observed_thread_id"])); turns=state.get("turns") or []; latest=turns[-1] if isinstance(turns,list) and turns else {}
                if not isinstance(latest,dict):latest={}
                thread=str(state.get("status") or "unknown").lower(); turn_id=str(latest.get("id") or latest.get("turn_id") or "") or None; turn=str(latest.get("status") or thread).lower()
                fp=hashlib.sha256(packed({"thread":monitor["observed_thread_id"],"status":thread,"turn":turn_id,"turn_status":turn}).encode()).hexdigest(); terminal=turn in {"completed","failed","cancelled","canceled","interrupted"}
                old=monitor.get("last_fingerprint"); event=("baseline_terminal" if terminal else "baseline_active") if not old else ("no_change" if old==fp else ("terminal_changed" if terminal else "active_or_unknown_changed"))
                self.store.observation(mid,event,fp,thread,turn_id,turn,int(monitor["interval_seconds"]))
                if event!="terminal_changed": results.append({"monitor_id":mid,"outcome":event}); continue
                if turn!="completed": self.store.status(mid,"REQUIRES_READBACK",turn); results.append({"monitor_id":mid,"outcome":"requires_readback"}); continue
                pending=False
                for i,output in enumerate(monitor["outputs"]):
                    assert isinstance(output,dict); kind=str(output["type"]); target=(str(output.get("target_thread_id") or "") if kind=="resume_thread" else str(monitor.get("source_thread_id") or "")) if kind.startswith("resume_") else None; client_id=f"monitor:{mid}:{fp[:12]}:{i}" if target else None
                    d=self.store.delivery(monitor,fp,i,output,target,client_id)
                    if d["delivery_status"]!="PENDING": pending|=d["delivery_status"]=="QUEUED"; continue
                    text=str(output.get("user_message_text") or output.get("notification_text") or ("JARVIS_MONITOR_COMPLETED_V1\n"+packed({"monitor_id":mid,"observed_thread_id":monitor["observed_thread_id"],"turn_id":turn_id})))
                    if target:
                        r=self.adapter.resume_thread(target,text,client_user_message_id=str(client_id),source_event_key=str(monitor["source_event_key"]),model=monitor.get("model") if isinstance(monitor.get("model"),str) else None,reasoning_effort=monitor.get("reasoning_effort") if isinstance(monitor.get("reasoning_effort"),str) else None)
                        if r.get("outcome")=="turn_completed":self.store.complete_delivery(d,"COMPLETED",turn_id=str(r.get("turn_id") or "") or None)
                        else:self.store.complete_delivery(d,"FAILED",error=str(r.get("error") or r.get("outcome"))); self.store.status(mid,"FAILED",str(r.get("outcome"))); break
                    else:
                        r=self.adapter.enqueue_bot_notification(monitor_id=mid,observed_thread_id=str(monitor["observed_thread_id"]),notification_text=text,source_event_key=str(monitor["source_event_key"])); self.store.complete_delivery(d,"QUEUED",outbox_id=str(r.get("outbox_id") or "") or None); pending=True
                else:self.store.status(mid,"OUTPUT_PENDING" if pending else "COMPLETED")
                results.append({"monitor_id":mid,"outcome":"terminal_changed"})
            except Exception as exc: self.store.status(mid,"FAILED",str(exc)); results.append({"monitor_id":mid,"outcome":"failed","error":str(exc)})
        return results
