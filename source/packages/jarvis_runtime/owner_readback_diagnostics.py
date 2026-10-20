"""Best-effort bounded telemetry; never lifecycle authority or restart state."""
import hashlib
import json
import os
from pathlib import Path
import threading
import time
import uuid


class OwnerReadbackDiagnostics:
    def __init__(self, state_dir, host_instance_id):
        self.directory=Path(state_dir)/'owner-readback-diagnostics'
        self.host_instance_id=host_instance_id
        self._lock=threading.Lock()
        self._pending={}

    def enqueue(self, value):
        """Owner-side path has no file I/O, waiting lock, or lifecycle mutation."""
        keys=('request_id','hold_id','thread_id','turn_id','model')
        if not isinstance(value,dict) or any(not isinstance(value.get(k),str) or not value[k] or len(value[k])>256 for k in keys):return
        method=value.get('method')
        if method not in ('thread/read','thread/turns/list','thread/items/list'):return
        cursor=value.get('cursor')
        if cursor is not None and (not isinstance(cursor,str) or len(cursor.encode())>4096):return
        record={k:value[k] for k in keys}
        record.update(schema='jarvis-owner-readback-diagnostic/v1',authority=False,restart_checkpoint=False,
            host_instance_id=self.host_instance_id,observed_at=time.time(),method=method,cursor=cursor,
            stage=value.get('stage') if value.get('stage') in ('scan','revalidate') else 'scan',
            classification=value.get('classification') if value.get('classification') in
                ('slice_pending','read_timeout_or_pending','scan_rejected','overall_budget_exhausted') else 'scan_rejected')
        identity=[self.host_instance_id]+[record[k] for k in keys]
        key=hashlib.sha256(json.dumps(identity,separators=(',',':')).encode()).hexdigest()
        if not self._lock.acquire(blocking=False):return
        try:
            if key in self._pending or len(self._pending)<128:self._pending[key]=record
        finally:self._lock.release()

    def flush(self):
        """Existing Host heartbeat calls this; at most four small files per tick."""
        if not self._lock.acquire(blocking=False):return
        try:
            batch=[]
            for key in list(self._pending)[:4]:batch.append((key,self._pending.pop(key)))
        finally:self._lock.release()
        for key,record in batch:
            temporary=None
            try:
                if self.directory.is_symlink():continue
                self.directory.mkdir(exist_ok=True)
                target=self.directory/(key+'.json')
                if target.is_symlink():continue
                temporary=self.directory/(key+'.'+uuid.uuid4().hex+'.tmp')
                raw=json.dumps(record,ensure_ascii=True,separators=(',',':')).encode()
                if len(raw)>32768:continue
                with temporary.open('xb') as handle:
                    handle.write(raw);handle.flush();os.fsync(handle.fileno())
                os.replace(temporary,target)
            except (OSError,ValueError,TypeError):
                pass # Optional telemetry can be lost; ownership never changes.
            finally:
                if temporary is not None:
                    try:temporary.unlink(missing_ok=True)
                    except OSError:pass
