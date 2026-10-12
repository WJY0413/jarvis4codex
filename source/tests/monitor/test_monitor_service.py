import sqlite3
import tempfile
import unittest
from pathlib import Path

from jarvis_monitor import MonitorService, MonitorStore


class FakeAdapter:
    def __init__(self):
        self.state={"id":"child-1","status":"running","turns":[{"id":"turn-1","status":"inProgress"}]}; self.resumes=[]; self.notifications=[]; self.bot_status="queued"
    def read_thread(self, thread_id): return self.state
    def resume_thread(self, thread_id, user_message_text, **kwargs):
        self.resumes.append((thread_id,user_message_text,kwargs)); return {"outcome":"turn_completed","turn_id":"turn-parent"}
    def enqueue_bot_notification(self, **kwargs): self.notifications.append(kwargs); return {"outbox_id":"outbox-1","queued":True}
    def read_bot_delivery(self, outbox_id): return {"delivery_status":self.bot_status}


class MonitorServiceTest(unittest.TestCase):
    def due_now(self, store, monitor_id):
        con=sqlite3.connect(store.db_path)
        try:
            con.execute("UPDATE monitors SET next_observation_at='2000-01-01T00:00:00+00:00' WHERE monitor_id=?",(monitor_id,)); con.commit()
        finally:
            con.close()

    def test_terminal_change_fans_out_custom_resume_and_default_bot_message_once(self):
        with tempfile.TemporaryDirectory() as temp:
            adapter=FakeAdapter(); store=MonitorStore(Path(temp)/"monitor.sqlite"); service=MonitorService(store,adapter)
            monitor=store.start({"monitor_key":"case-1","observed_thread_id":"child-1","source_thread_id":"source-1","outputs":[{"type":"resume_thread","target_thread_id":"parent-1","user_message_text":"自定义完成消息"},{"type":"notify_jarvis_bot"}]})
            service.run_once(); self.due_now(store,monitor["monitor_id"])
            adapter.state={"id":"child-1","status":"idle","turns":[{"id":"turn-1","status":"completed"}]}
            changed=service.run_once(); self.assertEqual(changed[0]["outcome"],"terminal_changed",changed)
            self.assertEqual(adapter.resumes[0][0],"parent-1"); self.assertEqual(adapter.resumes[0][1],"自定义完成消息")
            self.assertEqual(adapter.resumes[0][2]["client_user_message_id"].split(":")[0],"monitor")
            self.assertTrue(adapter.notifications[0]["notification_text"].startswith("JARVIS_MONITOR_COMPLETED_V1"))
            self.assertEqual(store.get(monitor["monitor_id"])["monitor_status"],"OUTPUT_PENDING")
            self.assertEqual(len(adapter.resumes),1)

    def test_existing_terminal_is_baseline_and_never_resumes(self):
        with tempfile.TemporaryDirectory() as temp:
            adapter=FakeAdapter(); adapter.state={"id":"child-1","status":"idle","turns":[{"id":"old","status":"completed"}]}; store=MonitorStore(Path(temp)/"monitor.sqlite"); service=MonitorService(store,adapter)
            monitor=store.start({"observed_thread_id":"child-1","outputs":[{"type":"resume_thread","target_thread_id":"parent-1"}]})
            self.assertEqual(service.run_once()[0]["outcome"],"baseline_terminal"); self.assertEqual(adapter.resumes,[])

    def test_queued_bot_output_is_read_back_before_monitor_completes(self):
        with tempfile.TemporaryDirectory() as temp:
            adapter=FakeAdapter(); store=MonitorStore(Path(temp)/"monitor.sqlite"); service=MonitorService(store,adapter)
            monitor=store.start({"observed_thread_id":"child-1","outputs":[{"type":"notify_jarvis_bot"}]})
            service.run_once(); self.due_now(store,monitor["monitor_id"]); adapter.state={"id":"child-1","status":"idle","turns":[{"id":"turn-1","status":"completed"}]}; service.run_once()
            self.assertEqual(store.get(monitor["monitor_id"])["monitor_status"],"OUTPUT_PENDING")
            self.due_now(store,monitor["monitor_id"]); adapter.bot_status="delivered"; self.assertEqual(service.run_once()[0]["outcome"],"completed")
            self.assertEqual(store.get(monitor["monitor_id"])["monitor_status"],"COMPLETED")

    def test_failed_bot_delivery_requires_readback(self):
        with tempfile.TemporaryDirectory() as temp:
            adapter=FakeAdapter(); store=MonitorStore(Path(temp)/"monitor.sqlite"); service=MonitorService(store,adapter)
            monitor=store.start({"observed_thread_id":"child-1","outputs":[{"type":"notify_jarvis_bot"}]})
            service.run_once(); self.due_now(store,monitor["monitor_id"]); adapter.state={"id":"child-1","status":"idle","turns":[{"id":"turn-1","status":"completed"}]}; service.run_once()
            self.due_now(store,monitor["monitor_id"]); adapter.bot_status="failed"; self.assertEqual(service.run_once()[0]["outcome"],"requires_readback")
            self.assertEqual(store.get(monitor["monitor_id"])["monitor_status"],"REQUIRES_READBACK")

    def test_two_outputs_preserve_prior_terminal_failures_across_restart_without_resending(self):
        # PKG-2: Astra's FAILED/QUEUED -> FAILED/DELIVERED reproduction.
        for first_status in ("failed", "expired", "delivered"):
            with self.subTest(first_status=first_status), tempfile.TemporaryDirectory() as temp:
                class TwoOutputs(FakeAdapter):
                    def __init__(self):
                        super().__init__()
                        self.statuses = {"a": first_status, "b": "queued"}
                    def enqueue_bot_notification(self, **kwargs):
                        self.notifications.append(kwargs)
                        return {"outbox_id": "a" if len(self.notifications) == 1 else "b"}
                    def read_bot_delivery(self, outbox_id): return {"delivery_status": self.statuses[outbox_id]}
                adapter = TwoOutputs()
                store = MonitorStore(Path(temp) / "monitor.sqlite")
                mid = store.start({"observed_thread_id": "child-1", "outputs": [{"type": "notify_jarvis_bot"}, {"type": "notify_jarvis_bot"}]})["monitor_id"]
                service = MonitorService(store, adapter)
                service.run_once(); self.due_now(store, mid)
                adapter.state = {"id": "child-1", "status": "idle", "turns": [{"id": "turn-1", "status": "completed"}]}
                service.run_once(); self.due_now(store, mid)
                self.assertEqual(service.run_once()[0]["outcome"], "output_pending")
                adapter.statuses["b"] = "delivered"
                store = MonitorStore(store.db_path)
                self.due_now(store, mid)
                expected = "COMPLETED" if first_status == "delivered" else "REQUIRES_READBACK"
                self.assertEqual(MonitorService(store, adapter).run_once()[0]["outcome"], expected.lower())
                self.assertEqual(store.get(mid)["monitor_status"], expected)
                self.assertEqual(len(adapter.notifications), 2)
                self.assertEqual(adapter.resumes, [])
                self.due_now(store, mid)
                self.assertEqual(MonitorService(store, adapter).run_once(), [])
                self.assertEqual(len(adapter.notifications), 2)

    def test_persisted_unknown_pending_or_missing_output_is_not_success(self):
        # PKG-2: old uncertain/incomplete delivery evidence cannot mean COMPLETED.
        for uncertain in ("PENDING", "UNKNOWN", "COMPLETED", None):
            with self.subTest(uncertain=uncertain), tempfile.TemporaryDirectory() as temp:
                adapter = FakeAdapter()
                store = MonitorStore(Path(temp) / "monitor.sqlite")
                mid = store.start({"observed_thread_id": "child-1", "outputs": [{"type": "notify_jarvis_bot"}, {"type": "notify_jarvis_bot"}]})["monitor_id"]
                service = MonitorService(store, adapter)
                service.run_once(); self.due_now(store, mid)
                adapter.state = {"id": "child-1", "status": "idle", "turns": [{"id": "turn-1", "status": "completed"}]}
                service.run_once()
                with store.session() as db:
                    db.execute("UPDATE monitor_deliveries SET delivery_status='DELIVERED' WHERE output_index=0")
                    if uncertain is None:
                        db.execute("DELETE FROM monitor_deliveries WHERE output_index=1")
                    else:
                        db.execute("UPDATE monitor_deliveries SET delivery_status=? WHERE output_index=1", (uncertain,))
                self.due_now(store, mid)
                self.assertEqual(service.run_once()[0]["outcome"], "requires_readback")
                self.assertEqual(store.get(mid)["monitor_status"], "REQUIRES_READBACK")
                self.assertEqual(len(adapter.notifications), 2)


if __name__ == "__main__": unittest.main()
