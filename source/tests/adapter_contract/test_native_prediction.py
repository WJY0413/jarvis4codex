"""Native protocol contract; synthetic transport, never production state."""
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from adapters.codex_app_server.prediction import NativePredictor, latest_turn, prepare_prediction_prompt


class Client:
    def __init__(self):
        self.calls = []
        self.source = {"id": "source", "turns": [{"id": "t1", "status": "completed"}]}
        self.output = '{"suggestion":"next step"}'
        self.changed = False
        self.config = SimpleNamespace(turn_completion_timeout_seconds=1)
    def start(self): pass
    def request(self, method, params, **kwargs):
        self.calls.append((method, params))
        if method == "thread/read":
            if self.changed and any(m == "turn/start" for m, _ in self.calls):
                return {"thread": {"id": "source", "turns": [{"id": "t2", "status": "completed"}]}}
            return {"thread": self.source}
        if method == "thread/turns/list":
            turn = {"id": "t2", "status": "completed"} if self.changed and any(m == "turn/start" for m, _ in self.calls) else self.source["turns"][-1]
            return {"data": [{**turn, "itemsView": "notLoaded", "items": []}], "nextCursor": None}
        if method == "thread/fork":
            native = params.get("experimentalPredictionMode") is True
            return {"thread": {"id": "prediction" if native else "parent", "ephemeral": native},
                    "model": "gpt-6.1-sol", "modelProvider": "openai"}
        if method == "turn/start": return {"turn": {"id": "prediction-turn"}}
        if method == "thread/archive": return {}
        raise AssertionError(method)
    def wait_for_turn_terminal(self, *args, **kwargs): return {"id": "prediction-turn", "status": "completed"}
    def wait_for_turn_readback(self, *args, **kwargs): return self.output


class PredictionContract(unittest.TestCase):
    def test_unknown_prediction_terminal_is_not_cleaned_by_parent_archive(self):
        with tempfile.TemporaryDirectory() as temp:
            client = Client()
            with patch.object(client, "wait_for_turn_terminal", side_effect=TimeoutError("exact terminal missing")):
                result = NativePredictor(client, Path(temp), self.config).predict(thread_id="source", request_id="unknown")
            self.assertEqual(result["status"], "requires_readback")
            self.assertFalse(result["cleanup_completed"])
            self.assertFalse(result["prediction_terminal_confirmed"])
            self.assertTrue(result["parent_archived"])
            self.assertIn(("thread/archive", {"threadId": "parent"}), client.calls)

    def test_failed_durable_intent_never_forks_or_dispatches(self):
        with tempfile.TemporaryDirectory() as temp:
            client = Client()
            with patch("os.fsync", side_effect=OSError("intent fsync failed")):
                result = NativePredictor(client, Path(temp), self.config).predict(thread_id="source", request_id="durability")
            self.assertEqual(result["status"], "failed")
            self.assertFalse(any(m in {"thread/fork", "turn/start"} for m, _ in client.calls))

    def test_latest_prediction_source_is_metadata_only_and_bounded(self):
        client = Client()
        result = latest_turn(client, "source")
        self.assertEqual(result["id"], "t1")
        self.assertTrue(all(p.get("includeTurns") is False for m, p in client.calls if m == "thread/read"))
        pages = [p for m, p in client.calls if m == "thread/turns/list"]
        self.assertEqual(pages, [{"threadId": "source", "itemsView": "notLoaded", "sortDirection": "desc", "limit": 1}])

    def test_active_source_deadline_does_not_dispatch_a_prediction(self):
        client = Client()
        client.source["turns"][0]["status"] = "inProgress"
        with tempfile.TemporaryDirectory() as temp, patch("adapters.codex_app_server.prediction.time.monotonic", side_effect=[0, 2]), patch("adapters.codex_app_server.prediction.time.sleep", side_effect=AssertionError("unbounded wait")):
            result = prepare_prediction_prompt(client, thread_id="source", request_id="timeout",
                options={"mode": "prediction"}, receipt_dir=Path(temp))
        self.assertEqual(result["status"], "requires_readback")
        self.assertFalse(any(m in {"turn/start", "thread/fork"} for m, _ in client.calls))

    def config(self):
        return {"is_enabled": True, "prompt": "official fixture", "prompt_version": "5",
                "prediction_reasoning_effort": "high", "unsupported_models": []}
    def test_native_protocol_replay_and_conflict(self):
        with tempfile.TemporaryDirectory() as temp:
            client = Client()
            predictor = NativePredictor(client, Path(temp), self.config)
            result = predictor.predict(thread_id="source", request_id="one", source_turn_id="t1")
            self.assertEqual(result["suggestion"], "next step")
            self.assertTrue(result["source_unchanged"] and result["cleanup_completed"])
            forks = [p for m, p in client.calls if m == "thread/fork"]
            self.assertTrue(forks[1]["experimentalPredictionMode"])
            self.assertEqual(forks[0]["lastTurnId"], "t1")
            count = len(client.calls)
            self.assertEqual(predictor.predict(thread_id="source", request_id="one", source_turn_id="t1"), result)
            self.assertEqual(len(client.calls), count)
            self.assertEqual(predictor.predict(thread_id="source", request_id="one", source_turn_id="t2")["status"], "conflict")
    def test_disabled_empty_invalid_and_stale(self):
        for case in ("disabled", "empty", "invalid", "stale", "active"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temp:
                client = Client()
                config = self.config()
                if case == "disabled": config["is_enabled"] = False
                if case == "empty": client.output = '{"suggestion":null}'
                if case == "invalid": client.output = '{}'
                if case == "stale": client.changed = True
                if case == "active": client.source["turns"][0]["status"] = "inProgress"
                result = NativePredictor(client, Path(temp), lambda: config).predict(thread_id="source", request_id="one")
                expected = {"disabled":"disabled", "empty":"no_prediction", "invalid":"failed", "stale":"stale", "active":"waiting"}
                self.assertEqual(result["status"], expected[case])
                if case in ("disabled", "active"):
                    self.assertFalse(any(m == "turn/start" for m, _ in client.calls))
                else:
                    self.assertIn(("thread/archive", {"threadId":"parent"}), client.calls)

