"""Explicit user requirements: composition, legacy migration, task stopping."""
import unittest
from jarvis_control.provisioning import compose_resume_prompt, validate_prediction_options
from jarvis_monitor import HoldTurnMonitor
from tests.monitor import test_hold_turn_monitor as monitor_tests


class PredictionPolicy(unittest.TestCase):
    def test_composition_preserves_fixed_text_and_legacy(self):
        fixed = '  Only change module A.\nKeep output in Chinese.\n'
        self.assertEqual(compose_resume_prompt("fixed", fixed, None), fixed)
        self.assertEqual(compose_resume_prompt("prediction", "", "do next"), "do next")
        combined = compose_resume_prompt("prediction_plus_fixed", fixed, "do next")
        self.assertIn(fixed, combined)
        self.assertIn("do next", combined)
        with self.assertRaises(ValueError): compose_resume_prompt("prediction_plus_fixed", fixed, None)
        self.assertEqual(validate_prediction_options(None), {})
        with self.assertRaises(ValueError): validate_prediction_options({"mode":"typo"})
    def test_project_done_before_budget_and_prediction(self):
        helper = monitor_tests.HoldTurnMonitorTest()
        req = helper.request(completion_marker="PROJECT_DONE", turn_count=2, max_turns=2)
        result = HoldTurnMonitor().observe(monitor_tests.FakeHeldTurnClient(content="finished\nPROJECT_DONE"), req)
        self.assertEqual((result.action, result.result_status, result.reason), ("STOP", "completed", "project_completion_marker"))
        result = HoldTurnMonitor().observe(monitor_tests.FakeHeldTurnClient(content="not PROJECT_DONE yet"), req)
        self.assertEqual(result.result_status, "turn_limit_reached")

