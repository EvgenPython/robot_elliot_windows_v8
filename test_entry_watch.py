import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import entry_watch


class EntryWatchTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.state_dir = Path(self.temporary.name)
        self.path_patch = patch.object(
            entry_watch, "ENTRY_WATCH_PATH", self.state_dir / "entry_watch.json"
        )
        self.dir_patch = patch.object(entry_watch, "STATE_DIR", self.state_dir)
        self.path_patch.start()
        self.dir_patch.start()

    def tearDown(self):
        self.path_patch.stop()
        self.dir_patch.stop()
        self.temporary.cleanup()

    def test_refresh_prefers_m5_primary_projection(self):
        analysis = {
            "timestamp": "2026-08-30T08:00:00+03:00",
            "visualization": {
                "projected_waves": [
                    {
                        "projection_id": "m15",
                        "scenario": "primary",
                        "timeframe": "M15",
                        "direction": "down",
                        "confirmation_level": 4500.0,
                    },
                    {
                        "projection_id": "m5",
                        "scenario": "primary",
                        "timeframe": "M5",
                        "direction": "down",
                        "confirmation_level": 4490.0,
                    },
                ]
            },
        }
        state = entry_watch.refresh_entry_watch(analysis, "2026-08-30T07:00:00+03:00")
        self.assertEqual(state["status"], "watching")
        self.assertEqual(state["projection"]["projection_id"], "m5")

    def test_closed_bar_confirmation_triggers_once(self):
        entry_watch._atomic_write(
            {
                "status": "watching",
                "projection": {
                    "projection_id": "p1",
                    "timeframe": "M5",
                    "direction": "down",
                    "confirmation_level": 4490.0,
                    "invalidation_level": 4520.0,
                },
                "last_checked_closed_bar_time": None,
            }
        )
        bar = {
            "time": "2026-08-30T08:05:00+03:00",
            "open": 4501.0,
            "high": 4502.0,
            "low": 4487.0,
            "close": 4489.0,
        }
        with patch.object(entry_watch, "_latest_closed_bar", return_value=bar):
            result = entry_watch.inspect_entry_trigger()
        self.assertTrue(result["triggered"])
        self.assertEqual(entry_watch.load_entry_watch()["status"], "triggered")

    def test_invalidation_wins_over_confirmation(self):
        entry_watch._atomic_write(
            {
                "status": "watching",
                "projection": {
                    "projection_id": "p2",
                    "timeframe": "M15",
                    "direction": "up",
                    "confirmation_level": 4520.0,
                    "invalidation_level": 4490.0,
                },
                "last_checked_closed_bar_time": None,
            }
        )
        bar = {
            "time": "2026-08-30T08:15:00+03:00",
            "open": 4500.0,
            "high": 4502.0,
            "low": 4480.0,
            "close": 4485.0,
        }
        with patch.object(entry_watch, "_latest_closed_bar", return_value=bar):
            result = entry_watch.inspect_entry_trigger()
        self.assertFalse(result["triggered"])
        self.assertTrue(result["invalidated"])
        saved = entry_watch.load_entry_watch()
        self.assertEqual(saved["status"], "invalidated")
        self.assertEqual(saved["last_checked_bar"]["close"], 4485.0)
        self.assertEqual(
            saved["terminal_event"]["kind"], "entry_projection_invalidated"
        )


if __name__ == "__main__":
    unittest.main()
