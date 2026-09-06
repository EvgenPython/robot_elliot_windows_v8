import unittest
from datetime import datetime, timezone

import pandas as pd

from market_facts import build_deterministic_market_facts


def _bar(hour, o, h, low, c, volume=100):
    return {
        "time_fp": datetime(2026, 9, 4, hour, tzinfo=timezone.utc),
        "open": o, "high": h, "low": low, "close": c,
        "tick_volume": volume,
    }


class MarketFactsTests(unittest.TestCase):
    def _snapshot(self):
        bars = [
            _bar(1, 99, 100, 98, 99, 100),
            _bar(2, 103, 105, 103, 104, 110),
            _bar(3, 105, 107, 104, 106, 300),
        ]
        return {"timeframes": {name: {"closed_bars": pd.DataFrame(bars)} for name in ("D1", "H4", "H1", "M15", "M5")}}

    def test_level_status_uses_closed_h1(self):
        reference = {"analysis": {"visualization": {"levels": [{"price": 102, "kind": "resistance", "label": "L", "timeframe": "H1"}]}}}
        facts = build_deterministic_market_facts(self._snapshot(), reference)
        level = facts["reference_level_statuses"][0]
        self.assertEqual(level["relation"], "above")
        self.assertEqual(level["cross_event"], "none")

    def test_detects_three_candle_fvg(self):
        facts = build_deterministic_market_facts(self._snapshot())
        item = facts["imbalances"]["H1"][0]
        self.assertEqual(item["direction"], "bullish")
        self.assertEqual(item["price_low"], 100.0)
        self.assertEqual(item["price_high"], 104.0)
        self.assertEqual(item["status"], "open")
        self.assertEqual(item["current_relation"], "above")
        self.assertTrue(
            facts["contract"]["imbalances_must_be_assessed_in_trade_decision"]
        )

    def test_fvg_tracks_partial_fill_without_marking_it_filled(self):
        bars = [
            _bar(1, 99, 100, 98, 99),
            _bar(2, 103, 105, 103, 104),
            _bar(3, 105, 107, 104, 106),
            _bar(4, 105, 106, 102, 103),
        ]
        snapshot = {"timeframes": {name: {"closed_bars": pd.DataFrame(bars)} for name in ("D1", "H4", "H1", "M15", "M5")}}
        item = build_deterministic_market_facts(snapshot)["imbalances"]["H1"][0]
        self.assertEqual(item["status"], "partially_filled")
        self.assertEqual(item["fill_fraction"], 0.5)

    def test_volume_is_normalized(self):
        facts = build_deterministic_market_facts(self._snapshot())
        volume = facts["h1_tick_volume"]
        self.assertEqual(volume["baseline_median_previous_h1"], 105.0)
        self.assertEqual(volume["classification"], "exceptionally_high")


if __name__ == "__main__":
    unittest.main()
