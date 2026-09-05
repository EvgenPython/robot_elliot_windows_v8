"""Deterministic market facts shared by Scout and FULL.

This module never labels Elliott waves and never makes a trading decision.
It only calculates facts that should not be delegated to an LLM: closed-bar
level relations, broker tick-volume statistics and three-candle imbalances.
"""

from __future__ import annotations

from statistics import median


TIMEFRAMES = ("D1", "H4", "H1", "M15", "M5")
MAX_LEVELS = 24
MAX_IMBALANCES_PER_TIMEFRAME = 8


def _number(value):
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result > 0 else None


def _bars_from_snapshot(snapshot: dict, timeframe: str) -> list[dict]:
    source = snapshot.get("timeframes", {}).get(timeframe, {})
    bars = source.get("closed_bars")
    if bars is None:
        return []
    if hasattr(bars, "to_dict"):
        bars = bars.to_dict("records")
    result = []
    for item in bars if isinstance(bars, list) else []:
        if not isinstance(item, dict):
            continue
        normalized = dict(item)
        if "time" not in normalized and "time_fp" in normalized:
            normalized["time"] = str(normalized["time_fp"])
        else:
            normalized["time"] = str(normalized.get("time", ""))
        result.append(normalized)
    return result


def _reference_levels(previous_reference: dict | None) -> list[dict]:
    if not isinstance(previous_reference, dict):
        return []
    analysis = previous_reference.get("analysis")
    if not isinstance(analysis, dict):
        return []
    visualization = analysis.get("visualization") or {}
    candidates = []
    for item in visualization.get("levels", []):
        if isinstance(item, dict):
            candidates.append({
                "price": item.get("price"),
                "kind": item.get("kind", "reference"),
                "label": item.get("label", ""),
                "timeframe": item.get("timeframe", "H1"),
            })
    for item in visualization.get("projected_waves", []):
        if not isinstance(item, dict):
            continue
        for field, kind in (("confirmation_level", "confirmation"),
                            ("invalidation_level", "invalidation")):
            candidates.append({
                "price": item.get(field), "kind": kind,
                "label": item.get("label", ""),
                "timeframe": item.get("timeframe", "H1"),
            })
    wave_count = analysis.get("wave_count") or {}
    candidates.append({
        "price": wave_count.get("invalidation_level"),
        "kind": "wave_invalidation", "label": "wave invalidation",
        "timeframe": "H1",
    })
    unique = {}
    for item in candidates:
        price = _number(item.get("price"))
        if price is None:
            continue
        key = (round(price, 8), str(item.get("kind")), str(item.get("label")))
        unique[key] = {**item, "price": price}
    return list(unique.values())[:MAX_LEVELS]


def _level_facts(snapshot: dict, previous_reference: dict | None) -> list[dict]:
    bars = _bars_from_snapshot(snapshot, "H1")
    if not bars:
        return []
    latest = bars[-1]
    previous = bars[-2] if len(bars) > 1 else None
    close = _number(latest.get("close"))
    high = _number(latest.get("high"))
    low = _number(latest.get("low"))
    previous_close = _number(previous.get("close")) if previous else None
    if close is None:
        return []
    result = []
    for item in _reference_levels(previous_reference):
        level = item["price"]
        relation = "above" if close > level else "below" if close < level else "at"
        prior_relation = None
        crossed = "none"
        if previous_close is not None:
            prior_relation = "above" if previous_close > level else "below" if previous_close < level else "at"
            if previous_close <= level < close:
                crossed = "closed_cross_up"
            elif previous_close >= level > close:
                crossed = "closed_cross_down"
        touched = bool(low is not None and high is not None and low <= level <= high)
        result.append({
            **item,
            "closed_time": latest.get("time"),
            "closed_h1_close": close,
            "relation": relation,
            "previous_relation": prior_relation,
            "cross_event": crossed,
            "touched_by_latest_h1": touched,
            "distance_price": round(close - level, 8),
            "confirmation": "one_closed_h1_only" if crossed != "none" else "not_newly_crossed",
        })
    return result


def _volume_facts(snapshot: dict) -> dict:
    bars = _bars_from_snapshot(snapshot, "H1")[-21:]
    volumes = []
    for bar in bars:
        value = _number(bar.get("tick_volume"))
        if value is not None:
            volumes.append(value)
    if not volumes:
        return {"available": False, "scope": "broker_tick_volume_only"}
    latest = volumes[-1]
    baseline_values = volumes[:-1] or volumes
    baseline = float(median(baseline_values))
    ratio = latest / baseline if baseline else None
    if ratio is None:
        classification = "unknown"
    elif ratio >= 2.0:
        classification = "exceptionally_high"
    elif ratio >= 1.35:
        classification = "high"
    elif ratio <= 0.5:
        classification = "low"
    else:
        classification = "normal"
    return {
        "available": True,
        "scope": "broker_tick_volume_only_not_centralized_order_flow",
        "latest_closed_h1_time": bars[-1].get("time"),
        "latest_tick_volume": latest,
        "baseline_median_previous_h1": baseline,
        "sample_size": len(baseline_values),
        "ratio_to_median": round(ratio, 4) if ratio is not None else None,
        "classification": classification,
    }


def _imbalance_facts(snapshot: dict, timeframe: str) -> list[dict]:
    bars = _bars_from_snapshot(snapshot, timeframe)
    found = []
    for index in range(2, len(bars)):
        first, middle, third = bars[index - 2], bars[index - 1], bars[index]
        first_high, first_low = _number(first.get("high")), _number(first.get("low"))
        third_high, third_low = _number(third.get("high")), _number(third.get("low"))
        if None in (first_high, first_low, third_high, third_low):
            continue
        direction = None
        low = high = None
        if third_low > first_high:
            direction, low, high = "bullish", first_high, third_low
        elif third_high < first_low:
            direction, low, high = "bearish", third_high, first_low
        if direction is None:
            continue
        later = bars[index + 1:]
        filled_at = None
        for bar in later:
            bar_low, bar_high = _number(bar.get("low")), _number(bar.get("high"))
            if bar_low is None or bar_high is None:
                continue
            if bar_low <= low and bar_high >= high:
                filled_at = bar.get("time")
                break
        found.append({
            "id": f"fvg_{timeframe}_{third.get('time')}_{direction}",
            "timeframe": timeframe,
            "direction": direction,
            "formed_at": third.get("time"),
            "impulse_bar_time": middle.get("time"),
            "price_low": low,
            "price_high": high,
            "size_price": round(high - low, 8),
            "status": "filled" if filled_at else "open",
            "filled_at": filled_at,
            "definition": "three_closed_candle_fair_value_gap",
        })
    open_items = [item for item in found if item["status"] == "open"]
    recent_filled = [item for item in found if item["status"] == "filled"][-2:]
    return (open_items[-MAX_IMBALANCES_PER_TIMEFRAME:] + recent_filled)[-MAX_IMBALANCES_PER_TIMEFRAME:]


def build_deterministic_market_facts(
    snapshot: dict,
    previous_reference: dict | None = None,
) -> dict:
    return {
        "contract": {
            "interpretation_owner": "Claude",
            "facts_owner": "Python",
            "level_relations_are_authoritative": True,
            "volume_statistics_are_authoritative_for_broker_tick_volume_only": True,
            "imbalances_are_geometry_only_not_trade_signals": True,
        },
        "reference_level_statuses": _level_facts(snapshot, previous_reference),
        "h1_tick_volume": _volume_facts(snapshot),
        "imbalances": {
            timeframe: _imbalance_facts(snapshot, timeframe)
            for timeframe in TIMEFRAMES
        },
    }
