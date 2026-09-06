import json
import os
from pathlib import Path
from instruments import symbol_state_path

import MetaTrader5 as mt5

from analysis_schedule import (
    ANALYSIS_POLICY_VERSION,
    DAILY_BASELINE_FULL_CLOSE_HOUR,
)
from claude_reference_state import MAX_REFERENCE_AGE_HOURS
from prop_time import now_fp


BASE_DIR = Path(__file__).resolve().parent
STATE_DIR = BASE_DIR / "state"
RUNNER_STATUS_PATH = symbol_state_path("runner_status.json")
TRADE_STATE_PATH = symbol_state_path("trade_state.json")
ANALYSIS_STATE_PATH = symbol_state_path("analysis_state.json")


def _read_json(path: Path) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as file:
            value = json.load(file)
    except (OSError, json.JSONDecodeError):
        return {}

    return value if isinstance(value, dict) else {}


def _selected_attributes(value, names: tuple[str, ...]) -> dict | None:
    if value is None:
        return None

    result = {}
    for name in names:
        item = getattr(value, name, None)
        if item is not None:
            result[name] = item

    return result


def _plan_summary(plan) -> dict | None:
    if not isinstance(plan, dict):
        return None

    fields = (
        "plan_id",
        "status",
        "execution_status",
        "action",
        "order_type",
        "entry_price",
        "stop_loss",
        "take_profit",
        "volume",
        "pending_order_ticket",
        "position_ticket",
        "source_h1_closed_bar_time",
        "created_at_fp",
        "updated_at_fp",
    )
    return {
        key: plan.get(key)
        for key in fields
        if key in plan
    }


def _atomic_write(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")

    with open(temporary_path, "w", encoding="utf-8") as file:
        json.dump(
            payload,
            file,
            ensure_ascii=False,
            indent=2,
        )
        file.flush()
        os.fsync(file.fileno())

    os.replace(temporary_path, path)


def write_runner_status(
    connected: bool,
    daily_state: dict | None,
    status: str = "running",
    last_error=None,
) -> bool:
    """
    Best-effort локальный heartbeat для read-only Linux dashboard.
    Любая ошибка здесь подавляется и не влияет на торговый runner.
    """
    try:
        terminal = mt5.terminal_info() if connected else None
        account = mt5.account_info() if connected else None

        trade_state = _read_json(TRADE_STATE_PATH)
        analysis_state = _read_json(ANALYSIS_STATE_PATH)
        managed_positions = trade_state.get("managed_positions", [])
        pending_actions = trade_state.get("pending_actions", [])

        if not isinstance(managed_positions, list):
            managed_positions = []
        if not isinstance(pending_actions, list):
            pending_actions = []

        payload = {
            "schema_version": 1,
            "generated_at_fp": now_fp().isoformat(),
            "runner_status": str(status),
            "runner_alive": status in {"running", "reconnecting"},
            "last_error": str(last_error) if last_error else None,
            "mt5": {
                "connected": bool(connected),
                "terminal": _selected_attributes(
                    terminal,
                    (
                        "connected",
                        "trade_allowed",
                        "tradeapi_disabled",
                        "build",
                    ),
                ),
            },
            "account": _selected_attributes(
                account,
                (
                    "balance",
                    "equity",
                    "margin",
                    "margin_free",
                    "margin_level",
                    "profit",
                    "currency",
                    "leverage",
                    "trade_allowed",
                    "trade_expert",
                ),
            ),
            "fundingpips_daily": daily_state,
            "analysis_policy": {
                "version": ANALYSIS_POLICY_VERSION,
                "scheduled_fulls_per_fp_day": 1,
                "daily_baseline_full_close_hour_fp": (
                    DAILY_BASELINE_FULL_CLOSE_HOUR
                ),
                "other_closed_h1_mode": "SCOUT",
                "event_full_triggers": [
                    "structure_break",
                    "new_wave",
                    "new_setup",
                    "uncertainty",
                    "missing_or_invalid_reference",
                ],
                "reference_same_fp_day_only": True,
                "reference_max_age_hours": MAX_REFERENCE_AGE_HOURS,
                "managed_position_mode": "CLAUDE_OFF",
                "resume_after_confirmed_close": True,
            },
            "trading": {
                "trade_state_updated_at_fp": trade_state.get("updated_at_fp"),
                "active_plan": _plan_summary(trade_state.get("active_plan")),
                "managed_positions_count": len(managed_positions),
                "managed_positions": [
                    _plan_summary(item)
                    for item in managed_positions
                    if isinstance(item, dict)
                ],
                "pending_actions_count": len(pending_actions),
                "last_decision": trade_state.get("last_decision"),
            },
            "analysis": {
                "analysis_state_updated_at_fp": analysis_state.get("updated_at_fp"),
                "last_analyzed_h1": analysis_state.get("last_analyzed_h1"),
                "last_analysis_time_fp": analysis_state.get("last_analysis_time_fp"),
                "last_processing_status": analysis_state.get("last_processing_status"),
                "last_cycle_type": analysis_state.get("last_cycle_type"),
                "last_risk_decision": analysis_state.get("last_risk_decision"),
                "last_claude_action": analysis_state.get("last_claude_action"),
                "last_confidence": analysis_state.get("last_confidence"),
                "last_plan_id": analysis_state.get("last_plan_id"),
            },
        }

        _atomic_write(RUNNER_STATUS_PATH, payload)
        return True
    except Exception:
        return False
