"""Paid, decision-only confirmation after a deterministic M15/M5 trigger."""

import copy

from analysis_archive import save_analysis_archive, safe_update_analysis_archive
from chart_contract import sanitize_visualization
from claude_client import validate_analysis_contract, validate_trade_levels
from claude_payload import build_claude_payload
from claude_reference_state import get_fresh_reference_for_snapshot
from claude_staged_client import analyze_entry_check, get_last_stage_usage
from entry_watch import mark_entry_check_result
from live_executor import execute_active_plan
from market_data import get_market_snapshot
from prop_time import now_fp
from risk_manager import evaluate_trade
from trade_state import register_trade_decision
from web_market_snapshot import save_web_market_snapshot


VISUAL_ARRAYS = (
    "wave_points", "levels", "zones", "scenario_paths", "trendlines",
    "channels", "pattern_shapes", "market_events", "projected_waves",
    "wave_structures",
)


def _merge_visualization(base: dict, fresh: dict) -> dict:
    result = copy.deepcopy(base if isinstance(base, dict) else {})
    for name in VISUAL_ARRAYS:
        merged = []
        seen = set()
        for item in list(result.get(name) or []) + list(fresh.get(name) or []):
            marker = repr(item)
            if marker in seen:
                continue
            seen.add(marker)
            merged.append(copy.deepcopy(item))
        result[name] = merged
    old_comment = str(result.get("chart_comment", "")).strip()
    new_comment = str(fresh.get("chart_comment", "")).strip()
    result["chart_comment"] = " ".join(item for item in (old_comment, new_comment) if item)
    return result


def run_entry_check(symbol: str = "XAUUSD") -> dict:
    reference = get_fresh_reference_for_snapshot(
        {"instrument": symbol, "generated_at_fp": now_fp().isoformat()}
    )
    if not reference or not isinstance(reference.get("analysis"), dict):
        mark_entry_check_result("blocked_no_fresh_full_reference")
        return {"ok": False, "reason": "no_fresh_full_reference"}

    snapshot = get_market_snapshot(symbol)
    save_web_market_snapshot(snapshot)
    payload = build_claude_payload(snapshot)
    archive = save_analysis_archive(
        snapshot=snapshot,
        cycle_type="ENTRY_CHECK",
        payload=payload,
        previous_reference=reference,
        note="ENTRY_CHECK payload saved before the single paid decision call.",
    )
    try:
        decision = analyze_entry_check(
            payload,
            market_map=reference["analysis"],
            previous_reference=reference,
        )
        analysis = copy.deepcopy(reference["analysis"])
        analysis["timestamp"] = payload.get("timestamp")
        analysis["recommendation"] = copy.deepcopy(decision["recommendation"])
        analysis["data_quality"] = copy.deepcopy(decision["data_quality"])
        analysis["visualization"] = _merge_visualization(
            analysis.get("visualization") or {}, decision.get("visualization") or {}
        )
        validate_trade_levels(analysis)
        validate_analysis_contract(analysis)
        sanitize_visualization(analysis, payload)

        risk_report = evaluate_trade(analysis=analysis, symbol=symbol)
        state_result = register_trade_decision(
            analysis=analysis, risk_report=risk_report, snapshot=snapshot
        )
        execution_report = execute_active_plan(snapshot=snapshot, symbol=symbol)
        safe_update_analysis_archive(
            archive,
            result=analysis,
            trade_decision_result=decision,
            trade_decision_usage=get_last_stage_usage("ENTRY_CHECK"),
            risk_report=risk_report,
            trade_state_result=state_result,
            execution_report=execution_report,
            note="ENTRY_CHECK completed; the higher-timeframe FULL map was not rebuilt.",
        )
        mark_entry_check_result(str(risk_report.get("decision") or "completed"))
        return {
            "ok": True,
            "analysis": analysis,
            "risk_report": risk_report,
            "state_result": state_result,
            "execution_report": execution_report,
            "archive": str(archive),
        }
    except Exception as error:
        safe_update_analysis_archive(
            archive,
            note=f"ENTRY_CHECK failed closed: {type(error).__name__}: {error}",
        )
        mark_entry_check_result(f"failed_closed:{type(error).__name__}")
        return {"ok": False, "reason": f"{type(error).__name__}: {error}", "archive": str(archive)}
