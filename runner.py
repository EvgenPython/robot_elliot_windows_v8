import time
from datetime import datetime
from pathlib import Path

import MetaTrader5 as mt5

from analysis_schedule import DAILY_BASELINE_FULL_CLOSE_HOUR
from analysis_state import load_analysis_state
from execution_control import inspect_execution_safety_gate
from live_executor import print_live_execution_report
from main import main as run_main_cycle
from mt5_client import connect_mt5, disconnect_mt5
from pending_executor import (
    PENDING_ORDER_TYPES,
    execute_pending_cancel,
    reconcile_active_pending_execution,
)
from prop_time import now_fp
from risk_manager import (
    get_account_info,
    get_daily_state,
    get_positions,
)
from runtime_policy import inspect_market_runtime_gate
from single_instance import (
    SingleInstanceError,
    SingleInstanceLock,
)
from trade_state import (
    get_active_plan,
    get_managed_positions,
)
from web_runtime_state import write_runner_status
from entry_watch import inspect_entry_trigger
from entry_check_cycle import run_entry_check


# ============================================================
# RUNNER SETTINGS
# ============================================================

SYMBOL = "XAUUSD"

BASE_DIR = Path(__file__).resolve().parent
LOCK_PATH = BASE_DIR / "state" / "runner.lock"

# Лёгкий polling MT5.
POLL_INTERVAL_SECONDS = 10

# Полный POSITION HOLD audit через существующий main.py.
POSITION_AUDIT_INTERVAL_SECONDS = 60

# Для market/not_sent/send_intent entry lifecycle.
ENTRY_RETRY_INTERVAL_SECONDS = 15

# Если Claude/API упал ДО регистрации H1, не долбим API каждые 10 сек.
SAME_H1_ANALYSIS_RETRY_SECONDS = 300

# Повтор отмены pending, если предыдущая попытка не смогла
# однозначно завершиться.
PENDING_CANCEL_RETRY_SECONDS = 60

# Короткий heartbeat, когда ничего не происходит.
HEARTBEAT_INTERVAL_SECONDS = 300

# На закрытом рынке одинаковый heartbeat раз в 5 минут создаёт сотни строк,
# не добавляя диагностической ценности.
MARKET_CLOSED_HEARTBEAT_INTERVAL_SECONDS = 3600

# При потере MT5 соединения.
RECONNECT_INTERVAL_SECONDS = 30


# ============================================================
# HELPERS
# ============================================================

def _parse_iso(value) -> datetime | None:
    if not value:
        return None

    try:
        result = datetime.fromisoformat(
            str(value)
        )
    except ValueError:
        return None

    if result.tzinfo is None:
        return None

    return result


def _seconds_since(
    timestamp: datetime | None,
) -> float:
    if timestamp is None:
        return float("inf")

    return max(
        0.0,
        (
            now_fp()
            - timestamp
        ).total_seconds(),
    )


def _mt5_connection_alive() -> bool:
    terminal = mt5.terminal_info()
    account = mt5.account_info()

    return bool(
        terminal is not None
        and account is not None
        and getattr(
            terminal,
            "connected",
            False,
        )
    )


def _print_runner_header():
    print()
    print("=" * 80)
    print("CLAUDE ROBOT — CONTINUOUS RUNNER")
    print("=" * 80)
    print(f"Symbol:                    {SYMBOL}")
    print(f"Polling:                   {POLL_INTERVAL_SECONDS} sec")
    print(
        "Position audit:            "
        f"{POSITION_AUDIT_INTERVAL_SECONDS} sec"
    )
    print(
        "Same H1 retry after error: "
        f"{SAME_H1_ANALYSIS_RETRY_SECONDS} sec"
    )
    print(f"Lock file:                 {LOCK_PATH}")
    print("[INFO] Технический мониторинг работает 24/7.")
    print("[INFO] Claude запускается только через все runtime gates.")
    print(
        "[POLICY] Daily baseline FULL: "
        f"{DAILY_BASELINE_FULL_CLOSE_HOUR:02d}:00 FP."
    )


def _heartbeat_interval(market_gate: dict | None) -> int:
    if isinstance(market_gate, dict) and not market_gate.get("tick_fresh", True):
        return MARKET_CLOSED_HEARTBEAT_INTERVAL_SECONDS
    return HEARTBEAT_INTERVAL_SECONDS
    print("[POLICY] Остальные новые H1: Scout -> event-driven FULL.")
    print("[POLICY] Managed position: Claude полностью выключен.")
    print("=" * 80)


def _print_heartbeat(
    daily_state: dict | None,
    market_gate: dict | None,
):
    print()
    print("-" * 80)
    print("RUNNER HEARTBEAT")
    print("-" * 80)
    print(f"FP Time:          {now_fp().isoformat()}")

    if daily_state:
        print(
            f"FP Day:           "
            f"{daily_state.get('fp_day')}"
        )
        print(
            f"Daily trusted:    "
            f"{daily_state.get('trusted')}"
        )
        print(
            f"Daily method:     "
            f"{daily_state.get('capture_method')}"
        )

    if market_gate:
        print(
            f"Market gate:      "
            f"{market_gate.get('allowed')}"
        )
        print(
            f"Window allowed:   "
            f"{market_gate.get('analysis_window_allowed')}"
        )
        print(
            f"Tick fresh:       "
            f"{market_gate.get('tick_fresh')}"
        )
        print(
            f"Latest H1:        "
            f"{market_gate.get('latest_closed_h1_time')}"
        )

    print("-" * 80)


def _run_full_cycle(
    reason: str,
):
    print()
    print("=" * 80)
    print("RUNNER -> MAIN CYCLE")
    print("=" * 80)
    print(f"Reason: {reason}")
    print(f"FP Time: {now_fp().isoformat()}")
    print("=" * 80)

    # MT5 уже подключён runner-ом.
    run_main_cycle(
        manage_connection=False
    )


def _refresh_daily_state() -> dict:
    account = get_account_info()
    positions = get_positions()

    return get_daily_state(
        account,
        positions,
    )


def _print_daily_rollover(
    previous_day: str | None,
    state: dict,
):
    current_day = str(
        state.get(
            "fp_day"
        )
    )

    if previous_day == current_day:
        return

    print()
    print("=" * 80)
    print("FUNDINGPIPS DAY ROLLOVER")
    print("=" * 80)
    print(f"Previous day:      {previous_day}")
    print(f"Current day:       {current_day}")
    print(
        f"Captured at FP:    "
        f"{state.get('captured_at_fp')}"
    )
    print(
        f"Opening Balance:   "
        f"{float(state.get('opening_balance', 0.0)):.2f}"
    )
    print(
        f"Opening Equity:    "
        f"{float(state.get('opening_equity', 0.0)):.2f}"
    )
    print(
        f"Daily baseline:    "
        f"{float(state.get('daily_baseline', 0.0)):.2f}"
    )
    print(
        f"Trusted:           "
        f"{state.get('trusted')}"
    )
    print(
        f"Capture method:    "
        f"{state.get('capture_method')}"
    )
    print("=" * 80)


def _pending_needs_session_cancel(
    plan: dict,
    market_gate: dict,
) -> bool:
    order_type = str(
        plan.get(
            "order_type",
            "",
        )
    )

    if order_type not in PENDING_ORDER_TYPES:
        return False

    # Вне окна НОВЫХ идей pending от старой идеи не переносим дальше.
    return not bool(
        market_gate.get(
            "analysis_window_allowed",
            False,
        )
    )


# ============================================================
# RUNNER
# ============================================================

def run_forever():
    _print_runner_header()

    connected = False
    previous_fp_day = None

    last_position_audit_at = None
    last_entry_retry_at = None
    last_heartbeat_at = None
    last_pending_cancel_attempt_at = None

    last_analysis_attempt_h1 = None
    last_analysis_attempt_at = None

    while True:
        try:
            # =================================================
            # CONNECTION
            # =================================================

            if not connected or not _mt5_connection_alive():
                if connected:
                    try:
                        mt5.shutdown()
                    except Exception:
                        pass

                connected = connect_mt5()

                if not connected:
                    write_runner_status(
                        connected=False,
                        daily_state=None,
                        status="reconnecting",
                        last_error=mt5.last_error(),
                    )

                    print()
                    print(
                        "[RUNNER] MT5 недоступен. "
                        f"Повтор через {RECONNECT_INTERVAL_SECONDS} сек."
                    )
                    time.sleep(
                        RECONNECT_INTERVAL_SECONDS
                    )
                    continue

                print()
                print(
                    "[RUNNER] Постоянное MT5 соединение установлено."
                )

            # =================================================
            # DAILY ROLLOVER — 24/7
            # =================================================

            daily_state = _refresh_daily_state()

            current_day = str(
                daily_state.get(
                    "fp_day"
                )
            )

            _print_daily_rollover(
                previous_fp_day,
                daily_state,
            )

            previous_fp_day = current_day

            write_runner_status(
                connected=True,
                daily_state=daily_state,
                status="running",
            )

            # =================================================
            # POSITION HOLD — 24/7
            # =================================================

            managed_positions = get_managed_positions()

            if managed_positions:
                if (
                    _seconds_since(
                        last_position_audit_at
                    )
                    >= POSITION_AUDIT_INTERVAL_SECONDS
                ):
                    _run_full_cycle(
                        "POSITION_HOLD_AUDIT"
                    )
                    last_position_audit_at = now_fp()

                time.sleep(
                    POLL_INTERVAL_SECONDS
                )
                continue

            # =================================================
            # ACTIVE PLAN / PENDING — 24/7
            # =================================================

            active_plan = get_active_plan()

            if active_plan is not None:
                order_type = str(
                    active_plan.get(
                        "order_type",
                        "",
                    )
                )

                # Pending reconciliation лёгкий и не вызывает Claude.
                if order_type in PENDING_ORDER_TYPES:
                    pending_report = (
                        reconcile_active_pending_execution(
                            symbol=SYMBOL
                        )
                    )

                    if pending_report.get(
                        "blocked",
                        False,
                    ):
                        print()
                        print("=" * 80)
                        print("[FAIL CLOSED] PENDING RECONCILIATION BLOCKED")
                        print("=" * 80)
                        print(
                            f"Decision: "
                            f"{pending_report.get('decision')}"
                        )
                        for error in pending_report.get(
                            "errors",
                            [],
                        ):
                            print(f"- {error}")
                        print("=" * 80)

                        time.sleep(
                            POLL_INTERVAL_SECONDS
                        )
                        continue

                    # Reconciliation мог превратить pending в managed position.
                    if get_managed_positions():
                        _run_full_cycle(
                            "PENDING_FILLED_TO_POSITION"
                        )
                        last_position_audit_at = now_fp()
                        time.sleep(
                            POLL_INTERVAL_SECONDS
                        )
                        continue

                    active_plan = get_active_plan()

                    if active_plan is None:
                        time.sleep(
                            POLL_INTERVAL_SECONDS
                        )
                        continue

                    market_gate = inspect_market_runtime_gate(
                        symbol=SYMBOL
                    )

                    # Никаких pending через окончание рабочего окна / выходные.
                    if _pending_needs_session_cancel(
                        active_plan,
                        market_gate,
                    ):
                        if (
                            _seconds_since(
                                last_pending_cancel_attempt_at
                            )
                            >= PENDING_CANCEL_RETRY_SECONDS
                        ):
                            print()
                            print(
                                "[RUNNER] Рабочее окно новых идей завершено. "
                                "Пытаемся безопасно отменить pending."
                            )

                            cancel_report = execute_pending_cancel(
                                plan=active_plan,
                                reason=(
                                    "Рабочее окно новых торговых идей "
                                    "FundingPips Platform Time завершено. "
                                    "Pending не должен переноситься дальше."
                                ),
                            )

                            print_live_execution_report(
                                cancel_report
                            )

                            last_pending_cancel_attempt_at = now_fp()

                        time.sleep(
                            POLL_INTERVAL_SECONDS
                        )
                        continue

                    source_h1 = str(
                        active_plan.get(
                            "source_h1_closed_bar_time",
                            "",
                        )
                    )

                    current_h1 = str(
                        market_gate.get(
                            "latest_closed_h1_time",
                            "",
                        )
                    )

                    # Новая H1 появилась — существующий pending должен
                    # пройти старый lifecycle раньше любого нового Claude.
                    if (
                        source_h1
                        and current_h1
                        and source_h1 != current_h1
                    ):
                        _run_full_cycle(
                            "NEW_H1_WITH_ACTIVE_PENDING"
                        )

                    time.sleep(
                        POLL_INTERVAL_SECONDS
                    )
                    continue

                # Market-plan / прочий active plan:
                # Executor должен закончить SEND_INTENT/TTL lifecycle.
                if (
                    _seconds_since(
                        last_entry_retry_at
                    )
                    >= ENTRY_RETRY_INTERVAL_SECONDS
                ):
                    _run_full_cycle(
                        "ACTIVE_ENTRY_PLAN"
                    )
                    last_entry_retry_at = now_fp()

                time.sleep(
                    POLL_INTERVAL_SECONDS
                )
                continue

            # =================================================
            # FLAT — NEW H1 TRIGGER
            # =================================================

            market_gate = inspect_market_runtime_gate(
                symbol=SYMBOL
            )

            if not market_gate.get(
                "allowed",
                False,
            ):
                if (
                    _seconds_since(
                        last_heartbeat_at
                    )
                    >= _heartbeat_interval(market_gate)
                ):
                    _print_heartbeat(
                        daily_state,
                        market_gate,
                    )
                    last_heartbeat_at = now_fp()

                time.sleep(
                    POLL_INTERVAL_SECONDS
                )
                continue

            # Daily baseline недоверенный — Claude всё равно нельзя вызывать.
            if not bool(
                daily_state.get(
                    "trusted",
                    False,
                )
            ):
                if (
                    _seconds_since(
                        last_heartbeat_at
                    )
                    >= _heartbeat_interval(market_gate)
                ):
                    _print_heartbeat(
                        daily_state,
                        market_gate,
                    )
                    print(
                        "[RUNNER] Claude OFF: "
                        "FundingPips daily baseline Trusted=False."
                    )
                    last_heartbeat_at = now_fp()

                time.sleep(
                    POLL_INTERVAL_SECONDS
                )
                continue

            # В DEMO_LIVE не запускаем Claude, если прямо сейчас
            # невозможно безопасное реальное исполнение.
            execution_safety = inspect_execution_safety_gate()

            if (
                execution_safety.get(
                    "mode"
                )
                == "DEMO_LIVE"
                and
                not execution_safety.get(
                    "order_send_allowed",
                    False,
                )
            ):
                if (
                    _seconds_since(
                        last_heartbeat_at
                    )
                    >= _heartbeat_interval(market_gate)
                ):
                    _print_heartbeat(
                        daily_state,
                        market_gate,
                    )
                    print(
                        "[RUNNER] Claude OFF: "
                        "Execution Safety Gate не разрешает order_send()."
                    )
                    last_heartbeat_at = now_fp()

                time.sleep(
                    POLL_INTERVAL_SECONDS
                )
                continue

            entry_trigger = inspect_entry_trigger(SYMBOL)
            if entry_trigger.get("invalidated"):
                print("[ENTRY WATCH] Условный план отменён закрытой свечой.")
            if entry_trigger.get("triggered"):
                print()
                print("=" * 80)
                print("ENTRY WATCH -> SHORT ENTRY_CHECK")
                print("=" * 80)
                print(f"Closed bar: {entry_trigger.get('bar')}")
                entry_result = run_entry_check(SYMBOL)
                print(
                    "[ENTRY_CHECK] "
                    + ("завершён." if entry_result.get("ok") else f"fail closed: {entry_result.get('reason')}")
                )
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            latest_h1 = str(
                market_gate.get(
                    "latest_closed_h1_time",
                    "",
                )
            )

            analysis_state = load_analysis_state()
            last_analyzed_h1 = str(
                analysis_state.get(
                    "last_analyzed_h1",
                    "",
                )
            )

            if (
                latest_h1
                and latest_h1 != last_analyzed_h1
            ):
                same_retry = (
                    latest_h1
                    == last_analysis_attempt_h1
                )

                retry_ready = (
                    not same_retry
                    or
                    _seconds_since(
                        last_analysis_attempt_at
                    )
                    >= SAME_H1_ANALYSIS_RETRY_SECONDS
                )

                if retry_ready:
                    last_analysis_attempt_h1 = latest_h1
                    last_analysis_attempt_at = now_fp()

                    _run_full_cycle(
                        "FRESH_NEW_H1"
                    )

            if (
                _seconds_since(
                    last_heartbeat_at
                )
                >= _heartbeat_interval(market_gate)
            ):
                _print_heartbeat(
                    daily_state,
                    market_gate,
                )
                last_heartbeat_at = now_fp()

            time.sleep(
                POLL_INTERVAL_SECONDS
            )

        except KeyboardInterrupt:
            raise

        except Exception as error:
            write_runner_status(
                connected=bool(connected),
                daily_state=None,
                status="error",
                last_error=f"{type(error).__name__}: {error}",
            )

            print()
            print("=" * 80)
            print("[RUNNER ERROR]")
            print("=" * 80)
            print(
                f"{type(error).__name__}: {error}"
            )
            print(
                "[FAIL CLOSED] Новый entry не выполняется в этом цикле."
            )
            print("=" * 80)

            # При ошибках MT5 лучше переподключиться чисто.
            try:
                mt5.shutdown()
            except Exception:
                pass

            connected = False

            time.sleep(
                RECONNECT_INTERVAL_SECONDS
            )

    # unreachable


# ============================================================
# ENTRY POINT
# ============================================================

def main():
    try:
        with SingleInstanceLock(
            LOCK_PATH
        ):
            try:
                run_forever()
            except KeyboardInterrupt:
                print()
                print("[RUNNER] Остановка по Ctrl+C.")

    except SingleInstanceError as error:
        print()
        print("=" * 80)
        print("[RUNNER BLOCKED]")
        print("=" * 80)
        print(str(error))
        print(
            "[SAFE MODE] Вторая копия робота не запущена."
        )
        print("=" * 80)

    finally:
        try:
            disconnect_mt5()
        except Exception:
            pass

        write_runner_status(
            connected=False,
            daily_state=None,
            status="stopped",
        )


if __name__ == "__main__":
    main()
