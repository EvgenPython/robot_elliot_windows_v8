import MetaTrader5 as mt5

from trade_state import (
    get_active_plan,
    get_pending_actions,
    get_managed_positions,
)


# ============================================================
# EXECUTION MODES
# ============================================================

EXECUTION_MODE_DRY_RUN = "DRY_RUN"
EXECUTION_MODE_DEMO_LIVE = "DEMO_LIVE"

ALLOWED_EXECUTION_MODES = {
    EXECUTION_MODE_DRY_RUN,
    EXECUTION_MODE_DEMO_LIVE,
}


# ============================================================
# ГЛАВНЫЙ ПЕРЕКЛЮЧАТЕЛЬ ИСПОЛНЕНИЯ
# ============================================================

# ВАЖНО:
#
# ТЕКУЩИЙ ЭТАП ПРОЕКТА:
# первый реальный order_send() на AMarkets DEMO.
#
# Для реального order_send() должны одновременно
# выполняться ДВА условия:
#
#     EXECUTION_MODE = "DEMO_LIVE"
#     DEMO_LIVE_ARMED = True
#
# Одного изменения EXECUTION_MODE недостаточно.
#
EXECUTION_MODE = EXECUTION_MODE_DEMO_LIVE

DEMO_LIVE_ARMED = True


# ============================================================
# MT5 ACCOUNT TRADE MODE
# ============================================================

def get_account_trade_mode_name(
    trade_mode: int | None,
) -> str:
    """
    Возвращает читаемое имя режима торгового счёта MT5.
    """

    if trade_mode is None:
        return "UNKNOWN"

    mapping = {
        int(
            getattr(
                mt5,
                "ACCOUNT_TRADE_MODE_DEMO",
                0,
            )
        ): "DEMO",
        int(
            getattr(
                mt5,
                "ACCOUNT_TRADE_MODE_CONTEST",
                1,
            )
        ): "CONTEST",
        int(
            getattr(
                mt5,
                "ACCOUNT_TRADE_MODE_REAL",
                2,
            )
        ): "REAL",
    }

    return mapping.get(
        int(trade_mode),
        f"UNKNOWN({trade_mode})",
    )


# ============================================================
# EXECUTION SAFETY GATE
# ============================================================

def inspect_execution_safety_gate() -> dict:
    """
    Проверяет глобальное разрешение на реальный order_send().

    Эта функция НЕ отправляет ордера.

    В режиме DRY_RUN:
        конфигурация считается корректной,
        но order_send запрещён.

    В режиме DEMO_LIVE реальный order_send разрешён только если:

        1. EXECUTION_MODE == DEMO_LIVE;
        2. DEMO_LIVE_ARMED == True;
        3. MT5 account_info доступен;
        4. счёт является DEMO;
        5. terminal_info доступен;
        6. терминал подключён;
        7. terminal.trade_allowed == True;
        8. account.trade_allowed == True, если поле доступно.

    Проверка login/server выполняется отдельно в connect_mt5().
    """

    errors = []
    warnings = []

    mode = str(
        EXECUTION_MODE
    ).strip().upper()

    if mode not in ALLOWED_EXECUTION_MODES:
        errors.append(
            "Неизвестный EXECUTION_MODE: "
            f"{EXECUTION_MODE}."
        )

    account = mt5.account_info()
    terminal = mt5.terminal_info()

    account_trade_mode = None
    account_trade_mode_name = "UNKNOWN"

    account_login = None
    account_server = None
    account_trade_allowed = None

    terminal_connected = None
    terminal_trade_allowed = None

    if account is None:
        errors.append(
            "MT5 account_info() вернул None."
        )
    else:
        account_trade_mode = getattr(
            account,
            "trade_mode",
            None,
        )

        account_trade_mode_name = (
            get_account_trade_mode_name(
                account_trade_mode
            )
        )

        account_login = getattr(
            account,
            "login",
            None,
        )

        account_server = getattr(
            account,
            "server",
            None,
        )

        account_trade_allowed = getattr(
            account,
            "trade_allowed",
            None,
        )

    if terminal is None:
        errors.append(
            "MT5 terminal_info() вернул None."
        )
    else:
        terminal_connected = bool(
            getattr(
                terminal,
                "connected",
                False,
            )
        )

        terminal_trade_allowed = bool(
            getattr(
                terminal,
                "trade_allowed",
                False,
            )
        )

    configuration_valid = (
        len(errors) == 0
        and
        mode in ALLOWED_EXECUTION_MODES
    )

    order_send_allowed = False

    if configuration_valid:

        if mode == EXECUTION_MODE_DRY_RUN:
            warnings.append(
                "Режим DRY_RUN: реальный mt5.order_send() запрещён."
            )

        elif mode == EXECUTION_MODE_DEMO_LIVE:

            if not DEMO_LIVE_ARMED:
                errors.append(
                    "DEMO_LIVE выбран, но DEMO_LIVE_ARMED=False. "
                    "Реальное исполнение заблокировано вторым gate."
                )

            expected_demo_mode = int(
                getattr(
                    mt5,
                    "ACCOUNT_TRADE_MODE_DEMO",
                    0,
                )
            )

            if (
                account_trade_mode is None
                or
                int(account_trade_mode)
                != expected_demo_mode
            ):
                errors.append(
                    "DEMO_LIVE разрешён только на MT5 DEMO account. "
                    f"Текущий режим счёта: {account_trade_mode_name}."
                )

            if terminal_connected is not True:
                errors.append(
                    "MT5 terminal.connected=False."
                )

            # terminal.trade_allowed НЕ является единственной защитой,
            # но при реальном DEMO execution он обязан быть True.
            if terminal_trade_allowed is not True:
                errors.append(
                    "MT5 terminal.trade_allowed=False."
                )

            if (
                account_trade_allowed is not None
                and
                bool(account_trade_allowed) is not True
            ):
                errors.append(
                    "MT5 account.trade_allowed=False."
                )

            if len(errors) == 0:
                order_send_allowed = True

    return {
        "mode": mode,
        "demo_live_armed": bool(
            DEMO_LIVE_ARMED
        ),
        "configuration_valid": (
            len(errors) == 0
            if mode == EXECUTION_MODE_DEMO_LIVE
            else configuration_valid
        ),
        "order_send_allowed": (
            order_send_allowed
        ),
        "account": {
            "login": account_login,
            "server": account_server,
            "trade_mode": account_trade_mode,
            "trade_mode_name": account_trade_mode_name,
            "trade_allowed": account_trade_allowed,
        },
        "terminal": {
            "connected": terminal_connected,
            "trade_allowed": terminal_trade_allowed,
        },
        "errors": errors,
        "warnings": warnings,
    }


# ============================================================
# PRE-CLAUDE GATE
# ============================================================

def inspect_pre_claude_gate(
    symbol: str,
) -> dict:
    """
    Fail-closed gate перед НОВЫМ Claude-анализом.

    Claude нельзя вызывать для новой H1, если существует
    незавершённое торговое состояние, которое сначала нужно
    обработать/сверить.

    Блокирующие состояния:

        - pending_actions в Trade State;
        - managed_positions в Trade State;
        - active_plan в Trade State;
        - реальные MT5 positions по symbol;
        - реальные MT5 orders по symbol;
        - ошибка чтения MT5 positions/orders.

    ВАЖНО:
    gate вызывается только после того, как H1 Analysis Gate
    уже подтвердил NEW_H1. Поэтому active_plan здесь означает
    старое незавершённое состояние, а не план текущего анализа.
    """

    blockers = []
    warnings = []

    pending_actions = (
        get_pending_actions()
    )

    managed_positions = (
        get_managed_positions()
    )

    active_plan = (
        get_active_plan()
    )

    if pending_actions:
        blockers.append(
            "В Trade State есть незавершённые pending_actions."
        )

    if managed_positions:
        blockers.append(
            "В Trade State есть managed_positions. "
            "Перед новым Claude-анализом нужна reconciliation."
        )

    if active_plan is not None:
        blockers.append(
            "В Trade State существует active_plan от предыдущего цикла. "
            "Сначала должен отработать Executor/reconciliation."
        )

    positions = mt5.positions_get(
        symbol=str(symbol)
    )

    if positions is None:
        blockers.append(
            "Не удалось прочитать MT5 positions_get(). "
            f"MT5 error: {mt5.last_error()}"
        )
        positions_count = None
    else:
        positions_count = len(
            positions
        )

        if positions_count > 0:
            blockers.append(
                f"В MT5 уже существует {positions_count} "
                f"позиция(и) по {symbol}."
            )

    orders = mt5.orders_get(
        symbol=str(symbol)
    )

    if orders is None:
        blockers.append(
            "Не удалось прочитать MT5 orders_get(). "
            f"MT5 error: {mt5.last_error()}"
        )
        orders_count = None
    else:
        orders_count = len(
            orders
        )

        if orders_count > 0:
            blockers.append(
                f"В MT5 уже существует {orders_count} "
                f"активный ордер(а) по {symbol}."
            )

    return {
        "allowed": (
            len(blockers) == 0
        ),
        "symbol": str(symbol),
        "blockers": blockers,
        "warnings": warnings,
        "trade_state": {
            "pending_actions_count": len(
                pending_actions
            ),
            "managed_positions_count": len(
                managed_positions
            ),
            "active_plan_present": (
                active_plan is not None
            ),
            "active_plan_id": (
                active_plan.get(
                    "plan_id"
                )
                if active_plan is not None
                else None
            ),
        },
        "mt5": {
            "positions_count": positions_count,
            "orders_count": orders_count,
        },
    }


# ============================================================
# PRINT EXECUTION SAFETY GATE
# ============================================================

def print_execution_safety_gate(
    report: dict,
):
    """
    Печатает глобальный execution gate.
    """

    print()
    print("=" * 80)
    print("EXECUTION SAFETY GATE")
    print("=" * 80)

    print(
        f"Mode:                 "
        f"{report['mode']}"
    )

    print(
        f"DEMO_LIVE armed:      "
        f"{report['demo_live_armed']}"
    )

    print(
        f"Configuration valid:  "
        f"{report['configuration_valid']}"
    )

    print(
        f"order_send allowed:   "
        f"{report['order_send_allowed']}"
    )

    account = report[
        "account"
    ]

    print()
    print("ACCOUNT")
    print("-" * 80)

    print(
        f"Login:                "
        f"{account['login']}"
    )

    print(
        f"Server:               "
        f"{account['server']}"
    )

    print(
        f"Trade mode:           "
        f"{account['trade_mode_name']}"
    )

    print(
        f"Account trade allowed:"
        f" {account['trade_allowed']}"
    )

    terminal = report[
        "terminal"
    ]

    print()
    print("TERMINAL")
    print("-" * 80)

    print(
        f"Connected:            "
        f"{terminal['connected']}"
    )

    print(
        f"Trade allowed:        "
        f"{terminal['trade_allowed']}"
    )

    if report[
        "warnings"
    ]:
        print()
        print("WARNINGS")
        print("-" * 80)

        for warning in report[
            "warnings"
        ]:
            print(
                f"- {warning}"
            )

    if report[
        "errors"
    ]:
        print()
        print("ERRORS")
        print("-" * 80)

        for error in report[
            "errors"
        ]:
            print(
                f"- {error}"
            )

    print("=" * 80)


# ============================================================
# PRINT PRE-CLAUDE GATE
# ============================================================

def print_pre_claude_gate(
    report: dict,
):
    """
    Печатает gate перед новым Claude-анализом.
    """

    print()
    print("=" * 80)
    print("PRE-CLAUDE EXECUTION GATE")
    print("=" * 80)

    print(
        f"Symbol:               "
        f"{report['symbol']}"
    )

    print(
        f"Claude allowed:       "
        f"{report['allowed']}"
    )

    state = report[
        "trade_state"
    ]

    print()
    print("TRADE STATE")
    print("-" * 80)

    print(
        f"Pending actions:      "
        f"{state['pending_actions_count']}"
    )

    print(
        f"Managed positions:    "
        f"{state['managed_positions_count']}"
    )

    print(
        f"Active plan:          "
        f"{state['active_plan_present']}"
    )

    if state[
        "active_plan_id"
    ]:
        print(
            f"Active plan ID:       "
            f"{state['active_plan_id']}"
        )

    mt5_state = report[
        "mt5"
    ]

    print()
    print("MT5")
    print("-" * 80)

    print(
        f"Positions:            "
        f"{mt5_state['positions_count']}"
    )

    print(
        f"Orders:               "
        f"{mt5_state['orders_count']}"
    )

    if report[
        "blockers"
    ]:
        print()
        print("BLOCKERS")
        print("-" * 80)

        for blocker in report[
            "blockers"
        ]:
            print(
                f"- {blocker}"
            )

    if report[
        "allowed"
    ]:
        print()
        print(
            "[OK] Незавершённых торговых состояний нет. "
            "Новый Claude-анализ может быть выполнен."
        )
    else:
        print()
        print(
            "[BLOCKED] Claude НЕ должен вызываться, "
            "пока блокирующее состояние не устранено."
        )

    print("=" * 80)
