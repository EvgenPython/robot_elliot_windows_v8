"""Two-stage FULL analysis for Claude.

The trading strategy is intentionally not implemented here.  This module
only changes how one frozen FULL market snapshot is analysed:

* FULL_MAP independently reconstructs D1/H4/H1 structure and Elliott waves;
* FULL_DECISION receives the validated map plus H1/M15/M5 execution context
  and returns the existing recommendation contract;
* Python deterministically assembles the same final object that Risk Manager
  and Executor already consume.

Each stage has its own durable retry cycle in ``main.py``.  A lost decision
stream therefore never causes the already validated market map to be bought
again.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
from pathlib import Path

import anthropic

from chart_contract import sanitize_visualization
from claude_stream_recovery import consume_structured_stream
from claude_client import (
    CLAUDE_RESPONSE_SCHEMA,
    ClaudeInvalidResponseError,
    ClaudePermanentRequestError,
    ClaudeRequestError,
    ClaudeRequestOutcomeUnknownError,
    ClaudeTransientRequestError,
    _anthropic_error_request_id,
    _anthropic_retry_after_seconds,
    _compact_json,
    _translate_api_status_error,
    build_transport_payload,
    create_anthropic_client,
    extract_text_response,
    get_effective_timeout_seconds,
    get_effort,
    get_model,
    get_usage_stats,
    load_anthropic_config,
    validate_analysis_contract,
    validate_stop_reason,
    validate_trade_levels,
)


BASE_DIR = Path(__file__).resolve().parent
DEBUG_DIR = BASE_DIR / "debug"
DEBUG_STAGE_ATTEMPTS_DIR = DEBUG_DIR / "claude_staged_attempts"

STAGED_ANALYSIS_VERSION = "full_staged_v7_1_local_wire_recovery_bilingual_guard"
# Claude Sonnet 5 counts adaptive thinking and final JSON against the same
# max_tokens ceiling.  Two paid production-like tests proved that MAX effort
# can consume the *entire* ceiling (first 48k, then 128k) without emitting a
# single complete Structured Output.  Anthropic's supported control for that
# failure mode is a lower effort level.  We therefore keep the same model,
# complete raw inputs, prompts, schema and validators, but bound reasoning with
# MEDIUM effort so a completed validated answer has priority over runaway
# hidden thinking.  max_tokens remains a ceiling, not a target.
MAP_EFFORT = "medium"
DECISION_EFFORT = "medium"
REPAIR_EFFORT = "medium"
MAP_MAX_TOKENS = 64_000
DECISION_MAX_TOKENS = 48_000
MAP_REPAIR_MAX_TOKENS = 32_000
DECISION_REPAIR_MAX_TOKENS = 24_000
# Stage 1 has already analysed all 360 H1 bars.  Stage 2 receives the latest
# five trading days of H1 (plus the full existing M15/M5 windows) to verify
# execution timing without buying the same long H1 history twice.
DECISION_H1_CLOSED_BARS = 120

MAP_TIMEFRAMES = ("D1", "H4", "H1")
DECISION_TIMEFRAMES = ("H1", "M15", "M5")

_LAST_STAGE_USAGE: dict[str, dict] = {}
_LAST_STAGE_DIAGNOSTICS: dict[str, dict] = {}
_LAST_WIRE_NORMALIZATION_WARNINGS: list[str] = []


def _schema_properties(*names: str) -> dict:
    source = CLAUDE_RESPONSE_SCHEMA["properties"]
    return {name: copy.deepcopy(source[name]) for name in names}


WAVE_REVISION_SCHEMA = {
    "type": "object",
    "properties": {
        "mode": {
            "type": "string",
            "enum": ["initialize", "unchanged", "extend", "recount"],
        },
        "preserved_anchor_ids": {
            "type": "array",
            "items": {"type": "string"},
        },
        "invalidated_anchor_ids": {
            "type": "array",
            "items": {"type": "string"},
        },
        "reason": {"type": "string"},
    },
    "required": [
        "mode",
        "preserved_anchor_ids",
        "invalidated_anchor_ids",
        "reason",
    ],
    "additionalProperties": False,
}


MARKET_MAP_SCHEMA = {
    "type": "object",
    "properties": {
        **_schema_properties(
            "timestamp",
            "instrument",
            "market_regime",
            "timeframe_analysis",
            "price_structure",
            "patterns",
            "wave_count",
            "higher_timeframe_context",
            "scenario_map",
            "visualization",
            "data_quality",
        ),
        "wave_revision": copy.deepcopy(WAVE_REVISION_SCHEMA),
    },
    "required": [
        "timestamp",
        "instrument",
        "market_regime",
        "timeframe_analysis",
        "price_structure",
        "patterns",
        "wave_count",
        "higher_timeframe_context",
        "scenario_map",
        "visualization",
        "data_quality",
        "wave_revision",
    ],
    "additionalProperties": False,
}


TRADE_DECISION_SCHEMA = {
    "type": "object",
    "properties": {
        **_schema_properties(
            "timestamp",
            "instrument",
            "visualization",
            "recommendation",
            "data_quality",
        ),
        "h1_execution_context": {"type": "string"},
        "microstructure_and_patterns": {"type": "string"},
        "multi_timeframe_relationship": {"type": "string"},
    },
    "required": [
        "timestamp",
        "instrument",
        "h1_execution_context",
        "microstructure_and_patterns",
        "multi_timeframe_relationship",
        "visualization",
        "recommendation",
        "data_quality",
    ],
    "additionalProperties": False,
}


# Anthropic compiles a Structured Output schema into a grammar before model
# generation starts.  The canonical trading/web contracts above intentionally
# remain rich, but their many nested objects can exceed the provider's
# internal compiled-grammar limit.  The API therefore receives this compact
# positional wire contract.  Python expands it back into the canonical
# objects and runs every existing semantic/chart/trading validator before the
# result can become a durable winner.
WIRE_STRING_ROW_SCHEMA = {
    "type": "array",
    "items": {"type": "string"},
}

# data_quality used to share the generic positional-array schema.  That schema
# constrains item types but cannot express our two-field semantic contract in
# the provider grammar used by this project.  Claude could therefore return
# ["true", issue1, issue2, ...], which is valid JSON for the transmitted
# schema but failed the local two-cell expander after the call was billed.
# Keep positional arrays for the large rows, but make this tiny field a strict
# closed object.  The expander below still accepts old saved arrays so already
# paid responses can be recovered locally without another API request.
DATA_QUALITY_WIRE_SCHEMA = {
    "type": "object",
    "properties": {
        "sufficient": {"type": "string"},
        "issues": {"type": "string"},
    },
    "required": ["sufficient", "issues"],
    "additionalProperties": False,
}

MARKET_REGIME_COLUMNS = (
    "primary_regime",
    "direction",
    "current_phase",
    "phase_status",
    "maturity",
    "location",
    "summary",
)
TIMEFRAME_ANALYSIS_COLUMNS = ("D1", "H4", "H1", "relationship", "summary")
PRICE_STRUCTURE_COLUMNS = (
    "structure_state",
    "swing_structure",
    "key_levels",
    "liquidity_context",
    "summary",
)
WAVE_COUNT_COLUMNS = (
    "structure_type",
    "direction",
    "current_label",
    "current_phase",
    "invalidation_level",
    "alternate_count",
    "summary",
)
HIGHER_TIMEFRAME_CONTEXT_COLUMNS = (
    "d1_trend",
    "d1_wave_context",
    "h4_trend",
    "h4_wave_context",
    "alignment",
    "summary",
)
SCENARIO_MAP_COLUMNS = (
    "primary_scenario",
    "alternate_scenario",
    "expected_path",
    "current_opportunity",
    "next_opportunity",
    "regime_change_trigger",
)
DATA_QUALITY_COLUMNS = ("sufficient", "issues")
RECOMMENDATION_COLUMNS = (
    "action",
    "setup_type",
    "trade_horizon",
    "setup_quality",
    "entry_quality",
    "order_type",
    "entry_price",
    "stop_loss",
    "take_profit",
    "invalidation_level",
    "confidence",
    "why_now",
    "structural_stop_basis",
    "target_basis",
    "reasoning",
    "invalidation_reason",
    "fvg_role",
    "fvg_ids",
    "fvg_basis",
)
WAVE_POINT_COLUMNS = (
    "scenario",
    "degree",
    "timeframe",
    "sequence",
    "label",
    "time",
    "price",
    "status",
    "structure_id",
    "parent_structure_id",
    "parent_wave_id",
    "wave_type",
)
LEVEL_COLUMNS = ("kind", "scenario", "timeframe", "price", "label", "basis")
ZONE_COLUMNS = (
    "kind",
    "scenario",
    "timeframe",
    "start_time",
    "end_time",
    "price_low",
    "price_high",
    "label",
)
SCENARIO_PATH_COLUMNS = (
    "scenario",
    "timeframe",
    "anchor_time",
    "anchor_price",
    "direction",
    "target_price_low",
    "target_price_high",
    "label",
)
TRENDLINE_COLUMNS = (
    "line_id", "kind", "scenario", "timeframe", "start_time",
    "start_price", "end_time", "end_price", "status", "label", "basis",
)
CHANNEL_COLUMNS = (
    "channel_id", "kind", "scenario", "timeframe",
    "upper_start_time", "upper_start_price", "upper_end_time", "upper_end_price",
    "lower_start_time", "lower_start_price", "lower_end_time", "lower_end_price",
    "status", "breakout_time", "breakout_price", "reentry_time", "reentry_price",
    "label", "basis",
)
PATTERN_SHAPE_COLUMNS = (
    "pattern_id", "kind", "scenario", "timeframe", "start_time", "end_time",
    "price_low", "price_high", "status", "confirmation_level",
    "invalidation_level", "target_price", "label", "basis",
)
MARKET_EVENT_COLUMNS = (
    "event_id", "kind", "scenario", "timeframe", "time", "price",
    "status", "label", "basis",
)
PROJECTED_WAVE_COLUMNS = (
    "projection_id", "structure_id", "parent_structure_id", "parent_wave_id",
    "scenario", "degree", "timeframe", "label", "wave_type", "direction",
    "anchor_time", "anchor_price", "target_price_low", "target_price_high",
    "confirmation_level", "invalidation_level", "status", "basis",
)
WAVE_STRUCTURE_COLUMNS = (
    "structure_id", "parent_structure_id", "parent_wave_id", "scenario",
    "degree", "timeframe", "label", "wave_type", "direction", "status",
    "current_phase", "confirmation_level", "invalidation_level", "summary",
)

COMPACT_VISUALIZATION_WIRE_SCHEMA = {
    "type": "object",
    "properties": {
        "wave_points": {
            "type": "array",
            "items": copy.deepcopy(WIRE_STRING_ROW_SCHEMA),
        },
        "levels": {
            "type": "array",
            "items": copy.deepcopy(WIRE_STRING_ROW_SCHEMA),
        },
        "zones": {
            "type": "array",
            "items": copy.deepcopy(WIRE_STRING_ROW_SCHEMA),
        },
        "scenario_paths": {
            "type": "array",
            "items": copy.deepcopy(WIRE_STRING_ROW_SCHEMA),
        },
        "trendlines": {"type": "array", "items": copy.deepcopy(WIRE_STRING_ROW_SCHEMA)},
        "channels": {"type": "array", "items": copy.deepcopy(WIRE_STRING_ROW_SCHEMA)},
        "pattern_shapes": {"type": "array", "items": copy.deepcopy(WIRE_STRING_ROW_SCHEMA)},
        "market_events": {"type": "array", "items": copy.deepcopy(WIRE_STRING_ROW_SCHEMA)},
        "projected_waves": {"type": "array", "items": copy.deepcopy(WIRE_STRING_ROW_SCHEMA)},
        "wave_structures": {"type": "array", "items": copy.deepcopy(WIRE_STRING_ROW_SCHEMA)},
        "chart_comment": {"type": "string"},
    },
    "required": [
        "wave_points",
        "levels",
        "zones",
        "scenario_paths",
        "trendlines",
        "channels",
        "pattern_shapes",
        "market_events",
        "projected_waves",
        "wave_structures",
        "chart_comment",
    ],
    "additionalProperties": False,
}


def _wire_row_schema() -> dict:
    return copy.deepcopy(WIRE_STRING_ROW_SCHEMA)


MARKET_MAP_WIRE_SCHEMA = {
    "type": "object",
    "properties": {
        "timestamp": {"type": "string"},
        "instrument": {"type": "string"},
        "market_regime": _wire_row_schema(),
        "timeframe_analysis": _wire_row_schema(),
        "price_structure": _wire_row_schema(),
        "patterns": {"type": "string"},
        "wave_count": _wire_row_schema(),
        "higher_timeframe_context": _wire_row_schema(),
        "scenario_map": _wire_row_schema(),
        "visualization": copy.deepcopy(COMPACT_VISUALIZATION_WIRE_SCHEMA),
        "data_quality": copy.deepcopy(DATA_QUALITY_WIRE_SCHEMA),
        "wave_revision": copy.deepcopy(WAVE_REVISION_SCHEMA),
    },
    "required": list(MARKET_MAP_SCHEMA["required"]),
    "additionalProperties": False,
}


TRADE_DECISION_WIRE_SCHEMA = {
    "type": "object",
    "properties": {
        "timestamp": {"type": "string"},
        "instrument": {"type": "string"},
        "h1_execution_context": {"type": "string"},
        "microstructure_and_patterns": {"type": "string"},
        "multi_timeframe_relationship": {"type": "string"},
        "visualization": copy.deepcopy(COMPACT_VISUALIZATION_WIRE_SCHEMA),
        "recommendation": _wire_row_schema(),
        "data_quality": copy.deepcopy(DATA_QUALITY_WIRE_SCHEMA),
    },
    "required": list(TRADE_DECISION_SCHEMA["required"]),
    "additionalProperties": False,
}


VISUALIZATION_WIRE_INSTRUCTIONS = """
ТЕХНИЧЕСКИЙ COMPACT WIRE-ФОРМАТ
Schema использует позиционные массивы строк только для надёжной передачи.
Это НЕ сокращение анализа. Не объединяй, не пропускай и не меняй порядок
значений. Числа пиши десятичной строкой без единиц и разделителя тысяч;
отсутствующее nullable число — пустая строка "".

visualization.wave_points row:
[scenario,degree,timeframe,sequence,label,time,price,status,structure_id,parent_structure_id,parent_wave_id,wave_type]
visualization.levels row:
[kind,scenario,timeframe,price,label,basis]
visualization.zones row:
[kind,scenario,timeframe,start_time,end_time,price_low,price_high,label]
visualization.scenario_paths row:
[scenario,timeframe,anchor_time,anchor_price,direction,target_price_low,target_price_high,label]
visualization.trendlines row:
[line_id,kind,scenario,timeframe,start_time,start_price,end_time,end_price,status,label,basis]
visualization.channels row:
[channel_id,kind,scenario,timeframe,upper_start_time,upper_start_price,upper_end_time,upper_end_price,lower_start_time,lower_start_price,lower_end_time,lower_end_price,status,breakout_time,breakout_price,reentry_time,reentry_price,label,basis]
visualization.pattern_shapes row:
[pattern_id,kind,scenario,timeframe,start_time,end_time,price_low,price_high,status,confirmation_level,invalidation_level,target_price,label,basis]
visualization.market_events row:
[event_id,kind,scenario,timeframe,time,price,status,label,basis]
visualization.projected_waves row:
[projection_id,structure_id,parent_structure_id,parent_wave_id,scenario,degree,timeframe,label,wave_type,direction,anchor_time,anchor_price,target_price_low,target_price_high,confirmation_level,invalidation_level,status,basis]
visualization.wave_structures row:
[structure_id,parent_structure_id,parent_wave_id,scenario,degree,timeframe,label,wave_type,direction,status,current_phase,confirmation_level,invalidation_level,summary]
Все ячейки этих строк являются JSON strings. Python восстановит именованные
объекты и числовые типы и затем выполнит прежнюю строгую проверку координат.
""".strip()


MARKET_MAP_WIRE_INSTRUCTIONS = f"""
{VISUALIZATION_WIRE_INSTRUCTIONS}

Остальные позиционные строки MARKET MAP:
market_regime: [{','.join(MARKET_REGIME_COLUMNS)}]
timeframe_analysis: [{','.join(TIMEFRAME_ANALYSIS_COLUMNS)}]
price_structure: [{','.join(PRICE_STRUCTURE_COLUMNS)}]
wave_count: [{','.join(WAVE_COUNT_COLUMNS)}]
higher_timeframe_context: [{','.join(HIGHER_TIMEFRAME_CONTEXT_COLUMNS)}]
scenario_map: [{','.join(SCENARIO_MAP_COLUMNS)}]
data_quality: {{"sufficient":"true","issues":"единая строка"}}.
Если проблем несколько, объедини их внутри ОДНОЙ строки issues через "; ".
wave_count.invalidation_level — десятичная строка или "" для null.
timestamp, instrument, patterns и wave_revision остаются обычными полями.

ОБЯЗАТЕЛЬНЫЙ ЯЗЫКОВОЙ КОНТРАКТ. Каждое объяснение должно содержать обе
полноценные версии `EN: ...\nRU: ...`, включая summary, D1/H4/H1/relationship,
swing_structure, key_levels, liquidity_context, patterns, alternate_count,
все 6 scenario_map, wave_revision.reason, chart_comment и basis/summary всех
графических объектов. Нельзя оставлять русскую часть пустой и нельзя копировать
английский текст после RU. Enum, ID, timestamp, price и короткие wave labels
не переводятся.
""".strip()


TRADE_DECISION_WIRE_INSTRUCTIONS = f"""
{VISUALIZATION_WIRE_INSTRUCTIONS}

recommendation — одна позиционная строка:
[{','.join(RECOMMENDATION_COLUMNS)}]
entry_price, stop_loss, take_profit и invalidation_level — десятичные строки
или "" для null. data_quality:
{{"sufficient":"true","issues":"единая строка"}}. Если проблем несколько,
объедини их внутри ОДНОЙ строки issues через "; ". Остальные top-level поля
остаются обычными строками.

ОБЯЗАТЕЛЬНЫЙ ЯЗЫКОВОЙ КОНТРАКТ. Полные `EN: ...\nRU: ...` обязательны в
h1_execution_context, microstructure_and_patterns,
multi_timeframe_relationship, why_now, structural_stop_basis, target_basis,
reasoning, invalidation_reason, chart_comment и basis/summary всех графических
объектов. Русская часть должна быть настоящим переводом той же мысли, а не
пустой строкой и не копией английской. Enum, ID, timestamp, price и wave labels
не переводятся.
""".strip()


MARKET_MAP_SYSTEM_PROMPT = """
Ты — первый этап профессионального анализа XAUUSD: MARKET MAP.
Ты НЕ принимаешь торговое решение и не предлагаешь Entry/SL/TP. Твоя задача
— независимо восстановить по полным свежим raw MT5 данным D1/H4/H1 текущий
режим, price action, структуру, ликвидность, паттерны, волны Эллиотта,
основной/альтернативный сценарии и точные координаты структурной карты.

КАЧЕСТВО И ИСТОЧНИК ИСТИНЫ
1. Raw candles текущего frozen snapshot — единственный источник истины.
2. Таблицы bars переданы как columns + rows: порядок значений каждой строки
   точно соответствует columns; ни одна свеча и ни одно число не удалены.
3. Сначала полностью и независимо проанализируй D1 -> H4 -> H1. Только после
   этого сравни результат с previous_confirmed_wave_anchors.
4. Предыдущий анализ не авторитетен и не заменяет свежий анализ. Его можно
   сохранить только там, где текущие raw данные независимо подтверждают его.
5. current_unclosed_bar — незакрытая свеча: используй как текущий контекст,
   но не объявляй её неподтверждённым закрытым сигналом.
6. Tick volume и spread — данные брокерского feed, не централизованный поток.
   Не выдумывай индикаторы, новости, календарь или order flow, которых нет.
7. deterministic_market_facts Python приоритетны. Для каждого уровня используй
   отдельные relation/cross_event/touch; общий статус уровней через `/` запрещён.
8. Объём оценивай только по готовым ratio_to_median/classification; не сравнивай
   незакрытый бар с закрытыми и не придумывай норму.
9. imbalances содержит трёхсвечные FVG, рассчитанные
   только по закрытым свечам. Оцени их роль в контексте Elliott, Fibonacci,
   структуры, ликвидности и паттернов. FVG сам по себе не является входом.

ПРОФЕССИОНАЛЬНЫЙ АНАЛИЗ
- Определи primary_regime только из: trend, correction, range, breakout,
  reversal, transition, unclear; direction только bullish, bearish, neutral,
  mixed, unclear.
- phase_status только developing, mature, completing, completed,
  transitioning, failed, unclear. Не смешивай долгосрочный режим и текущую
  фазу: тренд может находиться в коррекции, range может готовить breakout.
- Разбери swings HH/HL/LH/LL, BOS/CHOCH, импульс/коррекцию, поддержки,
  сопротивления, зоны реакции, liquidity sweep/false breakout, зрелость и
  положение цены. Учитывай compression/expansion, relative tick-volume,
  spread, obvious equal highs/lows и локальные/значимые swing extrema.
- Ищи только реально читаемые модели: flag/pennant/channel/triangle,
  breakout+retest, double top/bottom, head-and-shoulders, wedge/diagonal,
  failed breakout, rectangle/range, three-leg или complex correction.
  Паттерн без геометрии и контекста не существует.
- Elliott — важная, но не единственная часть анализа. Дай primary и alternate
  count, текущую волну/фазу и объективную инвалидацию. Не подгоняй count под
  желаемую сделку. Проверяй правила impulse/diagonal, zigzag/flat/triangle,
  W-X-Y/combination, alternation и незавершённость последней волны. Fibonacci
  — только подтверждение уже читаемой структуры, не способ придумать count.
- Построй непротиворечивую связь D1/H4/H1 и primary/alternate scenario map.
  Отличай реальный конфликт TF от нормальной вложенной коррекции.
- Выполни явный checklist на каждом D1/H4/H1: channel, trendline, range,
  triangle, wedge/diagonal, flag/pennant, double top/bottom,
  head-and-shoulders, breakout+retest, false breakout+reentry и liquidity
  sweep. Не заставляй паттерн существовать, но в patterns укажи, что
  проверено, что подтверждено и что отвергнуто.
- Выполни явный imbalance/FVG checklist на D1/H4/H1: какие открыты, какие
  заполнены, находится ли цена внутри/рядом, есть ли confluence с окончанием
  волны, Fibonacci, ретестом или liquidity sweep.
- Для подтверждённого канала проверь опорные касания обеих границ, параллель,
  breakout, false breakout и возврат внутрь канала.
- Построй иерархию wave_structures сверху вниз. Дочерний structure_id должен
  ссылаться на parent_structure_id и parent_wave_id. Объясни, какие младшие
  1-5 или A-B-C формируют текущую старшую волну.

ПЕРЕНОС ПОДТВЕРЖДЁННЫХ ВОЛН
Каждый previous anchor_id классифицируй ровно один раз: preserved или
invalidated. Preserved не дублируй в wave_points — Python перенесёт его.
Invalidated объясни и при необходимости верни новую точку. mode:
initialize=нет истории, unchanged=без изменений, extend=новые точки,
recount=объективная переразметка. Ошибочную карту не сохраняй.

ВИЗУАЛИЗАЦИЯ
Верни только объекты, вытекающие из анализа. initialize/recount требуют полной
актуальной разметки; unchanged/extend — новых/изменённых точек. Wave point:
реальный timestamp и точный OHLC. Entry/SL/TP здесь запрещены.

WAVE/FIB: label только I..V/A..C для primary, (I)..(V)/(A)..(C) для
intermediate, i..v/a..c для minor/micro; без слов Wave/Sub-wave. Fibonacci —
только подтверждение последней значимой ноги. В levels верни лишь использованные
fib_retracement/fib_extension: label=ratio, bilingual basis=точные anchors и
связь с target/invalidation. Декоративная полная сетка запрещена.

ГРАФИЧЕСКИЙ КОНТРАКТ:
- wave_structures: иерархия D1/H4/H1, короткие стабильные IDs;
- wave_points: родительские IDs, wave_type impulse/correction/diagonal;
- projected_waves: только обоснованная C/5, target/confirmation/invalidation;
- линии, каналы, фигуры и события используют точные свечи/цены;
- события: breakout/retest/false_breakout/reentry/sweep/BOS/CHOCH;
- значимые открытые FVG верни как zones с отдельным kind и отличимым label;
- basis кратко объясняет практический смысл.

Все narrative-поля верни двуязычно в одной строке строго как
EN: <English>\nRU: <Русский>. Числа, enum, time, IDs и wave labels не переводи.
Обе части описывают одну оценку; второго анализа для перевода нет.
Русская часть должна быть естественной профессиональной речью. Запрещены
непереведённые служебные слова stay_out, reference, structure_state,
relationship, raw tape, swing high/low и буквальные машинные enum. Используй:
«вне рынка», «предыдущая карта», «состояние структуры», «связь таймфреймов»,
«сырые рыночные данные», «максимум/минимум колебания».

Ответ — только Structured JSON по заданной schema. Поля должны быть содержательны,
но без повторения одного и того же объяснения в нескольких разделах.
""".strip()


TRADE_DECISION_SYSTEM_PROMPT = """
Ты — второй этап профессионального анализа XAUUSD: TRADE DECISION.
Ты получаешь (1) validated_market_map, построенную первым этапом по полным
D1/H4/H1 raw данным того же frozen snapshot, и (2) raw H1/M15/M5 данные для
точного исполнения. Твоя задача — проверить точку входа на младших TF и выдать
тот же строгий торговый контракт, который использует действующая стратегия.

deterministic_market_facts имеют приоритет для статусов уровней, относительного
тикового объёма и FVG. На H1/M15/M5 обязательно оцени реакцию на открытые и
частично заполненные имбалансы. FVG участвует в решении как confluence,
entry zone, target, invalidation или conflict, но не создаёт сделку в одиночку.
Каждый confirmation/invalidation
уровень описывай отдельно: касание, закрытие, закрепление и ретест — разные
события.

КОНТЕКСТ И КАЧЕСТВО
1. validated_market_map — обязательный старший контекст этого же snapshot.
   Не начинай независимую альтернативную стратегию и не переписывай карту без
   данных. M15/M5 могут подтвердить вход, ухудшить entry_quality или привести
   к stay_out, но не должны искусственно менять D1/H4 структуру.
2. Raw таблицы bars имеют формат columns + rows. Все числа сохранены точно.
3. current_unclosed_bar не является закрытым подтверждением. Не выдумывай
   отсутствующие индикаторы, новости, календарь или централизованный order flow.
4. Стабильность не означает обязательную сделку: качественный stay_out лучше
   слабого, позднего или плохо защищённого входа.

ОЦЕНКА SETUP
- Сопоставь D1/H4/H1 map с H1/M15/M5: импульс/коррекция, swings, слом/защита
  структуры, pattern, liquidity/false breakout, текущая цена и spread.
- Разложи текущую родительскую H1-волну на дочернюю структуру M15, а M15 — на
  M5 только там, где pivots объективно читаются. Не начинай независимый count
  младшего TF без parent_structure_id и parent_wave_id.
- Допустимые setup_type: trend_pullback, wave3_continuation,
  wave5_continuation, correction_a_leg, correction_b_leg, correction_c_leg,
  correction_completion, range_long, range_short, range_breakout,
  breakout_retest, false_breakout_reversal, trend_reversal,
  diagonal_reversal, pattern_continuation, pattern_reversal,
  transition_trade, other, no_trade.
- trade_horizon: intraday, swing, multi_day, unclear.
- setup_quality: weak, acceptable, good, excellent. entry_quality: poor, fair,
  good, excellent. confidence: low, medium, high.
- Не запрещай торговлю коррекции или range автоматически. Различай активную
  фазу и её завершение. correction_a/b/c_leg нельзя торговать, если map
  phase_status completed/failed; correction_completion допустим только при
  completing/completed/transitioning.
- H1-сделка против D1/H4 допустима только как ясно читаемая коррекционная или
  разворотная фаза с хорошей location, близкой объективной инвалидацией и
  реалистичной целью. В range ищи границы, false breakout или подтверждённый
  breakout/retest; середина range без отдельного сильного edge => stay_out.
- Оцени current setup отдельно от next opportunity. Не входи поздно только
  потому, что направление верно. weak setup или poor entry => stay_out.
- setup_quality оценивает весь edge: режим/фазу, maturity, structure/pattern,
  TF alignment, location, stop geometry, достижимый target, фактический
  reward/risk, volatility/spread, alternate scenario и data quality.
- recommendation.fvg_role обязателен: confirmation, entry_zone, target,
  invalidation, conflict, neutral или no_relevant_fvg. В fvg_ids перечисли
  точные deterministic IDs через запятую, а в bilingual fvg_basis объясни,
  как FVG повлиял на вход/отказ. Если FVG конфликтует с направлением или делает
  вход поздним, action должен быть stay_out. Не выдумывай FVG сверх Python facts.

ACTION И УРОВНИ
- action только enter_long, enter_short или stay_out.
- Для stay_out обязательно: setup_type=no_trade, order_type=none,
  entry_price=null, stop_loss=null, take_profit=null,
  invalidation_level=null. Чётко объясни, чего не хватает и что ждать.
- Для входа order_type только market, limit или stop; все Entry/SL/TP и
  invalidation_level обязательны. LONG: SL < Entry < TP. SHORT: TP < Entry < SL.
- market — вход около текущего Bid/Ask; limit — более выгодный откат/ретест;
  stop — вход только после пробоя/подтверждения. Не путай их геометрию.
- Stop Loss ставь за объективной структурной инвалидацией, а не по удобному
  расстоянию и не внутри структуры ради меньшего риска. Учитывай tick size,
  broker stop constraints и spread. Take Profit — у достижимой цели именно
  торгуемой фазы: structure/liquidity/range boundary/wave projection.
- recommendation.invalidation_level — уровень, после которого торговая идея
  неверна; он может совпадать со SL, но не выбирается произвольно.
- Не рассчитывай lot size, FundingPips лимиты или денежный риск: после ответа
  неизменённый Python Risk Manager решит, разрешена ли сделка.

ВИЗУАЛИЗАЦИЯ
Верни только новые execution/micro wave_points и актуальные M15/M5 levels,
zones, paths, trendlines, channels, pattern_shapes, market_events,
projected_waves и wave_structures. Не копируй preserved старшие точки из map: Python объединит их
детерминированно. Wave point обязан ссылаться на существующую raw свечу и иметь
цену ровно одного из её OHLC. Не придумывай координаты. Торговые уровни должны
в точности совпадать с recommendation; Python дополнительно канонизирует их.
Для каждого M15 и M5: если существует объективно читаемая execution/micro
волновая структура, верни не одиночный pivot, а минимум две связанные точки
одного degree/scenario с последовательными sequence. Если честного count нет,
верни для этого timeframe ноль wave_points и явно объясни это в chart_comment;
никогда не создавай недостающие точки ради заполнения графика. Даже без
волнового count верни подтверждённые структурные levels/zones/paths, если они
реально следуют из raw данных.
Использованный для Entry/SL/TP Fib верни в levels как fib_retracement или
fib_extension: label=ratio, bilingual basis=anchors и связь с уровнем; без сетки.

Все narrative-поля верни двуязычно в одной строке строго как
EN: <English>\nRU: <Русский>. Числа, enum, time, IDs и wave labels не переводи.
Обе части передают одно решение; перевод не является вторым анализом.

Ответ — только Structured JSON по schema. reasoning должен быть глубоким и
связным, но без дублирования уже принятой validated_market_map.
""".strip()


MARKET_MAP_REPAIR_SYSTEM_PROMPT = """
Ты — строго ограниченный REPAIR-этап MARKET MAP для XAUUSD.

Первичный анализ уже выполнен и оплачен. Не выполняй новый независимый
анализ и не меняй торговую стратегию. Исправь только перечисленную локальную
ошибку контракта/семантической проверки в supplied_invalid_result.

Обязательные правила:
- сохрани рыночную оценку, режим, направление, wave count и сценарии первичного
  результата, если конкретная validation_error не требует их исправления;
- instrument и timestamp возьми из immutable_facts;
- каждый previous anchor классифицируй ровно один раз как preserved или
  invalidated; неизвестные anchor_id запрещены;
- не добавляй Entry/SL/TP и не создавай новую торговую рекомендацию;
- верни полный MARKET_MAP строго по приложенной COMPACT WIRE schema;
- если исправление требует выбора, используй наиболее консервативный вариант,
  который не выдумывает отсутствующие рыночные данные.

Сохрани bilingual narrative: EN: <English>\nRU: <Русский>.
Ответ — только Structured JSON.
""".strip()


TRADE_DECISION_REPAIR_SYSTEM_PROMPT = """
Ты — строго ограниченный REPAIR-этап TRADE DECISION для XAUUSD.

Первичный анализ уже выполнен и оплачен. Сохрани validated_market_map и
логику действующей стратегии. Исправь только validation_error первичного
  trade decision, используя supplied_invalid_result и компактные неизменяемые
  факты последних закрытых свечей того же frozen snapshot. Не выполняй новый
  полный анализ истории.

Обязательные правила:
- не меняй направление/идею без необходимости, прямо вызванной ошибкой;
- не придумывай сделку ради заполнения полей: если безопасно исправить Entry,
  SL, TP или контракт нельзя, верни полноценный stay_out по исходной schema;
- для входа сохрани строгую геометрию LONG/SHORT и структурное основание SL/TP;
- current_unclosed_bar не является закрытым подтверждением;
- верни полный TRADE_DECISION строго по приложенной COMPACT WIRE schema.

Сохрани bilingual narrative: EN: <English>\nRU: <Русский>.
Ответ — только Structured JSON.
""".strip()


ENTRY_CHECK_SYSTEM_PROMPT = """
Ты — короткий финальный ENTRY CHECK для XAUUSD. Полный D1/H4/H1 анализ и
условный план уже оплачены и переданы в validated_market_map. Не перестраивай
старшую карту и не запускай новый широкий анализ. По свежим закрытым H1/M15/M5
проверь только фактическое срабатывание ранее заданного триггера, качество
текущего входа, сохранность структурной инвалидации и достижимость цели.

Разреши enter_long/enter_short только если триггер подтверждён закрытой
свечой, план не устарел, цена не ушла слишком далеко, stop остаётся за
структурой, а reward/risk не ухудшился. Иначе верни stay_out и чётко объясни
причину. Не создавай новую идею, направление или старшую волновую карту.
Верни свежие M15/M5 events и точки только если они объективно подтверждаются
raw OHLC. Все narrative-поля строго EN: <English>\nRU: <Русский>.
Ответ — только Structured JSON по заданной schema.
""".strip()

ENTRY_CHECK_MAX_TOKENS = 24_000
ENTRY_CHECK_EFFORT = "medium"


def _filtered_raw_payload(
    payload: dict,
    timeframes: tuple[str, ...],
    h1_closed_limit: int | None = None,
) -> dict:
    """Returns a raw-data copy for one stage without mutating the archive."""
    selected = copy.deepcopy(payload)

    history = selected.get("cacheable_history")
    if not isinstance(history, dict):
        history = {}
        selected["cacheable_history"] = history
    history_by_tf = history.get("closed_market_history_before_day_start")
    if not isinstance(history_by_tf, dict):
        history_by_tf = {}

    live = selected.get("live_market")
    if not isinstance(live, dict):
        live = {}
        selected["live_market"] = live
    live_by_tf = live.get("raw_timeframes_since_day_start")
    if not isinstance(live_by_tf, dict):
        live_by_tf = {}

    allowed = set(timeframes)
    history_by_tf = {
        str(name): value
        for name, value in history_by_tf.items()
        if name in allowed and isinstance(value, dict)
    }
    live_by_tf = {
        str(name): value
        for name, value in live_by_tf.items()
        if name in allowed and isinstance(value, dict)
    }

    if h1_closed_limit is not None and "H1" in allowed:
        limit = max(1, int(h1_closed_limit))
        live_h1 = live_by_tf.get("H1")
        if isinstance(live_h1, dict):
            live_bars = live_h1.get("closed_bars_since_day_start")
            if not isinstance(live_bars, list):
                live_bars = []
            live_bars = live_bars[-limit:]
            live_h1["closed_bars_since_day_start"] = live_bars
            live_h1["closed_bars_since_day_start_count"] = len(live_bars)
        else:
            live_bars = []

        remaining = max(0, limit - len(live_bars))
        history_h1 = history_by_tf.get("H1")
        if isinstance(history_h1, dict):
            history_bars = history_h1.get("closed_bars")
            if not isinstance(history_bars, list):
                history_bars = []
            history_bars = history_bars[-remaining:] if remaining else []
            history_h1["closed_bars"] = history_bars
            history_h1["closed_bars_count"] = len(history_bars)

    history["closed_market_history_before_day_start"] = history_by_tf
    live["raw_timeframes_since_day_start"] = live_by_tf
    return selected


def _anchor_id(point: dict) -> str:
    identity = [
        str(point.get("scenario", "")),
        str(point.get("degree", "")),
        str(point.get("timeframe", "")),
        int(point.get("sequence", 0) or 0),
        str(point.get("label", "")),
        str(point.get("time", "")),
        point.get("price"),
    ]
    digest = hashlib.sha256(_compact_json(identity).encode("utf-8")).hexdigest()
    return f"wave_{digest[:20]}"


def build_previous_confirmed_anchor_reference(
    previous_reference: dict | None,
) -> dict:
    """Builds compact stable IDs from the last successful FULL chart."""
    result = {
        "reference_available": False,
        "saved_at_fp": None,
        "market_snapshot_time_fp": None,
        "h1_closed_bar_time_fp": None,
        "previous_map_summary": None,
        "anchors": [],
    }
    if not isinstance(previous_reference, dict):
        return result

    analysis = previous_reference.get("analysis")
    if not isinstance(analysis, dict):
        return result

    visualization = analysis.get("visualization")
    if not isinstance(visualization, dict):
        visualization = {}

    anchors = []
    seen = set()
    for point in visualization.get("wave_points", []):
        if not isinstance(point, dict):
            continue
        if str(point.get("status", "")).strip().lower() != "confirmed":
            continue
        identifier = _anchor_id(point)
        if identifier in seen:
            continue
        seen.add(identifier)
        anchors.append({"anchor_id": identifier, "point": copy.deepcopy(point)})

    result.update(
        {
            "reference_available": True,
            "saved_at_fp": previous_reference.get("saved_at_fp"),
            "market_snapshot_time_fp": previous_reference.get(
                "market_snapshot_time_fp"
            ),
            "h1_closed_bar_time_fp": previous_reference.get(
                "h1_closed_bar_time_fp"
            ),
            "previous_map_summary": {
                name: copy.deepcopy(analysis.get(name))
                for name in (
                    "market_regime",
                    "timeframe_analysis",
                    "price_structure",
                    "patterns",
                    "wave_count",
                    "higher_timeframe_context",
                    "scenario_map",
                )
            },
            "anchors": anchors,
        }
    )
    return result


def build_market_map_stage_payload(
    payload: dict,
    previous_reference: dict | None,
) -> dict:
    raw = _filtered_raw_payload(payload, MAP_TIMEFRAMES)
    return {
        "stage": "FULL_MAP",
        "staged_analysis_version": STAGED_ANALYSIS_VERSION,
        "frozen_snapshot_timestamp": payload.get("timestamp"),
        "raw_market_d1_h4_h1": build_transport_payload(raw),
        "deterministic_market_facts": copy.deepcopy(
            payload.get("deterministic_market_facts", {})
        ),
        "previous_confirmed_wave_anchors": (
            build_previous_confirmed_anchor_reference(previous_reference)
        ),
    }


def build_trade_decision_stage_payload(
    payload: dict,
    market_map: dict,
    previous_reference: dict | None = None,
) -> dict:
    raw = _filtered_raw_payload(
        payload,
        DECISION_TIMEFRAMES,
        h1_closed_limit=DECISION_H1_CLOSED_BARS,
    )
    complete_map = copy.deepcopy(market_map)
    previous_anchors = build_previous_confirmed_anchor_reference(
        previous_reference
    )
    previous_by_id = {
        str(item["anchor_id"]): copy.deepcopy(item["point"])
        for item in previous_anchors.get("anchors", [])
        if isinstance(item, dict) and item.get("anchor_id")
    }
    revision = complete_map.get("wave_revision") or {}
    preserved = set(revision.get("preserved_anchor_ids") or [])
    invalidated = set(revision.get("invalidated_anchor_ids") or [])
    invalidated_keys = {
        _wave_key(previous_by_id[identifier])
        for identifier in invalidated
        if identifier in previous_by_id
    }
    visualization = complete_map.get("visualization")
    if not isinstance(visualization, dict):
        visualization = {
            "wave_points": [],
            "levels": [],
            "zones": [],
            "scenario_paths": [],
            "trendlines": [],
            "channels": [],
            "pattern_shapes": [],
            "market_events": [],
            "projected_waves": [],
            "wave_structures": [],
            "chart_comment": "",
        }
        complete_map["visualization"] = visualization
    wave_candidates = [
        previous_by_id[identifier]
        for identifier in previous_by_id
        if identifier in preserved
    ]
    wave_candidates.extend(visualization.get("wave_points") or [])
    wave_candidates = [
        point
        for point in wave_candidates
        if isinstance(point, dict) and _wave_key(point) not in invalidated_keys
    ]
    visualization["wave_points"] = _merge_unique(wave_candidates, _wave_key)

    return {
        "stage": "FULL_DECISION",
        "staged_analysis_version": STAGED_ANALYSIS_VERSION,
        "frozen_snapshot_timestamp": payload.get("timestamp"),
        "validated_market_map": complete_map,
        "raw_execution_market_h1_m15_m5": build_transport_payload(raw),
        "deterministic_market_facts": copy.deepcopy(
            payload.get("deterministic_market_facts", {})
        ),
    }


def _stage_raw_response(response, stage: str, request_id=None) -> Path:
    DEBUG_STAGE_ATTEMPTS_DIR.mkdir(parents=True, exist_ok=True)
    content = []
    for block in getattr(response, "content", []) or []:
        block_type = getattr(block, "type", None)
        item = {"type": block_type}
        if block_type == "text":
            item["text"] = getattr(block, "text", "")
        elif block_type in {"thinking", "redacted_thinking"}:
            item["thinking_present"] = True
        content.append(item)

    usage = get_usage_stats(response)
    raw = {
        "stage": stage,
        "request_id": str(request_id) if request_id not in (None, "") else None,
        "id": getattr(response, "id", None),
        "model": getattr(response, "model", None),
        "stop_reason": getattr(response, "stop_reason", None),
        "content": content,
        "usage": usage,
    }
    identifier = raw.get("id") or raw.get("request_id") or "unknown_response"
    safe_identifier = "".join(
        character
        for character in str(identifier)
        if character.isalnum() or character in {"-", "_"}
    )
    path = DEBUG_STAGE_ATTEMPTS_DIR / f"{stage.lower()}_{safe_identifier}.json"
    path.write_text(
        json.dumps(raw, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    latest = DEBUG_DIR / f"claude_{stage.lower()}_raw_response.json"
    latest.write_text(
        json.dumps(raw, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path


def _count_stage_tokens(client, model, system_prompt, content, effort, schema):
    counted = client.messages.count_tokens(
        model=model,
        system=system_prompt,
        messages=[{"role": "user", "content": content}],
        output_config={
            "effort": effort,
            "format": {"type": "json_schema", "schema": schema},
        },
    )
    return int(counted.input_tokens)


def _completion_reserve_instruction(max_tokens: int) -> str:
    response_reserve_tokens = min(
        32_000,
        max(8_000, int(max_tokens) // 3),
    )
    return (
        "КРИТИЧЕСКОЕ ПРАВИЛО ЗАВЕРШЕНИЯ: max_tokens включает одновременно "
        "внутреннее thinking и финальный Structured JSON. Заверши thinking "
        f"заранее и сохрани не менее {response_reserve_tokens:,} токенов "
        "доступного бюджета для полного JSON. Полный schema-valid ответ "
        "важнее дополнительного рассуждения у границы бюджета. Никогда не "
        "расходуй весь лимит, не завершив финальный JSON."
    )


def _request_structured_stage(
    *,
    stage: str,
    stage_payload: dict,
    system_prompt: str,
    schema: dict,
    max_tokens: int,
    effort_override: str | None = None,
    on_preflight=None,
    on_response=None,
) -> dict:
    normalized_stage = str(stage).upper()
    _LAST_STAGE_USAGE.pop(normalized_stage, None)
    _LAST_STAGE_DIAGNOSTICS.pop(normalized_stage, None)

    config = load_anthropic_config()
    model = get_model(config)
    effort = (
        str(effort_override).strip().lower()
        if effort_override not in (None, "")
        else get_effort(config)
    )
    client = create_anthropic_client(config)

    # Sonnet 5 has no strict adaptive-thinking token budget.  Effort is the
    # provider-supported control; this instruction is an additional soft
    # completion guard, not a replacement for the bounded effort above.
    system_prompt = (
        f"{system_prompt}\n\n"
        f"{_completion_reserve_instruction(max_tokens)}"
    )

    content = [
        {
            "type": "text",
            "text": (
                f"{normalized_stage} FROZEN INPUT.\n"
                "Use every supplied raw candle according to the stage "
                "instructions.\n\n"
                f"<{normalized_stage.lower()}_input>\n"
                f"{_compact_json(stage_payload)}\n"
                f"</{normalized_stage.lower()}_input>"
            ),
        }
    ]

    request_fingerprint = {
        "stage": normalized_stage,
        "model": model,
        "effort": effort,
        "max_tokens": int(max_tokens),
        "system": system_prompt,
        "content": content,
        "schema": schema,
    }
    encoded_payload = _compact_json(stage_payload).encode("utf-8")
    payload_sha256 = hashlib.sha256(
        _compact_json(request_fingerprint).encode("utf-8")
    ).hexdigest()

    print()
    print("=" * 80)
    print(f"ANTHROPIC API — {normalized_stage}")
    print("=" * 80)
    print(f"Модель:       {model}")
    print(f"Max tokens:   {max_tokens}")
    print(f"Effort:       {effort}")
    print("Transport:    SSE streaming")
    print(f"Timeout:      {get_effective_timeout_seconds(config):.0f} sec")
    print("SDK retries:  OFF; retries journaled per stage")
    print(f"Stage input:  {len(encoded_payload) / 1024:.1f} KB")
    print("[INFO] Считаем входные токены этапа...")

    try:
        token_count = _count_stage_tokens(
            client, model, system_prompt, content, effort, schema
        )
    except anthropic.AuthenticationError as error:
        raise ClaudePermanentRequestError(
            "Ошибка авторизации Anthropic API при подсчёте токенов.",
            request_id=_anthropic_error_request_id(error),
            status_code=401,
        ) from error
    except anthropic.PermissionDeniedError as error:
        raise ClaudePermanentRequestError(
            "API key не имеет доступа к модели при подсчёте токенов.",
            request_id=_anthropic_error_request_id(error),
            status_code=403,
        ) from error
    except (anthropic.APITimeoutError, anthropic.APIConnectionError) as error:
        raise ClaudeTransientRequestError(
            "Не удалось выполнить подсчёт входных токенов; платный "
            "Messages-запрос ещё не отправлялся.",
            request_id=_anthropic_error_request_id(error),
        ) from error
    except anthropic.APIStatusError as error:
        raise _translate_api_status_error(error, f"{normalized_stage} counting")

    diagnostics = {
        "model": model,
        "input_tokens": token_count,
        "transport_payload_bytes": len(encoded_payload),
        "payload_sha256": payload_sha256,
        "request_id": None,
        "response_id": None,
        "stop_reason": None,
        "response_received": False,
        "usage": None,
    }
    _LAST_STAGE_DIAGNOSTICS[normalized_stage] = dict(diagnostics)
    if callable(on_preflight):
        on_preflight(dict(diagnostics))

    print(f"[INFO] Входных токенов {normalized_stage}: {token_count:,}")
    print(f"[INFO] Отправляем {normalized_stage} Claude...")

    request_id = None
    recovered_result = None

    def record_stream_progress(values: dict):
        diagnostics.update(values)
        _LAST_STAGE_DIAGNOSTICS[normalized_stage] = dict(diagnostics)
        if callable(on_response):
            try:
                on_response(dict(diagnostics))
            except Exception as callback_error:
                # Telemetry must never break an already running paid stream.
                print(
                    "[API JOURNAL WARNING] Не удалось записать stream "
                    f"progress: {type(callback_error).__name__}: "
                    f"{callback_error}"
                )

    try:
        with client.messages.stream(
            model=model,
            max_tokens=int(max_tokens),
            system=system_prompt,
            messages=[{"role": "user", "content": content}],
            output_config={
                "effort": effort,
                "format": {"type": "json_schema", "schema": schema},
            },
        ) as stream:
            stream_result = consume_structured_stream(
                stream,
                stage=normalized_stage,
                schema=schema,
                payload_sha256=payload_sha256,
                on_progress=record_stream_progress,
            )

            diagnostics.update(stream_result.get("diagnostics") or {})
            request_id = diagnostics.get("request_id")
            recovered_result = stream_result.get("recovered_result")
            stream_error = stream_result.get("error")
            response = stream_result.get("response")

            if recovered_result is None and stream_error is not None:
                raise ClaudeRequestOutcomeUnknownError(
                    f"{normalized_stage}: SSE оборвался после открытия "
                    "потока; полный schema-valid JSON не восстановлен. "
                    "Исход генерации неизвестен и возможна тарификация.",
                    request_id=request_id,
                    diagnostics=diagnostics,
                ) from stream_error
    except anthropic.AuthenticationError as error:
        raise ClaudePermanentRequestError(
            "Ошибка авторизации Anthropic API. Точный повтор не поможет.",
            request_id=_anthropic_error_request_id(error),
            status_code=401,
        ) from error
    except anthropic.PermissionDeniedError as error:
        raise ClaudePermanentRequestError(
            "API key не имеет доступа к модели. Точный повтор не поможет.",
            request_id=_anthropic_error_request_id(error),
            status_code=403,
        ) from error
    except anthropic.RateLimitError as error:
        raise ClaudeTransientRequestError(
            "Превышен rate limit Anthropic API; controlled retry разрешён.",
            request_id=_anthropic_error_request_id(error),
            status_code=429,
            retry_after_seconds=_anthropic_retry_after_seconds(error),
        ) from error
    except anthropic.APITimeoutError as error:
        raise ClaudeRequestOutcomeUnknownError(
            f"{normalized_stage}: timeout после отправки; исход генерации "
            "неизвестен и возможна тарификация.",
            request_id=_anthropic_error_request_id(error),
            diagnostics=diagnostics,
        ) from error
    except anthropic.APIConnectionError as error:
        raise ClaudeRequestOutcomeUnknownError(
            f"{normalized_stage}: SSE соединение оборвалось; исход генерации "
            "неизвестен и возможна тарификация.",
            request_id=_anthropic_error_request_id(error),
            diagnostics=diagnostics,
        ) from error
    except anthropic.APIStatusError as error:
        raise _translate_api_status_error(
            error, f"{normalized_stage} Messages streaming"
        ) from error
    except ClaudeRequestError:
        raise
    except Exception as error:
        raise ClaudeRequestOutcomeUnknownError(
            f"{normalized_stage}: непредвиденная ошибка SSE; исход генерации "
            "неизвестен.",
            request_id=_anthropic_error_request_id(error),
            diagnostics=diagnostics,
        ) from error

    if recovered_result is not None:
        diagnostics.update(
            {
                "request_id": request_id,
                "response_id": None,
                "stop_reason": "recovered_complete_json_without_message_stop",
                "response_received": True,
                "usage": None,
                "delivery_recovered": True,
                "billing_status": "UNKNOWN_MAY_BE_BILLED",
            }
        )
        _LAST_STAGE_DIAGNOSTICS[normalized_stage] = dict(diagnostics)
        if callable(on_response):
            try:
                on_response(dict(diagnostics))
            except Exception as callback_error:
                print(
                    "[API JOURNAL WARNING] Восстановленный ответ уже "
                    "получен, но journal не обновлён: "
                    f"{type(callback_error).__name__}: {callback_error}"
                )
        print(
            f"[DELIVERY RECOVERED] {normalized_stage}: полный JSON "
            "восстановлен из локального SSE journal; новый запрос не нужен."
        )
        return recovered_result

    usage = get_usage_stats(response)
    _LAST_STAGE_USAGE[normalized_stage] = dict(usage)
    diagnostics.update(
        {
            "request_id": (
                str(request_id) if request_id not in (None, "") else None
            ),
            "response_id": getattr(response, "id", None),
            "stop_reason": getattr(response, "stop_reason", None),
            "response_received": True,
            "usage": dict(usage),
            "delivery_recovered": False,
            "billing_status": "USAGE_AVAILABLE",
        }
    )
    _LAST_STAGE_DIAGNOSTICS[normalized_stage] = dict(diagnostics)
    try:
        raw_path = _stage_raw_response(response, normalized_stage, request_id)
    except Exception as raw_error:
        # The paid response is already in memory.  A debug-file failure must
        # never throw it away and trigger another paid generation.
        raw_path = DEBUG_DIR / f"claude_{normalized_stage.lower()}_raw_response.json"
        print(
            "[RAW RESPONSE WARNING] Не удалось сохранить debug response, "
            "но полученный ответ продолжает обрабатываться: "
            f"{type(raw_error).__name__}: {raw_error}"
        )

    if callable(on_response):
        try:
            on_response(dict(diagnostics))
        except Exception as callback_error:
            print(
                "[API JOURNAL WARNING] Ответ этапа уже получен, но journal "
                f"не обновлён: {type(callback_error).__name__}: {callback_error}"
            )

    print(f"[INFO] {normalized_stage} response получен; request_id={request_id}")
    print(
        f"[TOKENS] input={usage['input_tokens']:,}; "
        f"output={usage['output_tokens']:,}; "
        f"thinking={usage.get('thinking_tokens', 0):,}"
    )

    try:
        if getattr(response, "stop_reason", None) == "model_context_window_exceeded":
            raise ClaudePermanentRequestError(
                f"{normalized_stage}: context window exceeded; точный повтор "
                "не исправит вход.",
                request_id=request_id,
            )
        validate_stop_reason(response)
        response_text = extract_text_response(response)
        if not response_text:
            raise RuntimeError("Structured Output отсутствует.")
        result = json.loads(response_text)
        if not isinstance(result, dict) or not result:
            raise RuntimeError("Structured Output должен быть непустым object.")
    except ClaudeRequestError:
        raise
    except Exception as error:
        raise ClaudeInvalidResponseError(
            f"{normalized_stage} тарифицирован, но ответ не прошёл разбор: "
            f"{type(error).__name__}: {error}. Raw: {raw_path}",
            request_id=request_id,
            validation_error=f"{type(error).__name__}: {error}",
        ) from error

    return result


def get_last_stage_usage(stage: str) -> dict | None:
    value = _LAST_STAGE_USAGE.get(str(stage).upper())
    return dict(value) if isinstance(value, dict) else None


def get_last_stage_diagnostics(stage: str) -> dict | None:
    value = _LAST_STAGE_DIAGNOSTICS.get(str(stage).upper())
    return dict(value) if isinstance(value, dict) else None


def _record_wire_warning(message: str) -> None:
    _LAST_WIRE_NORMALIZATION_WARNINGS.append(str(message))


def get_last_wire_normalization_warnings() -> list[str]:
    return list(_LAST_WIRE_NORMALIZATION_WARNINGS)


def _wire_row_to_object(
    value,
    columns: tuple[str, ...],
    path: str,
    *,
    allow_extra: bool = False,
    optional_trailing: int = 0,
) -> dict:
    if not isinstance(value, list):
        raise ValueError(f"{path} должен быть positional array.")
    if len(value) > len(columns) and allow_extra:
        _record_wire_warning(
            f"{path}: удалено лишних trailing-ячеек: {len(value) - len(columns)}."
        )
        value = value[: len(columns)]
    minimum = len(columns) - max(0, int(optional_trailing))
    if minimum <= len(value) < len(columns):
        _record_wire_warning(
            f"{path}: восстановлено пустых trailing-ячеек: {len(columns) - len(value)}."
        )
        value = list(value) + [""] * (len(columns) - len(value))
    if len(value) != len(columns):
        raise ValueError(
            f"{path}: ожидалось {len(columns)} ячеек, получено {len(value)}."
        )
    if any(not isinstance(item, str) for item in value):
        raise ValueError(f"{path}: каждая wire-ячейка должна быть string.")
    return dict(zip(columns, value))


def _expand_wire_data_quality(value, path: str = "data_quality") -> dict:
    """Expands current strict quality object and legacy paid array responses.

    Legacy compatibility is deliberately limited to this one field.  The
    first cell remains the boolean flag and every later string is an issue;
    joining those issue strings is lossless and does not alter market logic.
    Other positional rows remain exact-length and fail closed.
    """
    if isinstance(value, dict):
        if set(value) != {"sufficient", "issues"}:
            raise ValueError(f"{path}: wire object contract неверен.")
        if not isinstance(value["sufficient"], str):
            raise ValueError(f"{path}.sufficient должен быть string.")
        if not isinstance(value["issues"], str):
            raise ValueError(f"{path}.issues должен быть string.")
        result = dict(value)
    elif isinstance(value, list):
        if len(value) < 2:
            raise ValueError(
                f"{path}: ожидалось минимум 2 legacy-ячейки, "
                f"получено {len(value)}."
            )
        if any(not isinstance(item, str) for item in value):
            raise ValueError(f"{path}: каждая legacy wire-ячейка должна быть string.")
        issues = [item.strip() for item in value[1:] if item.strip()]
        result = {
            "sufficient": value[0],
            "issues": "; ".join(issues) if issues else "none",
        }
    else:
        raise ValueError(f"{path} должен быть strict object или legacy array.")

    result["sufficient"] = _wire_boolean(
        result["sufficient"], f"{path}.sufficient"
    )
    return result


def _wire_required_float(value: str, path: str) -> float:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{path}: обязательное число пусто.")
    try:
        number = float(text)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{path}: неверная decimal string {value!r}.") from error
    if not math.isfinite(number):
        raise ValueError(f"{path}: число должно быть finite.")
    return number


def _wire_optional_float(value: str, path: str) -> float | None:
    if not str(value).strip():
        return None
    return _wire_required_float(value, path)


def _wire_integer(value: str, path: str) -> int:
    text = str(value).strip()
    try:
        number = int(text)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{path}: неверная integer string {value!r}.") from error
    if str(number) != text and f"+{number}" != text:
        raise ValueError(f"{path}: integer должен быть записан без дробной части.")
    return number


def _wire_boolean(value: str, path: str) -> bool:
    normalized = str(value).strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ValueError(f'{path}: ожидалась строка "true" или "false".')


def _expand_wire_visualization(value) -> dict:
    if not isinstance(value, dict):
        raise ValueError("visualization должен быть compact wire object.")
    missing = set(COMPACT_VISUALIZATION_WIRE_SCHEMA["required"]) - set(value)
    extra = set(value) - set(COMPACT_VISUALIZATION_WIRE_SCHEMA["required"])
    if missing:
        _record_wire_warning(
            "visualization: восстановлены отсутствующие поля: "
            + ", ".join(sorted(missing))
            + "."
        )
    if extra:
        _record_wire_warning(
            "visualization: проигнорированы лишние поля: "
            + ", ".join(sorted(extra))
            + "."
        )

    def safe_rows(name, columns, required_prices=(), optional_prices=(), optional_trailing=0):
        result = []
        rows = value.get(name, [])
        if not isinstance(rows, list):
            _record_wire_warning(f"visualization.{name}: не-array отброшен.")
            return result
        for index, row in enumerate(rows):
            path = f"visualization.{name}[{index}]"
            try:
                item = _wire_row_to_object(
                    row,
                    columns,
                    path,
                    allow_extra=True,
                    optional_trailing=optional_trailing,
                )
                for field in required_prices:
                    item[field] = _wire_required_float(item[field], f"{path}.{field}")
                for field in optional_prices:
                    item[field] = _wire_optional_float(item[field], f"{path}.{field}")
                result.append(item)
            except (TypeError, ValueError) as error:
                _record_wire_warning(f"{path}: объект отброшен ({error}).")
        return result

    wave_points = safe_rows("wave_points", WAVE_POINT_COLUMNS, ("price",))
    valid_wave_points = []
    for index, item in enumerate(wave_points):
        try:
            item["sequence"] = _wire_integer(
                item["sequence"], f"visualization.wave_points[{index}].sequence"
            )
            valid_wave_points.append(item)
        except ValueError as error:
            _record_wire_warning(
                f"visualization.wave_points[{index}]: объект отброшен ({error})."
            )
    wave_points = valid_wave_points

    levels = safe_rows("levels", LEVEL_COLUMNS, ("price",))
    zones = safe_rows("zones", ZONE_COLUMNS, ("price_low", "price_high"))
    scenario_paths = safe_rows(
        "scenario_paths", SCENARIO_PATH_COLUMNS,
        ("anchor_price", "target_price_low", "target_price_high"),
    )

    trendlines = safe_rows(
        "trendlines", TRENDLINE_COLUMNS, ("start_price", "end_price")
    )
    channels = safe_rows(
        "channels",
        CHANNEL_COLUMNS,
        (
            "upper_start_price", "upper_end_price",
            "lower_start_price", "lower_end_price",
        ),
        ("breakout_price", "reentry_price"),
        optional_trailing=4,
    )
    pattern_shapes = safe_rows(
        "pattern_shapes",
        PATTERN_SHAPE_COLUMNS,
        ("price_low", "price_high"),
        ("confirmation_level", "invalidation_level", "target_price"),
    )
    market_events = safe_rows(
        "market_events", MARKET_EVENT_COLUMNS, ("price",)
    )
    projected_waves = safe_rows(
        "projected_waves",
        PROJECTED_WAVE_COLUMNS,
        ("anchor_price", "target_price_low", "target_price_high"),
        ("confirmation_level", "invalidation_level"),
    )
    wave_structures = safe_rows(
        "wave_structures",
        WAVE_STRUCTURE_COLUMNS,
        (),
        ("confirmation_level", "invalidation_level"),
    )

    chart_comment = value.get("chart_comment")
    if not isinstance(chart_comment, str):
        _record_wire_warning("visualization.chart_comment восстановлен пустой строкой.")
        chart_comment = ""
    return {
        "wave_points": wave_points,
        "levels": levels,
        "zones": zones,
        "scenario_paths": scenario_paths,
        "trendlines": trendlines,
        "channels": channels,
        "pattern_shapes": pattern_shapes,
        "market_events": market_events,
        "projected_waves": projected_waves,
        "wave_structures": wave_structures,
        "chart_comment": chart_comment,
    }


def _expand_market_map_wire_result(wire_result: dict) -> dict:
    _LAST_WIRE_NORMALIZATION_WARNINGS.clear()
    if not isinstance(wire_result, dict):
        raise ValueError("FULL_MAP wire response должен быть object.")
    if set(wire_result) != set(MARKET_MAP_WIRE_SCHEMA["required"]):
        raise ValueError("FULL_MAP wire top-level contract неверен.")

    result = copy.deepcopy(wire_result)
    result["market_regime"] = _wire_row_to_object(
        result["market_regime"], MARKET_REGIME_COLUMNS, "market_regime"
    )
    result["timeframe_analysis"] = _wire_row_to_object(
        result["timeframe_analysis"],
        TIMEFRAME_ANALYSIS_COLUMNS,
        "timeframe_analysis",
    )
    result["price_structure"] = _wire_row_to_object(
        result["price_structure"], PRICE_STRUCTURE_COLUMNS, "price_structure"
    )
    result["wave_count"] = _wire_row_to_object(
        result["wave_count"], WAVE_COUNT_COLUMNS, "wave_count"
    )
    result["wave_count"]["invalidation_level"] = _wire_optional_float(
        result["wave_count"]["invalidation_level"],
        "wave_count.invalidation_level",
    )
    result["higher_timeframe_context"] = _wire_row_to_object(
        result["higher_timeframe_context"],
        HIGHER_TIMEFRAME_CONTEXT_COLUMNS,
        "higher_timeframe_context",
    )
    result["scenario_map"] = _wire_row_to_object(
        result["scenario_map"], SCENARIO_MAP_COLUMNS, "scenario_map",
        allow_extra=True,
    )
    result["visualization"] = _expand_wire_visualization(
        result["visualization"]
    )
    result["data_quality"] = _expand_wire_data_quality(
        result["data_quality"]
    )
    return result


def _expand_trade_decision_wire_result(wire_result: dict) -> dict:
    _LAST_WIRE_NORMALIZATION_WARNINGS.clear()
    if not isinstance(wire_result, dict):
        raise ValueError("FULL_DECISION wire response должен быть object.")
    if set(wire_result) != set(TRADE_DECISION_WIRE_SCHEMA["required"]):
        raise ValueError("FULL_DECISION wire top-level contract неверен.")

    result = copy.deepcopy(wire_result)
    result["visualization"] = _expand_wire_visualization(
        result["visualization"]
    )
    result["recommendation"] = _wire_row_to_object(
        result["recommendation"], RECOMMENDATION_COLUMNS, "recommendation",
        allow_extra=True,
        optional_trailing=1,
    )
    for name in (
        "entry_price",
        "stop_loss",
        "take_profit",
        "invalidation_level",
    ):
        result["recommendation"][name] = _wire_optional_float(
            result["recommendation"][name], f"recommendation.{name}"
        )
    result["data_quality"] = _expand_wire_data_quality(
        result["data_quality"]
    )
    return result


def validate_market_map_result(
    result: dict,
    payload: dict,
    previous_anchor_reference: dict,
) -> None:
    if result.get("instrument") != "XAUUSD":
        raise ValueError("FULL_MAP вернул неожиданный instrument.")
    if set(result) != set(MARKET_MAP_SCHEMA["required"]):
        raise ValueError("FULL_MAP top-level contract не совпадает со schema.")

    revision = result.get("wave_revision")
    if not isinstance(revision, dict):
        raise ValueError("FULL_MAP не содержит wave_revision.")
    mode = revision.get("mode")
    if mode not in {"initialize", "unchanged", "extend", "recount"}:
        raise ValueError(f"Неизвестный wave revision mode: {mode}.")

    available = {
        str(item.get("anchor_id"))
        for item in previous_anchor_reference.get("anchors", [])
        if isinstance(item, dict) and item.get("anchor_id")
    }
    preserved = [str(value) for value in revision.get("preserved_anchor_ids", [])]
    invalidated = [
        str(value) for value in revision.get("invalidated_anchor_ids", [])
    ]
    if len(preserved) != len(set(preserved)):
        raise ValueError("wave_revision содержит повтор preserved id.")
    if len(invalidated) != len(set(invalidated)):
        raise ValueError("wave_revision содержит повтор invalidated id.")
    preserved_set = set(preserved)
    invalidated_set = set(invalidated)
    if preserved_set & invalidated_set:
        raise ValueError("Один anchor одновременно preserved и invalidated.")
    if (preserved_set | invalidated_set) != available:
        missing = sorted(available - preserved_set - invalidated_set)
        unknown = sorted((preserved_set | invalidated_set) - available)
        raise ValueError(
            "Каждый previous anchor должен быть явно классифицирован. "
            f"missing={missing}; unknown={unknown}."
        )
    if not available and mode not in {"initialize", "recount"}:
        raise ValueError("Без previous anchors FULL_MAP должен initialize карту.")
    if not str(revision.get("reason", "")).strip():
        raise ValueError("wave_revision.reason пуст.")

    quality = result.get("data_quality")
    if not isinstance(quality, dict) or not isinstance(
        quality.get("sufficient"), bool
    ):
        raise ValueError("FULL_MAP data_quality неверен.")

    # Reuse the legacy professional-trader semantic validator before the map
    # is marked as a durable winner.  A malformed regime must be retried at
    # FULL_MAP, not discovered after paying for FULL_DECISION.
    semantic_probe = copy.deepcopy(result)
    semantic_probe["recommendation"] = {
        "action": "stay_out",
        "setup_type": "no_trade",
        "trade_horizon": "unclear",
        "setup_quality": "weak",
        "entry_quality": "poor",
        "order_type": "none",
        "entry_price": None,
        "stop_loss": None,
        "take_profit": None,
        "invalidation_level": None,
        "confidence": "low",
        "why_now": "Map validation only.",
        "structural_stop_basis": "Not applicable.",
        "target_basis": "Not applicable.",
        "reasoning": "Map validation only.",
        "invalidation_reason": "No trade decision at FULL_MAP.",
        "fvg_role": "no_relevant_fvg",
        "fvg_ids": "",
        "fvg_basis": "EN: No trade decision is made at the market-map stage.\nRU: На этапе карты рынка торговое решение не принимается.",
    }
    validate_trade_levels(semantic_probe)
    validate_analysis_contract(semantic_probe)

    # The sanitizer validates only chart metadata and never changes analysis.
    sanitize_visualization(result, payload)


def analyze_market_map(
    payload: dict,
    previous_reference: dict | None = None,
    on_preflight=None,
    on_response=None,
) -> dict:
    stage_payload = build_market_map_stage_payload(payload, previous_reference)
    wire_result = _request_structured_stage(
        stage="FULL_MAP",
        stage_payload=stage_payload,
        system_prompt=(
            f"{MARKET_MAP_SYSTEM_PROMPT}\n\n{MARKET_MAP_WIRE_INSTRUCTIONS}"
        ),
        schema=MARKET_MAP_WIRE_SCHEMA,
        max_tokens=MAP_MAX_TOKENS,
        effort_override=MAP_EFFORT,
        on_preflight=on_preflight,
        on_response=on_response,
    )
    invalid_result = wire_result
    try:
        result = _expand_market_map_wire_result(wire_result)
        invalid_result = result
        validate_market_map_result(
            result,
            payload,
            stage_payload["previous_confirmed_wave_anchors"],
        )
    except Exception as error:
        raise ClaudeInvalidResponseError(
            "FULL_MAP тарифицирован, но не прошёл локальную проверку: "
            f"{type(error).__name__}: {error}.",
            invalid_result=invalid_result,
            validation_error=f"{type(error).__name__}: {error}",
        ) from error
    return result


def analyze_trade_decision(
    payload: dict,
    market_map: dict,
    previous_reference: dict | None = None,
    on_preflight=None,
    on_response=None,
) -> dict:
    stage_payload = build_trade_decision_stage_payload(
        payload,
        market_map,
        previous_reference=previous_reference,
    )
    wire_result = _request_structured_stage(
        stage="FULL_DECISION",
        stage_payload=stage_payload,
        system_prompt=(
            f"{TRADE_DECISION_SYSTEM_PROMPT}\n\n"
            f"{TRADE_DECISION_WIRE_INSTRUCTIONS}"
        ),
        schema=TRADE_DECISION_WIRE_SCHEMA,
        max_tokens=DECISION_MAX_TOKENS,
        effort_override=DECISION_EFFORT,
        on_preflight=on_preflight,
        on_response=on_response,
    )
    try:
        result = _expand_trade_decision_wire_result(wire_result)
    except Exception as error:
        raise ClaudeInvalidResponseError(
            "FULL_DECISION тарифицирован, но compact wire не развёрнут: "
            f"{type(error).__name__}: {error}.",
            invalid_result=wire_result,
            validation_error=f"{type(error).__name__}: {error}",
        ) from error
    if result.get("instrument") != "XAUUSD":
        raise ClaudeInvalidResponseError(
            "FULL_DECISION вернул неожиданный instrument.",
            invalid_result=result,
            validation_error="Unexpected instrument.",
        )
    if set(result) != set(TRADE_DECISION_SCHEMA["required"]):
        raise ClaudeInvalidResponseError(
            "FULL_DECISION top-level contract не совпадает со schema.",
            invalid_result=result,
            validation_error="Top-level contract does not match schema.",
        )
    return result


def analyze_entry_check(
    payload: dict,
    market_map: dict,
    previous_reference: dict | None = None,
    on_preflight=None,
    on_response=None,
) -> dict:
    """Small decision-only confirmation; never rebuilds the paid market map."""
    stage_payload = build_trade_decision_stage_payload(
        payload, market_map, previous_reference=previous_reference
    )
    stage_payload["stage"] = "ENTRY_CHECK"
    wire_result = _request_structured_stage(
        stage="ENTRY_CHECK",
        stage_payload=stage_payload,
        system_prompt=(
            f"{ENTRY_CHECK_SYSTEM_PROMPT}\n\n{TRADE_DECISION_WIRE_INSTRUCTIONS}"
        ),
        schema=TRADE_DECISION_WIRE_SCHEMA,
        max_tokens=ENTRY_CHECK_MAX_TOKENS,
        effort_override=ENTRY_CHECK_EFFORT,
        on_preflight=on_preflight,
        on_response=on_response,
    )
    try:
        result = _expand_trade_decision_wire_result(wire_result)
    except Exception as error:
        raise ClaudeInvalidResponseError(
            "ENTRY_CHECK тарифицирован, но compact wire не развёрнут: "
            f"{type(error).__name__}: {error}.",
            invalid_result=wire_result,
            validation_error=f"{type(error).__name__}: {error}",
        ) from error
    if result.get("instrument") != "XAUUSD" or set(result) != set(
        TRADE_DECISION_SCHEMA["required"]
    ):
        raise ClaudeInvalidResponseError(
            "ENTRY_CHECK contract неверен.",
            invalid_result=result,
            validation_error="ENTRY_CHECK contract mismatch.",
        )
    return result


def repair_market_map(
    payload: dict,
    invalid_result: dict,
    validation_error: str,
    previous_reference: dict | None = None,
    on_preflight=None,
    on_response=None,
) -> dict:
    """Repair one known invalid map without buying the raw MAX map again."""
    previous_anchors = build_previous_confirmed_anchor_reference(
        previous_reference
    )
    repair_payload = {
        "stage": "FULL_MAP_REPAIR",
        "repair_scope": "contract_and_reported_validation_error_only",
        "validation_error": str(validation_error),
        "immutable_facts": {
            "instrument": "XAUUSD",
            "timestamp": str(payload.get("timestamp")),
            "timezone": payload.get("timezone"),
            "symbol_specification": (
                payload.get("cacheable_history", {}).get(
                    "symbol_specification", {}
                )
                if isinstance(payload.get("cacheable_history"), dict)
                else {}
            ),
            "previous_confirmed_wave_anchors": previous_anchors,
        },
        "supplied_invalid_result": copy.deepcopy(invalid_result),
    }
    wire_result = _request_structured_stage(
        stage="FULL_MAP_REPAIR",
        stage_payload=repair_payload,
        system_prompt=(
            f"{MARKET_MAP_REPAIR_SYSTEM_PROMPT}\n\n"
            f"{MARKET_MAP_WIRE_INSTRUCTIONS}"
        ),
        schema=MARKET_MAP_WIRE_SCHEMA,
        max_tokens=MAP_REPAIR_MAX_TOKENS,
        effort_override=REPAIR_EFFORT,
        on_preflight=on_preflight,
        on_response=on_response,
    )
    invalid_repair_result = wire_result
    try:
        result = _expand_market_map_wire_result(wire_result)
        invalid_repair_result = result
        validate_market_map_result(result, payload, previous_anchors)
    except Exception as error:
        raise ClaudeInvalidResponseError(
            "FULL_MAP_REPAIR не прошёл локальную проверку: "
            f"{type(error).__name__}: {error}.",
            invalid_result=invalid_repair_result,
            validation_error=f"{type(error).__name__}: {error}",
        ) from error
    return result


def repair_trade_decision(
    payload: dict,
    market_map: dict,
    invalid_result: dict,
    validation_error: str,
    previous_reference: dict | None = None,
    on_preflight=None,
    on_response=None,
) -> dict:
    """Repair one decision contract using only its existing execution scope."""
    live_market = payload.get("live_market") or {}
    live_by_tf = live_market.get("raw_timeframes_since_day_start") or {}
    compact_bars = {}
    for timeframe in DECISION_TIMEFRAMES:
        source = live_by_tf.get(timeframe) or {}
        closed = source.get("closed_bars_since_day_start") or []
        compact_bars[timeframe] = {
            "latest_closed_bars": copy.deepcopy(closed[-8:]),
            "current_unclosed_bar": copy.deepcopy(
                source.get("current_unclosed_bar")
            ),
        }
    repair_payload = {
        "stage": "FULL_DECISION_REPAIR",
        "repair_scope": "contract_and_reported_validation_error_only",
        "validation_error": str(validation_error),
        "supplied_invalid_result": copy.deepcopy(invalid_result),
        # REPAIR must not buy the full H1/M15/M5 history again. The already
        # paid result contains the analysis; these immutable facts are enough
        # to fix a local contract/translation/level error or choose stay_out.
        "compact_immutable_context": {
            "frozen_snapshot_timestamp": payload.get("timestamp"),
            "current_price": copy.deepcopy(live_market.get("current_price")),
            "validated_market_map": copy.deepcopy(market_map),
            "latest_execution_bars": compact_bars,
        },
    }
    wire_result = _request_structured_stage(
        stage="FULL_DECISION_REPAIR",
        stage_payload=repair_payload,
        system_prompt=(
            f"{TRADE_DECISION_REPAIR_SYSTEM_PROMPT}\n\n"
            f"{TRADE_DECISION_WIRE_INSTRUCTIONS}"
        ),
        schema=TRADE_DECISION_WIRE_SCHEMA,
        max_tokens=DECISION_REPAIR_MAX_TOKENS,
        effort_override=REPAIR_EFFORT,
        on_preflight=on_preflight,
        on_response=on_response,
    )
    try:
        result = _expand_trade_decision_wire_result(wire_result)
    except Exception as error:
        raise ClaudeInvalidResponseError(
            "FULL_DECISION_REPAIR compact wire не развёрнут: "
            f"{type(error).__name__}: {error}.",
            invalid_result=wire_result,
            validation_error=f"{type(error).__name__}: {error}",
        ) from error
    if result.get("instrument") != "XAUUSD":
        raise ClaudeInvalidResponseError(
            "FULL_DECISION_REPAIR вернул неожиданный instrument.",
            invalid_result=result,
            validation_error="Unexpected instrument after repair.",
        )
    if set(result) != set(TRADE_DECISION_SCHEMA["required"]):
        raise ClaudeInvalidResponseError(
            "FULL_DECISION_REPAIR top-level contract не совпадает со schema.",
            invalid_result=result,
            validation_error="Top-level contract mismatch after repair.",
        )
    return result


def _wave_key(point: dict) -> tuple:
    return (
        str(point.get("scenario", "")),
        str(point.get("degree", "")),
        str(point.get("timeframe", "")),
        int(point.get("sequence", 0) or 0),
        str(point.get("label", "")),
        str(point.get("time", "")),
        point.get("price"),
    )


def _merge_unique(items: list[dict], key_builder) -> list[dict]:
    ordered = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        ordered[key_builder(item)] = copy.deepcopy(item)
    return list(ordered.values())


def _generic_visual_key(item: dict) -> str:
    return _compact_json(item)


def _bilingual_parts(value) -> tuple[str, str]:
    text = str(value or "").strip()
    if text.startswith("EN:") and "\nRU:" in text:
        english, russian = text[3:].split("\nRU:", 1)
        return english.strip(), russian.strip()
    if text.startswith("EN:") and " RU:" in text:
        english, russian = text[3:].split(" RU:", 1)
        return english.strip(), russian.strip()
    return text, text


def _merge_bilingual_text(*values, english: str = "", russian: str = "") -> str:
    english_parts = []
    russian_parts = []
    for value in values:
        en_part, ru_part = _bilingual_parts(value)
        if en_part:
            english_parts.append(en_part)
        if ru_part:
            russian_parts.append(ru_part)
    if english:
        english_parts.append(english.strip())
    if russian:
        russian_parts.append(russian.strip())
    return f"EN: {' '.join(english_parts)}\nRU: {' '.join(russian_parts)}"


def assemble_staged_analysis(
    *,
    payload: dict,
    market_map: dict,
    trade_decision: dict,
    previous_reference: dict | None,
) -> dict:
    """Assembles and validates the original FULL business contract."""
    previous_anchors = build_previous_confirmed_anchor_reference(
        previous_reference
    )
    previous_by_id = {
        str(item["anchor_id"]): copy.deepcopy(item["point"])
        for item in previous_anchors.get("anchors", [])
        if isinstance(item, dict) and item.get("anchor_id")
    }
    revision = market_map["wave_revision"]
    preserved_ids = set(revision.get("preserved_anchor_ids", []))
    invalidated_ids = set(revision.get("invalidated_anchor_ids", []))
    invalidated_keys = {
        _wave_key(previous_by_id[identifier])
        for identifier in invalidated_ids
        if identifier in previous_by_id
    }

    map_visual = market_map.get("visualization") or {}
    decision_visual = trade_decision.get("visualization") or {}
    wave_candidates = [
        previous_by_id[identifier]
        for identifier in previous_by_id
        if identifier in preserved_ids
    ]
    wave_candidates.extend(map_visual.get("wave_points") or [])
    wave_candidates.extend(decision_visual.get("wave_points") or [])
    wave_candidates = [
        point
        for point in wave_candidates
        if isinstance(point, dict) and _wave_key(point) not in invalidated_keys
    ]

    visualization = {
        "wave_points": _merge_unique(wave_candidates, _wave_key),
        "levels": _merge_unique(
            list(map_visual.get("levels") or [])
            + list(decision_visual.get("levels") or []),
            _generic_visual_key,
        ),
        "zones": _merge_unique(
            list(map_visual.get("zones") or [])
            + list(decision_visual.get("zones") or []),
            _generic_visual_key,
        ),
        "scenario_paths": _merge_unique(
            list(map_visual.get("scenario_paths") or [])
            + list(decision_visual.get("scenario_paths") or []),
            _generic_visual_key,
        ),
        "trendlines": _merge_unique(
            list(map_visual.get("trendlines") or [])
            + list(decision_visual.get("trendlines") or []),
            _generic_visual_key,
        ),
        "channels": _merge_unique(
            list(map_visual.get("channels") or [])
            + list(decision_visual.get("channels") or []),
            _generic_visual_key,
        ),
        "pattern_shapes": _merge_unique(
            list(map_visual.get("pattern_shapes") or [])
            + list(decision_visual.get("pattern_shapes") or []),
            _generic_visual_key,
        ),
        "market_events": _merge_unique(
            list(map_visual.get("market_events") or [])
            + list(decision_visual.get("market_events") or []),
            _generic_visual_key,
        ),
        "projected_waves": _merge_unique(
            list(map_visual.get("projected_waves") or [])
            + list(decision_visual.get("projected_waves") or []),
            _generic_visual_key,
        ),
        "wave_structures": _merge_unique(
            list(map_visual.get("wave_structures") or [])
            + list(decision_visual.get("wave_structures") or []),
            _generic_visual_key,
        ),
        "chart_comment": _merge_bilingual_text(
            map_visual.get("chart_comment"),
            decision_visual.get("chart_comment"),
            english=(
                f"Wave revision={revision.get('mode')}; "
                f"preserved={len(preserved_ids)}; "
                f"invalidated={len(invalidated_ids)}."
            ),
            russian=(
                f"Ревизия волн={revision.get('mode')}; "
                f"сохранено={len(preserved_ids)}; "
                f"отменено={len(invalidated_ids)}."
            ),
        ),
    }

    map_tf = copy.deepcopy(market_map["timeframe_analysis"])
    map_tf["H1"] = _merge_bilingual_text(
        map_tf["H1"], trade_decision["h1_execution_context"]
    )
    map_tf["relationship"] = _merge_bilingual_text(
        map_tf["relationship"], trade_decision["multi_timeframe_relationship"]
    )

    map_quality = market_map.get("data_quality") or {}
    decision_quality = trade_decision.get("data_quality") or {}
    quality_issues = []
    for value in (map_quality.get("issues"), decision_quality.get("issues")):
        normalized = str(value or "").strip()
        if normalized and normalized.lower() not in {"none", "no issues"}:
            if normalized not in quality_issues:
                quality_issues.append(normalized)

    analysis = {
        "timestamp": str(payload.get("timestamp") or trade_decision["timestamp"]),
        "instrument": "XAUUSD",
        "market_regime": copy.deepcopy(market_map["market_regime"]),
        "timeframe_analysis": map_tf,
        "price_structure": copy.deepcopy(market_map["price_structure"]),
        "patterns": _merge_bilingual_text(
            market_map["patterns"], trade_decision["microstructure_and_patterns"]
        ),
        "wave_count": copy.deepcopy(market_map["wave_count"]),
        "higher_timeframe_context": copy.deepcopy(
            market_map["higher_timeframe_context"]
        ),
        "scenario_map": copy.deepcopy(market_map["scenario_map"]),
        "visualization": visualization,
        "recommendation": copy.deepcopy(trade_decision["recommendation"]),
        "data_quality": {
            "sufficient": bool(map_quality.get("sufficient"))
            and bool(decision_quality.get("sufficient")),
            "issues": "; ".join(quality_issues) if quality_issues else "none",
        },
    }

    if set(analysis) != set(CLAUDE_RESPONSE_SCHEMA["required"]):
        raise ValueError("Assembled FULL top-level contract изменён.")
    validate_trade_levels(analysis)
    validate_analysis_contract(analysis)
    sanitize_visualization(analysis, payload)
    return analysis


def combine_stage_usage(
    map_usage: dict | None,
    decision_usage: dict | None,
) -> dict:
    map_usage = dict(map_usage) if isinstance(map_usage, dict) else {}
    decision_usage = (
        dict(decision_usage) if isinstance(decision_usage, dict) else {}
    )
    fields = {
        "input_tokens",
        "output_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
        "thinking_tokens",
    }
    totals = {
        field: int(map_usage.get(field, 0) or 0)
        + int(decision_usage.get(field, 0) or 0)
        for field in sorted(fields)
    }
    return {
        "staged_analysis_version": STAGED_ANALYSIS_VERSION,
        "market_map": map_usage,
        "trade_decision": decision_usage,
        "totals": totals,
    }
