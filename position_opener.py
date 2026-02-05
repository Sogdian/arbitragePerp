"""
Автоматическое открытие позиций (Long/Short) на биржах по API ключам из .env.

Требования:
- Ордера: лимитные по лучшим ценам стакана (ask для Long, bid для Short)
- Перед отправкой: preflight (шаги/минималки/минимальная сумма, ликвидность на best price)
- Маржа: изолированная
- Плечо: 1

Переменные окружения:
- BYBIT_API_KEY, BYBIT_API_SECRET
- GATEIO_API_KEY, GATEIO_API_SECRET
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import base64
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
from typing import Any, Dict, List, Optional, Tuple

import httpx

# Загружаем переменные окружения из .env файла (если еще не загружены)
try:
    from dotenv import load_dotenv
    load_dotenv('.env', override=False)
except ImportError:
    # Если python-dotenv не установлен, пробуем простую загрузку
    def load_dotenv(path: str = ".env") -> None:
        if not os.path.exists(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip('"').strip("'")
                    if k and (k not in os.environ):
                        os.environ[k] = v
        except Exception:
            pass
    load_dotenv('.env')


# Логируем в __main__, чтобы совпадало с основным логгером bot.py
logger = logging.getLogger("__main__")

# Глубина стакана при открытии: уровни 1..N, с первого уровня частичные ордера (IOC) до набора объёма.
MAX_ORDERBOOK_LEVELS = max(1, int(os.getenv("OPEN_ORDERBOOK_LEVELS", "5") or "5"))


def _format_number(value: Optional[float], precision: int = 3) -> str:
    """
    Форматирует число до указанной точности и убирает нули на конце.
    
    Args:
        value: Число для форматирования (может быть None)
        precision: Количество знаков после запятой (по умолчанию 3)
    
    Returns:
        Отформатированная строка или "N/A" если value is None
    """
    if value is None:
        return "N/A"
    
    formatted = f"{value:.{precision}f}"
    # Убираем нули на конце
    if '.' in formatted:
        formatted = formatted.rstrip('0').rstrip('.')
    
    return formatted


@dataclass
class OpenLegResult:
    exchange: str
    direction: str  # "long" | "short"
    ok: bool
    order_id: Optional[str] = None
    error: Optional[str] = None
    raw: Optional[Any] = None
    filled_qty: Optional[float] = None  # при открытии несколькими IOC — суммарно исполнено
    avg_price: Optional[float] = None   # средняя цена исполнения (для лога)


def _get_env(name: str) -> Optional[str]:
    v = os.getenv(name)
    if v and str(v).strip():
        return str(v).strip()
    return None


def _get_env_any(names: Tuple[str, ...]) -> Optional[str]:
    """
    Backward-compat helper: returns first existing env var from names.
    """
    for n in names:
        v = _get_env(n)
        if v:
            return v
    return None


def _floor_to_step(x: float, step: float) -> float:
    if step <= 0:
        return float(x)
    x = float(x)
    step = float(step)
    # protect against floating error: e.g. 1.0000000002 / 0.1
    k = math.floor((x + 1e-12) / step)
    return float(k * step)


def _ceil_to_step(x: float, step: float) -> float:
    if step <= 0:
        return float(x)
    x = float(x)
    step = float(step)
    k = math.ceil((x - 1e-12) / step)
    return float(k * step)


def _is_multiple_of_step(x: float, step: float, eps: float = 1e-12) -> bool:
    if step <= 0:
        return True
    k = x / step
    return abs(k - round(k)) <= eps


def _round_price_for_side(price: float, tick: float, side: str) -> float:
    """
    Округление цены по шагу (tick): для buy — вверх (ceil), для sell — вниз (floor).
    Так лимит получается «агрессивнее»: покупка по цене не ниже лучшего ask, продажа по цене не выше
    лучшего bid, чтобы ордер мог исполниться против текущего стакана.
    """
    price = float(price)
    tick = float(tick)
    if tick <= 0:
        return price
    s = (side or "").lower().strip()
    if s in ("buy", "long"):
        return _ceil_to_step(price, tick)
    return _floor_to_step(price, tick)


def _decimals_from_step_str(step_raw: Optional[str]) -> int:
    """
    Returns number of decimals implied by step string.
    Supports: "0.001", "1", "1e-5", "5E-3".
    """
    if not step_raw:
        return 0
    s = str(step_raw).strip()
    if not s:
        return 0
    try:
        d = Decimal(s)
    except InvalidOperation:
        # fallback: try float then Decimal
        try:
            d = Decimal(str(float(s)))
        except Exception:
            return 0
    # Normalize removes trailing zeros but keeps exponent
    # Example: Decimal("1e-5").as_tuple().exponent == -5
    exp = d.as_tuple().exponent
    return int(max(0, -exp))


def _format_by_step(x: float, step_raw: Optional[str]) -> str:
    """
    Format number with decimals implied by step_raw.
    - step_raw="0.001" -> 3 decimals
    - step_raw="1e-5"  -> 5 decimals
    Trims trailing zeros.
    """
    decimals = _decimals_from_step_str(step_raw)
    if decimals <= 0:
        # avoid "-0"
        xi = int(round(float(x)))
        return str(xi)
    # Use Decimal for stable rounding/formatting
    try:
        q = Decimal("1").scaleb(-decimals)  # 10^-decimals
        dx = Decimal(str(float(x))).quantize(q)  # default rounding half-even ok for display
        s = format(dx, "f")
    except Exception:
        s = f"{float(x):.{decimals}f}"
    s = s.rstrip("0").rstrip(".")
    return s if s else "0"


def _price_level_for_target_size(levels: Any, target_size: float) -> Tuple[Optional[float], float]:
    """
    Возвращает цену уровня, на котором суммарный объем (size) достигает target_size.
    levels: [[price, size], ...]
    """
    if not isinstance(levels, list) or target_size <= 0:
        return None, 0.0
    cum = 0.0
    for lvl in levels:
        if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
            continue
        try:
            p = float(lvl[0])
            s = float(lvl[1])
        except Exception:
            continue
        if p <= 0 or s <= 0:
            continue
        cum += s
        if cum + 1e-12 >= target_size:
            return p, cum
    return None, cum


async def open_long_short_positions(
    *,
    bot: Any,
    coin: str,
    long_exchange: str,
    short_exchange: str,
    coin_amount: float,
) -> Tuple[bool, Optional[float], Optional[float]]:
    """
    Открывает Long (на long_exchange) и Short (на short_exchange) на coin_amount монет (для каждой ноги).
    Возвращает (ok, long_price, short_price): ok=True только если обе ноги открылись успешно;
    при ok=True — фактические средние цены исполнения (long_price, short_price), иначе (None, None).
    """
    try:
        coin_amount = float(coin_amount)
    except Exception:
        logger.error(f"❌ Некорректное количество монет: {coin_amount!r}")
        return False, None, None
    if coin_amount <= 0:
        logger.error(f"❌ Количество монет должно быть > 0, получено: {coin_amount}")
        return False, None, None

    logger.info(f"🧩 Авто-открытие позиций: {coin} | Long {long_exchange} + Short {short_exchange} | qty={_format_number(coin_amount)} {coin}")

    long_obj = (getattr(bot, "exchanges", {}) or {}).get(long_exchange)
    short_obj = (getattr(bot, "exchanges", {}) or {}).get(short_exchange)
    if long_obj is None or short_obj is None:
        logger.error(f"❌ Не найдены биржи в bot.exchanges: long={long_exchange} short={short_exchange}")
        return False, None, None

    # 1) Preflight: проверяем возможность открытия на обеих биржах заранее
    long_plan = await _plan_one_leg(exchange_name=long_exchange, exchange_obj=long_obj, coin=coin, direction="long", coin_amount=coin_amount)
    if isinstance(long_plan, OpenLegResult) and not long_plan.ok:
        logger.error(f"❌ Preflight failed (Long): {long_plan.error}")
        return False, None, None
    short_plan = await _plan_one_leg(exchange_name=short_exchange, exchange_obj=short_obj, coin=coin, direction="short", coin_amount=coin_amount)
    if isinstance(short_plan, OpenLegResult) and not short_plan.ok:
        logger.error(f"❌ Preflight failed (Short): {short_plan.error}")
        return False, None, None
    if not isinstance(long_plan, dict) or not isinstance(short_plan, dict):
        logger.error("❌ Preflight не пройден, ордера не отправлены")
        return False, None, None

    # 2) Выставляем лимитные ордера параллельно
    long_task = _place_one_leg(planned=long_plan)
    short_task = _place_one_leg(planned=short_plan)
    long_res_raw, short_res_raw = await _gather2(long_task, short_task)
    long_res = _as_open_leg_result(long_res_raw, exchange=str(long_exchange), direction="long")
    short_res = _as_open_leg_result(short_res_raw, exchange=str(short_exchange), direction="short")

    _log_leg_result(long_res)
    _log_leg_result(short_res)

    # 3) Проверка fill: весь объём исполнен (одним ордером или несколькими частичными)
    long_filled_ok = False
    short_filled_ok = False
    long_filled_qty = 0.0
    short_filled_qty = 0.0
    if long_res.ok:
        if long_res.filled_qty is not None:
            long_filled_qty = long_res.filled_qty
            long_filled_ok = long_filled_qty >= coin_amount - 1e-9
        elif long_res.order_id:
            long_filled_ok, long_filled_qty = await _check_filled_full(planned=long_plan, order_id=long_res.order_id)
    if short_res.ok:
        if short_res.filled_qty is not None:
            short_filled_qty = short_res.filled_qty
            short_filled_ok = short_filled_qty >= coin_amount - 1e-9
        elif short_res.order_id:
            short_filled_ok, short_filled_qty = await _check_filled_full(planned=short_plan, order_id=short_res.order_id)

    if long_res.ok and not long_filled_ok:
        logger.error(f"❌ Ордер не исполнен полностью: {long_exchange} long | исполнено={_format_number(long_filled_qty)} {coin} | требовалось={_format_number(coin_amount)} {coin}")
    if short_res.ok and not short_filled_ok:
        logger.error(f"❌ Ордер не исполнен полностью: {short_exchange} short | исполнено={_format_number(short_filled_qty)} {coin} | требовалось={_format_number(coin_amount)} {coin}")

    ok_all = bool(long_res.ok and short_res.ok and long_filled_ok and short_filled_ok)
    long_px: Optional[float] = None
    short_px: Optional[float] = None
    if ok_all:
        long_px = float(long_res.avg_price if long_res.avg_price is not None else long_plan.get("limit_price") or 0)
        short_px = float(short_res.avg_price if short_res.avg_price is not None else short_plan.get("limit_price") or 0)
        spread_open = None
        if long_px > 0:
            spread_open = (short_px - long_px) / long_px * 100.0
        spread_str = f"{spread_open:.3f}%" if spread_open is not None else "N/A"
        logger.info(
            f"Биржа лонг: {long_exchange}, Цена входа Long: {long_px:.8f}, "
            f"Биржа шорт: {short_exchange}, Цена входа Short: {short_px:.8f}, "
            f"Количество монет: {_format_number(coin_amount)}, Спред открытия: {spread_str}"
        )
        logger.info(f"✅ Позиции открыты: {coin} | Long {long_exchange} | Short {short_exchange}")
    else:
        if (long_res.ok and long_filled_ok) and not (short_res.ok and short_filled_ok):
            logger.error(f"⚠️ Открыта только Long позиция: {coin} | Long ok=True | Short ok=False")
        elif (short_res.ok and short_filled_ok) and not (long_res.ok and long_filled_ok):
            logger.error(f"⚠️ Открыта только Short позиция: {coin} | Long ok=False | Short ok=True")
        else:
            logger.error(f"❌ Не удалось открыть позиции: {coin} | Long ok={long_res.ok and long_filled_ok} | Short ok={short_res.ok and short_filled_ok}")
    return ok_all, long_px, short_px


async def close_long_short_positions(
    *,
    bot: Any,
    coin: str,
    long_exchange: str,
    short_exchange: str,
    coin_amount: float,
) -> bool:
    """
    Автоматическое закрытие Long/Short позиций (две ноги) на coin_amount монет.

    ВАЖНО (по требованию пользователя):
    - закрываем ЛИМИТНЫМИ ордерами
    - допускаем частичное исполнение
    - можем отправлять несколько ордеров, пока весь объем не закроется
    - пока реализовано только для Bybit и Gate
    """
    try:
        coin_amount_f = float(coin_amount)
    except Exception:
        logger.error(f"❌ Некорректное количество монет для закрытия: {coin_amount!r}")
        return False
    if coin_amount_f <= 0:
        logger.error(f"❌ Количество монет для закрытия должно быть > 0, получено: {coin_amount_f}")
        return False

    long_obj = (getattr(bot, "exchanges", {}) or {}).get(long_exchange)
    short_obj = (getattr(bot, "exchanges", {}) or {}).get(short_exchange)
    if long_obj is None or short_obj is None:
        logger.error(f"❌ Не найдены биржи в bot.exchanges для закрытия: long={long_exchange} short={short_exchange}")
        return False

    logger.warning(
        f"🧯 Авто-закрытие позиций: {coin} | Long {long_exchange} + Short {short_exchange} | qty={_format_number(coin_amount_f)} {coin}"
    )

    async def _close_one(exchange_name: str, exchange_obj: Any, position_direction: str) -> Tuple[bool, Optional[float]]:
        """
        Закрывает одну ногу позиции. Возвращает (успех, средняя_цена_исполнения).
        """
        ex = (exchange_name or "").lower().strip()
        if ex == "bybit":
            return await _bybit_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=coin, position_direction=position_direction, coin_amount=coin_amount_f)
        if ex == "gate":
            return await _gate_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=coin, position_direction=position_direction, coin_amount=coin_amount_f)
        if ex == "binance":
            return await _binance_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=coin, position_direction=position_direction, coin_amount=coin_amount_f)
        if ex == "bitget":
            return await _bitget_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=coin, position_direction=position_direction, coin_amount=coin_amount_f)
        if ex == "bingx":
            return await _bingx_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=coin, position_direction=position_direction, coin_amount=coin_amount_f)
        if ex == "xt":
            return await _xt_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=coin, position_direction=position_direction, coin_amount=coin_amount_f)
        logger.error(f"❌ Авто-закрытие пока не реализовано для биржи: {exchange_name}")
        return False, None

    # Закрываем обе ноги (параллельно)
    # long_exchange содержит LONG позицию, short_exchange содержит SHORT позицию
    long_task = _close_one(long_exchange, long_obj, "long")
    short_task = _close_one(short_exchange, short_obj, "short")
    long_result, short_result = await _gather2(long_task, short_task)

    long_ok = isinstance(long_result, tuple) and len(long_result) >= 1 and long_result[0] is True
    short_ok = isinstance(short_result, tuple) and len(short_result) >= 1 and short_result[0] is True
    long_price = long_result[1] if isinstance(long_result, tuple) and len(long_result) >= 2 else None
    short_price = short_result[1] if isinstance(short_result, tuple) and len(short_result) >= 2 else None

    ok_all = bool(long_ok and short_ok)
    if ok_all:
        # Рассчитываем спред закрытия: (bid_long - ask_short) / ask_short * 100
        # Для закрытия: Long закрывается по bid (продаем), Short закрывается по ask (покупаем)
        closing_spread = None
        if long_price is not None and short_price is not None and short_price > 0:
            closing_spread = ((long_price - short_price) / short_price) * 100.0

        closing_spread_str = _format_number(closing_spread) + "%" if closing_spread is not None else "N/A"
        long_price_str = f"{long_price:.8f}" if long_price is not None else "N/A"
        short_price_str = f"{short_price:.8f}" if short_price is not None else "N/A"

        logger.info(
            f"✅ Позиции закрыты: Цена выхода Long: {long_price_str}, Цена выхода Short: {short_price_str}, "
            f"Спред закрытия: {closing_spread_str}, Количество монет: {_format_number(coin_amount_f)}"
        )
    else:
        logger.error(f"❌ Не удалось закрыть обе позиции: {coin} | Long ok={bool(long_ok)} | Short ok={bool(short_ok)}")
    return ok_all


async def _bybit_get_position_size_one_leg(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    coin: str,
    position_direction: str,  # "short" or "long"
    position_idx: Optional[int] = None,
) -> float:
    """
    Возвращает size позиции (abs) по направлению (short/long) для одного инструмента.
    Важно:
    - В hedge-mode удобно фильтровать по positionIdx (1=long, 2=short),
      но unified/one-way иногда игнорирует/меняет pidx. Поэтому:
        1) пробуем с position_idx (если задан),
        2) если не нашли — делаем fallback только по side.
    """
    pos_dir = (position_direction or "").lower().strip()
    if pos_dir not in ("short", "long"):
        return 0.0

    symbol = exchange_obj._normalize_symbol(coin)
    data = await _bybit_private_request(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        method="GET",
        path="/v5/position/list",
        params={"category": "linear", "symbol": symbol},
    )

    if not (isinstance(data, dict) and data.get("retCode") == 0):
        return 0.0

    items = ((data.get("result") or {}).get("list") or [])
    if not isinstance(items, list) or not items:
        return 0.0

    want_side = "sell" if pos_dir == "short" else "buy"

    def _best_match(use_pidx: bool) -> float:
        best = 0.0
        for it in items:
            if not isinstance(it, dict):
                continue
            if use_pidx and position_idx is not None:
                try:
                    pidx = int(it.get("positionIdx")) if it.get("positionIdx") is not None else None
                except Exception:
                    pidx = None
                if pidx is None or pidx != int(position_idx):
                    continue
            side = str(it.get("side") or "").lower().strip()
            if side and side != want_side:
                continue
            try:
                sz = float(it.get("size") or 0.0)
            except Exception:
                sz = 0.0
            if sz > best:
                best = sz
        return float(abs(best))

    # 1) если дали position_idx — сначала по нему
    if position_idx is not None:
        v = _best_match(use_pidx=True)
        if v > 0:
            return v
    # 2) fallback без position_idx
    return _best_match(use_pidx=False)


async def _bybit_close_leg_partial_ioc(
    *,
    exchange_obj: Any,
    coin: str,
    position_direction: str,
    coin_amount: float,
    position_idx: Optional[int] = None,
    # --- WS fast-path (0-REST in critical window) ---
    ws_public: Any = None,
    ws_trade: Any = None,
    ws_private: Any = None,
    tick_raw: Optional[str] = None,
    qty_step_raw: Optional[str] = None,
    offset_ms: Optional[int] = None,
) -> Tuple[bool, Optional[float]]:
    """
    Закрытие позиции на Bybit частями: limit + IOC + reduceOnly.

    ИСПРАВЛЕНО под твою стратегию:
    - не делаем "remaining" по каждому уровню;
    - выбираем ОДНУ цену: минимальный уровень, где cum_volume >= remaining,
      и отправляем 1 IOC reduceOnly на остаток;
    - если частично исполнилось — повторяем с новым стаканом.

    position_direction:
      "long"  -> закрываем Sell (reduceOnly)
      "short" -> закрываем Buy  (reduceOnly)
    """
    api_key = _get_env("BYBIT_API_KEY")
    api_secret = _get_env("BYBIT_API_SECRET")
    if not api_key or not api_secret:
        logger.error("❌ Bybit: missing BYBIT_API_KEY/BYBIT_API_SECRET in env")
        return False, None

    pos_dir = (position_direction or "").lower().strip()
    if pos_dir not in ("long", "short"):
        logger.error(f"❌ Bybit: некорректное направление позиции: {position_direction!r}")
        return False, None

    remaining = float(coin_amount)
    if remaining <= 0:
        return True, None

    symbol = exchange_obj._normalize_symbol(coin)

    # -----------------------------
    # WS fast path (no HTTP orderbook / no HTTP order.create)
    # -----------------------------
    if ws_public is not None and ws_trade is not None and tick_raw and qty_step_raw:
        try:
            qty_step = float(qty_step_raw) if qty_step_raw else 0.0
        except Exception:
            qty_step = 0.0
        try:
            tick = float(tick_raw) if tick_raw else 0.0
        except Exception:
            tick = 0.0

        if tick <= 0 or qty_step <= 0:
            logger.error(f"❌ Bybit close(ws): invalid tick/qtyStep | tick={tick_raw!r} qtyStep={qty_step_raw!r}")
            return False, None

        side_close = "Sell" if pos_dir == "long" else "Buy"
        price_side = "sell" if side_close == "Sell" else "buy"
        use_position_idx: Optional[int] = position_idx

        eps = max(1e-10, remaining * 1e-8)
        total_notional = 0.0
        total_filled = 0.0

        # настройки эскалации (используем те же env, что и REST-ветка)
        ESCALATE_EVERY = max(1, int(os.getenv("FUN_CLOSE_ESCALATE_EVERY", "3") or "3"))
        ESCALATE_TICKS_STEP = max(0, int(os.getenv("FUN_CLOSE_ESCALATE_TICKS_STEP", "2") or "2"))
        ESCALATE_TICKS_MAX = max(0, int(os.getenv("FUN_CLOSE_ESCALATE_TICKS_MAX", "50") or "50"))
        MAX_TOTAL_SEC = float(os.getenv("FUN_CLOSE_MAX_TOTAL_SEC", "30.0") or "30.0")
        RESYNC_EVERY_N = int(os.getenv("FUN_CLOSE_RESYNC_EVERY", "5") or "5")
        RESYNC_ON_NOFILL = int(os.getenv("FUN_CLOSE_RESYNC_ON_NOFILL", "1") or "1")
        RESYNC_MIN_INTERVAL_SEC = float(os.getenv("FUN_CLOSE_RESYNC_MIN_INTERVAL", "0.5") or "0.5")
        CLOSE_FALLBACK_MARKET = int(os.getenv("FUN_CLOSE_FALLBACK_MARKET", "0") or "0")

        start_ts = time.time()
        last_resync_ts = 0.0

        no_fill_streak = 0
        escalate_ticks = 0

        # базовая "агрессия" (тики), чтобы IOC был marketable от best_bid/best_ask
        base_safety_ticks = max(1, int(os.getenv("FUN_CLOSE_WS_SAFETY_TICKS", "2") or "2"))

        def _extract_order_id_from_ack(ack: Any) -> Optional[str]:
            if not isinstance(ack, dict):
                return None
            for path in (
                ("result", "orderId"),
                ("data", "orderId"),
                ("data", "result", "orderId"),
                ("result", "list", 0, "orderId"),
                ("data", "list", 0, "orderId"),
            ):
                cur: Any = ack
                ok = True
                for key in path:
                    try:
                        if isinstance(key, int):
                            cur = cur[key]
                        else:
                            cur = cur.get(key)
                    except Exception:
                        ok = False
                        break
                    if cur is None:
                        ok = False
                        break
                if ok:
                    try:
                        s = str(cur)
                        if s:
                            return s
                    except Exception:
                        pass
            return None

        def _pos_side_for_dir() -> str:
            return "Buy" if pos_dir == "long" else "Sell"

        def _pos_size_ws() -> Optional[float]:
            if ws_private is None or not getattr(ws_private, "ready", False):
                return None
            side_pos = _pos_side_for_dir()
            # hedge first (positionIdx=1/2), then one-way fallback (0)
            if use_position_idx is not None:
                v = ws_private.get_position_size(symbol=str(symbol), position_idx=int(use_position_idx), side=side_pos)
                if v is not None:
                    return float(v)
            v0 = ws_private.get_position_size(symbol=str(symbol), position_idx=0, side=side_pos)
            if v0 is not None:
                return float(v0)
            return None

        max_orders_total = max(10, int(os.getenv("FUN_CLOSE_WS_MAX_ORDERS", "80") or "80"))

        for order_n in range(1, max_orders_total + 1):
            if time.time() - start_ts > MAX_TOTAL_SEC:
                logger.error(f"❌ Bybit close(ws): timeout {MAX_TOTAL_SEC}s exceeded | remaining={_format_number(remaining)} {coin}")
                avg_price = (total_notional / total_filled) if total_filled > 0 else None
                return False, avg_price

            # ресинк remaining по private WS (best-effort), чтобы не закрывать уже закрытое
            now_ts = time.time()
            should_resync = False
            if order_n % max(1, RESYNC_EVERY_N) == 0:
                should_resync = True
            elif RESYNC_ON_NOFILL == 1 and no_fill_streak > 0 and (now_ts - last_resync_ts >= RESYNC_MIN_INTERVAL_SEC):
                should_resync = True

            if should_resync:
                try:
                    pos_sz = _pos_size_ws()
                    if pos_sz is not None:
                        last_resync_ts = now_ts
                        if pos_sz <= eps:
                            avg_price = (total_notional / total_filled) if total_filled > 0 else None
                            logger.info("✅ Bybit close(ws): позиция уже 0 по Private WS position")
                            return True, avg_price
                        remaining = min(remaining, float(pos_sz))
                except Exception:
                    pass

            if remaining <= eps:
                avg_price = (total_notional / total_filled) if total_filled > 0 else None
                return True, avg_price

            snap = ws_public.snapshot() if hasattr(ws_public, "snapshot") else {}
            try:
                best_bid = float(snap.get("best_bid") or 0.0)
            except Exception:
                best_bid = 0.0
            try:
                best_ask = float(snap.get("best_ask") or 0.0)
            except Exception:
                best_ask = 0.0

            if best_bid <= 0 or best_ask <= 0:
                logger.warning(f"⚠️ Bybit close(ws): no bid/ask from Public WS | bid={best_bid} ask={best_ask}")
                await asyncio.sleep(0)
                continue

            # вычисляем marketable цену от best bid/ask + эскалация
            safety_ticks = int(base_safety_ticks) + int(escalate_ticks)
            if side_close == "Buy":
                px = best_ask + float(safety_ticks) * tick
                px = _round_price_for_side(px, tick, "buy")
            else:
                px = best_bid - float(safety_ticks) * tick
                px = _round_price_for_side(px, tick, "sell")

            qty_send = remaining
            qty_send = _floor_to_step(qty_send, qty_step)
            if qty_send <= 0:
                avg_price = (total_notional / total_filled) if total_filled > 0 else None
                return True, avg_price

            qty_str = _format_by_step(qty_send, qty_step_raw)
            px_str = _format_by_step(px, tick_raw)

            logger.info(
                f"Bybit close(ws): попытка {order_n}/{max_orders_total} | side={side_close} qty={qty_str} "
                f"| px={px_str} | remaining={_format_number(remaining)} | no_fill_streak={no_fill_streak} "
                f"| escalate_ticks={escalate_ticks}"
            )

            server_ts_ms = int(time.time() * 1000) + int(offset_ms or 0)
            order = {
                "category": "linear",
                "symbol": str(symbol),
                "side": side_close,
                "orderType": "Limit",
                "qty": qty_str,
                "price": px_str,
                "timeInForce": "IOC",
                "reduceOnly": True,
            }
            if use_position_idx is not None:
                order["positionIdx"] = int(use_position_idx)

            try:
                ack = await ws_trade.create_order(order=order, server_ts_ms=server_ts_ms, timeout_sec=1.5)
            except Exception as e:
                logger.error(f"❌ Bybit close(ws): order.create failed: {type(e).__name__}: {e}")
                return False, (total_notional / total_filled) if total_filled > 0 else None

            order_id = _extract_order_id_from_ack(ack)

            filled_now = 0.0
            avg_now: Optional[float] = None

            if order_id and ws_private is not None and getattr(ws_private, "ready", False):
                try:
                    final = await ws_private.wait_final(str(order_id), timeout=2.0)
                    filled_now = float(final.filled_qty or 0.0)
                    avg_now = final.avg_price
                except asyncio.TimeoutError:
                    # IOC может успеть "cancelled" без апдейта; ресинк по position поможет на следующем цикле
                    filled_now = 0.0
                    avg_now = None
                except Exception:
                    filled_now = 0.0
                    avg_now = None

            if filled_now > eps:
                no_fill_streak = 0
                try:
                    px_use = float(avg_now) if avg_now is not None else float(px)
                except Exception:
                    px_use = float(px)
                total_notional += float(filled_now) * float(px_use)
                total_filled += float(filled_now)
                remaining = max(0.0, float(remaining) - float(filled_now))
            else:
                no_fill_streak += 1
                if (no_fill_streak % ESCALATE_EVERY) == 0:
                    escalate_ticks = min(int(ESCALATE_TICKS_MAX), int(escalate_ticks) + int(ESCALATE_TICKS_STEP))

            if remaining <= eps:
                avg_price = (total_notional / total_filled) if total_filled > 0 else None
                return True, avg_price

            # fallback: market reduceOnly через Trade WS (без REST), если включено
            if CLOSE_FALLBACK_MARKET == 1 and (time.time() - start_ts) > max(0.5, float(MAX_TOTAL_SEC) * 0.7):
                try:
                    qty_m = _floor_to_step(remaining, qty_step)
                    if qty_m > 0:
                        qty_m_str = _format_by_step(qty_m, qty_step_raw)
                        order_m = {
                            "category": "linear",
                            "symbol": str(symbol),
                            "side": side_close,
                            "orderType": "Market",
                            "qty": qty_m_str,
                            "reduceOnly": True,
                        }
                        if use_position_idx is not None:
                            order_m["positionIdx"] = int(use_position_idx)
                        server_ts_ms2 = int(time.time() * 1000) + int(offset_ms or 0)
                        await ws_trade.create_order(order=order_m, server_ts_ms=server_ts_ms2, timeout_sec=2.0)
                except Exception:
                    pass

            await asyncio.sleep(0)

        avg_price = (total_notional / total_filled) if total_filled > 0 else None
        return False, avg_price

    # -----------------------------
    # REST path (legacy)
    # -----------------------------
    f = await _bybit_fetch_instrument_filters(exchange_obj=exchange_obj, symbol=symbol)

    qty_step_raw = f.get("qtyStep")
    tick_raw = f.get("tickSize")

    qty_step = float(qty_step_raw) if qty_step_raw else 0.0
    tick = float(tick_raw) if tick_raw else 0.0

    side_close = "Sell" if pos_dir == "long" else "Buy"
    price_side = "sell" if side_close == "Sell" else "buy"

    # Hedge-mode может требовать positionIdx, one-way — запрещает его
    use_position_idx: Optional[int] = position_idx

    eps = max(1e-10, remaining * 1e-8)

    # Best-effort VWAP (точнее считать по execPrice из executions, но пока достаточно)
    total_notional = 0.0
    total_filled = 0.0

    max_orders_total = max(10, MAX_ORDERBOOK_LEVELS * 3)

    # Эскалация цены для закрытия (если цена уходит вверх)
    no_fill_streak = 0
    escalate_ticks = 0

    # Настройки через env
    CLOSE_OB_LEVELS = max(10, int(os.getenv("FUN_CLOSE_OB_LEVELS", "25") or "25"))
    ESCALATE_EVERY = max(1, int(os.getenv("FUN_CLOSE_ESCALATE_EVERY", "3") or "3"))  # каждые N попыток без fill
    ESCALATE_TICKS_STEP = max(0, int(os.getenv("FUN_CLOSE_ESCALATE_TICKS_STEP", "2") or "2"))
    ESCALATE_TICKS_MAX = max(0, int(os.getenv("FUN_CLOSE_ESCALATE_TICKS_MAX", "50") or "50"))
    CLOSE_FALLBACK_MARKET = int(os.getenv("FUN_CLOSE_FALLBACK_MARKET", "0") or "0")

    # ===== NEW: параметры контроля "первые секунды" и ресинк позиции =====
    CLOSE_FIRST_SEC = float(os.getenv("FUN_CLOSE_FIRST_SEC", "2.0") or "2.0")
    RESYNC_EVERY_N = int(os.getenv("FUN_CLOSE_RESYNC_EVERY", "5") or "5")
    RESYNC_ON_NOFILL = int(os.getenv("FUN_CLOSE_RESYNC_ON_NOFILL", "1") or "1")  # 1 => ресинк при streak>0
    MAX_TOTAL_SEC = float(os.getenv("FUN_CLOSE_MAX_TOTAL_SEC", "30.0") or "30.0")  # safety, увеличен до 30с для стратегии "закрыть любой ценой"
    RESYNC_MIN_INTERVAL_SEC = float(os.getenv("FUN_CLOSE_RESYNC_MIN_INTERVAL", "0.5") or "0.5")  # минимальный интервал между ресинками (защита от rate limit)
    # ================================================================

    start_ts = time.time()  # NEW: для тайм-режима и общего лимита
    last_resync_ts = 0.0  # NEW: для ограничения частоты ресинка

    for order_n in range(1, max_orders_total + 1):
        # NEW: safety по времени (чтобы не зависать бесконечно)
        if time.time() - start_ts > MAX_TOTAL_SEC:
            logger.error(f"❌ Bybit close: timeout {MAX_TOTAL_SEC}s exceeded | remaining={_format_number(remaining)} {coin}")
            avg_price = (total_notional / total_filled) if total_filled > 0 else None
            return False, avg_price

        # NEW: периодический ресинк remaining по реальной позиции (с ограничением частоты для защиты от rate limit)
        now_ts = time.time()
        should_resync = False
        if order_n % RESYNC_EVERY_N == 0:
            should_resync = True
        elif RESYNC_ON_NOFILL == 1 and no_fill_streak > 0:
            # ресинк при no_fill_streak, но не чаще чем раз в RESYNC_MIN_INTERVAL_SEC
            if now_ts - last_resync_ts >= RESYNC_MIN_INTERVAL_SEC:
                should_resync = True

        if should_resync:
            try:
                pos_sz = await _bybit_get_position_size_one_leg(
                    exchange_obj=exchange_obj,
                    api_key=api_key,
                    api_secret=api_secret,
                    coin=coin,
                    position_direction=pos_dir,
                    position_idx=use_position_idx,  # если hedge-mode — поможет
                )
                last_resync_ts = now_ts
                # Если биржа говорит "0" — считаем закрытым
                if pos_sz <= eps:
                    avg_price = (total_notional / total_filled) if total_filled > 0 else None
                    logger.info(f"✅ Bybit close: позиция уже 0 по /position/list | closed")
                    return True, avg_price
                # Подстраховка: не увеличиваем remaining сверх исходного coin_amount (на случай лагов)
                remaining = min(remaining, float(pos_sz))
            except Exception:
                pass

        if remaining <= eps:
            avg_price = (total_notional / total_filled) if total_filled > 0 else None
            return True, avg_price

        ob = await exchange_obj.get_orderbook(coin, limit=CLOSE_OB_LEVELS)
        if not ob or not ob.get("bids") or not ob.get("asks"):
            logger.error(f"❌ Bybit: orderbook недоступен для закрытия {coin}")
            return False, None

        levels = ob["bids"] if side_close == "Sell" else ob["asks"]
        if not isinstance(levels, list) or not levels:
            logger.error(f"❌ Bybit: пустой стакан для закрытия {coin}")
            return False, None

        # 1) Подбираем цену уровня, где cum >= remaining
        px_level, cum = _price_level_for_target_size(levels[:CLOSE_OB_LEVELS], remaining)
        if px_level is None:
            # если даже в CLOSE_OB_LEVELS не нашли cum>=remaining — берём последний доступный
            try:
                px_level = float(levels[min(len(levels), CLOSE_OB_LEVELS) - 1][0])
            except Exception:
                logger.error(f"❌ Bybit: не удалось взять последний уровень стакана для закрытия {coin}")
                return False, None

        # 2) Кол-во: НИКОГДА не округлять вверх (иначе reduceOnly может упасть / частично открыть обратную сторону)
        qty_send = remaining
        if qty_step > 0:
            qty_send = _floor_to_step(qty_send, qty_step)
            if qty_send <= 0:
                # остаток меньше шага — считаем позицию закрытой best-effort
                avg_price = (total_notional / total_filled) if total_filled > 0 else None
                return True, avg_price

        qty_str = _format_by_step(qty_send, qty_step_raw) if qty_step > 0 else str(qty_send)

        # 3) Цена: агрессивно округляем по tick
        px = _round_price_for_side(float(px_level), tick, price_side)

        # ===== NEW: тайм-режим "первые секунды" vs "после" =====
        elapsed = time.time() - start_ts
        if side_close == "Buy" and tick > 0:
            if elapsed <= CLOSE_FIRST_SEC:
                # первые секунды: эскалацию можно держать мягче (или вообще 0)
                # (оставляем как есть: px + escalate_ticks*tick, но escalation растёт медленнее ниже)
                if escalate_ticks > 0:
                    px = px + float(escalate_ticks) * tick
                    px = _round_price_for_side(px, tick, "buy")
            else:
                # после окна: делаем агрессивнее, даже если escalation маленький
                # минимальный "пинок" + уже накопленные тики
                bump = max(1, escalate_ticks)
                px = px + float(bump) * tick
                px = _round_price_for_side(px, tick, "buy")
        # =====================================================

        px_str = _format_by_step(px, tick_raw) if tick > 0 else str(px)

        logger.info(
            f"Bybit close: попытка {order_n}/{max_orders_total} | side={side_close} qty={qty_str} | "
            f"лимит={px_str} | remaining={_format_number(remaining)} | cum={_format_number(cum)} | "
            f"no_fill_streak={no_fill_streak} | escalate_ticks={escalate_ticks} | elapsed={elapsed:.2f}s"
        )

        body = {
            "category": "linear",
            "symbol": symbol,
            "side": side_close,
            "orderType": "Limit",
            "qty": qty_str,
            "price": px_str,
            "timeInForce": "IOC",
            "reduceOnly": True,
        }
        if use_position_idx is not None:
            body["positionIdx"] = int(use_position_idx)

        data = await _bybit_private_post(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            path="/v5/order/create",
            body=body,
        )

        if not isinstance(data, dict) or data.get("retCode") != 0:
            # retCode=110017: position is zero — уже закрыто (не ретраим, сразу успех)
            if isinstance(data, dict) and data.get("retCode") == 110017:
                avg_price = (total_notional / total_filled) if total_filled > 0 else None
                return True, avg_price

            # retCode=10001: one-way mode, positionIdx нельзя, пробуем без него
            if isinstance(data, dict) and data.get("retCode") == 10001 and use_position_idx is not None:
                use_position_idx = None
                body.pop("positionIdx", None)
                data = await _bybit_private_post(
                    exchange_obj=exchange_obj,
                    api_key=api_key,
                    api_secret=api_secret,
                    path="/v5/order/create",
                    body=body,
                )

            if not isinstance(data, dict) or data.get("retCode") != 0:
                # еще раз проверим 110017 после ретрая
                if isinstance(data, dict) and data.get("retCode") == 110017:
                    avg_price = (total_notional / total_filled) if total_filled > 0 else None
                    return True, avg_price
                logger.error(f"❌ Bybit close: api error: {data}")
                return False, None

        order_id = (data.get("result") or {}).get("orderId") if isinstance(data.get("result"), dict) else None
        if not order_id:
            logger.error(f"❌ Bybit close: no orderId in response: {data}")
            return False, None

        ok_full, filled = await _bybit_wait_full_fill(
            planned={"exchange_obj": exchange_obj, "api_key": api_key, "api_secret": api_secret, "symbol": symbol, "qty": qty_str},
            order_id=str(order_id),
        )

        if filled and float(filled) > 0:
            filled_q = float(filled)
            total_notional += filled_q * float(px)   # best-effort
            total_filled += filled_q
            remaining = max(0.0, remaining - filled_q)
            no_fill_streak = 0  # NEW: сбрасываем streak

            logger.info(
                f"Bybit close: исполнено={_format_number(filled_q)} {coin} | осталось={_format_number(remaining)} {coin} | full={ok_full}"
            )
            continue

        # ===== NEW: логика no-fill, эскалация быстрее после окна =====
        no_fill_streak += 1
        elapsed = time.time() - start_ts

        if elapsed <= CLOSE_FIRST_SEC:
            # первые секунды: эскалируем как раньше (мягко)
            if no_fill_streak % ESCALATE_EVERY == 0 and ESCALATE_TICKS_STEP > 0:
                escalate_ticks = min(ESCALATE_TICKS_MAX, escalate_ticks + ESCALATE_TICKS_STEP)
                logger.info(f"Bybit close: эскалация цены | escalate_ticks={escalate_ticks} (после {no_fill_streak} попыток без fill)")
        else:
            # после окна: эскалируем агрессивнее (в 2 раза чаще и/или больше шаг)
            if ESCALATE_TICKS_STEP > 0:
                step2 = max(ESCALATE_TICKS_STEP, ESCALATE_TICKS_STEP * 2)
                every2 = max(1, ESCALATE_EVERY // 2)
                if no_fill_streak % every2 == 0:
                    escalate_ticks = min(ESCALATE_TICKS_MAX, escalate_ticks + step2)
                    logger.info(f"Bybit close: агрессивная эскалация | escalate_ticks={escalate_ticks} (после {no_fill_streak} попыток, elapsed={elapsed:.2f}s)")

        logger.warning(f"Bybit close: 0 исполнено (IOC) | осталось={_format_number(remaining)} {coin}")

        # NEW: market fallback можно включать раньше после окна (если нужно)
        if CLOSE_FALLBACK_MARKET == 1:
            # первые секунды — не трогаем market, после окна — можно
            if elapsed > CLOSE_FIRST_SEC and no_fill_streak >= max(3, ESCALATE_EVERY * 2):
                logger.warning(f"Bybit close: fallback на Market ордер (после {no_fill_streak} попыток, elapsed={elapsed:.2f}s)")
                body_m = {
                    "category": "linear",
                    "symbol": symbol,
                    "side": side_close,
                    "orderType": "Market",
                    "qty": qty_str,
                    "timeInForce": "IOC",
                    "reduceOnly": True,
                }
                if use_position_idx is not None:
                    body_m["positionIdx"] = int(use_position_idx)

                data_m = await _bybit_private_post(
                    exchange_obj=exchange_obj,
                    api_key=api_key,
                    api_secret=api_secret,
                    path="/v5/order/create",
                    body=body_m,
                )

                # retCode=110017: position is zero — уже закрыто (не ретраим)
                if isinstance(data_m, dict) and data_m.get("retCode") == 110017:
                    avg_price = (total_notional / total_filled) if total_filled > 0 else None
                    return True, avg_price

                # retCode=10001: one-way mode, positionIdx нельзя, пробуем без него
                if isinstance(data_m, dict) and data_m.get("retCode") == 10001 and use_position_idx is not None:
                    use_position_idx = None
                    body_m.pop("positionIdx", None)
                    data_m = await _bybit_private_post(
                        exchange_obj=exchange_obj,
                        api_key=api_key,
                        api_secret=api_secret,
                        path="/v5/order/create",
                        body=body_m,
                    )

                if not (isinstance(data_m, dict) and data_m.get("retCode") == 0):
                    # еще раз проверим 110017 после ретрая
                    if isinstance(data_m, dict) and data_m.get("retCode") == 110017:
                        avg_price = (total_notional / total_filled) if total_filled > 0 else None
                        return True, avg_price
                    logger.error(f"❌ Bybit close market fallback error: {data_m}")
                else:
                    oid_m = (data_m.get("result") or {}).get("orderId") if isinstance(data_m.get("result"), dict) else None
                    if oid_m:
                        ok_m, filled_m = await _bybit_wait_full_fill(
                            planned={"exchange_obj": exchange_obj, "api_key": api_key, "api_secret": api_secret, "symbol": symbol, "qty": qty_str},
                            order_id=str(oid_m),
                        )
                        if filled_m and float(filled_m) > 0:
                            fq = float(filled_m)
                            # цену market тут не знаем точно — VWAP лучше считать по executions позже
                            remaining = max(0.0, remaining - fq)
                            no_fill_streak = 0
                            logger.warning(f"⚠️ Market fallback filled={_format_number(fq)} | remaining={_format_number(remaining)}")
                            continue
        # ==========================================================

        continue

    logger.error(f"❌ Bybit close: не удалось закрыть позицию полностью | осталось={_format_number(remaining)} {coin}")
    avg_price = (total_notional / total_filled) if total_filled > 0 else None
    return False, avg_price


async def _gate_wait_done_get_filled_contracts(*, planned: Dict[str, Any], order_id: str) -> Tuple[bool, int]:
    """
    Gate: ждём завершения IOC-ордера и возвращаем количество исполненных контрактов (int).
    """
    api_key = planned["api_key"]
    api_secret = planned["api_secret"]
    exchange_obj = planned["exchange_obj"]
    contract = planned["contract"]
    size_abs_req = int(abs(int(planned.get("size") or 0)))

    import asyncio
    for _ in range(20):
        data = await _gate_private_request(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            method="GET",
            path=f"/api/v4/futures/usdt/orders/{order_id}",
            params={"contract": contract},
        )
        if isinstance(data, dict) and data.get("_error"):
            return False, 0
        if not isinstance(data, dict):
            await asyncio.sleep(0.2)
            continue

        status = str(data.get("status") or "")
        try:
            left = int(float(data.get("left") or 0))
        except Exception:
            left = 0
        try:
            size_abs = int(abs(int(float(data.get("size") or 0))))
        except Exception:
            size_abs = size_abs_req

        filled_contracts = max(0, size_abs - max(0, left))

        if status.lower() in ("finished", "cancelled", "canceled"):
            logger.info(
                f"Gate close: статус ордера {order_id}: {status} | исполнено_контрактов={filled_contracts} | требовалось_контрактов={size_abs_req}"
            )
            return True, filled_contracts

        await asyncio.sleep(0.2)

    return False, 0


async def _gate_close_leg_partial_ioc(*, exchange_obj: Any, coin: str, position_direction: str, coin_amount: float) -> Tuple[bool, Optional[float]]:
    """
    Закрытие позиции на Gate частями: limit + IOC.
    position_direction: "long" (закрываем Sell => size отрицательный) или "short" (закрываем Buy => size положительный).
    """
    api_key = _get_env("GATEIO_API_KEY")
    api_secret = _get_env("GATEIO_API_SECRET")
    if not api_key or not api_secret:
        logger.error("❌ Gate: missing GATEIO_API_KEY/GATEIO_API_SECRET in env")
        return False, None

    pos_dir = (position_direction or "").lower().strip()
    if pos_dir not in ("long", "short"):
        logger.error(f"❌ Gate: некорректное направление позиции: {position_direction!r}")
        return False, None

    contract = exchange_obj._normalize_symbol(coin)
    cinfo = await _gate_fetch_contract_info(exchange_obj=exchange_obj, contract=contract)
    if not isinstance(cinfo, dict):
        logger.error(f"❌ Gate: contract info not available for {contract}")
        return False, None

    qmul_raw = cinfo.get("quanto_multiplier") or cinfo.get("contract_size") or cinfo.get("multiplier")
    try:
        qmul = float(qmul_raw)
    except Exception:
        qmul = 0.0
    if qmul <= 0:
        logger.error(f"❌ Gate: bad quanto_multiplier for {contract}: {qmul_raw}")
        return False, None

    contracts_exact = float(coin_amount) / qmul
    contracts_total = int(round(contracts_exact))
    if abs(contracts_exact - contracts_total) > 1e-9 or contracts_total <= 0:
        logger.error(f"❌ Gate close: qty {coin_amount} {coin} not compatible with contract size (qmul={qmul}) => contracts={contracts_exact:.8f} (must be integer)")
        return False, None

    price_step = _gate_price_step_from_contract_info(cinfo) or 0.0
    min_raw = cinfo.get("order_size_min")
    try:
        min_size = int(float(min_raw)) if min_raw is not None else None
    except Exception:
        min_size = None

    # Для long позиции: закрытие = sell => size отрицательный. Для short: закрытие = buy => size положительный.
    sign = -1 if pos_dir == "long" else 1
    remaining_contracts = int(contracts_total)
    max_orders_total = max(10, MAX_ORDERBOOK_LEVELS * 3)

    # Отслеживаем среднюю цену исполнения (VWAP)
    total_notional = 0.0
    total_filled_base = 0.0

    for order_n in range(1, max_orders_total + 1):
        if remaining_contracts <= 0:
            avg_price = total_notional / total_filled_base if total_filled_base > 0 else None
            return True, avg_price

        ob = await exchange_obj.get_orderbook(coin, limit=MAX_ORDERBOOK_LEVELS)
        if not ob or not ob.get("bids") or not ob.get("asks"):
            logger.error(f"❌ Gate: orderbook недоступен для закрытия {coin}")
            return False, None

        side = "buy" if pos_dir == "short" else "sell"
        levels = ob["asks"] if side == "buy" else ob["bids"]

        filled_any = 0
        for lvl_i, lvl in enumerate(levels[:MAX_ORDERBOOK_LEVELS], start=1):
            try:
                px_raw = float(lvl[0])
            except Exception:
                continue
            if px_raw <= 0:
                continue

            px = _round_price_for_side(px_raw, price_step, side)
            px_str = _format_by_step(px, str(price_step) if price_step > 0 else None)
            size_signed = int(sign * remaining_contracts)

            logger.info(f"Gate close: ордер {order_n}/{max_orders_total} | lvl {lvl_i}/{MAX_ORDERBOOK_LEVELS} | side={side} size={size_signed} | лимит={px_str}")

            body = {
                "contract": contract,
                "size": size_signed,
                "price": px_str,
                "tif": "ioc",
                # Критично для закрытия, особенно если включён hedge/dual режим:
                # reduce_only гарантирует, что ордер НЕ откроет/увеличит позицию, а только уменьшит существующую.
                "reduce_only": True,
            }
            data = await _gate_private_post(exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret, path="/api/v4/futures/usdt/orders", body=body)
            if not (isinstance(data, dict) and data.get("id") is not None and ("label" not in data) and ("message" not in data)):
                # Частая причина остатков при закрытии — минимальный размер ордера.
                if min_size is not None and remaining_contracts < min_size:
                    logger.error(
                        f"❌ Gate close: api error (возможная причина: остаток меньше min_size) | "
                        f"остаток_контрактов={remaining_contracts} min_size={min_size} | resp={data}"
                    )
                else:
                    logger.error(f"❌ Gate close: api error: {data}")
                return False, None

            order_id = str(data.get("id"))
            done, filled_contracts = await _gate_wait_done_get_filled_contracts(
                planned={"exchange_obj": exchange_obj, "api_key": api_key, "api_secret": api_secret, "contract": contract, "size": size_signed},
                order_id=order_id,
            )
            if not done:
                logger.warning(f"Gate close: не удалось подтвердить завершение ордера {order_id}, продолжаем")
            if filled_contracts > 0:
                filled_any = int(filled_contracts)
                # Обновляем VWAP: конвертируем контракты в базовую монету и добавляем notional
                filled_base = filled_any * qmul
                total_notional += filled_base * px
                total_filled_base += filled_base
                remaining_contracts = max(0, remaining_contracts - filled_any)
                logger.info(f"Gate close: исполнено_контрактов={filled_any} | осталось_контрактов={remaining_contracts}")
                break

        if filled_any <= 0:
            logger.warning(f"Gate close: 0 исполнено по уровням 1-{MAX_ORDERBOOK_LEVELS} | осталось_контрактов={remaining_contracts}")
            continue

    logger.error(f"❌ Gate close: не удалось закрыть позицию полностью | осталось_контрактов={remaining_contracts}")
    avg_price = total_notional / total_filled_base if total_filled_base > 0 else None
    return False, avg_price


async def _binance_close_leg_partial_ioc(*, exchange_obj: Any, coin: str, position_direction: str, coin_amount: float) -> Tuple[bool, Optional[float]]:
    """
    Binance USDT-M Futures: закрытие позиции частями лимитными IOC ордерами.
    position_direction: "long" (закрываем SELL) или "short" (закрываем BUY).
    """
    api_key = _get_env("BINANCE_API_KEY")
    api_secret = _get_env("BINANCE_API_SECRET")
    if not api_key or not api_secret:
        logger.error("❌ Binance: missing BINANCE_API_KEY/BINANCE_API_SECRET in env")
        return False, None

    pos_dir = (position_direction or "").lower().strip()
    if pos_dir not in ("long", "short"):
        logger.error(f"❌ Binance: некорректное направление позиции: {position_direction!r}")
        return False, None

    symbol = exchange_obj._normalize_symbol(coin)
    f = await _binance_get_symbol_filters(exchange_obj=exchange_obj, symbol=symbol)
    tick_raw = f.get("tickSize")
    step_raw = f.get("stepSize")
    tick = float(tick_raw) if tick_raw else 0.0
    step = float(step_raw) if step_raw else 0.0

    side_close = "SELL" if pos_dir == "long" else "BUY"
    # Для hedge-mode: positionSide может требоваться, для one-way — запрещаться.
    pos_side = "LONG" if pos_dir == "long" else "SHORT"

    remaining = float(coin_amount)
    eps = max(1e-10, remaining * 1e-8)

    total_notional = 0.0
    total_filled = 0.0

    max_orders_total = max(10, MAX_ORDERBOOK_LEVELS * 3)
    for order_n in range(1, max_orders_total + 1):
        if remaining <= eps:
            avg_price = total_notional / total_filled if total_filled > 0 else None
            return True, avg_price

        ob = await exchange_obj.get_orderbook(coin, limit=MAX_ORDERBOOK_LEVELS)
        if not ob or not ob.get("bids") or not ob.get("asks"):
            logger.error(f"❌ Binance: orderbook недоступен для закрытия {coin}")
            return False, None

        levels = ob["bids"] if side_close == "SELL" else ob["asks"]
        filled_any = 0.0

        for lvl_i, lvl in enumerate(levels[:MAX_ORDERBOOK_LEVELS], start=1):
            try:
                px_raw = float(lvl[0])
            except Exception:
                continue
            if px_raw <= 0:
                continue

            px = _round_price_for_side(px_raw, tick, "sell" if side_close == "SELL" else "buy")
            px_str = _format_by_step(px, tick_raw)

            qty_to_send = remaining
            if step > 0:
                qty_str = _format_by_step(qty_to_send, step_raw)
            else:
                qty_str = str(qty_to_send)

            logger.debug(f"Binance close: ордер {order_n}/{max_orders_total} | lvl {lvl_i}/{MAX_ORDERBOOK_LEVELS} | side={side_close} qty={qty_str} | лимит={px_str}")

            def _post(params: Dict[str, Any]) -> Any:
                return _binance_private_request(
                    exchange_obj=exchange_obj,
                    api_key=api_key,
                    api_secret=api_secret,
                    method="POST",
                    path="/fapi/v1/order",
                    params=params,
                )

            base_params = {
                "symbol": symbol,
                "side": side_close,
                "type": "LIMIT",
                "timeInForce": "IOC",
                "quantity": qty_str,
                "price": px_str,
                "reduceOnly": "true",
            }
            base_params_no_reduce = {k: v for k, v in base_params.items() if k != "reduceOnly"}

            # 1) пробуем с positionSide (hedge)
            data = await _post({**base_params, "positionSide": pos_side})
            if isinstance(data, dict) and (data.get("_error") or data.get("code") is not None):
                code = data.get("code")
                msg = str(data.get("msg") or data.get("_body") or "")
                msg_l = msg.lower()

                # Binance: в некоторых режимах параметр reduceOnly запрещён ("not required")
                if code == -1106 and "reduceonly" in msg_l:
                    logger.debug("Binance close: reduceOnly не поддержан в текущем режиме — повторяем без reduceOnly")
                    data = await _post({**base_params_no_reduce, "positionSide": pos_side})
                    # если positionSide тоже не подходит — ниже отработает fallback
                    if isinstance(data, dict) and (data.get("_error") or data.get("code") is not None):
                        code2 = data.get("code")
                        msg2 = str(data.get("msg") or data.get("_body") or "")
                        if code2 == -4061 or "position side" in msg2.lower():
                            data = await _post(base_params_no_reduce)

                # position side mismatch — пробуем без positionSide (one-way)
                elif code == -4061 or "position side" in msg_l:
                    data = await _post(base_params)
                    if isinstance(data, dict) and (data.get("_error") or data.get("code") is not None):
                        code2 = data.get("code")
                        msg2 = str(data.get("msg") or data.get("_body") or "")
                        if code2 == -1106 and "reduceonly" in msg2.lower():
                            logger.debug("Binance close: reduceOnly не поддержан в текущем режиме — повторяем без reduceOnly")
                            data = await _post(base_params_no_reduce)

            if not isinstance(data, dict):
                logger.error(f"❌ Binance close: api error: {data}")
                return False, None

            if data.get("_error") or data.get("code") is not None:
                logger.error(f"❌ Binance close: api error: {data}")
                return False, None

            order_id = str(data.get("orderId") or "")
            if not order_id:
                logger.error(f"❌ Binance close: no orderId in response: {data}")
                return False, None

            # Для IOC: ордер завершится быстро (FILLED / CANCELED).
            ok_full, executed = await _binance_wait_full_fill(
                planned={"exchange_obj": exchange_obj, "api_key": api_key, "api_secret": api_secret, "symbol": symbol, "qty": qty_str},
                order_id=order_id,
            )
            if executed and executed > 0:
                filled_any = float(executed)
                # Best-effort avgPrice from API (если есть)
                avg_px = None
                try:
                    avg_px = float(data.get("avgPrice")) if data.get("avgPrice") is not None else None
                except Exception:
                    avg_px = None
                use_px = avg_px if (avg_px is not None and avg_px > 0) else px
                total_notional += filled_any * float(use_px)
                total_filled += filled_any
                remaining = max(0.0, remaining - filled_any)
                logger.info(f"Binance close: исполнено={_format_number(filled_any)} {coin} | осталось={_format_number(remaining)} {coin} | full={ok_full}")
                break

        if filled_any <= 0:
            logger.debug(f"Binance close: 0 исполнено по уровням 1-{MAX_ORDERBOOK_LEVELS} | осталось={_format_number(remaining)} {coin}")
            continue

    logger.error(f"❌ Binance close: не удалось закрыть позицию полностью | осталось={_format_number(remaining)} {coin}")
    avg_price = total_notional / total_filled if total_filled > 0 else None
    return False, avg_price


async def _bitget_wait_done_get_filled_qty(*, planned: Dict[str, Any], order_id: str) -> Tuple[bool, float]:
    """
    Bitget: ждём завершения IOC-ордера и возвращаем исполненное количество (base qty).
    """
    exchange_obj = planned["exchange_obj"]
    api_key = planned["api_key"]
    api_secret = planned["api_secret"]
    api_pass = planned["api_passphrase"]
    symbol = planned["symbol"]
    product_type = planned["productType"]

    import asyncio
    for _ in range(20):
        data = await _bitget_private_request(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_pass,
            method="GET",
            path="/api/v2/mix/order/detail",
            params={"symbol": symbol, "productType": product_type, "orderId": order_id},
        )
        if isinstance(data, dict) and data.get("_error"):
            # try v1 fallback
            data = await _bitget_private_request(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_pass,
                method="GET",
                path="/api/mix/v1/order/detail",
                params={"symbol": symbol, "orderId": order_id, "marginCoin": "USDT"},
            )

        code, _msg = _bitget_extract_code_msg(data)
        if code is not None and code != "00000":
            return False, 0.0

        item = None
        if isinstance(data, dict):
            item = data.get("data") or data.get("result") or data
        if not isinstance(item, dict):
            await asyncio.sleep(0.2)
            continue

        status = str(item.get("state") or item.get("status") or "")
        filled_raw = item.get("filledQty") or item.get("fillSz") or item.get("filledSize") or item.get("dealSize") or item.get("baseVolume") or item.get("accBaseVolume")
        try:
            filled = float(filled_raw) if filled_raw is not None else 0.0
        except Exception:
            filled = 0.0

        if status.lower() in ("filled", "full_fill", "complete", "completed", "success", "closed", "canceled", "cancelled", "rejected"):
            logger.debug(f"Bitget close: статус ордера {order_id}: {status} | исполнено={_format_number(filled)}")
            return True, filled

        await asyncio.sleep(0.2)

    return False, 0.0


async def _bitget_close_leg_partial_ioc(*, exchange_obj: Any, coin: str, position_direction: str, coin_amount: float) -> Tuple[bool, Optional[float]]:
    """
    Bitget USDT-M Futures: закрытие позиции частями лимитными IOC ордерами.
    position_direction: "long" (закрываем Sell) или "short" (закрываем Buy).
    """
    api_key = _get_env("BITGET_API_KEY")
    api_secret = _get_env("BITGET_API_SECRET")
    api_pass = os.getenv("BITGET_API_PASSPHRASE", "").strip()
    if not api_key or not api_secret:
        logger.error("❌ Bitget: missing BITGET_API_KEY/BITGET_API_SECRET in env")
        return False, None
    if not api_pass:
        logger.error("❌ Bitget: missing BITGET_API_PASSPHRASE in env")
        return False, None

    pos_dir = (position_direction or "").lower().strip()
    if pos_dir not in ("long", "short"):
        logger.error(f"❌ Bitget: некорректное направление позиции: {position_direction!r}")
        return False, None

    symbol = exchange_obj._normalize_symbol(coin)
    f = await _bitget_get_contract_filters(exchange_obj=exchange_obj, symbol=symbol)
    tick_raw = f.get("tickSize")
    step_raw = f.get("stepSize")
    product_type = f.get("productType") or getattr(exchange_obj, "PRODUCT_TYPE", "USDT-FUTURES")
    tick = float(tick_raw) if tick_raw else 0.0
    step = float(step_raw) if step_raw else 0.0

    # marginMode обязателен; по умолчанию isolated
    bitget_margin_mode = (os.getenv("BITGET_MARGIN_MODE", "isolated") or "").strip().lower()
    if not bitget_margin_mode:
        bitget_margin_mode = "isolated"
    if bitget_margin_mode == "cross":
        bitget_margin_mode = "crossed"
    if bitget_margin_mode not in ("isolated", "crossed"):
        bitget_margin_mode = "isolated"

    # Для закрытия:
    # - long позиция закрывается sell / close_long
    # - short позиция закрывается buy / close_short
    if pos_dir == "long":
        side_close_open_style = "close_long"
        side_buy_sell = "sell"
        pos_side = "long"
        book_side_name = "bids"
    else:
        side_close_open_style = "close_short"
        side_buy_sell = "buy"
        pos_side = "short"
        book_side_name = "asks"

    # Pre-check: если позиции нет — закрывать нечего (успех).
    pos_resp, pos_path = await _bitget_fetch_positions_best_effort(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_pass,
        symbol=symbol,
        product_type=str(product_type),
        margin_coin="USDT",
    )
    if isinstance(pos_resp, dict):
        pos_list = _bitget_extract_positions_list(pos_resp)
        pos_qty, _pos_mode = _bitget_parse_position_qty_and_mode(positions=pos_list, symbol=symbol, hold_side=pos_side)
        if pos_qty is not None and pos_qty <= 0:
            logger.info(f"Bitget close: позиции нет (по position API {pos_path or 'unknown'}) — закрывать нечего")
            return True, None

    remaining = float(coin_amount)
    eps = max(1e-10, remaining * 1e-8)

    total_notional = 0.0
    total_filled = 0.0

    max_orders_total = max(10, MAX_ORDERBOOK_LEVELS * 3)
    for order_n in range(1, max_orders_total + 1):
        if remaining <= eps:
            avg_price = total_notional / total_filled if total_filled > 0 else None
            return True, avg_price

        ob = await exchange_obj.get_orderbook(coin, limit=MAX_ORDERBOOK_LEVELS)
        if not ob or not ob.get("bids") or not ob.get("asks"):
            logger.error(f"❌ Bitget: orderbook недоступен для закрытия {coin}")
            return False, None

        levels = ob.get(book_side_name) or []
        filled_any = 0.0

        for lvl_i, lvl in enumerate(levels[:MAX_ORDERBOOK_LEVELS], start=1):
            try:
                px_raw = float(lvl[0])
            except Exception:
                continue
            if px_raw <= 0:
                continue

            px = _round_price_for_side(px_raw, tick, "sell" if pos_dir == "long" else "buy")
            px_str = _format_by_step(px, tick_raw)

            qty_to_send = remaining
            if step > 0:
                qty_str = _format_by_step(qty_to_send, step_raw)
            else:
                qty_str = str(qty_to_send)

            logger.debug(f"Bitget close: ордер {order_n}/{max_orders_total} | lvl {lvl_i}/{MAX_ORDERBOOK_LEVELS} | qty={qty_str} | лимит={px_str}")

            base_body = {
                "symbol": symbol,
                "productType": str(product_type),
                "marginCoin": "USDT",
                "marginMode": bitget_margin_mode,
                "orderType": "limit",
                "price": px_str,
                "size": qty_str,
                "force": "ioc",
                "clientOid": f"arb-close-{int(time.time()*1000)}-{pos_dir}-{order_n}-{lvl_i}",
            }

            # Bitget: закрытие может требовать "обратный" side (встречалось на практике),
            # поэтому пробуем side_buy_sell и alt_side. Также оставляем несколько fallback-схем
            # для разных режимов аккаунта (hedge/unilateral).
            alt_side = "buy" if side_buy_sell == "sell" else "sell"
            candidate_bodies: List[Dict[str, Any]] = [
                # Primary: hedge-mode (часто работает)
                {**base_body, "side": side_buy_sell, "holdSide": pos_side, "posSide": pos_side},
                {**base_body, "side": alt_side, "holdSide": pos_side, "posSide": pos_side},
                # Fallback: tradeSide=close (на некоторых аккаунтах требуется)
                {**base_body, "side": side_buy_sell, "tradeSide": "close", "holdSide": pos_side, "posSide": pos_side},
                {**base_body, "side": alt_side, "tradeSide": "close", "holdSide": pos_side, "posSide": pos_side},
                # Fallback: упрощённые варианты (иногда API требует меньше полей)
                {**base_body, "side": side_buy_sell, "holdSide": pos_side},
                {**base_body, "side": alt_side, "holdSide": pos_side},
                {**base_body, "side": side_buy_sell, "posSide": pos_side},
                {**base_body, "side": alt_side, "posSide": pos_side},
            ]

            data: Any = None
            ok_created = False
            for body in candidate_bodies:
                data = await _bitget_private_request(
                    exchange_obj=exchange_obj,
                    api_key=api_key,
                    api_secret=api_secret,
                    api_passphrase=api_pass,
                    method="POST",
                    path="/api/v2/mix/order/place-order",
                    body=body,
                )
                code, msg = _bitget_extract_code_msg(data)
                if code is not None and code != "00000":
                    msg_l = (msg or "").lower()
                    if "side mismatch" in msg_l or "unilateral" in msg_l or str(code) in ("400172", "40774"):
                        continue
                    if str(code) == "22002" or "no position" in msg_l:
                        continue
                    if "reduceonly" in msg_l and str(code) in ("40017", "400017"):
                        continue
                    if msg and ("ioc" in msg_l or "fill" in msg_l or "immediately" in msg_l):
                        break
                    logger.error(f"❌ Bitget close: api error: {data}")
                    return False, None
                ok_created = True
                break

            if not ok_created:
                continue

            item = data.get("data") if isinstance(data, dict) else None
            order_id = None
            if isinstance(item, dict):
                order_id = item.get("orderId") or item.get("id")
            if not order_id and isinstance(data, dict):
                order_id = data.get("orderId") or data.get("id")
            if not order_id:
                logger.error(f"❌ Bitget close: no orderId in response: {data}")
                return False, None

            order_id_s = str(order_id)
            _done, filled = await _bitget_wait_done_get_filled_qty(
                planned={"exchange_obj": exchange_obj, "api_key": api_key, "api_secret": api_secret, "api_passphrase": api_pass, "symbol": symbol, "productType": str(product_type)},
                order_id=order_id_s,
            )
            if filled and filled > 0:
                filled_any = float(filled)
                total_notional += filled_any * px
                total_filled += filled_any
                remaining = max(0.0, remaining - filled_any)
                logger.info(f"Bitget close: исполнено={_format_number(filled_any)} {coin} | осталось={_format_number(remaining)} {coin}")
                break

        if filled_any <= 0:
            logger.debug(f"Bitget close: 0 исполнено по уровням 1-{MAX_ORDERBOOK_LEVELS} | осталось={_format_number(remaining)} {coin}")
            continue

    logger.error(f"❌ Bitget close: не удалось закрыть позицию полностью | осталось={_format_number(remaining)} {coin}")
    avg_price = total_notional / total_filled if total_filled > 0 else None
    # Final verification: если позиция реально уже закрыта (вручную/внешне) — вернем успех.
    pos_resp2, pos_path2 = await _bitget_fetch_positions_best_effort(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_pass,
        symbol=symbol,
        product_type=str(product_type),
        margin_coin="USDT",
    )
    if isinstance(pos_resp2, dict):
        pos_list2 = _bitget_extract_positions_list(pos_resp2)
        pos_qty2, _pos_mode2 = _bitget_parse_position_qty_and_mode(positions=pos_list2, symbol=symbol, hold_side=pos_side)
        if pos_qty2 is not None and pos_qty2 <= eps:
            logger.debug(f"Bitget close: позиция уже закрыта по position API {pos_path2 or 'unknown'} — считаем успехом")
            return True, avg_price
    return False, avg_price


async def _gather2(t1, t2):
    import asyncio
    r = await asyncio.gather(t1, t2, return_exceptions=True)
    out = []
    for it in r:
        if isinstance(it, Exception):
            out.append(it)
        else:
            out.append(it)
    # noqa: returning tuple
    return out[0], out[1]


def _as_open_leg_result(res: Any, *, exchange: str, direction: str) -> OpenLegResult:
    """
    asyncio.gather(..., return_exceptions=True) может вернуть Exception вместо OpenLegResult.
    Нормализуем в OpenLegResult, чтобы дальше код не падал.
    """
    if isinstance(res, OpenLegResult):
        return res
    if isinstance(res, Exception):
        import traceback
        tb = "".join(traceback.format_exception(type(res), res, res.__traceback__))
        logger.error(f"❌ Исключение в ноге {exchange} {direction}: {type(res).__name__}: {res}\n{tb}")
        return OpenLegResult(exchange=exchange, direction=direction, ok=False, error=f"exception: {type(res).__name__}: {res}")
    logger.error(f"❌ Некорректный результат ноги {exchange} {direction}: type={type(res).__name__} value={res!r}")
    return OpenLegResult(exchange=exchange, direction=direction, ok=False, error=f"bad result type: {type(res).__name__}")


def _log_leg_result(res: OpenLegResult) -> None:
    if getattr(res, "ok", False):
        logger.info(f"✅ Ордер выставлен: {res.exchange} {res.direction}")
        return
    msg = getattr(res, "error", None) or "unknown error"
    ex = getattr(res, "exchange", "unknown")
    d = getattr(res, "direction", "unknown")
    logger.error(f"❌ Ошибка открытия: {ex} {d} | {msg}")


async def _plan_one_leg(
    *,
    exchange_name: str,
    exchange_obj: Any,
    coin: str,
    direction: str,
    coin_amount: float,
) -> Any:
    ex = exchange_name.lower()
    if ex == "bybit":
        return await _bybit_plan_leg(exchange_obj=exchange_obj, coin=coin, direction=direction, coin_amount=coin_amount)
    if ex == "gate":
        return await _gate_plan_leg(exchange_obj=exchange_obj, coin=coin, direction=direction, coin_amount=coin_amount)
    if ex == "binance":
        return await _binance_plan_leg(exchange_obj=exchange_obj, coin=coin, direction=direction, coin_amount=coin_amount)
    if ex == "mexc":
        return await _mexc_plan_leg(exchange_obj=exchange_obj, coin=coin, direction=direction, coin_amount=coin_amount)
    if ex == "bitget":
        return await _bitget_plan_leg(exchange_obj=exchange_obj, coin=coin, direction=direction, coin_amount=coin_amount)
    if ex == "bingx":
        return await _bingx_plan_leg(exchange_obj=exchange_obj, coin=coin, direction=direction, coin_amount=coin_amount)
    if ex == "xt":
        return await _xt_plan_leg(exchange_obj=exchange_obj, coin=coin, direction=direction, coin_amount=coin_amount)
    return OpenLegResult(exchange=exchange_name, direction=direction, ok=False, error="trading not implemented for this exchange")


async def _place_one_leg(*, planned: Dict[str, Any]) -> OpenLegResult:
    ex = str(planned.get("exchange") or "").lower()
    if ex == "bybit":
        return await _bybit_place_leg(planned=planned)
    if ex == "gate":
        return await _gate_place_leg(planned=planned)
    if ex == "binance":
        return await _binance_place_leg(planned=planned)
    if ex == "mexc":
        return await _mexc_place_leg(planned=planned)
    if ex == "bitget":
        return await _bitget_place_leg(planned=planned)
    if ex == "bingx":
        return await _bingx_place_leg(planned=planned)
    if ex == "xt":
        return await _xt_place_leg(planned=planned)
    return OpenLegResult(exchange=str(planned.get("exchange")), direction=str(planned.get("direction")), ok=False, error="unknown exchange in plan")


async def _prepare_exchange_for_trading(*, exchange_name: str, exchange_obj: Any, coin: str) -> bool:
    """
    Настройка параметров торговли перед ордерами: isolated margin + leverage=1.
    """
    ex = exchange_name.lower()
    if ex == "bybit":
        symbol = exchange_obj._normalize_symbol(coin)
        ok, msg = await _bybit_switch_isolated_and_leverage_1(exchange_obj=exchange_obj, symbol=symbol)
        if not ok:
            # IMPORTANT: do not block trading if Bybit Unified forbids this endpoint.
            # retCode=100028: 'unified account is forbidden'
            m = str(msg)
            if "retCode': 100028" in m or "retCode\": 100028" in m or "unified account is forbidden" in m.lower():
                logger.warning(f"⚠️ Bybit: пропускаем настройку isolated/leverage=1 для {symbol} (unified account запретил endpoint)")
                return True
            logger.error(f"❌ Bybit: не удалось выставить isolated/leverage=1 для {symbol}: {msg}")
            return False
        return True
    if ex == "gate":
        contract = exchange_obj._normalize_symbol(coin)
        ok, msg = await _gate_set_isolated_and_leverage_1(exchange_obj=exchange_obj, contract=contract)
        if not ok:
            logger.error(f"❌ Gate: не удалось выставить isolated/leverage=1 для {contract}: {msg}")
            return False
        return True
    logger.error(f"❌ Trading preparation not implemented for {exchange_name}")
    return False


async def _check_filled_full(*, planned: Dict[str, Any], order_id: str) -> Tuple[bool, float]:
    """
    Проверяет, что ордер исполнен полностью. Возвращает (ok_full_fill, filled_qty_base).
    """
    ex = str(planned.get("exchange") or "").lower()
    if ex == "bybit":
        return await _bybit_wait_full_fill(planned=planned, order_id=order_id)
    if ex == "gate":
        return await _gate_wait_full_fill(planned=planned, order_id=order_id)
    if ex == "binance":
        return await _binance_wait_full_fill(planned=planned, order_id=order_id)
    if ex == "mexc":
        return await _mexc_wait_full_fill(planned=planned, order_id=order_id)
    if ex == "bitget":
        return await _bitget_wait_full_fill(planned=planned, order_id=order_id)
    if ex == "bingx":
        return await _bingx_wait_full_fill(planned=planned, order_id=order_id)
    return False, 0.0


async def _bybit_private_request(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
) -> Any:
    """
    Bybit v5 signing:
    sign = HMAC_SHA256(secret, timestamp + api_key + recv_window + (queryString|bodyJson))
    For GET: queryString is sorted by key and URL-encoded.
    """
    recv_window = str(int(float(os.getenv("BYBIT_RECV_WINDOW", "5000"))))
    ts = str(int(time.time() * 1000))
    method_u = method.upper()
    payload = ""
    req_kwargs: Dict[str, Any] = {}
    if method_u == "GET":
        p = params or {}
        # Важно: строка подписи должна совпадать с реальным querystring 1:1,
        # поэтому используем отсортированный список tuple и передаем его в httpx как params=list[tuple].
        from urllib.parse import urlencode
        pairs = sorted([(k, str(v)) for k, v in p.items() if v is not None], key=lambda kv: kv[0])
        payload = urlencode(pairs, doseq=True)
        req_kwargs["params"] = pairs
    else:
        body_json = json.dumps(body or {}, separators=(",", ":"), ensure_ascii=False)
        payload = body_json
        req_kwargs["content"] = body_json

    sign_payload = f"{ts}{api_key}{recv_window}{payload}"
    sign = hmac.new(api_secret.encode("utf-8"), sign_payload.encode("utf-8"), hashlib.sha256).hexdigest()
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv_window,
        "X-BAPI-SIGN": sign,
        "X-BAPI-SIGN-TYPE": "2",
        "Content-Type": "application/json",
    }
    try:
        resp = await exchange_obj.client.request(method_u, path, headers=headers, **req_kwargs)
    except Exception as e:
        return {"_error": f"http error: {type(e).__name__}: {e}"}
    if resp.status_code < 200 or resp.status_code >= 300:
        return {"_error": f"http {resp.status_code}", "_body": resp.text[:400]}
    try:
        return resp.json()
    except Exception:
        return {"_error": "bad json", "_body": resp.text[:400]}


async def _bybit_wait_full_fill(*, planned: Dict[str, Any], order_id: str) -> Tuple[bool, float]:
    """
    Возвращает (is_full, filled_qty).
    Почему так:
    - /v5/order/realtime иногда "не видит" уже завершённые ордера.
    - Для IOC нормальная ситуация: частично исполнилось и ордер стал Cancelled.
      Это НЕ ошибка — filled_qty просто будет < qty_req.
    """
    api_key = planned["api_key"]
    api_secret = planned["api_secret"]
    exchange_obj = planned["exchange_obj"]
    symbol = planned["symbol"]
    try:
        qty_req = float(planned["qty"])
    except Exception:
        qty_req = 0.0
    eps = max(1e-10, abs(qty_req) * 1e-8)
    poll_sleep = float(os.getenv("PO_BYBIT_FILL_POLL_SEC", "0.2") or "0.2")
    max_polls = int(os.getenv("PO_BYBIT_FILL_MAX_POLLS", "30") or "30")  # 30 * 0.2 = ~6s

    import asyncio
    last_seen: Optional[Dict[str, Any]] = None
    last_err: Optional[str] = None

    async def _fetch_one(path: str) -> Optional[Dict[str, Any]]:
        nonlocal last_err
        data = await _bybit_private_request(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            method="GET",
            path=path,
            params={"category": "linear", "symbol": symbol, "orderId": str(order_id)},
        )
        if isinstance(data, dict) and data.get("_error"):
            last_err = str(data)
            return None
        if not (isinstance(data, dict) and data.get("retCode") == 0):
            if isinstance(data, dict):
                last_err = f"{path} retCode={data.get('retCode')} retMsg={data.get('retMsg')}"
            return None
        items = ((data.get("result") or {}).get("list") or [])
        item = items[0] if items and isinstance(items[0], dict) else None
        return item

    def _parse_filled(item: Dict[str, Any]) -> float:
        v = item.get("cumExecQty")
        try:
            return float(v) if v is not None else 0.0
        except Exception:
            return 0.0

    def _parse_status(item: Dict[str, Any]) -> str:
        s = str(item.get("orderStatus") or "").strip()
        return s

    terminal = {
        "filled",
        "cancelled", "canceled",
        "rejected",
        "deactivated",
        "partiallyfilledcanceled",  # встречается у некоторых шлюзов
    }

    for _ in range(max_polls):
        # realtime
        item_rt = await _fetch_one("/v5/order/realtime")
        if item_rt:
            last_seen = item_rt
        # history (важнее)
        item_h = await _fetch_one("/v5/order/history")
        if item_h:
            last_seen = item_h

        if last_seen:
            status = _parse_status(last_seen)
            filled = _parse_filled(last_seen)
            st_low = status.lower().replace("_", "").replace(" ", "")

            # Filled — успех, Cancelled/Rej — терминал, но не обязательно full
            if st_low in terminal or st_low == "partiallyfilled":
                # Для IOC: PartiallyFilled может очень быстро перейти в Cancelled.
                # Поэтому считаем "full" только по qty.
                is_full = (filled + eps) >= qty_req if qty_req > 0 else (filled > 0)
                return is_full, float(filled)

        await asyncio.sleep(max(0.05, poll_sleep))

    tail = f" | last_error={last_err}" if last_err else ""
    # Если ничего не нашли — считаем 0 исполнено
    return False, 0.0


async def _gate_private_request(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
) -> Any:
    """
    Gate v4 signing:
    SIGN = HMAC_SHA512(secret, method + '\n' + path + '\n' + query + '\n' + sha512(body) + '\n' + timestamp)
    For GET: body hash is sha512('').
    """
    method_u = method.upper()
    from urllib.parse import urlencode
    query = urlencode(sorted([(k, str(v)) for k, v in (params or {}).items() if v is not None]), doseq=True)
    body_json = json.dumps(body or {}, separators=(",", ":"), ensure_ascii=False) if method_u != "GET" else ""
    payload_hash = hashlib.sha512(body_json.encode("utf-8")).hexdigest()
    ts = str(int(time.time()))
    sign_str = "\n".join([method_u, path, query, payload_hash, ts])
    sign = hmac.new(api_secret.encode("utf-8"), sign_str.encode("utf-8"), hashlib.sha512).hexdigest()
    headers = {"KEY": api_key, "Timestamp": ts, "SIGN": sign, "Accept": "application/json"}
    if method_u != "GET":
        headers["Content-Type"] = "application/json"
    try:
        resp = await exchange_obj.client.request(method_u, path, headers=headers, params=(params or None), content=(body_json if body_json else None))
    except Exception as e:
        return {"_error": f"http error: {type(e).__name__}: {e}"}
    if resp.status_code < 200 or resp.status_code >= 300:
        return {"_error": f"http {resp.status_code}", "_body": resp.text[:400]}
    try:
        return resp.json()
    except Exception:
        return {"_error": "bad json", "_body": resp.text[:400]}


async def _gate_wait_full_fill(*, planned: Dict[str, Any], order_id: str) -> Tuple[bool, float]:
    api_key = planned["api_key"]
    api_secret = planned["api_secret"]
    exchange_obj = planned["exchange_obj"]
    contract = planned["contract"]
    # For futures: size is contracts, but we need base filled amount. We'll approximate via contracts * quanto_multiplier.
    cinfo = await _gate_fetch_contract_info(exchange_obj=exchange_obj, contract=contract)
    qmul = None
    try:
        qmul = float((cinfo or {}).get("quanto_multiplier"))
    except Exception:
        qmul = None
    if not qmul or qmul <= 0:
        qmul = 1.0

    qty_req_contracts = abs(float(planned.get("size", 0)))
    qty_req_base = qty_req_contracts * qmul
    eps = max(1e-10, qty_req_base * 1e-8)

    import asyncio
    for _ in range(20):
        data = await _gate_private_request(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            method="GET",
            path=f"/api/v4/futures/usdt/orders/{order_id}",
            params={"contract": contract},
        )
        if isinstance(data, dict) and data.get("_error"):
            return False, 0.0
        if not isinstance(data, dict):
            await asyncio.sleep(0.2)
            continue
        status = str(data.get("status") or "")
        finish_as = str(data.get("finish_as") or "")
        # Gate futures: left is remaining contracts; size is original signed contracts
        try:
            left = float(data.get("left") or 0)
        except Exception:
            left = 0.0
        try:
            size_abs = abs(float(data.get("size") or 0))
        except Exception:
            size_abs = 0.0
        filled_contracts = max(0.0, size_abs - left)
        filled_base = filled_contracts * qmul

        if status.lower() in ("finished", "cancelled", "canceled"):
            ok_full = (filled_contracts + 1e-9 >= qty_req_contracts)
            logger.info(
                f"Gate: статус ордера {order_id}: {status}"
                + (f"/{finish_as}" if finish_as else "")
                + f" | исполнено_контрактов={_format_number(filled_contracts)} | требовалось_контрактов={_format_number(qty_req_contracts)}"
            )
            return ok_full, filled_base
        await asyncio.sleep(0.2)
    return False, 0.0


# =========================
# Binance Futures (USDT-M) trading (FOK + 3 levels)
# =========================

_BINANCE_EXCHANGE_INFO_CACHE: Dict[str, Dict[str, Any]] = {}


async def _binance_get_symbol_filters(*, exchange_obj: Any, symbol: str) -> Dict[str, Optional[str]]:
    """
    Возвращает фильтры Binance Futures для symbol:
    tickSize, stepSize, minQty, minNotional.
    """
    if symbol in _BINANCE_EXCHANGE_INFO_CACHE:
        return _BINANCE_EXCHANGE_INFO_CACHE[symbol]
    try:
        data = await exchange_obj._request_json("GET", "/fapi/v1/exchangeInfo", params={"symbol": symbol})
        if not isinstance(data, dict):
            return {}
        syms = data.get("symbols") or []
        # Найдем нужный символ (Binance может вернуть все символы даже с параметром symbol)
        item = None
        for s in syms:
            if isinstance(s, dict) and s.get("symbol") == symbol:
                item = s
                break
        if not item:
            # Fallback: если не нашли, возьмем первый (для обратной совместимости)
            item = syms[0] if syms and isinstance(syms[0], dict) else None
        if not item:
            logger.warning(f"Binance: symbol {symbol} not found in exchangeInfo")
            return {}
        # Проверка: убедимся, что нашли правильный символ
        if item.get("symbol") != symbol:
            logger.warning(f"Binance: expected symbol {symbol}, got {item.get('symbol')} in exchangeInfo")
        filters = item.get("filters") or []
        out: Dict[str, Optional[str]] = {}
        for f in filters:
            if not isinstance(f, dict):
                continue
            ft = f.get("filterType")
            if ft == "PRICE_FILTER":
                out["tickSize"] = str(f.get("tickSize")) if f.get("tickSize") is not None else None
            elif ft == "LOT_SIZE":
                out["stepSize"] = str(f.get("stepSize")) if f.get("stepSize") is not None else None
                out["minQty"] = str(f.get("minQty")) if f.get("minQty") is not None else None
            elif ft == "MIN_NOTIONAL":
                notional_val = f.get("notional") or f.get("minNotional")
                out["minNotional"] = str(notional_val) if notional_val is not None else None
                if notional_val is not None:
                    logger.debug(f"Binance {symbol}: MIN_NOTIONAL = {notional_val}")
        _BINANCE_EXCHANGE_INFO_CACHE[symbol] = out
        return out
    except Exception:
        return {}


async def _binance_plan_leg(*, exchange_obj: Any, coin: str, direction: str, coin_amount: float) -> Any:
    api_key = _get_env("BINANCE_API_KEY")
    api_secret = _get_env("BINANCE_API_SECRET")
    if not api_key or not api_secret:
        return OpenLegResult(exchange="binance", direction=direction, ok=False, error="missing BINANCE_API_KEY/BINANCE_API_SECRET in env")

    symbol = exchange_obj._normalize_symbol(coin)
    ob = await exchange_obj.get_orderbook(coin, limit=MAX_ORDERBOOK_LEVELS)
    if not ob or not ob.get("bids") or not ob.get("asks"):
        return OpenLegResult(exchange="binance", direction=direction, ok=False, error=f"orderbook not available for {coin}")
    side = "BUY" if direction == "long" else "SELL"
    book_side = ob["asks"] if side == "BUY" else ob["bids"]
    best_price = float(book_side[0][0])
    if best_price <= 0:
        return OpenLegResult(exchange="binance", direction=direction, ok=False, error="bad orderbook best price")

    f = await _binance_get_symbol_filters(exchange_obj=exchange_obj, symbol=symbol)
    tick_raw = f.get("tickSize")
    step_raw = f.get("stepSize")
    min_qty_raw = f.get("minQty")
    min_notional_raw = f.get("minNotional")
    tick = float(tick_raw) if tick_raw else 0.0
    step = float(step_raw) if step_raw else 0.0
    min_qty = float(min_qty_raw) if min_qty_raw else 0.0
    min_notional = float(min_notional_raw) if min_notional_raw else 0.0
    if step > 0 and not _is_multiple_of_step(coin_amount, step):
        return OpenLegResult(exchange="binance", direction=direction, ok=False, error=f"qty {coin_amount} not multiple of stepSize {step_raw}")
    if min_qty > 0 and coin_amount < min_qty:
        return OpenLegResult(exchange="binance", direction=direction, ok=False, error=f"qty {coin_amount} < minQty {min_qty_raw}")
    notional = coin_amount * best_price
    if min_notional > 0 and notional < min_notional:
        min_qty_needed = min_notional / best_price if best_price > 0 else 0
        return OpenLegResult(
            exchange="binance", direction=direction, ok=False,
            error=f"минимальная номинальная стоимость ордера {min_notional_raw} USDT > запрошенная {_format_number(notional)} USDT. Минимум монет: ~{_format_number(min_qty_needed)} {coin}",
        )
    qty_str = _format_by_step(coin_amount, step_raw)
    logger.info(f"План Binance: {direction} qty={qty_str} | best={_format_number(best_price)} | уровни 1..{MAX_ORDERBOOK_LEVELS} (IOC)")

    return {
        "exchange": "binance",
        "direction": direction,
        "exchange_obj": exchange_obj,
        "symbol": symbol,
        "side": side,
        "coin": coin,
        "coin_amount": coin_amount,
        "qty": qty_str,
        "api_key": api_key,
        "api_secret": api_secret,
        "tickSize": tick_raw,
        "stepSize": step_raw,
    }


async def _binance_private_request(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    method: str,
    path: str,
    params: Dict[str, Any],
) -> Any:
    from urllib.parse import urlencode
    ts = str(int(time.time() * 1000))
    recv_window = str(int(float(os.getenv("BINANCE_RECV_WINDOW", "5000"))))
    base_pairs = [(k, str(v)) for k, v in params.items() if v is not None]
    base_pairs.append(("timestamp", ts))
    base_pairs.append(("recvWindow", recv_window))
    base_pairs = sorted(base_pairs, key=lambda kv: kv[0])
    query = urlencode(base_pairs, doseq=True)
    sign = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    pairs = base_pairs + [("signature", sign)]
    headers = {"X-MBX-APIKEY": api_key}
    try:
        resp = await exchange_obj.client.request(method.upper(), path, params=pairs, headers=headers)
    except Exception as e:
        return {"_error": f"http error: {type(e).__name__}: {e}"}
    if resp.status_code < 200 or resp.status_code >= 300:
        body_text = (resp.text or "")[:400]
        # Binance almost always returns JSON even on HTTP 4xx.
        try:
            j = resp.json()
            if isinstance(j, dict):
                return {
                    "_error": f"http {resp.status_code}",
                    "code": j.get("code"),
                    "msg": j.get("msg"),
                    "_body": body_text,
                }
        except Exception:
            pass
        return {"_error": f"http {resp.status_code}", "_body": body_text}
    try:
        return resp.json()
    except Exception:
        return {"_error": "bad json", "_body": resp.text[:400]}


async def get_binance_fees_from_trades(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    coin: str,
    direction: str,
    start_ms: int,
    end_ms: int,
) -> Optional[float]:
    """
    Сумма комиссий в USDT по сделкам Binance Futures за период [start_ms, end_ms].
    direction: "long" → только BUY, "short" → только SELL.
    Возвращает None при ошибке API или если комиссий в USDT нет.
    """
    symbol = exchange_obj._normalize_symbol(coin)
    want_side = "BUY" if (direction or "").lower().strip() == "long" else "SELL"
    data = await _binance_private_request(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        method="GET",
        path="/fapi/v1/userTrades",
        params={"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 1000},
    )
    if isinstance(data, dict) and data.get("_error"):
        logger.warning(
            "Binance userTrades: ошибка запроса | %s",
            data.get("_error") or data.get("msg") or data.get("code") or str(data)[:200],
        )
        return None
    if not isinstance(data, list):
        logger.warning("Binance userTrades: неожиданный ответ (не список)")
        return None
    total = 0.0
    for t in data:
        if not isinstance(t, dict):
            continue
        if str(t.get("side") or "").upper() != want_side:
            continue
        if str(t.get("commissionAsset") or "").upper() != "USDT":
            continue
        try:
            total += abs(float(t.get("commission") or 0))
        except (TypeError, ValueError):
            pass
    return total if total > 0 else None


async def get_binance_funding_from_income(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    coin: str,
    start_ms: int,
    end_ms: int,
) -> Optional[float]:
    """
    Сумма фандинга (USDT) из Binance Futures Income за период [start_ms, end_ms].
    GET /fapi/v1/income, incomeType=FUNDING_FEE. Положительное = получено, отрицательное = уплачено.
    Возвращает None при ошибке API.
    """
    symbol = exchange_obj._normalize_symbol(coin)
    data = await _binance_private_request(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        method="GET",
        path="/fapi/v1/income",
        params={
            "symbol": symbol,
            "incomeType": "FUNDING_FEE",
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 1000,
        },
    )
    if isinstance(data, dict) and data.get("_error"):
        logger.warning(
            "Binance income (FUNDING_FEE): ошибка запроса | %s",
            data.get("_error") or data.get("msg") or data.get("code") or str(data)[:200],
        )
        return None
    if not isinstance(data, list):
        logger.warning("Binance income: неожиданный ответ (не список)")
        return None
    total = 0.0
    for it in data:
        if not isinstance(it, dict):
            continue
        if str(it.get("incomeType") or "").upper() != "FUNDING_FEE":
            continue
        if str(it.get("asset") or "").upper() != "USDT":
            continue
        sym = str(it.get("symbol") or "").upper()
        if sym and sym != symbol.upper():
            continue
        try:
            total += float(it.get("income") or 0)
        except (TypeError, ValueError):
            pass
    return total


async def _binance_wait_full_fill(*, planned: Dict[str, Any], order_id: str) -> Tuple[bool, float]:
    exchange_obj = planned["exchange_obj"]
    api_key = planned["api_key"]
    api_secret = planned["api_secret"]
    symbol = planned["symbol"]
    qty_req = float(planned["qty"])
    import asyncio
    for _ in range(20):
        data = await _binance_private_request(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            method="GET",
            path="/fapi/v1/order",
            params={"symbol": symbol, "orderId": order_id},
        )
        if isinstance(data, dict) and data.get("_error"):
            return False, 0.0
        if not isinstance(data, dict):
            await asyncio.sleep(0.2)
            continue
        status = str(data.get("status") or "")
        try:
            executed = float(data.get("executedQty") or 0.0)
        except Exception:
            executed = 0.0
        if status.upper() in ("FILLED", "CANCELED", "CANCELLED", "EXPIRED", "REJECTED"):
            logger.debug(f"Binance: статус ордера {order_id}: {status} | исполнено={_format_number(executed)} | требовалось={_format_number(qty_req)}")
            return (executed + 1e-10 >= qty_req), executed
        await asyncio.sleep(0.2)
    return False, 0.0


async def get_gate_fees_from_trades(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    coin: str,
    direction: str,
    start_ms: int,
    end_ms: int,
) -> Optional[float]:
    """
    Сумма комиссий в USDT по сделкам Gate.io Futures за период [start_ms, end_ms].
    direction: "long" → только buy trades, "short" → только sell trades.
    Возвращает None при ошибке API или если комиссий в USDT нет.
    """
    contract = exchange_obj._normalize_symbol(coin)
    want_side = 1 if (direction or "").lower().strip() == "long" else -1  # Gate: 1=buy, -1=sell
    
    data = await _gate_private_request(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        method="GET",
        path="/api/v4/futures/usdt/trades",
        params={
            "contract": contract,
            "from": start_ms // 1000,  # Gate использует секунды
            "to": end_ms // 1000,
            "limit": 1000,
        },
    )
    
    if isinstance(data, dict) and data.get("_error"):
        logger.warning(
            "Gate trades: ошибка запроса | %s",
            data.get("_error") or str(data)[:200],
        )
        return None
    
    if not isinstance(data, list):
        logger.warning("Gate trades: неожиданный ответ (не список)")
        return None
    
    total = 0.0
    for t in data:
        if not isinstance(t, dict):
            continue
        # Gate: size положительный для buy, отрицательный для sell
        size_raw = t.get("size") or t.get("contracts") or 0
        try:
            size = float(size_raw)
        except Exception:
            continue
        # Проверяем направление: size > 0 для buy (long), size < 0 для sell (short)
        if (want_side == 1 and size <= 0) or (want_side == -1 and size >= 0):
            continue
        
        # Комиссия в Gate может быть в поле "fee" или "fee_value"
        fee_raw = t.get("fee") or t.get("fee_value") or t.get("fee_usdt")
        if fee_raw is None:
            continue
        
        try:
            fee_val = abs(float(fee_raw))
        except Exception:
            continue
        
        # Проверяем, что комиссия в USDT (Gate обычно возвращает в USDT для USDT-M)
        fee_currency = str(t.get("fee_currency") or t.get("fee_asset") or "USDT").upper()
        if fee_currency in ("USDT", "USDC"):
            total += fee_val
    
    return total if total > 0 else None


async def get_gate_funding_from_settlements(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    coin: str,
    start_ms: int,
    end_ms: int,
) -> Optional[float]:
    """
    Сумма фандинга (USDT) из Gate.io Futures Settlements за период [start_ms, end_ms].
    Положительное = получено, отрицательное = уплачено.
    Возвращает None при ошибке API.
    """
    contract = exchange_obj._normalize_symbol(coin)
    
    data = await _gate_private_request(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        method="GET",
        path="/api/v4/futures/usdt/settlements",
        params={
            "contract": contract,
            "from": start_ms // 1000,  # Gate использует секунды
            "to": end_ms // 1000,
            "limit": 1000,
        },
    )
    
    if isinstance(data, dict) and data.get("_error"):
        logger.warning(
            "Gate settlements (funding): ошибка запроса | %s",
            data.get("_error") or str(data)[:200],
        )
        return None
    
    if not isinstance(data, list):
        logger.warning("Gate settlements: неожиданный ответ (не список)")
        return None
    
    total = 0.0
    for s in data:
        if not isinstance(s, dict):
            continue
        # Gate settlements содержит funding в поле "funding" или "funding_rate" * "size"
        funding_raw = s.get("funding") or s.get("funding_fee") or s.get("funding_value")
        if funding_raw is None:
            # Попробуем вычислить из funding_rate и size
            rate_raw = s.get("funding_rate")
            size_raw = s.get("size") or s.get("contracts")
            if rate_raw is not None and size_raw is not None:
                try:
                    rate = float(rate_raw)
                    size = abs(float(size_raw))
                    # Для Gate: funding = rate * size (в базовой валюте, обычно USDT)
                    funding_raw = rate * size
                except Exception:
                    continue
        
        if funding_raw is None:
            continue
        
        try:
            funding_val = float(funding_raw)
        except Exception:
            continue
        
        total += funding_val
    
    return total if total != 0.0 else None


async def _binance_place_leg(*, planned: Dict[str, Any]) -> OpenLegResult:
    exchange_obj = planned["exchange_obj"]
    api_key = planned["api_key"]
    api_secret = planned["api_secret"]
    direction = planned["direction"]
    symbol = planned["symbol"]
    side = planned["side"]
    coin = planned["coin"]
    coin_amount = float(planned["coin_amount"])
    tick_raw = planned.get("tickSize")
    step_raw = planned.get("stepSize")
    tick = float(tick_raw) if tick_raw else 0.0

    total_filled = 0.0
    total_notional = 0.0
    last_order_id: Optional[str] = None

    for level_idx in range(1, MAX_ORDERBOOK_LEVELS + 1):
        remaining = coin_amount - total_filled
        if remaining <= 0:
            break
        ob = await exchange_obj.get_orderbook(coin, limit=MAX_ORDERBOOK_LEVELS)
        if not ob or not ob.get("bids") or not ob.get("asks"):
            break
        book_side = ob["asks"] if side == "BUY" else ob["bids"]
        if level_idx > len(book_side):
            break
        try:
            price_raw = float(book_side[level_idx - 1][0])
        except (IndexError, TypeError, ValueError):
            break
        px = _round_price_for_side(price_raw, tick, "buy" if side == "BUY" else "sell")
        px_str = _format_by_step(px, tick_raw)
        qty_str = _format_by_step(remaining, step_raw)
        if float(qty_str) <= 0:
            break
        logger.info(f"Binance: уровень {level_idx}/{MAX_ORDERBOOK_LEVELS} | {direction} qty={qty_str} | лимит={px_str} | осталось={_format_number(remaining)}")
        data = await _binance_private_request(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            method="POST",
            path="/fapi/v1/order",
            params={
                "symbol": symbol,
                "side": side,
                "type": "LIMIT",
                "timeInForce": "IOC",
                "quantity": qty_str,
                "price": px_str,
            },
        )
        if not isinstance(data, dict):
            return OpenLegResult(exchange="binance", direction=direction, ok=False, error=f"api error: {data}", raw=data)
        if data.get("code") is not None or data.get("_error"):
            return OpenLegResult(exchange="binance", direction=direction, ok=False, error=f"api error: {data}", raw=data)
        order_id = str(data.get("orderId") or "")
        if not order_id:
            return OpenLegResult(exchange="binance", direction=direction, ok=False, error=f"no orderId in response: {data}", raw=data)
        last_order_id = order_id
        ok_full, executed = await _binance_wait_full_fill(planned={**planned, "qty": qty_str, "price_str": px_str, "limit_price": px}, order_id=order_id)
        total_filled += executed
        total_notional += executed * px
        if executed > 0:
            logger.info(f"Binance: уровень {level_idx} исполнено={_format_number(executed)} | всего={_format_number(total_filled)}")

    if total_filled >= coin_amount - 1e-9:
        avg_price = total_notional / total_filled if total_filled > 0 else None
        return OpenLegResult(
            exchange="binance", direction=direction, ok=True,
            order_id=last_order_id, filled_qty=total_filled, avg_price=avg_price, raw=None,
        )
    return OpenLegResult(
        exchange="binance", direction=direction, ok=False,
        error=f"не набрали объём за {MAX_ORDERBOOK_LEVELS} уровней: исполнено={_format_number(total_filled)}, требовалось={_format_number(coin_amount)}",
    )


# =========================
# MEXC Futures (Contract v1) trading (best-effort FOK + 3 levels)
# =========================

async def _mexc_private_request(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    method: str,
    path: str,
    params: Dict[str, Any],
) -> Any:
    """
    Best-effort signing for MEXC contract API:
    - adds api_key, req_time (ms)
    - sign = HMAC_SHA256(secret, urlencode(sorted(params)))
    - sends as query params
    """
    from urllib.parse import urlencode
    req_time = str(int(time.time() * 1000))
    p = {**params, "api_key": api_key, "req_time": req_time}
    pairs = sorted([(k, str(v)) for k, v in p.items() if v is not None], key=lambda kv: kv[0])
    query = urlencode(pairs, doseq=True)
    sign = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    pairs.append(("sign", sign))
    try:
        resp = await exchange_obj.client.request(method.upper(), path, params=pairs)
    except Exception as e:
        return {"_error": f"http error: {type(e).__name__}: {e}"}
    if resp.status_code < 200 or resp.status_code >= 300:
        return {"_error": f"http {resp.status_code}", "_body": resp.text[:400]}
    try:
        return resp.json()
    except Exception:
        return {"_error": "bad json", "_body": resp.text[:400]}


async def _mexc_plan_leg(*, exchange_obj: Any, coin: str, direction: str, coin_amount: float) -> Any:
    api_key = _get_env("MEXC_API_KEY")
    api_secret = _get_env("MEXC_API_SECRET")
    if not api_key or not api_secret:
        return OpenLegResult(exchange="mexc", direction=direction, ok=False, error="missing MEXC_API_KEY/MEXC_API_SECRET in env")

    # Проверка разрешений API ключа: пробуем простой приватный запрос
    test_resp = await _mexc_private_request(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        method="GET",
        path="/api/v1/private/account/assets",
        params={},
    )
    if isinstance(test_resp, dict) and test_resp.get("_error"):
        error_code = test_resp.get("_error", "")
        error_body = test_resp.get("_body", "")
        if "401" in error_code or "Not logged in" in error_body:
            return OpenLegResult(
                exchange="mexc",
                direction=direction,
                ok=False,
                error="MEXC API: не удалось авторизоваться (401 Not logged in or login has expired). Проверь API key/secret и права (contract/futures trading).",
            )
        if "403" in error_code or "Access Denied" in error_body:
            return OpenLegResult(
                exchange="mexc",
                direction=direction,
                ok=False,
                error=f"API ключ не имеет разрешений на торговлю (403 Access Denied). Проверь настройки API ключа на MEXC: разрешения на торговлю должны быть включены, IP-адрес должен быть разрешен (если включен whitelist)"
            )
        # Другие ошибки тоже логируем, но не блокируем (может быть временная проблема)
        logger.warning(f"MEXC: предварительная проверка API ключа вернула ошибку: {test_resp}")

    symbol = exchange_obj._normalize_symbol(coin)
    ob = await exchange_obj.get_orderbook(coin, limit=MAX_ORDERBOOK_LEVELS)
    if not ob or not ob.get("bids") or not ob.get("asks"):
        return OpenLegResult(exchange="mexc", direction=direction, ok=False, error=f"orderbook not available for {coin}")
    mexc_side = 1 if direction == "long" else 3
    book_side = ob["asks"] if direction == "long" else ob["bids"]
    best_price = float(book_side[0][0])
    if best_price <= 0:
        return OpenLegResult(exchange="mexc", direction=direction, ok=False, error="bad orderbook best price")

    mexc_type = int(os.getenv("MEXC_ORDER_TYPE", "4"))
    open_type = int(os.getenv("MEXC_OPEN_TYPE", "2"))
    logger.info(f"План MEXC: {direction} qty={_format_number(coin_amount)} | best={_format_number(best_price)} | уровни 1..{MAX_ORDERBOOK_LEVELS} (IOC)")

    return {
        "exchange": "mexc",
        "direction": direction,
        "exchange_obj": exchange_obj,
        "symbol": symbol,
        "coin": coin,
        "coin_amount": coin_amount,
        "mexc_side": mexc_side,
        "api_key": api_key,
        "api_secret": api_secret,
        "mexc_type": mexc_type,
        "openType": open_type,
        "externalOid": f"arb-{int(time.time()*1000)}-{direction}",
    }


async def _mexc_wait_full_fill(*, planned: Dict[str, Any], order_id: str) -> Tuple[bool, float]:
    """
    Best-effort: query order by externalOid (preferred), else by order_id.
    Checks state==3 and dealVol==vol.
    """
    exchange_obj = planned["exchange_obj"]
    api_key = planned["api_key"]
    api_secret = planned["api_secret"]
    symbol = planned["symbol"]
    ext = planned.get("externalOid")
    qty_req = float(planned["qty"])
    import asyncio
    for _ in range(20):
        if ext:
            data = await _mexc_private_request(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                method="GET",
                path=f"/api/v1/private/order/external/{symbol}/{ext}",
                params={},
            )
        else:
            data = await _mexc_private_request(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                method="GET",
                path=f"/api/v1/private/order/get/{order_id}",
                params={},
            )
        if isinstance(data, dict) and data.get("_error"):
            return False, 0.0

        # normalize data container
        item = None
        if isinstance(data, dict):
            item = data.get("data") or data.get("result") or data
        if not isinstance(item, dict):
            await asyncio.sleep(0.2)
            continue

        state = item.get("state")
        deal_vol = item.get("dealVol") or item.get("deal_vol") or item.get("deal_volume")
        vol = item.get("vol") or item.get("volume") or item.get("qty")
        try:
            deal = float(deal_vol) if deal_vol is not None else 0.0
        except Exception:
            deal = 0.0

        # state: 3 filled, 4 cancelled (per docs snippet)
        if str(state) in ("3", "4", "5"):
            logger.info(f"MEXC: статус ордера {order_id}: state={state} | исполнено={_format_number(deal)} | требовалось={_format_number(qty_req)}")
            return (deal + 1e-10 >= qty_req) and str(state) == "3", deal
        await asyncio.sleep(0.2)
    return False, 0.0


async def _mexc_place_leg(*, planned: Dict[str, Any]) -> OpenLegResult:
    exchange_obj = planned["exchange_obj"]
    api_key = planned["api_key"]
    api_secret = planned["api_secret"]
    direction = planned["direction"]
    symbol = planned["symbol"]
    coin = planned["coin"]
    coin_amount = float(planned["coin_amount"])
    mexc_side = planned["mexc_side"]
    mexc_type = planned["mexc_type"]
    open_type = planned["openType"]

    total_filled = 0.0
    total_notional = 0.0
    last_order_id: Optional[str] = None

    for level_idx in range(1, MAX_ORDERBOOK_LEVELS + 1):
        remaining = coin_amount - total_filled
        if remaining <= 0:
            break
        ob = await exchange_obj.get_orderbook(coin, limit=MAX_ORDERBOOK_LEVELS)
        if not ob or not ob.get("bids") or not ob.get("asks"):
            break
        book_side = ob["asks"] if direction == "long" else ob["bids"]
        if level_idx > len(book_side):
            break
        try:
            price_raw = float(book_side[level_idx - 1][0])
        except (IndexError, TypeError, ValueError):
            break
        px_str = _format_number(price_raw)
        qty_str = _format_number(remaining)
        if float(qty_str) <= 0:
            break
        external_oid = f"{planned.get('externalOid','arb')}-{level_idx}-{int(time.time()*1000)}"
        logger.info(f"MEXC: уровень {level_idx}/{MAX_ORDERBOOK_LEVELS} | {direction} qty={qty_str} | лимит={px_str} | осталось={_format_number(remaining)}")
        data = await _mexc_private_request(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            method="POST",
            path="/api/v1/private/order/create",
            params={
                "symbol": symbol,
                "price": px_str,
                "vol": qty_str,
                "side": str(mexc_side),
                "type": str(mexc_type),
                "openType": str(open_type),
                "externalOid": external_oid,
            },
        )
        if not isinstance(data, dict) or data.get("_error"):
            if isinstance(data, dict) and ("403" in str(data.get("_error", "")) or "Access Denied" in str(data.get("_body", ""))):
                return OpenLegResult(exchange="mexc", direction=direction, ok=False, error="API ключ не имеет разрешений на размещение ордеров (403).", raw=data)
            return OpenLegResult(exchange="mexc", direction=direction, ok=False, error=f"api error: {data}", raw=data)
        item = data.get("data") or data.get("result") or data
        order_id = item.get("orderId") or item.get("id") if isinstance(item, dict) else data.get("orderId")
        if not order_id:
            return OpenLegResult(exchange="mexc", direction=direction, ok=False, error=f"no orderId in response: {data}", raw=data)
        last_order_id = str(order_id)
        ok_full, deal = await _mexc_wait_full_fill(planned={**planned, "qty": qty_str, "externalOid": external_oid}, order_id=last_order_id)
        total_filled += deal
        total_notional += deal * price_raw
        if deal > 0:
            logger.info(f"MEXC: уровень {level_idx} исполнено={_format_number(deal)} | всего={_format_number(total_filled)}")

    if total_filled >= coin_amount - 1e-9:
        avg_price = total_notional / total_filled if total_filled > 0 else None
        return OpenLegResult(
            exchange="mexc", direction=direction, ok=True,
            order_id=last_order_id, filled_qty=total_filled, avg_price=avg_price, raw=None,
        )
    return OpenLegResult(
        exchange="mexc", direction=direction, ok=False,
        error=f"не набрали объём за {MAX_ORDERBOOK_LEVELS} уровней: исполнено={_format_number(total_filled)}, требовалось={_format_number(coin_amount)}",
    )


# =========================
# Bitget Futures trading (levels 1..MAX_ORDERBOOK_LEVELS, FOK, full-fill)
# =========================

_BITGET_CONTRACT_CACHE: Dict[str, Dict[str, Optional[str]]] = {}


def _step_str_from_precision(prec: int) -> Optional[str]:
    try:
        p = int(prec)
    except Exception:
        return None
    if p < 0:
        return None
    if p == 0:
        return "1"
    return "0." + ("0" * (p - 1)) + "1"


def _bitget_extract_code_msg(data: Any) -> Tuple[Optional[str], Optional[str]]:
    if not isinstance(data, dict):
        return None, None
    code = data.get("code")
    msg = data.get("msg") or data.get("message")
    return (str(code) if code is not None else None), (str(msg) if msg is not None else None)


async def _bitget_private_request(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    api_passphrase: str,
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
) -> Any:
    """
    Bitget private request signing (official pattern):
    prehash = timestamp + method + requestPath + ("?" + queryString if any) + bodyString
    sign = base64(HMAC_SHA256(secret, prehash))
    headers: ACCESS-KEY, ACCESS-SIGN, ACCESS-TIMESTAMP, ACCESS-PASSPHRASE
    """
    from urllib.parse import urlencode

    ts = str(int(time.time() * 1000))
    method_u = method.upper()
    params = params or {}
    body_json = json.dumps(body, separators=(",", ":"), ensure_ascii=False) if body else ""

    pairs = [(k, str(v)) for k, v in params.items() if v is not None]
    pairs = sorted(pairs, key=lambda kv: kv[0])
    query = urlencode(pairs, doseq=True) if pairs else ""
    path_with_query = f"{path}?{query}" if query else path

    prehash = f"{ts}{method_u}{path_with_query}{body_json}"
    sign = base64.b64encode(hmac.new(api_secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()).decode("utf-8")

    headers = {
        "ACCESS-KEY": api_key,
        "ACCESS-SIGN": sign,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": api_passphrase,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    try:
        resp = await exchange_obj.client.request(
            method_u,
            path,
            params=pairs if pairs else None,
            headers=headers,
            content=body_json if body_json else None,
        )
    except Exception as e:
        return {"_error": f"http error: {type(e).__name__}: {e}"}

    if resp.status_code < 200 or resp.status_code >= 300:
        txt = (resp.text or "")[:600]
        try:
            j = resp.json()
            if isinstance(j, dict):
                return {"_error": f"http {resp.status_code}", **j, "_body": txt}
        except Exception:
            pass
        return {"_error": f"http {resp.status_code}", "_body": txt}
    try:
        return resp.json()
    except Exception:
        return {"_error": "bad json", "_body": (resp.text or "")[:600]}


async def _bitget_get_contract_filters(*, exchange_obj: Any, symbol: str) -> Dict[str, Optional[str]]:
    """
    Best-effort: fetch tick/step/minQty from Bitget public contracts endpoint.
    Falls back to empty dict if unavailable.
    """
    if symbol in _BITGET_CONTRACT_CACHE:
        return _BITGET_CONTRACT_CACHE[symbol]
    product_type = getattr(exchange_obj, "PRODUCT_TYPE", "umcbl")
    url = "/api/v2/mix/market/contracts"
    for pt in (product_type, "USDT-FUTURES"):
        try:
            data = await exchange_obj._request_json("GET", url, params={"productType": pt, "symbol": symbol})
            if not isinstance(data, dict):
                continue
            code, msg = _bitget_extract_code_msg(data)
            if code is not None and code != "00000":
                continue
            items = data.get("data")
            if isinstance(items, list) and items:
                item = items[0]
            elif isinstance(items, dict):
                item = items
            else:
                item = None
            if not isinstance(item, dict):
                continue

            # Common fields (best-effort):
            # pricePlace/pricePrecision -> tickSize
            # sizePlace/quantityPrecision -> stepSize
            price_prec = item.get("pricePlace") or item.get("pricePrecision") or item.get("priceScale")
            qty_prec = item.get("sizePlace") or item.get("quantityPrecision") or item.get("volumePlace")
            tick_raw = item.get("tickSize") or _step_str_from_precision(int(price_prec)) if price_prec is not None else None
            step_raw = item.get("sizeMultiplier") or item.get("stepSize") or _step_str_from_precision(int(qty_prec)) if qty_prec is not None else None

            min_qty_raw = item.get("minTradeNum") or item.get("minOrderQty") or item.get("minTradeSize") or item.get("minSize")
            min_notional_raw = item.get("minNotional") or item.get("minTradeUSDT") or item.get("minTradeValue")

            out = {
                "tickSize": str(tick_raw) if tick_raw is not None else None,
                "stepSize": str(step_raw) if step_raw is not None else None,
                "minQty": str(min_qty_raw) if min_qty_raw is not None else None,
                "minNotional": str(min_notional_raw) if min_notional_raw is not None else None,
                "productType": str(pt),
            }
            _BITGET_CONTRACT_CACHE[symbol] = out
            return out
        except Exception:
            continue
    _BITGET_CONTRACT_CACHE[symbol] = {}
    return {}


async def _bitget_fetch_positions_best_effort(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    api_passphrase: str,
    symbol: str,
    product_type: str,
    margin_coin: str = "USDT",
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Bitget endpoints vary between versions; try multiple known paths and return the first successful response.
    Returns (response_json, used_path) or (None, None) on failure.
    """
    product_type_s = str(product_type)
    symbol_s = str(symbol)
    margin_coin_s = str(margin_coin)

    candidates: List[Tuple[str, Dict[str, Any]]] = [
        ("/api/v2/mix/position/single-position", {"symbol": symbol_s, "productType": product_type_s, "marginCoin": margin_coin_s}),
        ("/api/v2/mix/position/get-single-position", {"symbol": symbol_s, "productType": product_type_s, "marginCoin": margin_coin_s}),
        ("/api/v2/mix/position/all-position", {"productType": product_type_s, "marginCoin": margin_coin_s}),
        ("/api/v2/mix/position/get-all-position", {"productType": product_type_s, "marginCoin": margin_coin_s}),
    ]

    for path, params in candidates:
        data = await _bitget_private_request(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
            method="GET",
            path=path,
            params=params,
        )
        if not isinstance(data, dict):
            continue
        code, _msg = _bitget_extract_code_msg(data)
        if code is None:
            return data, path
        if code == "00000":
            return data, path
    return None, None


def _bitget_extract_positions_list(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = resp.get("data")
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        lst = data.get("list") or data.get("positions") or data.get("resultList")
        if isinstance(lst, list):
            return [x for x in lst if isinstance(x, dict)]
        return [data]
    return [resp] if isinstance(resp, dict) else []


def _bitget_parse_position_qty_and_mode(
    *,
    positions: List[Dict[str, Any]],
    symbol: Optional[str],
    hold_side: str,
) -> Tuple[Optional[float], Optional[str]]:
    """
    Parse total qty for a given hold side ("long"/"short") from Bitget position objects.
    Returns (qty, posMode) where posMode can be None if not present.
    """
    hs = (hold_side or "").lower().strip()
    qty_sum = 0.0
    saw_any = False
    mode: Optional[str] = None

    sym = (symbol or "").strip()

    for p in positions:
        if not isinstance(p, dict):
            continue
        if mode is None:
            pm = p.get("posMode") or p.get("positionMode")
            if pm is not None:
                mode = str(pm)

        # Filter by symbol if provided
        if sym:
            p_sym = p.get("symbol") or p.get("instId") or p.get("contract") or p.get("contractCode")
            p_sym_s = str(p_sym).strip() if p_sym is not None else ""
            if p_sym_s and p_sym_s != sym:
                continue

        p_hs = p.get("holdSide") or p.get("posSide") or p.get("positionSide") or p.get("side")
        p_hs_s = str(p_hs).lower().strip() if p_hs is not None else ""
        if hs and p_hs_s and p_hs_s != hs:
            continue

        for key in ("total", "available", "holdSize", "positionAmt", "size", "pos"):
            if key in p and p.get(key) is not None:
                try:
                    v = float(p.get(key))
                    qty_sum += abs(v)
                    saw_any = True
                    break
                except Exception:
                    continue

    if not saw_any:
        return None, mode
    return qty_sum, mode


def _bitget_extract_position_fields(p: Dict[str, Any]) -> Dict[str, Any]:
    """
    Small helper for logging/debugging (no secrets).
    """
    out: Dict[str, Any] = {}
    for k in ("symbol", "holdSide", "posSide", "posMode", "marginMode", "total", "available", "holdSize", "positionAmt", "size"):
        if k in p:
            out[k] = p.get(k)
    return out


async def _bitget_plan_leg(*, exchange_obj: Any, coin: str, direction: str, coin_amount: float) -> Any:
    api_key = _get_env("BITGET_API_KEY")
    api_secret = _get_env("BITGET_API_SECRET")
    api_pass = os.getenv("BITGET_API_PASSPHRASE", "").strip()
    if not api_key or not api_secret:
        return OpenLegResult(exchange="bitget", direction=direction, ok=False, error="missing BITGET_API_KEY/BITGET_API_SECRET in env")
    if not api_pass:
        return OpenLegResult(exchange="bitget", direction=direction, ok=False, error="Bitget требует passphrase (ACCESS-PASSPHRASE). Добавь BITGET_API_PASSPHRASE в .env")

    symbol = exchange_obj._normalize_symbol(coin)
    ob = await exchange_obj.get_orderbook(coin, limit=MAX_ORDERBOOK_LEVELS)
    if not ob or not ob.get("bids") or not ob.get("asks"):
        return OpenLegResult(exchange="bitget", direction=direction, ok=False, error=f"orderbook not available for {coin}")

    # Bitget может работать в unilateral (one-way) режиме, где ожидаются "unilateral" типы side:
    # open_long/open_short вместо buy/sell. Это устраняет ошибку 40774.
    side = "open_long" if direction == "long" else "open_short"
    book_side = ob["asks"] if direction == "long" else ob["bids"]
    best_price = float(book_side[0][0])
    if best_price <= 0:
        return OpenLegResult(exchange="bitget", direction=direction, ok=False, error="bad orderbook best price")

    f = await _bitget_get_contract_filters(exchange_obj=exchange_obj, symbol=symbol)
    tick_raw = f.get("tickSize")
    step_raw = f.get("stepSize")
    min_qty_raw = f.get("minQty")
    min_notional_raw = f.get("minNotional")
    product_type = f.get("productType") or getattr(exchange_obj, "PRODUCT_TYPE", "umcbl")
    tick = float(tick_raw) if tick_raw else 0.0
    step = float(step_raw) if step_raw else 0.0
    min_qty = float(min_qty_raw) if min_qty_raw else 0.0
    min_notional = float(min_notional_raw) if min_notional_raw else 0.0
    if step > 0 and not _is_multiple_of_step(coin_amount, step):
        return OpenLegResult(exchange="bitget", direction=direction, ok=False, error=f"qty {coin_amount} not multiple of stepSize {step_raw}")
    if min_qty > 0 and coin_amount < min_qty:
        return OpenLegResult(exchange="bitget", direction=direction, ok=False, error=f"qty {coin_amount} < minQty {min_qty_raw}")
    if min_notional > 0 and coin_amount * best_price < min_notional:
        return OpenLegResult(exchange="bitget", direction=direction, ok=False, error=f"minNotional {min_notional_raw} > requested ~{_format_number(coin_amount * best_price)}")
    qty_str = _format_by_step(coin_amount, step_raw)
    logger.info(f"План Bitget: {direction} qty={qty_str} | best={_format_number(best_price)} | уровни 1..{MAX_ORDERBOOK_LEVELS} (IOC)")

    return {
        "exchange": "bitget",
        "direction": direction,
        "exchange_obj": exchange_obj,
        "symbol": symbol,
        "productType": str(product_type),
        "marginCoin": "USDT",
        "side": side,
        "coin": coin,
        "coin_amount": coin_amount,
        "qty": qty_str,
        "api_key": api_key,
        "api_secret": api_secret,
        "api_passphrase": api_pass,
        "tickSize": tick_raw,
        "stepSize": step_raw,
    }


async def _bitget_wait_full_fill(*, planned: Dict[str, Any], order_id: str) -> Tuple[bool, float]:
    exchange_obj = planned["exchange_obj"]
    api_key = planned["api_key"]
    api_secret = planned["api_secret"]
    api_pass = planned["api_passphrase"]
    symbol = planned["symbol"]
    product_type = planned["productType"]
    qty_req = float(planned["qty"])
    import asyncio

    # Best-effort endpoints: v2 detail first, fallback to v1 detail
    for _ in range(20):
        data = await _bitget_private_request(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_pass,
            method="GET",
            path="/api/v2/mix/order/detail",
            params={"symbol": symbol, "productType": product_type, "orderId": order_id},
        )
        if isinstance(data, dict) and data.get("_error"):
            # try v1 fallback
            data = await _bitget_private_request(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_pass,
                method="GET",
                path="/api/mix/v1/order/detail",
                params={"symbol": symbol, "orderId": order_id, "marginCoin": "USDT"},
            )

        code, msg = _bitget_extract_code_msg(data)
        if code is not None and code != "00000":
            return False, 0.0

        item = None
        if isinstance(data, dict):
            item = data.get("data") or data.get("result") or data
        if not isinstance(item, dict):
            await asyncio.sleep(0.2)
            continue

        status = str(item.get("state") or item.get("status") or "")
        filled_raw = item.get("filledQty") or item.get("fillSz") or item.get("filledSize") or item.get("dealSize") or item.get("baseVolume") or item.get("accBaseVolume")
        try:
            filled = float(filled_raw) if filled_raw is not None else 0.0
        except Exception:
            filled = 0.0

        if status.lower() in ("filled", "full_fill", "complete", "completed", "success", "closed", "canceled", "cancelled", "rejected"):
            logger.info(f"Bitget: статус ордера {order_id}: {status} | исполнено={_format_number(filled)} | требовалось={_format_number(qty_req)}")
            return (filled + 1e-10 >= qty_req) and status.lower() in ("filled", "full_fill", "complete", "completed", "success", "closed"), filled

        await asyncio.sleep(0.2)

    return False, 0.0


async def _bitget_place_leg(*, planned: Dict[str, Any]) -> OpenLegResult:
    exchange_obj = planned["exchange_obj"]
    api_key = planned["api_key"]
    api_secret = planned["api_secret"]
    api_pass = planned["api_passphrase"]
    direction = planned["direction"]
    symbol = planned["symbol"]
    product_type = planned["productType"]
    coin = planned["coin"]
    coin_amount = float(planned["coin_amount"])
    tick_raw = planned.get("tickSize")
    step_raw = planned.get("stepSize")
    tick = float(tick_raw) if tick_raw else 0.0
    side = planned["side"]
    bitget_margin_mode = (os.getenv("BITGET_MARGIN_MODE", "isolated") or "").strip().lower()
    if not bitget_margin_mode or bitget_margin_mode == "cross":
        bitget_margin_mode = "crossed" if bitget_margin_mode == "cross" else "isolated"
    if bitget_margin_mode not in ("isolated", "crossed"):
        bitget_margin_mode = "isolated"

    total_filled = 0.0
    total_notional = 0.0
    last_order_id: Optional[str] = None

    for level_idx in range(1, MAX_ORDERBOOK_LEVELS + 1):
        remaining = coin_amount - total_filled
        if remaining <= 0:
            break
        ob = await exchange_obj.get_orderbook(coin, limit=MAX_ORDERBOOK_LEVELS)
        if not ob or not ob.get("bids") or not ob.get("asks"):
            break
        book_side = ob["asks"] if direction == "long" else ob["bids"]
        if level_idx > len(book_side):
            break
        try:
            price_raw = float(book_side[level_idx - 1][0])
        except (IndexError, TypeError, ValueError):
            break
        px = _round_price_for_side(price_raw, tick, "buy" if direction == "long" else "sell")
        px_str = _format_by_step(px, tick_raw)
        qty_str = _format_by_step(remaining, step_raw)
        if float(qty_str) <= 0:
            break
        logger.info(f"Bitget: уровень {level_idx}/{MAX_ORDERBOOK_LEVELS} | {direction} qty={qty_str} | лимит={px_str} | осталось={_format_number(remaining)}")
        base_body = {
            "symbol": symbol,
            "productType": product_type,
            "marginCoin": "USDT",
            "marginMode": bitget_margin_mode,
            "orderType": "limit",
            "price": px_str,
            "size": qty_str,
            "force": "ioc",
            "clientOid": f"arb-{int(time.time()*1000)}-{direction}-{level_idx}",
        }
        if direction == "long":
            side_open, side_buy_sell, pos_side = "open_long", "buy", "long"
        else:
            side_open, side_buy_sell, pos_side = "open_short", "sell", "short"
        candidate_bodies = [
            {**base_body, "side": side_open},
            {**base_body, "side": side_buy_sell, "tradeSide": "open", "posSide": pos_side},
            {**base_body, "side": side_buy_sell, "posSide": pos_side},
            {**base_body, "side": side_buy_sell, "tradeSide": "open", "holdSide": pos_side},
            {**base_body, "side": side_buy_sell, "holdSide": pos_side},
            {**base_body, "side": side_buy_sell, "tradeSide": side_open},
        ]
        data: Any = None
        for body in candidate_bodies:
            data = await _bitget_private_request(
                exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret,
                api_passphrase=api_pass, method="POST", path="/api/v2/mix/order/place-order", body=body,
            )
            code, msg = _bitget_extract_code_msg(data)
            if code is not None and code != "00000":
                msg_l = (msg or "").lower()
                if "sign" in msg_l or "signature" in msg_l or "passphrase" in msg_l:
                    return OpenLegResult(exchange="bitget", direction=direction, ok=False, error=f"Bitget auth error: {msg}", raw=data)
                # Ошибка недостаточного баланса
                if str(code) == "40762" or "balance" in msg_l or "exceeds the balance" in msg_l:
                    return OpenLegResult(
                        exchange="bitget", direction=direction, ok=False,
                        error=f"Bitget: недостаточно баланса для открытия позиции (код {code}: {msg}). Проверьте баланс USDT на бирже Bitget.",
                        raw=data
                    )
                if "side mismatch" in msg_l or "unilateral" in msg_l or str(code) in ("400172", "40774"):
                    continue
                return OpenLegResult(exchange="bitget", direction=direction, ok=False, error=f"api error: {data}", raw=data)
            break
        if not isinstance(data, dict):
            break
        item = data.get("data")
        order_id = (item.get("orderId") or item.get("id")) if isinstance(item, dict) else data.get("orderId") or data.get("id")
        if not order_id:
            return OpenLegResult(exchange="bitget", direction=direction, ok=False, error=f"no orderId in response: {data}", raw=data)
        last_order_id = str(order_id)
        ok_full, filled = await _bitget_wait_full_fill(planned={**planned, "qty": qty_str, "price_str": px_str, "limit_price": px}, order_id=last_order_id)
        total_filled += filled
        total_notional += filled * px
        if filled > 0:
            logger.info(f"Bitget: уровень {level_idx} исполнено={_format_number(filled)} | всего={_format_number(total_filled)}")

    if total_filled >= coin_amount - 1e-9:
        avg_price = total_notional / total_filled if total_filled > 0 else None
        return OpenLegResult(
            exchange="bitget", direction=direction, ok=True,
            order_id=last_order_id, filled_qty=total_filled, avg_price=avg_price, raw=None,
        )
    return OpenLegResult(
        exchange="bitget", direction=direction, ok=False,
        error=f"не набрали объём за {MAX_ORDERBOOK_LEVELS} уровней: исполнено={_format_number(total_filled)}, требовалось={_format_number(coin_amount)}",
    )


# =========================
# BingX Futures trading (levels 1..MAX_ORDERBOOK_LEVELS, FOK, full-fill)
# =========================

_BINGX_CONTRACT_CACHE: Dict[str, Dict[str, Optional[str]]] = {}


async def _bingx_get_contract_filters(*, exchange_obj: Any, symbol: str) -> Dict[str, Optional[str]]:
    if symbol in _BINGX_CONTRACT_CACHE:
        return _BINGX_CONTRACT_CACHE[symbol]
    try:
        data = await exchange_obj._request_json("GET", "/openApi/swap/v2/quote/contracts", params={})
        if not isinstance(data, dict) or str(data.get("code")) != "0":
            _BINGX_CONTRACT_CACHE[symbol] = {}
            return {}
        items = data.get("data") or []
        if not isinstance(items, list):
            _BINGX_CONTRACT_CACHE[symbol] = {}
            return {}
        item = None
        for it in items:
            if isinstance(it, dict) and it.get("symbol") == symbol:
                item = it
                break
        if not item:
            _BINGX_CONTRACT_CACHE[symbol] = {}
            return {}

        # Best-effort fields:
        price_prec = item.get("priceScale") or item.get("pricePrecision") or item.get("priceDecimal")
        qty_prec = item.get("quantityScale") or item.get("quantityPrecision") or item.get("volumePrecision")
        tick_raw = item.get("tickSize") or (_step_str_from_precision(int(price_prec)) if price_prec is not None else None)
        step_raw = item.get("stepSize") or (_step_str_from_precision(int(qty_prec)) if qty_prec is not None else None)
        min_qty_raw = item.get("minTradeNum") or item.get("minTradeQty") or item.get("minQty") or item.get("tradeMinQuantity")
        min_notional_raw = item.get("minNotional") or item.get("minTradeValue") or item.get("minTradeUSDT")

        out = {
            "tickSize": str(tick_raw) if tick_raw is not None else None,
            "stepSize": str(step_raw) if step_raw is not None else None,
            "minQty": str(min_qty_raw) if min_qty_raw is not None else None,
            "minNotional": str(min_notional_raw) if min_notional_raw is not None else None,
        }
        _BINGX_CONTRACT_CACHE[symbol] = out
        return out
    except Exception:
        _BINGX_CONTRACT_CACHE[symbol] = {}
        return {}


async def _bingx_private_request(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    method: str,
    path: str,
    params: Dict[str, Any],
) -> Any:
    """
    BingX swap signing (common pattern):
    - add timestamp (ms)
    - sign HMAC_SHA256(secret, urlencode(sorted(params))) as hex
    - send signature as query param
    - header: X-BX-APIKEY
    """
    from urllib.parse import urlencode

    ts = str(int(time.time() * 1000))
    p = {**params, "timestamp": ts}
    pairs = sorted([(k, str(v)) for k, v in p.items() if v is not None], key=lambda kv: kv[0])
    query = urlencode(pairs, doseq=True)
    sign = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    pairs.append(("signature", sign))
    headers = {"X-BX-APIKEY": api_key, "Accept": "application/json"}
    try:
        resp = await exchange_obj.client.request(method.upper(), path, params=pairs, headers=headers)
    except Exception as e:
        return {"_error": f"http error: {type(e).__name__}: {e}"}
    if resp.status_code < 200 or resp.status_code >= 300:
        return {"_error": f"http {resp.status_code}", "_body": (resp.text or "")[:600]}
    try:
        return resp.json()
    except Exception:
        return {"_error": "bad json", "_body": (resp.text or "")[:600]}


async def _bingx_plan_leg(*, exchange_obj: Any, coin: str, direction: str, coin_amount: float) -> Any:
    api_key = _get_env("BINGX_API_KEY")
    api_secret = _get_env("BINGX_API_SECRET")
    if not api_key or not api_secret:
        return OpenLegResult(exchange="bingx", direction=direction, ok=False, error="missing BINGX_API_KEY/BINGX_API_SECRET in env")

    symbol = exchange_obj._normalize_symbol(coin)
    ob = await exchange_obj.get_orderbook(coin, limit=MAX_ORDERBOOK_LEVELS)
    if not ob or not ob.get("bids") or not ob.get("asks"):
        return OpenLegResult(exchange="bingx", direction=direction, ok=False, error=f"orderbook not available for {coin}")

    side = "BUY" if direction == "long" else "SELL"
    position_side = "LONG" if direction == "long" else "SHORT"
    book_side = ob["asks"] if direction == "long" else ob["bids"]
    best_price = float(book_side[0][0])
    if best_price <= 0:
        return OpenLegResult(exchange="bingx", direction=direction, ok=False, error="bad orderbook best price")

    f = await _bingx_get_contract_filters(exchange_obj=exchange_obj, symbol=symbol)
    tick_raw = f.get("tickSize")
    step_raw = f.get("stepSize")
    min_qty_raw = f.get("minQty")
    min_notional_raw = f.get("minNotional")
    tick = float(tick_raw) if tick_raw else 0.0
    step = float(step_raw) if step_raw else 0.0
    min_qty = float(min_qty_raw) if min_qty_raw else 0.0
    min_notional = float(min_notional_raw) if min_notional_raw else 0.0
    if step > 0 and not _is_multiple_of_step(coin_amount, step):
        return OpenLegResult(exchange="bingx", direction=direction, ok=False, error=f"qty {coin_amount} not multiple of stepSize {step_raw}")
    if min_qty > 0 and coin_amount < min_qty:
        return OpenLegResult(exchange="bingx", direction=direction, ok=False, error=f"qty {coin_amount} < minQty {min_qty_raw}")
    if min_notional > 0 and coin_amount * best_price < min_notional:
        return OpenLegResult(exchange="bingx", direction=direction, ok=False, error=f"minNotional {min_notional_raw} > requested ~{_format_number(coin_amount * best_price)}")
    qty_str = _format_by_step(coin_amount, step_raw)
    logger.info(f"План BingX: {direction} qty={qty_str} | best={_format_number(best_price)} | уровни 1..{MAX_ORDERBOOK_LEVELS} (IOC)")

    return {
        "exchange": "bingx",
        "direction": direction,
        "exchange_obj": exchange_obj,
        "symbol": symbol,
        "side": side,
        "positionSide": position_side,
        "coin": coin,
        "coin_amount": coin_amount,
        "qty": qty_str,
        "api_key": api_key,
        "api_secret": api_secret,
        "tickSize": tick_raw,
        "stepSize": step_raw,
    }


async def _bingx_wait_full_fill(*, planned: Dict[str, Any], order_id: str) -> Tuple[bool, float]:
    exchange_obj = planned["exchange_obj"]
    api_key = planned["api_key"]
    api_secret = planned["api_secret"]
    symbol = planned["symbol"]
    qty_req = float(planned["qty"])
    import asyncio

    for _ in range(20):
        data = await _bingx_private_request(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            method="GET",
            path="/openApi/swap/v2/trade/order",
            params={"symbol": symbol, "orderId": order_id},
        )
        if isinstance(data, dict) and data.get("_error"):
            return False, 0.0
        if not isinstance(data, dict):
            await asyncio.sleep(0.2)
            continue
        code = str(data.get("code")) if data.get("code") is not None else None
        if code is not None and code != "0":
            return False, 0.0

        item = data.get("data") if isinstance(data, dict) else None
        if not isinstance(item, dict):
            await asyncio.sleep(0.2)
            continue

        # BingX query может возвращать вложенность data.order (как в ответе place order)
        if isinstance(item.get("order"), dict):
            item = item.get("order")  # type: ignore[assignment]

        status = str(item.get("status") or item.get("state") or "")
        filled_raw = item.get("executedQty") or item.get("filledQty") or item.get("dealQty") or item.get("dealSize") or item.get("filledSize")
        try:
            filled = float(filled_raw) if filled_raw is not None else 0.0
        except Exception:
            filled = 0.0

        if status.upper() in ("FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED"):
            logger.info(f"BingX: статус ордера {order_id}: {status} | исполнено={_format_number(filled)} | требовалось={_format_number(qty_req)}")
            return (filled + 1e-10 >= qty_req) and status.upper() == "FILLED", filled

        await asyncio.sleep(0.2)

    return False, 0.0


async def _bingx_place_leg(*, planned: Dict[str, Any]) -> OpenLegResult:
    exchange_obj = planned["exchange_obj"]
    api_key = planned["api_key"]
    api_secret = planned["api_secret"]
    direction = planned["direction"]
    symbol = planned["symbol"]
    side = planned["side"]
    position_side = planned.get("positionSide")
    coin = planned["coin"]
    coin_amount = float(planned["coin_amount"])
    tick_raw = planned.get("tickSize")
    step_raw = planned.get("stepSize")
    tick = float(tick_raw) if tick_raw else 0.0

    total_filled = 0.0
    total_notional = 0.0
    last_order_id: Optional[str] = None

    for level_idx in range(1, MAX_ORDERBOOK_LEVELS + 1):
        remaining = coin_amount - total_filled
        if remaining <= 0:
            break
        ob = await exchange_obj.get_orderbook(coin, limit=MAX_ORDERBOOK_LEVELS)
        if not ob or not ob.get("bids") or not ob.get("asks"):
            break
        book_side = ob["asks"] if direction == "long" else ob["bids"]
        if level_idx > len(book_side):
            break
        try:
            price_raw = float(book_side[level_idx - 1][0])
        except (IndexError, TypeError, ValueError):
            break
        px = _round_price_for_side(price_raw, tick, "buy" if direction == "long" else "sell")
        px_str = _format_by_step(px, tick_raw)
        qty_str = _format_by_step(remaining, step_raw)
        if float(qty_str) <= 0:
            break
        logger.info(f"BingX: уровень {level_idx}/{MAX_ORDERBOOK_LEVELS} | {direction} qty={qty_str} | лимит={px_str} | осталось={_format_number(remaining)}")
        params = {
            "symbol": symbol,
            "side": side,
            "type": "LIMIT",
            "price": px_str,
            "quantity": qty_str,
            "timeInForce": "IOC",
        }
        if position_side:
            params["positionSide"] = position_side
        data = await _bingx_private_request(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            method="POST",
            path="/openApi/swap/v2/trade/order",
            params=params,
        )
        if not isinstance(data, dict) or data.get("_error"):
            return OpenLegResult(exchange="bingx", direction=direction, ok=False, error=f"api error: {data}", raw=data)
        if str(data.get("code")) not in (None, "0"):
            return OpenLegResult(exchange="bingx", direction=direction, ok=False, error=f"api error: {data}", raw=data)
        item = data.get("data")
        order_container = item.get("order") if isinstance(item, dict) and isinstance(item.get("order"), dict) else item if isinstance(item, dict) else None
        order_id = None
        if isinstance(order_container, dict):
            order_id = order_container.get("orderId") or order_container.get("orderID") or order_container.get("id") or order_container.get("order_id")
        if not order_id:
            order_id = data.get("orderId")
        if not order_id:
            return OpenLegResult(exchange="bingx", direction=direction, ok=False, error=f"no orderId in response: {data}", raw=data)
        last_order_id = str(order_id)
        ok_full, filled = await _bingx_wait_full_fill(planned={**planned, "qty": qty_str, "price_str": px_str, "limit_price": px}, order_id=last_order_id)
        total_filled += filled
        total_notional += filled * px
        if filled > 0:
            logger.info(f"BingX: уровень {level_idx} исполнено={_format_number(filled)} | всего={_format_number(total_filled)}")

    if total_filled >= coin_amount - 1e-9:
        avg_price = total_notional / total_filled if total_filled > 0 else None
        return OpenLegResult(
            exchange="bingx", direction=direction, ok=True,
            order_id=last_order_id, filled_qty=total_filled, avg_price=avg_price, raw=None,
        )
    return OpenLegResult(
        exchange="bingx", direction=direction, ok=False,
        error=f"не набрали объём за {MAX_ORDERBOOK_LEVELS} уровней: исполнено={_format_number(total_filled)}, требовалось={_format_number(coin_amount)}",
    )


async def _bingx_close_leg_partial_ioc(*, exchange_obj: Any, coin: str, position_direction: str, coin_amount: float) -> Tuple[bool, Optional[float]]:
    """
    BingX USDT-M Futures: закрытие позиции частями лимитными IOC ордерами.
    position_direction: "long" (закрываем SELL) или "short" (закрываем BUY).
    """
    api_key = _get_env("BINGX_API_KEY")
    api_secret = _get_env("BINGX_API_SECRET")
    if not api_key or not api_secret:
        logger.error("❌ BingX: missing BINGX_API_KEY/BINGX_API_SECRET in env")
        return False, None

    pos_dir = (position_direction or "").lower().strip()
    if pos_dir not in ("long", "short"):
        logger.error(f"❌ BingX: некорректное направление позиции: {position_direction!r}")
        return False, None

    remaining = float(coin_amount)
    if remaining <= 0:
        return True, None

    symbol = exchange_obj._normalize_symbol(coin)
    f = await _bingx_get_contract_filters(exchange_obj=exchange_obj, symbol=symbol)
    tick_raw = f.get("tickSize")
    step_raw = f.get("stepSize")
    tick = float(tick_raw) if tick_raw else 0.0001
    step = float(step_raw) if step_raw else 0.0

    # Для закрытия: LONG позиция закрывается SELL, SHORT позиция закрывается BUY
    side_close = "SELL" if pos_dir == "long" else "BUY"
    position_side = "LONG" if pos_dir == "long" else "SHORT"

    total_notional = 0.0
    total_filled = 0.0
    eps = max(1e-10, remaining * 1e-8)

    max_orders_total = max(10, MAX_ORDERBOOK_LEVELS * 3)
    for order_n in range(1, max_orders_total + 1):
        if remaining <= eps:
            avg_price = total_notional / total_filled if total_filled > 0 else None
            return True, avg_price

        ob = await exchange_obj.get_orderbook(coin, limit=MAX_ORDERBOOK_LEVELS)
        if not ob or not ob.get("bids") or not ob.get("asks"):
            logger.error(f"❌ BingX: orderbook недоступен для закрытия {coin}")
            return False, None

        levels = ob["bids"] if side_close == "SELL" else ob["asks"]
        filled_any = 0.0

        for lvl_i, lvl in enumerate(levels[:MAX_ORDERBOOK_LEVELS], start=1):
            try:
                px_raw = float(lvl[0])
            except Exception:
                continue
            if px_raw <= 0:
                continue

            px = _round_price_for_side(px_raw, tick, "sell" if side_close == "SELL" else "buy")
            px_str = _format_by_step(px, tick_raw)

            qty_to_send = remaining
            if step > 0:
                qty_str = _format_by_step(qty_to_send, step_raw)
            else:
                qty_str = str(qty_to_send)

            if float(qty_str) <= 0:
                break

            logger.info(f"BingX close: попытка {order_n}/{max_orders_total} | side={side_close} qty={qty_str} | лимит={px_str} | remaining={_format_number(remaining)}")

            params = {
                "symbol": symbol,
                "side": side_close,
                "type": "LIMIT",
                "price": px_str,
                "quantity": qty_str,
                "timeInForce": "IOC",
                "positionSide": position_side,
            }

            # BingX может требовать reduceOnly, но сначала пробуем без него
            data = await _bingx_private_request(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                method="POST",
                path="/openApi/swap/v2/trade/order",
                params=params,
            )

            if not isinstance(data, dict):
                logger.error(f"❌ BingX close: api error: {data}")
                continue

            if data.get("_error") or str(data.get("code")) not in (None, "0"):
                logger.error(f"❌ BingX close: api error: {data}")
                continue

            # Парсим orderId из ответа
            item = data.get("data")
            order_container = item.get("order") if isinstance(item, dict) and isinstance(item.get("order"), dict) else item if isinstance(item, dict) else None
            order_id = None
            if isinstance(order_container, dict):
                order_id = order_container.get("orderId") or order_container.get("orderID") or order_container.get("id") or order_container.get("order_id")
            if not order_id:
                order_id = data.get("orderId")
            if not order_id:
                logger.error(f"❌ BingX close: no orderId in response: {data}")
                continue

            # Ждём исполнения IOC ордера
            ok_full, executed = await _bingx_wait_full_fill(
                planned={"exchange_obj": exchange_obj, "api_key": api_key, "api_secret": api_secret, "symbol": symbol, "qty": qty_str},
                order_id=str(order_id),
            )
            if executed and executed > 0:
                filled_any = float(executed)
                # Best-effort avgPrice from API (если есть)
                avg_px = None
                try:
                    if isinstance(item, dict):
                        avg_px = float(item.get("avgPrice") or item.get("avg_price") or item.get("price")) if (item.get("avgPrice") or item.get("avg_price") or item.get("price")) is not None else None
                except Exception:
                    avg_px = None
                use_px = avg_px if (avg_px is not None and avg_px > 0) else px
                total_notional += filled_any * float(use_px)
                total_filled += filled_any
                remaining = max(0.0, remaining - filled_any)
                logger.info(f"BingX close: исполнено={_format_number(filled_any)} {coin} | осталось={_format_number(remaining)} {coin} | full={ok_full}")
                break

        if filled_any <= 0:
            logger.debug(f"BingX close: 0 исполнено по уровням 1-{MAX_ORDERBOOK_LEVELS} | осталось={_format_number(remaining)} {coin}")
            continue

    logger.error(f"❌ BingX close: не удалось закрыть позицию полностью | осталось={_format_number(remaining)} {coin}")
    avg_price = total_notional / total_filled if total_filled > 0 else None
    return False, avg_price


async def _open_one_leg(
    *,
    exchange_name: str,
    exchange_obj: Any,
    coin: str,
    direction: str,
    notional_usdt: float,
) -> OpenLegResult:
    ex = exchange_name.lower()
    try:
        if ex == "bybit":
            return await _bybit_open_leg(exchange_obj=exchange_obj, coin=coin, direction=direction, notional_usdt=notional_usdt)
        if ex == "gate":
            return await _gate_open_leg(exchange_obj=exchange_obj, coin=coin, direction=direction, notional_usdt=notional_usdt)
        if ex == "xt":
            # Use plan + place pattern for XT
            plan = await _xt_plan_leg(exchange_obj=exchange_obj, coin=coin, direction=direction, coin_amount=notional_usdt / (await exchange_obj.get_futures_ticker(coin) or {}).get("price", 1.0))
            if isinstance(plan, OpenLegResult) and not plan.ok:
                return plan
            if not isinstance(plan, dict):
                return OpenLegResult(exchange=exchange_name, direction=direction, ok=False, error="plan failed")
            return await _xt_place_leg(planned=plan)
        return OpenLegResult(exchange=exchange_name, direction=direction, ok=False, error="trading not implemented for this exchange")
    except Exception as e:
        return OpenLegResult(exchange=exchange_name, direction=direction, ok=False, error=str(e))


async def _bybit_open_leg(*, exchange_obj: Any, coin: str, direction: str, notional_usdt: float) -> OpenLegResult:
    api_key = _get_env_any(("BYBIT_API_KEY",))
    api_secret = _get_env_any(("BYBIT_API_SECRET",))
    if not api_key or not api_secret:
        return OpenLegResult(exchange="bybit", direction=direction, ok=False, error="missing BYBIT_API_KEY/BYBIT_API_SECRET in env")

    ticker = await exchange_obj.get_futures_ticker(coin)
    if not ticker:
        return OpenLegResult(exchange="bybit", direction=direction, ok=False, error=f"ticker not available for {coin}")

    ask = float(ticker.get("ask") or ticker.get("price") or 0)
    bid = float(ticker.get("bid") or ticker.get("price") or 0)
    if ask <= 0 or bid <= 0:
        return OpenLegResult(exchange="bybit", direction=direction, ok=False, error=f"bad bid/ask for {coin}: bid={bid} ask={ask}")

    symbol = exchange_obj._normalize_symbol(coin)
    side = "Buy" if direction == "long" else "Sell"
    ref_price = ask if side == "Buy" else bid
    qty_est = notional_usdt / ref_price

    qty_step_raw, min_qty_raw = await _bybit_fetch_lot_filters(exchange_obj=exchange_obj, symbol=symbol)
    qty_step = float(qty_step_raw) if qty_step_raw else 0.0
    min_qty = float(min_qty_raw) if min_qty_raw else 0.0
    qty_adj = _floor_to_step(qty_est, qty_step) if qty_step > 0 else qty_est
    if qty_adj <= 0:
        return OpenLegResult(exchange="bybit", direction=direction, ok=False, error=f"qty computed as {qty_adj} (est={qty_est})")
    if min_qty > 0 and qty_adj < min_qty:
        return OpenLegResult(exchange="bybit", direction=direction, ok=False, error=f"qty {qty_adj} < minOrderQty {min_qty}")

    qty_decimals = _decimals_from_step_str(qty_step_raw)
    qty_str = f"{qty_adj:.{qty_decimals}f}" if qty_decimals > 0 else str(int(qty_adj))
    qty_str = qty_str.rstrip("0").rstrip(".") if "." in qty_str else qty_str

    body = {
        "category": "linear",
        "symbol": symbol,
        "side": side,
        "orderType": "Market",
        "qty": qty_str,
    }
    body_json = json.dumps(body, separators=(",", ":"), ensure_ascii=False)

    recv_window = str(int(float(os.getenv("BYBIT_RECV_WINDOW", "5000"))))
    ts = str(int(time.time() * 1000))
    sign_payload = f"{ts}{api_key}{recv_window}{body_json}"
    sign = hmac.new(api_secret.encode("utf-8"), sign_payload.encode("utf-8"), hashlib.sha256).hexdigest()

    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv_window,
        "X-BAPI-SIGN": sign,
        "Content-Type": "application/json",
    }

    try:
        resp = await exchange_obj.client.request("POST", "/v5/order/create", headers=headers, content=body_json)
    except Exception as e:
        return OpenLegResult(exchange="bybit", direction=direction, ok=False, error=f"http error: {type(e).__name__}: {e}")

    text = resp.text
    if resp.status_code < 200 or resp.status_code >= 300:
        return OpenLegResult(exchange="bybit", direction=direction, ok=False, error=f"http {resp.status_code}: {text[:300]}")

    try:
        data = resp.json()
    except Exception:
        return OpenLegResult(exchange="bybit", direction=direction, ok=False, error=f"bad json: {text[:300]}")

    if not isinstance(data, dict) or data.get("retCode") != 0:
        return OpenLegResult(exchange="bybit", direction=direction, ok=False, error=f"api error: {data}", raw=data)

    order_id = None
    try:
        order_id = (data.get("result") or {}).get("orderId")
    except Exception:
        order_id = None

    return OpenLegResult(exchange="bybit", direction=direction, ok=True, order_id=str(order_id) if order_id else None, raw=data)


async def _bybit_fetch_lot_filters(*, exchange_obj: Any, symbol: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Возвращает (qtyStep, minOrderQty) как строки, чтобы не потерять точность на float.
    """
    try:
        resp = await exchange_obj.client.request(
            "GET",
            "/v5/market/instruments-info",
            params={"category": "linear", "symbol": symbol},
        )
        if resp.status_code < 200 or resp.status_code >= 300:
            return None, None
        data = resp.json()
        if not isinstance(data, dict) or data.get("retCode") != 0:
            return None, None
        items = ((data.get("result") or {}).get("list") or [])
        if not items or not isinstance(items[0], dict):
            return None, None
        lot = (items[0].get("lotSizeFilter") or {})
        qty_step = lot.get("qtyStep")
        min_qty = lot.get("minOrderQty")
        return (str(qty_step) if qty_step is not None else None, str(min_qty) if min_qty is not None else None)
    except Exception:
        return None, None


async def _gate_open_leg(*, exchange_obj: Any, coin: str, direction: str, notional_usdt: float) -> OpenLegResult:
    api_key = _get_env_any(("GATEIO_API_KEY",))
    api_secret = _get_env_any(("GATEIO_API_SECRET",))
    if not api_key or not api_secret:
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error="missing GATEIO_API_KEY/GATEIO_API_SECRET in env")

    ticker = await exchange_obj.get_futures_ticker(coin)
    if not ticker:
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"ticker not available for {coin}")

    ask = float(ticker.get("ask") or ticker.get("price") or 0)
    bid = float(ticker.get("bid") or ticker.get("price") or 0)
    if ask <= 0 or bid <= 0:
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"bad bid/ask for {coin}: bid={bid} ask={ask}")

    contract = exchange_obj._normalize_symbol(coin)
    # Gate futures: size is contracts count (integer); contract size in base is quanto_multiplier
    qmul, min_size = await _gate_fetch_contract_filters(exchange_obj=exchange_obj, contract=contract)
    if qmul is None or qmul <= 0:
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"bad quanto_multiplier for {contract}: {qmul}")

    ref_price = ask if direction == "long" else bid
    base_qty = notional_usdt / ref_price
    contracts_est = base_qty / qmul
    contracts_i = int(math.floor(contracts_est))
    if min_size is not None and contracts_i < int(min_size):
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"contracts {contracts_i} < min {min_size} (est={contracts_est:.3f})")
    if contracts_i <= 0:
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"contracts computed as {contracts_i} (est={contracts_est:.3f})")

    size_signed = contracts_i if direction == "long" else -contracts_i

    body = {
        "contract": contract,
        "size": size_signed,
        "price": "0",   # market
        "tif": "ioc",
    }
    body_json = json.dumps(body, separators=(",", ":"), ensure_ascii=False)

    method = "POST"
    path = "/api/v4/futures/usdt/orders"
    query_string = ""
    ts = str(int(time.time()))
    payload_hash = hashlib.sha512(body_json.encode("utf-8")).hexdigest()
    sign_str = "\n".join([method, path, query_string, payload_hash, ts])
    sign = hmac.new(api_secret.encode("utf-8"), sign_str.encode("utf-8"), hashlib.sha512).hexdigest()

    headers = {
        "KEY": api_key,
        "Timestamp": ts,
        "SIGN": sign,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    try:
        resp = await exchange_obj.client.request(method, path, headers=headers, content=body_json)
    except Exception as e:
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"http error: {type(e).__name__}: {e}")

    text = resp.text
    if resp.status_code < 200 or resp.status_code >= 300:
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"http {resp.status_code}: {text[:300]}")

    try:
        data = resp.json()
    except Exception:
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"bad json: {text[:300]}")

    # На успех обычно приходит dict с id
    if isinstance(data, dict) and data.get("id") is not None:
        return OpenLegResult(exchange="gate", direction=direction, ok=True, order_id=str(data.get("id")), raw=data)

    return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"api error: {data}", raw=data)


async def _gate_fetch_contract_filters(*, exchange_obj: Any, contract: str) -> Tuple[Optional[float], Optional[int]]:
    """
    Возвращает (quanto_multiplier, order_size_min) для контракта.
    """
    try:
        resp = await exchange_obj.client.request("GET", f"/api/v4/futures/usdt/contracts/{contract}")
        if resp.status_code < 200 or resp.status_code >= 300:
            return None, None
        data = resp.json()
        if not isinstance(data, dict):
            return None, None
        qmul_raw = data.get("quanto_multiplier") or data.get("contract_size") or data.get("multiplier")
        qmul = float(qmul_raw) if qmul_raw is not None else None
        min_raw = data.get("order_size_min") or data.get("order_size_minimum") or data.get("order_size_minimal")
        min_size = int(float(min_raw)) if min_raw is not None else None
        return qmul, min_size
    except Exception:
        return None, None


# =========================
# New limit-order flow
# =========================

async def _bybit_fetch_instrument_filters(*, exchange_obj: Any, symbol: str) -> Dict[str, Optional[str]]:
    """
    Возвращает фильтры инструмента Bybit (строками): qtyStep, minOrderQty, minOrderAmt, tickSize.
    """
    try:
        resp = await exchange_obj.client.request(
            "GET",
            "/v5/market/instruments-info",
            params={"category": "linear", "symbol": symbol},
        )
        if resp.status_code < 200 or resp.status_code >= 300:
            return {}
        data = resp.json()
        if not isinstance(data, dict) or data.get("retCode") != 0:
            return {}
        items = ((data.get("result") or {}).get("list") or [])
        if not items or not isinstance(items[0], dict):
            return {}
        item = items[0]
        lot = (item.get("lotSizeFilter") or {})
        pf = (item.get("priceFilter") or {})
        return {
            "qtyStep": str(lot.get("qtyStep")) if lot.get("qtyStep") is not None else None,
            "minOrderQty": str(lot.get("minOrderQty")) if lot.get("minOrderQty") is not None else None,
            "minOrderAmt": str(lot.get("minOrderAmt") or lot.get("minNotionalValue") or lot.get("minOrderValue")) if (lot.get("minOrderAmt") or lot.get("minNotionalValue") or lot.get("minOrderValue")) is not None else None,
            "tickSize": str(pf.get("tickSize")) if pf.get("tickSize") is not None else None,
        }
    except Exception:
        return {}


async def _bybit_private_post(*, exchange_obj: Any, api_key: str, api_secret: str, path: str, body: Dict[str, Any]) -> Any:
    # Use unified request helper (also supports GET signing)
    return await _bybit_private_request(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        method="POST",
        path=path,
        body=body,
    )


async def _bybit_switch_isolated_and_leverage_1(*, exchange_obj: Any, symbol: str) -> Tuple[bool, str]:
    api_key = _get_env("BYBIT_API_KEY")
    api_secret = _get_env("BYBIT_API_SECRET")
    if not api_key or not api_secret:
        return False, "missing BYBIT_API_KEY/BYBIT_API_SECRET"
    body = {
        "category": "linear",
        "symbol": symbol,
        "tradeMode": 1,  # 0 cross, 1 isolated
        "buyLeverage": "1",
        "sellLeverage": "1",
    }
    data = await _bybit_private_post(exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret, path="/v5/position/switch-isolated", body=body)
    if not isinstance(data, dict) or data.get("retCode") != 0:
        return False, str(data)
    return True, "ok"


async def _bybit_plan_leg(*, exchange_obj: Any, coin: str, direction: str, coin_amount: float) -> Any:
    api_key = _get_env("BYBIT_API_KEY")
    api_secret = _get_env("BYBIT_API_SECRET")
    if not api_key or not api_secret:
        return OpenLegResult(exchange="bybit", direction=direction, ok=False, error="missing BYBIT_API_KEY/BYBIT_API_SECRET in env")

    symbol = exchange_obj._normalize_symbol(coin)
    side = "Buy" if direction == "long" else "Sell"

    ob = await exchange_obj.get_orderbook(coin, limit=MAX_ORDERBOOK_LEVELS)
    if not ob or not ob.get("bids") or not ob.get("asks"):
        return OpenLegResult(exchange="bybit", direction=direction, ok=False, error=f"orderbook not available for {coin}")
    book_side = ob["asks"] if side == "Buy" else ob["bids"]
    best_price = float(book_side[0][0])
    if best_price <= 0:
        return OpenLegResult(exchange="bybit", direction=direction, ok=False, error="bad orderbook best price")

    f = await _bybit_fetch_instrument_filters(exchange_obj=exchange_obj, symbol=symbol)
    qty_step_raw = f.get("qtyStep")
    min_qty_raw = f.get("minOrderQty")
    min_amt_raw = f.get("minOrderAmt")
    tick_raw = f.get("tickSize")
    qty_step = float(qty_step_raw) if qty_step_raw else 0.0
    min_qty = float(min_qty_raw) if min_qty_raw else 0.0
    min_amt = float(min_amt_raw) if min_amt_raw else 0.0
    tick = float(tick_raw) if tick_raw else 0.0

    if qty_step > 0 and not _is_multiple_of_step(coin_amount, qty_step):
        return OpenLegResult(exchange="bybit", direction=direction, ok=False, error=f"qty {coin_amount} not multiple of qtyStep {qty_step_raw}")
    if min_qty > 0 and coin_amount < min_qty:
        return OpenLegResult(exchange="bybit", direction=direction, ok=False, error=f"qty {coin_amount} < minOrderQty {min_qty_raw}")
    notional = coin_amount * best_price
    if min_amt > 0 and notional < min_amt:
        return OpenLegResult(
            exchange="bybit", direction=direction, ok=False,
            error=f"Order does not meet minimum order value {min_amt:.3f}USDT (requested ~{notional:.3f}USDT)",
        )

    qty_str = _format_by_step(coin_amount, qty_step_raw)
    logger.info(f"План Bybit: {direction} qty={qty_str} | best={_format_number(best_price)} | уровни 1..{MAX_ORDERBOOK_LEVELS} (IOC)")

    return {
        "exchange": "bybit",
        "direction": direction,
        "exchange_obj": exchange_obj,
        "symbol": symbol,
        "side": side,
        "coin": coin,
        "coin_amount": coin_amount,
        "qty": qty_str,
        "qty_step_raw": qty_step_raw,
        "tick_raw": tick_raw,
        "api_key": api_key,
        "api_secret": api_secret,
    }


async def _bybit_place_leg(*, planned: Dict[str, Any]) -> OpenLegResult:
    exchange_obj = planned["exchange_obj"]
    api_key = planned["api_key"]
    api_secret = planned["api_secret"]
    direction = planned["direction"]
    side = planned["side"]
    coin = planned["coin"]
    coin_amount = float(planned["coin_amount"])
    tick_raw = planned.get("tick_raw")
    tick = float(tick_raw) if tick_raw else 0.0
    qty_step_raw = planned.get("qty_step_raw")

    total_filled = 0.0
    total_notional = 0.0
    last_order_id: Optional[str] = None

    for level_idx in range(1, MAX_ORDERBOOK_LEVELS + 1):
        remaining = coin_amount - total_filled
        if remaining <= 0:
            break
        ob = await exchange_obj.get_orderbook(coin, limit=MAX_ORDERBOOK_LEVELS)
        if not ob or not ob.get("bids") or not ob.get("asks"):
            break
        book_side = ob["asks"] if side == "Buy" else ob["bids"]
        if level_idx > len(book_side):
            break
        try:
            price_raw = float(book_side[level_idx - 1][0])
        except (IndexError, TypeError, ValueError):
            break
        px = _round_price_for_side(price_raw, tick, "buy" if side == "Buy" else "sell")
        px_str = _format_by_step(px, tick_raw)
        qty_str = _format_by_step(remaining, qty_step_raw)
        if float(qty_str) <= 0:
            break
        logger.info(f"Bybit: уровень {level_idx}/{MAX_ORDERBOOK_LEVELS} | {direction} qty={qty_str} | лимит={px_str} | осталось={_format_number(remaining)}")
        body = {
            "category": "linear",
            "symbol": planned["symbol"],
            "side": side,
            "orderType": "Limit",
            "qty": qty_str,
            "price": px_str,
            "timeInForce": "IOC",
        }
        data = await _bybit_private_post(exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret, path="/v5/order/create", body=body)
        if not isinstance(data, dict) or data.get("retCode") != 0:
            return OpenLegResult(exchange="bybit", direction=direction, ok=False, error=f"api error: {data}", raw=data)
        order_id = (data.get("result") or {}).get("orderId") if isinstance(data.get("result"), dict) else None
        if not order_id:
            return OpenLegResult(exchange="bybit", direction=direction, ok=False, error=f"no orderId in response: {data}", raw=data)
        last_order_id = str(order_id)
        ok_full, filled_qty = await _bybit_wait_full_fill(
            planned={**planned, "qty": qty_str, "price_str": px_str, "limit_price": px},
            order_id=last_order_id,
        )
        total_filled += filled_qty
        total_notional += filled_qty * px
        if filled_qty > 0:
            logger.info(f"Bybit: уровень {level_idx} исполнено={_format_number(filled_qty)} | всего={_format_number(total_filled)}")

    if total_filled >= coin_amount - 1e-9:
        avg_price = total_notional / total_filled if total_filled > 0 else None
        return OpenLegResult(
            exchange="bybit", direction=direction, ok=True,
            order_id=last_order_id, filled_qty=total_filled, avg_price=avg_price, raw=None,
        )
    return OpenLegResult(
        exchange="bybit", direction=direction, ok=False,
        error=f"не набрали объём за {MAX_ORDERBOOK_LEVELS} уровней: исполнено={_format_number(total_filled)}, требовалось={_format_number(coin_amount)}",
    )


async def _gate_private_post(*, exchange_obj: Any, api_key: str, api_secret: str, path: str, body: Dict[str, Any]) -> Any:
    body_json = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
    method = "POST"
    query_string = ""
    ts = str(int(time.time()))
    payload_hash = hashlib.sha512(body_json.encode("utf-8")).hexdigest()
    sign_str = "\n".join([method, path, query_string, payload_hash, ts])
    sign = hmac.new(api_secret.encode("utf-8"), sign_str.encode("utf-8"), hashlib.sha512).hexdigest()
    headers = {
        "KEY": api_key,
        "Timestamp": ts,
        "SIGN": sign,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    try:
        resp = await exchange_obj.client.request(method, path, headers=headers, content=body_json)
    except Exception as e:
        return {"_error": f"http error: {type(e).__name__}: {e}"}
    if resp.status_code < 200 or resp.status_code >= 300:
        return {"_error": f"http {resp.status_code}", "_body": resp.text[:400]}
    try:
        return resp.json()
    except Exception:
        return {"_error": "bad json", "_body": resp.text[:400]}


async def _gate_fetch_contract_info(*, exchange_obj: Any, contract: str) -> Any:
    try:
        resp = await exchange_obj.client.request("GET", f"/api/v4/futures/usdt/contracts/{contract}")
        if resp.status_code < 200 or resp.status_code >= 300:
            return None
        return resp.json()
    except Exception:
        return None


def _gate_price_step_from_contract_info(info: Dict[str, Any]) -> Optional[float]:
    raw = info.get("order_price_round") or info.get("order_price_precision")
    if raw is None:
        return None
    try:
        return float(raw)
    except Exception:
        return None


async def _gate_set_isolated_and_leverage_1(*, exchange_obj: Any, contract: str) -> Tuple[bool, str]:
    """
    Gate USDT futures: пробуем выставить leverage=1 и isolated (через cross_leverage_limit=0).
    Если endpoint/поля отличаются — вернется ошибка, и мы не будем открывать ордера.
    """
    api_key = _get_env("GATEIO_API_KEY")
    api_secret = _get_env("GATEIO_API_SECRET")
    if not api_key or not api_secret:
        return False, "missing GATEIO_API_KEY/GATEIO_API_SECRET"
    path = f"/api/v4/futures/usdt/positions/{contract}/leverage"
    body = {"leverage": "1", "cross_leverage_limit": "0"}
    data = await _gate_private_post(exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret, path=path, body=body)
    if isinstance(data, dict) and data.get("_error"):
        return False, str(data)
    if isinstance(data, dict) and ("label" in data or "message" in data) and ("leverage" not in data):
        return False, str(data)
    return True, "ok"


async def _gate_plan_leg(*, exchange_obj: Any, coin: str, direction: str, coin_amount: float) -> Any:
    api_key = _get_env("GATEIO_API_KEY")
    api_secret = _get_env("GATEIO_API_SECRET")
    if not api_key or not api_secret:
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error="missing GATEIO_API_KEY/GATEIO_API_SECRET in env")

    contract = exchange_obj._normalize_symbol(coin)
    ob = await exchange_obj.get_orderbook(coin, limit=MAX_ORDERBOOK_LEVELS)
    if not ob or not ob.get("bids") or not ob.get("asks"):
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"orderbook not available for {coin}")
    best_bid = float(ob["bids"][0][0])
    best_ask = float(ob["asks"][0][0])
    if best_bid <= 0 or best_ask <= 0:
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"bad orderbook prices: bid={best_bid} ask={best_ask}")

    cinfo = await _gate_fetch_contract_info(exchange_obj=exchange_obj, contract=contract)
    if not isinstance(cinfo, dict):
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"contract info not available for {contract}")
    qmul_raw = cinfo.get("quanto_multiplier") or cinfo.get("contract_size") or cinfo.get("multiplier")
    try:
        qmul = float(qmul_raw)
    except Exception:
        qmul = None
    if qmul is None or qmul <= 0:
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"bad quanto_multiplier for {contract}: {qmul_raw}")
    min_raw = cinfo.get("order_size_min")
    min_size = int(float(min_raw)) if min_raw is not None else None
    contracts_exact = coin_amount / qmul
    contracts_i = int(round(contracts_exact))
    if abs(contracts_exact - contracts_i) > 1e-9:
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"qty {coin_amount} {coin} not compatible with contract size (quanto_multiplier={qmul}) => contracts={contracts_exact:.8f} (must be integer)")
    if contracts_i <= 0:
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"contracts computed as {contracts_i}")
    if min_size is not None and contracts_i < min_size:
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"contracts {contracts_i} < min {min_size}")

    side = "buy" if direction == "long" else "sell"
    price_step = _gate_price_step_from_contract_info(cinfo) or 0.0
    logger.info(f"План Gate: {direction} contracts={contracts_i} | best_bid={_format_number(best_bid)} best_ask={_format_number(best_ask)} | уровни 1..{MAX_ORDERBOOK_LEVELS} (IOC)")

    return {
        "exchange": "gate",
        "direction": direction,
        "exchange_obj": exchange_obj,
        "contract": contract,
        "coin": coin,
        "coin_amount": coin_amount,
        "contracts_i": contracts_i,
        "qmul": qmul,
        "price_step": price_step,
        "api_key": api_key,
        "api_secret": api_secret,
    }


async def _gate_place_leg(*, planned: Dict[str, Any]) -> OpenLegResult:
    exchange_obj = planned["exchange_obj"]
    api_key = planned["api_key"]
    api_secret = planned["api_secret"]
    direction = planned["direction"]
    coin = planned["coin"]
    contracts_i = int(planned["contracts_i"])
    qmul = float(planned["qmul"])
    coin_amount = float(planned["coin_amount"])
    price_step = float(planned.get("price_step") or 0.0)
    side = "buy" if direction == "long" else "sell"
    sign = 1 if direction == "long" else -1

    total_filled_base = 0.0
    total_notional = 0.0
    remaining_contracts = contracts_i
    last_order_id: Optional[str] = None

    for level_idx in range(1, MAX_ORDERBOOK_LEVELS + 1):
        if remaining_contracts <= 0:
            break
        ob = await exchange_obj.get_orderbook(coin, limit=MAX_ORDERBOOK_LEVELS)
        if not ob or not ob.get("bids") or not ob.get("asks"):
            break
        book_side = ob["asks"] if side == "buy" else ob["bids"]
        if level_idx > len(book_side):
            break
        try:
            price_raw = float(book_side[level_idx - 1][0])
        except (IndexError, TypeError, ValueError):
            break
        px = _round_price_for_side(price_raw, price_step, side)
        px_str = _format_by_step(px, str(price_step) if price_step > 0 else None)
        size_signed = sign * remaining_contracts
        logger.info(f"Gate: уровень {level_idx}/{MAX_ORDERBOOK_LEVELS} | {direction} size={size_signed} | лимит={px_str} | осталось_контрактов={remaining_contracts}")
        body = {
            "contract": planned["contract"],
            "size": size_signed,
            "price": px_str,
            "tif": "ioc",
        }
        data = await _gate_private_post(exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret, path="/api/v4/futures/usdt/orders", body=body)
        if not (isinstance(data, dict) and data.get("id") is not None and ("label" not in data) and ("message" not in data)):
            return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"api error: {data}", raw=data)
        order_id = str(data.get("id"))
        last_order_id = order_id
        ok_full, filled_base = await _gate_wait_full_fill(planned={**planned, "size": size_signed, "price_str": px_str, "limit_price": px}, order_id=order_id)
        filled_contracts = int(filled_base / qmul) if qmul > 0 else 0
        remaining_contracts -= filled_contracts
        total_filled_base += filled_base
        total_notional += filled_base * px
        if filled_base > 0:
            logger.info(f"Gate: уровень {level_idx} исполнено={_format_number(filled_base)} base | всего base={_format_number(total_filled_base)}")

    if total_filled_base >= coin_amount - 1e-9:
        avg_price = total_notional / total_filled_base if total_filled_base > 0 else None
        return OpenLegResult(
            exchange="gate", direction=direction, ok=True,
            order_id=last_order_id, filled_qty=total_filled_base, avg_price=avg_price, raw=None,
        )
    return OpenLegResult(
        exchange="gate", direction=direction, ok=False,
        error=f"не набрали объём за {MAX_ORDERBOOK_LEVELS} уровней: исполнено={_format_number(total_filled_base)}, требовалось={_format_number(coin_amount)}",
    )


# =========================
# XT.com Futures trading
# =========================

async def get_xt_fees_from_trades(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    coin: str,
    direction: str,  # "long" -> BUY trades, "short" -> SELL trades (совместимо с scan_fundings_spreads_bot)
    start_ms: int,
    end_ms: int,
) -> Optional[float]:
    """
    Пытается получить комиссии XT Futures из trade-list за окно времени.
    Возвращает сумму комиссий в USDT или None если не удалось.
    """
    try:
        symbol = exchange_obj._normalize_symbol(coin)
        want_side = "BUY" if (direction or "").lower().strip() == "long" else "SELL"

        # XT docs for trade-list params are inconsistent; пробуем несколько вариантов params.
        params_variants: List[Dict[str, Any]] = [
            {"symbol": symbol, "startTime": str(start_ms), "endTime": str(end_ms)},
            {"symbol": symbol, "startTime": start_ms, "endTime": end_ms},
            {"symbol": symbol},
            {"startTime": str(start_ms), "endTime": str(end_ms)},
        ]

        data: Any = None
        for params in params_variants:
            data = await _xt_private_request(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                method="GET",
                path="/future/trade/v1/order/trade-list",
                params=params,
            )
            if isinstance(data, dict) and data.get("returnCode") == 0:
                break

        if not isinstance(data, dict) or data.get("returnCode") != 0:
            return None

        # Extract trades list from result
        trades: List[Dict[str, Any]] = []
        r = data.get("result")
        if isinstance(r, list):
            trades = [x for x in r if isinstance(x, dict)]
        elif isinstance(r, dict):
            for k in ("list", "items", "data", "rows", "trades"):
                v = r.get(k)
                if isinstance(v, list):
                    trades = [x for x in v if isinstance(x, dict)]
                    break
        if not trades:
            return None

        fee_total = 0.0
        for t in trades:
            side = str(t.get("side") or t.get("orderSide") or t.get("tradeSide") or "").upper()
            if side and side != want_side:
                continue

            fee_raw = (
                t.get("fee")
                or t.get("commission")
                or t.get("tradeFee")
                or t.get("execFee")
                or t.get("feeAmount")
                or t.get("feeValue")
            )
            if fee_raw is None:
                continue
            try:
                fee_val = abs(float(fee_raw))
            except Exception:
                continue

            fee_coin = str(t.get("feeCoin") or t.get("feeAsset") or t.get("commissionAsset") or "USDT")
            if fee_coin.upper() in ("USDT", "USDC"):
                fee_total += fee_val
                continue

            # If fee is in base coin, try convert using trade price
            px_raw = t.get("price") or t.get("execPrice") or t.get("dealPrice") or t.get("tradePrice")
            if px_raw is not None and fee_coin.lower() in (coin.lower(), coin.lower() + "usdt"):
                try:
                    fee_total += fee_val * float(px_raw)
                except Exception:
                    pass

        return fee_total if fee_total > 0 else None
    except Exception:
        return None

async def _xt_private_request(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
) -> Any:
    """
    XT.com Futures API signing:
    - Headers (X): validate-algorithms, validate-appkey, validate-recvwindow, validate-timestamp (sorted, joined with &)
    - Data (Y): #method#path#query#body
    - Signature: HMAC_SHA256(secretKey, X + Y) as hex
    - Header: validate-signature
    """
    from urllib.parse import urlencode

    method_u = method.upper()
    ts = str(int(time.time() * 1000))
    recv_window = str(int(float(os.getenv("XT_RECV_WINDOW", "5000"))))

    is_future_api = path.startswith("/future/")
    use_json_for_body = is_future_api  # фьючерсные приватные эндпоинты в SDK используют JSON

    # -------------------------
    # Scheme A (FUTURES): follow official XT futures SDK behavior
    # - X includes ONLY validate-appkey + validate-timestamp
    # - Y is "#path" or "#path#payload" (NO method)
    # - signature is UPPERCASE hex
    # -------------------------
    if is_future_api:
        x_header = {
            "validate-appkey": api_key,
            "validate-timestamp": ts,
        }
        X = urlencode(dict(sorted(x_header.items(), key=lambda kv: (kv[0], kv[1]))))

        body_str_for_sign = ""
        content_type = "application/json;charset=UTF-8"

        if params and not body:
            # urlencoded mode (like SDK's payload.data["urlencoded"]=True)
            tmp = urlencode(
                dict(
                    sorted(
                        [(k, str(v)) for k, v in params.items() if v is not None],
                        key=lambda kv: (kv[0], kv[1]),
                    )
                ),
                safe=",",
            )
            Y = f"#{path}#{tmp}"
            content_type = "application/x-www-form-urlencoded"
        elif body:
            # IMPORTANT: SDK signs json.dumps(...) DEFAULT formatting (with spaces)
            # To avoid mismatch, we send exactly the same bytes we sign.
            body_str_for_sign = json.dumps(body, ensure_ascii=True)
            Y = f"#{path}#{body_str_for_sign}"
        else:
            Y = f"#{path}"

        original = X + Y
        sign = hmac.new(api_secret.encode("utf-8"), original.encode("utf-8"), hashlib.sha256).hexdigest().upper()

        # Even though futures-SDK signs only appkey+timestamp, XT API often expects
        # validate-algorithms/validate-recvwindow headers to be present.
        headers = {
            "validate-algorithms": "HmacSHA256",
            "validate-appkey": api_key,
            "validate-recvwindow": recv_window,
            "validate-timestamp": ts,
            "validate-signature": sign,
            "Content-Type": content_type,
            "Accept": "application/json",
        }

        req_kwargs: Dict[str, Any] = {"headers": headers}
        if params and not body:
            req_kwargs["params"] = params
        if body:
            req_kwargs["content"] = body_str_for_sign.encode("utf-8")

    # -------------------------
    # Scheme B (V4 / others): follow XT docs
    # - X includes validate-algorithms/appkey/recvwindow/timestamp
    # - Y is #METHOD#path#query#body
    # - signature is lowercase hex
    # -------------------------
    else:
        x_parts = [
            ("validate-algorithms", "HmacSHA256"),
            ("validate-appkey", api_key),
            ("validate-recvwindow", recv_window),
            ("validate-timestamp", ts),
        ]
        x_parts_sorted = sorted([(k, str(v)) for (k, v) in x_parts], key=lambda kv: kv[0])
        header_part = "&".join([f"{k}={v}" for (k, v) in x_parts_sorted])

        query = ""
        if params:
            q_pairs = sorted([(k, str(v)) for k, v in params.items() if v is not None], key=lambda kv: kv[0])
            query = "&".join([f"{k}={v}" for (k, v) in q_pairs])

        body_str_for_sign = ""
        if body:
            b_pairs = sorted([(k, str(v)) for k, v in body.items() if v is not None], key=lambda kv: kv[0])
            body_str_for_sign = "&".join([f"{k}={v}" for (k, v) in b_pairs])

        if query and body_str_for_sign:
            data_part = f"#{method_u}#{path}#{query}#{body_str_for_sign}"
        elif query and (not body_str_for_sign):
            data_part = f"#{method_u}#{path}#{query}"
        elif (not query) and body_str_for_sign:
            data_part = f"#{method_u}#{path}#{body_str_for_sign}"
        else:
            data_part = f"#{method_u}#{path}"

        original = header_part + data_part
        sign = hmac.new(api_secret.encode("utf-8"), original.encode("utf-8"), hashlib.sha256).hexdigest()

        headers = {
            "validate-algorithms": "HmacSHA256",
            "validate-appkey": api_key,
            "validate-recvwindow": recv_window,
            "validate-timestamp": ts,
            "validate-signature": sign,
        }
        req_kwargs = {"headers": headers}
        if body_str_for_sign:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            req_kwargs["content"] = body_str_for_sign
        else:
            headers["Content-Type"] = "application/json"
            headers["Accept"] = "application/json"
        if params:
            req_kwargs["params"] = params
    
    try:
        resp = await exchange_obj.client.request(method_u, path, **req_kwargs)
    except Exception as e:
        return {"_error": f"http error: {type(e).__name__}: {e}"}
    if resp.status_code < 200 or resp.status_code >= 300:
        return {"_error": f"http {resp.status_code}", "_body": resp.text[:400]}
    try:
        return resp.json()
    except Exception:
        return {"_error": "bad json", "_body": resp.text[:400]}


async def _xt_fetch_instrument_filters(*, exchange_obj: Any, symbol: str) -> Dict[str, Optional[str]]:
    """
    Возвращает фильтры инструмента XT.com (fapi): qtyStep, minOrderQty, minOrderAmt, tickSize, contractSize.
    Пробует несколько эндпоинтов для получения информации об инструменте.
    """
    def _precision_to_step_str(raw: Any) -> Optional[str]:
        if raw is None:
            return None
        # Sometimes XT returns *precision* as integer (number of decimals), not a step size.
        try:
            p = int(str(raw))
            if 0 <= p < 20:
                return "1" if p == 0 else str(10 ** (-p))
        except Exception:
            pass
        # Otherwise treat as already a step value
        try:
            s = str(raw).strip()
            return s if s else None
        except Exception:
            return None

    # Official SDK uses /future/market/v1/public/symbol/detail for symbol metadata.
    endpoints = [
        "/future/market/v1/public/symbol/detail",
        # fallbacks (older/alternative)
        "/future/market/v1/public/q/instrument",
        "/future/market/v1/public/q/symbol",
        "/future/market/v1/public/q/contract",
    ]
    
    for endpoint in endpoints:
        try:
            resp = await exchange_obj.client.request(
                "GET",
                endpoint,
                params={"symbol": symbol},
            )
            if resp.status_code < 200 or resp.status_code >= 300:
                continue
            data = resp.json()
            if not isinstance(data, dict) or data.get("returnCode") != 0:
                continue
            result = data.get("result")
            info: Optional[Dict[str, Any]] = None
            if isinstance(result, dict):
                info = result
            elif isinstance(result, list) and result and isinstance(result[0], dict):
                info = result[0]
            if not isinstance(info, dict):
                continue
            
            # Extract filters from info - try multiple keys.
            qty_step = info.get("qtyStep") or info.get("stepSize") or info.get("lotSize")
            qty_precision = info.get("quantityPrecision") or info.get("qtyPrecision") or info.get("qtyScale")
            if qty_step is None and qty_precision is not None:
                qty_step = _precision_to_step_str(qty_precision)
            else:
                qty_step = _precision_to_step_str(qty_step)

            min_qty = info.get("minOrderQty") or info.get("minQty") or info.get("minQuantity")
            min_amt = info.get("minOrderAmt") or info.get("minNotional") or info.get("minOrderValue")

            tick_size = info.get("tickSize") or info.get("priceTick") or info.get("tick")
            price_precision = info.get("pricePrecision") or info.get("priceScale")
            if tick_size is None and price_precision is not None:
                tick_size = _precision_to_step_str(price_precision)
            else:
                tick_size = _precision_to_step_str(tick_size)

            contract_size = (
                info.get("contractSize")
                or info.get("contractUnit")
                or info.get("contractValue")
                or info.get("contractVal")
                or info.get("multiplier")
                or info.get("contractMultiplier")
                or info.get("contractMul")
            )
            
            return {
                "qtyStep": str(qty_step) if qty_step is not None else None,
                "minOrderQty": str(min_qty) if min_qty is not None else None,
                "minOrderAmt": str(min_amt) if min_amt is not None else None,
                "tickSize": str(tick_size) if tick_size is not None else None,
                "contractSize": str(contract_size) if contract_size is not None else None,
            }
        except Exception:
            continue
    
    # Fallback: возвращаем пустой dict, код будет использовать значения по умолчанию
    logger.warning(f"⚠️ XT: не удалось получить фильтры для {symbol}, используем значения по умолчанию")
    return {}


async def _xt_plan_leg(*, exchange_obj: Any, coin: str, direction: str, coin_amount: float) -> Any:
    api_key = _get_env("XT_API_KEY")
    api_secret = _get_env("XT_API_SECRET")
    if not api_key or not api_secret:
        return OpenLegResult(exchange="xt", direction=direction, ok=False, error="missing XT_API_KEY/XT_API_SECRET in env")
    
    symbol = exchange_obj._normalize_symbol(coin)
    order_side = "BUY" if direction == "long" else "SELL"
    
    ob = await exchange_obj.get_orderbook(coin, limit=MAX_ORDERBOOK_LEVELS)
    if not ob or not ob.get("bids") or not ob.get("asks"):
        return OpenLegResult(exchange="xt", direction=direction, ok=False, error=f"orderbook not available for {coin}")
    book_side = ob["asks"] if order_side == "BUY" else ob["bids"]
    best_price = float(book_side[0][0])
    if best_price <= 0:
        return OpenLegResult(exchange="xt", direction=direction, ok=False, error="bad orderbook best price")
    
    f = await _xt_fetch_instrument_filters(exchange_obj=exchange_obj, symbol=symbol)
    qty_step_raw = f.get("qtyStep")
    min_qty_raw = f.get("minOrderQty")
    min_amt_raw = f.get("minOrderAmt")
    tick_raw = f.get("tickSize")
    contract_size_raw = f.get("contractSize")

    # NOTE: On XT futures, origQty is typically in CONTRACTS, not in base coins.
    # We keep public API of our bot in "coin_amount" (base coins), but convert to contracts for XT.
    contract_size = float(contract_size_raw) if contract_size_raw else 1.0
    if contract_size <= 0:
        contract_size = 1.0

    qty_step = float(qty_step_raw) if qty_step_raw else 0.0001  # step in contracts
    min_qty = float(min_qty_raw) if min_qty_raw else 0.0         # min in contracts
    min_amt = float(min_amt_raw) if min_amt_raw else 0.0
    tick = float(tick_raw) if tick_raw else 0.0001

    contracts = coin_amount / contract_size
    if qty_step > 0 and not _is_multiple_of_step(contracts, qty_step):
        return OpenLegResult(
            exchange="xt",
            direction=direction,
            ok=False,
            error=f"qty {coin_amount} {coin} (= {contracts} contracts) not multiple of qtyStep {qty_step_raw}",
        )
    if min_qty > 0 and contracts < min_qty:
        min_coins = min_qty * contract_size
        return OpenLegResult(
            exchange="xt",
            direction=direction,
            ok=False,
            error=f"qty {coin_amount} {coin} (< {min_coins} {coin} min) | minOrderQty={min_qty_raw} contracts | contractSize={contract_size_raw}",
        )
    notional = coin_amount * best_price
    if min_amt > 0 and notional < min_amt:
        return OpenLegResult(
            exchange="xt", direction=direction, ok=False,
            error=f"Order does not meet minimum order value {min_amt:.3f}USDT (requested ~{notional:.3f}USDT)",
        )

    # Display qty in coins (user-facing), but store contracts for order placement.
    qty_coin_str = _format_number(coin_amount)
    qty_contracts_str = _format_by_step(contracts, qty_step_raw) if qty_step_raw else _format_number(contracts)
    logger.info(f"План XT: {direction} qty={qty_coin_str} | best={_format_number(best_price)} | уровни 1..{MAX_ORDERBOOK_LEVELS} (IOC)")
    
    return {
        "exchange": "xt",
        "direction": direction,
        "exchange_obj": exchange_obj,
        "symbol": symbol,
        "order_side": order_side,
        "coin": coin,
        "coin_amount": coin_amount,
        "qty": qty_coin_str,
        "contracts_qty": contracts,
        "contracts_qty_str": qty_contracts_str,
        "contract_size": contract_size,
        "qty_step_raw": qty_step_raw,
        "tick_raw": tick_raw,
        "api_key": api_key,
        "api_secret": api_secret,
    }


async def _xt_place_leg(*, planned: Dict[str, Any]) -> OpenLegResult:
    exchange_obj = planned["exchange_obj"]
    api_key = planned["api_key"]
    api_secret = planned["api_secret"]
    direction = planned["direction"]
    order_side = planned["order_side"]
    coin = planned["coin"]
    coin_amount = float(planned["coin_amount"])
    contract_size = float(planned.get("contract_size") or 1.0)
    if contract_size <= 0:
        contract_size = 1.0
    tick_raw = planned.get("tick_raw")
    tick = float(tick_raw) if tick_raw else 0.0
    qty_step_raw = planned.get("qty_step_raw")
    
    total_filled = 0.0
    total_notional = 0.0
    last_order_id: Optional[str] = None
    
    for level_idx in range(1, MAX_ORDERBOOK_LEVELS + 1):
        remaining = coin_amount - total_filled
        if remaining <= 0:
            break
        ob = await exchange_obj.get_orderbook(coin, limit=MAX_ORDERBOOK_LEVELS)
        if not ob or not ob.get("bids") or not ob.get("asks"):
            break
        book_side = ob["asks"] if order_side == "BUY" else ob["bids"]
        if level_idx > len(book_side):
            break
        try:
            price_raw = float(book_side[level_idx - 1][0])
        except (IndexError, TypeError, ValueError):
            break
        if price_raw <= 0:
            logger.error(f"❌ XT: invalid price_raw={price_raw} at level {level_idx}")
            break
        px = _round_price_for_side(price_raw, tick, "buy" if order_side == "BUY" else "sell")
        if px <= 0:
            logger.error(f"❌ XT: rounded price <= 0: px={px}, price_raw={price_raw}, tick={tick}")
            break
        px_str = _format_by_step(px, tick_raw) if tick_raw else str(px)
        remaining_contracts = remaining / contract_size
        qty_contracts_str = _format_by_step(remaining_contracts, qty_step_raw) if qty_step_raw else _format_number(remaining_contracts)
        try:
            if float(qty_contracts_str) <= 0:
                break
        except Exception:
            break
        # Log qty in coins (user-facing), send qty in contracts to XT
        logger.info(
            f"XT: уровень {level_idx}/{MAX_ORDERBOOK_LEVELS} | {direction} qty={_format_number(remaining)} | "
            f"лимит={px_str} | осталось={_format_number(remaining)}"
        )
        
        # XT.com order creation endpoint - пробуем несколько вариантов
        # Параметры должны быть строками
        # positionSide обязателен для фьючерсов: LONG для long позиции, SHORT для short позиции
        position_side = "LONG" if direction == "long" else "SHORT"
        
        # Для /v4/order могут быть другие названия параметров (side вместо orderSide, type вместо orderType)
        # Пробуем оба варианта в зависимости от эндпоинта
        # Для фьючерсов может потребоваться positionSide даже в /v4/order
        body_future = {
            "symbol": str(planned["symbol"]),
            "orderSide": str(order_side),  # Для /future/trade/v1/*
            "orderType": "LIMIT",
            "origQty": str(qty_contracts_str),  # contracts
            "price": str(px_str),
            "timeInForce": "IOC",
            "positionSide": position_side,
        }
        
        # Пробуем правильные эндпоинты для создания ордеров на фьючерсах XT.com
        # Из официального SDK: правильный эндпоинт - /future/trade/v1/order/create с json=data
        # /v4/order не предназначен для фьючерсов (вернул returnCode=0, но без orderId)
        # Futures: official SDK uses ONLY /future/trade/v1/order/create
        # Other endpoints here were returning 404 or non-actionable responses.
        endpoints_to_try = [
            ("/future/trade/v1/order/create", body_future),
        ]
        
        data = None
        last_error = None
        successful_data = None
        
        for endpoint, body_to_use in endpoints_to_try:
            data = await _xt_private_request(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                method="POST",
                path=endpoint,
                body=body_to_use,
            )
            
            if isinstance(data, dict) and data.get("returnCode") == 0:
                # Проверяем, есть ли orderId в ответе
                result = data.get("result")
                order_id_check = None
                if isinstance(result, dict):
                    order_id_check = result.get("orderId") or result.get("order_id") or result.get("id") or result.get("orderIdStr")
                elif isinstance(result, (str, int)) and str(result).strip():
                    # XT может вернуть orderId строкой прямо в result
                    order_id_check = str(result).strip()
                if not order_id_check:
                    order_id_check = data.get("orderId") or data.get("order_id") or data.get("id")
                if not order_id_check and isinstance(data.get("data"), dict):
                    order_id_check = data.get("data").get("orderId") or data.get("data").get("id")
                
                # Если orderId найден - используем этот ответ
                if order_id_check:
                    successful_data = data
                    break
                # Если orderId не найден, но returnCode=0 - возможно неправильный эндпоинт
                # Продолжаем пробовать другие эндпоинты
                # У некоторых эндпоинтов result может быть строкой/None → не вызываем .get() без проверки
                r0 = data.get("result")
                if isinstance(r0, dict) and r0.get("openapiDocs"):
                    logger.warning(f"⚠️ XT: {endpoint} вернул returnCode=0, но без orderId (только openapiDocs), пробуем следующий эндпоинт")
                    continue
                # Если нет openapiDocs, но и нет orderId - тоже продолжаем
                logger.warning(f"⚠️ XT: {endpoint} вернул returnCode=0, но orderId не найден. result_type={type(r0).__name__}")
                continue
            
            if isinstance(data, dict) and data.get("_error"):
                # Логируем ошибки для отладки
                error_msg = str(data.get("_error", ""))
                error_body = str(data.get("_body", ""))[:200]
                logger.warning(f"⚠️ XT: {endpoint} вернул ошибку: {error_msg} | body: {error_body}")
                # Если ошибка не 404 (endpoint не найден), сохраняем её
                if "404" not in error_msg:
                    last_error = data
                continue
            
            # Если returnCode != 0, логируем подробнее (важно видеть data['error'])
            if isinstance(data, dict) and data.get("returnCode") != 0:
                logger.warning(
                    f"⚠️ XT: {endpoint} вернул returnCode={data.get('returnCode')}, "
                    f"msgInfo={data.get('msgInfo', 'N/A')}, error={data.get('error')}"
                )
                last_error = data
                continue
        
        # Используем успешный ответ с orderId, если есть
        if successful_data:
            data = successful_data
        elif data is None:
            data = last_error or {"_error": "all endpoints failed"}
        
        if not isinstance(data, dict) or data.get("returnCode") != 0:
            return OpenLegResult(exchange="xt", direction=direction, ok=False, error=f"api error: {data}", raw=data)
        
        # XT.com может возвращать orderId в разных местах
        # Проверяем несколько возможных путей
        order_id = None
        result = data.get("result")
        
        # Вариант 1: result.orderId или result.id
        if isinstance(result, dict):
            order_id = result.get("orderId") or result.get("order_id") or result.get("id") or result.get("orderIdStr")
        elif isinstance(result, (str, int)) and str(result).strip():
            order_id = str(result).strip()
        
        # Вариант 2: напрямую в data
        if not order_id:
            order_id = data.get("orderId") or data.get("order_id") or data.get("id")
        
        # Вариант 3: в data.data (если есть вложенная структура)
        if not order_id and isinstance(data.get("data"), dict):
            order_id = data.get("data").get("orderId") or data.get("data").get("id")
        
        # Если orderId все еще не найден после всех попыток
        if not order_id:
            logger.error(f"❌ XT: orderId не найден в ответе после всех попыток. Response: {str(data)[:500]}")
            return OpenLegResult(exchange="xt", direction=direction, ok=False, error=f"no orderId in response (returnCode=0): {str(data)[:300]}", raw=data)
        
        last_order_id = str(order_id)

        # Get fill from order detail (executedQty is typically in contracts).
        # IMPORTANT: don't send next IOC order until this one is in a final state,
        # otherwise we risk overfilling (XT can lag in order updates).
        await asyncio.sleep(0.15)

        detail: Any = None
        for _poll in range(8):  # up to ~1.6s
            detail = await _xt_private_request(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                method="GET",
                path="/future/trade/v1/order/detail",
                params={"orderId": last_order_id},
            )
            # If API error, retry briefly
            if not (isinstance(detail, dict) and detail.get("returnCode") == 0):
                await asyncio.sleep(0.2)
                continue
            # Try to detect a terminal status if present
            r_any = detail.get("result")
            status = None
            if isinstance(r_any, dict):
                status = r_any.get("status") or r_any.get("state") or r_any.get("orderStatus")
            if status is not None:
                st = str(status).upper()
                if st in ("FILLED", "CANCELED", "CANCELLED", "DONE", "FINISHED", "CLOSED", "PARTIALLY_FILLED", "PARTIALLYFILLED"):
                    break
            # If no status field, do a few polls anyway
            await asyncio.sleep(0.2)

        def _first_dict(x: Any) -> Optional[Dict[str, Any]]:
            if isinstance(x, dict):
                return x
            if isinstance(x, list):
                for it in x:
                    if isinstance(it, dict):
                        return it
            return None

        def _find_num_by_keys(obj: Any, keys: Tuple[str, ...]) -> Optional[float]:
            # Depth-first search for a numeric field by key name
            if isinstance(obj, dict):
                for k in keys:
                    if k in obj and obj[k] is not None:
                        try:
                            return float(obj[k])
                        except Exception:
                            pass
                for v in obj.values():
                    got = _find_num_by_keys(v, keys)
                    if got is not None:
                        return got
            elif isinstance(obj, list):
                for it in obj:
                    got = _find_num_by_keys(it, keys)
                    if got is not None:
                        return got
            return None

        filled_contracts = 0.0
        avg_px = None
        if isinstance(detail, dict) and detail.get("returnCode") == 0:
            r_any = detail.get("result")
            r = _first_dict(r_any) or _first_dict(detail.get("data")) or _first_dict(detail)

            # Try common keys, then recursive search (XT responses vary)
            fc = None
            if isinstance(r, dict):
                fc_raw = r.get("executedQty") or r.get("filledQty") or r.get("cumQty") or r.get("dealQty") or r.get("executedQuantity")
                if fc_raw is not None:
                    try:
                        fc = float(fc_raw)
                    except Exception:
                        fc = None
            if fc is None:
                fc = _find_num_by_keys(r_any, ("executedQty", "filledQty", "cumQty", "dealQty", "executedQuantity"))
            if fc is not None:
                filled_contracts = float(fc)

            ap = None
            if isinstance(r, dict):
                ap_raw = r.get("avgPrice") or r.get("avgPx") or r.get("avg") or r.get("dealAvgPrice") or r.get("avgDealPrice")
                if ap_raw is not None:
                    try:
                        ap = float(ap_raw)
                    except Exception:
                        ap = None
            if ap is None:
                ap = _find_num_by_keys(r_any, ("avgPrice", "avgPx", "dealAvgPrice", "avgDealPrice"))
            if ap is not None:
                avg_px = float(ap)
        else:
            # If detail failed, log once (helps debug without crashing)
            if isinstance(detail, dict):
                logger.warning(f"⚠️ XT detail: returnCode={detail.get('returnCode')}, msgInfo={detail.get('msgInfo')}, error={detail.get('error')}")
            else:
                logger.warning(f"⚠️ XT detail: bad response type={type(detail).__name__} value={str(detail)[:200]}")

        filled_qty = filled_contracts * contract_size
        if avg_px is None:
            avg_px = px

        if filled_qty > 0:
            total_filled += filled_qty
            total_notional += filled_qty * float(avg_px)
            logger.info(f"XT: уровень {level_idx} исполнено={_format_number(filled_qty)} | всего={_format_number(total_filled)}")
        else:
            # IOC order: if 0 filled, we can move to next level; if parsing failed, this prevents silent looping
            logger.debug(f"XT: уровень {level_idx} 0 исполнено по order/detail | orderId={last_order_id}")
    
    if total_filled >= coin_amount - 1e-9:
        avg_price = total_notional / total_filled if total_filled > 0 else None
        return OpenLegResult(
            exchange="xt", direction=direction, ok=True,
            order_id=last_order_id, filled_qty=total_filled, avg_price=avg_price, raw=None,
        )
    return OpenLegResult(
        exchange="xt", direction=direction, ok=False,
        error=f"не набрали объём за {MAX_ORDERBOOK_LEVELS} уровней: исполнено={_format_number(total_filled)}, требовалось={_format_number(coin_amount)}",
    )


async def _xt_close_leg_partial_ioc(
    *,
    exchange_obj: Any,
    coin: str,
    position_direction: str,
    coin_amount: float,
    position_idx: Optional[int] = None,
) -> Tuple[bool, Optional[float]]:
    """
    Закрытие позиции на XT.com частями: limit + IOC + reduceOnly.
    """
    api_key = _get_env("XT_API_KEY")
    api_secret = _get_env("XT_API_SECRET")
    if not api_key or not api_secret:
        logger.error("❌ XT: missing XT_API_KEY/XT_API_SECRET in env")
        return False, None
    
    pos_dir = (position_direction or "").lower().strip()
    if pos_dir not in ("long", "short"):
        logger.error(f"❌ XT: некорректное направление позиции: {position_direction!r}")
        return False, None
    
    remaining = float(coin_amount)
    if remaining <= 0:
        return True, None
    
    symbol = exchange_obj._normalize_symbol(coin)
    order_side = "SELL" if pos_dir == "long" else "BUY"  # Close long = sell, close short = buy
    position_side = "LONG" if pos_dir == "long" else "SHORT"
    
    f = await _xt_fetch_instrument_filters(exchange_obj=exchange_obj, symbol=symbol)
    qty_step_raw = f.get("qtyStep")
    tick_raw = f.get("tickSize")
    contract_size_raw = f.get("contractSize")
    contract_size = float(contract_size_raw) if contract_size_raw else 1.0
    if contract_size <= 0:
        contract_size = 1.0
    # qtyStep/tick can be missing depending on XT response format. For futures, qty is in contracts.
    # If qtyStep is missing, assume integer contracts (step=1). This is safer than aborting close.
    if not qty_step_raw:
        logger.warning(f"⚠️ XT close: qtyStep отсутствует для {symbol}, fallback qtyStep=1 (контракты)")
        qty_step_raw = "1"
    qty_step = float(qty_step_raw) if qty_step_raw else 1.0

    if not tick_raw:
        logger.warning(f"⚠️ XT close: tickSize отсутствует для {symbol}, fallback tickSize=0.0001")
        tick_raw = "0.0001"
    tick = float(tick_raw) if tick_raw else 0.0001
    
    eps = max(1e-10, remaining * 1e-8)
    total_notional = 0.0
    total_filled = 0.0
    
    max_orders_total = max(10, MAX_ORDERBOOK_LEVELS * 3)
    CLOSE_OB_LEVELS = max(10, int(os.getenv("FUN_CLOSE_OB_LEVELS", "25") or "25"))
    MAX_TOTAL_SEC = float(os.getenv("FUN_CLOSE_MAX_TOTAL_SEC", "30.0") or "30.0")
    
    start_ts = time.time()
    
    def _first_dict(x: Any) -> Optional[Dict[str, Any]]:
        if isinstance(x, dict):
            return x
        if isinstance(x, list):
            for it in x:
                if isinstance(it, dict):
                    return it
        return None

    def _find_num_by_keys(obj: Any, keys: Tuple[str, ...]) -> Optional[float]:
        if isinstance(obj, dict):
            for k in keys:
                if k in obj and obj[k] is not None:
                    try:
                        return float(obj[k])
                    except Exception:
                        pass
            for v in obj.values():
                got = _find_num_by_keys(v, keys)
                if got is not None:
                    return got
        elif isinstance(obj, list):
            for it in obj:
                got = _find_num_by_keys(it, keys)
                if got is not None:
                    return got
        return None

    for order_n in range(1, max_orders_total + 1):
        if time.time() - start_ts > MAX_TOTAL_SEC:
            logger.error(f"❌ XT close: timeout {MAX_TOTAL_SEC}s exceeded | remaining={_format_number(remaining)} {coin}")
            avg_price = (total_notional / total_filled) if total_filled > 0 else None
            return False, avg_price
        
        if remaining <= eps:
            avg_price = (total_notional / total_filled) if total_filled > 0 else None
            return True, avg_price
        
        ob = await exchange_obj.get_orderbook(coin, limit=CLOSE_OB_LEVELS)
        if not ob or not ob.get("bids") or not ob.get("asks"):
            logger.error(f"❌ XT: orderbook недоступен для закрытия {coin}")
            return False, None
        
        levels = ob["bids"] if order_side == "SELL" else ob["asks"]
        if not isinstance(levels, list) or not levels:
            logger.error(f"❌ XT: пустой стакан для закрытия {coin}")
            return False, None
        
        # Choose price from orderbook level (like open logic), to gradually get more aggressive.
        level_idx = min(order_n, min(len(levels), CLOSE_OB_LEVELS))
        try:
            px_level = float(levels[level_idx - 1][0])
        except Exception:
            logger.error(f"❌ XT close: не удалось взять цену уровня {level_idx} для {coin}")
            return False, None
        
        # XT futures origQty is in contracts; convert remaining coins -> contracts
        remaining_contracts = remaining / contract_size
        qty_send_contracts = remaining_contracts
        if qty_step > 0:
            qty_send_contracts = _floor_to_step(qty_send_contracts, qty_step)
            if qty_send_contracts <= 0:
                avg_price = (total_notional / total_filled) if total_filled > 0 else None
                return True, avg_price
        qty_str = _format_by_step(qty_send_contracts, qty_step_raw) if qty_step > 0 else str(qty_send_contracts)
        px = _round_price_for_side(float(px_level), tick, "sell" if order_side == "SELL" else "buy")
        px_str = _format_by_step(px, tick_raw) if tick > 0 else str(px)
        
        logger.info(
            f"XT close: попытка {order_n}/{max_orders_total} | side={order_side} qty={qty_str} | "
            f"лимит={px_str} | remaining={_format_number(remaining)}"
        )
        
        body = {
            "symbol": symbol,
            "orderSide": order_side,
            "orderType": "LIMIT",
            "origQty": qty_str,
            "price": px_str,
            "timeInForce": "IOC",
            "reduceOnly": True,  # Close position
            "positionSide": position_side,
        }
        
        data = await _xt_private_request(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            method="POST",
            path="/future/trade/v1/order/create",
            body=body,
        )
        
        if not isinstance(data, dict) or data.get("returnCode") != 0:
            logger.error(f"❌ XT close: api error: {data}")
            return False, None
        
        order_id = None
        result = data.get("result")
        if isinstance(result, dict):
            order_id = result.get("orderId") or result.get("order_id") or result.get("id") or result.get("orderIdStr")
        elif isinstance(result, (str, int)) and str(result).strip():
            order_id = str(result).strip()
        
        if order_id:
            await asyncio.sleep(0.1)
            detail = await _xt_private_request(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                method="GET",
                path="/future/trade/v1/order/detail",
                params={"orderId": str(order_id)},
            )
            
            filled_contracts = 0.0
            if isinstance(detail, dict) and detail.get("returnCode") == 0:
                r_any = detail.get("result")
                r = _first_dict(r_any) or _first_dict(detail.get("data")) or _first_dict(detail)
                fc = None
                if isinstance(r, dict):
                    fc_raw = r.get("executedQty") or r.get("filledQty") or r.get("cumQty") or r.get("dealQty") or r.get("executedQuantity")
                    if fc_raw is not None:
                        try:
                            fc = float(fc_raw)
                        except Exception:
                            fc = None
                if fc is None:
                    fc = _find_num_by_keys(r_any, ("executedQty", "filledQty", "cumQty", "dealQty", "executedQuantity"))
                if fc is not None:
                    filled_contracts = float(fc)
            
            filled_qty = filled_contracts * contract_size
            if filled_qty > 0:
                total_notional += filled_qty * px
                total_filled += filled_qty
                remaining = max(0.0, remaining - filled_qty)
        
        await asyncio.sleep(0)
    
    avg_price = (total_notional / total_filled) if total_filled > 0 else None
    return remaining <= eps, avg_price


