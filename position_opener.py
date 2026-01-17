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

import hashlib
import hmac
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import httpx


# Логируем в __main__, чтобы совпадало с основным логгером bot.py
logger = logging.getLogger("__main__")


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
        return x
    return math.floor(x / step) * step


def _ceil_to_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return math.ceil(x / step) * step


def _is_multiple_of_step(x: float, step: float, eps: float = 1e-12) -> bool:
    if step <= 0:
        return True
    k = x / step
    return abs(k - round(k)) <= eps


def _round_price_for_side(price: float, tick: float, side: str) -> float:
    """
    Чтобы лимитка исполнилась агрессивно:
    - Buy: округляем ВВЕРХ к tick (не ниже ask)
    - Sell: округляем ВНИЗ к tick (не выше bid)
    """
    if tick <= 0:
        return price
    if side.lower() in ("buy", "long"):
        return _ceil_to_step(price, tick)
    return _floor_to_step(price, tick)


def _decimals_from_step_str(step_raw: Optional[str]) -> int:
    if not step_raw:
        return 8
    s = str(step_raw).strip()
    if "e" in s.lower():
        # fallback
        return 8
    if "." not in s:
        return 0
    frac = s.split(".", 1)[1]
    frac = frac.rstrip("0")
    return max(0, len(frac))


def _format_by_step(x: float, step_raw: Optional[str]) -> str:
    decimals = _decimals_from_step_str(step_raw)
    s = f"{x:.{decimals}f}" if decimals > 0 else str(int(x))
    return s.rstrip("0").rstrip(".") if "." in s else s


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
) -> bool:
    """
    Открывает Long (на long_exchange) и Short (на short_exchange) на coin_amount монет (для каждой ноги).
    Возвращает True только если обе ноги открылись успешно.
    """
    try:
        coin_amount = float(coin_amount)
    except Exception:
        logger.error(f"❌ Некорректное количество монет: {coin_amount!r}")
        return False
    if coin_amount <= 0:
        logger.error(f"❌ Количество монет должно быть > 0, получено: {coin_amount}")
        return False

    logger.info(f"🧩 Авто-открытие позиций: {coin} | Long {long_exchange} + Short {short_exchange} | qty={_format_number(coin_amount)} {coin}")

    long_obj = (getattr(bot, "exchanges", {}) or {}).get(long_exchange)
    short_obj = (getattr(bot, "exchanges", {}) or {}).get(short_exchange)
    if long_obj is None or short_obj is None:
        logger.error(f"❌ Не найдены биржи в bot.exchanges: long={long_exchange} short={short_exchange}")
        return False

    # 1) Preflight: проверяем возможность открытия на обеих биржах заранее
    long_plan = await _plan_one_leg(exchange_name=long_exchange, exchange_obj=long_obj, coin=coin, direction="long", coin_amount=coin_amount)
    if isinstance(long_plan, OpenLegResult) and not long_plan.ok:
        logger.error(f"❌ Preflight failed (Long): {long_plan.error}")
        return False
    short_plan = await _plan_one_leg(exchange_name=short_exchange, exchange_obj=short_obj, coin=coin, direction="short", coin_amount=coin_amount)
    if isinstance(short_plan, OpenLegResult) and not short_plan.ok:
        logger.error(f"❌ Preflight failed (Short): {short_plan.error}")
        return False
    if not isinstance(long_plan, dict) or not isinstance(short_plan, dict):
        logger.error("❌ Preflight не пройден, ордера не отправлены")
        return False

    # 2) Выставляем лимитные ордера параллельно
    long_task = _place_one_leg(planned=long_plan)
    short_task = _place_one_leg(planned=short_plan)
    long_res, short_res = await _gather2(long_task, short_task)

    _log_leg_result(long_res)
    _log_leg_result(short_res)

    # 3) Проверка fill: строго 100% (иначе считаем ошибкой)
    long_filled_ok = False
    short_filled_ok = False
    long_filled_qty = 0.0
    short_filled_qty = 0.0
    if long_res.ok and long_res.order_id:
        long_filled_ok, long_filled_qty = await _check_filled_full(planned=long_plan, order_id=long_res.order_id)
    if short_res.ok and short_res.order_id:
        short_filled_ok, short_filled_qty = await _check_filled_full(planned=short_plan, order_id=short_res.order_id)

    if long_res.ok and not long_filled_ok:
        logger.error(f"❌ Ордер не исполнен полностью: {long_exchange} long | исполнено={_format_number(long_filled_qty)} {coin} | требовалось={_format_number(coin_amount)} {coin}")
    if short_res.ok and not short_filled_ok:
        logger.error(f"❌ Ордер не исполнен полностью: {short_exchange} short | исполнено={_format_number(short_filled_qty)} {coin} | требовалось={_format_number(coin_amount)} {coin}")

    ok_all = bool(long_res.ok and short_res.ok and long_filled_ok and short_filled_ok)
    if ok_all:
        long_px = float(long_plan.get("limit_price") or 0)
        short_px = float(short_plan.get("limit_price") or 0)
        spread_open = None
        if long_px > 0:
            spread_open = (short_px - long_px) / long_px * 100.0
        spread_str = f"{spread_open:.3f}%" if spread_open is not None else "N/A"
        logger.info(
            f"Биржа лонг: {long_exchange}, Цена входа Long: {_format_number(long_px)}, "
            f"Биржа шорт: {short_exchange}, Цена входа Short: {_format_number(short_px)}, "
            f"Количество монет: {_format_number(coin_amount)}, Спред открытия: {spread_str}"
        )
        logger.info(f"✅ Позиции открыты: {coin} | Long {long_exchange} (order={long_res.order_id}) | Short {short_exchange} (order={short_res.order_id})")
    else:
        if (long_res.ok and long_filled_ok) and not (short_res.ok and short_filled_ok):
            logger.error(f"⚠️ Открыта только Long позиция: {coin} | Long ok=True | Short ok=False")
        elif (short_res.ok and short_filled_ok) and not (long_res.ok and long_filled_ok):
            logger.error(f"⚠️ Открыта только Short позиция: {coin} | Long ok=False | Short ok=True")
        else:
            logger.error(f"❌ Не удалось открыть позиции: {coin} | Long ok={long_res.ok and long_filled_ok} | Short ok={short_res.ok and short_filled_ok}")
    return ok_all


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


def _log_leg_result(res: OpenLegResult) -> None:
    if res.ok:
        logger.info(f"✅ Ордер выставлен: {res.exchange} {res.direction} | order_id={res.order_id}")
        return
    msg = res.error or "unknown error"
    logger.error(f"❌ Ошибка открытия: {res.exchange} {res.direction} | {msg}")


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
    return OpenLegResult(exchange=exchange_name, direction=direction, ok=False, error="trading not implemented for this exchange")


async def _place_one_leg(*, planned: Dict[str, Any]) -> OpenLegResult:
    ex = str(planned.get("exchange") or "").lower()
    if ex == "bybit":
        return await _bybit_place_leg(planned=planned)
    if ex == "gate":
        return await _gate_place_leg(planned=planned)
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
    Bybit: realtime endpoint может не возвращать уже filled/cancelled ордера,
    поэтому используем fallback на /v5/order/history.
    """
    api_key = planned["api_key"]
    api_secret = planned["api_secret"]
    exchange_obj = planned["exchange_obj"]
    symbol = planned["symbol"]
    qty_req = float(planned["qty"])
    eps = max(1e-10, qty_req * 1e-8)

    import asyncio
    last_seen: Optional[Dict[str, Any]] = None
    last_err: Optional[str] = None
    for _ in range(30):  # ~6s total
        # 1) realtime (open orders)
        data_rt = await _bybit_private_request(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            method="GET",
            path="/v5/order/realtime",
            params={"category": "linear", "symbol": symbol, "orderId": order_id},
        )
        if isinstance(data_rt, dict) and data_rt.get("_error"):
            last_err = str(data_rt)
            logger.debug(f"Bybit fill check realtime error: {data_rt}")
        elif isinstance(data_rt, dict) and data_rt.get("retCode") not in (None, 0):
            last_err = f"realtime retCode={data_rt.get('retCode')} retMsg={data_rt.get('retMsg')}"
        if isinstance(data_rt, dict) and data_rt.get("retCode") == 0:
            items = ((data_rt.get("result") or {}).get("list") or [])
            item = items[0] if items and isinstance(items[0], dict) else None
            if item:
                last_seen = item

        # 2) history (filled/cancelled)
        data_h = await _bybit_private_request(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            method="GET",
            path="/v5/order/history",
            params={"category": "linear", "symbol": symbol, "orderId": order_id},
        )
        if isinstance(data_h, dict) and data_h.get("_error"):
            last_err = str(data_h)
            logger.debug(f"Bybit fill check history error: {data_h}")
        elif isinstance(data_h, dict) and data_h.get("retCode") not in (None, 0):
            last_err = f"history retCode={data_h.get('retCode')} retMsg={data_h.get('retMsg')}"
        if isinstance(data_h, dict) and data_h.get("retCode") == 0:
            items = ((data_h.get("result") or {}).get("list") or [])
            item = items[0] if items and isinstance(items[0], dict) else None
            if item:
                last_seen = item

        if last_seen:
            status = str(last_seen.get("orderStatus") or "")
            cum_exec = last_seen.get("cumExecQty")
            try:
                filled = float(cum_exec) if cum_exec is not None else 0.0
            except Exception:
                filled = 0.0
            if status.lower() in ("filled", "cancelled", "canceled", "rejected", "partiallyfilled", "partially_filled"):
                logger.info(f"Bybit: статус ордера {order_id}: {status} | исполнено={_format_number(filled)} | требовалось={_format_number(qty_req)}")
                return (filled + eps >= qty_req), filled

        await asyncio.sleep(0.2)

    # если ничего не нашли — это скорее проблема доступа/подписи/ответа API
    tail = f" | last_error={last_err}" if last_err else ""
    logger.error(f"Bybit fill check: не удалось получить статус ордера {order_id} (symbol={symbol}){tail}")
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

    ob_levels = min(3, int(os.getenv("ENTRY_OB_LEVELS", "3")))
    ob = await exchange_obj.get_orderbook(coin, limit=ob_levels)
    if not ob or not ob.get("bids") or not ob.get("asks"):
        return OpenLegResult(exchange="bybit", direction=direction, ok=False, error=f"orderbook not available for {coin}")

    side = "Buy" if direction == "long" else "Sell"
    book_side = ob["asks"] if side == "Buy" else ob["bids"]
    best_price = float(book_side[0][0])
    if best_price <= 0:
        return OpenLegResult(exchange="bybit", direction=direction, ok=False, error="bad orderbook best price")

    # Кандидаты цен: максимум 3 уровня. Пытаемся 1й, потом 2й, потом 3й.
    candidates: list[float] = []
    cum = 0.0
    for lvl in book_side[:ob_levels]:
        try:
            p = float(lvl[0])
            s = float(lvl[1])
        except Exception:
            continue
        if p <= 0 or s <= 0:
            continue
        cum += s
        if cum + 1e-12 >= coin_amount:
            candidates.append(p)
    if not candidates:
        return OpenLegResult(exchange="bybit", direction=direction, ok=False, error=f"not enough depth in first {ob_levels} levels: need {coin_amount}, available {cum}")
    limit_price_raw = float(candidates[0])

    f = await _bybit_fetch_instrument_filters(exchange_obj=exchange_obj, symbol=symbol)
    qty_step_raw = f.get("qtyStep")
    min_qty_raw = f.get("minOrderQty")
    min_amt_raw = f.get("minOrderAmt")
    tick_raw = f.get("tickSize")

    qty_step = float(qty_step_raw) if qty_step_raw else 0.0
    min_qty = float(min_qty_raw) if min_qty_raw else 0.0
    min_amt = float(min_amt_raw) if min_amt_raw else 0.0
    tick = float(tick_raw) if tick_raw else 0.0

    # Проверка шагов/минималок количества — не подгоняем, а валидируем ввод
    if qty_step > 0 and not _is_multiple_of_step(coin_amount, qty_step):
        return OpenLegResult(exchange="bybit", direction=direction, ok=False, error=f"qty {coin_amount} not multiple of qtyStep {qty_step_raw}")
    if min_qty > 0 and coin_amount < min_qty:
        return OpenLegResult(exchange="bybit", direction=direction, ok=False, error=f"qty {coin_amount} < minOrderQty {min_qty_raw}")

    # Лимитная цена по лучшему уровню стакана + округление по tick
    limit_price = _round_price_for_side(limit_price_raw, tick, "buy" if side == "Buy" else "sell")
    notional = coin_amount * limit_price

    # Минимальная сумма ордера (полезно, чтобы не ловить 110094)
    if min_amt > 0 and notional < min_amt:
        return OpenLegResult(
            exchange="bybit",
            direction=direction,
            ok=False,
            error=f"Order does not meet minimum order value {min_amt:.3f}USDT (requested ~{notional:.3f}USDT)",
        )

    # Примечание: мы уже выбрали price level, на котором хватает cum_sz >= coin_amount,
    # поэтому отдельная проверка top-of-book не нужна.

    qty_str = _format_by_step(coin_amount, qty_step_raw)
    price_str = _format_by_step(limit_price, tick_raw)

    candidates_str = "[" + ", ".join([_format_number(c) for c in candidates]) + "]"
    logger.info(f"План Bybit: {direction} qty={qty_str} | best={_format_number(best_price)} | кандидаты уровней(<=3)={candidates_str}")

    return {
        "exchange": "bybit",
        "direction": direction,
        "exchange_obj": exchange_obj,
        "symbol": symbol,
        "side": side,
        "qty": qty_str,
        "limit_price": float(price_str),
        "price_str": price_str,
        "candidate_prices_raw": candidates,
        "api_key": api_key,
        "api_secret": api_secret,
    }


async def _bybit_place_leg(*, planned: Dict[str, Any]) -> OpenLegResult:
    exchange_obj = planned["exchange_obj"]
    api_key = planned["api_key"]
    api_secret = planned["api_secret"]
    direction = planned["direction"]
    side = planned["side"]
    tick_raw = (await _bybit_fetch_instrument_filters(exchange_obj=exchange_obj, symbol=planned["symbol"])).get("tickSize")
    tick = float(tick_raw) if tick_raw else 0.0

    candidates = planned.get("candidate_prices_raw") or [float(planned["price_str"])]
    # максимум 3 попытки: уровень 1 -> 2 -> 3
    attempts = candidates[:3]
    for idx, px_raw in enumerate(attempts, start=1):
        px = _round_price_for_side(float(px_raw), tick, "buy" if side == "Buy" else "sell")
        px_str = _format_by_step(px, tick_raw)
        logger.info(f"Bybit: попытка {idx}/{len(attempts)} | {direction} qty={planned['qty']} | лимит={px_str}")
        body = {
            "category": "linear",
            "symbol": planned["symbol"],
            "side": side,
            "orderType": "Limit",
            "qty": planned["qty"],
            "price": px_str,
            # строго 100% или отмена
            "timeInForce": "FOK",
        }
        data = await _bybit_private_post(exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret, path="/v5/order/create", body=body)
        if not isinstance(data, dict) or data.get("retCode") != 0:
            return OpenLegResult(exchange="bybit", direction=direction, ok=False, error=f"api error: {data}", raw=data)
        order_id = (data.get("result") or {}).get("orderId") if isinstance(data.get("result"), dict) else None
        if not order_id:
            return OpenLegResult(exchange="bybit", direction=direction, ok=False, error=f"no orderId in response: {data}", raw=data)

        ok_full, filled_qty = await _bybit_wait_full_fill(planned={**planned, "price_str": px_str, "limit_price": float(px_str)}, order_id=str(order_id))
        if ok_full:
            return OpenLegResult(exchange="bybit", direction=direction, ok=True, order_id=str(order_id), raw=data)
        # если не исполнилось, пробуем следующий уровень
        logger.warning(f"Bybit: не исполнилось полностью на уровне {idx} | исполнено={_format_number(filled_qty)} | требовалось={_format_number(float(planned['qty']))}")

    return OpenLegResult(exchange="bybit", direction=direction, ok=False, error="не удалось исполнить ордер полностью за 3 попытки (уровни 1-3)")


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
    ob_levels = min(3, int(os.getenv("ENTRY_OB_LEVELS", "3")))
    ob = await exchange_obj.get_orderbook(coin, limit=ob_levels)
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

    # coin_amount -> contracts должен быть целым (не подгоняем)
    contracts_exact = coin_amount / qmul
    contracts_i = int(round(contracts_exact))
    if abs(contracts_exact - contracts_i) > 1e-9:
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"qty {coin_amount} {coin} not compatible with contract size (quanto_multiplier={qmul}) => contracts={contracts_exact:.8f} (must be integer)")
    if contracts_i <= 0:
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"contracts computed as {contracts_i}")
    if min_size is not None and contracts_i < min_size:
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"contracts {contracts_i} < min {min_size}")

    side = "buy" if direction == "long" else "sell"
    book_side = ob["asks"] if side == "buy" else ob["bids"]
    # Кандидаты цен: максимум 3 уровня. Пытаемся 1й, потом 2й, потом 3й.
    candidates: list[float] = []
    cum = 0.0
    for lvl in book_side[:ob_levels]:
        try:
            p = float(lvl[0])
            s = float(lvl[1])
        except Exception:
            continue
        if p <= 0 or s <= 0:
            continue
        cum += s
        if cum + 1e-12 >= float(contracts_i):
            candidates.append(p)
    if not candidates:
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"not enough depth in first {ob_levels} levels: need {contracts_i} contracts, available {cum}")
    limit_price_raw = float(candidates[0])

    price_step = _gate_price_step_from_contract_info(cinfo) or 0.0
    limit_price = _round_price_for_side(limit_price_raw, price_step, side)

    candidates_str = "[" + ", ".join([_format_number(c) for c in candidates]) + "]"
    logger.info(f"План Gate: {direction} contracts={contracts_i} | best_bid={_format_number(best_bid)} best_ask={_format_number(best_ask)} | кандидаты уровней(<=3)={candidates_str}")

    size_signed = contracts_i if direction == "long" else -contracts_i
    price_str = _format_by_step(limit_price, str(price_step) if price_step > 0 else None)

    return {
        "exchange": "gate",
        "direction": direction,
        "exchange_obj": exchange_obj,
        "contract": contract,
        "size": size_signed,
        "limit_price": float(price_str),
        "price_str": price_str,
        "candidate_prices_raw": candidates,
        "api_key": api_key,
        "api_secret": api_secret,
    }


async def _gate_place_leg(*, planned: Dict[str, Any]) -> OpenLegResult:
    exchange_obj = planned["exchange_obj"]
    api_key = planned["api_key"]
    api_secret = planned["api_secret"]
    direction = planned["direction"]
    candidates = planned.get("candidate_prices_raw") or [float(planned["price_str"])]
    attempts = candidates[:3]
    for idx, px_raw in enumerate(attempts, start=1):
        px = float(px_raw)
        px_str = _format_by_step(px, None)
        logger.info(f"Gate: попытка {idx}/{len(attempts)} | {direction} size={planned['size']} | лимит={px_str}")
        body = {
            "contract": planned["contract"],
            "size": planned["size"],
            "price": px_str,
            # Требование: строго 100% исполнение или отмена
            "tif": "fok",
        }
        data = await _gate_private_post(exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret, path="/api/v4/futures/usdt/orders", body=body)
        # Gate может вернуть dict с label/message при ошибке — не считаем это успехом
        if not (isinstance(data, dict) and data.get("id") is not None and ("label" not in data) and ("message" not in data)):
            return OpenLegResult(exchange="gate", direction=direction, ok=False, error=f"api error: {data}", raw=data)
        order_id = str(data.get("id"))
        ok_full, filled_base = await _gate_wait_full_fill(planned={**planned, "price_str": px_str, "limit_price": float(px_str)}, order_id=order_id)
        if ok_full:
            return OpenLegResult(exchange="gate", direction=direction, ok=True, order_id=order_id, raw=data)
        if filled_base > 0:
            # partial fill — не продолжаем, чтобы не добирать остаток по худшей цене без явного разрешения
            logger.warning(f"Gate: частичное исполнение, дальнейшие попытки остановлены | исполнено~{_format_number(filled_base)} base")
            return OpenLegResult(exchange="gate", direction=direction, ok=False, order_id=order_id, error="частичное исполнение (не 100%)", raw=data)
        logger.warning("Gate: не исполнилось полностью на уровне %s (0 исполнено) — пробуем следующий", idx)

    return OpenLegResult(exchange="gate", direction=direction, ok=False, error="не удалось исполнить ордер полностью за 3 попытки (уровни 1-3)")


