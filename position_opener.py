"""
Автоматическое открытие позиций (Long/Short) на биржах по API ключам из .env.

Сейчас реализовано:
- Bybit USDT-M Perp (v5): /v5/order/create
- Gate.io USDT-M Perp (v4): /api/v4/futures/usdt/orders

Важно:
- Размер notional_usdt трактуется как номинал КАЖДОЙ ноги (лонг и шорт открываются на одну и ту же сумму).
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


@dataclass
class OpenLegResult:
    exchange: str
    direction: str  # "long" | "short"
    ok: bool
    order_id: Optional[str] = None
    error: Optional[str] = None
    raw: Optional[Any] = None


def _get_env_any(names: Tuple[str, ...]) -> Optional[str]:
    for n in names:
        v = os.getenv(n)
        if v and str(v).strip():
            return str(v).strip()
    return None


def _floor_to_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return math.floor(x / step) * step


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


async def open_long_short_positions(
    *,
    bot: Any,
    coin: str,
    long_exchange: str,
    short_exchange: str,
    notional_usdt: float,
) -> bool:
    """
    Открывает Long (на long_exchange) и Short (на short_exchange) на notional_usdt (для каждой ноги).
    Возвращает True только если обе ноги открылись успешно.
    """
    if notional_usdt is None:
        try:
            notional_usdt = float(notional_usdt)
        except Exception:
            logger.error(f"❌ Некорректный notional_usdt: {notional_usdt!r}")
            return False
    if notional_usdt <= 0:
        logger.error(f"❌ notional_usdt должен быть > 0, получено: {notional_usdt}")
        return False

    logger.info(f"🧩 Авто-открытие позиций: {coin} | Long {long_exchange} + Short {short_exchange} | size={notional_usdt} USDT (each leg)")

    long_obj = (getattr(bot, "exchanges", {}) or {}).get(long_exchange)
    short_obj = (getattr(bot, "exchanges", {}) or {}).get(short_exchange)
    if long_obj is None or short_obj is None:
        logger.error(f"❌ Не найдены биржи в bot.exchanges: long={long_exchange} short={short_exchange}")
        return False

    long_task = _open_one_leg(exchange_name=long_exchange, exchange_obj=long_obj, coin=coin, direction="long", notional_usdt=notional_usdt)
    short_task = _open_one_leg(exchange_name=short_exchange, exchange_obj=short_obj, coin=coin, direction="short", notional_usdt=notional_usdt)
    long_res, short_res = await _gather2(long_task, short_task)

    _log_leg_result(long_res)
    _log_leg_result(short_res)

    ok_all = bool(long_res.ok and short_res.ok)
    if ok_all:
        logger.info(f"✅ Позиции открыты: {coin} | Long {long_exchange} (order={long_res.order_id}) | Short {short_exchange} (order={short_res.order_id})")
    else:
        logger.error(f"❌ Не удалось открыть обе позиции: {coin} | Long ok={long_res.ok} | Short ok={short_res.ok}")
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
    api_key = _get_env_any(("BYBIT_API_KEY", "BYBIT_KEY", "BYBIT_APIKEY"))
    api_secret = _get_env_any(("BYBIT_API_SECRET", "BYBIT_SECRET", "BYBIT_APISECRET"))
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
    api_key = _get_env_any(("GATE_API_KEY", "GATEIO_API_KEY", "GATE_KEY"))
    api_secret = _get_env_any(("GATE_API_SECRET", "GATEIO_API_SECRET", "GATE_SECRET"))
    if not api_key or not api_secret:
        return OpenLegResult(exchange="gate", direction=direction, ok=False, error="missing GATE_API_KEY/GATE_API_SECRET in env")

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


