import asyncio
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from bot import PerpArbitrageBot
import position_opener as po


# ----------------------------
# ENV loader (без зависимостей)
# ----------------------------
def load_dotenv(path: str = ".env") -> None:
    """
    Простой загрузчик .env:
    - поддерживает строки KEY=VALUE
    - игнорирует пустые строки и # comments
    - не перетирает уже заданные переменные окружения
    """
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
        return


load_dotenv(".env")


# ----------------------------
# Logging
# ----------------------------
LOG_LEVEL = os.getenv("FUN_LOG_LEVEL", "INFO").upper()
LOG_FILE = os.getenv("FUN_LOG_FILE", "fun.log")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
    force=True,
)

logger = logging.getLogger("fun")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("bot").setLevel(logging.CRITICAL)
logging.getLogger("news_monitor").setLevel(logging.CRITICAL)
logging.getLogger("announcements_monitor").setLevel(logging.CRITICAL)
logging.getLogger("exchanges").setLevel(logging.CRITICAL)


# ----------------------------
# Settings
# ----------------------------
TEST_OB_LEVELS = int(os.getenv("FUN_TEST_OB_LEVELS", "15"))
MAIN_OB_LEVELS = int(os.getenv("FUN_MAIN_OB_LEVELS", "15"))
POLL_SEC = float(os.getenv("FUN_POLL_SEC", "0.2"))
SHORT_OPEN_LEVELS = int(os.getenv("FUN_SHORT_OPEN_LEVELS", "10"))
SHORT_OPEN_REFRESH_ROUNDS = int(os.getenv("FUN_SHORT_OPEN_REFRESH_ROUNDS", "2"))  # 1 = only pre-snapshot, 2 = +1 refresh after payout
LONG_OPEN_MAX_SEC = float(os.getenv("FUN_LONG_OPEN_MAX_SEC", "3.0"))
OPEN_LEAD_MS = int(os.getenv("FUN_OPEN_LEAD_MS", "150"))
FIX_KLINE_INTERVAL = str(os.getenv("FUN_FIX_KLINE_INTERVAL", "1") or "1").strip()
NEWS_DAYS_BACK = int(os.getenv("FUN_NEWS_DAYS_BACK", "60"))
BALANCE_BUFFER_USDT = float(os.getenv("FUN_BALANCE_BUFFER_USDT", "0"))
FAST_PREP_LEAD_SEC = float(os.getenv("FUN_FAST_PREP_LEAD_SEC", "2.0"))  # do prep before payout-2s
FAST_CLOSE_DELAY_SEC = float(os.getenv("FUN_FAST_CLOSE_DELAY_SEC", "1.0"))  # start closing at payout+1s
FAST_CLOSE_MAX_ATTEMPTS = int(os.getenv("FUN_FAST_CLOSE_MAX_ATTEMPTS", "15"))
FAST_SILENT_TRADING = int(os.getenv("FUN_FAST_SILENT_TRADING", "1"))  # 1 => disable logging during open/close window


YES_WORDS = {"да", "y", "yes", "д"}


def _fmt(x: Optional[float], p: int = 6) -> str:
    if x is None:
        return "N/A"
    try:
        s = f"{float(x):.{p}f}"
        return s.rstrip("0").rstrip(".")
    except Exception:
        return "N/A"


def _is_yes(s: str) -> bool:
    t = (s or "").strip().lower()
    if not t:
        return False
    # "Да", "да", "Да, ..." etc.
    if t.split(",", 1)[0].strip() in YES_WORDS:
        return True
    return t in YES_WORDS


def _parse_pct(s: str) -> float:
    """
    "-2%" -> -0.02
    "-0.3%" -> -0.003
    """
    raw = (s or "").strip()
    m = re.fullmatch(r"(-?\d+(?:\.\d+)?)\s*%$", raw)
    if not m:
        raise ValueError(f"bad percent: {s!r} (expected like -2% or -0.3%)")
    return float(m.group(1)) / 100.0


def _parse_time_hhmm(s: str) -> Tuple[int, int]:
    raw = (s or "").strip()
    m = re.fullmatch(r"(\d{1,2}):(\d{2})$", raw)
    if not m:
        raise ValueError(f"bad time: {s!r} (expected HH:MM)")
    hh = int(m.group(1))
    mm = int(m.group(2))
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        raise ValueError(f"bad time: {s!r}")
    return hh, mm


@dataclass
class FunParams:
    coin: str
    exchange: str
    coin_qty: float
    offset_pct: float  # decimal, e.g. -0.003
    # Эти поля будут заполнены из API запросов:
    funding_pct: Optional[float] = None  # decimal, e.g. -0.02 (получается из API)
    payout_hh: Optional[int] = None  # получается из API
    payout_mm: Optional[int] = None  # получается из API


def parse_cmd(cmd: str) -> FunParams:
    parts = (cmd or "").strip().split()
    if len(parts) != 4:
        raise ValueError(
            "bad command format. Expected: COIN EXCHANGE QTY OFFSET% "
            'e.g. "STO Bybit 30 -0.3%"'
        )
    coin = parts[0].strip().upper()
    exchange = parts[1].strip().lower()
    coin_qty = float(parts[2])
    if coin_qty <= 0:
        raise ValueError(f"coin qty must be > 0, got: {coin_qty}")
    offset_pct = _parse_pct(parts[3])
    # по ТЗ проценты отрицательные (но валидируем мягко)
    return FunParams(
        coin=coin,
        exchange=exchange,
        coin_qty=float(coin_qty),
        offset_pct=float(offset_pct),
    )


def _local_dt_today(hh: int, mm: int) -> datetime:
    now = datetime.now()
    return now.replace(hour=hh, minute=mm, second=0, microsecond=0)


def _next_funding_time_to_local_hhmm(next_funding_time: Optional[int], exchange: str) -> Optional[Tuple[int, int]]:
    """
    Преобразует timestamp следующей выплаты фандинга в локальное время (HH, MM).
    
    Args:
        next_funding_time: Timestamp следующей выплаты (в миллисекундах для Bybit/Binance, в секундах для Gate)
        exchange: Название биржи
        
    Returns:
        Кортеж (HH, MM) или None если невозможно вычислить
    """
    if next_funding_time is None:
        return None
    
    try:
        # Bybit и Binance возвращают timestamp в миллисекундах
        # Gate возвращает в секундах
        if exchange.lower() == "gate":
            # Gate: timestamp в секундах
            funding_timestamp = next_funding_time
        else:
            # Bybit, Binance: timestamp в миллисекундах
            funding_timestamp = next_funding_time / 1000
        
        # Преобразуем в локальное время
        funding_dt = datetime.fromtimestamp(funding_timestamp, tz=timezone.utc)
        local_dt = funding_dt.astimezone()  # конвертируем в локальный timezone
        
        return local_dt.hour, local_dt.minute
    except Exception:
        return None


async def _sleep_until(dt_local: datetime) -> None:
    while True:
        now = datetime.now()
        sec = (dt_local - now).total_seconds()
        if sec <= 0:
            return
        # Более точное ожидание (особенно на Windows), чтобы не просыпаться на +200ms.
        if sec > 1.0:
            await asyncio.sleep(0.2)
        elif sec > 0.2:
            await asyncio.sleep(0.05)
        elif sec > 0.05:
            await asyncio.sleep(0.01)
        elif sec > 0.01:
            await asyncio.sleep(0.002)
        else:
            await asyncio.sleep(min(0.001, sec))


def _ticker_last_price(t: Optional[Dict[str, Any]]) -> Optional[float]:
    if not t:
        return None
    if t.get("price") is not None:
        try:
            return float(t.get("price"))
        except Exception:
            return None
    bid = t.get("bid")
    ask = t.get("ask")
    try:
        if bid is not None and ask is not None:
            return (float(bid) + float(ask)) / 2.0
    except Exception:
        return None
    return None


# =========================
# Bybit helpers (private API via position_opener signing)
# =========================
async def _bybit_get_filters(exchange_obj: Any, coin: str) -> Dict[str, Optional[str]]:
    symbol = exchange_obj._normalize_symbol(coin)
    return await po._bybit_fetch_instrument_filters(exchange_obj=exchange_obj, symbol=symbol)


def _ceil_to_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return float(int((x + step - 1e-18) // step) * step) if step >= 1 else po._ceil_to_step(x, step)


def _floor_to_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return po._floor_to_step(x, step)


async def _bybit_place_limit(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    coin: str,
    side: str,  # "Buy" | "Sell"
    qty_str: str,
    price_str: str,
    tif: str,  # "FOK" | "IOC" | "GTC"
    reduce_only: Optional[bool] = None,
    position_idx: Optional[int] = None,
) -> str:
    symbol = exchange_obj._normalize_symbol(coin)
    body: Dict[str, Any] = {
        "category": "linear",
        "symbol": symbol,
        "side": side,
        "orderType": "Limit",
        "qty": qty_str,
        "price": price_str,
        "timeInForce": tif,
    }
    if reduce_only is not None:
        body["reduceOnly"] = bool(reduce_only)
    if position_idx is not None:
        body["positionIdx"] = int(position_idx)
    data = await po._bybit_private_post(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        path="/v5/order/create",
        body=body,
    )
    if not isinstance(data, dict) or data.get("retCode") != 0:
        raise RuntimeError(f"Bybit create order failed: {data}")
    order_id = (data.get("result") or {}).get("orderId") if isinstance(data.get("result"), dict) else None
    if not order_id:
        raise RuntimeError(f"Bybit create order: no orderId: {data}")
    return str(order_id)


async def _bybit_place_stop_market(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    coin: str,
    side: str,  # "Buy" closes short
    qty_str: str,
    trigger_price_str: str,
    position_idx: Optional[int] = None,
) -> str:
    """
    Best-effort stop-market. Bybit v5:
    - orderType="Market"
    - triggerPrice + triggerDirection
    - reduceOnly=True
    """
    symbol = exchange_obj._normalize_symbol(coin)
    body: Dict[str, Any] = {
        "category": "linear",
        "symbol": symbol,
        "side": side,
        "orderType": "Market",
        "qty": qty_str,
        "timeInForce": "GTC",
        "triggerPrice": trigger_price_str,
        "triggerDirection": 1,  # price rises
        "triggerBy": "LastPrice",
        "reduceOnly": True,
    }
    if position_idx is not None:
        body["positionIdx"] = int(position_idx)
    data = await po._bybit_private_post(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        path="/v5/order/create",
        body=body,
    )
    if not isinstance(data, dict) or data.get("retCode") != 0:
        raise RuntimeError(f"Bybit create stop failed: {data}")
    order_id = (data.get("result") or {}).get("orderId") if isinstance(data.get("result"), dict) else None
    if not order_id:
        raise RuntimeError(f"Bybit create stop: no orderId: {data}")
    return str(order_id)


async def _bybit_cancel_order(*, exchange_obj: Any, api_key: str, api_secret: str, coin: str, order_id: str) -> bool:
    symbol = exchange_obj._normalize_symbol(coin)
    body = {"category": "linear", "symbol": symbol, "orderId": str(order_id)}
    data = await po._bybit_private_post(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        path="/v5/order/cancel",
        body=body,
    )
    if isinstance(data, dict) and data.get("retCode") == 0:
        return True
    return False


async def _bybit_get_usdt_available(*, exchange_obj: Any, api_key: str, api_secret: str) -> Optional[float]:
    """
    Best-effort fetch of available USDT balance for derivatives trading.
    We try multiple accountType values to support different Bybit account modes.
    """

    def _extract(data: Any) -> Optional[float]:
        if not isinstance(data, dict):
            return None
        if data.get("retCode") != 0:
            return None
        res = data.get("result") or {}
        lst = res.get("list") or []
        if not isinstance(lst, list):
            return None
        for acc in lst:
            if not isinstance(acc, dict):
                continue
            coins = acc.get("coin") or acc.get("coins") or []
            if isinstance(coins, dict):
                coins = [coins]
            if not isinstance(coins, list):
                continue
            for c in coins:
                if not isinstance(c, dict):
                    continue
                if str(c.get("coin") or "").upper() != "USDT":
                    continue
                for key in ("availableToWithdraw", "availableBalance", "walletBalance", "equity"):
                    raw = c.get(key)
                    if raw is None:
                        continue
                    try:
                        v = float(raw)
                    except Exception:
                        continue
                    if v >= 0:
                        return v
        return None

    for account_type in ("UNIFIED", "CONTRACT"):
        data = await po._bybit_private_request(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            method="GET",
            path="/v5/account/wallet-balance",
            params={"accountType": account_type, "coin": "USDT"},
        )
        v = _extract(data)
        if v is not None:
            return v
    return None


async def _bybit_get_order_status(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    coin: str,
    order_id: str,
) -> Tuple[Optional[str], float, Optional[float]]:
    """
    Returns (status, cumExecQty, avg_exec_price).
    status example: Filled / Cancelled / New / PartiallyFilled / Rejected ...
    """
    symbol = exchange_obj._normalize_symbol(coin)

    async def _query(path: str) -> Optional[Dict[str, Any]]:
        data = await po._bybit_private_request(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            method="GET",
            path=path,
            params={"category": "linear", "symbol": symbol, "orderId": str(order_id)},
        )
        if not (isinstance(data, dict) and data.get("retCode") == 0):
            return None
        items = ((data.get("result") or {}).get("list") or [])
        item = items[0] if items and isinstance(items[0], dict) else None
        return item

    item = await _query("/v5/order/realtime")
    if item is None:
        item = await _query("/v5/order/history")
    if not item:
        return None, 0.0, None
    status = str(item.get("orderStatus") or "")
    try:
        filled = float(item.get("cumExecQty") or 0.0)
    except Exception:
        filled = 0.0
    # Best-effort avg execution price from the same response (no extra requests)
    avg_px: Optional[float] = None
    for k in ("avgPrice", "avgPx", "avgFillPrice"):
        try:
            v = item.get(k)
            if v is None:
                continue
            px = float(v)
            if px > 0:
                avg_px = px
                break
        except Exception:
            pass
    if avg_px is None:
        try:
            qty = float(item.get("cumExecQty") or 0.0)
        except Exception:
            qty = 0.0
        try:
            val = float(item.get("cumExecValue") or 0.0)
        except Exception:
            val = 0.0
        if qty > 0 and val > 0:
            avg_px = val / qty

    return status, filled, avg_px


async def _bybit_wait_done_get_filled_qty(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    coin: str,
    order_id: str,
    timeout_sec: float = 6.0,
    poll_sleep_sec: float = 0.12,
) -> Tuple[Optional[str], float, Optional[float]]:
    """
    Wait until order becomes terminal (Filled/Cancelled/Rejected/Expired) and return (final_status, filled_qty).
    For IOC/FOK orders it should resolve quickly.
    """
    deadline = time.time() + max(0.2, float(timeout_sec))
    last_status: Optional[str] = None
    last_filled: float = 0.0
    last_avg_px: Optional[float] = None
    while time.time() < deadline:
        st, filled, avg_px = await _bybit_get_order_status(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            coin=coin,
            order_id=order_id,
        )
        if st:
            last_status = st
            last_filled = filled
            last_avg_px = avg_px
            if st.lower() in ("filled", "cancelled", "canceled", "rejected", "expired"):
                return st, filled, avg_px
        await asyncio.sleep(max(0.0, float(poll_sleep_sec)))
    return last_status, last_filled, last_avg_px


async def _bybit_fetch_executions(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    coin: str,
    start_ms: int,
    end_ms: int,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """
    Post-trade helper: fetch executions from Bybit, to compute realized PnL and avg prices AFTER all orders are done.
    IMPORTANT: call this only after open/close are finished.
    """
    symbol = exchange_obj._normalize_symbol(coin)
    params: Dict[str, Any] = {
        "category": "linear",
        "symbol": symbol,
        "startTime": int(max(0, start_ms)),
        "endTime": int(max(0, end_ms)),
        "limit": int(max(1, min(1000, limit))),
    }
    data = await po._bybit_private_request(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        method="GET",
        path="/v5/execution/list",
        params=params,
    )
    if not (isinstance(data, dict) and data.get("retCode") == 0):
        return []
    items = ((data.get("result") or {}).get("list") or [])
    return items if isinstance(items, list) else []


async def _bybit_get_short_position_qty(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    coin: str,
) -> float:
    """
    Returns current short position size (absolute qty) for coin on Bybit (best-effort).
    Works for one-way and hedge accounts.
    """
    symbol = exchange_obj._normalize_symbol(coin)
    data = await po._bybit_private_request(
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
    if not isinstance(items, list):
        return 0.0
    short_qty = 0.0
    for it in items:
        if not isinstance(it, dict):
            continue
        side = str(it.get("side") or "").strip()
        try:
            sz = float(it.get("size") or 0.0)
        except Exception:
            sz = 0.0
        if sz <= 0:
            continue
        if side.lower() == "sell":
            short_qty += abs(sz)
    return float(short_qty)


async def _bybit_fetch_recent_trades(*, exchange_obj: Any, coin: str, limit: int = 200) -> List[Dict[str, Any]]:
    """
    Best-effort: fetch recent public trades to get last trade price close to a specific timestamp.
    Bybit v5: /v5/market/recent-trade
    """
    try:
        symbol = exchange_obj._normalize_symbol(coin)
        params: Dict[str, Any] = {"category": "linear", "symbol": symbol, "limit": int(max(1, min(1000, limit)))}
        data = await exchange_obj._request_json("GET", "/v5/market/recent-trade", params=params)
        if not (isinstance(data, dict) and data.get("retCode") == 0):
            return []
        items = ((data.get("result") or {}).get("list") or [])
        return items if isinstance(items, list) else []
    except Exception:
        return []


def _bybit_parse_trade_time_ms(it: Dict[str, Any]) -> Optional[int]:
    for k in ("tradeTimeMs", "time", "execTime", "timestamp", "ts"):
        v = it.get(k)
        if v is None:
            continue
        try:
            # most Bybit fields are ms as string
            t = int(float(v))
            # if looks like seconds, convert to ms
            if t < 10_000_000_000:
                t *= 1000
            return t
        except Exception:
            continue
    return None


def _bybit_parse_trade_price(it: Dict[str, Any]) -> Optional[float]:
    for k in ("price", "execPrice", "p"):
        v = it.get(k)
        if v is None:
            continue
        try:
            px = float(v)
            if px > 0:
                return px
        except Exception:
            continue
    return None


async def _bybit_last_trade_price_before(*, exchange_obj: Any, coin: str, before_ms: int) -> Optional[float]:
    """
    Returns last trade price with trade time <= before_ms (best-effort).
    """
    trades = await _bybit_fetch_recent_trades(exchange_obj=exchange_obj, coin=coin, limit=200)
    best_t: Optional[int] = None
    best_px: Optional[float] = None
    for it in trades:
        if not isinstance(it, dict):
            continue
        t = _bybit_parse_trade_time_ms(it)
        if t is None or t > before_ms:
            continue
        px = _bybit_parse_trade_price(it)
        if px is None:
            continue
        if best_t is None or t > best_t:
            best_t = t
            best_px = px
    return best_px


async def _bybit_fetch_kline(
    *,
    exchange_obj: Any,
    coin: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int = 200,
) -> List[List[Any]]:
    """
    Bybit v5: /v5/market/kline
    Returns list of arrays: [startTime, open, high, low, close, volume, turnover]
    """
    try:
        symbol = exchange_obj._normalize_symbol(coin)
        params: Dict[str, Any] = {
            "category": "linear",
            "symbol": symbol,
            "interval": str(interval),
            "start": int(max(0, start_ms)),
            "end": int(max(0, end_ms)),
            "limit": int(max(1, min(1000, limit))),
        }
        data = await exchange_obj._request_json("GET", "/v5/market/kline", params=params)
        if not (isinstance(data, dict) and data.get("retCode") == 0):
            return []
        items = ((data.get("result") or {}).get("list") or [])
        return items if isinstance(items, list) else []
    except Exception:
        return []


async def _bybit_kline_close_for_minute_ending_at(
    *,
    exchange_obj: Any,
    coin: str,
    end_ms_inclusive: int,
) -> Optional[float]:
    """
    Best-effort: returns candle close for configured interval (FUN_FIX_KLINE_INTERVAL, minutes)
    for the bucket that contains end_ms_inclusive (e.g. 09:59:59.xxx).
    """
    # Determine bucket start for the configured interval (assume minutes as int; fallback to 1m)
    try:
        interval_min = int(str(FIX_KLINE_INTERVAL).strip())
    except Exception:
        interval_min = 1
    interval_min = max(1, int(interval_min))
    bucket_ms = int(interval_min) * 60_000
    bucket_start_ms = int(end_ms_inclusive - (end_ms_inclusive % bucket_ms))
    items = await _bybit_fetch_kline(
        exchange_obj=exchange_obj,
        coin=coin,
        interval=FIX_KLINE_INTERVAL,
        start_ms=bucket_start_ms,
        end_ms=bucket_start_ms + bucket_ms,
        limit=10,
    )
    best_close: Optional[float] = None
    best_start: Optional[int] = None
    for it in items:
        if not isinstance(it, list) or len(it) < 5:
            continue
        try:
            st = int(float(it[0]))
        except Exception:
            continue
        if st != bucket_start_ms:
            continue
        try:
            close_px = float(it[4])
        except Exception:
            continue
        if close_px > 0:
            best_start = st
            best_close = close_px
            break
    return best_close

def _bybit_calc_pnl_usdt_from_execs(execs: List[Dict[str, Any]]) -> Tuple[Optional[float], int, int, Optional[float], Optional[float]]:
    """
    Returns:
      pnl_usdt_total (sell_notional - buy_notional),
      buys_count, sells_count,
      avg_buy_px, avg_sell_px
    """
    buy_notional = 0.0
    sell_notional = 0.0
    buy_qty = 0.0
    sell_qty = 0.0
    buys = 0
    sells = 0

    for it in execs:
        if not isinstance(it, dict):
            continue
        side = str(it.get("side") or "")
        try:
            px = float(it.get("execPrice") or 0.0)
            q = float(it.get("execQty") or 0.0)
        except Exception:
            continue
        if px <= 0 or q <= 0:
            continue
        notional = px * q
        if side.lower() == "buy":
            buys += 1
            buy_notional += notional
            buy_qty += q
        elif side.lower() == "sell":
            sells += 1
            sell_notional += notional
            sell_qty += q

    if buys == 0 and sells == 0:
        return None, 0, 0, None, None

    avg_buy = (buy_notional / buy_qty) if buy_qty > 0 else None
    avg_sell = (sell_notional / sell_qty) if sell_qty > 0 else None
    pnl = sell_notional - buy_notional
    # Вычитаем комиссии: 0.005 USDT за каждую сделку (execution)
    commission_total = (buys + sells) * 0.005
    pnl = pnl - commission_total
    return pnl, buys, sells, avg_buy, avg_sell


async def _bybit_preflight_and_min_qty(
    *,
    exchange_obj: Any,
    coin: str,
    qty_desired: float,
    price_hint: float,
) -> Tuple[float, Dict[str, Optional[str]]]:
    f = await _bybit_get_filters(exchange_obj, coin)
    qty_step_raw = f.get("qtyStep")
    min_qty_raw = f.get("minOrderQty")
    min_amt_raw = f.get("minOrderAmt")
    tick_raw = f.get("tickSize")

    qty_step = float(qty_step_raw) if qty_step_raw else 0.0
    min_qty = float(min_qty_raw) if min_qty_raw else 0.0
    min_amt = float(min_amt_raw) if min_amt_raw else 0.0

    # min qty from exchange filters (+ min notional)
    min_qty_from_amt = (min_amt / price_hint) if (min_amt > 0 and price_hint > 0) else 0.0
    q_min = max(min_qty, min_qty_from_amt)
    if qty_step > 0:
        q_min = po._ceil_to_step(q_min, qty_step)

    # validate desired qty
    if qty_step > 0 and (not po._is_multiple_of_step(qty_desired, qty_step)):
        raise ValueError(f"qty {qty_desired} not multiple of qtyStep {qty_step_raw}")
    if min_qty > 0 and qty_desired < min_qty:
        raise ValueError(f"qty {qty_desired} < minOrderQty {min_qty_raw}")
    if min_amt > 0 and (qty_desired * price_hint) < min_amt:
        raise ValueError(f"order notional too small: {qty_desired}*{price_hint} < minOrderAmt {min_amt_raw}")

    _ = tick_raw  # keep for caller
    return float(q_min), f


async def _check_news_one_exchange(bot: PerpArbitrageBot, exchange: str, coin: str) -> Tuple[bool, str, List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns (ok, msg, delisting_news, security_news)
    """
    try:
        now_utc = datetime.now(timezone.utc)
        lookback = now_utc - timedelta(days=NEWS_DAYS_BACK, hours=6)
        anns = await bot.news_monitor._fetch_exchange_announcements(limit=200, days_back=NEWS_DAYS_BACK, exchanges=[exchange])
        delisting = await bot.news_monitor.find_delisting_news(anns, coin_symbol=coin, lookback=lookback)
        if delisting:
            return False, f"найдены новости о делистинге ({len(delisting)} шт.)", delisting, []
        security = await bot.announcements_monitor.find_security_news(anns, coin_symbol=coin, lookback=lookback)
        if security:
            return False, f"найдены security-новости ({len(security)} шт.)", delisting or [], security
        return True, "ok", [], []
    except Exception as e:
        # conservative: treat failures as NOT ok
        return False, f"ошибка проверки новостей: {type(e).__name__}: {e}", [], []


async def _bybit_test_orders(bot: PerpArbitrageBot, coin: str, qty_test: float) -> bool:
    exchange_obj = bot.exchanges["bybit"]
    api_key = po._get_env("BYBIT_API_KEY") or ""
    api_secret = po._get_env("BYBIT_API_SECRET") or ""
    if not api_key or not api_secret:
        logger.error("❌ Нет BYBIT_API_KEY/BYBIT_API_SECRET в .env")
        return False
    t0_ms = int(time.time() * 1000) - 10_000

    ob = await exchange_obj.get_orderbook(coin, limit=TEST_OB_LEVELS)
    if not ob or not ob.get("bids") or not ob.get("asks"):
        logger.error("❌ Bybit: не удалось получить orderbook для теста")
        return False

    f = await _bybit_get_filters(exchange_obj, coin)
    tick_raw = f.get("tickSize")
    tick = float(tick_raw) if tick_raw else 0.0
    qty_step_raw = f.get("qtyStep")
    qty_str = po._format_by_step(qty_test, qty_step_raw)

    # 1) Open test Short (Sell) FOK at price where cum bids >= qty (within first 15 levels)
    bids = ob["bids"][:TEST_OB_LEVELS]
    px_level, _cum = po._price_level_for_target_size(bids, qty_test)
    if px_level is None:
        logger.error(f"❌ Bybit test: недостаточно ликвидности в bids (1-{TEST_OB_LEVELS}) для qty={_fmt(qty_test)}")
        return False
    px = po._round_price_for_side(float(px_level), tick, "sell")
    px_str = po._format_by_step(px, tick_raw)

    logger.info(f"🧪 Тест: открываем Short (Sell FOK) | qty={qty_str} | лимит={px_str}")
    try:
        # hedge-mode: positionIdx=2 for short; one-way: omit it
        try:
            short_order_id = await _bybit_place_limit(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                coin=coin,
                side="Sell",
                qty_str=qty_str,
                price_str=px_str,
                tif="FOK",
                reduce_only=None,
                position_idx=2,
            )
        except Exception:
            short_order_id = await _bybit_place_limit(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                coin=coin,
                side="Sell",
                qty_str=qty_str,
                price_str=px_str,
                tif="FOK",
                reduce_only=None,
                position_idx=None,
            )
    except Exception as e:
        logger.error(f"❌ Bybit test: не удалось создать Short ордер: {e}")
        return False

    st_s, filled, short_entry_avg = await _bybit_wait_done_get_filled_qty(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        coin=coin,
        order_id=str(short_order_id),
        timeout_sec=8.0,
    )
    if not filled or filled + 1e-12 < float(qty_str):
        logger.error(f"❌ Bybit test: Short не исполнился полностью | status={st_s} | filled={_fmt(filled)}")
        return False
    logger.info(f"✅ Тест: Short открыт | filled={_fmt(filled)} {coin}")

    # 2) Open test Long (Buy) after short is opened, at best ask (FOK) for the same qty
    asks = ob["asks"][:TEST_OB_LEVELS]
    px_level_a, _cum_a = po._price_level_for_target_size(asks, qty_test)
    if px_level_a is None:
        logger.error(f"❌ Bybit test: недостаточно ликвидности в asks (1-{TEST_OB_LEVELS}) для qty={_fmt(qty_test)}")
        # close short best-effort
        _ok_s, avg_exit_short = await po._bybit_close_leg_partial_ioc(
            exchange_obj=exchange_obj,
            coin=coin,
            position_direction="short",
            coin_amount=qty_test,
        )
        pnl_short = None
        if short_entry_avg is not None and avg_exit_short is not None:
            pnl_short = (short_entry_avg - avg_exit_short) * qty_test
        t1_ms = int(time.time() * 1000) + 10_000
        execs = await _bybit_fetch_executions(exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret, coin=coin, start_ms=t0_ms, end_ms=t1_ms)
        pnl_total, buys_n, sells_n, avg_buy, avg_sell = _bybit_calc_pnl_usdt_from_execs(execs)
        logger.info(
            f"📊 Итог (ТЕСТ): монета={coin} | "
            f"ср_цена_покупки={_fmt(avg_buy)} | ср_цена_продажи={_fmt(avg_sell)} | "
            f"покупок={buys_n} продаж={sells_n} | PnL_USDT_итого={_fmt(pnl_total, 3) if pnl_total is not None else 'N/A'}"
        )
        return False
    px_a = po._round_price_for_side(float(px_level_a), tick, "buy")
    px_a_str = po._format_by_step(px_a, tick_raw)

    logger.info(f"🧪 Тест: открываем Long (Buy FOK) | qty={qty_str} | лимит={px_a_str}")
    try:
        # hedge-mode: positionIdx=1 for long; one-way: omit it
        try:
            long_order_id = await _bybit_place_limit(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                coin=coin,
                side="Buy",
                qty_str=qty_str,
                price_str=px_a_str,
                tif="FOK",
                reduce_only=None,
                position_idx=1,
            )
        except Exception:
            long_order_id = await _bybit_place_limit(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                coin=coin,
                side="Buy",
                qty_str=qty_str,
                price_str=px_a_str,
                tif="FOK",
                reduce_only=None,
                position_idx=None,
            )
    except Exception as e:
        logger.error(f"❌ Bybit test: не удалось создать Long ордер: {e}")
        _ok_s, avg_exit_short = await po._bybit_close_leg_partial_ioc(
            exchange_obj=exchange_obj,
            coin=coin,
            position_direction="short",
            coin_amount=qty_test,
        )
        pnl_short = None
        if short_entry_avg is not None and avg_exit_short is not None:
            pnl_short = (short_entry_avg - avg_exit_short) * qty_test
        t1_ms = int(time.time() * 1000) + 10_000
        execs = await _bybit_fetch_executions(exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret, coin=coin, start_ms=t0_ms, end_ms=t1_ms)
        pnl_total, buys_n, sells_n, avg_buy, avg_sell = _bybit_calc_pnl_usdt_from_execs(execs)
        logger.info(
            f"📊 Итог (ТЕСТ): монета={coin} | "
            f"ср_цена_покупки={_fmt(avg_buy)} | ср_цена_продажи={_fmt(avg_sell)} | "
            f"покупок={buys_n} продаж={sells_n} | PnL_USDT_итого={_fmt(pnl_total, 3) if pnl_total is not None else 'N/A'}"
        )
        return False

    st_l, filled_l, long_entry_avg = await _bybit_wait_done_get_filled_qty(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        coin=coin,
        order_id=str(long_order_id),
        timeout_sec=8.0,
    )
    if not filled_l or filled_l + 1e-12 < float(qty_str):
        logger.warning(
            f"⚠️ Bybit test: Long не исполнился (FOK) | status={st_l} | filled={_fmt(filled_l)}. "
            f"Закрываем открытые позиции best-effort и продолжаем."
        )
        # close both best-effort
        _ok_s, avg_exit_short = await po._bybit_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=coin, position_direction="short", coin_amount=qty_test)
        _ok_l, avg_exit_long = await po._bybit_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=coin, position_direction="long", coin_amount=qty_test)

        pnl_short = None
        if short_entry_avg is not None and avg_exit_short is not None:
            pnl_short = (short_entry_avg - avg_exit_short) * qty_test
        t1_ms = int(time.time() * 1000) + 10_000
        execs = await _bybit_fetch_executions(exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret, coin=coin, start_ms=t0_ms, end_ms=t1_ms)
        pnl_total, buys_n, sells_n, avg_buy, avg_sell = _bybit_calc_pnl_usdt_from_execs(execs)
        logger.info(
            f"📊 Итог (ТЕСТ): монета={coin} | "
            f"ср_цена_покупки={_fmt(avg_buy)} | ср_цена_продажи={_fmt(avg_sell)} | "
            f"покупок={buys_n} продаж={sells_n} | PnL_USDT_итого={_fmt(pnl_total, 3) if pnl_total is not None else 'N/A'}"
        )
        return False
    logger.info(f"✅ Тест: Long открыт | filled={_fmt(filled_l)} {coin}")

    # 3) Close both test positions at best prices from orderbook (IOC reduceOnly, partial allowed)
    logger.info("🧪 Тест: закрываем обе позиции (Short+Long) по стакану")
    close_short_t = po._bybit_close_leg_partial_ioc(
        exchange_obj=exchange_obj,
        coin=coin,
        position_direction="short",
        coin_amount=qty_test,
        position_idx=2,
    )
    close_long_t = po._bybit_close_leg_partial_ioc(
        exchange_obj=exchange_obj,
        coin=coin,
        position_direction="long",
        coin_amount=qty_test,
        position_idx=1,
    )
    ok_s, avg_s = await close_short_t
    ok_l, avg_l = await close_long_t
    if not (ok_s and ok_l):
        logger.error(f"❌ Bybit test: не удалось закрыть обе позиции | short_ok={ok_s} long_ok={ok_l}")
        return False
    logger.info(f"✅ Тест: позиции закрыты | avg_close_short_buy={_fmt(avg_s)} | avg_close_long_sell={_fmt(avg_l)}")
    t1_ms = int(time.time() * 1000) + 10_000
    execs = await _bybit_fetch_executions(exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret, coin=coin, start_ms=t0_ms, end_ms=t1_ms)
    pnl_total, buys_n, sells_n, avg_buy, avg_sell = _bybit_calc_pnl_usdt_from_execs(execs)
    logger.info(
        f"📊 Итог (ТЕСТ): монета={coin} | "
        f"ср_цена_покупки={_fmt(avg_buy)} | ср_цена_продажи={_fmt(avg_sell)} | "
        f"покупок={buys_n} продаж={sells_n} | PnL_USDT_итого={_fmt(pnl_total, 3) if pnl_total is not None else 'N/A'}"
    )
    return True


async def _run_bybit_trade(bot: PerpArbitrageBot, p: FunParams) -> int:
    exchange_obj = bot.exchanges["bybit"]
    api_key = po._get_env("BYBIT_API_KEY") or ""
    api_secret = po._get_env("BYBIT_API_SECRET") or ""
    if not api_key or not api_secret:
        logger.error("❌ Нет BYBIT_API_KEY/BYBIT_API_SECRET в .env")
        return 2
    t0_ms = int(time.time() * 1000) - 10_000

    # Validate coin existence best-effort
    try:
        coins = await exchange_obj.get_all_futures_coins()
        if coins and p.coin not in coins:
            logger.error(f"❌ Bybit: монета {p.coin} не найдена в futures coins")
            return 2
    except Exception:
        pass

    # Получаем информацию о фандинге из API
    funding_info = await exchange_obj.get_funding_info(p.coin)
    if not funding_info:
        logger.error(f"❌ Не удалось получить информацию о фандинге для {p.coin}")
        return 2
    
    funding_rate = funding_info.get("funding_rate")
    next_funding_time = funding_info.get("next_funding_time")
    
    if funding_rate is None:
        logger.error(f"❌ Не удалось получить funding_rate для {p.coin}")
        return 2
    
    # Заполняем параметры из API
    p.funding_pct = float(funding_rate)
    payout_hhmm = _next_funding_time_to_local_hhmm(next_funding_time, "bybit")
    if payout_hhmm:
        p.payout_hh, p.payout_mm = payout_hhmm
    else:
        logger.error(f"❌ Не удалось получить время следующей выплаты для {p.coin}")
        return 2

    # Basic orderbook + filters + qty validation
    ticker = await exchange_obj.get_futures_ticker(p.coin)
    last_px = _ticker_last_price(ticker)
    if last_px is None or last_px <= 0:
        logger.error("❌ Bybit: не удалось получить last price для preflight")
        return 2

    try:
        min_test_qty, _filters = await _bybit_preflight_and_min_qty(
            exchange_obj=exchange_obj,
            coin=p.coin,
            qty_desired=p.coin_qty,
            price_hint=float(last_px),
        )
    except Exception as e:
        logger.error(f"❌ Preflight qty error: {e}")
        return 2

    ob = await exchange_obj.get_orderbook(p.coin, limit=MAIN_OB_LEVELS)
    if not ob or not ob.get("bids") or not ob.get("asks"):
        logger.error("❌ Bybit: не удалось получить orderbook для preflight")
        return 2

    bids = ob["bids"][:MAIN_OB_LEVELS]
    px_short_level, cum_bids = po._price_level_for_target_size(bids, p.coin_qty)
    if px_short_level is None:
        logger.error(f"❌ Недостаточно ликвидности в bids (1-{MAIN_OB_LEVELS}) | need={_fmt(p.coin_qty)} {p.coin} | available={_fmt(cum_bids)}")
        return 2

    asks = ob["asks"][:MAIN_OB_LEVELS]
    px_long_level, cum_asks = po._price_level_for_target_size(asks, p.coin_qty)
    if px_long_level is None:
        logger.error(f"❌ Недостаточно ликвидности в asks (1-{MAIN_OB_LEVELS}) | need={_fmt(p.coin_qty)} {p.coin} | available={_fmt(cum_asks)}")
        return 2

    # Balance check (best-effort): estimate required USDT for isolated 1x opening both legs.
    # We use worst prices from the current 15-level window.
    bid_entry_est = float(px_short_level)
    ask_worst_est = float(asks[min(len(asks), MAIN_OB_LEVELS) - 1][0]) if asks else float(px_long_level)
    required_usdt = float(p.coin_qty) * (max(0.0, bid_entry_est) + max(0.0, ask_worst_est)) + max(0.0, BALANCE_BUFFER_USDT)
    avail_usdt = await _bybit_get_usdt_available(exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret)
    if avail_usdt is None:
        logger.error("❌ Не удалось получить баланс USDT на Bybit (wallet-balance). Останавливаемся для безопасности.")
        return 2
    if avail_usdt + 1e-6 < required_usdt:
        logger.error(f"❌ Недостаточно баланса USDT | доступно={_fmt(avail_usdt, 3)} | нужно~{_fmt(required_usdt, 3)} (isolated 1x, оценка по стакану)")
        return 2

    # Check liquidity for Long (entry_long mode)
    notional_usdt = float(p.coin_qty) * float(last_px)
    long_liquidity = await exchange_obj.check_liquidity(
        p.coin,
        notional_usdt=notional_usdt,
        ob_limit=50,
        max_spread_bps=30.0,
        max_impact_bps=50.0,
        mode="entry_long",
    )

    # News check
    news_ok, news_msg, delisting_news, security_news = await _check_news_one_exchange(bot, "bybit", p.coin)
    if not news_ok:
        logger.error(f"❌ Новости/делистинг: {news_msg}")
        return 2

    # Format output
    cmd_str = f'python fun.py "{p.coin} {p.exchange.capitalize()} {_fmt(p.coin_qty)} {_fmt(p.offset_pct*100, 3)}%"'
    sep = "=" * 60
    coin_upper = p.coin.upper()
    exchange_cap = p.exchange.capitalize()
    price_str = _fmt(last_px, 3)
    notional_str = _fmt(notional_usdt, 3)
    funding_str = f"{_fmt(p.funding_pct*100, 3)}%" if p.funding_pct is not None else "N/A"
    payout_time_str = f"{p.payout_hh:02d}:{p.payout_mm:02d}" if p.payout_hh is not None and p.payout_mm is not None else "N/A"
    offset_str = f"{_fmt(p.offset_pct*100, 3)}%"
    min_qty_str = _fmt(min_test_qty)

    print(sep)
    print(f"Анализ арбитража для {exchange_cap} ({coin_upper})")
    print(sep)
    print(f"Цена: {price_str} (количество монет: {_fmt(p.coin_qty)} {coin_upper} | ~{notional_str} USDT)")
    print(f"Фандинг: {funding_str}")
    print(f"Время следующей выплаты: {payout_time_str}")
    print(f"Доп отступ вниз от справедливой цены: {offset_str}")
    print(f"Мин количество монет для ордера: {min_qty_str}")
    print(sep)

    # Liquidity check output
    if long_liquidity:
        status = "✅" if long_liquidity["ok"] else "❌"
        spread_bps = long_liquidity.get("spread_bps", 0.0)
        buy_impact = long_liquidity.get("buy_impact_bps")
        buy_impact_str = f"{buy_impact:.1f}bps" if buy_impact is not None else "0.0bps"
        logger.info(f"{status} Ликвидность {exchange_cap} Long ({coin_upper}): {notional_str} USDT | spread={spread_bps:.1f}bps, buy_impact={buy_impact_str}")
    else:
        logger.warning(f"Не удалось проверить ликвидность {exchange_cap} Long ({coin_upper}) для {notional_str} USDT")

    # News output
    if delisting_news:
        logger.info(f"❌ Найдены новости о делистинге {coin_upper} ({exchange_cap}) за последние {NEWS_DAYS_BACK} дней ({len(delisting_news)} шт.)")
    else:
        logger.info(f"✅ Новостей о делистинге {coin_upper} ({exchange_cap}) за последние {NEWS_DAYS_BACK} дней не найдено")

    if security_news:
        logger.info(f"❌ Найдены новости о взломах/безопасности {coin_upper} ({exchange_cap}) за последние {NEWS_DAYS_BACK} дней ({len(security_news)} шт.)")
    else:
        logger.info(f"✅ Новостей о взломах/безопасности {coin_upper} ({exchange_cap}) за последние {NEWS_DAYS_BACK} дней не найдено")

    # Ask about test orders
    ans = input("Совершить тестовые открытия шорт и лонг ? (Да/Нет): ").strip()
    if _is_yes(ans):
        logger.info("🧪 Тестовые ордера: запуск")
        ok_test = await _bybit_test_orders(bot, p.coin, min_test_qty)
        if not ok_test:
            logger.warning("⚠️ Тестовые ордера не прошли (или прошли частично) — продолжаем без остановки")
        else:
            logger.info("✅ Тестовые ордера прошли успешно")
    else:
        logger.info("⏭️ Тестовые ордера пропущены")

    # Ask about real positions
    ans2 = input("Открывать позиции лонг и шорт? (Да/Нет): ").strip()
    if not _is_yes(ans2):
        logger.info("Отклонено пользователем. Завершаем.")
        return 0

    # Проверяем, что funding_pct и payout_hh/payout_mm заполнены из API
    if p.funding_pct is None or p.payout_hh is None or p.payout_mm is None:
        logger.error("❌ Не удалось получить данные о фандинге из API")
        return 2

    payout_dt = _local_dt_today(p.payout_hh, p.payout_mm)
    now = datetime.now()
    if now > payout_dt:
        logger.error(f"❌ Скрипт запущен после времени выплаты: now={now.strftime('%H:%M:%S')} > payout={payout_dt.strftime('%H:%M:%S')}")
        return 2

    # --- FAST mode: everything heavy BEFORE 09:59:59, and no price/orderbook calls at 10:00:00 ---
    prep_dt = payout_dt - timedelta(seconds=max(0.0, float(FAST_PREP_LEAD_SEC)))
    if prep_dt < now:
        prep_dt = now
    logger.info(f"⏳ Ждём подготовки (filters/isolated/leverage) до: {prep_dt.strftime('%H:%M:%S')} (локальное время)")
    await _sleep_until(prep_dt)

    # Prepare trading settings (isolated + leverage 1) BEFORE payout time
    ok_prep = await po._prepare_exchange_for_trading(exchange_name="bybit", exchange_obj=exchange_obj, coin=p.coin)
    if not ok_prep:
        logger.error("❌ Не удалось подготовить торговые параметры (isolated/leverage=1)")
        return 2

    # Fetch filters BEFORE payout to avoid any extra calls at 10:00:00
    f = await _bybit_get_filters(exchange_obj, p.coin)
    tick_raw = f.get("tickSize")
    qty_step_raw = f.get("qtyStep")
    tick = float(tick_raw) if tick_raw else 0.0
    qty_str = po._format_by_step(p.coin_qty, qty_step_raw)

    fix_dt = payout_dt - timedelta(seconds=1)
    logger.info(f"⏳ Ждём фиксации цены/стакана: {fix_dt.strftime('%H:%M:%S')} (локальное время)")
    await _sleep_until(fix_dt)

    # Fix close_price at 09:59:59 using kline close for configured interval (can be 60m via FUN_FIX_KLINE_INTERVAL=60)
    t_fix = await exchange_obj.get_futures_ticker(p.coin)
    close_price = _ticker_last_price(t_fix)
    try:
        fix_ms = int(fix_dt.timestamp() * 1000)
        kline_close = await _bybit_kline_close_for_minute_ending_at(exchange_obj=exchange_obj, coin=p.coin, end_ms_inclusive=fix_ms)
        if kline_close is not None and kline_close > 0:
            close_price = float(kline_close)
        trade_px = await _bybit_last_trade_price_before(exchange_obj=exchange_obj, coin=p.coin, before_ms=fix_ms)
        if (kline_close is None or kline_close <= 0) and trade_px is not None and trade_px > 0:
            close_price = float(trade_px)
    except Exception:
        pass
    if close_price is None or close_price <= 0:
        logger.error("❌ Не удалось зафиксировать цену close_price в 09:59:59")
        return 2

    # fair_price = close_price - (close_price × (|funding_pct| + |offset_pct|))
    # 1. Складываем проценты фандинга и отступа (в абсолютных значениях)
    total_pct_abs = abs(p.funding_pct) + abs(p.offset_pct)  # 0.00941 + 0.002 = 0.01141
    # 2. Находим этот процент от close_price
    deduction = close_price * total_pct_abs  # 0.10412 × 0.01141 ≈ 0.00118801
    # 3. Вычитаем от close_price
    fair_price = close_price - deduction  # 0.10412 - 0.00118801 ≈ 0.10293199
    long_target_price = fair_price  # long_target = fair (одинаковая формула)
    logger.info(
        f"📌 Фиксация: close_price={_fmt(close_price)} | fair={_fmt(fair_price)} | long_target={_fmt(long_target_price)} "
        f"(funding={_fmt(p.funding_pct*100,3)}%, offset={_fmt(p.offset_pct*100,3)}%)"
    )

    # Snapshot orderbook at 09:59:59 (so that 10:00:00 has no orderbook calls).
    ob2 = await exchange_obj.get_orderbook(p.coin, limit=MAIN_OB_LEVELS)
    if not ob2 or not ob2.get("bids") or not ob2.get("asks"):
        logger.error("❌ Bybit: orderbook недоступен для открытия (09:59:59)")
        return 2

    # Decide opening short limit (aggressive) from bids snapshot: price at which cum bids >= qty (within first N levels).
    bids2 = (ob2.get("bids") or [])[: max(1, min(int(SHORT_OPEN_LEVELS), 10, MAIN_OB_LEVELS))]
    px_level_open, cum_open = po._price_level_for_target_size(bids2, float(p.coin_qty))
    if px_level_open is None:
        # IOC allows partial; still try at best bid
        try:
            px_level_open = float(bids2[0][0])
            cum_open = float(bids2[0][1]) if len(bids2[0]) > 1 else 0.0
        except Exception:
            logger.error("❌ Bybit: не удалось подобрать цену открытия Short по стакану")
            return 2
    px_open = po._round_price_for_side(float(px_level_open), tick, "sell")
    px_open_str = po._format_by_step(px_open, tick_raw)

    logger.info(
        f"🧠 План (БОЕВОЙ): открыть Short в {payout_dt.strftime('%H:%M:%S')} (Sell IOC) | qty={qty_str} | limit={px_open_str} | "
        f"cum_bids~{_fmt(cum_open)} | затем закрывать Short c { (payout_dt + timedelta(seconds=max(0.0, float(FAST_CLOSE_DELAY_SEC)))).strftime('%H:%M:%S') } (Buy reduceOnly IOC, {FAST_CLOSE_MAX_ATTEMPTS} попыток)"
    )

    logger.info(f"⏳ Ждём времени выплаты: {payout_dt.strftime('%H:%M:%S')} (локальное время)")
    await _sleep_until(payout_dt)

    # --- CRITICAL WINDOW: no logs & minimal overhead ---
    old_disable_level = None
    if int(FAST_SILENT_TRADING) == 1:
        old_disable_level = logging.root.manager.disable
        logging.disable(logging.CRITICAL)

    # 10:00:00.000 — place ONE Short (Sell IOC) with the precomputed limit, no orderbook/price calls here.
    short_order_id: Optional[str] = None
    try:
        try:
            short_order_id = await _bybit_place_limit(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                coin=p.coin,
                side="Sell",
                qty_str=qty_str,
                price_str=px_open_str,
                tif="IOC",
                reduce_only=None,
                position_idx=2,
            )
        except Exception:
            short_order_id = await _bybit_place_limit(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                coin=p.coin,
                side="Sell",
                qty_str=qty_str,
                price_str=px_open_str,
                tif="IOC",
                reduce_only=None,
                position_idx=None,
            )
    except Exception:
        short_order_id = None

    # 10:00:01 — start closing short (important to close even if open was partial)
    close_dt = payout_dt + timedelta(seconds=max(0.0, float(FAST_CLOSE_DELAY_SEC)))
    await _sleep_until(close_dt)

    # Re-enable logging after the strict window (but keep it minimal until fully closed)
    if old_disable_level is not None:
        logging.disable(old_disable_level)
        old_disable_level = None

    qty_pos = await _bybit_get_short_position_qty(exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret, coin=p.coin)
    if qty_pos <= 0:
        logger.warning("⚠️ Похоже, Short не открылся (позиция=0) — завершаем без закрытия")
        return 0

    # Close Short with aggressive Buy reduceOnly IOC; max 15 attempts as requested
    remaining = float(qty_pos)
    eps = max(1e-10, remaining * 1e-8)
    total_notional_close = 0.0
    total_filled_close = 0.0

    max_attempts = max(1, int(FAST_CLOSE_MAX_ATTEMPTS))
    for _ in range(1, max_attempts + 1):
        if remaining <= eps:
            break
        ob_c = await exchange_obj.get_orderbook(p.coin, limit=max(5, min(MAIN_OB_LEVELS, 25)))
        if not ob_c or not ob_c.get("asks"):
            continue
        asks = ob_c["asks"][: max(1, min(int(SHORT_OPEN_LEVELS), 10, len(ob_c["asks"])))]
        px_level_close, _cum_close = po._price_level_for_target_size(asks, remaining)
        if px_level_close is None:
            try:
                px_level_close = float(asks[-1][0])
            except Exception:
                continue
        px_close = po._round_price_for_side(float(px_level_close), tick, "buy")
        px_close_str = po._format_by_step(px_close, tick_raw)
        qty_rem_str = po._format_by_step(remaining, qty_step_raw)

        try:
            try:
                oid = await _bybit_place_limit(
                    exchange_obj=exchange_obj,
                    api_key=api_key,
                    api_secret=api_secret,
                    coin=p.coin,
                    side="Buy",
                    qty_str=qty_rem_str,
                    price_str=px_close_str,
                    tif="IOC",
                    reduce_only=True,
                    position_idx=2,
                )
            except Exception:
                oid = await _bybit_place_limit(
                    exchange_obj=exchange_obj,
                    api_key=api_key,
                    api_secret=api_secret,
                    coin=p.coin,
                    side="Buy",
                    qty_str=qty_rem_str,
                    price_str=px_close_str,
                    tif="IOC",
                    reduce_only=True,
                    position_idx=None,
                )
        except Exception:
            continue

        st_c, filled_c, avg_c = await _bybit_wait_done_get_filled_qty(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            coin=p.coin,
            order_id=str(oid),
            timeout_sec=0.6,
            poll_sleep_sec=0.03,
        )
        if filled_c and filled_c > 0:
            qf = float(filled_c)
            total_filled_close += qf
            px_use = float(avg_c) if (avg_c is not None and avg_c > 0) else float(px_close)
            total_notional_close += qf * px_use
            remaining = max(0.0, remaining - qf)

    # Final check: ensure flat
    qty_left = await _bybit_get_short_position_qty(exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret, coin=p.coin)
    if qty_left > eps:
        logger.error(f"❌ Не удалось закрыть Short полностью | осталось={_fmt(qty_left)}")
        return 2

    avg_exit_short = (total_notional_close / total_filled_close) if total_filled_close > 0 else None
    logger.info(f"✅ Short закрыт | qty={_fmt(qty_pos)} | avg_exit_buy={_fmt(avg_exit_short)}")

    t1_ms = int(time.time() * 1000) + 10_000
    execs = await _bybit_fetch_executions(exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret, coin=p.coin, start_ms=t0_ms, end_ms=t1_ms)
    pnl_hist, buys_n, sells_n, avg_buy, avg_sell = _bybit_calc_pnl_usdt_from_execs(execs)
    logger.info(
        f"📊 Итог (БОЕВОЙ): монета={p.coin} | "
        f"ср_цена_покупки={_fmt(avg_buy)} | ср_цена_продажи={_fmt(avg_sell)} | "
        f"покупок={buys_n} продаж={sells_n} | PnL_USDT_итого={_fmt(pnl_hist, 3) if pnl_hist is not None else 'N/A'}"
    )
    return 0


async def main() -> int:
    if len(sys.argv) < 2:
        logger.error('Usage: python fun.py "COIN EXCHANGE QTY OFFSET%" e.g. "STO Bybit 30 -0.3%"')
        return 2

    cmd = " ".join(sys.argv[1:]).strip()
    try:
        p = parse_cmd(cmd)
    except Exception as e:
        logger.error(f"❌ {e}")
        return 2

    bot = PerpArbitrageBot()
    try:
        if p.exchange != "bybit":
            logger.error(f"❌ Пока реализовано только для Bybit. Получено exchange={p.exchange!r}")
            return 2
        return await _run_bybit_trade(bot, p)
    finally:
        await bot.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))


