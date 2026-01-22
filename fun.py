# fun.py
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

# legacy/test knobs
SHORT_OPEN_LEVELS = int(os.getenv("FUN_SHORT_OPEN_LEVELS", "10"))
SHORT_OPEN_REFRESH_ROUNDS = int(os.getenv("FUN_SHORT_OPEN_REFRESH_ROUNDS", "2"))
LONG_OPEN_MAX_SEC = float(os.getenv("FUN_LONG_OPEN_MAX_SEC", "3.0"))
OPEN_LEAD_MS = int(os.getenv("FUN_OPEN_LEAD_MS", "150"))

# kline / wick
FIX_KLINE_INTERVAL = str(os.getenv("FUN_FIX_KLINE_INTERVAL", "1") or "1").strip()

# safety/news
NEWS_DAYS_BACK = int(os.getenv("FUN_NEWS_DAYS_BACK", "60"))
BALANCE_BUFFER_USDT = float(os.getenv("FUN_BALANCE_BUFFER_USDT", "0"))
BALANCE_FEE_SAFETY_BPS = float(os.getenv("FUN_BALANCE_FEE_SAFETY_BPS", "20"))  # 20 bps = 0.20% safety

# timing around payout
FAST_PREP_LEAD_SEC = float(os.getenv("FUN_FAST_PREP_LEAD_SEC", "2.0"))          # prep at payout-2s
FAST_CLOSE_DELAY_SEC = float(os.getenv("FUN_FAST_CLOSE_DELAY_SEC", "1.0"))      # start close at payout+1s
FAST_CLOSE_MAX_ATTEMPTS = int(os.getenv("FUN_FAST_CLOSE_MAX_ATTEMPTS", "15"))

# critical window: silence logs
FAST_SILENT_TRADING = int(os.getenv("FUN_FAST_SILENT_TRADING", "1"))            # 1 => disable logging during open/close window

# open timing
OPEN_AFTER_MS = int(os.getenv("FUN_OPEN_AFTER_MS", "10"))                       # открыть ПОСЛЕ payout (мс)

# open safety for Sell IOC (use fixed snapshot; no runtime orderbook calls)
OPEN_SAFETY_BPS = float(os.getenv("FUN_OPEN_SAFETY_BPS", "10"))                 # 10 bps = 0.10%
OPEN_SAFETY_MIN_TICKS = int(os.getenv("FUN_OPEN_SAFETY_MIN_TICKS", "3"))        # min ticks below best_bid_fix

# open ladder (multiple IOC create attempts, no orderbook/status calls in critical window)
OPEN_IOC_TRIES = int(os.getenv("FUN_OPEN_IOC_TRIES", "3"))
OPEN_IOC_EXTRA_BPS = float(os.getenv("FUN_OPEN_IOC_EXTRA_BPS", "50"))
OPEN_IOC_GAP_MS = int(os.getenv("FUN_OPEN_IOC_GAP_MS", "0"))

# late-start tolerance vs server-time
LATE_TOL_MS = int(os.getenv("FUN_LATE_TOL_MS", "400"))                          # 300-500ms recommended

# fix price selection
FIX_PRICE_MODE = str(os.getenv("FUN_FIX_PRICE_MODE", "last") or "last").strip().lower()

# opening fallback
SHORT_OPEN_FALLBACK_MARKET = int(os.getenv("FUN_SHORT_OPEN_FALLBACK_MARKET", "0"))


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


def _normalize_epoch_ms(x: Any) -> Optional[int]:
    """
    Универсальный нормализатор epoch времени:
    - ns -> ms
    - ms -> ms
    - seconds -> ms
    """
    if x is None:
        return None
    try:
        v = int(float(x))
    except Exception:
        return None
    if v >= 10**17:  # ns
        return int(v // 1_000_000)
    if v >= 10**12:  # ms
        return int(v)
    if v >= 10**9:  # seconds
        return int(v * 1000)
    return None


def _epoch_ms_to_local_hhmm(ms: Optional[int]) -> Optional[Tuple[int, int]]:
    if ms is None:
        return None
    try:
        dt = datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc).astimezone()
        return dt.hour, dt.minute
    except Exception:
        return None


async def _sleep_until_epoch_ms(target_epoch_ms: int) -> None:
    """
    Best-effort precise sleep until local epoch ms (time.time()*1000).
    Uses dynamic sleeps + short tail wait (Windows-friendly).
    """
    target = int(target_epoch_ms)
    while True:
        now_ms = int(time.time() * 1000)
        delta = target - now_ms
        if delta <= 0:
            return
        if delta > 1500:
            await asyncio.sleep(max(0.05, (delta - 800) / 1000.0))
            continue
        if delta > 300:
            await asyncio.sleep(max(0.02, (delta - 160) / 1000.0))
            continue
        if delta > 80:
            await asyncio.sleep(max(0.005, (delta - 40) / 1000.0))
            continue
        if delta > 25:
            await asyncio.sleep(max(0.001, delta / 2000.0))
            continue
        # Tail wait (<=25ms)
        deadline = time.perf_counter() + (delta / 1000.0)
        while time.perf_counter() < deadline:
            time.sleep(0.001)
        return


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


async def _bybit_place_market(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    coin: str,
    side: str,  # "Buy" | "Sell"
    qty_str: str,
    reduce_only: Optional[bool] = None,
    position_idx: Optional[int] = None,
) -> str:
    symbol = exchange_obj._normalize_symbol(coin)
    body: Dict[str, Any] = {
        "category": "linear",
        "symbol": symbol,
        "side": side,
        "orderType": "Market",
        "qty": qty_str,
        "timeInForce": "IOC",
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
        raise RuntimeError(f"Bybit create market order failed: {data}")
    order_id = (data.get("result") or {}).get("orderId") if isinstance(data.get("result"), dict) else None
    if not order_id:
        raise RuntimeError(f"Bybit create market order: no orderId: {data}")
    return str(order_id)


async def _bybit_private_get(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    path: str,
    params: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    try:
        data = await po._bybit_private_request(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            method="GET",
            path=path,
            params=params,
        )
        return data if isinstance(data, dict) else None
    except Exception:
        return None


async def _bybit_get_usdt_available(*, exchange_obj: Any, api_key: str, api_secret: str) -> Optional[float]:
    """
    Best-effort fetch of available USDT balance for derivatives trading.
    Try multiple accountType values (UNIFIED/CONTRACT).
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
        data = await _bybit_private_get(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
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
        data = await _bybit_private_get(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
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
    data = await _bybit_private_get(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
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
    data = await _bybit_private_get(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
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
    Public trades: /v5/market/recent-trade
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


async def _bybit_fetch_tickers_item(*, exchange_obj: Any, coin: str) -> Optional[Dict[str, Any]]:
    """
    /v5/market/tickers (raw item)
    """
    try:
        symbol = exchange_obj._normalize_symbol(coin)
        params: Dict[str, Any] = {"category": "linear", "symbol": symbol}
        data = await exchange_obj._request_json("GET", "/v5/market/tickers", params=params)
        if not (isinstance(data, dict) and data.get("retCode") == 0):
            return None
        items = (data.get("result") or {}).get("list") or []
        if not items or not isinstance(items, list) or not isinstance(items[0], dict):
            return None
        return items[0]
    except Exception:
        return None


async def _bybit_get_server_time_ms(*, exchange_obj: Any) -> Optional[int]:
    """
    /v5/market/time -> server epoch ms
    """
    try:
        data = await exchange_obj._request_json("GET", "/v5/market/time", params={})
        if not (isinstance(data, dict) and data.get("retCode") == 0):
            return None
        r = data.get("result") or {}
        if not isinstance(r, dict):
            return None
        tn = r.get("timeNano")
        if tn is not None:
            try:
                ns = int(str(tn))
                if ns > 0:
                    return int(ns // 1_000_000)
            except Exception:
                pass
        ts = r.get("timeSecond")
        if ts is not None:
            try:
                sec = int(str(ts))
                if sec > 0:
                    return int(sec * 1000)
            except Exception:
                pass
        for k in ("time", "serverTime", "server_time"):
            v = r.get(k)
            if v is None:
                continue
            try:
                x = int(float(v))
                if x > 10_000_000_000:  # ms
                    return x
                if x > 0:  # sec
                    return x * 1000
            except Exception:
                continue
        return None
    except Exception:
        return None


async def _bybit_estimate_time_offset_ms(*, exchange_obj: Any, samples: int = 5) -> Optional[int]:
    """
    Estimate offset_ms = server_ms - local_ms (local_ms is time.time()*1000).
    Uses multiple samples and returns median to reduce jitter.
    """
    try:
        n = max(1, min(int(samples), 9))
    except Exception:
        n = 5
    offsets: List[int] = []
    for _ in range(n):
        t0 = int(time.time() * 1000)
        srv = await _bybit_get_server_time_ms(exchange_obj=exchange_obj)
        t1 = int(time.time() * 1000)
        if srv is None:
            continue
        mid = (t0 + t1) // 2
        offsets.append(int(srv - mid))
        await asyncio.sleep(0.02)
    if not offsets:
        return None
    offsets.sort()
    return int(offsets[len(offsets) // 2])


def _bybit_parse_trade_time_ms(it: Dict[str, Any]) -> Optional[int]:
    for k in ("tradeTimeMs", "time", "execTime", "timestamp", "ts"):
        v = it.get(k)
        if v is None:
            continue
        try:
            t = int(float(v))
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
    /v5/market/kline -> list: [startTime, open, high, low, close, volume, turnover]
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


async def _bybit_kline_ohlc_for_bucket_ending_at(
    *,
    exchange_obj: Any,
    coin: str,
    end_ms_inclusive: int,
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Returns (open, high, low, close) for the configured kline interval bucket that contains end_ms_inclusive.
    """
    try:
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
                o = float(it[1])
            except Exception:
                o = None
            try:
                h = float(it[2])
            except Exception:
                h = None
            try:
                l = float(it[3])
            except Exception:
                l = None
            try:
                c = float(it[4])
            except Exception:
                c = None
            return o, h, l, c
    except Exception:
        pass
    return None, None, None, None


def _bybit_calc_pnl_usdt_from_execs(execs: List[Dict[str, Any]]) -> Tuple[Optional[float], int, int, Optional[float], Optional[float]]:
    """
    Returns:
      pnl_usdt_total (sell_notional - buy_notional - fee_total),
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

    fee_total = 0.0
    for it in execs:
        if not isinstance(it, dict):
            continue
        raw_fee = it.get("execFee")
        if raw_fee is not None:
            try:
                fee_total += abs(float(raw_fee))
            except Exception:
                pass
        elif it.get("execFeeRate") is not None:
            try:
                fee_rate = float(it.get("execFeeRate", 0))
                if abs(fee_rate) > 0.01:
                    fee_rate = fee_rate / 100.0
                px = float(it.get("execPrice") or 0.0)
                q = float(it.get("execQty") or 0.0)
                if px > 0 and q > 0:
                    fee_total += abs(px * q * fee_rate)
            except Exception:
                pass

    pnl = sell_notional - buy_notional - fee_total
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

    qty_step = float(qty_step_raw) if qty_step_raw else 0.0
    min_qty = float(min_qty_raw) if min_qty_raw else 0.0
    min_amt = float(min_amt_raw) if min_amt_raw else 0.0

    min_qty_from_amt = (min_amt / price_hint) if (min_amt > 0 and price_hint > 0) else 0.0
    q_min = max(min_qty, min_qty_from_amt)
    if qty_step > 0:
        q_min = po._ceil_to_step(q_min, qty_step)

    qty_norm = po._floor_to_step(float(qty_desired), qty_step) if qty_step > 0 else float(qty_desired)
    if min_qty > 0 and qty_norm < min_qty:
        raise ValueError(f"qty {qty_desired} (normalized={qty_norm}) < minOrderQty {min_qty_raw}")
    if min_amt > 0 and (qty_norm * float(price_hint)) < min_amt:
        raise ValueError(f"order notional too small: {qty_norm}*{price_hint} < minOrderAmt {min_amt_raw}")

    return float(q_min), f


async def _check_news_one_exchange(
    bot: PerpArbitrageBot,
    exchange: str,
    coin: str,
) -> Tuple[bool, str, List[Dict[str, Any]], List[Dict[str, Any]]]:
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

    # 1) Open test Short (Sell) FOK at price where cum bids >= qty
    bids = ob["bids"][:TEST_OB_LEVELS]
    px_level, cum = po._price_level_for_target_size(bids, qty_test)
    if px_level is None:
        logger.error(f"❌ Bybit test: недостаточно ликвидности в bids (1-{TEST_OB_LEVELS}) для qty={_fmt(qty_test)}")
        return False
    px = po._round_price_for_side(float(px_level), tick, "sell")
    px_str = po._format_by_step(px, tick_raw)

    logger.info(f"🧪 Тест: открываем Short (Sell FOK) | qty={qty_str} | лимит={px_str}")
    try:
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

    st_s, filled, short_entry_avg = await _bybit_get_order_status(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        coin=coin,
        order_id=str(short_order_id),
    )
    if not filled or filled + 1e-12 < float(qty_str):
        logger.error(f"❌ Bybit test: Short не исполнился полностью | status={st_s} | filled={_fmt(filled)}")
        return False
    logger.info(f"✅ Тест: Short открыт | filled={_fmt(filled)} {coin}")

    # 2) Open test Long (Buy) FOK
    asks = ob["asks"][:TEST_OB_LEVELS]
    px_level_a, cum_a = po._price_level_for_target_size(asks, qty_test)
    if px_level_a is None:
        logger.error(f"❌ Bybit test: недостаточно ликвидности в asks (1-{TEST_OB_LEVELS}) для qty={_fmt(qty_test)}")
        await po._bybit_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=coin, position_direction="short", coin_amount=qty_test)
        return False
    px_a = po._round_price_for_side(float(px_level_a), tick, "buy")
    px_a_str = po._format_by_step(px_a, tick_raw)

    logger.info(f"🧪 Тест: открываем Long (Buy FOK) | qty={qty_str} | лимит={px_a_str}")
    try:
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
        await po._bybit_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=coin, position_direction="short", coin_amount=qty_test)
        return False

    st_l, filled_l, long_entry_avg = await _bybit_get_order_status(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        coin=coin,
        order_id=str(long_order_id),
    )
    if not filled_l or filled_l + 1e-12 < float(qty_str):
        logger.warning(f"⚠️ Bybit test: Long не исполнился (FOK) | status={st_l} | filled={_fmt(filled_l)}. Закрываем позиции best-effort.")
        await po._bybit_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=coin, position_direction="short", coin_amount=qty_test)
        await po._bybit_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=coin, position_direction="long", coin_amount=qty_test)
        return False
    logger.info(f"✅ Тест: Long открыт | filled={_fmt(filled_l)} {coin}")

    # 3) Close both
    logger.info("🧪 Тест: закрываем обе позиции (Short+Long) по стакану")
    ok_s, avg_s = await po._bybit_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=coin, position_direction="short", coin_amount=qty_test, position_idx=2)
    ok_l, avg_l = await po._bybit_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=coin, position_direction="long", coin_amount=qty_test, position_idx=1)
    if not (ok_s and ok_l):
        logger.error(f"❌ Bybit test: не удалось закрыть обе позиции | short_ok={ok_s} long_ok={ok_l}")
        return False

    t1_ms = int(time.time() * 1000) + 10_000
    execs = await _bybit_fetch_executions(exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret, coin=coin, start_ms=t0_ms, end_ms=t1_ms)
    pnl_total, buys_n, sells_n, avg_buy, avg_sell = _bybit_calc_pnl_usdt_from_execs(execs)
    logger.info(
        f"📊 Итог (ТЕСТ): монета={coin} | ср_цена_покупки={_fmt(avg_buy)} | ср_цена_продажи={_fmt(avg_sell)} | "
        f"покупок={buys_n} продаж={sells_n} | PnL_USDT_итого={_fmt(pnl_total, 3) if pnl_total is not None else 'N/A'}"
    )
    return True


@dataclass
class FunParams:
    coin: str
    exchange: str
    coin_qty: float
    offset_pct: float  # decimal, e.g. -0.003
    funding_pct: Optional[float] = None
    payout_hh: Optional[int] = None
    payout_mm: Optional[int] = None
    next_funding_time_ms: Optional[int] = None  # server epoch ms


def parse_cmd(cmd: str) -> FunParams:
    parts = (cmd or "").strip().split()
    if len(parts) != 4:
        raise ValueError('bad command format. Expected: COIN EXCHANGE QTY OFFSET% e.g. "STO Bybit 30 -0.3%"')
    coin = parts[0].strip().upper()
    exchange = parts[1].strip().lower()
    coin_qty = float(parts[2])
    if coin_qty <= 0:
        raise ValueError(f"coin qty must be > 0, got: {coin_qty}")
    offset_pct = _parse_pct(parts[3])
    return FunParams(coin=coin, exchange=exchange, coin_qty=float(coin_qty), offset_pct=float(offset_pct))


def _task_result_safe(t: "asyncio.Task[Any]", default: Any) -> Any:
    try:
        if t.cancelled():
            return default
        exc = t.exception()
        if exc is not None:
            return default
        return t.result()
    except Exception:
        return default


async def _run_bybit_trade(bot: PerpArbitrageBot, p: FunParams) -> int:
    exchange_obj = bot.exchanges["bybit"]
    api_key = po._get_env("BYBIT_API_KEY") or ""
    api_secret = po._get_env("BYBIT_API_SECRET") or ""
    if not api_key or not api_secret:
        logger.error("❌ Нет BYBIT_API_KEY/BYBIT_API_SECRET в .env")
        return 2

    # Validate coin existence best-effort
    try:
        coins = await exchange_obj.get_all_futures_coins()
        if coins and p.coin not in coins:
            logger.error(f"❌ Bybit: монета {p.coin} не найдена в futures coins")
            return 2
    except Exception:
        pass

    # Funding info from exchange wrapper
    funding_info = await exchange_obj.get_funding_info(p.coin)
    if not funding_info:
        logger.error(f"❌ Не удалось получить информацию о фандинге для {p.coin}")
        return 2

    funding_rate = funding_info.get("funding_rate")
    next_funding_time = funding_info.get("next_funding_time")
    if funding_rate is None:
        logger.error(f"❌ Не удалось получить funding_rate для {p.coin}")
        return 2

    p.funding_pct = float(funding_rate)
    p.next_funding_time_ms = _normalize_epoch_ms(next_funding_time)
    payout_hhmm = _epoch_ms_to_local_hhmm(p.next_funding_time_ms)
    if payout_hhmm:
        p.payout_hh, p.payout_mm = payout_hhmm
    else:
        logger.error(f"❌ Не удалось получить время следующей выплаты | next_funding_time_raw={next_funding_time!r}")
        return 2

    # Preflight ticker/price
    ticker = await exchange_obj.get_futures_ticker(p.coin)
    last_px_pre = _ticker_last_price(ticker)
    if last_px_pre is None or last_px_pre <= 0:
        logger.error("❌ Bybit: не удалось получить last price для preflight")
        return 2

    # Preflight qty against filters
    try:
        min_qty_allowed, filters = await _bybit_preflight_and_min_qty(
            exchange_obj=exchange_obj,
            coin=p.coin,
            qty_desired=p.coin_qty,
            price_hint=float(last_px_pre),
        )
    except Exception as e:
        logger.error(f"❌ Preflight qty error: {e}")
        return 2

    qty_step_raw = filters.get("qtyStep")
    qty_step = float(qty_step_raw) if qty_step_raw else 0.0
    qty_norm = po._floor_to_step(float(p.coin_qty), qty_step) if qty_step > 0 else float(p.coin_qty)
    if qty_norm <= 0:
        logger.error("❌ qty после нормализации <= 0")
        return 2
    if qty_norm < float(min_qty_allowed):
        logger.error(f"❌ qty слишком маленький после нормализации | qty_norm={_fmt(qty_norm)} < min_qty={_fmt(min_qty_allowed)}")
        return 2
    p.coin_qty = float(qty_norm)

    # Orderbook liquidity precheck (not at open time)
    ob = await exchange_obj.get_orderbook(p.coin, limit=MAIN_OB_LEVELS)
    if not ob or not ob.get("bids"):
        logger.error("❌ Bybit: не удалось получить orderbook для preflight")
        return 2
    bids = ob["bids"][:MAIN_OB_LEVELS]
    px_short_level, cum_bids = po._price_level_for_target_size(bids, p.coin_qty)
    if px_short_level is None:
        logger.error(f"❌ Недостаточно ликвидности в bids (1-{MAIN_OB_LEVELS}) | need={_fmt(p.coin_qty)} | available={_fmt(cum_bids)}")
        return 2

    # Balance check (best-effort, not blocking)
    notional_est = float(p.coin_qty) * max(float(last_px_pre), float(px_short_level))
    fee_safety = notional_est * max(0.0, float(BALANCE_FEE_SAFETY_BPS)) / 10_000.0
    required_usdt_est = notional_est + max(0.0, float(BALANCE_BUFFER_USDT)) + fee_safety
    avail_usdt = await _bybit_get_usdt_available(exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret)
    if avail_usdt is None:
        logger.warning("⚠️ Не удалось получить баланс USDT (wallet-balance). Продолжаем (best-effort).")
    elif avail_usdt + 1e-6 < required_usdt_est:
        logger.warning(f"⚠️ Возможна нехватка USDT (оценка) | доступно={_fmt(avail_usdt, 3)} | нужно~{_fmt(required_usdt_est, 3)}")

    # News check
    news_ok, news_msg, delisting_news, security_news = await _check_news_one_exchange(bot, "bybit", p.coin)
    if not news_ok:
        logger.error(f"❌ Новости/делистинг: {news_msg}")
        return 2

    # Summary
    sep = "=" * 60
    coin_upper = p.coin.upper()
    exchange_cap = p.exchange.capitalize()
    payout_time_str = f"{p.payout_hh:02d}:{p.payout_mm:02d}" if p.payout_hh is not None and p.payout_mm is not None else "N/A"
    print(
        "\n".join(
            [
                sep,
                f"Анализ арбитража для {exchange_cap} ({coin_upper})",
                sep,
                f"Цена (pre): {_fmt(last_px_pre, 6)} | qty={_fmt(p.coin_qty)} {coin_upper} | notional~{_fmt(p.coin_qty*last_px_pre, 3)} USDT",
                f"Фандинг (pre): {_fmt(p.funding_pct*100, 6)}%",
                f"Время следующей выплаты (local): {payout_time_str}",
                f"Доп отступ: {_fmt(p.offset_pct*100, 6)}%",
                f"Мин qty: {_fmt(min_qty_allowed)} | qty_norm: {_fmt(p.coin_qty)}",
                sep,
            ]
        )
    )

    if delisting_news:
        logger.info(f"❌ Найдены новости о делистинге {coin_upper} ({exchange_cap}) за {NEWS_DAYS_BACK} дней ({len(delisting_news)} шт.)")
    else:
        logger.info(f"✅ Новостей о делистинге {coin_upper} ({exchange_cap}) за {NEWS_DAYS_BACK} дней не найдено")

    if security_news:
        logger.info(f"❌ Найдены security-новости {coin_upper} ({exchange_cap}) за {NEWS_DAYS_BACK} дней ({len(security_news)} шт.)")
    else:
        logger.info(f"✅ Security-новостей {coin_upper} ({exchange_cap}) за {NEWS_DAYS_BACK} дней не найдено")

    ans = input("Совершить тестовые открытия шорт и лонг ? (Да/Нет): ").strip()
    if _is_yes(ans):
        logger.info("🧪 Тестовые ордера: запуск")
        ok_test = await _bybit_test_orders(bot, p.coin, float(min_qty_allowed))
        if not ok_test:
            logger.warning("⚠️ Тестовые ордера не прошли (или прошли частично) — продолжаем")
        else:
            logger.info("✅ Тестовые ордера прошли успешно")
    else:
        logger.info("⏭️ Тестовые ордера пропущены")

    # mode is short-only on negative funding
    if p.funding_pct is None or float(p.funding_pct) >= 0:
        logger.error(f"❌ Funding не отрицательный ({_fmt(p.funding_pct*100,6)}%). Этот режим рассчитан на отрицательный funding.")
        return 2

    ans2 = input("Открывать БОЕВОЙ short после payout? (Да/Нет): ").strip()
    if not _is_yes(ans2):
        logger.info("Отклонено пользователем. Завершаем.")
        return 0

    if p.next_funding_time_ms is None:
        logger.error("❌ Не удалось получить next_funding_time_ms")
        return 2

    payout_server_ms = int(p.next_funding_time_ms)

    # Sync local with server clock (critical for scheduling)
    offset_ms = await _bybit_estimate_time_offset_ms(exchange_obj=exchange_obj, samples=5)
    if offset_ms is None:
        logger.error("❌ Не удалось получить Bybit server time (market/time)")
        return 2

    now_local_ms = int(time.time() * 1000)
    now_server_est_ms = int(now_local_ms + int(offset_ms))
    if now_server_est_ms > payout_server_ms + int(max(0, int(LATE_TOL_MS))):
        logger.error("❌ Скрипт запущен слишком поздно относительно времени выплаты (по server time Bybit)")
        return 2

    # Server-time schedule
    prep_server_ms = int(payout_server_ms - int(max(0.0, float(FAST_PREP_LEAD_SEC)) * 1000))
    prep_local_ms = int(prep_server_ms - offset_ms)
    logger.info(
        f"⏳ Ждём подготовки (server-time) | offset_ms={offset_ms} | до: {datetime.fromtimestamp(prep_local_ms/1000.0).strftime('%H:%M:%S')} (локальное время)"
    )
    await _sleep_until_epoch_ms(prep_local_ms)

    # Prepare exchange settings BEFORE payout
    ok_prep = await po._prepare_exchange_for_trading(exchange_name="bybit", exchange_obj=exchange_obj, coin=p.coin)
    if not ok_prep:
        logger.error("❌ Не удалось подготовить торговые параметры (isolated/leverage=1)")
        return 2

    # Fetch filters BEFORE payout
    f = await _bybit_get_filters(exchange_obj, p.coin)
    tick_raw = f.get("tickSize")
    qty_step_raw = f.get("qtyStep")
    tick = float(tick_raw) if tick_raw else 0.0
    qty_str = po._format_by_step(p.coin_qty, qty_step_raw)

    # Fix snapshot at payout-1000ms (server time)
    fix_server_ms = int(payout_server_ms - 1000)
    fix_local_ms = int(fix_server_ms - offset_ms)
    logger.info(f"⏳ Ждём фиксации (server-time): {datetime.fromtimestamp(fix_local_ms/1000.0).strftime('%H:%M:%S')} (локальное время)")
    await _sleep_until_epoch_ms(fix_local_ms)

    # Collect snapshot in parallel
    fix_ms = int(fix_server_ms)
    tick_item_task = asyncio.create_task(_bybit_fetch_tickers_item(exchange_obj=exchange_obj, coin=p.coin))
    ob2_task = asyncio.create_task(exchange_obj.get_orderbook(p.coin, limit=MAIN_OB_LEVELS))
    trades_task = asyncio.create_task(_bybit_fetch_recent_trades(exchange_obj=exchange_obj, coin=p.coin, limit=200))
    ohlc_task = asyncio.create_task(_bybit_kline_ohlc_for_bucket_ending_at(exchange_obj=exchange_obj, coin=p.coin, end_ms_inclusive=fix_ms))

    done, pending = await asyncio.wait({tick_item_task, ob2_task, trades_task, ohlc_task}, timeout=0.9)
    for t in pending:
        t.cancel()

    tick_item = _task_result_safe(tick_item_task, None)
    ob2 = _task_result_safe(ob2_task, None)
    trades = _task_result_safe(trades_task, [])
    ohlc = _task_result_safe(ohlc_task, (None, None, None, None))

    # Delayed logs (print only after critical window)
    post_logs: List[str] = []

    # Funding just before payout: prefer predictedFundingRate
    pred_fr: Optional[float] = None
    fr: Optional[float] = None
    if isinstance(tick_item, dict):
        for k in ("predictedFundingRate", "predFundingRate", "predicted_funding_rate"):
            v = tick_item.get(k)
            if v is None:
                continue
            try:
                pred_fr = float(v)
                break
            except Exception:
                pass
        v = tick_item.get("fundingRate")
        if v is not None:
            try:
                fr = float(v)
            except Exception:
                pass

    if pred_fr is not None:
        p.funding_pct = float(pred_fr)
    elif fr is not None:
        p.funding_pct = float(fr)

    # Final in-memory funding gate (no extra network calls)
    abort_reason: Optional[str] = None
    if p.funding_pct is None:
        abort_reason = "funding перед payout не подтверждён (tickers=None)"
    elif float(p.funding_pct) >= 0:
        abort_reason = f"funding стал неотрицательным перед payout: {p.funding_pct*100:.6f}%"

    if abort_reason:
        post_logs.append(f"⛔ ОТМЕНА БОЕВОГО ВХОДА: {abort_reason}. Режим short-only работает только при отрицательном funding.")
        for m in post_logs:
            logger.info(m)
        return 0

    # Extract best bid/ask from orderbook snapshot
    best_bid_fix: Optional[float] = None
    best_ask_fix: Optional[float] = None
    if isinstance(ob2, dict):
        try:
            if ob2.get("bids"):
                best_bid_fix = float(ob2["bids"][0][0])
            if ob2.get("asks"):
                best_ask_fix = float(ob2["asks"][0][0])
        except Exception:
            best_bid_fix = None
            best_ask_fix = None

    # Extract last/bid/ask from tickers snapshot
    last_px: Optional[float] = None
    bid1: Optional[float] = None
    ask1: Optional[float] = None
    if isinstance(tick_item, dict):
        try:
            last_px = float(tick_item.get("lastPrice")) if tick_item.get("lastPrice") is not None else None
        except Exception:
            last_px = None
        try:
            bid1 = float(tick_item.get("bid1Price")) if tick_item.get("bid1Price") is not None else None
        except Exception:
            bid1 = None
        try:
            ask1 = float(tick_item.get("ask1Price")) if tick_item.get("ask1Price") is not None else None
        except Exception:
            ask1 = None

    # Last trade price <= fix_ms
    trade_px: Optional[float] = None
    try:
        best_t: Optional[int] = None
        best_px: Optional[float] = None
        for it in (trades or []):
            if not isinstance(it, dict):
                continue
            tms = _bybit_parse_trade_time_ms(it)
            if tms is None or tms > fix_ms:
                continue
            px = _bybit_parse_trade_price(it)
            if px is None:
                continue
            if best_t is None or tms > best_t:
                best_t = tms
                best_px = float(px)
        if best_px is not None and best_px > 0:
            trade_px = float(best_px)
    except Exception:
        trade_px = None

    # OHLC
    _o, k_high, _l, k_close = ohlc if isinstance(ohlc, tuple) and len(ohlc) == 4 else (None, None, None, None)

    def _pick_fix_price() -> Optional[float]:
        base_last = trade_px if (trade_px is not None and trade_px > 0) else (last_px if (last_px is not None and last_px > 0) else None)
        if FIX_PRICE_MODE in ("last", "trade_last", "trade_or_last"):
            return base_last
        if FIX_PRICE_MODE in ("kclose", "kline_close"):
            return k_close if (k_close is not None and k_close > 0) else base_last
        if FIX_PRICE_MODE in ("wick", "high", "khigh", "kline_high"):
            return k_high if (k_high is not None and k_high > 0) else base_last
        if FIX_PRICE_MODE in ("max", "max_all"):
            candidates: List[float] = []
            for v in (trade_px, last_px, ask1, best_ask_fix, bid1, best_bid_fix, k_high, k_close):
                try:
                    if v is not None and float(v) > 0:
                        candidates.append(float(v))
                except Exception:
                    continue
            return max(candidates) if candidates else base_last
        return base_last

    close_price = _pick_fix_price()
    if close_price is None or close_price <= 0:
        logger.error("❌ Не удалось зафиксировать цену close_price (HH:MM:59)")
        return 2

    # fair price formula
    total_pct_abs = abs(float(p.funding_pct)) + abs(float(p.offset_pct))
    fair_price = float(close_price) - (float(close_price) * float(total_pct_abs))
    long_target_price = fair_price

    post_logs.append(
        f"📌 Фиксация: close_price={_fmt(close_price)} | fair={_fmt(fair_price)} | long_target={_fmt(long_target_price)} "
        f"(funding={_fmt(p.funding_pct*100,6)}%, offset={_fmt(p.offset_pct*100,6)}%)"
    )
    post_logs.append(
        f"📌 Диагностика: last={_fmt(last_px)} trade={_fmt(trade_px)} ask1={_fmt(ask1)} bid1={_fmt(bid1)} "
        f"| ob_best_bid={_fmt(best_bid_fix)} ob_best_ask={_fmt(best_ask_fix)} | k_high={_fmt(k_high)} k_close={_fmt(k_close)} "
        f"| fundingRate={_fmt(fr*100,6) if fr is not None else 'N/A'} predicted={_fmt(pred_fr*100,6) if pred_fr is not None else 'N/A'} "
        f"| fix_price_mode={FIX_PRICE_MODE}"
    )

    if not isinstance(ob2, dict) or not ob2.get("bids") or not ob2.get("asks"):
        logger.error("❌ Bybit: orderbook недоступен для открытия (HH:MM:59)")
        return 2

    # Plan times
    open_server_ms = int(payout_server_ms + max(0, int(OPEN_AFTER_MS)))
    open_local_ms = int(open_server_ms - offset_ms)
    close_server_ms = int(payout_server_ms + int(max(0.0, float(FAST_CLOSE_DELAY_SEC)) * 1000))
    close_local_ms = int(close_server_ms - offset_ms)

    # -----------------------------
    # OPEN PRICE PLAN (NO EXTRA CALLS AT OPEN TIME)
    # -----------------------------
    # Мы хотим, чтобы Sell IOC был marketable даже при резком падении цены между fix (HH:MM:59)
    # и отправкой (HH:MM:00.xxx). Поэтому:
    # 1) базовый лимит = min(best_bid_fix - safety, fair_price)
    # 2) в критический момент отправляем ЛЕСЕНКУ IOC ордеров всё ниже и ниже (без запросов стакана/статуса)
    if tick and float(tick) > 0:
        _tick = float(tick)
    else:
        _tick = 0.0

    def _floor_to_tick(px: float) -> float:
        if _tick <= 0:
            return float(px)
        return po._floor_to_step(float(px), float(_tick))

    base_open_raw: float
    if best_bid_fix and float(best_bid_fix) > 0:
        min_ticks = max(0, int(OPEN_SAFETY_MIN_TICKS))
        safety_bps = max(0.0, float(OPEN_SAFETY_BPS))
        ded_ticks = float(min_ticks) * (_tick if _tick > 0 else 0.0)
        ded_bps = float(best_bid_fix) * (safety_bps / 10_000.0)
        ded = max(ded_ticks, ded_bps)
        base_open_raw = float(best_bid_fix) - float(ded)
        # Ключевое: дополнительно прижимаем вниз до fair_price,
        # чтобы пережить резкий слив сразу после payout.
        if fair_price and float(fair_price) > 0:
            base_open_raw = min(float(base_open_raw), float(fair_price))
    else:
        # если bid_fix нет — открываемся от fair_price (она уже "низкая")
        base_open_raw = float(fair_price) if (fair_price and float(fair_price) > 0) else float(close_price)

    base_open = _floor_to_tick(base_open_raw)
    if base_open <= 0:
        logger.error("❌ base_open для Sell IOC получился <= 0")
        return 2

    # Собираем лесенку цен (вниз). ВАЖНО: не делаем никаких запросов на биржу ради расчёта.
    tries = max(1, min(int(OPEN_IOC_TRIES), 6))  # hard cap
    extra_bps = max(0.0, float(OPEN_IOC_EXTRA_BPS))

    open_prices: List[float] = []
    for i in range(tries):
        # i=0: base_open
        # i=1: base_open*(1 - extra_bps)
        # i=2: base_open*(1 - 2*extra_bps)
        factor = 1.0 - (extra_bps / 10_000.0) * float(i)
        px_i = float(base_open) * float(factor)
        px_i = _floor_to_tick(px_i)
        if px_i > 0:
            open_prices.append(px_i)

    # Уберём дубликаты после floor-to-tick (чтобы не слать одинаковые цены)
    uniq_prices: List[float] = []
    last_v: Optional[float] = None
    for v in open_prices:
        if last_v is None or abs(v - last_v) > 1e-12:
            uniq_prices.append(v)
        last_v = v

    if not uniq_prices:
        logger.error("❌ Не удалось собрать open_prices для лесенки Sell IOC")
        return 2

    # Для логов (после окна) сохраняем только первую цену
    px_open = float(uniq_prices[0])
    px_open_str = po._format_by_step(px_open, tick_raw)

    open_local_str = datetime.fromtimestamp(open_local_ms / 1000.0).strftime("%H:%M:%S")
    close_local_str = datetime.fromtimestamp(close_local_ms / 1000.0).strftime("%H:%M:%S")
    ladder_preview = ", ".join([po._format_by_step(float(x), tick_raw) for x in uniq_prices[: min(5, len(uniq_prices))]])

    post_logs.append(
        f"🧠 План (БОЕВОЙ, server-time): открыть Short в {open_local_str} (Sell IOC ЛЕСЕНКА, ПОСЛЕ payout+{OPEN_AFTER_MS}ms) | qty={qty_str} | "
        f"open_prices=[{ladder_preview}{'...' if len(uniq_prices) > 5 else ''}] "
        f"(best_bid_fix={_fmt(best_bid_fix)}, fair={_fmt(fair_price)}, safety={OPEN_SAFETY_BPS}bps|min_ticks={OPEN_SAFETY_MIN_TICKS}, "
        f"extra={OPEN_IOC_EXTRA_BPS}bps, tries={OPEN_IOC_TRIES}) | "
        f"затем закрывать Short c {close_local_str} (Buy reduceOnly IOC, эскалация)"
    )

    # Wait until open time
    await _sleep_until_epoch_ms(open_local_ms)

    # ----------------------------
    # CRITICAL WINDOW: минимальный overhead
    # ----------------------------
    old_disable_level = None
    if int(FAST_SILENT_TRADING) == 1:
        old_disable_level = logging.root.manager.disable
        logging.disable(logging.CRITICAL)

    # 10:00:00.000 — send multiple Sell IOC orders (ladder) with precomputed limits.
    # NO orderbook/status calls here. Just fire-and-forget creates.
    short_order_id: Optional[str] = None
    short_place_err: Optional[str] = None

    gap_ms = max(0, int(OPEN_IOC_GAP_MS))

    # IMPORTANT: positionIdx may fail depending on account mode (hedge vs one-way),
    # so we try hedge first, then one-way — for EACH ladder step.
    for px_try in uniq_prices:
        px_try_str = po._format_by_step(float(px_try), tick_raw)
        try:
            try:
                short_order_id = await _bybit_place_limit(
                    exchange_obj=exchange_obj,
                    api_key=api_key,
                    api_secret=api_secret,
                    coin=p.coin,
                    side="Sell",
                    qty_str=qty_str,
                    price_str=px_try_str,
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
                    price_str=px_try_str,
                    tif="IOC",
                    reduce_only=None,
                    position_idx=None,
                )
            # Не проверяем статус/filled здесь — это критическое окно.
        except Exception as e:
            short_place_err = str(e)
            short_order_id = None

        if gap_ms > 0:
            # микропаузу делаем через time.sleep (не await), чтобы не уступать event loop лишний раз
            time.sleep(gap_ms / 1000.0)

    # Wait until close start time (still no extra calls)
    await _sleep_until_epoch_ms(close_local_ms)

    # End critical window: re-enable logs
    if old_disable_level is not None:
        logging.disable(old_disable_level)
        old_disable_level = None

    # Print delayed logs here (after critical window)
    for m in post_logs:
        logger.info(m)
    if short_place_err:
        logger.warning(f"⚠️ Short: ошибка создания ордера: {short_place_err}")

    # IMPORTANT: no market fallback here — only ladder IOC creates.

    # Query position qty and close it
    qty_pos = await _bybit_get_short_position_qty(exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret, coin=p.coin)
    if qty_pos <= 0:
        logger.warning("⚠️ Похоже, Short не открылся (позиция=0). Завершаем.")
        return 0

    ok_close, avg_exit_short = await po._bybit_close_leg_partial_ioc(
        exchange_obj=exchange_obj,
        coin=p.coin,
        position_direction="short",
        coin_amount=float(qty_pos),
        position_idx=2,
    )
    if not ok_close:
        ok_close, avg_exit_short = await po._bybit_close_leg_partial_ioc(
            exchange_obj=exchange_obj,
            coin=p.coin,
            position_direction="short",
            coin_amount=float(qty_pos),
            position_idx=None,
        )

    if ok_close:
        logger.info(f"✅ Short закрыт | qty={_fmt(qty_pos)} | avg_exit_buy={_fmt(avg_exit_short)}")
    else:
        logger.error(f"❌ Short НЕ закрыт полностью (best-effort) | qty={_fmt(qty_pos)} | avg_exit_buy={_fmt(avg_exit_short)}")

    # Post-trade executions / PnL
    t_start_ms = int(open_server_ms - 5_000)
    t_end_ms = int(close_server_ms + 10_000)
    execs = await _bybit_fetch_executions(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        coin=p.coin,
        start_ms=t_start_ms,
        end_ms=t_end_ms,
    )
    pnl_hist, buys_n, sells_n, avg_buy, avg_sell = _bybit_calc_pnl_usdt_from_execs(execs)
    logger.info(
        f"📊 Итог (БОЕВОЙ): монета={p.coin} | ср_цена_покупки={_fmt(avg_buy)} | ср_цена_продажи={_fmt(avg_sell)} | "
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
