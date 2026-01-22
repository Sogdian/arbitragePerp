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
from exchanges.bybit_ws import BybitPublicWS
from exchanges.bybit_ws_trade import BybitTradeWS
from exchanges.bybit_ws_private import BybitPrivateWS


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
FAST_CLOSE_DELAY_SEC = float(os.getenv("FUN_FAST_CLOSE_DELAY_SEC", "1.2"))      # start close at payout+1s
FAST_CLOSE_MAX_ATTEMPTS = int(os.getenv("FUN_FAST_CLOSE_MAX_ATTEMPTS", "15"))

# critical window: silence logs
FAST_SILENT_TRADING = int(os.getenv("FUN_FAST_SILENT_TRADING", "1"))            # 1 => disable logging during open/close window

# open timing
OPEN_AFTER_MS = int(os.getenv("FUN_OPEN_AFTER_MS", "0"))                       # открыть ПОСЛЕ payout (мс) - теперь 0 для WS

# WS strategy: "enter near close_price or abort"
WS_FIX_LEAD_MS = int(os.getenv("FUN_WS_FIX_LEAD_MS", "30"))                 # за сколько мс до payout фиксируем ref
OPEN_MAX_STALENESS_MS = int(os.getenv("FUN_OPEN_MAX_STALENESS_MS", "200"))  # если WS-данные старее — abort
OPEN_SAFETY_TICKS = int(os.getenv("FUN_OPEN_SAFETY_TICKS", "1"))            # 1 тик агрессии для sell
USE_TRADE_WS = int(os.getenv("FUN_USE_TRADE_WS", "1"))

# Rule of admission: soft/hard допуски для падения рынка
OPEN_SOFT_DOWN_BPS = float(os.getenv("FUN_OPEN_SOFT_DOWN_BPS", "600"))      # 6% — не узко, предупреждение
OPEN_HARD_DOWN_BPS = float(os.getenv("FUN_OPEN_HARD_DOWN_BPS", "2500"))     # 25% — жесткий стоп, abort

# open safety for Sell IOC (use fixed snapshot; no runtime orderbook calls)
OPEN_SAFETY_BPS = float(os.getenv("FUN_OPEN_SAFETY_BPS", "10"))                 # 10 bps = 0.10%
OPEN_SAFETY_MIN_TICKS = int(os.getenv("FUN_OPEN_SAFETY_MIN_TICKS", "3"))        # min ticks below best_bid_fix

# open ladder (multiple IOC create attempts, no orderbook/status calls in critical window)
OPEN_IOC_TRIES = int(os.getenv("FUN_OPEN_IOC_TRIES", "3"))
OPEN_IOC_EXTRA_BPS = float(os.getenv("FUN_OPEN_IOC_EXTRA_BPS", "50"))
OPEN_IOC_GAP_MS = int(os.getenv("FUN_OPEN_IOC_GAP_MS", "0"))

# ---- Phase-1 (quality entry near best_bid_fix) ----
OPEN_PHASE1_TRIES = int(os.getenv("FUN_OPEN_PHASE1_TRIES", "1"))
OPEN_PHASE1_MIN_TICKS = int(os.getenv("FUN_OPEN_PHASE1_MIN_TICKS", "1"))
OPEN_PHASE1_SAFETY_BPS = float(os.getenv("FUN_OPEN_PHASE1_SAFETY_BPS", "0"))
OPEN_PHASE1_WAIT_MS = int(os.getenv("FUN_OPEN_PHASE1_WAIT_MS", "30"))

# ---- Phase-2 (guarantee fill if price dumps) ----
OPEN_PHASE2_ENABLE = int(os.getenv("FUN_OPEN_PHASE2_ENABLE", "1"))
OPEN_PHASE2_TRIES = int(os.getenv("FUN_OPEN_PHASE2_TRIES", "6"))
OPEN_PHASE2_EXTRA_BPS = float(os.getenv("FUN_OPEN_PHASE2_EXTRA_BPS", "200"))
OPEN_PHASE2_MIN_TICKS = int(os.getenv("FUN_OPEN_PHASE2_MIN_TICKS", str(OPEN_SAFETY_MIN_TICKS)))
OPEN_PHASE2_SAFETY_BPS = float(os.getenv("FUN_OPEN_PHASE2_SAFETY_BPS", str(OPEN_SAFETY_BPS)))

# guardrail
OPEN_HARD_CAP_ENABLE = int(os.getenv("FUN_OPEN_HARD_CAP_ENABLE", "1"))

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
    Best-effort precise sleep until local epoch ms (time.time()*1000),
    WITHOUT blocking the event loop (no time.sleep).
    """
    target = int(target_epoch_ms)
    while True:
        now_ms = int(time.time() * 1000)
        delta = target - now_ms
        if delta <= 0:
            return
        # coarse sleep
        if delta > 200:
            await asyncio.sleep((delta - 80) / 1000.0)
            continue
        if delta > 40:
            await asyncio.sleep((delta - 15) / 1000.0)
            continue
        if delta > 8:
            await asyncio.sleep(delta / 2000.0)  # half of remaining
            continue
        # last ~8ms: yield loop without blocking
        await asyncio.sleep(0)


async def _sleep_until_server_ms(target_server_ms: int, offset_ms: int) -> None:
    """
    Sleep until server time target.
    server_ms = local_ms + offset_ms  => local_target = server_target - offset_ms
    """
    await _sleep_until_epoch_ms(int(target_server_ms - offset_ms))


def _extract_order_id_from_trade_ack(ack: dict) -> Optional[str]:
    """
    Извлекает order_id из ответа Trade WS create_order.
    
    Bybit Trade WS ответы могут иметь разную структуру, поэтому проверяем несколько путей.
    """
    if not isinstance(ack, dict):
        return None
    
    for path in (
        ("result", "orderId"),
        ("data", "orderId"),
        ("data", "result", "orderId"),
        ("result", "list", 0, "orderId"),
        ("data", "list", 0, "orderId"),
    ):
        cur = ack
        ok = True
        for key in path:
            try:
                cur = cur[key] if isinstance(key, int) else cur.get(key)
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


def _extract_order_id_from_trade_ack(ack: dict) -> Optional[str]:
    """
    Извлекает order_id из ответа Trade WS create_order.
    
    Bybit Trade WS ответы могут иметь разную структуру, поэтому проверяем несколько путей.
    """
    if not isinstance(ack, dict):
        return None
    
    for path in (
        ("result", "orderId"),
        ("data", "orderId"),
        ("data", "result", "orderId"),
        ("result", "list", 0, "orderId"),
        ("data", "list", 0, "orderId"),
    ):
        cur = ack
        ok = True
        for key in path:
            try:
                cur = cur[key] if isinstance(key, int) else cur.get(key)
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


async def _bybit_detect_position_idx(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    symbol: str,
) -> int:
    """
    Return preferred positionIdx for opening SHORT:
      - 2 for hedge-mode short leg
      - 0 for one-way mode
    Uses REST once (NOT in critical window).
    """
    data = await _bybit_private_get(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        path="/v5/position/list",
        params={"category": "linear", "symbol": str(symbol)},
    )
    if not (isinstance(data, dict) and data.get("retCode") == 0):
        return 0
    items = ((data.get("result") or {}).get("list") or [])
    if not isinstance(items, list):
        return 0
    # If we see positionIdx 1/2 in response, account is likely hedge-mode.
    seen = set()
    for it in items:
        if not isinstance(it, dict):
            continue
        try:
            pi = int(it.get("positionIdx", 0))
        except Exception:
            pi = 0
        seen.add(pi)
    if 2 in seen or 1 in seen:
        return 2
    return 0


async def _bybit_get_short_qty_snapshot(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    coin: str,
    ws_private: Optional["BybitPrivateWS"] = None,
    symbol: Optional[str] = None,
    position_idx: int = 2,
) -> float:
    """
    Snapshot total short size (Sell side) for symbol.
    Use BEFORE/AFTER to compute delta-opened.
    """
    # Fast path: use Private WS cached position (0-REST)
    try:
        if ws_private is not None and getattr(ws_private, "ready", False) and symbol:
            v = ws_private.get_position_size(symbol=str(symbol), position_idx=int(position_idx), side="Sell")
            if v is not None:
                return float(v)
            # best-effort wait for initial snapshot
            try:
                await ws_private.wait_position(symbol=str(symbol), position_idx=int(position_idx), side="Sell", timeout=0.8)
            except Exception:
                pass
            v2 = ws_private.get_position_size(symbol=str(symbol), position_idx=int(position_idx), side="Sell")
            if v2 is not None:
                return float(v2)
    except Exception:
        # fallback below
        pass

    # Fallback: REST position/list (can be slow; avoid in critical windows)
    return float(
        await _bybit_get_short_position_qty(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            coin=coin,
        )
    )


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
    """
    Тест (Вариант A, устойчивый к микросдвигам рынка):
      1) Открыть Short (Sell FOK) как marketable-limit от best_bid (best_bid - N*tick),
         с несколькими попытками (переснимаем стакан на каждую попытку).
      2) Сразу закрыть этот Short (Buy IOC) (несколько попыток).
      3) Посчитать executions только по orderId-ам этой тестовой сделки.
    """
    exchange_obj = bot.exchanges["bybit"]
    api_key = po._get_env("BYBIT_API_KEY") or ""
    api_secret = po._get_env("BYBIT_API_SECRET") or ""
    if not api_key or not api_secret:
        logger.error("❌ Нет BYBIT_API_KEY/BYBIT_API_SECRET в .env")
        return False

    # --- time offset (best-effort) to build tight executions window in server-ms ---
    offset_ms = await _bybit_estimate_time_offset_ms(exchange_obj=exchange_obj, samples=5)
    if offset_ms is None:
        offset_ms = 0

    def _srv_ms() -> int:
        return int(time.time() * 1000) + int(offset_ms)

    # --- filters ---
    f = await _bybit_get_filters(exchange_obj, coin)
    tick_raw = f.get("tickSize")
    qty_step_raw = f.get("qtyStep")
    tick = float(tick_raw) if tick_raw else 0.0
    if tick <= 0:
        logger.error(f"❌ Bybit test: invalid tickSize={tick_raw!r}")
        return False

    qty_str = po._format_by_step(float(qty_test), qty_step_raw)

    # =========================
    # 1) OPEN SHORT (Sell FOK)
    # =========================
    OPEN_TRIES = 3
    OPEN_AGGR_TICKS = 2  # sell limit = best_bid - 2*tick (повышает шанс FOK)
    test_order_ids: list[str] = []

    short_order_id: Optional[str] = None
    filled_s: float = 0.0
    avg_s: Optional[float] = None

    t_open_srv_ms = _srv_ms()

    for attempt in range(1, OPEN_TRIES + 1):
        ob = await exchange_obj.get_orderbook(coin, limit=TEST_OB_LEVELS)
        if not ob or not ob.get("bids"):
            logger.error("❌ Bybit test(A): не удалось получить orderbook (bids) для открытия")
            return False

        bids = ob["bids"][:TEST_OB_LEVELS]
        px_level, cum = po._price_level_for_target_size(bids, float(qty_test))
        if px_level is None:
            logger.error(
                f"❌ Bybit test(A): недостаточно ликвидности в bids (1-{TEST_OB_LEVELS}) "
                f"для qty={_fmt(qty_test)} | available={_fmt(cum)}"
            )
            return False

        # ВАЖНО: marketable-limit от best_bid, чтобы FOK не отменялся из-за микро-сдвига.
        try:
            best_bid = float(bids[0][0])
        except Exception:
            logger.error("❌ Bybit test(A): не смогли прочитать best_bid из стакана")
            return False

        px_open = best_bid - float(OPEN_AGGR_TICKS) * tick
        px_open = po._floor_to_step(px_open, tick)
        if px_open <= 0:
            logger.error("❌ Bybit test(A): computed px_open <= 0")
            return False
        px_open_str = po._format_by_step(px_open, tick_raw)

        logger.info(
            f"🧪 Тест(A): open attempt {attempt}/{OPEN_TRIES} | Sell FOK | qty={qty_str} "
            f"| best_bid={_fmt(best_bid)} | лимит={px_open_str}"
        )

        try:
            try:
                short_order_id = await _bybit_place_limit(
                    exchange_obj=exchange_obj,
                    api_key=api_key,
                    api_secret=api_secret,
                    coin=coin,
                    side="Sell",
                    qty_str=qty_str,
                    price_str=px_open_str,
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
                    price_str=px_open_str,
                    tif="FOK",
                    reduce_only=None,
                    position_idx=None,
                )
        except Exception as e:
            logger.warning(f"⚠️ Bybit test(A): open create failed on attempt {attempt}: {e}")
            await asyncio.sleep(0)
            continue

        test_order_ids.append(str(short_order_id))

        st_s, filled_s, avg_s = await _bybit_get_order_status(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            coin=coin,
            order_id=str(short_order_id),
        )

        if filled_s + 1e-12 >= float(qty_test):
            logger.info(
                f"✅ Тест(A): Short открыт | filled={_fmt(filled_s)} {coin} | avg_entry={_fmt(avg_s)}"
            )
            break

        logger.error(
            f"❌ Bybit test(A): Short не исполнился полностью | status={st_s} | filled={_fmt(filled_s)} "
            f"| attempt={attempt}/{OPEN_TRIES}"
        )
        short_order_id = None
        await asyncio.sleep(0)

    if short_order_id is None:
        return False

    # ==================================
    # 2) CLOSE SHORT immediately (Buy IOC)
    # ==================================
    logger.info("🧪 Тест(A): закрываем Short сразу (Buy IOC)")

    remaining = float(qty_test)
    close_attempts = 6
    t_close_start_srv_ms = _srv_ms()

    for attempt in range(1, close_attempts + 1):
        ob2 = await exchange_obj.get_orderbook(coin, limit=5)
        if not ob2 or not ob2.get("asks"):
            logger.warning(f"⚠️ Bybit test(A): нет asks в стакане на попытке close #{attempt}")
            await asyncio.sleep(0)
            continue

        try:
            best_ask = float(ob2["asks"][0][0])
        except Exception:
            logger.warning(f"⚠️ Bybit test(A): не смогли прочитать best_ask на close #{attempt}")
            await asyncio.sleep(0)
            continue

        # Buy IOC: marketable-limit => ставим >= best_ask (+2 тика для устойчивости)
        px_close = best_ask + 2.0 * tick
        px_close = po._ceil_to_step(px_close, tick)
        px_close_str = po._format_by_step(px_close, tick_raw)

        qty_close_str = po._format_by_step(remaining, qty_step_raw)

        try:
            try:
                close_order_id = await _bybit_place_limit(
                    exchange_obj=exchange_obj,
                    api_key=api_key,
                    api_secret=api_secret,
                    coin=coin,
                    side="Buy",
                    qty_str=qty_close_str,
                    price_str=px_close_str,
                    tif="IOC",
                    reduce_only=True,
                    position_idx=2,
                )
            except Exception:
                close_order_id = await _bybit_place_limit(
                    exchange_obj=exchange_obj,
                    api_key=api_key,
                    api_secret=api_secret,
                    coin=coin,
                    side="Buy",
                    qty_str=qty_close_str,
                    price_str=px_close_str,
                    tif="IOC",
                    reduce_only=True,
                    position_idx=None,
                )
        except Exception as e:
            logger.warning(f"⚠️ Bybit test(A): close IOC create failed (attempt #{attempt}): {e}")
            await asyncio.sleep(0)
            continue

        test_order_ids.append(str(close_order_id))

        st_c, filled_c, avg_c = await _bybit_get_order_status(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            coin=coin,
            order_id=str(close_order_id),
        )

        if filled_c > 0:
            remaining = max(0.0, remaining - float(filled_c))

        if remaining <= 1e-12:
            logger.info(
                f"✅ Тест(A): Short закрыт | qty={_fmt(qty_test)} | avg_exit_buy={_fmt(avg_c)}"
            )
            break

        logger.warning(
            f"⚠️ Bybit test(A): close IOC не закрыл полностью | attempt={attempt}/{close_attempts} "
            f"| status={st_c} | filled={_fmt(filled_c)} | remaining={_fmt(remaining)}"
        )
        await asyncio.sleep(0)

    if remaining > 1e-12:
        logger.error(f"❌ Bybit test(A): Short НЕ закрыт полностью | remaining={_fmt(remaining)}")
        return False

    # =========================
    # 3) Executions (filtered)
    # =========================
    t_end_srv_ms = _srv_ms()
    start_ms = int(min(t_open_srv_ms, t_close_start_srv_ms) - 2_000)
    end_ms = int(max(t_end_srv_ms, t_close_start_srv_ms) + 2_000)

    execs_all = await _bybit_fetch_executions(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        coin=coin,
        start_ms=start_ms,
        end_ms=end_ms,
        limit=500,
    )

    ids = set(str(x) for x in test_order_ids if x)
    execs: list[Dict[str, Any]] = []
    for it in execs_all:
        if not isinstance(it, dict):
            continue
        oid = it.get("orderId") or it.get("orderID") or it.get("order_id")
        if oid is None:
            continue
        if str(oid) in ids:
            execs.append(it)

    pnl_total, buys_n, sells_n, avg_buy, avg_sell = _bybit_calc_pnl_usdt_from_execs(execs)

    logger.info(
        f"📊 Executions(test A): получили={len(execs)} (из {len(execs_all)}) | orderIds={len(ids)} "
        f"| окно={start_ms}..{end_ms} server-ms"
    )
    logger.info(
        f"📊 Итог (ТЕСТ A): монета={coin} | ср_цена_покупки={_fmt(avg_buy)} | ср_цена_продажи={_fmt(avg_sell)} | "
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

    # --- Log funding context for BOEVOY run ---
    try:
        fr = float(p.funding_pct or 0.0)
    except Exception:
        fr = 0.0
    try:
        notional_pre = float(p.coin_qty) * float(last_px_pre)
    except Exception:
        notional_pre = 0.0
    est_funding_usdt = notional_pre * fr  # sign included
    logger.info(
        f"💸 FUNDING CONTEXT: rate={_fmt(fr*100, 6)}% | qty={_fmt(p.coin_qty)} | "
        f"px_pre={_fmt(last_px_pre)} | notional~{_fmt(notional_pre, 3)} USDT | "
        f"est_funding~{_fmt(est_funding_usdt, 4)} USDT (sign included)"
    )
    if p.next_funding_time_ms:
        logger.info(f"🕒 NEXT FUNDING (server ms): {int(p.next_funding_time_ms)}")

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

    # --- START WS EARLY (public + trade) ---
    symbol = exchange_obj._normalize_symbol(p.coin)
    
    # Public WS
    ws_public = BybitPublicWS(symbol=symbol)
    ws_public_task = asyncio.create_task(ws_public.run())
    
    logger.info("🔌 WS: запускаем public stream (orderbook.1 + publicTrade + tickers)")
    try:
        ready = await ws_public.wait_ready(timeout=15.0)
        if not ready:
            logger.warning("⚠️ Public WS не готов в течение 15 секунд, продолжаем (best-effort)")
        else:
            logger.info("✅ Public WS готов, данные поступают")
    except Exception as e:
        logger.warning(f"⚠️ Ошибка при ожидании готовности Public WS: {e}, продолжаем (best-effort)")
    
    # Trade WS
    ws_trade = None
    if USE_TRADE_WS == 1:
        logger.info("🔌 WS: запускаем trade stream (order.create)")
        try:
            ws_trade = BybitTradeWS(
                api_key=api_key,
                api_secret=api_secret,
                logger=logger,
                referer="arb-bot",
                recv_window_ms=8000,
            )
            await ws_trade.start()
            logger.info("✅ Trade WS готов")
        except Exception as e:
            logger.error(f"❌ Ошибка запуска Trade WS: {e}")
            if ws_trade:
                await ws_trade.stop()
            ws_trade = None
    
    # Private WS (order/execution stream)
    ws_private = None
    try:
        logger.info("🔌 WS: запускаем private stream (order/execution/position)")
        ws_private = BybitPrivateWS(
            api_key=api_key,
            api_secret=api_secret,
        )
        await ws_private.start()
        # Best-effort: дождаться хотя бы одного position update, чтобы position snapshot был 0-REST
        try:
            await ws_private.wait_any_position(timeout=3.0)
        except Exception:
            pass
        logger.info("✅ Private WS готов (order/execution/position stream)")
    except Exception as e:
        logger.error(f"❌ Ошибка запуска Private WS: {e}")
        if ws_private:
            await ws_private.stop()
        ws_private = None

    # --- Determine positionIdx once (avoid 10001 at payout) ---
    position_idx = await _bybit_detect_position_idx(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        symbol=symbol,
    )
    logger.info(f"🧭 Bybit position mode: using positionIdx={position_idx} (0=one-way, 2=hedge-short)")

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

    # --- FIX REFERENCE PRICE FROM WS (very close to payout) ---
    fix_server_ms = int(payout_server_ms - max(0, WS_FIX_LEAD_MS))
    await _sleep_until_server_ms(fix_server_ms, offset_ms)
    
    snap_fix = ws_public.snapshot()
    
    # Выбираем close_price на основе свежести данных
    trade_age = snap_fix.get("trade_age_ms", 1e9)
    ticker_age = snap_fix.get("ticker_age_ms", 1e9)
    
    close_price = None
    if trade_age <= OPEN_MAX_STALENESS_MS:
        close_price = snap_fix.get("last_trade")
    elif ticker_age <= OPEN_MAX_STALENESS_MS:
        close_price = snap_fix.get("last_price")
    
    best_bid_fix = snap_fix.get("best_bid")
    best_ask_fix = snap_fix.get("best_ask")
    
    if not close_price or close_price <= 0:
        logger.error(
            f"❌ WS FIX: close_price is not available | "
            f"trade_age={trade_age:.0f}ms ticker_age={ticker_age:.0f}ms "
            f"(max={OPEN_MAX_STALENESS_MS}ms)"
        )
        try:
            if ws_trade:
                await ws_trade.stop()
        except Exception:
            pass
        try:
            if ws_private:
                await ws_private.stop()
        except Exception:
            pass
        try:
            await ws_public.stop()
        except Exception:
            pass
        return 0
    
    if not best_bid_fix or best_bid_fix <= 0:
        logger.error("❌ WS FIX: best_bid not available")
        try:
            if ws_trade:
                await ws_trade.stop()
        except Exception:
            pass
        try:
            if ws_private:
                await ws_private.stop()
        except Exception:
            pass
        try:
            await ws_public.stop()
        except Exception:
            pass
        return 0
    
    logger.info(
        f"📌 WS FIX: close_price={_fmt(close_price)} best_bid={_fmt(best_bid_fix)} best_ask={_fmt(best_ask_fix)} "
        f"staleness_ms={snap_fix.get('staleness_ms')}"
    )
    logger.info(f"💸 FUNDING (fixed pre-payout): rate={_fmt((p.funding_pct or 0.0)*100, 6)}%")

    # Plan times
    open_server_ms = int(payout_server_ms + max(0, int(OPEN_AFTER_MS)))
    close_server_ms = int(payout_server_ms + int(max(0.0, float(FAST_CLOSE_DELAY_SEC)) * 1000))
    close_local_ms = int(close_server_ms - offset_ms)

    # --- PREBUILD OPEN ORDER BEFORE PAYOUT ---
    # Важно: мы НЕ используем best_bid_now после payout как условие входа.
    # Мы пытаемся попасть в matching как можно раньше.
    
    tick = float(tick_raw) if tick_raw else 0.0
    if tick <= 0:
        logger.error("❌ tickSize is invalid; cannot compute IOC price safely")
        try:
            if ws_trade:
                await ws_trade.stop()
        except Exception:
            pass
        try:
            if ws_private:
                await ws_private.stop()
        except Exception:
            pass
        try:
            await ws_public.stop()
        except Exception:
            pass
        return 0
    
    qty_str = po._format_by_step(p.coin_qty, qty_step_raw)
    
    # Лимит "около close_price":
    # Sell Limit исполняется, если bid >= limit.
    # Чтобы попытаться заполниться максимально близко к close_price (и не "догонять"),
    # ставим limit чуть ниже close_price на OPEN_SAFETY_TICKS.
    limit_px = float(close_price) - float(OPEN_SAFETY_TICKS) * tick
    limit_px = po._floor_to_step(limit_px, tick)
    
    if limit_px <= 0:
        logger.error("❌ computed limit_px <= 0; abort")
        try:
            if ws_trade:
                await ws_trade.stop()
        except Exception:
            pass
        try:
            if ws_private:
                await ws_private.stop()
        except Exception:
            pass
        try:
            await ws_public.stop()
        except Exception:
            pass
        return 0
    
    px_str = po._format_by_step(limit_px, tick_raw)
    
    # Сборка order заранее (до payout) — на границе менять только timestamp в header внутри create_order()
    order = {
        "category": "linear",
        "symbol": symbol,
        "side": "Sell",
        "orderType": "Limit",
        "qty": qty_str,
        "price": px_str,
        "timeInForce": "IOC",
        "positionIdx": int(position_idx),
    }
    
    logger.info(
        f"🧷 OPEN PREPARED: close_price={_fmt(close_price)} "
        f"limit_px={px_str} qty={qty_str} (safety_ticks={OPEN_SAFETY_TICKS})"
    )
    
    # Snapshot existing short BEFORE open (если у тебя уже переведено на WS — ок)
    short_before = await _bybit_get_short_qty_snapshot(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        coin=p.coin,
        ws_private=ws_private,
        symbol=symbol,
        position_idx=int(position_idx),
    )
    logger.info(f"📍 Позиция до входа: short_before={_fmt(short_before)} {p.coin}")
    
    # --- OPEN AT PAYOUT (minimal work) ---
    await _sleep_until_server_ms(open_server_ms, offset_ms)
    
    # Минимальный sanity-check: WS не должен быть "мертвым".
    # Это НЕ проверка рынка, а только проверка "канал живой".
    snap_open = ws_public.snapshot()
    st_ms = float(snap_open.get("staleness_ms", 1e9))
    if st_ms > float(OPEN_MAX_STALENESS_MS):
        logger.warning(f"⛔ ABORT OPEN: WS stale {st_ms:.0f}ms > {OPEN_MAX_STALENESS_MS}ms")
        try:
            if ws_trade:
                await ws_trade.stop()
        except Exception:
            pass
        try:
            if ws_private:
                await ws_private.stop()
        except Exception:
            pass
        try:
            await ws_public.stop()
        except Exception:
            pass
        return 0
    
    logger.info(
        f"🚀 OPEN SEND (Sell IOC): close_price={_fmt(close_price)} "
        f"limit_px={px_str} qty={qty_str} staleness_ms={st_ms:.0f} "
        f"| funding={_fmt((p.funding_pct or 0.0)*100, 6)}%"
    )
    
    # Отправляем ордер через Trade WS (fast path)
    order_id = None
    if USE_TRADE_WS == 1 and ws_trade is not None:
        try:
            # Важно: timestamp считаем прямо в момент отправки
            server_ts_ms = int(time.time() * 1000) + int(offset_ms)
            ack = await ws_trade.create_order(order=order, server_ts_ms=server_ts_ms, timeout_sec=1.0)
            result = ack.get("result", {})
            if isinstance(result, dict):
                order_id = result.get("orderId")
            elif isinstance(result, str):
                order_id = result
            if not order_id:
                data = ack.get("data", {})
                if isinstance(data, dict):
                    order_id = data.get("orderId")
            logger.info(f"✅ OPEN ACK (trade ws): order_id={order_id}")
        except Exception as e:
            logger.error(f"❌ OPEN FAILED (trade ws): {type(e).__name__}: {e}")
            try:
                if ws_trade:
                    await ws_trade.stop()
            except Exception:
                pass
            try:
                if ws_private:
                    await ws_private.stop()
            except Exception:
                pass
            try:
                await ws_public.stop()
            except Exception:
                pass
            return 0
    else:
        # fallback: REST (если вдруг выключишь FUN_USE_TRADE_WS)
        try:
            order_id = await _bybit_place_limit(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                coin=p.coin,
                side="Sell",
                qty_str=qty_str,
                price_str=px_str,
                tif="IOC",
                reduce_only=None,
                position_idx=int(position_idx),
            )
            logger.info(f"✅ OPEN (rest): order_id={order_id}")
        except Exception as e:
            logger.error(f"❌ OPEN FAILED (rest): {type(e).__name__}: {e}")
            try:
                if ws_trade:
                    await ws_trade.stop()
            except Exception:
                pass
            try:
                if ws_private:
                    await ws_private.stop()
            except Exception:
                pass
            try:
                await ws_public.stop()
            except Exception:
                pass
            return 0
    
    # Ждём финальный статус через Private WS (без REST polling)
    if order_id and ws_private and ws_private.ready:
        try:
            final = await ws_private.wait_final(str(order_id), timeout=1.5)
            qty_expected = float(p.coin_qty)
            if final.status.lower() == "filled" and final.filled_qty >= 0.999 * qty_expected:
                logger.info(
                    f"✅ OPEN FILLED (private ws): order_id={order_id} "
                    f"filled_qty={_fmt(final.filled_qty)} avg_price={_fmt(final.avg_price)}"
                )
            else:
                # Это нормальный исход для твоей стратегии: не успели до дампа — не заполнились.
                logger.warning(
                    f"⚠️ OPEN NOT FILLED (private ws): order_id={order_id} "
                    f"status={final.status} filled_qty={_fmt(final.filled_qty)} expected={_fmt(qty_expected)}"
                )
        except asyncio.TimeoutError:
            logger.warning(f"⚠️ OPEN TIMEOUT (private ws): order_id={order_id} - не получили финальный статус за 1.5s")
        except Exception as e:
            logger.warning(f"⚠️ OPEN WAIT ERROR (private ws): order_id={order_id} error={type(e).__name__}: {e}")

    # Wait until close start time
    await _sleep_until_server_ms(close_server_ms, offset_ms)

    # Compute opened delta (check position at close-start, not immediately after open)
    short_after_final = await _bybit_get_short_qty_snapshot(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        coin=p.coin,
        ws_private=ws_private,
        symbol=symbol,
        position_idx=int(position_idx),
    )
    opened_qty = max(0.0, float(short_after_final) - float(short_before))

    logger.info(f"📍 Позиция после входа: short_after={_fmt(short_after_final)} | opened_delta={_fmt(opened_qty)} {p.coin}")

    if opened_qty <= 0:
        logger.warning("⚠️ Новый Short не открылся (opened_delta=0). Ничего не закрываем. Завершаем.")
        try:
            if ws_trade:
                await ws_trade.stop()
        except Exception:
            pass
        try:
            if ws_private:
                await ws_private.stop()
        except Exception:
            pass
        try:
            await ws_public.stop()
        except Exception:
            pass
        return 0

    ok_close, avg_exit_short = await po._bybit_close_leg_partial_ioc(
        exchange_obj=exchange_obj,
        coin=p.coin,
        position_direction="short",
        coin_amount=float(opened_qty),
        position_idx=int(position_idx),
        ws_public=ws_public,
        ws_trade=ws_trade,
        ws_private=ws_private,
        tick_raw=tick_raw,
        qty_step_raw=qty_step_raw,
        offset_ms=offset_ms,
    )
    if not ok_close:
        ok_close, avg_exit_short = await po._bybit_close_leg_partial_ioc(
            exchange_obj=exchange_obj,
            coin=p.coin,
            position_direction="short",
            coin_amount=float(opened_qty),
            position_idx=None,
            ws_public=ws_public,
            ws_trade=ws_trade,
            ws_private=ws_private,
            tick_raw=tick_raw,
            qty_step_raw=qty_step_raw,
            offset_ms=offset_ms,
        )

    if ok_close:
        logger.info(f"✅ Short закрыт | qty={_fmt(opened_qty)} | avg_exit_buy={_fmt(avg_exit_short)}")
    else:
        logger.error(f"❌ Short НЕ закрыт полностью (best-effort) | qty={_fmt(opened_qty)} | avg_exit_buy={_fmt(avg_exit_short)}")

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
    
    # Остановка WebSocket
    try:
        if ws_trade:
            await ws_trade.stop()
    except Exception as e:
        logger.warning(f"⚠️ Ошибка при остановке Trade WS: {e}")
    
    try:
        if ws_private:
            await ws_private.stop()
    except Exception as e:
        logger.warning(f"⚠️ Ошибка при остановке Private WS: {e}")
    
    try:
        await ws_public.stop()
        logger.debug("🔌 Public WebSocket остановлен")
    except Exception as e:
        logger.warning(f"⚠️ Ошибка при остановке Public WS: {e}")
    
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
