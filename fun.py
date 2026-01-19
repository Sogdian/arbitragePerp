import asyncio
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

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
NEWS_DAYS_BACK = int(os.getenv("FUN_NEWS_DAYS_BACK", "60"))
BALANCE_BUFFER_USDT = float(os.getenv("FUN_BALANCE_BUFFER_USDT", "0"))


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
    funding_pct: float  # decimal, e.g. -0.02
    payout_hh: int
    payout_mm: int
    offset_pct: float  # decimal, e.g. -0.003


def parse_cmd(cmd: str) -> FunParams:
    parts = (cmd or "").strip().split()
    if len(parts) != 6:
        raise ValueError(
            "bad command format. Expected: COIN EXCHANGE QTY FUNDING% HH:MM OFFSET% "
            'e.g. "STO Bybit 30 -2% 12:00 -0.3%"'
        )
    coin = parts[0].strip().upper()
    exchange = parts[1].strip().lower()
    coin_qty = float(parts[2])
    if coin_qty <= 0:
        raise ValueError(f"coin qty must be > 0, got: {coin_qty}")
    funding_pct = _parse_pct(parts[3])
    offset_pct = _parse_pct(parts[5])
    hh, mm = _parse_time_hhmm(parts[4])
    # по ТЗ проценты отрицательные (но валидируем мягко)
    return FunParams(
        coin=coin,
        exchange=exchange,
        coin_qty=float(coin_qty),
        funding_pct=float(funding_pct),
        payout_hh=hh,
        payout_mm=mm,
        offset_pct=float(offset_pct),
    )


def _local_dt_today(hh: int, mm: int) -> datetime:
    now = datetime.now()
    return now.replace(hour=hh, minute=mm, second=0, microsecond=0)


async def _sleep_until(dt_local: datetime) -> None:
    while True:
        now = datetime.now()
        sec = (dt_local - now).total_seconds()
        if sec <= 0:
            return
        await asyncio.sleep(min(0.2, sec))


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
) -> Tuple[Optional[str], float]:
    """
    Returns (status, cumExecQty).
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
        return None, 0.0
    status = str(item.get("orderStatus") or "")
    try:
        filled = float(item.get("cumExecQty") or 0.0)
    except Exception:
        filled = 0.0
    return status, filled


async def _bybit_wait_done_get_filled_qty(
    *,
    exchange_obj: Any,
    api_key: str,
    api_secret: str,
    coin: str,
    order_id: str,
    timeout_sec: float = 6.0,
) -> Tuple[Optional[str], float]:
    """
    Wait until order becomes terminal (Filled/Cancelled/Rejected/Expired) and return (final_status, filled_qty).
    For IOC/FOK orders it should resolve quickly.
    """
    deadline = time.time() + max(0.5, float(timeout_sec))
    last_status: Optional[str] = None
    last_filled: float = 0.0
    while time.time() < deadline:
        st, filled = await _bybit_get_order_status(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            coin=coin,
            order_id=order_id,
        )
        if st:
            last_status = st
            last_filled = filled
            if st.lower() in ("filled", "cancelled", "canceled", "rejected", "expired"):
                return st, filled
        await asyncio.sleep(0.12)
    return last_status, last_filled


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


async def _check_news_one_exchange(bot: PerpArbitrageBot, exchange: str, coin: str) -> Tuple[bool, str]:
    try:
        now_utc = datetime.now(timezone.utc)
        lookback = now_utc - timedelta(days=NEWS_DAYS_BACK, hours=6)
        anns = await bot.news_monitor._fetch_exchange_announcements(limit=200, days_back=NEWS_DAYS_BACK, exchanges=[exchange])
        delisting = await bot.news_monitor.find_delisting_news(anns, coin_symbol=coin, lookback=lookback)
        if delisting:
            return False, f"найдены новости о делистинге ({len(delisting)} шт.)"
        security = await bot.announcements_monitor.find_security_news(anns, coin_symbol=coin, lookback=lookback)
        if security:
            return False, f"найдены security-новости ({len(security)} шт.)"
        return True, "ok"
    except Exception as e:
        # conservative: treat failures as NOT ok
        return False, f"ошибка проверки новостей: {type(e).__name__}: {e}"


async def _bybit_test_orders(bot: PerpArbitrageBot, coin: str, qty_test: float) -> bool:
    exchange_obj = bot.exchanges["bybit"]
    api_key = po._get_env("BYBIT_API_KEY") or ""
    api_secret = po._get_env("BYBIT_API_SECRET") or ""
    if not api_key or not api_secret:
        logger.error("❌ Нет BYBIT_API_KEY/BYBIT_API_SECRET в .env")
        return False

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

    ok_full, filled = await po._bybit_wait_full_fill(
        planned={"exchange_obj": exchange_obj, "api_key": api_key, "api_secret": api_secret, "symbol": exchange_obj._normalize_symbol(coin), "qty": qty_str},
        order_id=str(short_order_id),
    )
    if not ok_full:
        logger.error(f"❌ Bybit test: Short не исполнился полностью | filled={_fmt(filled)}")
        return False
    logger.info(f"✅ Тест: Short открыт | order={short_order_id} | filled={_fmt(filled)} {coin}")

    # 2) Open test Long (Buy) after short is opened, at best ask (FOK) for the same qty
    asks = ob["asks"][:TEST_OB_LEVELS]
    px_level_a, _cum_a = po._price_level_for_target_size(asks, qty_test)
    if px_level_a is None:
        logger.error(f"❌ Bybit test: недостаточно ликвидности в asks (1-{TEST_OB_LEVELS}) для qty={_fmt(qty_test)}")
        # close short best-effort
        await po._bybit_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=coin, position_direction="short", coin_amount=qty_test)
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
        await po._bybit_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=coin, position_direction="short", coin_amount=qty_test)
        return False

    ok_full_l, filled_l = await po._bybit_wait_full_fill(
        planned={"exchange_obj": exchange_obj, "api_key": api_key, "api_secret": api_secret, "symbol": exchange_obj._normalize_symbol(coin), "qty": qty_str},
        order_id=str(long_order_id),
    )
    if not ok_full_l:
        logger.error(f"❌ Bybit test: Long не исполнился полностью | filled={_fmt(filled_l)}")
        # close both best-effort
        await po._bybit_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=coin, position_direction="short", coin_amount=qty_test)
        await po._bybit_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=coin, position_direction="long", coin_amount=qty_test)
        return False
    logger.info(f"✅ Тест: Long открыт | order={long_order_id} | filled={_fmt(filled_l)} {coin}")

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
    return True


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

    # News check
    news_ok, news_msg = await _check_news_one_exchange(bot, "bybit", p.coin)
    if not news_ok:
        logger.error(f"❌ Новости/делистинг: {news_msg}")
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

    logger.info(
        f"✅ Подготовка OK | exchange=bybit | coin={p.coin} | qty={_fmt(p.coin_qty)} | "
        f"funding={_fmt(p.funding_pct*100, 3)}% | offset={_fmt(p.offset_pct*100, 3)}% | "
        f"min_test_qty~{_fmt(min_test_qty)} | usdt_avail={_fmt(avail_usdt, 3)} | usdt_need~{_fmt(required_usdt, 3)}"
    )

    # Ask about test orders
    ans = input("Совершить тестовые открытия шорт и лонг ? (Да/Нет): ").strip()
    if _is_yes(ans):
        logger.info("🧪 Тестовые ордера: запуск")
        ok_test = await _bybit_test_orders(bot, p.coin, min_test_qty)
        if not ok_test:
            logger.error("❌ Тестовые ордера не прошли — останавливаемся")
            return 2
        logger.info("✅ Тестовые ордера прошли успешно")
    else:
        logger.info("⏭️ Тестовые ордера пропущены")

    # Ask about real positions
    ans2 = input("Открывать позиции лонг и шорт? (Да/Нет): ").strip()
    if not _is_yes(ans2):
        logger.info("Отклонено пользователем. Завершаем.")
        return 0

    payout_dt = _local_dt_today(p.payout_hh, p.payout_mm)
    now = datetime.now()
    if now > payout_dt:
        logger.error(f"❌ Скрипт запущен после времени выплаты: now={now.strftime('%H:%M:%S')} > payout={payout_dt.strftime('%H:%M:%S')}")
        return 2

    fix_dt = payout_dt - timedelta(seconds=1)
    logger.info(f"⏳ Ждём фиксации цены: {fix_dt.strftime('%H:%M:%S')} (локальное время)")
    await _sleep_until(fix_dt)

    t_fix = await exchange_obj.get_futures_ticker(p.coin)
    close_price = _ticker_last_price(t_fix)
    if close_price is None or close_price <= 0:
        logger.error("❌ Не удалось зафиксировать цену (last price) в 11:59:59")
        return 2

    fair_price = close_price * (1.0 + p.funding_pct)
    long_target_price = close_price * (1.0 + p.funding_pct + p.offset_pct)
    logger.info(
        f"📌 Фиксация: close_price={_fmt(close_price)} | fair={_fmt(fair_price)} | long_target={_fmt(long_target_price)} "
        f"(funding={_fmt(p.funding_pct*100,3)}%, offset={_fmt(p.offset_pct*100,3)}%)"
    )

    logger.info(f"⏳ Ждём времени выплаты: {payout_dt.strftime('%H:%M:%S')} (локальное время)")
    await _sleep_until(payout_dt)

    # At payout: check last price is still above target (per TЗ)
    t_now = await exchange_obj.get_futures_ticker(p.coin)
    last_now = _ticker_last_price(t_now)
    if last_now is None or last_now <= 0:
        logger.error("❌ Не удалось получить Last price в момент открытия")
        return 2
    if last_now <= long_target_price:
        logger.warning(
            f"⚠️ Last price уже ниже/равен уровню long_target — не открываем сделку | last={_fmt(last_now)} <= target={_fmt(long_target_price)}"
        )
        return 0

    # Prepare trading settings (isolated + leverage 1)
    ok_prep = await po._prepare_exchange_for_trading(exchange_name="bybit", exchange_obj=exchange_obj, coin=p.coin)
    if not ok_prep:
        logger.error("❌ Не удалось подготовить торговые параметры (isolated/leverage=1)")
        return 2

    # Refresh orderbook and compute short limit price for full fill (within first 15 levels)
    ob2 = await exchange_obj.get_orderbook(p.coin, limit=MAIN_OB_LEVELS)
    if not ob2 or not ob2.get("bids") or not ob2.get("asks"):
        logger.error("❌ Bybit: orderbook недоступен для открытия")
        return 2

    f = await _bybit_get_filters(exchange_obj, p.coin)
    tick_raw = f.get("tickSize")
    qty_step_raw = f.get("qtyStep")
    tick = float(tick_raw) if tick_raw else 0.0
    qty_str = po._format_by_step(p.coin_qty, qty_step_raw)

    bids2 = ob2["bids"][:MAIN_OB_LEVELS]
    px_short_level2, _ = po._price_level_for_target_size(bids2, p.coin_qty)
    if px_short_level2 is None:
        logger.error(f"❌ Недостаточно ликвидности в bids (1-{MAIN_OB_LEVELS}) для открытия Short qty={_fmt(p.coin_qty)}")
        return 2

    # Sell price: to allow match across levels, use that level price (rounded down to tick)
    px_short = po._round_price_for_side(float(px_short_level2), tick, "sell")
    px_short_str = po._format_by_step(px_short, tick_raw)

    # 12:00:00 — открытие: Short (FOK), Long (partial allowed, up to 15 attempts), StopLoss for Short
    logger.info(f"📤 Открываем Short (Sell FOK) | qty={qty_str} | limit={px_short_str}")
    try:
        try:
            short_order_id = await _bybit_place_limit(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                coin=p.coin,
                side="Sell",
                qty_str=qty_str,
                price_str=px_short_str,
                tif="FOK",
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
                price_str=px_short_str,
                tif="FOK",
                reduce_only=None,
                position_idx=None,
            )
    except Exception as e:
        logger.error(f"❌ Не удалось создать Short ордер: {e}")
        return 2

    st_s, filled_s = await _bybit_wait_done_get_filled_qty(
        exchange_obj=exchange_obj,
        api_key=api_key,
        api_secret=api_secret,
        coin=p.coin,
        order_id=str(short_order_id),
        timeout_sec=8.0,
    )
    if not filled_s or filled_s <= 0:
        logger.error(f"❌ Short не открылся (FOK) | status={st_s} | filled={_fmt(filled_s)}")
        return 2
    if filled_s + 1e-12 < float(qty_str):
        logger.warning(f"⚠️ Short исполнился частично (не ожидалось для FOK) | filled={_fmt(filled_s)} | req={qty_str}")

    qty_trade = float(filled_s)
    qty_trade_str = po._format_by_step(qty_trade, qty_step_raw)
    logger.info(f"✅ Short открыт | order={short_order_id} | qty={qty_trade_str} | entry~{px_short_str}")

    # Stop loss for short at entry price (best bid used for short)
    sl_order_id: Optional[str] = None
    try:
        sl_trigger_str = po._format_by_step(float(px_short), tick_raw)
        logger.info(f"🛡️ Выставляем Stop Loss для Short (stop-market Buy) | trigger={sl_trigger_str}")
        try:
            sl_order_id = await _bybit_place_stop_market(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                coin=p.coin,
                side="Buy",
                qty_str=qty_trade_str,
                trigger_price_str=sl_trigger_str,
                position_idx=2,
            )
        except Exception:
            sl_order_id = await _bybit_place_stop_market(
                exchange_obj=exchange_obj,
                api_key=api_key,
                api_secret=api_secret,
                coin=p.coin,
                side="Buy",
                qty_str=qty_trade_str,
                trigger_price_str=sl_trigger_str,
                position_idx=None,
            )
    except Exception as e:
        logger.error(f"❌ Не удалось выставить Stop Loss: {e}")
        # safety: close short immediately
        await po._bybit_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=p.coin, position_direction="short", coin_amount=qty_trade, position_idx=2)
        return 2

    # Long open attempts: buy limit at long_target_price, partial allowed (IOC), up to 15 attempts
    long_px = float(long_target_price)
    if tick > 0:
        long_px = _floor_to_step(long_px, tick)
    long_px_str = po._format_by_step(long_px, tick_raw)

    remaining = float(qty_trade)
    filled_long_total = 0.0
    long_attempts = max(1, MAIN_OB_LEVELS)

    for attempt in range(1, long_attempts + 1):
        if remaining <= max(1e-10, qty_trade * 1e-8):
            break

        ob_l = await exchange_obj.get_orderbook(p.coin, limit=MAIN_OB_LEVELS)
        if not ob_l or not ob_l.get("asks"):
            logger.error("❌ Bybit: orderbook недоступен для Long попытки")
            break
        best_ask = float(ob_l["asks"][0][0])
        if best_ask > long_px + 1e-12:
            logger.info(f"Long попытка {attempt}/{long_attempts}: лучшая ask={_fmt(best_ask)} выше long_price={long_px_str} -> ждём")
            await asyncio.sleep(POLL_SEC)
            continue

        qty_rem_str = po._format_by_step(remaining, qty_step_raw)
        logger.info(f"📥 Long попытка {attempt}/{long_attempts} (Buy IOC) | qty={qty_rem_str} | limit={long_px_str}")
        try:
            try:
                long_order_id = await _bybit_place_limit(
                    exchange_obj=exchange_obj,
                    api_key=api_key,
                    api_secret=api_secret,
                    coin=p.coin,
                    side="Buy",
                    qty_str=qty_rem_str,
                    price_str=long_px_str,
                    tif="IOC",
                    reduce_only=None,
                    position_idx=1,
                )
            except Exception:
                long_order_id = await _bybit_place_limit(
                    exchange_obj=exchange_obj,
                    api_key=api_key,
                    api_secret=api_secret,
                    coin=p.coin,
                    side="Buy",
                    qty_str=qty_rem_str,
                    price_str=long_px_str,
                    tif="IOC",
                    reduce_only=None,
                    position_idx=None,
                )
        except Exception as e:
            logger.error(f"❌ Long попытка {attempt}: ошибка создания ордера: {e}")
            break

        st_l, filled_l = await _bybit_wait_done_get_filled_qty(
            exchange_obj=exchange_obj,
            api_key=api_key,
            api_secret=api_secret,
            coin=p.coin,
            order_id=str(long_order_id),
            timeout_sec=8.0,
        )
        if filled_l and filled_l > 0:
            filled_long_total += float(filled_l)
            remaining = max(0.0, remaining - float(filled_l))
            logger.info(f"Long попытка {attempt}: исполнено={_fmt(filled_l)} | всего_long={_fmt(filled_long_total)} | осталось={_fmt(remaining)} | status={st_l}")
        else:
            logger.info(f"Long попытка {attempt}: 0 исполнено | status={st_l}")

        # без лишних пауз, но учитываем rate limits
        await asyncio.sleep(0.05)

    if filled_long_total + 1e-10 < float(qty_trade):
        logger.warning(f"⚠️ Long не исполнился полностью за {long_attempts} попыток | filled_long={_fmt(filled_long_total)} | need={_fmt(qty_trade)}")
        # cancel stop-loss
        if sl_order_id:
            await _bybit_cancel_order(exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret, coin=p.coin, order_id=sl_order_id)

        # If we opened some long, close it; always close short as per spec.
        if filled_long_total > 0:
            await po._bybit_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=p.coin, position_direction="long", coin_amount=filled_long_total, position_idx=1)
        await po._bybit_close_leg_partial_ioc(exchange_obj=exchange_obj, coin=p.coin, position_direction="short", coin_amount=qty_trade, position_idx=2)
        return 0

    logger.info(f"✅ Long исполнен полностью | qty={qty_trade_str} | limit={long_px_str} -> закрываем обе позиции")

    # cancel stop-loss before closing
    if sl_order_id:
        await _bybit_cancel_order(exchange_obj=exchange_obj, api_key=api_key, api_secret=api_secret, coin=p.coin, order_id=sl_order_id)

    # Close both simultaneously (limit IOC reduceOnly, partial fills allowed)
    close_short_task = po._bybit_close_leg_partial_ioc(
        exchange_obj=exchange_obj,
        coin=p.coin,
        position_direction="short",
        coin_amount=qty_trade,
        position_idx=2,
    )
    close_long_task = po._bybit_close_leg_partial_ioc(
        exchange_obj=exchange_obj,
        coin=p.coin,
        position_direction="long",
        coin_amount=qty_trade,
        position_idx=1,
    )
    ok_s, avg_s = await close_short_task
    ok_l, avg_l = await close_long_task
    if not (ok_s and ok_l):
        logger.error(f"❌ Не удалось закрыть обе позиции | short_ok={ok_s} long_ok={ok_l}")
        return 2
    logger.info(f"✅ Позиции закрыты | exit_short_buy_vwap={_fmt(avg_s)} | exit_long_sell_vwap={_fmt(avg_l)} | qty={qty_trade_str}")
    return 0


async def main() -> int:
    if len(sys.argv) < 2:
        logger.error('Usage: python fun.py "STO Bybit 30 -2% 12:00 -0.3%"')
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


