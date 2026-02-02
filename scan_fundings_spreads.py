"""
Бот scan_fundings_spreads: поиск пар бирж и монет по стратегии фандинг-арбитража.
Условия: спред фандинга (получаем на Long, платим на Short) >= MIN_FUNDING_SPREAD,
спред цен по модулю между биржами <= MAX_PRICE_SPREAD. Лог в консоль, Telegram — как в scan_spreads (картинка + caption).
"""
import asyncio
import io
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from itertools import combinations
from typing import Any, Dict, List, Optional, Set, Tuple

from bot import PerpArbitrageBot
from telegram_sender import TelegramSender
import config

try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False


# ----------------------------
# ENV loader
# ----------------------------
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
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and (k not in os.environ):
                    os.environ[k] = v
    except Exception:
        return


# ----------------------------
# Settings
# ----------------------------
load_dotenv(".env")

MIN_FUNDING_SPREAD = float(os.getenv("MIN_FUNDING_SPREAD", "1.5"))  # спред фандинга >= (Long получаем, Short платим), %
MAX_PRICE_SPREAD = float(os.getenv("MAX_PRICE_SPREAD", "2"))  # |спред цен| <= %, для минимального проскальзывания
SCAN_INTERVAL_SEC = float(os.getenv("SCAN_FUNDING_INTERVAL_SEC", "60"))
MAX_CONCURRENCY = int(os.getenv("SCAN_FUNDING_MAX_CONCURRENCY", "20"))
COIN_BATCH_SIZE = int(os.getenv("SCAN_FUNDING_COIN_BATCH_SIZE", "50"))
REQ_TIMEOUT_SEC = float(os.getenv("SCAN_FUNDING_REQ_TIMEOUT_SEC", "20"))
MEXC_REQ_TIMEOUT_SEC = float(os.getenv("SCAN_FUNDING_MEXC_REQ_TIMEOUT_SEC", "45"))
SCAN_FUNDING_MIN_TIME_TO_PAY = float(os.getenv("SCAN_FUNDING_MIN_TIME_TO_PAY", "60"))  # мин до выплаты: слать в TG только если <
SCAN_COIN_INVEST = float(os.getenv("SCAN_COIN_INVEST", "50"))
NEWS_CACHE_TTL_SEC = float(os.getenv("SCAN_NEWS_CACHE_TTL_SEC", "180"))
ANALYSIS_MAX_CONCURRENCY = int(os.getenv("SCAN_ANALYSIS_MAX_CONCURRENCY", "2"))
EXCLUDE_EXCHANGES = {"lbank"}

EXCLUDE_COINS_STR = os.getenv("EXCLUDE_COINS", "").strip()
EXCLUDE_COINS = {c.strip().upper() for c in EXCLUDE_COINS_STR.split(",") if c.strip()} if EXCLUDE_COINS_STR else set()


# ----------------------------
# Logging (только консоль, без файла)
# ----------------------------
LOG_LEVEL = os.getenv("SCAN_FUNDING_LOG_LEVEL", "INFO").upper()
logger = logging.getLogger("scan_fundings_spreads")
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
logger.propagate = False
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(_handler)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("bot").setLevel(logging.CRITICAL)
logging.getLogger("news_monitor").setLevel(logging.CRITICAL)
logging.getLogger("announcements_monitor").setLevel(logging.CRITICAL)
logging.getLogger("x_news_monitor").setLevel(logging.WARNING)
logging.getLogger("exchanges").setLevel(logging.CRITICAL)


# ----------------------------
# Helpers
# ----------------------------
def is_ignored_coin(coin: str) -> bool:
    return bool(coin) and coin[0].isdigit()


def calculate_minutes_until_funding(next_funding_time: Optional[int], exchange: str) -> Optional[int]:
    if next_funding_time is None:
        return None
    try:
        is_seconds = next_funding_time < 10**12
        ts = float(next_funding_time) if is_seconds else next_funding_time / 1000
        sec = ts - time.time()
        if sec < 0:
            return None
        return int(sec / 60)
    except Exception:
        return None


def calc_open_spread_pct(ask_long: Optional[float], bid_short: Optional[float]) -> Optional[float]:
    if ask_long is None or bid_short is None or ask_long <= 0:
        return None
    return ((bid_short - ask_long) / ask_long) * 100.0


def funding_spread_pct(funding_long: Optional[float], funding_short: Optional[float]) -> Optional[float]:
    """Спред фандинга: получаем на Long (-rate_long), платим на Short (rate_short). Long -2%, Short +0.5% -> 1.5%."""
    if funding_long is None or funding_short is None:
        return None
    return (-funding_long - funding_short) * 100.0


# ----------------------------
# News cache (coin, ex) -> (expires, delisting, security)
# ----------------------------
_news_cache: Dict[Tuple[str, str], Tuple[float, List[Dict[str, Any]], List[Dict[str, Any]]]] = {}


async def _get_news_cached(
    bot: PerpArbitrageBot,
    coin: str,
    long_ex: str,
    short_ex: str,
    days_back: int = 60,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], bool]:
    now_m = time.monotonic()
    now_utc = datetime.now(timezone.utc)
    lookback = now_utc - timedelta(days=days_back, hours=6) if days_back > 0 else None

    def _merge_dedupe(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set()
        out: List[Dict[str, Any]] = []
        for it in items or []:
            url = str(it.get("url") or "").strip()
            key = url or (str(it.get("title") or "").strip()[:200])
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(it)
        return out

    async def _get_exchange_news(ex: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], bool]:
        k = (coin, ex)
        cached = _news_cache.get(k)
        if cached and cached[0] > now_m:
            return cached[1], cached[2], True
        anns = await bot.news_monitor._fetch_exchange_announcements(
            limit=200, days_back=days_back, exchanges=[ex],
        )
        delisting_news = await bot.news_monitor.find_delisting_news(anns, coin_symbol=coin, lookback=lookback)
        if (not delisting_news) and getattr(bot, "x_news_monitor", None) and bot.x_news_monitor.enabled:
            try:
                x_del = await bot.x_news_monitor.find_delisting_news(
                    coin_symbol=coin, exchanges=[ex], lookback=lookback,
                )
                if x_del:
                    delisting_news = _merge_dedupe((delisting_news or []) + x_del)
            except Exception:
                pass
        security_news: List[Dict[str, Any]] = []
        if not delisting_news:
            security_news = await bot.announcements_monitor.find_security_news(
                anns, coin_symbol=coin, lookback=lookback,
            )
            if (not security_news) and getattr(bot, "x_news_monitor", None) and bot.x_news_monitor.enabled:
                try:
                    x_sec = await bot.x_news_monitor.find_security_news(
                        coin_symbol=coin, exchanges=[ex], lookback=lookback,
                    )
                    if x_sec:
                        security_news = _merge_dedupe((security_news or []) + x_sec)
                except Exception:
                    pass
        _news_cache[k] = (now_m + NEWS_CACHE_TTL_SEC, delisting_news, security_news)
        return delisting_news, security_news, False

    del_long, sec_long, c1 = await _get_exchange_news(long_ex)
    del_short, sec_short, c2 = await _get_exchange_news(short_ex)
    delisting_news = _merge_dedupe((del_long or []) + (del_short or []))
    security_news = _merge_dedupe((sec_long or []) + (sec_short or []))
    return delisting_news, security_news, bool(c1 and c2)


async def fetch(
    bot: PerpArbitrageBot,
    ex: str,
    coin: str,
    sem: asyncio.Semaphore,
) -> Optional[Dict[str, Any]]:
    """Тикер + funding_info (funding_rate, next_funding_time)."""
    exchange = bot.exchanges.get(ex)
    if not exchange:
        return None
    timeout_ticker = REQ_TIMEOUT_SEC
    timeout_funding = MEXC_REQ_TIMEOUT_SEC if ex.lower() == "mexc" else REQ_TIMEOUT_SEC
    try:
        async with sem:
            ticker = await asyncio.wait_for(
                exchange.get_futures_ticker(coin), timeout=timeout_ticker,
            )
    except asyncio.TimeoutError:
        logger.debug(f"Timeout: {ex} {coin} ticker")
        return None
    except Exception as e:
        logger.debug(f"Fetch error: {ex} {coin} ticker: {e}")
        return None
    if not ticker:
        return None
    out: Dict[str, Any] = {
        "price": ticker.get("price"),
        "bid": ticker.get("bid"),
        "ask": ticker.get("ask"),
    }
    try:
        if hasattr(exchange, "get_funding_info"):
            async with sem:
                info = await asyncio.wait_for(
                    exchange.get_funding_info(coin), timeout=timeout_funding,
                )
            if info:
                out["funding_rate"] = info.get("funding_rate")
                out["next_funding_time"] = info.get("next_funding_time")
        else:
            async with sem:
                rate = await asyncio.wait_for(
                    exchange.get_funding_rate(coin), timeout=timeout_funding,
                )
            if rate is not None:
                out["funding_rate"] = rate
    except asyncio.TimeoutError:
        logger.debug(f"Timeout: {ex} {coin} funding")
    except Exception:
        logger.debug(f"Fetch error: {ex} {coin} funding", exc_info=True)
    return out


async def _analyze_and_log_opportunity(
    bot: PerpArbitrageBot,
    coin: str,
    long_ex: str,
    short_ex: str,
    open_spread_pct: float,
    funding_spread_val: float,
    analysis_sem: asyncio.Semaphore,
    long_data: Optional[Dict[str, Any]] = None,
    short_data: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Ликвидность + новости, одна строка в лог, вердикт ✅/❌. Funding spread = (-long - short)*100."""
    async with analysis_sem:
        ok = False
        long_liq = None
        short_liq = None
        delisting_news: List[Dict[str, Any]] = []
        security_news: List[Dict[str, Any]] = []
        try:
            long_obj = bot.exchanges.get(long_ex)
            short_obj = bot.exchanges.get(short_ex)
            if long_obj and short_obj:
                long_liq = await long_obj.check_liquidity(
                    coin, notional_usdt=SCAN_COIN_INVEST, ob_limit=50,
                    max_spread_bps=30.0, max_impact_bps=50.0, mode="entry_long",
                )
                short_liq = await short_obj.check_liquidity(
                    coin, notional_usdt=SCAN_COIN_INVEST, ob_limit=50,
                    max_spread_bps=30.0, max_impact_bps=50.0, mode="entry_short",
                )
                liq_ok = bool(
                    long_liq and long_liq.get("ok") is True
                    and short_liq and short_liq.get("ok") is True
                )
            else:
                liq_ok = False
            delisting_news, security_news, _ = await _get_news_cached(
                bot, coin=coin, long_ex=long_ex, short_ex=short_ex, days_back=60,
            )
            news_ok = bool((not delisting_news) and (not security_news))
            ok = bool(liq_ok and news_ok)
        except Exception:
            ok = False

        total_spread = open_spread_pct + funding_spread_val
        verdict = "✅ арбитражить" if ok else "❌ не арбитражить"
        price_long = long_data.get("ask") if long_data else None
        price_short = short_data.get("bid") if short_data else None
        coins_info = ""
        if ok and price_long and price_short and price_long > 0 and price_short > 0:
            coins_long = SCAN_COIN_INVEST / price_long
            coins_short = SCAN_COIN_INVEST / price_short
            coins_info = f" ({long_ex}: {coins_long:.3f} {coin}, {short_ex}: {coins_short:.3f} {coin})"
        reasons_parts: List[str] = []
        if not ok:
            if long_liq and not long_liq.get("ok"):
                r = long_liq.get("reasons", [])
                if r:
                    reasons_parts.append(f"ликв. Long: {'; '.join(r)}")
            if short_liq and not short_liq.get("ok"):
                r = short_liq.get("reasons", [])
                if r:
                    reasons_parts.append(f"ликв. Short: {'; '.join(r)}")
            if delisting_news:
                reasons_parts.append("делистинг")
            if security_news:
                reasons_parts.append("безопасность")
        reasons_str = f" ({'; '.join(reasons_parts)})" if reasons_parts else ""
        log_message = (
            f"{coin} Long ({long_ex}), Short ({short_ex}) "
            f"Спред на цену: {open_spread_pct:.3f}% | Фандинг: {funding_spread_val:.3f}% | "
            f"Спред общий: {total_spread:.3f}% {verdict}{coins_info}{reasons_str}"
        )
        logger.info(log_message)

        if ok:
            minutes_until = None
            if long_data and long_data.get("next_funding_time") is not None:
                minutes_until = calculate_minutes_until_funding(
                    long_data["next_funding_time"], long_ex,
                )
            return {
                "coin": coin,
                "long_ex": long_ex,
                "short_ex": short_ex,
                "open_spread_pct": open_spread_pct,
                "funding_spread_pct": funding_spread_val,
                "long_data": long_data,
                "short_data": short_data,
                "long_liq": long_liq,
                "short_liq": short_liq,
                "minutes_until": minutes_until,
            }
        return None


def _get_exchange_url(exchange: str, coin: str) -> str:
    ex_l = exchange.lower()
    coin_u = coin.upper()
    if ex_l == "bybit":
        return f"https://www.bybit.com/trade/usdt/{coin_u}USDT"
    if ex_l == "gate":
        return f"https://www.gate.com/ru/futures/USDT/{coin_u}_USDT"
    if ex_l == "okx":
        return f"https://www.okx.com/ru/trade-swap/{coin.lower()}-usdt-swap"
    if ex_l == "binance":
        return f"https://www.binance.com/ru/futures/{coin_u}USDT"
    if ex_l == "bitget":
        return f"https://www.bitget.com/ru/futures/{coin_u}USDT"
    if ex_l == "bingx":
        return f"https://bingx.com/ru-ru/perpetual/{coin_u}-USDT"
    if ex_l == "mexc":
        return f"https://www.mexc.com/ru-RU/futures/{coin_u}_USDT"
    if ex_l == "xt":
        return f"https://www.xt.com/ru/trade/{coin_u.lower()}_usdt"
    if ex_l == "lbank":
        return f"https://www.lbank.com/ru/trade/{coin_u.lower()}/"
    return f"https://www.{ex_l}.com"


def _format_combined_telegram_message(
    coin: str,
    opportunities: List[Dict[str, Any]],
) -> str:
    """Текст для Telegram: как в scan_spreads, funding spread = (-long - short)*100."""
    lines = [f'🔔 💰<b>Спред фандингов: монета {coin}</b> (для ликв.: {SCAN_COIN_INVEST:.1f} USDT)']
    lines.append("")
    for opp in opportunities:
        long_ex = opp["long_ex"]
        short_ex = opp["short_ex"]
        long_data = opp.get("long_data")
        short_data = opp.get("short_data")
        open_spread_pct = opp["open_spread_pct"]
        funding_spread_val = opp.get("funding_spread_pct")
        price_long = None
        funding_long = None
        if long_data:
            price_long = long_data.get("price") or (long_data.get("bid") and long_data.get("ask") and (long_data["bid"] + long_data["ask"]) / 2.0)
            funding_long = long_data.get("funding_rate")
        price_short = None
        funding_short = None
        if short_data:
            price_short = short_data.get("price") or (short_data.get("bid") and short_data.get("ask") and (short_data["bid"] + short_data["ask"]) / 2.0)
            funding_short = short_data.get("funding_rate")
        long_url = _get_exchange_url(long_ex, coin)
        short_url = _get_exchange_url(short_ex, coin)
        long_cap = long_ex.capitalize()
        short_cap = short_ex.capitalize()
        long_price_str = f"{price_long:.3f}" if price_long is not None else "N/A"
        long_funding_str = f"{funding_long * 100:.3f}%" if funding_long is not None else "N/A"
        short_price_str = f"{price_short:.3f}" if price_short is not None else "N/A"
        short_funding_str = f"{funding_short * 100:.3f}%" if funding_short is not None else "N/A"
        lines.append(f'🟢 Лонг (<a href="{long_url}">{long_cap}</a>) | Цена: {long_price_str} | Фандинг: {long_funding_str}')
        lines.append(f'🔴 Шорт (<a href="{short_url}">{short_cap}</a>) | Цена: {short_price_str} | Фандинг: {short_funding_str}')
        fr_str = f" | Спред по фандингу: {funding_spread_val:.3f}%" if funding_spread_val is not None else ""
        lines.append(f'• Спред по цене: {open_spread_pct:.3f}%{fr_str}')
        lines.append(f'💎 Стратегия: {coin} Лонг (<a href="{long_url}">{long_cap}</a>), Шорт (<a href="{short_url}">{short_cap}</a>)')
        lines.append("")
    return "\n".join(lines)


def _generate_arbitrage_table_image(
    coin: str,
    opportunities: List[Dict[str, Any]],
) -> Optional[io.BytesIO]:
    if not PIL_AVAILABLE or not opportunities:
        return None
    try:
        cell_padding = 8
        cell_height = 35
        header_height = 40
        row_height = cell_height + cell_padding * 2
        border_width = 2
        rows = []
        for opp in opportunities:
            long_data = opp.get("long_data")
            short_data = opp.get("short_data")
            open_spread_pct = opp["open_spread_pct"]
            funding_spread_val = opp.get("funding_spread_pct")
            price_long = long_data.get("ask") or long_data.get("price") if long_data else None
            if not price_long and long_data and long_data.get("bid") and long_data.get("ask"):
                price_long = (long_data["bid"] + long_data["ask"]) / 2.0
            price_short = short_data.get("bid") or short_data.get("price") if short_data else None
            if not price_short and short_data and short_data.get("bid") and short_data.get("ask"):
                price_short = (short_data["bid"] + short_data["ask"]) / 2.0
            funding_long = long_data.get("funding_rate") if long_data else None
            funding_short = short_data.get("funding_rate") if short_data else None
            total_spread = open_spread_pct + (funding_spread_val if funding_spread_val is not None else 0)
            rows.append({
                "coin": coin,
                "pr_long": f"{price_long:.3f}" if price_long else "none",
                "pr_short": f"{price_short:.3f}" if price_short else "none",
                "funding_long": f"{funding_long * 100:.3f}" if funding_long is not None else "none",
                "funding_short": f"{funding_short * 100:.3f}" if funding_short is not None else "none",
                "pr_spread": f"{open_spread_pct:.3f}",
                "fr_spread": f"{funding_spread_val:.3f}" if funding_spread_val is not None else "none",
                "total_spread": f"{total_spread:.3f}",
                "ex_spread": f"Long ({opp['long_ex']}), Short ({opp['short_ex']})",
                "total_spread_num": total_spread,  # для сортировки
            })
        rows.sort(key=lambda x: x["total_spread_num"], reverse=True)
        col_widths = {
            "coin": 120, "pr_long": 90, "pr_short": 90,
            "funding_long": 80, "funding_short": 80,
            "pr_spread": 100, "fr_spread": 100, "total_spread": 100, "ex_spread": 200,
        }
        total_width = sum(col_widths.values()) + border_width * (len(col_widths) + 1)
        total_height = header_height + len(rows) * row_height + border_width * 2
        img = Image.new("RGB", (total_width, total_height), color="white")
        draw = ImageDraw.Draw(img)
        try:
            font = ImageFont.truetype("arial.ttf", 12)
            font_bold = ImageFont.truetype("arialbd.ttf", 12)
        except Exception:
            try:
                font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 12)
                font_bold = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 12)
            except Exception:
                font = ImageFont.load_default()
                font_bold = font
        headers = ["coin", "pr_long", "pr_short", "funding_long", "funding_short", "pr_spread", "fr_spread", "total_spread", "ex_spread"]
        x, y = border_width, border_width
        draw.rectangle([x, y, total_width - border_width, y + header_height], fill="#e0e0e0", outline="#000000", width=border_width)
        for h in headers:
            w = col_widths[h]
            draw.text((x + cell_padding, y + (header_height - 20) // 2), h, fill="black", font=font_bold)
            x += w
        y = border_width + header_height
        for row_idx, row in enumerate(rows):
            x = border_width
            row_y = y + row_idx * row_height
            fill = "#f5f5f5" if row_idx % 2 == 0 else "white"
            draw.rectangle([x, row_y, total_width - border_width, row_y + row_height], fill=fill, outline="#000000", width=1)
            for h in headers:
                w = col_widths[h]
                draw.text((x + cell_padding, row_y + cell_padding), str(row.get(h, "")), fill="black", font=font)
                x += w
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)
        return buf
    except Exception as e:
        logger.error(f"Ошибка генерации изображения для {coin}: {e}", exc_info=True)
        return None


async def collect_coins_by_exchange(bot: PerpArbitrageBot, exchanges: List[str]) -> Dict[str, Set[str]]:
    tasks = {ex: asyncio.create_task(bot.exchanges[ex].get_all_futures_coins()) for ex in exchanges}
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    out: Dict[str, Set[str]] = {}
    for ex, res in zip(tasks.keys(), results):
        if isinstance(res, Exception) or not res:
            out[ex] = set()
        else:
            out[ex] = {c for c in set(res) if not is_ignored_coin(c) and c.upper() not in EXCLUDE_COINS}
    return out


def build_union(coins_by_exchange: Dict[str, Set[str]]) -> List[str]:
    sets = [s for s in coins_by_exchange.values() if s]
    return sorted(set.union(*sets)) if sets else []


async def process_coin(
    bot: PerpArbitrageBot,
    exchanges: List[str],
    coin: str,
    sem: asyncio.Semaphore,
    coins_by_exchange: Dict[str, Set[str]],
    analysis_sem: asyncio.Semaphore,
) -> None:
    ex_list = [ex for ex in exchanges if coin in coins_by_exchange.get(ex, set())]
    if len(ex_list) < 2:
        return
    tasks = {ex: asyncio.create_task(fetch(bot, ex, coin, sem)) for ex in ex_list}
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    ex_data: Dict[str, Optional[Dict[str, Any]]] = {}
    for ex, res in zip(tasks.keys(), results):
        ex_data[ex] = res if not isinstance(res, Exception) else None
    available = {
        ex: d for ex, d in ex_data.items()
        if d and d.get("bid") is not None and d.get("ask") is not None
    }
    if len(available) < 2:
        return

    per_coin_found: List[Tuple[str, str, float, float]] = []
    for ex1, ex2 in combinations(available.keys(), 2):
        d1, d2 = available[ex1], available[ex2]
        fl1, fl2 = d1.get("funding_rate"), d2.get("funding_rate")
        if fl1 is None or fl2 is None:
            continue
        # Long ex1, Short ex2
        spread_price = calc_open_spread_pct(d1["ask"], d2["bid"])
        spread_funding = funding_spread_pct(fl1, fl2)
        if spread_price is not None and spread_funding is not None:
            if spread_funding >= MIN_FUNDING_SPREAD and abs(spread_price) <= MAX_PRICE_SPREAD:
                per_coin_found.append((ex1, ex2, spread_price, spread_funding))
        # Long ex2, Short ex1
        spread_price2 = calc_open_spread_pct(d2["ask"], d1["bid"])
        spread_funding2 = funding_spread_pct(fl2, fl1)
        if spread_price2 is not None and spread_funding2 is not None:
            if spread_funding2 >= MIN_FUNDING_SPREAD and abs(spread_price2) <= MAX_PRICE_SPREAD:
                per_coin_found.append((ex2, ex1, spread_price2, spread_funding2))

    if not per_coin_found:
        return

    results = await asyncio.gather(
        *(
            _analyze_and_log_opportunity(
                bot, coin, long_ex, short_ex, open_spread_pct, funding_spread_val,
                analysis_sem,
                long_data=available.get(long_ex),
                short_data=available.get(short_ex),
            )
            for long_ex, short_ex, open_spread_pct, funding_spread_val in per_coin_found
        ),
        return_exceptions=True,
    )
    opportunities = [r for r in results if r is not None and not isinstance(r, Exception)]
    # В Telegram только если есть хотя бы одна возможность с minutes_until < SCAN_FUNDING_MIN_TIME_TO_PAY
    to_send = [
        o for o in opportunities
        if o.get("minutes_until") is not None and o["minutes_until"] < SCAN_FUNDING_MIN_TIME_TO_PAY
    ]
    if not to_send:
        return
    try:
        telegram = TelegramSender()
        if not telegram.enabled:
            return
        channel_id = config.TEST_CHANNEL_ID
        if not channel_id:
            logger.warning("Telegram: канал не настроен")
            return
        if PIL_AVAILABLE:
            table_image = _generate_arbitrage_table_image(coin, to_send)
            if table_image:
                max_fr_spread = max((o.get("funding_spread_pct") or 0) for o in to_send)
                max_opp = max(to_send, key=lambda o: o["open_spread_pct"] + o.get("funding_spread_pct", 0))
                long_ex = max_opp["long_ex"]
                short_ex = max_opp["short_ex"]
                long_url = _get_exchange_url(long_ex, coin)
                short_url = _get_exchange_url(short_ex, coin)
                caption = (
                    f'🔔 💰Спред фандингов: монета {coin} (для ликв.: {SCAN_COIN_INVEST:.1f} USDT)\n'
                    f'{coin} Лонг (<a href="{long_url}">{long_ex.capitalize()}</a>), '
                    f'Шорт (<a href="{short_url}">{short_ex.capitalize()}</a>) макс. спред фандингов: {max_fr_spread:.3f}%'
                )
                success = await telegram.send_photo(table_image, caption=caption, channel_id=channel_id)
                if success:
                    logger.debug(f"📱 Telegram: отправлено изображение для {coin}")
                else:
                    await telegram.send_message(
                        _format_combined_telegram_message(coin, to_send), channel_id=channel_id,
                    )
            else:
                await telegram.send_message(
                    _format_combined_telegram_message(coin, to_send), channel_id=channel_id,
                )
        else:
            await telegram.send_message(
                _format_combined_telegram_message(coin, to_send), channel_id=channel_id,
            )
    except Exception as e:
        logger.warning(f"Ошибка отправки в Telegram для {coin}: {e}", exc_info=True)


async def scan_once(
    bot: PerpArbitrageBot,
    exchanges: List[str],
    coins: List[str],
    sem: asyncio.Semaphore,
    coins_by_exchange: Dict[str, Set[str]],
    analysis_sem: asyncio.Semaphore,
) -> None:
    for i in range(0, len(coins), COIN_BATCH_SIZE):
        batch = coins[i : i + COIN_BATCH_SIZE]
        await asyncio.gather(
            *(process_coin(bot, exchanges, coin, sem, coins_by_exchange, analysis_sem) for coin in batch),
            return_exceptions=True,
        )
        logger.info(f"Progress: {min(i + COIN_BATCH_SIZE, len(coins))}/{len(coins)} coins processed")


async def main():
    bot = PerpArbitrageBot()
    try:
        exchanges = [ex for ex in bot.exchanges.keys() if ex not in EXCLUDE_EXCHANGES]
        sem = asyncio.Semaphore(MAX_CONCURRENCY)
        analysis_sem = asyncio.Semaphore(ANALYSIS_MAX_CONCURRENCY)
        telegram = TelegramSender()
        logger.info(
            f"scan_fundings_spreads started | mode={config.ENV_MODE} | "
            f"MIN_FUNDING_SPREAD={MIN_FUNDING_SPREAD:.2f}% | MAX_PRICE_SPREAD={MAX_PRICE_SPREAD:.2f}% | "
            f"MIN_TIME_TO_PAY={SCAN_FUNDING_MIN_TIME_TO_PAY:.0f} min | interval={SCAN_INTERVAL_SEC}s | "
            f"exchanges={exchanges} | telegram={'enabled' if telegram.enabled else 'disabled'}"
        )
        printed_stats = False
        while True:
            coins_by_exchange = await collect_coins_by_exchange(bot, exchanges)
            coins = build_union(coins_by_exchange)
            if not printed_stats:
                logger.info(f"Всего монет (union): {len(coins)}")
                for ex in exchanges:
                    logger.info(f"{ex}: {len(coins_by_exchange.get(ex, set()))} монет")
                printed_stats = True
            logger.info(f"🔄 Новый цикл | coins={len(coins)}")
            t0 = time.perf_counter()
            if coins:
                await scan_once(bot, exchanges, coins, sem, coins_by_exchange, analysis_sem)
            dt = time.perf_counter() - t0
            logger.info(f"scan_once finished in {dt:.1f}s; sleeping {SCAN_INTERVAL_SEC:.1f}s")
            await asyncio.sleep(SCAN_INTERVAL_SEC)
    except KeyboardInterrupt:
        logger.info("scan_fundings_spreads stopped by user")
    finally:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())
