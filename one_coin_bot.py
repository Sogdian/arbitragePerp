import asyncio
import contextlib
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple


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
        # если .env битый — молча пропускаем, чтобы скрипт не падал
        return


load_dotenv(".env")


# ----------------------------
# Settings (как в scan_spreads.py, но для одного коина)
# ----------------------------
MIN_SPREAD = float(os.getenv("MIN_SPREAD", "2"))  # минимальный спред цены для вердикта ✅
MAX_CONCURRENCY = int(os.getenv("SCAN_MAX_CONCURRENCY", "40"))
REQ_TIMEOUT_SEC = float(os.getenv("SCAN_REQ_TIMEOUT_SEC", "12"))
TICKER_TIMEOUT_SEC = float(os.getenv("SCAN_TICKER_TIMEOUT_SEC", str(REQ_TIMEOUT_SEC)))
FUNDING_TIMEOUT_SEC = float(os.getenv("SCAN_FUNDING_TIMEOUT_SEC", str(REQ_TIMEOUT_SEC)))
FETCH_RETRIES = int(os.getenv("SCAN_FETCH_RETRIES", "1"))
FETCH_RETRY_BACKOFF_SEC = float(os.getenv("SCAN_FETCH_RETRY_BACKOFF_SEC", "0.6"))
SCAN_COIN_INVEST = float(os.getenv("SCAN_COIN_INVEST", "50"))
NEWS_CACHE_TTL_SEC = float(os.getenv("SCAN_NEWS_CACHE_TTL_SEC", "180"))
ANALYSIS_MAX_CONCURRENCY = int(os.getenv("SCAN_ANALYSIS_MAX_CONCURRENCY", "2"))
EXCLUDE_EXCHANGES = {"lbank"}  # не использовать


# ----------------------------
# Logging (отдельный лог-файл)
# ----------------------------
LOG_LEVEL = os.getenv("SCAN_LOG_LEVEL", "INFO").upper()
LOG_FILE = os.getenv("SCAN_LOG_FILE", "scan_one_coin.log")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
    force=True,  # важно: перебиваем возможный logging.basicConfig из bot.py
)

logger = logging.getLogger("scan_one_coin")
logging.getLogger("httpx").setLevel(logging.WARNING)
# Убираем шум внутренних модулей; ошибки логируем здесь сами.
logging.getLogger("bot").setLevel(logging.CRITICAL)
logging.getLogger("news_monitor").setLevel(logging.CRITICAL)
logging.getLogger("announcements_monitor").setLevel(logging.CRITICAL)
logging.getLogger("exchanges").setLevel(logging.CRITICAL)


from bot import PerpArbitrageBot  # noqa: E402  (import after logging setup)


# ----------------------------
# Helpers
# ----------------------------
def _fmt_price(x: Optional[float]) -> str:
    if x is None:
        return "N/A"
    try:
        return f"{float(x):.3f}".rstrip("0").rstrip(".")
    except Exception:
        return "N/A"


def _fmt_pct(x: Optional[float], decimals: int = 3) -> str:
    if x is None:
        return "N/A"
    try:
        return f"{float(x):.{decimals}f}%"
    except Exception:
        return "N/A"


def _price_from_ticker(d: Optional[Dict[str, Any]]) -> Optional[float]:
    if not d:
        return None
    price = d.get("price")
    if price is not None:
        return price
    bid = d.get("bid")
    ask = d.get("ask")
    if bid is not None and ask is not None:
        try:
            return (float(bid) + float(ask)) / 2.0
        except Exception:
            return None
    return None


def calc_open_spread_pct(ask_long: Optional[float], bid_short: Optional[float]) -> Optional[float]:
    # open_spread = (bid_short - ask_long) / ask_long * 100
    if ask_long is None or bid_short is None:
        return None
    if ask_long <= 0:
        return None
    return ((bid_short - ask_long) / ask_long) * 100.0


# ----------------------------
# News cache
# ----------------------------
# ВАЖНО: кешируем по (coin, exchange), чтобы переиспользовать результаты между разными парами бирж.
# key=(coin, ex) -> (expires_at_monotonic, delisting_news, security_news)
_news_cache: Dict[Tuple[str, str], Tuple[float, List[Dict[str, Any]], List[Dict[str, Any]]]] = {}


async def _get_news_cached(
    bot: PerpArbitrageBot,
    coin: str,
    long_ex: str,
    short_ex: str,
    days_back: int = 60,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], bool]:
    """
    Возвращает (delisting_news, security_news, cached) для пары бирж,
    используя кеш на уровне (coin, exchange).
    TTL по умолчанию берём из SCAN_NEWS_CACHE_TTL_SEC.
    """
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
            limit=200,
            days_back=days_back,
            exchanges=[ex],
        )

        delisting_news = await bot.news_monitor.find_delisting_news(anns, coin_symbol=coin, lookback=lookback)

        # X (optional): дергаем только если по официальным announcements ничего не нашли
        if (not delisting_news) and getattr(bot, "x_news_monitor", None) is not None and bot.x_news_monitor.enabled:
            try:
                x_del = await bot.x_news_monitor.find_delisting_news(
                    coin_symbol=coin,
                    exchanges=[ex],
                    lookback=lookback,
                )
                if x_del:
                    delisting_news = _merge_dedupe((delisting_news or []) + x_del)
            except Exception:
                pass

        security_news: List[Dict[str, Any]] = []
        if not delisting_news:
            security_news = await bot.announcements_monitor.find_security_news(anns, coin_symbol=coin, lookback=lookback)
            # X (optional): дергаем только если security по announcements не нашли
            if (not security_news) and getattr(bot, "x_news_monitor", None) is not None and bot.x_news_monitor.enabled:
                try:
                    x_sec = await bot.x_news_monitor.find_security_news(
                        coin_symbol=coin,
                        exchanges=[ex],
                        lookback=lookback,
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
    cached_any = bool(c1 and c2)
    return delisting_news, security_news, cached_any


# ----------------------------
# Exchange data fetch
# ----------------------------
async def fetch_exchange_data(bot: PerpArbitrageBot, ex: str, coin: str, sem: asyncio.Semaphore) -> Dict[str, Any]:
    """
    Возвращает dict:
      - price, bid, ask (если доступны)
      - funding_rate (если доступен)
    Ошибки логируем, но возвращаем хоть что-то (в т.ч. пустой dict), чтобы пары могли вывести N/A.
    """
    out: Dict[str, Any] = {}
    exchange = bot.exchanges.get(ex)
    if not exchange:
        return out

    # 1) Тикер (важно для спреда): ретраим только timeouts.
    ticker: Optional[Dict[str, Any]] = None
    for attempt in range(max(0, FETCH_RETRIES) + 1):
        try:
            async with sem:
                ticker = await asyncio.wait_for(exchange.get_futures_ticker(coin), timeout=TICKER_TIMEOUT_SEC)
            break
        except asyncio.TimeoutError:
            is_last = attempt >= max(0, FETCH_RETRIES)
            if is_last:
                logger.warning(f"Timeout: {ex} {coin} ticker > {TICKER_TIMEOUT_SEC:.1f}s")
            else:
                logger.debug(f"Timeout: {ex} {coin} ticker > {TICKER_TIMEOUT_SEC:.1f}s (retry {attempt + 1})")
            await asyncio.sleep(FETCH_RETRY_BACKOFF_SEC * (attempt + 1))
        except Exception as e:
            logger.warning(f"Fetch error: {ex} {coin} ticker: {e}", exc_info=True)
            ticker = None
            break

    if ticker:
        out.update(
            {
                "price": ticker.get("price"),
                "bid": ticker.get("bid"),
                "ask": ticker.get("ask"),
            }
        )

    # 2) Funding (не критично для цены): таймаут/ошибка не должны "убивать" результат.
    try:
        async with sem:
            funding_rate = await asyncio.wait_for(exchange.get_funding_rate(coin), timeout=FUNDING_TIMEOUT_SEC)
        if funding_rate is not None:
            out["funding_rate"] = funding_rate
    except asyncio.TimeoutError:
        logger.debug(f"Timeout: {ex} {coin} funding > {FUNDING_TIMEOUT_SEC:.1f}s")
    except Exception as e:
        logger.warning(f"Fetch error: {ex} {coin} funding: {e}", exc_info=True)

    return out


async def _collect_supported_exchanges(bot: PerpArbitrageBot, coin: str) -> List[str]:
    """
    Возвращает список бирж (ключи bot.exchanges), где coin доступен.
    Если монеты нет — биржа просто пропускается (без логов).
    Если произошла ошибка запроса списка монет — логируем.
    """
    coin_u = coin.upper()
    tasks: Dict[str, asyncio.Task] = {}
    for ex, ex_obj in bot.exchanges.items():
        if ex in EXCLUDE_EXCHANGES:
            continue
        tasks[ex] = asyncio.create_task(ex_obj.get_all_futures_coins())

    supported: List[str] = []
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    for ex, res in zip(tasks.keys(), results):
        if isinstance(res, Exception):
            logger.warning(f"Coin list error: {ex} {coin_u}: {res}", exc_info=True)
            continue
        if not res:
            continue
        try:
            coins: Set[str] = {str(x).upper() for x in res if x}
        except Exception:
            coins = set()
        if coin_u in coins:
            supported.append(ex)
    return supported


async def _analyze_pair_line(
    bot: PerpArbitrageBot,
    coin: str,
    long_ex: str,
    short_ex: str,
    data_by_ex: Dict[str, Dict[str, Any]],
    analysis_sem: asyncio.Semaphore,
) -> str:
    long_data = data_by_ex.get(long_ex) or {}
    short_data = data_by_ex.get(short_ex) or {}

    # Цена: используем price если есть, иначе mid(bid/ask)
    price_long = _price_from_ticker(long_data)
    price_short = _price_from_ticker(short_data)

    # Для спреда цены берём ask_long и bid_short (если есть), иначе fallback на price
    ask_long = long_data.get("ask")
    bid_short = short_data.get("bid")
    if ask_long is None:
        ask_long = price_long
    if bid_short is None:
        bid_short = price_short

    open_spread_pct = calc_open_spread_pct(ask_long, bid_short)

    # Funding
    funding_long = long_data.get("funding_rate")
    funding_short = short_data.get("funding_rate")
    funding_long_pct = (funding_long * 100.0) if funding_long is not None else None
    funding_short_pct = (funding_short * 100.0) if funding_short is not None else None
    funding_spread_pct = None
    if funding_long is not None and funding_short is not None:
        funding_spread_pct = (funding_short - funding_long) * 100.0

    # Базовая строка (всегда)
    price_spread_str = f"{open_spread_pct:.3f}%" if open_spread_pct is not None else "N/A"
    funding_spread_str = f"{funding_spread_pct:.3f}%" if funding_spread_pct is not None else "N/A"
    
    # Вычисляем общий спред (спред на цену + спред на фандинги)
    total_spread_pct = None
    if open_spread_pct is not None and funding_spread_pct is not None:
        total_spread_pct = open_spread_pct + funding_spread_pct
    total_spread_str = f"{total_spread_pct:.3f}%" if total_spread_pct is not None else "N/A"

    base_line = (
        f"📈 Long {long_ex} Ц/Ф: {_fmt_price(price_long)} / {_fmt_pct(funding_long_pct)} | "
        f"📉 Short {short_ex} Ц/Ф: {_fmt_price(price_short)} / {_fmt_pct(funding_short_pct)} | "
        f"📊 Спред Ц/Ф/О: {price_spread_str} / {funding_spread_str} / {total_spread_str}"
    )

    # Если в логе есть N/A — вердикт не выводим
    if "N/A" in base_line:
        return base_line

    # Если спред цены меньше MIN_SPREAD — ❌ не арбитражить
    if open_spread_pct < MIN_SPREAD:
        return f"{base_line} | ❌ не арбитр (спред {open_spread_pct:.3f}% < {MIN_SPREAD}%)"

    # Вердикт: MIN_SPREAD + (ликвидность OK) + (нет проблемных новостей)
    long_liq = None
    short_liq = None
    delisting_news = []
    security_news = []
    long_obj = bot.exchanges.get(long_ex)
    short_obj = bot.exchanges.get(short_ex)
    ok = False
    try:
        async with analysis_sem:
            liq_ok = False
            if long_obj and short_obj:
                long_liq = await long_obj.check_liquidity(
                    coin,
                    notional_usdt=SCAN_COIN_INVEST,
                    ob_limit=50,
                    max_spread_bps=30.0,
                    max_impact_bps=50.0,
                    mode="entry_long",
                )
                short_liq = await short_obj.check_liquidity(
                    coin,
                    notional_usdt=SCAN_COIN_INVEST,
                    ob_limit=50,
                    max_spread_bps=30.0,
                    max_impact_bps=50.0,
                    mode="entry_short",
                )
                liq_ok = bool(
                    long_liq
                    and long_liq.get("ok") is True
                    and short_liq
                    and short_liq.get("ok") is True
                )

            delisting_news, security_news, _cached = await _get_news_cached(
                bot,
                coin=coin,
                long_ex=long_ex,
                short_ex=short_ex,
                days_back=60,
            )
            news_ok = bool((not delisting_news) and (not security_news))
            ok = bool(liq_ok and news_ok)

        verdict = "✅ арбитр" if ok else "❌ не арбитр"
        
        # Вычисляем количество монет для каждой биржи (если вердикт "✅ арбитр")
        coins_info = ""
        if ok and price_long is not None and price_short is not None and price_long > 0 and price_short > 0:
            coins_long = SCAN_COIN_INVEST / price_long
            coins_short = SCAN_COIN_INVEST / price_short
            coins_info = f" ({long_ex}: {coins_long:.3f} {coin}, {short_ex}: {coins_short:.3f} {coin})"
        
        # Собираем причины, если вердикт "❌ не арбитр"
        reasons_parts = []
        if not ok:
            # Причины из ликвидности Long биржи
            if long_obj and long_liq and not long_liq.get("ok"):
                long_reasons = long_liq.get("reasons", [])
                if long_reasons:
                    reasons_parts.append(f"ликв. Long: {'; '.join(long_reasons)}")
            
            # Причины из ликвидности Short биржи
            if short_obj and short_liq and not short_liq.get("ok"):
                short_reasons = short_liq.get("reasons", [])
                if short_reasons:
                    reasons_parts.append(f"ликв. Short: {'; '.join(short_reasons)}")
            
            # Причины из новостей
            if delisting_news:
                reasons_parts.append("делистинг")
            if security_news:
                reasons_parts.append("безопасность")
        
        # Формируем финальное сообщение
        if reasons_parts:
            reasons_str = f" ({'; '.join(reasons_parts)})"
        else:
            reasons_str = ""
        
        return f"{base_line} | {verdict}{coins_info}{reasons_str}"
    except Exception as e:
        # По требованию: логируем ошибку, но выводим строку БЕЗ вердикта
        logger.warning(f"Analyze error: {coin} long={long_ex} short={short_ex}: {e}", exc_info=True)
        return base_line


async def main() -> int:
    if len(sys.argv) < 2 or not sys.argv[1].strip() or sys.argv[1].strip() in ("-h", "--help", "/?"):
        logger.info("Usage: python one_coin_bot.py COIN")
        return 2

    coin = sys.argv[1].strip().upper()
    logger.info(f"Анализ монеты {coin} (инвестиции (SCAN_COIN_INVEST): {SCAN_COIN_INVEST} USDT на позицию)")

    bot = PerpArbitrageBot()
    try:
        supported = await _collect_supported_exchanges(bot, coin)
        if len(supported) < 2:
            logger.info("Недостаточно бирж с этой монетой (нужно минимум 2).")
            return 0

        sem = asyncio.Semaphore(MAX_CONCURRENCY)
        analysis_sem = asyncio.Semaphore(max(1, ANALYSIS_MAX_CONCURRENCY))

        # 1) Достаём price+funding по всем биржам, где монета есть
        data_by_ex: Dict[str, Dict[str, Any]] = {}
        fetch_tasks: Dict[str, asyncio.Task] = {
            ex: asyncio.create_task(fetch_exchange_data(bot, ex, coin, sem)) for ex in supported
        }
        fetch_results = await asyncio.gather(*fetch_tasks.values(), return_exceptions=True)
        for ex, res in zip(fetch_tasks.keys(), fetch_results):
            if isinstance(res, Exception):
                logger.warning(f"Fetch error: {ex} {coin}: {res}", exc_info=True)
                data_by_ex[ex] = {}
            else:
                data_by_ex[ex] = res or {}

        # 2) Анализируем все направленные пары Long/Short (без сортировки; печатаем по мере готовности)
        pair_tasks: List[asyncio.Task] = []
        for long_ex in supported:
            for short_ex in supported:
                if long_ex == short_ex:
                    continue
                pair_tasks.append(
                    asyncio.create_task(_analyze_pair_line(bot, coin, long_ex, short_ex, data_by_ex, analysis_sem))
                )

        for fut in asyncio.as_completed(pair_tasks):
            try:
                line = await fut
                logger.info(line)
            except Exception as e:
                logger.warning(f"Unexpected pair task error: {e}", exc_info=True)

        return 0
    finally:
        with contextlib.suppress(Exception):
            await bot.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))


