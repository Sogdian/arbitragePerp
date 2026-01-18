import asyncio
import logging
import os
import sys
import time
import contextlib
import io
from datetime import datetime, timedelta, timezone
from itertools import combinations
from typing import Any, Dict, Optional, List, Tuple, Set

from bot import PerpArbitrageBot
from telegram_sender import TelegramSender
import config

try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False


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


# ----------------------------
# Settings
# ----------------------------
load_dotenv(".env")

MIN_SPREAD = float(os.getenv("MIN_SPREAD", "2"))  # в процентах, например 2
SCAN_INTERVAL_SEC = float(os.getenv("SCAN_INTERVAL_SEC", "5"))  # каждые N секунд
MAX_CONCURRENCY = int(os.getenv("SCAN_MAX_CONCURRENCY", "40"))  # сколько одновременных http запросов
COIN_BATCH_SIZE = int(os.getenv("SCAN_COIN_BATCH_SIZE", "50"))  # сколько монет обрабатывать за пачку
REQ_TIMEOUT_SEC = float(os.getenv("SCAN_REQ_TIMEOUT_SEC", "12"))  # таймаут на запрос к бирже (8-12 норм)
TICKER_TIMEOUT_SEC = float(os.getenv("SCAN_TICKER_TIMEOUT_SEC", str(REQ_TIMEOUT_SEC)))  # таймаут только на ticker (сек)
FUNDING_TIMEOUT_SEC = float(os.getenv("SCAN_FUNDING_TIMEOUT_SEC", str(REQ_TIMEOUT_SEC)))  # таймаут только на funding (сек)
FETCH_RETRIES = int(os.getenv("SCAN_FETCH_RETRIES", "1"))  # сколько доп. попыток на ticker при timeout (0-2 разумно)
FETCH_RETRY_BACKOFF_SEC = float(os.getenv("SCAN_FETCH_RETRY_BACKOFF_SEC", "0.6"))  # backoff между попытками ticker
SCAN_COIN_INVEST = float(os.getenv("SCAN_COIN_INVEST", "50"))  # размер позиции (USDT) для проверки ликвидности в сканере
NEWS_CACHE_TTL_SEC = float(os.getenv("SCAN_NEWS_CACHE_TTL_SEC", "180"))  # TTL кеша новостей (сек), по умолчанию 3 минуты
ANALYSIS_MAX_CONCURRENCY = int(os.getenv("SCAN_ANALYSIS_MAX_CONCURRENCY", "2"))  # параллелизм "глубокого" анализа спредов
EXCLUDE_EXCHANGES = {"lbank"}  # не использовать

# Монеты для исключения из поиска спредов (через запятую, например: EXCLUDE_COINS=FLOW,BTC)
EXCLUDE_COINS_STR = os.getenv("EXCLUDE_COINS", "").strip()
EXCLUDE_COINS = {coin.strip().upper() for coin in EXCLUDE_COINS_STR.split(",") if coin.strip()} if EXCLUDE_COINS_STR else set()

# Монеты теперь собираются автоматически со всех бирж
# COINS из .env больше не используется


# ----------------------------
# Logging
# ----------------------------
LOG_LEVEL = os.getenv("SCAN_LOG_LEVEL", "INFO").upper()
LOG_FILE = os.getenv("SCAN_LOG_FILE", "scan_spreads.log")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger("scan_spreads")
logging.getLogger("httpx").setLevel(logging.WARNING)
# В scan_spreads не печатаем "подробные" логи из bot/news/бирж — только итоговую строку с ✅/❌
logging.getLogger("bot").setLevel(logging.CRITICAL)
logging.getLogger("news_monitor").setLevel(logging.CRITICAL)
logging.getLogger("announcements_monitor").setLevel(logging.CRITICAL)
# x_news_monitor: разрешаем WARNING/ERROR для сообщений о лимитах API
logging.getLogger("x_news_monitor").setLevel(logging.WARNING)
logging.getLogger("exchanges").setLevel(logging.CRITICAL)


# ----------------------------
# News cache (only for scan_spreads)
# ----------------------------
# ВАЖНО: кешируем по (coin, exchange), а не по паре бирж.
# Это позволяет переиспользовать результаты при появлении новых комбинаций:
# - сначала посчитали (BTC, bybit) и (BTC, binance)
# - затем для пары (BTC, bybit, gate) докачиваем только (BTC, gate)
# key=(coin, ex) -> (expires_at_monotonic, delisting_news, security_news)
_news_cache: Dict[Tuple[str, str], Tuple[float, List[Dict[str, Any]], List[Dict[str, Any]]]] = {}


@contextlib.contextmanager
def _temp_log_level(logger_names: List[str], level: int):
    """
    Вспомогательный контекст: временно меняет уровень логирования для набора логгеров.
    (Сейчас в основном оставлен для гибкости; ключевые логгеры уже заглушены глобально.)
    """
    old_levels: Dict[str, int] = {}
    for name in logger_names:
        lg = logging.getLogger(name)
        old_levels[name] = lg.level
        lg.setLevel(level)
    try:
        yield
    finally:
        for name, old in old_levels.items():
            logging.getLogger(name).setLevel(old)


# ----------------------------
# Spread math
# ----------------------------
def calc_open_spread_pct(ask_long: Optional[float], bid_short: Optional[float]) -> Optional[float]:
    # open_spread = (bid_short - ask_long) / ask_long * 100
    if ask_long is None or bid_short is None:
        return None
    if ask_long <= 0:
        return None
    return ((bid_short - ask_long) / ask_long) * 100.0


# Семафор для ограничения параллелизма (создается в main() после загрузки настроек)


def is_ignored_coin(coin: str) -> bool:
    """Проверяет, нужно ли игнорировать монету (начинается с цифры)"""
    return bool(coin) and coin[0].isdigit()


async def fetch(bot: PerpArbitrageBot, ex: str, coin: str, sem: asyncio.Semaphore) -> Optional[Dict[str, Any]]:
    """
    Запрос данных с ограничением параллелизма через семафор.

    Важно: тикер (bid/ask) и funding запрашиваем отдельно.
    Если funding завис/затупил — мы всё равно возвращаем тикер, чтобы не терять данные по монете.
    """
    exchange = bot.exchanges.get(ex)
    if not exchange:
        return None

    # Используем глобальные таймауты и ретраи для всех бирж
    ticker_timeout = TICKER_TIMEOUT_SEC
    funding_timeout = FUNDING_TIMEOUT_SEC
    ticker_retries = FETCH_RETRIES

    # 1) Тикер (важно для спреда): ретраим только timeouts.
    # ВАЖНО: семафор держим только во время реального HTTP, не во время sleep/backoff.
    ticker: Optional[Dict[str, Any]] = None
    for attempt in range(max(0, ticker_retries) + 1):
        try:
            async with sem:
                ticker = await asyncio.wait_for(exchange.get_futures_ticker(coin), timeout=ticker_timeout)
            break
        except asyncio.TimeoutError:
            is_last = (attempt >= max(0, ticker_retries))
            if is_last:
                logger.warning(f"Timeout: {ex} {coin} ticker > {ticker_timeout:.1f}s")
            else:
                logger.debug(f"Timeout: {ex} {coin} ticker > {ticker_timeout:.1f}s (retry {attempt + 1})")
            await asyncio.sleep(FETCH_RETRY_BACKOFF_SEC * (attempt + 1))
        except Exception as e:
            logger.warning(f"Fetch error: {ex} {coin} ticker: {e}", exc_info=True)
            return None

    if not ticker:
        return None

    out: Dict[str, Any] = {
        "price": ticker.get("price"),
        "bid": ticker.get("bid"),
        "ask": ticker.get("ask"),
    }

    # 2) Funding (не критично для спреда цены): таймаут/ошибка не должны "убивать" тикер.
    try:
        async with sem:
            funding_rate = await asyncio.wait_for(exchange.get_funding_rate(coin), timeout=funding_timeout)
        if funding_rate is not None:
            out["funding_rate"] = funding_rate
    except asyncio.TimeoutError:
        logger.debug(f"Timeout: {ex} {coin} funding > {funding_timeout:.1f}s")
    except Exception:
        logger.debug(f"Fetch error: {ex} {coin} funding", exc_info=True)

    return out


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
    TTL по умолчанию 3 минуты.
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

        # Один сетевой проход на биржу: announcements для конкретной биржи
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
        # Security проверяем только если делистинг не найден (экономим запросы/шум).
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


async def _analyze_and_log_opportunity(
    bot: PerpArbitrageBot,
    coin: str,
    long_ex: str,
    short_ex: str,
    open_spread_pct: float,
    analysis_sem: asyncio.Semaphore,
    long_data: Optional[Dict[str, Any]] = None,
    short_data: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Считает "как bot.py" (ликвидность + новости), но НЕ печатает подробные логи.
    В логи попадает только 1 строка: "💰 ... spread ... ✅/❌".
    Использует только переданные данные (без дополнительных запросов).
    """
    async with analysis_sem:
        ok = False
        long_liq = None
        short_liq = None
        delisting_news = []
        security_news = []
        
        try:
            
            # 1) Ликвидность (тихо)
            long_obj = bot.exchanges.get(long_ex)
            short_obj = bot.exchanges.get(short_ex)
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
                    long_liq and long_liq.get("ok") is True and short_liq and short_liq.get("ok") is True
                )

            # 2) Новости (тихо, + кеш 3 минуты)
            delisting_news, security_news, _cached = await _get_news_cached(
                bot,
                coin=coin,
                long_ex=long_ex,
                short_ex=short_ex,
                days_back=60,
            )
            news_ok = bool((not delisting_news) and (not security_news))

            ok = bool(liq_ok and news_ok)
        except Exception:
            ok = False

        # Извлекаем funding rates из переданных данных
        funding_long = long_data.get("funding_rate") if long_data else None
        funding_short = short_data.get("funding_rate") if short_data else None
        
        # Вычисляем funding spread, если оба доступны
        funding_spread = None
        funding_spread_str = "N/A"
        if funding_long is not None and funding_short is not None:
            funding_spread = (funding_short - funding_long) * 100
            funding_spread_str = f"{funding_spread:.3f}%"
        
        # Вычисляем общий спред (спред на цену + спред на фандинги)
        total_spread = open_spread_pct
        if funding_spread is not None:
            total_spread = open_spread_pct + funding_spread

        # Извлекаем цены для расчета количества монет
        # ВАЖНО: используем ТОЧНО те же цены, что и для расчета спреда (ask_long и bid_short)
        # чтобы количество монет соответствовало реальному спреду
        # НЕ используем fallback на price, так как это может привести к несоответствию со спредом
        price_long = long_data.get("ask") if long_data else None
        price_short = short_data.get("bid") if short_data else None

        verdict = "✅ арбитражить" if ok else "❌ не арбитражить"
        
        # Вычисляем количество монет для каждой биржи (если вердикт "✅ арбитражить")
        # ВАЖНО: используем те же цены, что и для расчета спреда (ask_long и bid_short)
        coins_info = ""
        if ok and price_long is not None and price_short is not None and price_long > 0 and price_short > 0:
            coins_long = SCAN_COIN_INVEST / price_long
            coins_short = SCAN_COIN_INVEST / price_short
            coins_info = f" ({long_ex}: {coins_long:.3f} {coin}, {short_ex}: {coins_short:.3f} {coin})"
        
        # Собираем причины, если вердикт "❌ не арбитражить"
        reasons_parts = []
        if not ok:
            # Причины из ликвидности Long биржи
            if long_liq and not long_liq.get("ok"):
                long_reasons = long_liq.get("reasons", [])
                if long_reasons:
                    reasons_parts.append(f"ликв. Long: {'; '.join(long_reasons)}")
            
            # Причины из ликвидности Short биржи
            if short_liq and not short_liq.get("ok"):
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
        
        log_message = f"💰 {coin} Long ({long_ex}), Short ({short_ex}) Спред на цену: {open_spread_pct:.3f}% | Фандинг: {funding_spread_str} | Спред общий: {total_spread:.3f}% {verdict}{coins_info}{reasons_str}"
        
        logger.info(log_message)
        
        # Возвращаем данные о найденной возможности, если вердикт "✅ арбитражить"
        if ok:
            return {
                "coin": coin,
                "long_ex": long_ex,
                "short_ex": short_ex,
                "open_spread_pct": open_spread_pct,
                "long_data": long_data,
                "short_data": short_data,
                "long_liq": long_liq,
                "short_liq": short_liq,
                "delisting_news": delisting_news,
                "security_news": security_news,
            }
        return None


def _get_exchange_url(exchange: str, coin: str) -> str:
    """
    Генерирует ссылку на торговую страницу биржи для конкретной монеты.
    
    Args:
        exchange: Название биржи (lowercase, например "bybit", "gate")
        coin: Название монеты (например "FLOW")
    
    Returns:
        URL ссылка на торговую страницу биржи
    """
    exchange_lower = exchange.lower()
    coin_upper = coin.upper()
    
    # Формируем символ в зависимости от биржи
    if exchange_lower == "bybit":
        symbol = f"{coin_upper}USDT"
        # Bybit: у trade-ссылок нет ru-префикса, рабочий вариант без локали
        return f"https://www.bybit.com/trade/usdt/{symbol}"
    elif exchange_lower == "gate":
        symbol = f"{coin_upper}_USDT"
        return f"https://www.gate.com/ru/futures/USDT/{symbol}"
    elif exchange_lower == "okx":
        symbol = f"{coin_upper}-USDT-SWAP"
        return f"https://www.okx.com/ru/trade/futures/{symbol}"
    elif exchange_lower == "binance":
        symbol = f"{coin_upper}USDT"
        return f"https://www.binance.com/ru/futures/{symbol}"
    elif exchange_lower == "bitget":
        symbol = f"{coin_upper}USDT"
        return f"https://www.bitget.com/ru/futures/{symbol}"
    elif exchange_lower == "bingx":
        symbol = f"{coin_upper}-USDT"
        return f"https://bingx.com/ru-ru/perpetual/{symbol}"
    elif exchange_lower == "mexc":
        symbol = f"{coin_upper}_USDT"
        return f"https://www.mexc.com/ru-RU/futures/{symbol}"
    elif exchange_lower == "xt":
        symbol = f"{coin_upper}_USDT"
        return f"https://www.xt.com/ru/trade/{symbol.lower()}"
    elif exchange_lower == "lbank":
        symbol = f"{coin_upper}USDT"
        return f"https://www.lbank.com/ru/trade/{symbol.lower()}/"
    else:
        # Fallback: возвращаем просто название биржи без ссылки
        return f"https://www.{exchange_lower}.com"


def _format_telegram_message(
    coin: str,
    long_ex: str,
    short_ex: str,
    long_data: Optional[Dict[str, Any]],
    short_data: Optional[Dict[str, Any]],
    open_spread_pct: float,
    long_liq: Optional[Dict[str, Any]],
    short_liq: Optional[Dict[str, Any]],
    delisting_news: List[Dict[str, Any]],
    security_news: List[Dict[str, Any]],
) -> str:
    """Форматирует сообщение для Telegram на английском языке, используя только данные из сканера"""
    # Заголовок
    lines = [f'🔔 <b>Signal: {coin}</b> (Liq: {SCAN_COIN_INVEST:.1f} USDT)']
    lines.append("")
    
    # Long данные - используем price, если есть, иначе среднее от bid/ask
    price_long = None
    funding_long = None
    if long_data:
        price_long = long_data.get("price")
        if price_long is None:
            bid_long = long_data.get("bid")
            ask_long = long_data.get("ask")
            if bid_long is not None and ask_long is not None:
                price_long = (bid_long + ask_long) / 2.0
        funding_long = long_data.get("funding_rate")
    
    # Short данные - используем price, если есть, иначе среднее от bid/ask
    price_short = None
    funding_short = None
    if short_data:
        price_short = short_data.get("price")
        if price_short is None:
            bid_short = short_data.get("bid")
            ask_short = short_data.get("ask")
            if bid_short is not None and ask_short is not None:
                price_short = (bid_short + ask_short) / 2.0
        funding_short = short_data.get("funding_rate")
    
    # LONG секция с ссылкой
    long_ex_capitalized = long_ex.capitalize()
    long_url = _get_exchange_url(long_ex, coin)
    lines.append(f'🟢 <b>LONG</b> (<a href="{long_url}">{long_ex_capitalized}</a>)')
    if price_long is not None:
        lines.append(f'├ Price: <code>{price_long:.3f}</code>')
    if funding_long is not None:
        funding_long_pct = funding_long * 100
        lines.append(f'└ Funding: <code>{funding_long_pct:.3f}%</code>')
    else:
        if price_long is not None:
            lines.append('└ Funding: <code>N/A</code>')
    
    # SHORT секция с ссылкой
    short_ex_capitalized = short_ex.capitalize()
    short_url = _get_exchange_url(short_ex, coin)
    lines.append(f'🔴 <b>SHORT</b> (<a href="{short_url}">{short_ex_capitalized}</a>)')
    if price_short is not None:
        lines.append(f'├ Price: <code>{price_short:.3f}</code>')
    if funding_short is not None:
        funding_short_pct = funding_short * 100
        lines.append(f'└ Funding: <code>{funding_short_pct:.3f}%</code>')
    else:
        if price_short is not None:
            lines.append('└ Funding: <code>N/A</code>')
    
    lines.append("")
    
    # Спреды
    lines.append('<b>📊 Spreads:</b>')
    lines.append(f'• Price Spread: <b>{open_spread_pct:.3f}%</b>')
    
    # Спред на фандинги с порогами
    if funding_long is not None and funding_short is not None:
        funding_spread = (funding_short - funding_long) * 100
        lines.append(f'• Funding Spread: <b>{funding_spread:.3f}%</b>')
    
    lines.append("")
    
    # Стратегия с ссылками
    lines.append(f'💎 <b>Strategy:</b> {coin} Long (<a href="{long_url}">{long_ex_capitalized}</a>), Short (<a href="{short_url}">{short_ex_capitalized}</a>)')
    
    return "\n".join(lines)


def _format_combined_telegram_message(
    coin: str,
    opportunities: List[Dict[str, Any]],
) -> str:
    """Форматирует объединенное сообщение для Telegram с несколькими возможностями по одной монете"""
    lines = [f'🔔 <b>Signal: {coin}</b> (Liq: {SCAN_COIN_INVEST:.1f} USDT)']
    lines.append("")
    
    for opp in opportunities:
        long_ex = opp["long_ex"]
        short_ex = opp["short_ex"]
        long_data = opp.get("long_data")
        short_data = opp.get("short_data")
        open_spread_pct = opp["open_spread_pct"]
        
        # Long данные
        price_long = None
        funding_long = None
        if long_data:
            price_long = long_data.get("price")
            if price_long is None:
                bid_long = long_data.get("bid")
                ask_long = long_data.get("ask")
                if bid_long is not None and ask_long is not None:
                    price_long = (bid_long + ask_long) / 2.0
            funding_long = long_data.get("funding_rate")
        
        # Short данные
        price_short = None
        funding_short = None
        if short_data:
            price_short = short_data.get("price")
            if price_short is None:
                bid_short = short_data.get("bid")
                ask_short = short_data.get("ask")
                if bid_short is not None and ask_short is not None:
                    price_short = (bid_short + ask_short) / 2.0
            funding_short = short_data.get("funding_rate")
        
        # LONG секция
        long_ex_capitalized = long_ex.capitalize()
        long_url = _get_exchange_url(long_ex, coin)
        long_price_str = f"{price_long:.3f}" if price_long is not None else "N/A"
        long_funding_str = f"{funding_long * 100:.3f}%" if funding_long is not None else "N/A"
        lines.append(f'🟢 LONG (<a href="{long_url}">{long_ex_capitalized}</a>) | Price: {long_price_str} | Funding: {long_funding_str}')
        
        # SHORT секция
        short_ex_capitalized = short_ex.capitalize()
        short_url = _get_exchange_url(short_ex, coin)
        short_price_str = f"{price_short:.3f}" if price_short is not None else "N/A"
        short_funding_str = f"{funding_short * 100:.3f}%" if funding_short is not None else "N/A"
        lines.append(f'🔴 SHORT (<a href="{short_url}">{short_ex_capitalized}</a>) | Price: {short_price_str} | Funding: {short_funding_str}')
        
        # Спреды
        funding_spread_str = ""
        if funding_long is not None and funding_short is not None:
            funding_spread = (funding_short - funding_long) * 100
            funding_spread_str = f" | Funding spread: {funding_spread:.3f}%"
        lines.append(f'• Price spread: {open_spread_pct:.3f}%{funding_spread_str}')
        
        # Strategy с ссылками
        lines.append(f'💎 Strategy: {coin} Long (<a href="{long_url}">{long_ex_capitalized}</a>), Short (<a href="{short_url}">{short_ex_capitalized}</a>)')
        lines.append("")
    
    return "\n".join(lines)


def _generate_arbitrage_table_image(
    coin: str,
    opportunities: List[Dict[str, Any]],
) -> Optional[io.BytesIO]:
    """
    Генерирует изображение таблицы с данными арбитража для отправки в Telegram
    
    Args:
        coin: Название монеты
        opportunities: Список возможностей арбитража
        
    Returns:
        BytesIO объект с изображением или None если PIL недоступен
    """
    if not PIL_AVAILABLE:
        return None
    
    if not opportunities:
        return None
    
    try:
        # Параметры изображения
        cell_padding = 8
        cell_height = 35
        header_height = 40
        row_height = cell_height + cell_padding * 2
        border_width = 2
        
        # Подготовка данных для таблицы
        rows = []
        for opp in opportunities:
            long_ex = opp["long_ex"]
            short_ex = opp["short_ex"]
            long_data = opp.get("long_data")
            short_data = opp.get("short_data")
            open_spread_pct = opp["open_spread_pct"]
            
            # Получаем цены
            price_long = None
            if long_data:
                # Для pr_long показываем цену входа в Long: покупка по ask
                price_long = long_data.get("ask")
                if price_long is None:
                    price_long = long_data.get("price")
                if price_long is None:
                    bid_long = long_data.get("bid")
                    ask_long = long_data.get("ask")
                    if bid_long is not None and ask_long is not None:
                        price_long = (bid_long + ask_long) / 2.0
            
            price_short = None
            if short_data:
                # Для pr_short показываем цену входа в Short: продажа по bid
                price_short = short_data.get("bid")
                if price_short is None:
                    price_short = short_data.get("price")
                if price_short is None:
                    bid_short = short_data.get("bid")
                    ask_short = short_data.get("ask")
                    if bid_short is not None and ask_short is not None:
                        price_short = (bid_short + ask_short) / 2.0
            
            # Получаем funding rates
            funding_long = long_data.get("funding_rate") if long_data else None
            funding_short = short_data.get("funding_rate") if short_data else None
            
            # Вычисляем funding spread
            funding_spread = None
            if funding_long is not None and funding_short is not None:
                funding_spread = (funding_short - funding_long) * 100
            
            # Total spread
            total_spread = open_spread_pct
            if funding_spread is not None:
                total_spread = open_spread_pct + funding_spread
            
            # Форматируем значения (все с округлением до 3 знаков после запятой)
            price_long_str = f"{price_long:.3f}" if price_long is not None else "none"
            price_short_str = f"{price_short:.3f}" if price_short is not None else "none"
            funding_long_str = f"{funding_long * 100:.3f}" if funding_long is not None else "none"
            funding_short_str = f"{funding_short * 100:.3f}" if funding_short is not None else "none"
            funding_spread_str = f"{funding_spread:.3f}" if funding_spread is not None else "none"
            
            rows.append({
                "coin": coin,
                "pr_long": price_long_str,
                "pr_short": price_short_str,
                "funding_long": funding_long_str,
                "funding_short": funding_short_str,
                "pr_spread": f"{open_spread_pct:.3f}",
                "fr_spread": funding_spread_str,
                "total_spread": f"{total_spread:.3f}",
                "ex_spread": f"Long ({long_ex}), Short ({short_ex})",
                "total_spread_num": total_spread if total_spread is not None else float('-inf'),  # Для сортировки
            })
        
        # Сортируем строки по total_spread в порядке убывания (от большего к меньшему)
        rows.sort(key=lambda x: x["total_spread_num"], reverse=True)
        
        # Определяем ширину колонок
        col_widths = {
            "coin": 120,
            "pr_long": 90,
            "pr_short": 90,
            "funding_long": 80,
            "funding_short": 80,
            "pr_spread": 100,
            "fr_spread": 100,
            "total_spread": 100,
            "ex_spread": 200,
        }
        
        # Вычисляем общую ширину и высоту
        total_width = sum(col_widths.values()) + border_width * (len(col_widths) + 1)
        total_height = header_height + len(rows) * row_height + border_width * 2
        
        # Создаем изображение
        img = Image.new("RGB", (total_width, total_height), color="white")
        draw = ImageDraw.Draw(img)
        
        # Пробуем загрузить шрифт, если не получается - используем default
        try:
            font = ImageFont.truetype("arial.ttf", 12)
            font_bold = ImageFont.truetype("arialbd.ttf", 12)
        except:
            try:
                font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 12)
                font_bold = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 12)
            except:
                font = ImageFont.load_default()
                font_bold = ImageFont.load_default()
        
        # Рисуем заголовок
        headers = ["coin", "pr_long", "pr_short", "funding_long", "funding_short", "pr_spread", "fr_spread", "total_spread", "ex_spread"]
        header_labels = ["coin", "pr_long", "pr_short", "fr_long", "fr_short", "pr_spread", "fr_spread", "total_spread", "ex_spread"]
        
        x = border_width
        y = border_width
        
        # Фон заголовка
        draw.rectangle([x, y, total_width - border_width, y + header_height], fill="#e0e0e0", outline="#000000", width=border_width)
        
        # Текст заголовка
        for i, header in enumerate(headers):
            label = header_labels[i]
            width = col_widths[header]
            text_x = x + cell_padding
            text_y = y + (header_height - 20) // 2
            draw.text((text_x, text_y), label, fill="black", font=font_bold)
            x += width
        
        # Рисуем строки данных
        y = border_width + header_height
        for row_idx, row in enumerate(rows):
            x = border_width
            row_y = y + row_idx * row_height
            
            # Фон строки (чередование цветов)
            if row_idx % 2 == 0:
                draw.rectangle([x, row_y, total_width - border_width, row_y + row_height], fill="#f5f5f5", outline="#000000", width=1)
            else:
                draw.rectangle([x, row_y, total_width - border_width, row_y + row_height], fill="white", outline="#000000", width=1)
            
            # Текст данных
            for header in headers:
                width = col_widths[header]
                value = str(row.get(header, ""))
                text_x = x + cell_padding
                text_y = row_y + cell_padding
                draw.text((text_x, text_y), value, fill="black", font=font)
                x += width
        
        # Конвертируем в BytesIO
        img_bytes = io.BytesIO()
        img.save(img_bytes, format="PNG")
        img_bytes.seek(0)
        
        return img_bytes
        
    except Exception as e:
        logger.error(f"Ошибка генерации изображения таблицы для {coin}: {e}", exc_info=True)
        return None


async def collect_coins_by_exchange(bot: PerpArbitrageBot, exchanges: List[str]) -> Dict[str, Set[str]]:
    """
    Собирает карту монет для каждой биржи.
    
    Returns:
        Словарь {exchange_name: set_of_coins}
    """
    tasks = {ex: asyncio.create_task(bot.exchanges[ex].get_all_futures_coins()) for ex in exchanges}
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    
    out: Dict[str, Set[str]] = {}
    for ex, res in zip(tasks.keys(), results):
        if isinstance(res, Exception) or not res:
            out[ex] = set()
        else:
            # фильтруем цифро-префиксные и исключенные монеты
            filtered = {c for c in set(res) if not is_ignored_coin(c) and c.upper() not in EXCLUDE_COINS}
            out[ex] = filtered
    
    return out


def build_union(coins_by_exchange: Dict[str, Set[str]]) -> List[str]:
    """Строит union всех монет и возвращает отсортированный список"""
    sets = [s for s in coins_by_exchange.values() if s]
    if not sets:
        return []
    return sorted(set.union(*sets))


async def process_coin(
    bot: PerpArbitrageBot,
    exchanges: List[str],
    coin: str,
    sem: asyncio.Semaphore,
    coins_by_exchange: Dict[str, Set[str]],
    analysis_sem: asyncio.Semaphore,
) -> None:
    """
    Обрабатывает одну монету: запрашивает данные только с бирж, где монета есть,
    вычисляет спреды и логирует находки.
    """
    # Берём только биржи, где coin есть в их списке
    ex_list = [ex for ex in exchanges if coin in coins_by_exchange.get(ex, set())]
    if len(ex_list) < 2:
        return

    tasks = {ex: asyncio.create_task(fetch(bot, ex, coin, sem)) for ex in ex_list}
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    ex_data: Dict[str, Optional[Dict[str, Any]]] = {}
    for ex, res in zip(tasks.keys(), results):
        if isinstance(res, Exception):
            ex_data[ex] = None
        else:
            ex_data[ex] = res

    available = {
        ex: d for ex, d in ex_data.items()
        if d and d.get("bid") is not None and d.get("ask") is not None
    }

    if len(available) < 2:
        return

    per_coin_found: List[Tuple[str, str, float]] = []
    for ex1, ex2 in combinations(available.keys(), 2):
        d1 = available[ex1]
        d2 = available[ex2]

        s1 = calc_open_spread_pct(d1["ask"], d2["bid"])
        if s1 is not None and s1 >= MIN_SPREAD:
            per_coin_found.append((ex1, ex2, s1))

        s2 = calc_open_spread_pct(d2["ask"], d1["bid"])
        if s2 is not None and s2 >= MIN_SPREAD:
            per_coin_found.append((ex2, ex1, s2))

    if per_coin_found:
        per_coin_found.sort(key=lambda x: x[2], reverse=True)
        # Анализируем найденные связки (можно параллельно, но ограничено ANALYSIS_MAX_CONCURRENCY)
        results = await asyncio.gather(
            *(
                _analyze_and_log_opportunity(
                    bot=bot,
                    coin=coin,
                    long_ex=long_ex,
                    short_ex=short_ex,
                    open_spread_pct=spread,
                    analysis_sem=analysis_sem,
                    long_data=available.get(long_ex),
                    short_data=available.get(short_ex),
                )
                for long_ex, short_ex, spread in per_coin_found
            ),
            return_exceptions=True,
        )
        
        # Собираем все найденные возможности (где вердикт "✅ арбитражить")
        opportunities = [r for r in results if r is not None and not isinstance(r, Exception)]
        
        # Отправляем одно объединенное сообщение в Telegram, если есть найденные возможности
        if opportunities:
            try:
                telegram = TelegramSender()
                if telegram.enabled:
                    # scan_spreads.py всегда использует TEST_CHANNEL_ID
                    channel_id = config.TEST_CHANNEL_ID
                    if channel_id:
                        # Вычисляем максимальный total_spread и соответствующую пару бирж для caption
                        max_total_spread = None
                        max_opp = None
                        for opp in opportunities:
                            long_data = opp.get("long_data")
                            short_data = opp.get("short_data")
                            open_spread_pct = opp["open_spread_pct"]
                            
                            funding_long = long_data.get("funding_rate") if long_data else None
                            funding_short = short_data.get("funding_rate") if short_data else None
                            
                            funding_spread = None
                            if funding_long is not None and funding_short is not None:
                                funding_spread = (funding_short - funding_long) * 100
                            
                            total_spread = open_spread_pct
                            if funding_spread is not None:
                                total_spread = open_spread_pct + funding_spread
                            
                            if max_total_spread is None or total_spread > max_total_spread:
                                max_total_spread = total_spread
                                max_opp = opp
                        
                        # Пробуем отправить изображение таблицы (если доступно)
                        if PIL_AVAILABLE:
                            table_image = _generate_arbitrage_table_image(coin=coin, opportunities=opportunities)
                            if table_image:
                                max_spread_str = f"{max_total_spread:.3f}" if max_total_spread is not None else "N/A"
                                if max_opp:
                                    long_ex = max_opp["long_ex"]
                                    short_ex = max_opp["short_ex"]
                                    long_ex_cap = long_ex.capitalize()
                                    short_ex_cap = short_ex.capitalize()
                                    caption = f'🔔 Signal: {coin} (for liq: {SCAN_COIN_INVEST:.1f} USDT)\n{coin} Long ({long_ex_cap}), Short ({short_ex_cap}) max total spread: {max_spread_str}'
                                else:
                                    caption = f'🔔 Signal: {coin} (for liq: {SCAN_COIN_INVEST:.1f} USDT)\nmax total spread: {max_spread_str}'
                                success = await telegram.send_photo(table_image, caption=caption, channel_id=channel_id)
                                if success:
                                    logger.debug(f"📱 Отправлено изображение таблицы в Telegram для {coin} ({len(opportunities)} opportunities, режим: {config.ENV_MODE})")
                                else:
                                    # Fallback на текстовое сообщение, если изображение не отправилось
                                    telegram_message = _format_combined_telegram_message(
                                        coin=coin,
                                        opportunities=opportunities,
                                    )
                                    await telegram.send_message(telegram_message, channel_id=channel_id)
                                    logger.debug(f"📱 Отправлено текстовое сообщение (fallback) в Telegram для {coin} ({len(opportunities)} opportunities, режим: {config.ENV_MODE})")
                            else:
                                # Если не удалось сгенерировать изображение - отправляем текст
                                telegram_message = _format_combined_telegram_message(
                                    coin=coin,
                                    opportunities=opportunities,
                                )
                                await telegram.send_message(telegram_message, channel_id=channel_id)
                                logger.debug(f"📱 Отправлено текстовое сообщение в Telegram для {coin} ({len(opportunities)} opportunities, режим: {config.ENV_MODE})")
                        else:
                            # Если PIL недоступен - отправляем текстовое сообщение
                            telegram_message = _format_combined_telegram_message(
                                coin=coin,
                                opportunities=opportunities,
                            )
                            await telegram.send_message(telegram_message, channel_id=channel_id)
                            logger.debug(f"📱 Отправлено текстовое сообщение в Telegram для {coin} ({len(opportunities)} opportunities, режим: {config.ENV_MODE})")
                    else:
                        logger.warning(f"📱 Telegram включен, но канал не настроен для режима {config.ENV_MODE}")
            except Exception as e:
                logger.warning(f"Ошибка отправки объединенного сообщения в Telegram для {coin}: {e}", exc_info=True)


async def scan_once(
    bot: PerpArbitrageBot,
    exchanges: List[str],
    coins: List[str],
    sem: asyncio.Semaphore,
    coins_by_exchange: Dict[str, Set[str]],
    analysis_sem: asyncio.Semaphore,
) -> None:
    """
    Один проход по всем монетам батчами.
    
    Обрабатывает монеты параллельно батчами размера COIN_BATCH_SIZE.
    Логирует находки сразу после полной обработки каждой монеты.
    Ничего не возвращает.
    """
    total = len(coins)
    for i in range(0, total, COIN_BATCH_SIZE):
        batch = coins[i:i + COIN_BATCH_SIZE]
        await asyncio.gather(
            *(process_coin(bot, exchanges, coin, sem, coins_by_exchange, analysis_sem) for coin in batch),
            return_exceptions=True
        )
        logger.info(f"Progress: {min(i + COIN_BATCH_SIZE, total)}/{total} coins processed")


async def main():
    bot = PerpArbitrageBot()
    try:
        exchanges = [ex for ex in bot.exchanges.keys() if ex not in EXCLUDE_EXCHANGES]
        
        # Создаем семафор для ограничения параллелизма
        sem = asyncio.Semaphore(MAX_CONCURRENCY)
        analysis_sem = asyncio.Semaphore(ANALYSIS_MAX_CONCURRENCY)

        # Логируем режим работы и настройки Telegram
        telegram = TelegramSender()
        telegram_status = "enabled" if telegram.enabled else "disabled"
        channel_info = f"channel={config.TEST_CHANNEL_ID or 'not set'}"
        
        exclude_coins_info = f"exclude_coins={sorted(EXCLUDE_COINS)}" if EXCLUDE_COINS else "exclude_coins=none"
        logger.info(
            f"scan_spreads started | mode={config.ENV_MODE} | MIN_SPREAD={MIN_SPREAD:.2f}% | interval={SCAN_INTERVAL_SEC}s | "
            f"exchanges={exchanges} | "
            f"max_concurrency={MAX_CONCURRENCY} | timeout={REQ_TIMEOUT_SEC:.1f}s | "
            f"invest={SCAN_COIN_INVEST:.2f} | analysis_max_concurrency={ANALYSIS_MAX_CONCURRENCY} | news_cache_ttl={NEWS_CACHE_TTL_SEC:.0f}s | "
            f"telegram={telegram_status} | {channel_info} | {exclude_coins_info}"
        )

        printed_stats = False
        while True:
            # Перед каждым глобальным циклом обновляем список монет по биржам
            coins_by_exchange = await collect_coins_by_exchange(bot, exchanges)
            coins = build_union(coins_by_exchange)

            # Статистика по монетам (как раньше, один раз в начале запуска)
            if not printed_stats:
                logger.info(f"Всего монет (union по биржам): {len(coins)}")
                for ex in exchanges:
                    logger.info(f"{ex}: {len(coins_by_exchange.get(ex, set()))} монет")
                printed_stats = True

            logger.info(f"🔄 Новый цикл поиска | total_coins={len(coins)}")
            t0 = time.perf_counter()
            if coins:
                await scan_once(bot, exchanges, coins, sem, coins_by_exchange, analysis_sem)
            else:
                logger.warning("Нет монет для сканирования (все списки пустые); пропускаю scan_once")
            dt = time.perf_counter() - t0
            logger.info(f"scan_once finished in {dt:.1f}s; sleeping {SCAN_INTERVAL_SEC:.1f}s")
            await asyncio.sleep(SCAN_INTERVAL_SEC)

    except KeyboardInterrupt:
        logger.info("scan_spreads stopped by user")
    finally:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())

