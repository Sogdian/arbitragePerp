"""
Скрипт для поиска фандингов на монеты на бирже Bybit.
Ищет фандинги >= MIN_FUNDING_SPREAD и отправляет уведомления в Telegram.
"""
import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List, Set

from bot import PerpArbitrageBot
from telegram_sender import TelegramSender
import config
import position_opener as po
# Импортируем функцию для вычисления минимального количества монет из fun.py
from fun import _bybit_preflight_and_min_qty


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

MIN_FUNDING_SPREAD = float(os.getenv("MIN_FUNDING_SPREAD", "-1"))  # в процентах, например -1
SCAN_INTERVAL_SEC = float(os.getenv("SCAN_FUNDING_INTERVAL_SEC", "60"))  # каждые N секунд
MAX_CONCURRENCY = int(os.getenv("SCAN_FUNDING_MAX_CONCURRENCY", "20"))  # сколько одновременных http запросов
COIN_BATCH_SIZE = int(os.getenv("SCAN_FUNDING_COIN_BATCH_SIZE", "50"))  # сколько монет обрабатывать за пачку
REQ_TIMEOUT_SEC = float(os.getenv("SCAN_FUNDING_REQ_TIMEOUT_SEC", "12"))  # таймаут на запрос к бирже
SCAN_FUNDING_MIN_TIME_TO_PAY = float(os.getenv("SCAN_FUNDING_MIN_TIME_TO_PAY", "0"))  # минимальное время до выплаты в минутах (если >= этого значения, не отправляем в Telegram)
SCAN_COIN_INVEST = float(os.getenv("SCAN_COIN_INVEST", "50"))  # размер позиции (USDT) для расчета минимального количества монет
EXCLUDE_EXCHANGES = {"lbank"}  # не использовать

# Монеты для исключения из поиска фандингов (через запятую, например: EXCLUDE_COINS=FLOW,BTC)
EXCLUDE_COINS_STR = os.getenv("EXCLUDE_COINS", "").strip()
EXCLUDE_COINS = {coin.strip().upper() for coin in EXCLUDE_COINS_STR.split(",") if coin.strip()} if EXCLUDE_COINS_STR else set()

# Биржи для сканирования фандингов
FUNDING_EXCHANGES = ["bybit"]


# ----------------------------
# Logging
# ----------------------------
LOG_LEVEL = os.getenv("SCAN_FUNDING_LOG_LEVEL", "INFO").upper()
LOG_FILE = os.getenv("SCAN_FUNDING_LOG_FILE", "scan_fundings.log")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger("scan_fundings")
logging.getLogger("httpx").setLevel(logging.WARNING)
# В scan_fundings не печатаем "подробные" логи из bot/бирж
logging.getLogger("bot").setLevel(logging.CRITICAL)
logging.getLogger("exchanges").setLevel(logging.CRITICAL)


# ----------------------------
# Helper functions
# ----------------------------
def is_ignored_coin(coin: str) -> bool:
    """Проверяет, нужно ли игнорировать монету (начинается с цифры)"""
    return bool(coin) and coin[0].isdigit()


def calculate_minutes_until_funding(next_funding_time: Optional[int], exchange: str) -> Optional[int]:
    """
    Вычисляет количество минут до следующей выплаты фандинга.
    Использует только данные из API, без хардкода расписания.
    
    Args:
        next_funding_time: Timestamp следующей выплаты (в миллисекундах для Bybit)
        exchange: Название биржи (только bybit)
        
    Returns:
        Количество минут до выплаты или None если невозможно вычислить
    """
    if next_funding_time is None:
        return None
    
    try:
        # Bybit возвращает timestamp в миллисекундах
        funding_timestamp = next_funding_time / 1000
        
        now_timestamp = time.time()
        seconds_until = funding_timestamp - now_timestamp
        
        if seconds_until < 0:
            # Если время уже прошло, возвращаем None (не вычисляем искусственно)
            return None
        
        minutes_until = int(seconds_until / 60)
        return minutes_until
    except Exception:
        return None


async def calculate_min_qty_for_exchange(
    bot: PerpArbitrageBot,
    exchange_name: str,
    coin: str,
    price_hint: float,
    notional_usdt: float,
) -> Optional[float]:
    """
    Вычисляет минимальное количество монет для ордера с учетом ограничений биржи
    (minOrderQty, minOrderAmt, qtyStep).
    Переиспользует функцию _bybit_preflight_and_min_qty из fun.py.
    
    Args:
        bot: Экземпляр бота
        exchange_name: Название биржи (только bybit)
        coin: Название монеты
        price_hint: Цена монеты (для расчета minOrderAmt)
        notional_usdt: Размер позиции в USDT (не используется)
        
    Returns:
        Минимальное количество монет или None если не удалось вычислить
    """
    if exchange_name.lower() != "bybit":
        # Поддерживается только Bybit
        return None
    
    try:
        exchange_obj = bot.exchanges.get(exchange_name)
        if not exchange_obj:
            return None
        
        # Используем функцию из fun.py для вычисления минимального количества
        # Передаем qty_desired как большое значение, чтобы валидация прошла
        # Нам нужен только первый элемент возвращаемого кортежа (min_qty)
        qty_desired = max(notional_usdt / price_hint if price_hint > 0 else 1000, 1000)
        min_qty, _ = await _bybit_preflight_and_min_qty(
            exchange_obj=exchange_obj,
            coin=coin,
            qty_desired=qty_desired,
            price_hint=price_hint,
        )
        
        return float(min_qty) if min_qty > 0 else None
    except Exception as e:
        logger.debug(f"Ошибка вычисления min_qty для {exchange_name} {coin}: {e}")
        # Fallback: простой расчет
        return notional_usdt / price_hint if price_hint > 0 else None


async def fetch_funding_info(
    bot: PerpArbitrageBot,
    exchange_name: str,
    coin: str,
    sem: asyncio.Semaphore,
) -> Optional[Dict[str, Any]]:
    """
    Запрос информации о фандинге с ограничением параллелизма через семафор.
    
    Returns:
        Словарь с данными:
        {
            "funding_rate": float,  # Ставка фандинга (например, 0.0001 = 0.01%)
            "next_funding_time": int,  # Timestamp следующей выплаты
        }
        или None если ошибка
    """
    exchange = bot.exchanges.get(exchange_name)
    if not exchange:
        return None

    try:
        async with sem:
            funding_info = await asyncio.wait_for(
                exchange.get_funding_info(coin),
                timeout=REQ_TIMEOUT_SEC
            )
        return funding_info
    except asyncio.TimeoutError:
        logger.info(f"Timeout: {exchange_name} {coin} funding > {REQ_TIMEOUT_SEC:.1f}s")
        return None
    except Exception as e:
        logger.info(f"Fetch error: {exchange_name} {coin} funding: {e}")
        return None


async def fetch_ticker_info(
    bot: PerpArbitrageBot,
    exchange_name: str,
    coin: str,
    sem: asyncio.Semaphore,
) -> Optional[Dict[str, Any]]:
    """
    Запрос информации о тикере с ограничением параллелизма через семафор.
    
    Returns:
        Словарь с данными:
        {
            "bid": float,  # Лучшая цена покупки
            "ask": float,  # Лучшая цена продажи
            "price": float,  # Текущая цена
        }
        или None если ошибка
    """
    exchange = bot.exchanges.get(exchange_name)
    if not exchange:
        return None

    try:
        async with sem:
            ticker_info = await asyncio.wait_for(
                exchange.get_futures_ticker(coin),
                timeout=REQ_TIMEOUT_SEC
            )
        return ticker_info
    except asyncio.TimeoutError:
        logger.debug(f"Timeout: {exchange_name} {coin} ticker > {REQ_TIMEOUT_SEC:.1f}s")
        return None
    except Exception as e:
        logger.debug(f"Fetch error: {exchange_name} {coin} ticker: {e}")
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


async def process_coin(
    bot: PerpArbitrageBot,
    exchange_name: str,
    coin: str,
    sem: asyncio.Semaphore,
    telegram: Optional[TelegramSender] = None,
    channel_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Обрабатывает одну монету: запрашивает информацию о фандинге и проверяет условие.
    Отправляет уведомление в Telegram сразу после нахождения возможности.
    
    Returns:
        Словарь с данными о найденной возможности или None
    """
    funding_info = await fetch_funding_info(bot, exchange_name, coin, sem)
    
    if not funding_info:
        logger.debug(f"💲 {coin} {exchange_name} | Фандинг: N/A (funding_info is None)")
        return None
    
    funding_rate = funding_info.get("funding_rate")
    if funding_rate is None:
        logger.debug(f"💲 {coin} {exchange_name} | Фандинг: N/A (funding_rate is None)")
        return None
    
    # Проверяем условие: фандинг >= MIN_FUNDING_SPREAD
    # MIN_FUNDING_SPREAD обычно отрицательный (например, -1)
    # Если MIN_FUNDING_SPREAD = -1, то ищем фандинги <= -1 (т.е. -1.1, -1.2 и т.д.)
    # Это означает, что мы ищем фандинги, которые более отрицательные или равны MIN_FUNDING_SPREAD
    funding_rate_pct = funding_rate * 100  # Конвертируем в проценты
    
    # Если MIN_FUNDING_SPREAD отрицательный, ищем фандинги <= MIN_FUNDING_SPREAD (более отрицательные)
    # Например, если MIN_FUNDING_SPREAD = -1, то ищем фандинги <= -1 (т.е. -1.1, -1.2 и т.д.)
    # Если MIN_FUNDING_SPREAD положительный, ищем фандинги >= MIN_FUNDING_SPREAD
    if MIN_FUNDING_SPREAD < 0:
        # Для отрицательных порогов: ищем фандинги <= MIN_FUNDING_SPREAD (более отрицательные)
        if funding_rate_pct > MIN_FUNDING_SPREAD:
            return None
    else:
        # Для положительных порогов: ищем фандинги >= MIN_FUNDING_SPREAD
        if funding_rate_pct < MIN_FUNDING_SPREAD:
            return None
    
    # Вычисляем время до следующей выплаты
    next_funding_time = funding_info.get("next_funding_time")
    minutes_until = calculate_minutes_until_funding(next_funding_time, exchange_name)
    
    # Проверка ликвидности (опционально, можно добавить позже)
    # Пока считаем, что если фандинг найден, то это возможность для арбитража
    ok = True  # Можно добавить проверку ликвидности здесь
    
    verdict = "✅ арбитражить" if ok else "❌ не арбитражить"
    
    # Получаем цену монеты для расчета минимального количества монет для шорт ордера
    ticker_info = await fetch_ticker_info(bot, exchange_name, coin, sem)
    min_coins_short = None
    if ticker_info and ticker_info.get("bid") is not None:
        bid_price = ticker_info.get("bid")
        if bid_price and bid_price > 0:
            # Вычисляем минимальное количество монет с учетом ограничений биржи (minOrderQty, minOrderAmt, qtyStep)
            min_coins_short = await calculate_min_qty_for_exchange(
                bot, exchange_name, coin, bid_price, SCAN_COIN_INVEST
            )
    
    # Формируем строку с минимальным количеством монет для шорт ордера
    coins_info = ""
    if ok and min_coins_short is not None:
        coins_info = f" (min short: {min_coins_short:.3f} {coin})"
    
    # Логируем найденную возможность
    minutes_str = f"{minutes_until} мин" if minutes_until is not None else "N/A"
    logger.info(
        f"💲 {coin} {exchange_name} | Фандинг: {funding_rate_pct:.3f}% | "
        f"Время выплаты: {minutes_str} {verdict}{coins_info}"
    )
    
    if ok:
        opportunity = {
            "coin": coin,
            "exchange": exchange_name,
            "funding_rate": funding_rate,
            "funding_rate_pct": funding_rate_pct,
            "next_funding_time": next_funding_time,
            "minutes_until": minutes_until,
        }
        
        # Отправляем уведомление в Telegram только если время до выплаты < SCAN_FUNDING_MIN_TIME_TO_PAY
        # Если minutes_until is None или >= SCAN_FUNDING_MIN_TIME_TO_PAY, не отправляем
        should_send_telegram = False
        if minutes_until is not None:
            if minutes_until < SCAN_FUNDING_MIN_TIME_TO_PAY:
                should_send_telegram = True
        # Если minutes_until is None, не отправляем (неизвестно время до выплаты)
        
        if should_send_telegram and telegram and telegram.enabled and channel_id:
            try:
                message = format_telegram_message(opportunity)
                await telegram.send_message(message, channel_id=channel_id)
                logger.debug(f"📱 Отправлено сообщение в Telegram для {coin} {exchange_name} (время до выплаты: {minutes_until} мин)")
            except Exception as e:
                logger.warning(f"Ошибка отправки сообщения в Telegram для {coin} {exchange_name}: {e}", exc_info=True)
        elif minutes_until is not None and minutes_until >= SCAN_FUNDING_MIN_TIME_TO_PAY:
            logger.debug(f"⏭️ Пропущена отправка в Telegram для {coin} {exchange_name} (время до выплаты {minutes_until} мин >= {SCAN_FUNDING_MIN_TIME_TO_PAY} мин)")
        
        return opportunity
    
    return None


def format_telegram_message(opportunity: Dict[str, Any]) -> str:
    """
    Форматирует сообщение для Telegram.
    
    Args:
        opportunity: Словарь с данными о найденной возможности
        
    Returns:
        Отформатированное сообщение для Telegram
    """
    coin = opportunity["coin"]
    exchange = opportunity["exchange"]
    funding_rate_pct = opportunity["funding_rate_pct"]
    minutes_until = opportunity["minutes_until"]
    
    minutes_str = f"{minutes_until} min" if minutes_until is not None else "N/A"
    
    lines = [
        f"🔔💲 {exchange} {coin}",
        f"funding: {funding_rate_pct:.3f}%",
        f"time to pay: {minutes_str}",
    ]
    
    return "\n".join(lines)


async def scan_once(
    bot: PerpArbitrageBot,
    exchanges: List[str],
    coins_by_exchange: Dict[str, Set[str]],
    sem: asyncio.Semaphore,
) -> None:
    """
    Один проход по всем монетам батчами.
    
    Обрабатывает монеты параллельно батчами размера COIN_BATCH_SIZE.
    Уведомления в Telegram отправляются сразу после нахождения каждой возможности.
    """
    # Инициализируем Telegram один раз для всех монет
    telegram = TelegramSender()
    channel_id = config.TEST_CHANNEL_ID if telegram.enabled else None
    
    opportunities: List[Dict[str, Any]] = []
    
    for exchange_name in exchanges:
        coins = coins_by_exchange.get(exchange_name, set())
        if not coins:
            continue
        
        coins_list = sorted(list(coins))
        total = len(coins_list)
        
        for i in range(0, total, COIN_BATCH_SIZE):
            batch = coins_list[i:i + COIN_BATCH_SIZE]
            results = await asyncio.gather(
                *(process_coin(bot, exchange_name, coin, sem, telegram, channel_id) for coin in batch),
                return_exceptions=True
            )
            
            for result in results:
                if isinstance(result, Exception):
                    continue
                if result is not None:
                    opportunities.append(result)
            
            logger.debug(f"Progress {exchange_name}: {min(i + COIN_BATCH_SIZE, total)}/{total} coins processed")


async def main():
    bot = PerpArbitrageBot()
    try:
        exchanges = [ex for ex in FUNDING_EXCHANGES if ex in bot.exchanges and ex not in EXCLUDE_EXCHANGES]
        
        if not exchanges:
            logger.error("Нет доступных бирж для сканирования фандингов")
            return
        
        # Создаем семафор для ограничения параллелизма
        sem = asyncio.Semaphore(MAX_CONCURRENCY)
        
        # Логируем режим работы и настройки Telegram
        telegram = TelegramSender()
        telegram_status = "enabled" if telegram.enabled else "disabled"
        channel_info = f"channel={config.TEST_CHANNEL_ID or 'not set'}"
        
        exclude_coins_info = f"exclude_coins={sorted(EXCLUDE_COINS)}" if EXCLUDE_COINS else "exclude_coins=none"
        logger.info(
            f"scan_fundings started | mode={config.ENV_MODE} | MIN_FUNDING_SPREAD={MIN_FUNDING_SPREAD:.2f}% | "
            f"MIN_TIME_TO_PAY={SCAN_FUNDING_MIN_TIME_TO_PAY:.0f} мин | "
            f"interval={SCAN_INTERVAL_SEC}s | exchanges={exchanges} | "
            f"max_concurrency={MAX_CONCURRENCY} | timeout={REQ_TIMEOUT_SEC:.1f}s | "
            f"telegram={telegram_status} | {channel_info} | {exclude_coins_info}"
        )
        
        printed_stats = False
        while True:
            # Перед каждым глобальным циклом обновляем список монет по биржам
            coins_by_exchange = await collect_coins_by_exchange(bot, exchanges)
            
            # Статистика по монетам (один раз в начале запуска)
            if not printed_stats:
                total_coins = sum(len(coins) for coins in coins_by_exchange.values())
                logger.info(f"Всего монет (union по биржам): {total_coins}")
                for ex in exchanges:
                    logger.info(f"{ex}: {len(coins_by_exchange.get(ex, set()))} монет")
                printed_stats = True
            
            logger.info(f"🔄 Новый цикл поиска фандингов | exchanges={exchanges}")
            t0 = time.perf_counter()
            
            await scan_once(bot, exchanges, coins_by_exchange, sem)
            
            dt = time.perf_counter() - t0
            logger.info(f"scan_once finished in {dt:.1f}s; sleeping {SCAN_INTERVAL_SEC:.1f}s")
            await asyncio.sleep(SCAN_INTERVAL_SEC)
    
    except KeyboardInterrupt:
        logger.info("scan_fundings stopped by user")
    finally:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())

