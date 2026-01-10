import asyncio
import logging
import os
import sys
from itertools import combinations
from typing import Any, Dict, Optional, List, Tuple, Set

from bot import PerpArbitrageBot


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
EXCLUDE_EXCHANGES = {"lbank"}  # не использовать

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


async def fetch(bot: PerpArbitrageBot, ex: str, coin: str, sem: asyncio.Semaphore) -> Optional[Dict[str, Any]]:
    """Запрос данных с ограничением параллелизма через семафор"""
    async with sem:
        return await bot.get_futures_data(ex, coin)


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
            out[ex] = set(res)  # res уже является Set[str]
    
    return out


def build_union(coins_by_exchange: Dict[str, Set[str]]) -> List[str]:
    """Строит union всех монет и возвращает отсортированный список"""
    sets = [s for s in coins_by_exchange.values() if s]
    if not sets:
        return []
    return sorted(set.union(*sets))


async def scan_coin(
    bot: PerpArbitrageBot,
    coin: str,
    coins_by_exchange: Dict[str, Set[str]],
    exchanges: List[str],
    sem: asyncio.Semaphore,
) -> List[Tuple[str, str, str, float]]:
    """
    Сканирует одну монету на всех биржах, где она реально есть.
    
    Returns:
        Список найденных возможностей: (coin, long_exchange, short_exchange, open_spread_pct)
    """
    # биржи где монета реально есть
    present = [ex for ex in exchanges if coin in coins_by_exchange.get(ex, set())]
    if len(present) < 2:
        return []

    # запрашиваем только present биржи
    tasks = {ex: asyncio.create_task(fetch(bot, ex, coin, sem)) for ex in present}
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    available: Dict[str, Dict[str, Any]] = {}
    for ex, res in zip(tasks.keys(), results):
        if isinstance(res, Exception) or not res:
            continue
        bid = res.get("bid")
        ask = res.get("ask")
        if bid is None or ask is None:
            continue
        available[ex] = res

    if len(available) < 2:
        return []

    found: List[Tuple[str, str, str, float]] = []
    for ex1, ex2 in combinations(available.keys(), 2):
        d1 = available[ex1]
        d2 = available[ex2]

        s1 = calc_open_spread_pct(d1["ask"], d2["bid"])
        if s1 is not None and s1 >= MIN_SPREAD:
            found.append((coin, ex1, ex2, s1))

        s2 = calc_open_spread_pct(d2["ask"], d1["bid"])
        if s2 is not None and s2 >= MIN_SPREAD:
            found.append((coin, ex2, ex1, s2))

    return found


async def scan_once(
    bot: PerpArbitrageBot,
    exchanges: List[str],
    coins: List[str],
    coins_by_exchange: Dict[str, Set[str]],
    sem: asyncio.Semaphore,
) -> List[Tuple[str, str, str, float]]:
    """
    Сканирует все монеты батчами с прогресс-логом.
    
    Returns:
        Список найденных возможностей: (coin, long_exchange, short_exchange, open_spread_pct)
    """
    found_all: List[Tuple[str, str, str, float]] = []

    total = len(coins)
    processed = 0

    for i in range(0, total, COIN_BATCH_SIZE):
        batch = coins[i:i + COIN_BATCH_SIZE]

        batch_tasks = [asyncio.create_task(scan_coin(bot, c, coins_by_exchange, exchanges, sem)) for c in batch]
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

        for res in batch_results:
            if isinstance(res, Exception) or not res:
                continue
            found_all.extend(res)

        processed += len(batch)

        # прогресс раз в пачку
        logger.info(f"scan progress: {processed}/{total} coins")

    found_all.sort(key=lambda x: x[3], reverse=True)
    return found_all


async def main():
    bot = PerpArbitrageBot()
    try:
        exchanges = [ex for ex in bot.exchanges.keys() if ex not in EXCLUDE_EXCHANGES]
        
        # Создаем семафор для ограничения параллелизма
        sem = asyncio.Semaphore(MAX_CONCURRENCY)
        
        # Собираем карту монет для каждой биржи
        coins_by_exchange = await collect_coins_by_exchange(bot, exchanges)
        coins = build_union(coins_by_exchange)
        
        logger.info(f"Всего монет (union по биржам): {len(coins)}")
        for ex in exchanges:
            logger.info(f"{ex}: {len(coins_by_exchange.get(ex, set()))} монет")

        logger.info(
            f"scan_spreads started | MIN_SPREAD={MIN_SPREAD:.2f}% | interval={SCAN_INTERVAL_SEC}s | "
            f"exchanges={exchanges} | total_coins={len(coins)} | "
            f"batch={COIN_BATCH_SIZE} | max_concurrency={MAX_CONCURRENCY}"
        )

        while True:
            found = await scan_once(bot, exchanges, coins, coins_by_exchange, sem)

            # Пишем в лог ТОЛЬКО если нашли
            if found:
                for coin, long_ex, short_ex, spread in found:
                    # Формат ровно под copy-paste в bot.py
                    # BTC Long (gate), Short (bingx) spread 2.34%
                    logger.info(f'{coin} Long ({long_ex}), Short ({short_ex}) spread {spread:.4f}%')

            await asyncio.sleep(SCAN_INTERVAL_SEC)

    except KeyboardInterrupt:
        logger.info("scan_spreads stopped by user")
    finally:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())

