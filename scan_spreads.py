import asyncio
import logging
import os
import sys
import time
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
REQ_TIMEOUT_SEC = float(os.getenv("SCAN_REQ_TIMEOUT_SEC", "12"))  # таймаут на запрос к бирже (8-12 норм)
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
    """Запрос данных с ограничением параллелизма через семафор и таймаутом"""
    async with sem:
        try:
            return await asyncio.wait_for(bot.get_futures_data(ex, coin), timeout=REQ_TIMEOUT_SEC)
        except asyncio.TimeoutError:
            logger.warning(f"Timeout: {ex} {coin} get_futures_data > {REQ_TIMEOUT_SEC:.1f}s")
            return None
        except Exception as e:
            logger.warning(f"Fetch error: {ex} {coin}: {e}", exc_info=True)
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
            out[ex] = set(res)  # res уже является Set[str]
    
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
) -> None:
    """
    Обрабатывает одну монету: запрашивает данные со всех бирж, вычисляет спреды и логирует находки.
    """
    tasks = {ex: asyncio.create_task(fetch(bot, ex, coin, sem)) for ex in exchanges}
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
        for long_ex, short_ex, spread in per_coin_found:
            logger.info(f'{coin} Long ({long_ex}), Short ({short_ex}) spread {spread:.4f}%')


async def scan_once(
    bot: PerpArbitrageBot,
    exchanges: List[str],
    coins: List[str],
    sem: asyncio.Semaphore,
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
            *(process_coin(bot, exchanges, coin, sem) for coin in batch),
            return_exceptions=True
        )
        logger.info(f"Progress: {min(i + COIN_BATCH_SIZE, total)}/{total} coins processed")


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
            f"max_concurrency={MAX_CONCURRENCY} | timeout={REQ_TIMEOUT_SEC:.1f}s"
        )

        while True:
            t0 = time.perf_counter()
            await scan_once(bot, exchanges, coins, sem)
            dt = time.perf_counter() - t0
            logger.info(f"scan_once finished in {dt:.1f}s; sleeping {SCAN_INTERVAL_SEC:.1f}s")
            await asyncio.sleep(SCAN_INTERVAL_SEC)

    except KeyboardInterrupt:
        logger.info("scan_spreads stopped by user")
    finally:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())

