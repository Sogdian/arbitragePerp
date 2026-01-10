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


async def fetch(bot: PerpArbitrageBot, ex: str, coin: str) -> Optional[Dict[str, Any]]:
    return await bot.get_futures_data(ex, coin)


async def collect_coins_map(bot: PerpArbitrageBot, exchanges: List[str]) -> Dict[str, Set[str]]:
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
            out[ex] = res  # res уже является Set[str]
    
    return out


async def scan_once(
    bot: PerpArbitrageBot,
    exchanges: List[str],
    coins_map: Dict[str, Set[str]],
) -> List[Tuple[str, str, str, float]]:
    """
    Возвращает список найденных возможностей в виде:
    (coin, long_exchange, short_exchange, open_spread_pct)
    
    Сканирует по парам бирж, используя только общие монеты (intersection).
    """
    found: List[Tuple[str, str, str, float]] = []

    # для каждой пары бирж
    for ex1, ex2 in combinations(exchanges, 2):
        s1 = coins_map.get(ex1) or set()
        s2 = coins_map.get(ex2) or set()
        common = s1 & s2  # intersection - только общие монеты
        
        if not common:
            continue

        # сканируем только общие монеты
        for coin in common:
            d1_task = asyncio.create_task(fetch(bot, ex1, coin))
            d2_task = asyncio.create_task(fetch(bot, ex2, coin))
            d1, d2 = await asyncio.gather(d1_task, d2_task, return_exceptions=True)
            
            if isinstance(d1, Exception) or isinstance(d2, Exception):
                continue
            if not d1 or not d2:
                continue

            # нужны bid/ask с обеих сторон
            if d1.get("ask") is None or d1.get("bid") is None:
                continue
            if d2.get("ask") is None or d2.get("bid") is None:
                continue

            # LONG ex1 / SHORT ex2
            sp12 = calc_open_spread_pct(d1["ask"], d2["bid"])
            if sp12 is not None and sp12 >= MIN_SPREAD:
                found.append((coin, ex1, ex2, sp12))

            # LONG ex2 / SHORT ex1
            sp21 = calc_open_spread_pct(d2["ask"], d1["bid"])
            if sp21 is not None and sp21 >= MIN_SPREAD:
                found.append((coin, ex2, ex1, sp21))

    # сортируем по спреду (desc) чтобы в лог шло от лучшего
    found.sort(key=lambda x: x[3], reverse=True)
    return found


async def main():
    bot = PerpArbitrageBot()
    try:
        exchanges = [ex for ex in bot.exchanges.keys() if ex not in EXCLUDE_EXCHANGES]
        
        # Собираем карту монет для каждой биржи
        coins_map = await collect_coins_map(bot, exchanges)
        
        # Статистика
        total_union = set().union(*coins_map.values()) if coins_map else set()
        logger.info(f"Всего монет (union по биржам): {len(total_union)}")
        
        empty = [ex for ex, s in coins_map.items() if not s]
        if empty:
            logger.warning(f"Биржи без списка монет (будут фактически пропущены): {empty}")
        
        # Логируем количество монет на каждой бирже
        for ex, coins_set in coins_map.items():
            logger.info(f"{ex}: {len(coins_set)} монет")

        logger.info(
            f"scan_spreads started | MIN_SPREAD={MIN_SPREAD:.2f}% | interval={SCAN_INTERVAL_SEC}s | "
            f"exchanges={exchanges} | total_coins={len(total_union)}"
        )

        while True:
            found = await scan_once(bot, exchanges, coins_map)

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

