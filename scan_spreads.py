import asyncio
import logging
import os
import sys
from itertools import combinations
from typing import Any, Dict, Optional, List, Tuple

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


async def collect_all_coins(bot: PerpArbitrageBot, exchanges: List[str]) -> List[str]:
    """
    Собирает список всех монет со всех бирж и возвращает их объединение.
    
    Returns:
        Отсортированный список всех монет, доступных хотя бы на одной бирже
    """
    tasks = []
    for ex in exchanges:
        ex_obj = bot.exchanges[ex]
        tasks.append(ex_obj.get_all_futures_coins())
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    sets = []
    for ex, res in zip(exchanges, results):
        if isinstance(res, Exception) or not res:
            continue
        # res уже является Set[str]
        sets.append(res)
    
    # берем объединение всех монет со всех бирж
    if not sets:
        return []
    
    common = set.union(*sets)   # все монеты со всех бирж
    return sorted(common)


async def scan_once(bot: PerpArbitrageBot, exchanges: List[str], coins: List[str]) -> List[Tuple[str, str, str, float]]:
    """
    Возвращает список найденных возможностей в виде:
    (coin, long_exchange, short_exchange, open_spread_pct)
    """
    found: List[Tuple[str, str, str, float]] = []

    for coin in coins:
        # 1) Запрашиваем все биржи параллельно
        tasks = {ex: asyncio.create_task(fetch(bot, ex, coin)) for ex in exchanges}
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        ex_data: Dict[str, Optional[Dict[str, Any]]] = {}
        for ex, res in zip(tasks.keys(), results):
            if isinstance(res, Exception):
                ex_data[ex] = None
            else:
                ex_data[ex] = res

        # 2) Оставляем только где есть bid/ask
        available = {
            ex: d for ex, d in ex_data.items()
            if d and d.get("bid") is not None and d.get("ask") is not None
        }
        if len(available) < 2:
            continue

        # 3) Считаем пары и оба направления
        for ex1, ex2 in combinations(available.keys(), 2):
            d1 = available[ex1]
            d2 = available[ex2]

            # LONG ex1 / SHORT ex2
            s1 = calc_open_spread_pct(d1["ask"], d2["bid"])
            if s1 is not None and s1 >= MIN_SPREAD:
                found.append((coin, ex1, ex2, s1))

            # LONG ex2 / SHORT ex1
            s2 = calc_open_spread_pct(d2["ask"], d1["bid"])
            if s2 is not None and s2 >= MIN_SPREAD:
                found.append((coin, ex2, ex1, s2))

    # сортируем по спреду (desc) чтобы в лог шло от лучшего
    found.sort(key=lambda x: x[3], reverse=True)
    return found


async def main():
    bot = PerpArbitrageBot()
    try:
        exchanges = [ex for ex in bot.exchanges.keys() if ex not in EXCLUDE_EXCHANGES]
        
        # Собираем все монеты со всех бирж
        coins = await collect_all_coins(bot, exchanges)
        logger.info(f"Всего монет для сканирования: {len(coins)}")

        logger.info(
            f"scan_spreads started | MIN_SPREAD={MIN_SPREAD:.2f}% | interval={SCAN_INTERVAL_SEC}s | "
            f"exchanges={exchanges} | coins_count={len(coins)}"
        )

        while True:
            found = await scan_once(bot, exchanges, coins)

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

