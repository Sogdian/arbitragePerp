"""
Бот для арбитража фьючерсов между биржами
"""
import asyncio
import logging
import sys
from typing import Optional, Dict
from exchanges.async_bybit import AsyncBybitExchange
from exchanges.async_gate import AsyncGateExchange
from input_parser import parse_input
import config

# Настройка логирования
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(config.LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class PerpArbitrageBot:
    """Бот для анализа арбитража фьючерсов"""
    
    def __init__(self):
        self.bybit = AsyncBybitExchange()
        self.gate = AsyncGateExchange()
        self.exchanges = {
            "bybit": self.bybit,
            "gate": self.gate
        }
    
    async def close(self):
        """Закрывает соединения с биржами"""
        await asyncio.gather(
            self.bybit.close(),
            self.gate.close(),
            return_exceptions=True
        )
    
    async def get_futures_data(self, exchange_name: str, coin: str) -> Optional[Dict]:
        """
        Получить данные о фьючерсе (цена и фандинг) для монеты на бирже
        
        Args:
            exchange_name: Название биржи ("bybit" или "gate")
            coin: Название монеты (например, "CVC")
            
        Returns:
            Словарь с данными:
            {
                "price": float,
                "bid": float,
                "ask": float,
                "funding_rate": float
            }
            или None если ошибка
        """
        exchange = self.exchanges.get(exchange_name)
        if not exchange:
            logger.error(f"Неизвестная биржа: {exchange_name}")
            return None
        
        # Получаем тикер и фандинг параллельно
        ticker_task = exchange.get_futures_ticker(coin)
        funding_task = exchange.get_funding_rate(coin)
        
        ticker, funding_rate = await asyncio.gather(
            ticker_task,
            funding_task,
            return_exceptions=True
        )
        
        if isinstance(ticker, Exception):
            logger.error(f"{exchange_name}: ошибка при получении тикера для {coin}: {ticker}")
            ticker = None
        
        if isinstance(funding_rate, Exception):
            logger.error(f"{exchange_name}: ошибка при получении фандинга для {coin}: {funding_rate}")
            funding_rate = None
        
        if not ticker:
            return None
        
        return {
            "price": ticker.get("price"),
            "bid": ticker.get("bid"),
            "ask": ticker.get("ask"),
            "funding_rate": funding_rate
        }
    
    def calculate_spread(self, price1: Optional[float], price2: Optional[float]) -> Optional[float]:
        """
        Вычислить спред между двумя ценами (в процентах)
        
        Args:
            price1: Первая цена
            price2: Вторая цена
            
        Returns:
            Спред в процентах или None если невозможно вычислить
        """
        if price1 is None or price2 is None:
            return None
        
        if price2 == 0:
            return None
        
        spread = ((price1 - price2) / price2) * 100
        return spread
    
    def calculate_funding_spread(self, funding1: Optional[float], funding2: Optional[float]) -> Optional[float]:
        """
        Вычислить спред между двумя ставками фандинга (в процентах)
        
        Args:
            funding1: Первая ставка фандинга
            funding2: Вторая ставка фандинга
            
        Returns:
            Разница в процентах или None если невозможно вычислить
        """
        if funding1 is None or funding2 is None:
            return None
        
        # Конвертируем в проценты (funding rate обычно в формате 0.0001 = 0.01%)
        funding1_pct = funding1 * 100
        funding2_pct = funding2 * 100
        
        spread = funding1_pct - funding2_pct
        return spread
    
    async def process_input(self, input_text: str):
        """
        Обработать вводные данные и вывести информацию о фьючерсах и фандингах
        
        Args:
            input_text: Строка с вводными данными (например, "CVC Long (bybit), Short (gate)")
        """
        # Парсим вводные данные
        parsed = parse_input(input_text)
        if not parsed:
            logger.error("Не удалось распарсить вводные данные")
            return
        
        coin = parsed["coin"]
        long_exchange = parsed["long_exchange"]
        short_exchange = parsed["short_exchange"]
        
        logger.info(f"Обработка арбитража для {coin}: Long на {long_exchange}, Short на {short_exchange}")
        
        # Получаем данные с обеих бирж параллельно
        long_data_task = self.get_futures_data(long_exchange, coin)
        short_data_task = self.get_futures_data(short_exchange, coin)
        
        long_data, short_data = await asyncio.gather(
            long_data_task,
            short_data_task,
            return_exceptions=True
        )
        
        if isinstance(long_data, Exception):
            logger.error(f"Ошибка при получении данных с {long_exchange}: {long_data}")
            long_data = None
        
        if isinstance(short_data, Exception):
            logger.error(f"Ошибка при получении данных с {short_exchange}: {short_data}")
            short_data = None
        
        # Выводим информацию
        logger.info("=" * 60)
        logger.info(f"Анализ арбитража для {coin}")
        logger.info("=" * 60)
        
        # Данные Long биржи
        if long_data:
            price_long = long_data.get("price")
            funding_long = long_data.get("funding_rate")
            
            logger.info(f"({long_exchange}) Цена монеты на фьючерс ({coin}): {price_long}")
            
            if funding_long is not None:
                funding_long_pct = funding_long * 100
                logger.info(f"({long_exchange}) Фандинг ({coin}): {funding_long_pct:.6f}%")
            else:
                logger.info(f"({long_exchange}) Фандинг ({coin}): недоступно")
        else:
            logger.error(f"Не удалось получить данные с {long_exchange}")
            price_long = None
            funding_long = None
        
        # Данные Short биржи
        if short_data:
            price_short = short_data.get("price")
            funding_short = short_data.get("funding_rate")
            
            logger.info(f"({short_exchange}) Цена монеты на фьючерс ({coin}): {price_short}")
            
            if funding_short is not None:
                funding_short_pct = funding_short * 100
                logger.info(f"({short_exchange}) Фандинг ({coin}): {funding_short_pct:.6f}%")
            else:
                logger.info(f"({short_exchange}) Фандинг ({coin}): недоступно")
        else:
            logger.error(f"Не удалось получить данные с {short_exchange}")
            price_short = None
            funding_short = None
        
        # Вычисляем спреды
        price_spread = None
        if price_long is not None and price_short is not None:
            price_spread = self.calculate_spread(price_long, price_short)
            if price_spread is not None:
                logger.info(f"({long_exchange} и {short_exchange}) Спред на цену на фьючерс: {price_spread:.4f}%")
            else:
                logger.info(f"({long_exchange} и {short_exchange}) Спред на цену на фьючерс: невозможно вычислить")
        else:
            logger.info(f"({long_exchange} и {short_exchange}) Спред на цену на фьючерс: недоступно")
        
        if funding_long is not None and funding_short is not None:
            funding_spread = self.calculate_funding_spread(funding_long, funding_short)
            if funding_spread is not None:
                logger.info(f"({long_exchange} и {short_exchange}) Спред на фандинги: {funding_spread:.6f}%")
            else:
                logger.info(f"({long_exchange} и {short_exchange}) Спред на фандинги: невозможно вычислить")
        else:
            logger.info(f"({long_exchange} и {short_exchange}) Спред на фандинги: недоступно")
            funding_spread = None
        
        # Вычисляем общий спред (сумма спреда на цену и спреда на фандинги)
        if price_spread is not None and funding_spread is not None:
            total_spread = price_spread + funding_spread
            logger.info(f"({long_exchange} и {short_exchange}) Общий спред: {total_spread:.6f}%")
        else:
            logger.info(f"({long_exchange} и {short_exchange}) Общий спред: невозможно вычислить")
        
        logger.info("=" * 60)


async def main():
    """Главная функция"""
    bot = PerpArbitrageBot()
    
    try:
        # Читаем вводные данные из командной строки или stdin
        if len(sys.argv) > 1:
            # Вводные данные переданы как аргумент командной строки
            input_text = " ".join(sys.argv[1:])
        else:
            # Читаем из stdin
            print("Введите данные в формате: 'монета Long (биржа), Short (биржа)'")
            print("Пример: CVC Long (bybit), Short (gate)")
            input_text = input().strip()
        
        if not input_text:
            logger.error("Не указаны вводные данные")
            return
        
        await bot.process_input(input_text)
        
    except KeyboardInterrupt:
        logger.info("Прервано пользователем")
    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
    finally:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())

