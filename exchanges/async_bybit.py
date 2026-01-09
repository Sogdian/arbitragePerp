"""
Асинхронная реализация Bybit для фьючерсов на базе AsyncBaseExchange

Фиксированная логика:
- Рынок: Derivatives
- Тип: Perpetual
- Категория: linear (только USDT пары)
- Символ: COINUSDT (без подчеркивания)
- USDC / inverse / альтернативные категории - НЕ поддерживаются
- Если COINUSDT не найден → считаем, что инструмента нет
"""
from typing import Dict, Optional
import logging
from .async_base_exchange import AsyncBaseExchange

logger = logging.getLogger(__name__)


class AsyncBybitExchange(AsyncBaseExchange):
    BASE_URL = "https://api.bybit.com"

    def __init__(self, pool_limit: int = 100):
        super().__init__("Bybit", pool_limit)

    def _normalize_symbol(self, coin: str) -> str:
        """Преобразует монету в формат Bybit для фьючерсов (например, CVC -> CVCUSDT)"""
        return f"{coin.upper()}USDT"

    async def get_futures_ticker(self, coin: str) -> Optional[Dict]:
        """
        Получить тикер фьючерса для монеты
        
        Args:
            coin: Название монеты без /USDT (например, "CVC")
            
        Returns:
            Словарь с данными тикера:
            {
                "price": float,  # Текущая цена
                "bid": float,     # Лучшая цена покупки
                "ask": float,     # Лучшая цена продажи
            }
            или None если ошибка
        """
        try:
            symbol = self._normalize_symbol(coin)
            url = "/v5/market/tickers"
            params = {
                "category": "linear",
                "symbol": symbol,
            }
            
            data = await self._request_json("GET", url, params=params)
            if not data or data.get("retCode") != 0:
                logger.warning(f"Bybit: тикер для {coin} не найден")
                return None
            
            items = (data.get("result") or {}).get("list") or []
            if not items:
                logger.warning(f"Bybit: тикер для {coin} не найден")
                return None
            
            item = items[0]
            last_price = item.get("lastPrice")
            if last_price is None:
                return None
            
            return {
                "price": float(last_price),
                "bid": float(item.get("bid1Price", last_price)),
                "ask": float(item.get("ask1Price", last_price)),
            }
                
        except Exception as e:
            logger.error(f"Bybit: ошибка при получении тикера для {coin}: {e}", exc_info=True)
            return None

    async def get_funding_rate(self, coin: str) -> Optional[float]:
        """
        Получить текущую ставку фандинга для монеты
        
        Args:
            coin: Название монеты без /USDT (например, "CVC")
            
        Returns:
            Ставка фандинга (например, 0.0001 = 0.01%) или None если ошибка
        """
        try:
            symbol = self._normalize_symbol(coin)
            url = "/v5/market/funding/history"
            params = {
                "category": "linear",
                "symbol": symbol,
                "limit": 1,
            }
            
            data = await self._request_json("GET", url, params=params)
            if not data or data.get("retCode") != 0:
                logger.warning(f"Bybit: фандинг для {coin} не найден")
                return None
            
            items = (data.get("result") or {}).get("list") or []
            if not items:
                logger.warning(f"Bybit: фандинг для {coin} не найден")
                return None
            
            funding_rate_raw = items[0].get("fundingRate")
            if funding_rate_raw is None:
                return None
            
            return float(funding_rate_raw)
                
        except Exception as e:
            logger.error(f"Bybit: ошибка при получении фандинга для {coin}: {e}", exc_info=True)
            return None
    
    async def get_orderbook(self, coin: str, limit: int = 50) -> Optional[Dict]:
        """
        Получить книгу заявок (orderbook) для монеты
        
        Args:
            coin: Название монеты без /USDT (например, "GPS")
            limit: Количество уровней (по умолчанию 50)
            
        Returns:
            Словарь с данными книги заявок:
            {
                "bids": [[price, size], ...],  # Заявки на покупку
                "asks": [[price, size], ...]    # Заявки на продажу
            }
            или None если ошибка
        """
        try:
            symbol = self._normalize_symbol(coin)
            url = "/v5/market/orderbook"
            params = {
                "category": "linear",
                "symbol": symbol,
                "limit": limit,
            }
            
            data = await self._request_json("GET", url, params=params)
            if not data or data.get("retCode") != 0:
                logger.warning(f"Bybit: orderbook error for {coin}: {data}")
                return None
            
            result = data.get("result") or {}
            bids = result.get("b") or []  # [[price, size], ...]
            asks = result.get("a") or []
            
            if not bids or not asks:
                return None
            
            return {"bids": bids, "asks": asks}
                
        except Exception as e:
            logger.error(f"Bybit: ошибка при получении orderbook для {coin}: {e}", exc_info=True)
            return None
    

