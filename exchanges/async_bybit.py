"""
Асинхронная реализация Bybit для фьючерсов на базе AsyncBaseExchange
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
            params = {"category": "linear", "symbol": symbol}
            data = await self._request_json("GET", url, params=params)
            
            if not data:
                logger.warning(f"Bybit: не удалось получить тикер для {coin}")
                return None
            
            ret_code = data.get("retCode")
            if ret_code != 0:
                ret_msg = data.get("retMsg", "Unknown error")
                logger.warning(f"Bybit: API вернул ошибку для {coin}: retCode={ret_code}, retMsg={ret_msg}")
                return None
            
            result = data.get("result", {})
            list_data = result.get("list", [])
            
            if not list_data:
                logger.warning(f"Bybit: тикер для {coin} не найден")
                return None
            
            item = list_data[0]
            
            # Извлекаем цены
            last_price_raw = item.get("lastPrice")
            bid_raw = item.get("bid1Price")
            ask_raw = item.get("ask1Price")
            
            if not last_price_raw:
                logger.warning(f"Bybit: нет цены для {coin}")
                return None
            
            try:
                price = float(last_price_raw)
                bid = float(bid_raw) if bid_raw else price
                ask = float(ask_raw) if ask_raw else price
                
                return {
                    "price": price,
                    "bid": bid,
                    "ask": ask,
                }
            except (ValueError, TypeError) as e:
                logger.warning(f"Bybit: ошибка парсинга цен для {coin}: {e}")
                return None
                
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
            params = {"category": "linear", "symbol": symbol, "limit": 1}
            data = await self._request_json("GET", url, params=params)
            
            if not data:
                logger.warning(f"Bybit: не удалось получить фандинг для {coin}")
                return None
            
            ret_code = data.get("retCode")
            if ret_code != 0:
                ret_msg = data.get("retMsg", "Unknown error")
                logger.warning(f"Bybit: API вернул ошибку для фандинга {coin}: retCode={ret_code}, retMsg={ret_msg}")
                return None
            
            result = data.get("result", {})
            list_data = result.get("list", [])
            
            if not list_data:
                logger.warning(f"Bybit: фандинг для {coin} не найден")
                return None
            
            item = list_data[0]
            funding_rate_raw = item.get("fundingRate")
            
            if funding_rate_raw is None:
                logger.warning(f"Bybit: нет ставки фандинга для {coin}")
                return None
            
            try:
                funding_rate = float(funding_rate_raw)
                return funding_rate
            except (ValueError, TypeError) as e:
                logger.warning(f"Bybit: ошибка парсинга фандинга для {coin}: {e}")
                return None
                
        except Exception as e:
            logger.error(f"Bybit: ошибка при получении фандинга для {coin}: {e}", exc_info=True)
            return None

