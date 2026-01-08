"""
Асинхронная реализация Gate.io для фьючерсов на базе AsyncBaseExchange
"""
from typing import Dict, Optional
import logging
from .async_base_exchange import AsyncBaseExchange

logger = logging.getLogger(__name__)


class AsyncGateExchange(AsyncBaseExchange):
    BASE_URL = "https://api.gateio.ws"

    def __init__(self):
        super().__init__("Gate")

    def _normalize_symbol(self, coin: str) -> str:
        """Преобразует монету в формат Gate.io для фьючерсов (например, CVC -> CVC_USDT)"""
        return f"{coin.upper()}_USDT"

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
            url = "/api/v4/futures/usdt/tickers"
            params = {"contract": symbol}
            data = await self._request_json("GET", url, params=params)
            
            if not data:
                logger.warning(f"Gate: не удалось получить тикер для {coin}")
                return None
            
            # Gate.io возвращает список или словарь
            item = None
            if isinstance(data, list) and data:
                item = data[0]
            elif isinstance(data, dict):
                item = data
            
            if not item:
                logger.warning(f"Gate: тикер для {coin} не найден")
                return None
            
            # Извлекаем цены
            last_price_raw = item.get("last")
            bid_raw = item.get("bid")
            ask_raw = item.get("ask")
            
            if not last_price_raw:
                logger.warning(f"Gate: нет цены для {coin}")
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
                logger.warning(f"Gate: ошибка парсинга цен для {coin}: {e}")
                return None
                
        except Exception as e:
            logger.error(f"Gate: ошибка при получении тикера для {coin}: {e}", exc_info=True)
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
            # Gate.io использует эндпоинт для получения текущей ставки фандинга
            url = "/api/v4/futures/usdt/funding_rate"
            params = {"contract": symbol}
            data = await self._request_json("GET", url, params=params)
            
            if not data:
                logger.warning(f"Gate: не удалось получить фандинг для {coin}")
                return None
            
            # Gate.io возвращает список или словарь
            item = None
            if isinstance(data, list) and data:
                item = data[0]
            elif isinstance(data, dict):
                # Если это словарь, проверяем наличие поля "result"
                if "result" in data:
                    result = data["result"]
                    if isinstance(result, list) and result:
                        item = result[0]
                    elif isinstance(result, dict):
                        item = result
                else:
                    item = data
            
            if not item:
                logger.warning(f"Gate: фандинг для {coin} не найден")
                return None
            
            # Gate.io может возвращать ставку в разных полях
            funding_rate_raw = item.get("r") or item.get("funding_rate") or item.get("rate")
            
            if funding_rate_raw is None:
                logger.warning(f"Gate: нет ставки фандинга для {coin} в ответе: {item}")
                return None
            
            try:
                funding_rate = float(funding_rate_raw)
                return funding_rate
            except (ValueError, TypeError) as e:
                logger.warning(f"Gate: ошибка парсинга фандинга для {coin}: {e}")
                return None
                
        except Exception as e:
            logger.error(f"Gate: ошибка при получении фандинга для {coin}: {e}", exc_info=True)
            return None

