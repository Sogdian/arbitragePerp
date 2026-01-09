"""
Асинхронная реализация MEXC для фьючерсов на базе AsyncBaseExchange
"""
from typing import Dict, Optional
import logging
from .async_base_exchange import AsyncBaseExchange

logger = logging.getLogger(__name__)


class AsyncMexcExchange(AsyncBaseExchange):
    BASE_URL = "https://contract.mexc.com"

    def __init__(self, pool_limit: int = 100):
        super().__init__("MEXC", pool_limit)

    def _normalize_symbol(self, coin: str) -> str:
        """Преобразует монету в формат MEXC для фьючерсов (например, CVC -> CVC_USDT)"""
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
            url = "/api/v1/contract/ticker"
            params = {"symbol": symbol}
            data = await self._request_json("GET", url, params=params)
            
            if not data:
                logger.warning(f"MEXC: не удалось получить тикер для {coin}")
                return None
            
            # MEXC может возвращать данные в разных форматах
            item = None
            if isinstance(data, dict):
                code = data.get("code")
                if code != 0:
                    msg = data.get("msg", "Unknown error")
                    logger.warning(f"MEXC: API вернул ошибку для {coin}: code={code}, msg={msg}")
                    return None
                
                item = data.get("data")
                # Если data - это список, берем первый элемент
                if isinstance(item, list) and item:
                    item = item[0]
            elif isinstance(data, list) and data:
                item = data[0]
            
            if not item:
                logger.warning(f"MEXC: тикер для {coin} не найден")
                return None
            
            # Проверяем, что item - это словарь
            if not isinstance(item, dict):
                logger.warning(f"MEXC: неожиданный формат данных для тикера {coin}: {type(item)}")
                return None
            
            # Извлекаем цены
            last_price_raw = item.get("lastPrice") or item.get("last")
            bid_raw = item.get("bid1") or item.get("bid")
            ask_raw = item.get("ask1") or item.get("ask")
            
            if not last_price_raw:
                logger.warning(f"MEXC: нет цены для {coin}")
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
                logger.warning(f"MEXC: ошибка парсинга цен для {coin}: {e}")
                return None
                
        except Exception as e:
            logger.error(f"MEXC: ошибка при получении тикера для {coin}: {e}", exc_info=True)
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
            url = "/api/v1/contract/funding_rate"
            params = {"symbol": symbol}
            data = await self._request_json("GET", url, params=params)
            
            if not data:
                logger.warning(f"MEXC: не удалось получить фандинг для {coin}")
                return None
            
            # MEXC может возвращать данные в разных форматах
            item = None
            if isinstance(data, dict):
                code = data.get("code")
                if code != 0:
                    msg = data.get("msg", "Unknown error")
                    logger.warning(f"MEXC: API вернул ошибку для фандинга {coin}: code={code}, msg={msg}")
                    return None
                
                item = data.get("data")
                # Если data - это список, ищем элемент с правильным символом
                if isinstance(item, list) and item:
                    # Ищем элемент с нужным символом
                    symbol_to_find = self._normalize_symbol(coin)
                    for elem in item:
                        if isinstance(elem, dict) and elem.get("symbol") == symbol_to_find:
                            item = elem
                            break
                    else:
                        # Если не нашли, берем первый элемент (fallback)
                        item = item[0]
                elif isinstance(item, dict):
                    # Проверяем, что символ совпадает
                    item_symbol = item.get("symbol")
                    symbol_to_find = self._normalize_symbol(coin)
                    if item_symbol and item_symbol != symbol_to_find:
                        logger.warning(f"MEXC: получен фандинг для {item_symbol} вместо {symbol_to_find}")
                        return None
            elif isinstance(data, list) and data:
                # Ищем элемент с правильным символом
                symbol_to_find = self._normalize_symbol(coin)
                for elem in data:
                    if isinstance(elem, dict) and elem.get("symbol") == symbol_to_find:
                        item = elem
                        break
                else:
                    # Если не нашли, берем первый элемент (fallback)
                    item = data[0]
            
            if not item:
                logger.warning(f"MEXC: фандинг для {coin} не найден")
                return None
            
            # Проверяем, что item - это словарь
            if not isinstance(item, dict):
                logger.warning(f"MEXC: неожиданный формат данных для фандинга {coin}: {type(item)}")
                return None
            
            # Проверяем, что символ совпадает
            item_symbol = item.get("symbol")
            symbol_to_find = self._normalize_symbol(coin)
            if item_symbol and item_symbol != symbol_to_find:
                logger.warning(f"MEXC: получен фандинг для {item_symbol} вместо {symbol_to_find}, пропускаем")
                return None
            
            # MEXC может возвращать ставку в разных полях
            funding_rate_raw = item.get("fundingRate") or item.get("rate") or item.get("r")
            
            if funding_rate_raw is None:
                logger.warning(f"MEXC: нет ставки фандинга для {coin} в ответе: {item}")
                return None
            
            try:
                funding_rate = float(funding_rate_raw)
                # MEXC возвращает фандинг уже в decimal формате (например, 0.000052 = 0.0052%)
                # как и другие биржи, поэтому просто возвращаем значение
                # bot.py потом умножит на 100 для отображения
                return funding_rate
            except (ValueError, TypeError) as e:
                logger.warning(f"MEXC: ошибка парсинга фандинга для {coin}: {e}")
                return None
                
        except Exception as e:
            logger.error(f"MEXC: ошибка при получении фандинга для {coin}: {e}", exc_info=True)
            return None

    async def get_orderbook(self, coin: str, limit: int = 50) -> Optional[Dict]:
        """
        Получить orderbook (книгу заявок) для монеты
        
        Args:
            coin: Название монеты без /USDT (например, "GPS")
            limit: Количество уровней (по умолчанию 50)
            
        Returns:
            Словарь с данными orderbook:
            {
                "bids": [[price, size], ...],  # Заявки на покупку (от высокой к низкой)
                "asks": [[price, size], ...],  # Заявки на продажу (от низкой к высокой)
            }
            или None если ошибка
        """
        try:
            symbol = self._normalize_symbol(coin)
            url = "/api/v1/contract/depth"
            params = {"symbol": symbol, "limit": limit}
            
            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"MEXC: не удалось получить orderbook для {coin}")
                return None
            
            # MEXC возвращает данные в поле "data" или напрямую
            if isinstance(data, dict):
                code = data.get("code")
                if code != 0:
                    msg = data.get("msg", "Unknown error")
                    logger.warning(f"MEXC: API вернул ошибку для orderbook {coin}: code={code}, msg={msg}")
                    return None
                
                result = data.get("data") or data
            else:
                result = data
            
            # MEXC может возвращать bids/asks как списки или словари
            if isinstance(result, dict):
                bids = result.get("bids", [])
                asks = result.get("asks", [])
            else:
                logger.warning(f"MEXC: неожиданный формат orderbook для {coin}")
                return None
            
            if not bids or not asks:
                logger.warning(f"MEXC: пустой orderbook для {coin}")
                return None
            
            return {"bids": bids, "asks": asks}
                
        except Exception as e:
            logger.error(f"MEXC: ошибка при получении orderbook для {coin}: {e}", exc_info=True)
            return None

