"""
Асинхронная реализация XT.com для фьючерсов на базе AsyncBaseExchange

Фиксированная логика:
- Рынок: Futures
- Тип: Perpetual
- Символ: coin_usdt (нижний регистр, с подчеркиванием)
- Если coin_usdt не найден → считаем, что инструмента нет
"""
from typing import Dict, Optional
import logging
from .async_base_exchange import AsyncBaseExchange

logger = logging.getLogger(__name__)


class AsyncXtExchange(AsyncBaseExchange):
    BASE_URL = "https://fapi.xt.com"

    def __init__(self, pool_limit: int = 100):
        super().__init__("XT", pool_limit)

    def _normalize_symbol(self, coin: str) -> str:
        """Преобразует монету в формат XT для фьючерсов (например, CVC -> cvc_usdt)"""
        return f"{coin.lower()}_usdt"

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
            url = "/future/market/v1/public/q/ticker"
            params = {"symbol": symbol}
            
            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"XT: не удалось получить ответ для тикера {coin} (символ: {symbol})")
                return None
            
            return_code = data.get("returnCode")
            if return_code != 0:
                msg = data.get("msgInfo", "Unknown error")
                logger.warning(f"XT: API вернул ошибку для тикера {coin} (символ: {symbol}): returnCode={return_code}, msg={msg}")
                return None
            
            result = data.get("result")
            if not result or not isinstance(result, dict):
                logger.warning(f"XT: тикер для {coin} не найден (символ: {symbol}, result пустой или не словарь)")
                return None
            
            # XT возвращает: c = last price, b = bid, a = ask
            last_price = result.get("c")
            if last_price is None:
                return None
            
            last = float(last_price)
            bid = float(result.get("b", last_price))
            ask = float(result.get("a", last_price))
            
            return {
                "price": last,
                "bid": bid,
                "ask": ask,
            }
                
        except Exception as e:
            logger.error(f"XT: ошибка при получении тикера для {coin}: {e}", exc_info=True)
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
            url = "/future/market/v1/public/q/funding-rate"
            params = {"symbol": symbol}
            
            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"XT: не удалось получить ответ для фандинга {coin} (символ: {symbol})")
                return None
            
            return_code = data.get("returnCode")
            if return_code != 0:
                msg = data.get("msgInfo", "Unknown error")
                logger.warning(f"XT: API вернул ошибку для фандинга {coin} (символ: {symbol}): returnCode={return_code}, msg={msg}")
                return None
            
            result = data.get("result")
            if not result or not isinstance(result, dict):
                logger.warning(f"XT: фандинг для {coin} не найден (символ: {symbol}, result пустой или не словарь)")
                return None
            
            funding_rate_raw = result.get("fundingRate")
            if funding_rate_raw is None:
                return None
            
            return float(funding_rate_raw)
                
        except Exception as e:
            logger.error(f"XT: ошибка при получении фандинга для {coin}: {e}", exc_info=True)
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
            url = "/future/market/v1/public/q/depth"
            params = {"symbol": symbol, "limit": limit}
            
            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"XT: не удалось получить orderbook для {coin}")
                return None
            
            return_code = data.get("returnCode")
            if return_code != 0:
                msg = data.get("msgInfo", "Unknown error")
                logger.warning(f"XT: API вернул ошибку для orderbook {coin}: returnCode={return_code}, msg={msg}")
                return None
            
            result = data.get("result")
            if not result or not isinstance(result, dict):
                logger.warning(f"XT: orderbook для {coin} не найден (result пустой или не словарь)")
                return None
            
            # XT возвращает bids и asks в поле result
            bids = result.get("bids", [])
            asks = result.get("asks", [])
            
            if not bids or not asks:
                logger.warning(f"XT: пустой orderbook для {coin}")
                return None
            
            return {"bids": bids, "asks": asks}
                
        except Exception as e:
            logger.error(f"XT: ошибка при получении orderbook для {coin}: {e}", exc_info=True)
            return None

