"""
Асинхронная реализация Binance Futures для фьючерсов на базе AsyncBaseExchange

Фиксированная логика:
- Рынок: USDT-M Futures (Perpetual)
- Символ: COINUSDT (без подчеркивания)
- Если COINUSDT не найден → считаем, что инструмента нет
"""
from typing import Dict, Optional
import logging
from .async_base_exchange import AsyncBaseExchange

logger = logging.getLogger(__name__)


class AsyncBinanceExchange(AsyncBaseExchange):
    BASE_URL = "https://fapi.binance.com"

    def __init__(self, pool_limit: int = 100):
        super().__init__("Binance", pool_limit)

    def _normalize_symbol(self, coin: str) -> str:
        """Преобразует монету в формат Binance для фьючерсов (например, CVC -> CVCUSDT)"""
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
            url = "/fapi/v1/ticker/24hr"
            params = {"symbol": symbol}
            
            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"Binance: тикер для {coin} не найден")
                return None
            
            # Binance возвращает ошибки в формате {"code": -1121, "msg": "Invalid symbol."}
            # Успешные ответы не содержат поле "code", ошибки содержат отрицательные коды
            if isinstance(data, dict) and "code" in data:
                logger.warning(f"Binance: тикер для {coin} не найден (code: {data.get('code')}, msg: {data.get('msg')})")
                return None
            
            # Binance возвращает словарь напрямую
            last_price = data.get("lastPrice")
            if last_price is None:
                return None
            
            last = float(last_price)
            
            # Проверка, что last_price тоже валиден (не мусор)
            if last <= 0:
                return None
            
            def _safe_px(raw: object, fallback: float) -> float:
                """
                Безопасное преобразование цены с проверками на разумность
                
                Args:
                    raw: Сырое значение цены (может быть строкой, числом, None, или мусором)
                    fallback: Значение по умолчанию (обычно last_price)
                    
                Returns:
                    Валидная цена или fallback
                """
                try:
                    v = float(raw)
                except (TypeError, ValueError):
                    return fallback
                
                if v <= 0:
                    return fallback
                
                # Sanity check: если отличается от fallback больше чем в 10 раз — считаем мусором
                # Порог 10x обычно достаточен; для очень волатильных инструментов можно сделать параметром (5x-20x)
                if v > fallback * 10 or v < fallback / 10:
                    return fallback
                
                return v
            
            bid = _safe_px(data.get("bidPrice"), last)
            ask = _safe_px(data.get("askPrice"), last)
            
            # Если после проверок bid > ask (бывает на мусорных данных) — откатываем на last
            if bid > ask:
                bid = last
                ask = last
            
            return {
                "price": last,
                "bid": bid,
                "ask": ask,
            }
                
        except Exception as e:
            logger.error(f"Binance: ошибка при получении тикера для {coin}: {e}", exc_info=True)
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
            url = "/fapi/v1/premiumIndex"
            params = {"symbol": symbol}
            
            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"Binance: фандинг для {coin} не найден")
                return None
            
            # Binance возвращает ошибки в формате {"code": -1121, "msg": "Invalid symbol."}
            # Успешные ответы не содержат поле "code", ошибки содержат отрицательные коды
            if isinstance(data, dict) and "code" in data:
                logger.warning(f"Binance: фандинг для {coin} не найден (code: {data.get('code')}, msg: {data.get('msg')})")
                return None
            
            funding_rate_raw = data.get("lastFundingRate")
            if funding_rate_raw is None:
                return None
            
            return float(funding_rate_raw)
                
        except Exception as e:
            logger.error(f"Binance: ошибка при получении фандинга для {coin}: {e}", exc_info=True)
            return None
    
    async def get_orderbook(self, coin: str, limit: int = 50) -> Optional[Dict]:
        """
        Получить книгу заявок (orderbook) для монеты
        
        Args:
            coin: Название монеты без /USDT (например, "GPS")
            limit: Количество уровней (по умолчанию 50, максимум 5000)
            
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
            url = "/fapi/v1/depth"
            # Binance принимает limit от 5 до 5000
            limit = max(5, min(int(limit), 5000))
            params = {
                "symbol": symbol,
                "limit": limit,
            }
            
            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"Binance: orderbook error for {coin}")
                return None
            
            # Binance возвращает ошибки в формате {"code": -1121, "msg": "Invalid symbol."}
            # Успешные ответы не содержат поле "code", ошибки содержат отрицательные коды
            if isinstance(data, dict) and "code" in data:
                logger.warning(f"Binance: orderbook error for {coin} (code: {data.get('code')}, msg: {data.get('msg')})")
                return None
            
            bids = data.get("bids") or []  # [[price, size], ...]
            asks = data.get("asks") or []
            
            if not bids or not asks:
                return None
            
            return {"bids": bids, "asks": asks}
                
        except Exception as e:
            logger.error(f"Binance: ошибка при получении orderbook для {coin}: {e}", exc_info=True)
            return None

