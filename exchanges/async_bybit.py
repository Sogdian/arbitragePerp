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
from typing import Dict, Optional, Set
import logging
from .async_base_exchange import AsyncBaseExchange
from .coin_list_fetchers import fetch_bybit_coins

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
            
            bid = _safe_px(item.get("bid1Price"), last)
            ask = _safe_px(item.get("ask1Price"), last)
            
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

            # Bybit:
            # - /v5/market/tickers -> текущая/индикативная ставка (то, что UI показывает как "Текущая ставка")
            # - /v5/market/funding/history -> история (то, что UI показывает как "Предыдущая ставка")
            url_tickers = "/v5/market/tickers"
            params_tickers = {"category": "linear", "symbol": symbol}

            data = await self._request_json("GET", url_tickers, params=params_tickers)
            if data and data.get("retCode") == 0:
                items = (data.get("result") or {}).get("list") or []
                if items and isinstance(items[0], dict):
                    r = items[0].get("fundingRate")
                    if r is not None:
                        try:
                            return float(r)
                        except (TypeError, ValueError):
                            pass

            # Fallback на историю (предыдущая применённая ставка)
            url_history = "/v5/market/funding/history"
            params_history = {"category": "linear", "symbol": symbol, "limit": 1}

            data = await self._request_json("GET", url_history, params=params_history)
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

    async def get_all_futures_coins(self) -> Set[str]:
        """
        Возвращает множество монет, доступных во фьючерсах на Bybit.
        
        Returns:
            Множество монет без суффиксов (например, {"BTC", "ETH", "SOL", ...})
        """
        return await fetch_bybit_coins(self)



