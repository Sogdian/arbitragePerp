"""
Асинхронная реализация Bitget для фьючерсов на базе AsyncBaseExchange

Фиксированная логика:
- Рынок: USDT-M Futures (Perpetual)
- Символ: COINUSDT (без подчеркивания и суффиксов)
- API версия: v2
- productType: usdt-futures
- Если COINUSDT не найден → считаем, что инструмента нет
"""
from typing import Dict, Optional, List
import logging
from .async_base_exchange import AsyncBaseExchange

logger = logging.getLogger(__name__)


class AsyncBitgetExchange(AsyncBaseExchange):
    BASE_URL = "https://api.bitget.com"
    PRODUCT_TYPE = "usdt-futures"

    def __init__(self, pool_limit: int = 100):
        super().__init__("Bitget", pool_limit)

    def _normalize_symbol(self, coin: str) -> str:
        """Преобразует монету в формат Bitget для фьючерсов (например, CVC -> CVCUSDT)"""
        return f"{coin.upper()}USDT"

    def _safe_px(self, raw: object, fallback: float) -> float:
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
        if v > fallback * 10 or v < fallback / 10:
            return fallback

        return v

    def _is_api_error(self, data: object) -> bool:
        """
        Bitget возвращает ошибки в формате {"code": "40001", "msg": "...", "requestTime": ...}
        Успешные ответы содержат "code": "00000" или "data" поле.
        """
        if not isinstance(data, dict):
            return False
        # Нормализуем code в строку для надежного сравнения
        code = str(data.get("code")) if data.get("code") is not None else None
        return code is not None and code != "00000"

    async def get_futures_ticker(self, coin: str) -> Optional[Dict]:
        """
        Получить тикер фьючерса для монеты

        Returns:
            {
                "price": float,  # Текущая цена (last)
                "bid": float,    # Лучшая цена покупки
                "ask": float,    # Лучшая цена продажи
            }
            или None если ошибка/символ не найден
        """
        try:
            symbol = self._normalize_symbol(coin)
            url = "/api/v2/mix/market/ticker"
            params = {"symbol": symbol, "productType": self.PRODUCT_TYPE}

            data = await self._request_json("GET", url, params=params)
            if not data or self._is_api_error(data):
                return None

            d = data.get("data")
            if isinstance(d, list) and d:
                d = d[0]
            if not isinstance(d, dict):
                return None

            last = float(d.get("lastPr"))
            if last <= 0:
                return None

            bid = self._safe_px(d.get("bidPr"), last)
            ask = self._safe_px(d.get("askPr"), last)
            if bid > ask:
                bid = last
                ask = last

            return {"price": last, "bid": bid, "ask": ask}

        except Exception as e:
            logger.error(f"Bitget: ошибка при получении тикера для {coin}: {e}", exc_info=True)
            return None

    async def get_funding_rate(self, coin: str) -> Optional[float]:
        """
        Получить текущую ставку фандинга для монеты

        Returns:
            Ставка фандинга в decimal формате (например, 0.0001 = 0.01%) или None если ошибка
            Примечание: bot.py умножает на 100 для отображения в процентах
        """
        try:
            symbol = self._normalize_symbol(coin)
            url = "/api/v2/mix/market/current-fund-rate"
            params = {"symbol": symbol, "productType": self.PRODUCT_TYPE}

            data = await self._request_json("GET", url, params=params)
            if not data or self._is_api_error(data):
                return None

            d = data.get("data")
            if isinstance(d, list) and d:
                d = d[0]
            if not isinstance(d, dict):
                return None

            fr = d.get("fundingRate")
            return None if fr is None else float(fr)

        except Exception as e:
            logger.error(f"Bitget: ошибка при получении фандинга для {coin}: {e}", exc_info=True)
            return None

    async def get_orderbook(self, coin: str, limit: int = 50) -> Optional[Dict]:
        """
        Получить книгу заявок (orderbook) для монеты

        Returns:
            {
                "bids": [[price, size], ...],
                "asks": [[price, size], ...]
            }
            или None если ошибка/символ не найден
        """
        try:
            symbol = self._normalize_symbol(coin)
            url = "/api/v2/mix/market/merge-depth"
            
            # Bitget принимает limit как строку "50" или "max"
            limit_str = "50" if limit <= 50 else "max"
            params = {"symbol": symbol, "productType": self.PRODUCT_TYPE, "limit": limit_str}

            data = await self._request_json("GET", url, params=params)
            if not data or self._is_api_error(data):
                return None

            d = data.get("data")
            if not isinstance(d, dict):
                return None

            bids = d.get("bids") or []
            asks = d.get("asks") or []
            if not bids or not asks:
                return None

            # Минимальная валидация top-of-book (как в других биржах)
            if (not isinstance(bids[0], (list, tuple)) or len(bids[0]) < 2 or
                    not isinstance(asks[0], (list, tuple)) or len(asks[0]) < 2):
                logger.warning(f"Bitget: неверный формат orderbook уровней для {coin} (symbol={symbol})")
                return None

            try:
                float(bids[0][0]); float(bids[0][1])
                float(asks[0][0]); float(asks[0][1])
            except (TypeError, ValueError, IndexError) as e:
                logger.warning(f"Bitget: top level bids/asks not numeric for {coin} (symbol={symbol}): {e}")
                return None

            # Сортировка "на всякий случай": bids по убыванию price, asks по возрастанию price
            try:
                bids_sorted = sorted(bids, key=lambda x: float(x[0]), reverse=True)  # price desc
                asks_sorted = sorted(asks, key=lambda x: float(x[0]))  # price asc
            except (TypeError, ValueError, IndexError):
                # Если сортировка не удалась, возвращаем как есть
                bids_sorted = bids
                asks_sorted = asks

            # Обрезаем до limit, если API вернул больше уровней
            limit_i = min(limit, len(bids_sorted), len(asks_sorted))
            return {"bids": bids_sorted[:limit_i], "asks": asks_sorted[:limit_i]}

        except Exception as e:
            logger.error(f"Bitget: ошибка при получении orderbook для {coin}: {e}", exc_info=True)
            return None

    async def get_all_futures_coins(self) -> List[str]:
        """
        Возвращает список монет, доступных во фьючерсах на Bitget.
        
        Returns:
            Список монет без суффиксов (например, ["BTC", "ETH", "SOL", ...])
        """
        try:
            url = "/api/v2/mix/market/contracts"
            params = {"productType": self.PRODUCT_TYPE}
            
            data = await self._request_json("GET", url, params=params)
            if not data or self._is_api_error(data):
                logger.warning("Bitget: не удалось получить список контрактов")
                return []
            
            contracts_data = data.get("data")
            if isinstance(contracts_data, list) and contracts_data:
                contracts_data = contracts_data[0]
            if not isinstance(contracts_data, dict):
                contracts_list = data.get("data") or []
            else:
                contracts_list = contracts_data.get("data") or []
            
            if not isinstance(contracts_list, list):
                contracts_list = []
            
            coins = []
            for contract in contracts_list:
                if isinstance(contract, dict):
                    symbol = contract.get("symbol", "")
                    # Извлекаем монету из символа (например, "BTCUSDT" -> "BTC")
                    if symbol.endswith("USDT"):
                        coin = symbol[:-4]  # Убираем "USDT"
                        if coin:
                            coins.append(coin)
            
            return sorted(set(coins))
        except Exception as e:
            logger.error(f"Bitget: ошибка при получении списка монет: {e}", exc_info=True)
            return []
