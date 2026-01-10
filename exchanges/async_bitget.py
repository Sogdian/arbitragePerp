"""
Асинхронная реализация Bitget для фьючерсов на базе AsyncBaseExchange

Фиксированная логика:
- Рынок: USDT-M Futures (Perpetual)
- Символ: COINUSDT (без подчеркивания)
- Если COINUSDT не найден → считаем, что инструмента нет
"""
from typing import Dict, Optional
import logging
from .async_base_exchange import AsyncBaseExchange

logger = logging.getLogger(__name__)


class AsyncBitgetExchange(AsyncBaseExchange):
    BASE_URL = "https://api.bitget.com"

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
        code = data.get("code")
        return code is not None and code != "00000" and code != 0

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
            url = "/api/mix/v1/market/ticker"
            params = {"symbol": symbol, "productType": "umcbl"}  # umcbl = USDT-M perpetual

            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"Bitget: пустой ответ ticker для {coin} (symbol={symbol})")
                return None

            if self._is_api_error(data):
                logger.warning(
                    f"Bitget: тикер для {coin} не найден "
                    f"(symbol={symbol}, code={data.get('code')}, msg={data.get('msg')})"
                )
                return None

            if not isinstance(data, dict):
                logger.warning(f"Bitget: неожиданный формат ticker для {coin}: {type(data)}")
                return None

            # Bitget возвращает данные в поле "data"
            ticker_data = data.get("data")
            if not isinstance(ticker_data, dict):
                logger.warning(f"Bitget: нет data в ticker для {coin} (symbol={symbol})")
                return None

            last_price_raw = ticker_data.get("last")
            if last_price_raw is None:
                logger.warning(f"Bitget: нет last в ticker для {coin} (symbol={symbol})")
                return None

            try:
                last = float(last_price_raw)
            except (TypeError, ValueError) as e:
                logger.warning(f"Bitget: last не число для {coin} (symbol={symbol}): {e}")
                return None

            if last <= 0:
                return None

            bid = self._safe_px(ticker_data.get("bestBid"), last)
            ask = self._safe_px(ticker_data.get("bestAsk"), last)

            # Если после проверок bid > ask — откатываем на last
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
            url = "/api/mix/v1/market/current-fundRate"
            params = {"symbol": symbol, "productType": "umcbl"}  # umcbl = USDT-M perpetual

            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"Bitget: пустой ответ funding для {coin} (symbol={symbol})")
                return None

            if self._is_api_error(data):
                logger.warning(
                    f"Bitget: фандинг для {coin} не найден "
                    f"(symbol={symbol}, code={data.get('code')}, msg={data.get('msg')})"
                )
                return None

            if not isinstance(data, dict):
                logger.warning(f"Bitget: неожиданный формат funding для {coin}: {type(data)}")
                return None

            # Bitget возвращает данные в поле "data"
            funding_data = data.get("data")
            
            # Иногда Bitget может вернуть data как list (редко, но бывает)
            if isinstance(funding_data, list) and funding_data:
                funding_data = funding_data[0]
            
            if not isinstance(funding_data, dict):
                logger.warning(f"Bitget: нет data в funding для {coin} (symbol={symbol}), data type: {type(funding_data)}")
                return None

            funding_rate_raw = funding_data.get("fundingRate") or funding_data.get("fundingRateRound")
            if funding_rate_raw is None:
                logger.warning(f"Bitget: нет fundingRate/fundingRateRound для {coin} (symbol={symbol})")
                return None

            try:
                return float(funding_rate_raw)
            except (TypeError, ValueError) as e:
                logger.warning(f"Bitget: funding_rate не число для {coin} (symbol={symbol}): {e}")
                return None

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
            url = "/api/mix/v1/market/depth"
            
            # Bitget принимает limit от 1 до 200
            limit_i = max(1, min(int(limit), 200))
            params = {"symbol": symbol, "productType": "umcbl", "limit": limit_i}

            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"Bitget: пустой ответ orderbook для {coin} (symbol={symbol})")
                return None

            if self._is_api_error(data):
                logger.warning(
                    f"Bitget: orderbook error for {coin} "
                    f"(symbol={symbol}, code={data.get('code')}, msg={data.get('msg')})"
                )
                return None

            if not isinstance(data, dict):
                logger.warning(f"Bitget: неожиданный формат orderbook для {coin}: {type(data)}")
                return None

            # Bitget возвращает данные в поле "data"
            ob_data = data.get("data")
            if not isinstance(ob_data, dict):
                logger.warning(f"Bitget: нет data в orderbook для {coin} (symbol={symbol})")
                return None

            bids = ob_data.get("bids") or []  # [[price, size], ...] (обычно строки)
            asks = ob_data.get("asks") or []

            if not bids or not asks:
                logger.warning(f"Bitget: пустой bids/asks для {coin} (symbol={symbol})")
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

            return {"bids": bids_sorted, "asks": asks_sorted}

        except Exception as e:
            logger.error(f"Bitget: ошибка при получении orderbook для {coin}: {e}", exc_info=True)
            return None

