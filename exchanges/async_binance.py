"""
Асинхронная реализация Binance Futures для фьючерсов на базе AsyncBaseExchange

Фиксированная логика:
- Рынок: USDT-M Futures (Perpetual)
- Символ: COINUSDT (без подчеркивания)
- Если COINUSDT не найден → считаем, что инструмента нет
"""
from typing import Dict, Optional, Set
import logging
from .async_base_exchange import AsyncBaseExchange
from .coin_list_fetchers import fetch_binance_coins

logger = logging.getLogger(__name__)


class AsyncBinanceExchange(AsyncBaseExchange):
    BASE_URL = "https://fapi.binance.com"

    def __init__(self, pool_limit: int = 100):
        super().__init__("Binance", pool_limit)

    def _normalize_symbol(self, coin: str) -> str:
        """Преобразует монету в формат Binance для фьючерсов (например, CVC -> CVCUSDT)"""
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
        Binance возвращает ошибки в формате {"code": -1121, "msg": "Invalid symbol."}
        Успешные ответы (для этих эндпоинтов) не должны содержать поле "code".
        """
        return isinstance(data, dict) and "code" in data

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
            url = "/fapi/v1/ticker/24hr"
            params = {"symbol": symbol}

            data = await self._request_json("GET", url, params=params)
            if not data:
                # Причина чаще всего уже залогирована в AsyncBaseExchange._request_json (timeout/connection/HTTP status),
                # поэтому здесь не дублируем WARNING.
                logger.debug(f"Binance: пустой ответ ticker для {coin} (symbol={symbol})")
                return None

            if self._is_api_error(data):
                # "Invalid symbol" / "not found" — это шум для сканера, опускаем в DEBUG
                logger.debug(
                    f"Binance: тикер для {coin} не найден "
                    f"(symbol={symbol}, code={data.get('code')}, msg={data.get('msg')})"
                )
                return None

            if not isinstance(data, dict):
                logger.warning(f"Binance: неожиданный формат ticker для {coin}: {type(data)}")
                return None

            last_price_raw = data.get("lastPrice")
            if last_price_raw is None:
                logger.warning(f"Binance: нет lastPrice в ticker для {coin} (symbol={symbol})")
                return None

            try:
                last = float(last_price_raw)
            except (TypeError, ValueError) as e:
                logger.warning(f"Binance: lastPrice не число для {coin} (symbol={symbol}): {e}")
                return None

            if last <= 0:
                return None

            bid = self._safe_px(data.get("bidPrice"), last)
            ask = self._safe_px(data.get("askPrice"), last)

            # Если после проверок bid > ask — откатываем на last
            if bid > ask:
                bid = last
                ask = last

            return {"price": last, "bid": bid, "ask": ask}

        except Exception as e:
            logger.error(f"Binance: ошибка при получении тикера для {coin}: {e}", exc_info=True)
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
            url = "/fapi/v1/premiumIndex"
            params = {"symbol": symbol}

            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"Binance: пустой ответ funding для {coin} (symbol={symbol})")
                return None

            if self._is_api_error(data):
                logger.warning(
                    f"Binance: фандинг для {coin} не найден "
                    f"(symbol={symbol}, code={data.get('code')}, msg={data.get('msg')})"
                )
                return None

            if not isinstance(data, dict):
                logger.warning(f"Binance: неожиданный формат funding для {coin}: {type(data)}")
                return None

            funding_rate_raw = data.get("lastFundingRate")
            if funding_rate_raw is None:
                logger.warning(f"Binance: нет lastFundingRate для {coin} (symbol={symbol})")
                return None

            try:
                return float(funding_rate_raw)
            except (TypeError, ValueError) as e:
                logger.warning(f"Binance: funding_rate не число для {coin} (symbol={symbol}): {e}")
                return None

        except Exception as e:
            logger.error(f"Binance: ошибка при получении фандинга для {coin}: {e}", exc_info=True)
            return None

    async def get_funding_info(self, coin: str) -> Optional[Dict]:
        """
        Получить информацию о фандинге (ставка и время до следующей выплаты)

        Returns:
            Словарь с данными:
            {
                "funding_rate": float,  # Ставка фандинга (например, 0.0001 = 0.01%)
                "next_funding_time": int,  # Timestamp следующей выплаты в миллисекундах
            }
            или None если ошибка
        """
        try:
            symbol = self._normalize_symbol(coin)
            url = "/fapi/v1/premiumIndex"
            params = {"symbol": symbol}

            data = await self._request_json("GET", url, params=params)
            if not data:
                return None

            if self._is_api_error(data):
                return None

            if not isinstance(data, dict):
                return None

            funding_rate_raw = data.get("lastFundingRate")
            next_funding_time_raw = data.get("nextFundingTime")

            if funding_rate_raw is None:
                return None

            try:
                funding_rate = float(funding_rate_raw)
                next_funding_time = int(next_funding_time_raw) if next_funding_time_raw is not None else None
                return {
                    "funding_rate": funding_rate,
                    "next_funding_time": next_funding_time,
                }
            except (TypeError, ValueError):
                return None

        except Exception as e:
            logger.error(f"Binance: ошибка при получении funding info для {coin}: {e}", exc_info=True)
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
            url = "/fapi/v1/depth"

            # Binance принимает limit от 5 до 5000
            limit_i = max(5, min(int(limit), 5000))
            params = {"symbol": symbol, "limit": limit_i}

            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"Binance: пустой ответ orderbook для {coin} (symbol={symbol})")
                return None

            if self._is_api_error(data):
                logger.warning(
                    f"Binance: orderbook error for {coin} "
                    f"(symbol={symbol}, code={data.get('code')}, msg={data.get('msg')})"
                )
                return None

            if not isinstance(data, dict):
                logger.warning(f"Binance: неожиданный формат orderbook для {coin}: {type(data)}")
                return None

            bids = data.get("bids") or []  # [[price, size], ...] (обычно строки)
            asks = data.get("asks") or []

            if not bids or not asks:
                logger.warning(f"Binance: пустой bids/asks для {coin} (symbol={symbol})")
                return None

            # Минимальная валидация top-of-book (как в LBank/MEXC)
            if (not isinstance(bids[0], (list, tuple)) or len(bids[0]) < 2 or
                    not isinstance(asks[0], (list, tuple)) or len(asks[0]) < 2):
                logger.warning(f"Binance: неверный формат orderbook уровней для {coin} (symbol={symbol})")
                return None

            try:
                float(bids[0][0]); float(bids[0][1])
                float(asks[0][0]); float(asks[0][1])
            except (TypeError, ValueError, IndexError) as e:
                logger.warning(f"Binance: top level bids/asks not numeric for {coin} (symbol={symbol}): {e}")
                return None

            return {"bids": bids, "asks": asks}

        except Exception as e:
            logger.error(f"Binance: ошибка при получении orderbook для {coin}: {e}", exc_info=True)
            return None

    async def get_all_futures_coins(self) -> Set[str]:
        """
        Возвращает множество монет, доступных во фьючерсах на Binance.
        
        Returns:
            Множество монет без суффиксов (например, {"BTC", "ETH", "SOL", ...})
        """
        return await fetch_binance_coins(self)
