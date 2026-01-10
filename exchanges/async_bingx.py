"""
Асинхронная реализация BingX для фьючерсов на базе AsyncBaseExchange

Фиксированная логика:
- Рынок: USDT-M Futures (Perpetual)
- Символ: COIN-USDT (с дефисом)
- API версия: v2
- Если COIN-USDT не найден → считаем, что инструмента нет
"""
from typing import Dict, Optional, Any, List
import logging
from .async_base_exchange import AsyncBaseExchange

logger = logging.getLogger(__name__)


class AsyncBingxExchange(AsyncBaseExchange):
    BASE_URL = "https://open-api.bingx.com"

    def __init__(self, pool_limit: int = 100):
        super().__init__("BingX", pool_limit)

    def _normalize_symbol(self, coin: str) -> str:
        """Преобразует монету в формат BingX для фьючерсов (например, CVC -> CVC-USDT)"""
        return f"{coin.upper()}-USDT"

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

        # Если fallback == 0, sanity-check не имеет смысла
        if fallback <= 0:
            return v

        # Sanity check: если отличается от fallback больше чем в 10 раз — считаем мусором
        # Для очень маленьких цен (fallback < 0.0001) не применяем проверку /10, чтобы не отсекать валидные значения
        if fallback >= 0.0001:
            if v > fallback * 10 or v < fallback / 10:
                return fallback
        else:
            # Для очень маленьких цен проверяем только верхнюю границу
            if v > fallback * 10:
                return fallback

        return v

    def _is_api_error(self, data: object) -> bool:
        """
        BingX возвращает ошибки в формате {"code": 100001, "msg": "...", "data": null}
        Успешные ответы содержат "code": 0 или "0" и "data" с данными.
        """
        if not isinstance(data, dict):
            return False
        code = data.get("code")
        # Успех: 0 или "0"
        return code is not None and str(code) != "0"

    async def get_futures_ticker(self, coin: str) -> Optional[Dict[str, Any]]:
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
            url = "/openApi/swap/v2/quote/ticker"
            params = {"symbol": symbol}

            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"BingX: пустой ответ ticker для {coin} (symbol={symbol})")
                return None
            
            if self._is_api_error(data):
                logger.warning(
                    f"BingX: ticker API error для {coin} "
                    f"(symbol={symbol}, code={data.get('code')}, msg={data.get('msg', '')})"
                )
                return None

            # BingX возвращает данные в поле "data"
            ticker_data = data.get("data")
            # Иногда BingX может вернуть data как list (редко, но бывает)
            if isinstance(ticker_data, list) and ticker_data:
                ticker_data = ticker_data[0]
            if not isinstance(ticker_data, dict):
                return None

            # BingX может возвращать ключи в разных вариантах
            last_price_raw = ticker_data.get("lastPrice") or ticker_data.get("last")
            if last_price_raw is None:
                return None

            try:
                last = float(last_price_raw)
            except (TypeError, ValueError):
                return None

            if last <= 0:
                return None

            bid_raw = ticker_data.get("bidPrice") or ticker_data.get("bid")
            ask_raw = ticker_data.get("askPrice") or ticker_data.get("ask")
            bid = self._safe_px(bid_raw, last)
            ask = self._safe_px(ask_raw, last)

            # Если после проверок bid > ask — откатываем на last
            if bid > ask:
                bid = last
                ask = last

            return {"price": last, "bid": bid, "ask": ask}

        except Exception as e:
            logger.error(f"BingX: ошибка при получении тикера для {coin}: {e}", exc_info=True)
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
            url = "/openApi/swap/v2/quote/premiumIndex"
            params = {"symbol": symbol}

            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"BingX: пустой ответ premiumIndex для {coin} (symbol={symbol})")
                return None
            
            if self._is_api_error(data):
                logger.warning(
                    f"BingX: premiumIndex API error для {coin} "
                    f"(symbol={symbol}, code={data.get('code')}, msg={data.get('msg', '')})"
                )
                return None

            # BingX возвращает данные в поле "data"
            funding_data = data.get("data")
            # Иногда BingX может вернуть data как list (редко, но бывает)
            if isinstance(funding_data, list) and funding_data:
                funding_data = funding_data[0]
            if not isinstance(funding_data, dict):
                return None

            # BingX может возвращать funding rate в разных полях
            funding_rate_raw = (
                funding_data.get("lastFundingRate")
                or funding_data.get("fundingRate")
                or funding_data.get("fundingRateNext")
                or funding_data.get("nextFundingRate")
            )
            if funding_rate_raw is None:
                return None

            try:
                return float(funding_rate_raw)
            except (TypeError, ValueError):
                return None

        except Exception as e:
            logger.error(f"BingX: ошибка при получении фандинга для {coin}: {e}", exc_info=True)
            return None

    async def get_orderbook(self, coin: str, limit: int = 50) -> Optional[Dict[str, Any]]:
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
            url = "/openApi/swap/v2/quote/depth"
            
            # BingX принимает limit от 5 до 200
            limit_i = max(5, min(int(limit), 200))
            params = {"symbol": symbol, "limit": limit_i}

            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"BingX: пустой ответ orderbook для {coin} (symbol={symbol})")
                return None
            
            if self._is_api_error(data):
                logger.warning(
                    f"BingX: orderbook API error для {coin} "
                    f"(symbol={symbol}, code={data.get('code')}, msg={data.get('msg', '')})"
                )
                return None

            # BingX возвращает данные в поле "data"
            ob_data = data.get("data")
            # Иногда BingX может вернуть data как list (редко, но бывает)
            if isinstance(ob_data, list) and ob_data:
                ob_data = ob_data[0]
            if not isinstance(ob_data, dict):
                return None

            # BingX может возвращать ключи в разных вариантах
            bids = ob_data.get("bids") or ob_data.get("buy") or ob_data.get("b") or []
            asks = ob_data.get("asks") or ob_data.get("sell") or ob_data.get("a") or []

            if not bids or not asks:
                return None

            # Функция для очистки уровней: пропускает битые уровни вместо падения всего метода
            def _clean(levels):
                out = []
                for lvl in levels:
                    if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
                        continue
                    try:
                        px = float(lvl[0])
                        sz_val = float(lvl[1])
                    except (TypeError, ValueError):
                        continue
                    if px > 0 and sz_val > 0:
                        out.append([px, sz_val])
                return out

            bids_clean = _clean(bids)
            asks_clean = _clean(asks)

            if not bids_clean or not asks_clean:
                return None

            # Сортировка "на всякий случай": bids по убыванию price, asks по возрастанию price
            bids_sorted = sorted(bids_clean, key=lambda x: x[0], reverse=True)  # price desc
            asks_sorted = sorted(asks_clean, key=lambda x: x[0])  # price asc

            # Обрезаем до limit_i, если API вернул больше уровней
            return {"bids": bids_sorted[:limit_i], "asks": asks_sorted[:limit_i]}

        except Exception as e:
            logger.error(f"BingX: ошибка при получении orderbook для {coin}: {e}", exc_info=True)
            return None

