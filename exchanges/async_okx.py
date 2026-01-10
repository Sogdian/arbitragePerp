"""
Асинхронная реализация OKX для фьючерсов на базе AsyncBaseExchange

Фиксированная логика:
- Рынок: USDT-M Futures (Perpetual/SWAP)
- Символ: COIN-USDT-SWAP (с дефисами и суффиксом SWAP)
- API версия: v5
- Если COIN-USDT-SWAP не найден → считаем, что инструмента нет
"""
from typing import Dict, Optional
import logging
from .async_base_exchange import AsyncBaseExchange

logger = logging.getLogger(__name__)


class AsyncOkxExchange(AsyncBaseExchange):
    BASE_URL = "https://www.okx.com"

    def __init__(self, pool_limit: int = 100):
        super().__init__("OKX", pool_limit)

    def _normalize_symbol(self, coin: str) -> str:
        """Преобразует монету в формат OKX для фьючерсов (например, CVC -> CVC-USDT-SWAP)"""
        return f"{coin.upper()}-USDT-SWAP"

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
        OKX возвращает ошибки в формате {"code": "51000", "msg": "...", "data": []}
        Успешные ответы содержат "code": "0" и "data" с данными.
        """
        if not isinstance(data, dict):
            return False
        code = data.get("code")
        # OKX успешные ответы имеют code "0" (строка)
        return code is not None and str(code) != "0"

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
            url = "/api/v5/market/ticker"
            params = {"instId": symbol}

            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"OKX: пустой ответ ticker для {coin} (instId={symbol})")
                return None
            
            if self._is_api_error(data):
                logger.warning(
                    f"OKX: ticker API error для {coin} "
                    f"(instId={symbol}, code={data.get('code')}, msg={data.get('msg', '')})"
                )
                return None

            # OKX возвращает данные в поле "data" как список
            data_list = data.get("data")
            if not isinstance(data_list, list) or not data_list:
                return None

            ticker_data = data_list[0]
            if not isinstance(ticker_data, dict):
                return None

            last_price_raw = ticker_data.get("last")
            if last_price_raw is None:
                return None

            try:
                last = float(last_price_raw)
            except (TypeError, ValueError):
                return None

            if last <= 0:
                return None

            bid = self._safe_px(ticker_data.get("bidPx"), last)
            ask = self._safe_px(ticker_data.get("askPx"), last)

            # Если после проверок bid > ask — откатываем на last
            if bid > ask:
                bid = last
                ask = last

            return {"price": last, "bid": bid, "ask": ask}

        except Exception as e:
            logger.error(f"OKX: ошибка при получении тикера для {coin}: {e}", exc_info=True)
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
            url = "/api/v5/public/funding-rate"
            params = {"instId": symbol}

            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"OKX: пустой ответ funding для {coin} (instId={symbol})")
                return None
            
            if self._is_api_error(data):
                logger.warning(
                    f"OKX: funding API error для {coin} "
                    f"(instId={symbol}, code={data.get('code')}, msg={data.get('msg', '')})"
                )
                return None

            # OKX возвращает данные в поле "data" как список
            data_list = data.get("data")
            if not isinstance(data_list, list) or not data_list:
                # Логируем для диагностики: "нет инструмента" vs "временный ответ"
                msg = data.get("msg", "")
                logger.warning(f"OKX: пустой data для funding {coin} (instId={symbol}, msg={msg})")
                return None

            funding_data = data_list[0]
            if not isinstance(funding_data, dict):
                msg = data.get("msg", "")
                logger.warning(f"OKX: неожиданный формат funding data для {coin} (instId={symbol}, msg={msg})")
                return None

            funding_rate_raw = funding_data.get("fundingRate")
            if funding_rate_raw is None:
                msg = data.get("msg", "")
                logger.warning(f"OKX: нет fundingRate в ответе для {coin} (instId={symbol}, msg={msg})")
                return None

            try:
                return float(funding_rate_raw)
            except (TypeError, ValueError):
                return None

        except Exception as e:
            logger.error(f"OKX: ошибка при получении фандинга для {coin}: {e}", exc_info=True)
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
            url = "/api/v5/market/books"
            
            # OKX принимает sz (size) параметр для количества уровней (от 1 до 400)
            sz = max(1, min(int(limit), 400))
            params = {"instId": symbol, "sz": sz}

            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"OKX: пустой ответ orderbook для {coin} (instId={symbol})")
                return None
            
            if self._is_api_error(data):
                logger.warning(
                    f"OKX: orderbook API error для {coin} "
                    f"(instId={symbol}, code={data.get('code')}, msg={data.get('msg', '')})"
                )
                return None

            # OKX возвращает данные в поле "data" как список
            data_list = data.get("data")
            if not isinstance(data_list, list) or not data_list:
                return None

            ob_data = data_list[0]
            if not isinstance(ob_data, dict):
                return None

            bids = ob_data.get("bids") or []  # [[price, size, ...], ...]
            asks = ob_data.get("asks") or []

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

            # sz уже ограничивает количество уровней в запросе, поэтому не режем второй раз
            return {"bids": bids_sorted, "asks": asks_sorted}

        except Exception as e:
            logger.error(f"OKX: ошибка при получении orderbook для {coin}: {e}", exc_info=True)
            return None

