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
    
    def _canon(self, sym: str) -> str:
        """
        Канонизирует символ: убирает _ и - для сравнения
        Например: GPS_USDT, GPS-USDT, GPSUSDT -> GPSUSDT
        """
        return (sym or "").replace("_", "").replace("-", "").upper()
    
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
        # Порог 10x обычно достаточен; для очень волатильных инструментов можно сделать параметром (5x-20x)
        if v > fallback * 10 or v < fallback / 10:
            return fallback
        
        return v

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
            # Нормализуем coin один раз
            c = coin.upper()
            
            # Пробуем сначала с форматом CVC_USDT
            symbol = f"{c}_USDT"
            url = "/api/v1/contract/ticker"
            params = {"symbol": symbol}
            data = await self._request_json("GET", url, params=params)
            
            # Fallback: если не получили данные, code != 0, или dict без полезных полей - пробуем формат без подчеркивания (CVCUSDT)
            bad_code = isinstance(data, dict) and ("code" in data) and data.get("code") != 0
            # Проверяем, что dict не содержит полезных полей (data, result, code)
            # Для MEXC поля bids/asks/lastPrice/last обычно внутри data, поэтому проверяем только верхний уровень
            looks_empty = isinstance(data, dict) and not any(k in data for k in ("data", "result", "code"))
            if not data or bad_code or looks_empty:
                symbol_fallback = f"{c}USDT"
                logger.debug(f"MEXC: пробуем fallback формат {symbol_fallback} для {coin} (symbol={symbol})")
                params = {"symbol": symbol_fallback}
                data = await self._request_json("GET", url, params=params)
                if data and isinstance(data, dict) and ("code" in data) and data.get("code") == 0:
                    symbol = symbol_fallback
                    logger.debug(f"MEXC: fallback успешен, используем {symbol} для {coin}")
            
            if not data:
                logger.warning(f"MEXC: не удалось получить тикер для {coin}")
                return None
            
            # MEXC может возвращать данные в разных форматах
            item = None
            if isinstance(data, dict):
                # Проверяем code только если он присутствует
                if "code" in data and data.get("code") != 0:
                    code = data.get("code")
                    msg = data.get("msg", "Unknown error")
                    logger.warning(f"MEXC: API вернул ошибку для {coin}: code={code}, msg={msg}")
                    return None
                
                # Пробуем сначала data, потом result
                item = data.get("data")
                if item is None:
                    item = data.get("result")
                
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
                
                # Проверка, что price валиден (не мусор)
                if price <= 0:
                    return None
                
                bid = self._safe_px(bid_raw, price)
                ask = self._safe_px(ask_raw, price)
                
                # Если после проверок bid > ask (бывает на мусорных данных) — откатываем на price
                if bid > ask:
                    bid = price
                    ask = price
                
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
            # Нормализуем coin один раз
            c = coin.upper()
            
            # Пробуем сначала с форматом CVC_USDT
            symbol = f"{c}_USDT"
            url = "/api/v1/contract/funding_rate"
            params = {"symbol": symbol}
            data = await self._request_json("GET", url, params=params)
            
            # Fallback: если не получили данные, code != 0, или dict без полезных полей - пробуем формат без подчеркивания (CVCUSDT)
            bad_code = isinstance(data, dict) and ("code" in data) and data.get("code") != 0
            # Проверяем, что dict не содержит полезных полей (data, result, code)
            # Для MEXC поля bids/asks/lastPrice/last обычно внутри data, поэтому проверяем только верхний уровень
            looks_empty = isinstance(data, dict) and not any(k in data for k in ("data", "result", "code"))
            if not data or bad_code or looks_empty:
                symbol_fallback = f"{c}USDT"
                logger.debug(f"MEXC: пробуем fallback формат {symbol_fallback} для funding {coin} (symbol={symbol})")
                params = {"symbol": symbol_fallback}
                data = await self._request_json("GET", url, params=params)
                if data and isinstance(data, dict) and ("code" in data) and data.get("code") == 0:
                    symbol = symbol_fallback
                    logger.debug(f"MEXC: fallback успешен, используем {symbol} для funding {coin}")
            
            if not data:
                logger.warning(f"MEXC: не удалось получить фандинг для {coin}")
                return None
            
            # MEXC может возвращать данные в разных форматах
            # symbol_to_find строим от фактически используемого symbol
            symbol_to_find = self._canon(symbol)
            item = None
            
            if isinstance(data, dict):
                # Проверяем code только если он присутствует
                if "code" in data and data.get("code") != 0:
                    code = data.get("code")
                    msg = data.get("msg", "Unknown error")
                    logger.warning(f"MEXC: API вернул ошибку для фандинга {coin}: code={code}, msg={msg}")
                    return None
                
                # Пробуем сначала data, потом result
                item = data.get("data")
                if item is None:
                    item = data.get("result")
                
                # Если data и result оба None, это неожиданный формат
                if item is None:
                    logger.warning(f"MEXC: unexpected shape for funding {coin}: dict without data/result fields, keys={list(data.keys())}")
                    return None
                
                # Если item - это список, ищем элемент с правильным символом (с канонизацией)
                if isinstance(item, list) and item:
                    found = None
                    for elem in item:
                        if isinstance(elem, dict) and self._canon(elem.get("symbol")) == symbol_to_find:
                            found = elem
                            break
                    if not found:
                        logger.warning(f"MEXC: funding list but symbol not found: coin={coin}, tried={symbol}, want={symbol_to_find}")
                        return None
                    item = found
                elif isinstance(item, dict):
                    # Проверяем, что символ совпадает (с канонизацией)
                    item_symbol = item.get("symbol")
                    if item_symbol and self._canon(item_symbol) != symbol_to_find:
                        logger.warning(f"MEXC: получен фандинг для {item_symbol} вместо {symbol_to_find}")
                        return None
            elif isinstance(data, list) and data:
                # Ищем элемент с правильным символом (с канонизацией)
                found = None
                for elem in data:
                    if isinstance(elem, dict) and self._canon(elem.get("symbol")) == symbol_to_find:
                        found = elem
                        break
                if not found:
                    logger.warning(f"MEXC: funding list but symbol not found: coin={coin}, tried={symbol}, want={symbol_to_find}")
                    return None
                item = found
            
            if not item:
                logger.warning(f"MEXC: фандинг для {coin} не найден")
                return None
            
            # Проверяем, что item - это словарь
            if not isinstance(item, dict):
                logger.warning(f"MEXC: неожиданный формат данных для фандинга {coin}: {type(item)}")
                return None
            
            # Валидация symbol: проверяем, что полученный символ совпадает с запрошенным (особенно важно после fallback)
            item_symbol = item.get("symbol")
            if item_symbol:
                got_symbol = self._canon(item_symbol)
                if got_symbol != symbol_to_find:
                    logger.warning(f"MEXC: получен фандинг для {item_symbol} (canon={got_symbol}) вместо {symbol} (want raw) / {symbol_to_find} (want canon), пропускаем")
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
            # Нормализуем coin один раз
            c = coin.upper()
            
            # Пробуем сначала с форматом CVC_USDT
            symbol = f"{c}_USDT"
            url = "/api/v1/contract/depth"
            params = {"symbol": symbol, "limit": limit}
            data = await self._request_json("GET", url, params=params)
            
            # Fallback: если не получили данные, code != 0, или dict без полезных полей - пробуем формат без подчеркивания (CVCUSDT)
            bad_code = isinstance(data, dict) and ("code" in data) and data.get("code") != 0
            # Проверяем, что dict не содержит полезных полей (data, result, code)
            # Для MEXC поля bids/asks/lastPrice/last обычно внутри data, поэтому проверяем только верхний уровень
            looks_empty = isinstance(data, dict) and not any(k in data for k in ("data", "result", "code"))
            if not data or bad_code or looks_empty:
                symbol_fallback = f"{c}USDT"
                logger.debug(f"MEXC: пробуем fallback формат {symbol_fallback} для orderbook {coin} (symbol={symbol})")
                params = {"symbol": symbol_fallback, "limit": limit}
                data = await self._request_json("GET", url, params=params)
                if data and isinstance(data, dict) and ("code" in data) and data.get("code") == 0:
                    symbol = symbol_fallback
                    logger.debug(f"MEXC: fallback успешен, используем {symbol} для orderbook {coin}")
            
            if not data:
                logger.warning(f"MEXC: не удалось получить orderbook для {coin}")
                return None
            
            # MEXC возвращает данные в поле "data" или напрямую
            if isinstance(data, dict):
                # Проверяем code только если он присутствует
                if "code" in data and data.get("code") != 0:
                    code = data.get("code")
                    msg = data.get("msg", "Unknown error")
                    logger.warning(f"MEXC: API вернул ошибку для orderbook {coin}: code={code}, msg={msg}")
                    return None
                
                # Более корректная логика: пробуем сначала data, потом result, потом весь data
                result = data.get("data")
                if result is None:
                    result = data.get("result")
                if result is None:
                    result = data
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
            
            # Валидация формата уровней: убеждаемся, что bids[0] и asks[0] - это список/кортеж длины >= 2
            if (not isinstance(bids[0], (list, tuple)) or len(bids[0]) < 2 or
                not isinstance(asks[0], (list, tuple)) or len(asks[0]) < 2):
                logger.warning(f"MEXC: unexpected orderbook level format for {coin}: ожидается [[price, size], ...]")
                return None
            
            # Проверяем, что элементы можно конвертировать в float (защита от объектов/невалидных типов)
            try:
                float(bids[0][0])
                float(bids[0][1])
                float(asks[0][0])
                float(asks[0][1])
            except (TypeError, ValueError, IndexError) as e:
                logger.warning(f"MEXC: bids/asks top level not numeric for {coin}: {e}")
                return None
            
            return {"bids": bids, "asks": asks}
                
        except Exception as e:
            logger.error(f"MEXC: ошибка при получении orderbook для {coin}: {e}", exc_info=True)
            return None

