"""
Асинхронная реализация LBank для фьючерсов на базе AsyncBaseExchange

Использует публичный API LBank для фьючерсов:
- Базовый URL: https://lbkperp.lbank.com
- Эндпоинты: /cfd/openApi/v1/pub/*
"""
from typing import Dict, Optional
import logging
import time
from .async_base_exchange import AsyncBaseExchange

logger = logging.getLogger(__name__)


class AsyncLbankExchange(AsyncBaseExchange):
    # LBank использует отдельный домен для фьючерсов (perp)
    BASE_URL = "https://lbkperp.lbank.com"
    PRODUCT_GROUP = "SwapU"  # Обязательный параметр для получения данных
    
    def __init__(self, pool_limit: int = 100):
        super().__init__("LBank", pool_limit)
        # Кеш для списка инструментов (TTL 5 минут)
        self._instruments_cache: Optional[list] = None
        self._instruments_cache_ts: float = 0.0
        self._instruments_cache_ttl: float = 300.0  # 5 минут

    def _normalize_symbol(self, coin: str) -> str:
        """Преобразует монету в формат LBank для фьючерсов (например, CVC -> CVCUSDT)"""
        # LBank использует формат без подчеркивания для фьючерсов
        return f"{coin.upper()}USDT"
    
    def _canon(self, sym: str) -> str:
        """
        Канонизирует символ: убирает - и _ для сравнения
        Например: GPS-USDT, GPS_USDT, GPSUSDT -> GPSUSDT
        """
        return (sym or "").replace("-", "").replace("_", "").upper()
    
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
    
    def _check_api_error(self, data: dict, context: str) -> bool:
        """
        Проверяет наличие ошибок API в ответе
        
        Args:
            data: Ответ от API
            context: Контекст для логирования (например, "ticker")
            
        Returns:
            True если есть ошибка (и нужно вернуть None), False если всё ок
        """
        if not isinstance(data, dict):
            return False
        
        if data.get("success") is False:
            error_code = data.get("error_code")
            msg = data.get("msg", "Unknown error")
            logger.warning(f"LBank: API failure ({context}): error_code={error_code}, msg={msg}")
            return True
        
        error_code = data.get("error_code")
        if error_code is not None and str(error_code) != "0":
            msg = data.get("msg", "Unknown error")
            logger.warning(f"LBank: API error_code={error_code} ({context}), msg={msg}")
            return True
        
        return False
    
    def _pick_market_item(self, data: dict, symbol_to_use: str) -> Optional[dict]:
        """
        Выбирает из marketData ровно тот элемент, у которого symbol == symbol_to_use.
        
        Args:
            data: Ответ от API marketData
            symbol_to_use: Символ для поиска (например, "IOTAUSDT")
            
        Returns:
            Словарь с данными инструмента или None если не найден
        """
        if not isinstance(data, dict):
            return None
        
        # Проверяем разные возможные поля: data, result
        payload = None
        if "data" in data:
            payload = data.get("data")
        elif "result" in data:
            payload = data.get("result")
        
        if payload is None:
            return None
        
        # Иногда приходит dict, иногда list
        symbol_canon = self._canon(symbol_to_use)
        
        if isinstance(payload, dict):
            item_symbol = payload.get("symbol") or ""
            if self._canon(item_symbol) == symbol_canon:
                return payload
            return None
        
        if isinstance(payload, list):
            for it in payload:
                if isinstance(it, dict):
                    item_symbol = it.get("symbol") or ""
                    if self._canon(item_symbol) == symbol_canon:
                        return it
            return None
        
        return None
    
    async def _get_instruments_with_cache(self) -> Optional[list]:
        """
        Получить список доступных инструментов на LBank с кешированием
        
        Returns:
            Список инструментов или None если ошибка
        """
        # Проверяем кеш
        current_time = time.time()
        if (self._instruments_cache is not None and 
            current_time - self._instruments_cache_ts < self._instruments_cache_ttl):
            return self._instruments_cache
        
        try:
            url = "/cfd/openApi/v1/pub/instrument"
            params = {"productGroup": self.PRODUCT_GROUP}
            data = await self._request_json("GET", url, params=params)
            if data and data.get("data") and isinstance(data["data"], list):
                # Обновляем кеш
                self._instruments_cache = data["data"]
                self._instruments_cache_ts = current_time
                return self._instruments_cache
            return None
        except Exception as e:
            logger.debug(f"LBank: ошибка при получении списка инструментов: {e}")
            return None
    
    async def resolve_symbol(self, coin: str) -> str:
        """
        Разрешает символ монеты в правильный формат для LBank API
        
        Args:
            coin: Название монеты без /USDT (например, "IOTA")
            
        Returns:
            Правильный символ для использования в API (например, "IOTAUSDT")
        """
        symbol = self._normalize_symbol(coin)  # IOTAUSDT
        
        # Получаем список инструментов (с кешем)
        instruments_list = await self._get_instruments_with_cache()
        if not instruments_list:
            # Если не удалось получить список, используем нормализованный символ
            return symbol
        
        # Ищем точное совпадение (с канонизацией)
        symbol_canon = self._canon(symbol)
        for instrument in instruments_list:
            if isinstance(instrument, dict):
                inst_symbol = instrument.get("symbol") or instrument.get("instrumentId") or instrument.get("instrument_id")
                if inst_symbol and self._canon(inst_symbol) == symbol_canon:
                    logger.debug(f"LBank: найден точный символ {inst_symbol} для {coin}")
                    return inst_symbol
        
        # Если точное совпадение не найдено, возвращаем нормализованный символ
        return symbol

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
            # Разрешаем правильный символ (с кешем инструментов)
            symbol_to_use = await self.resolve_symbol(coin)
            
            # Пробуем получить данные через marketData с правильным символом
            url = "/cfd/openApi/v1/pub/marketData"
            params = {"productGroup": self.PRODUCT_GROUP, "symbol": symbol_to_use}
            data = await self._request_json("GET", url, params=params)
            
            # Проверяем ошибки API
            if data and isinstance(data, dict) and self._check_api_error(data, "ticker"):
                return None
            
            # Всегда используем _pick_market_item для поиска нужного символа
            if data and isinstance(data, dict):
                item = self._pick_market_item(data, symbol_to_use)
                if item:
                    parsed = self._parse_ticker_response({"data": item}, coin)
                    if parsed:
                        return parsed
            
            # fallback: грузим весь список и ищем точное совпадение
            data = await self._request_json("GET", url, params={"productGroup": self.PRODUCT_GROUP})
            
            # Проверяем ошибки API
            if data and isinstance(data, dict) and self._check_api_error(data, "ticker (fallback)"):
                return None
            
            item = self._pick_market_item(data, symbol_to_use)
            if item:
                parsed = self._parse_ticker_response({"data": item}, coin)
                if parsed:
                    return parsed
            
            logger.warning(f"LBank: не удалось получить тикер для {coin}. API вернул пустой массив данных.")
            logger.warning(f"LBank: проверьте доступность символа {coin} на LBank фьючерсах.")
            return None
                
        except Exception as e:
            logger.error(f"LBank: ошибка при получении тикера для {coin}: {e}", exc_info=True)
            return None
    
    def _parse_ticker_response(self, data, coin: str) -> Optional[Dict]:
        """Парсит ответ API для тикера"""
        # LBank может возвращать данные в разных форматах
        item = None
        if isinstance(data, dict):
            if "data" in data:
                data_list = data["data"]
                # Проверяем, что data не пустой массив
                if isinstance(data_list, list):
                    if len(data_list) > 0:
                        item = data_list[0]
                    else:
                        # Пустой массив - символ не найден
                        return None
                elif isinstance(data_list, dict):
                    item = data_list
            elif "result" in data:
                item = data["result"]
            else:
                item = data
        elif isinstance(data, list) and len(data) > 0:
            item = data[0]
        
        if not item:
            return None
        
        # Для depth эндпоинта: bids и asks - это массивы [цена, количество]
        if "bids" in item and "asks" in item:
            bids = item.get("bids", [])
            asks = item.get("asks", [])
            
            if bids and asks:
                try:
                    bid = float(bids[0][0]) if isinstance(bids[0], (list, tuple)) else float(bids[0])
                    ask = float(asks[0][0]) if isinstance(asks[0], (list, tuple)) else float(asks[0])
                    # Используем среднюю цену как текущую цену
                    price = (bid + ask) / 2
                    
                    return {
                        "price": price,
                        "bid": bid,
                        "ask": ask,
                    }
                except (ValueError, TypeError, IndexError) as e:
                    logger.warning(f"LBank: ошибка парсинга depth для {coin}: {e}")
        
        # Для marketData эндпоинта: проверяем стандартные поля
        # Приоритет: lastPrice (официальное поле) > last > close > price > latestPrice
        # НЕ используем markPrice как last без необходимости (это может быть другая цена)
        last_price_raw = (item.get("lastPrice") or item.get("last") or item.get("close") or 
                        item.get("price") or item.get("latestPrice"))
        bid_raw = (item.get("bidPrice") or item.get("bid1") or item.get("bid") or 
                  item.get("bestBid") or item.get("buy"))
        ask_raw = (item.get("askPrice") or item.get("ask1") or item.get("ask") or 
                  item.get("bestAsk") or item.get("sell"))
        
        if not last_price_raw:
            logger.warning(f"LBank: нет цены для {coin} в ответе. Доступные поля: {list(item.keys()) if isinstance(item, dict) else 'N/A'}")
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
            logger.warning(f"LBank: ошибка парсинга цен для {coin}: {e}")
            return None

    async def get_funding_rate(self, coin: str) -> Optional[float]:
        """
        Получить текущую ставку фандинга для монеты
        
        Args:
            coin: Название монеты без /USDT (например, "CVC")
            
        Returns:
            Ставка фандинга в decimal формате (например, 0.0001 = 0.01%) или None если ошибка
            Примечание: bot.py умножает на 100 для отображения в процентах
        """
        try:
            # Разрешаем правильный символ (с кешем инструментов)
            symbol_to_use = await self.resolve_symbol(coin)
            
            # Пробуем получить фандинг через marketData с правильным символом
            url = "/cfd/openApi/v1/pub/marketData"
            params = {"productGroup": self.PRODUCT_GROUP, "symbol": symbol_to_use}
            data = await self._request_json("GET", url, params=params)
            
            # Проверяем ошибки API
            if data and isinstance(data, dict) and self._check_api_error(data, "funding"):
                return None
            
            # Всегда используем _pick_market_item для поиска нужного символа
            if data and isinstance(data, dict):
                item = self._pick_market_item(data, symbol_to_use)
                if item:
                    parsed = self._parse_funding_response({"data": item}, coin)
                    if parsed is not None:
                        return parsed
            
            # fallback: полный список
            data = await self._request_json("GET", url, params={"productGroup": self.PRODUCT_GROUP})
            
            # Проверяем ошибки API
            if data and isinstance(data, dict) and self._check_api_error(data, "funding (fallback)"):
                return None
            
            item = self._pick_market_item(data, symbol_to_use)
            if item:
                parsed = self._parse_funding_response({"data": item}, coin)
                if parsed is not None:
                    return parsed
            
            logger.warning(f"LBank: не удалось получить фандинг для {coin}. API вернул пустой массив данных.")
            logger.warning(f"LBank: проверьте доступность символа {coin} на LBank фьючерсах.")
            return None
                
        except Exception as e:
            logger.error(f"LBank: ошибка при получении фандинга для {coin}: {e}", exc_info=True)
            return None
    
    def _parse_funding_response(self, data, coin: str) -> Optional[float]:
        """Парсит ответ API для фандинга"""
        # LBank может возвращать данные в разных форматах
        item = None
        if isinstance(data, dict):
            if "data" in data:
                data_list = data["data"]
                # Проверяем, что data не пустой массив
                if isinstance(data_list, list):
                    if len(data_list) > 0:
                        # Для истории фандинга - берем первую (последнюю) запись
                        item = data_list[0]
                    else:
                        # Пустой массив - символ не найден
                        return None
                elif isinstance(data_list, dict):
                    item = data_list
            elif "result" in data:
                result = data["result"]
                if isinstance(result, list) and len(result) > 0:
                    item = result[0]
                elif isinstance(result, dict):
                    item = result
            else:
                item = data
        elif isinstance(data, list) and len(data) > 0:
            item = data[0]
        
        if not item:
            return None
        
        # LBank может возвращать ставку в разных полях
        # Приоритет: fundingRate (основное поле) > positionFeeRate (альтернатива)
        funding_rate_raw = item.get("fundingRate")
        if funding_rate_raw is None:
            funding_rate_raw = item.get("positionFeeRate")
        if funding_rate_raw is None:
            # Пробуем другие поля
            funding_rate_raw = (item.get("rate") or item.get("r") or
                              item.get("funding_rate") or
                              item.get("fundRate") or item.get("fund_rate"))
        
        if funding_rate_raw is None:
            logger.warning(f"LBank: нет ставки фандинга для {coin} в ответе. Доступные поля: {list(item.keys()) if isinstance(item, dict) else 'N/A'}")
            return None
        
        try:
            funding_rate_decimal = float(funding_rate_raw)   # например 0.00001122
            # Возвращаем в decimal формате (как другие биржи), bot.py умножит на 100 для отображения
            return funding_rate_decimal
        except (ValueError, TypeError) as e:
            logger.warning(f"LBank: ошибка парсинга фандинга для {coin}: {e}")
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
            # Разрешаем правильный символ (с кешем инструментов)
            symbol = await self.resolve_symbol(coin)
            
            # LBank использует эндпоинт depth для получения orderbook
            url = "/cfd/openApi/v1/pub/depth"
            params = {
                "productGroup": self.PRODUCT_GROUP,
                "symbol": symbol,
                "limit": limit
            }
            
            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"LBank: не удалось получить orderbook для {coin}")
                return None
            
            # Проверяем ошибки API
            if isinstance(data, dict) and self._check_api_error(data, "orderbook"):
                return None
            
            # LBank возвращает данные в поле "data"
            result = data.get("data")
            if not result:
                logger.warning(f"LBank: пустой ответ orderbook для {coin}")
                return None
            
            # Если result - это список, берем первый элемент
            if isinstance(result, list) and result:
                result = result[0]
            
            if not isinstance(result, dict):
                logger.warning(f"LBank: неожиданный формат orderbook для {coin}")
                return None
            
            bids = result.get("bids", [])
            asks = result.get("asks", [])
            
            if not bids or not asks:
                logger.warning(f"LBank: пустой orderbook для {coin}")
                return None
            
            # Валидация формата уровней: убеждаемся, что bids[0] и asks[0] - это список/кортеж длины >= 2
            if (not isinstance(bids[0], (list, tuple)) or len(bids[0]) < 2 or
                not isinstance(asks[0], (list, tuple)) or len(asks[0]) < 2):
                logger.warning(f"LBank: неверный формат orderbook уровней для {coin}: ожидается [[price, size], ...]")
                return None
            
            # Проверяем, что элементы можно конвертировать в float (защита от объектов/невалидных типов)
            try:
                float(bids[0][0])
                float(bids[0][1])
                float(asks[0][0])
                float(asks[0][1])
            except (TypeError, ValueError, IndexError) as e:
                logger.warning(f"LBank: bids/asks top level not numeric for {coin}: {e}")
                return None
            
            return {"bids": bids, "asks": asks}
                
        except Exception as e:
            logger.error(f"LBank: ошибка при получении orderbook для {coin}: {e}", exc_info=True)
            return None

