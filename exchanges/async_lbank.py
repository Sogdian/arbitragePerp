"""
Асинхронная реализация LBank для фьючерсов на базе AsyncBaseExchange

Использует публичный API LBank для фьючерсов:
- Базовый URL: https://lbkperp.lbank.com
- Эндпоинты: /cfd/openApi/v1/pub/*
"""
from typing import Dict, Optional
import logging
from .async_base_exchange import AsyncBaseExchange

logger = logging.getLogger(__name__)


class AsyncLbankExchange(AsyncBaseExchange):
    # LBank использует отдельный домен для фьючерсов (perp)
    BASE_URL = "https://lbkperp.lbank.com"
    PRODUCT_GROUP = "SwapU"  # Обязательный параметр для получения данных
    
    def __init__(self, pool_limit: int = 100):
        super().__init__("LBank", pool_limit)

    def _normalize_symbol(self, coin: str) -> str:
        """Преобразует монету в формат LBank для фьючерсов (например, CVC -> CVCUSDT)"""
        # LBank использует формат без подчеркивания для фьючерсов
        return f"{coin.upper()}USDT"
    
    def _pick_market_item(self, data: dict, symbol_to_use: str) -> Optional[dict]:
        """
        Выбирает из marketData ровно тот элемент, у которого symbol == symbol_to_use.
        
        Args:
            data: Ответ от API marketData
            symbol_to_use: Символ для поиска (например, "IOTAUSDT")
            
        Returns:
            Словарь с данными инструмента или None если не найден
        """
        if not isinstance(data, dict) or "data" not in data:
            return None
        
        symbol_upper = symbol_to_use.upper()
        payload = data.get("data")
        
        # Иногда приходит dict, иногда list
        if isinstance(payload, dict):
            item_symbol = (payload.get("symbol") or "").upper()
            return payload if item_symbol == symbol_upper else None
        
        if isinstance(payload, list):
            for it in payload:
                if isinstance(it, dict):
                    item_symbol = (it.get("symbol") or "").upper()
                    if item_symbol == symbol_upper:
                        return it
            return None
        
        return None
    
    async def get_available_instruments(self) -> Optional[list]:
        """
        Получить список доступных инструментов на LBank
        Может быть полезно для отладки формата символов
        """
        try:
            url = "/cfd/openApi/v1/pub/instrument"
            params = {"productGroup": self.PRODUCT_GROUP}
            data = await self._request_json("GET", url, params=params)
            if data and data.get("data") and isinstance(data["data"], list):
                return data["data"]
            return None
        except Exception as e:
            logger.debug(f"LBank: ошибка при получении списка инструментов: {e}")
            return None

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
            symbol = self._normalize_symbol(coin)  # IOTAUSDT
            
            # Сначала получаем список всех инструментов, чтобы найти правильный формат символа
            url = "/cfd/openApi/v1/pub/instrument"
            instruments_data = await self._request_json("GET", url, params={"productGroup": self.PRODUCT_GROUP})
            
            correct_symbol = None
            if instruments_data and instruments_data.get("data"):
                instruments_list = instruments_data.get("data")
                if isinstance(instruments_list, list):
                    # Ищем наш символ в списке инструментов
                    for instrument in instruments_list:
                        if isinstance(instrument, dict):
                            inst_symbol = instrument.get("symbol") or instrument.get("instrumentId") or instrument.get("instrument_id")
                            if inst_symbol:
                                # Проверяем точное совпадение (приоритет) или точное окончание
                                inst_symbol_upper = inst_symbol.upper()
                                symbol_upper = symbol.upper()
                                if symbol_upper == inst_symbol_upper:
                                    # Точное совпадение - это то, что нужно
                                    correct_symbol = inst_symbol
                                    logger.debug(f"LBank: найден точный символ {correct_symbol} для {coin}")
                                    break
                                elif inst_symbol_upper.endswith(symbol_upper) and len(inst_symbol_upper) == len(symbol_upper):
                                    # Точное окончание без дополнительных символов
                                    correct_symbol = inst_symbol
                                    logger.debug(f"LBank: найден символ {correct_symbol} для {coin} (по окончанию)")
                                    # Не break, продолжаем искать точное совпадение
            
            # Используем найденный символ или исходный
            symbol_to_use = correct_symbol or symbol
            
            # Пробуем получить данные через marketData с правильным символом
            url = "/cfd/openApi/v1/pub/marketData"
            params = {"productGroup": self.PRODUCT_GROUP, "symbol": symbol_to_use}
            data = await self._request_json("GET", url, params=params)
            
            item = self._pick_market_item(data, symbol_to_use)
            if item:
                parsed = self._parse_ticker_response({"data": item}, coin)
                if parsed:
                    return parsed
            
            # fallback: грузим весь список и ищем точное совпадение
            data = await self._request_json("GET", url, params={"productGroup": self.PRODUCT_GROUP})
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
                    bid = float(bids[0][0]) if isinstance(bids[0], list) else float(bids[0])
                    ask = float(asks[0][0]) if isinstance(asks[0], list) else float(asks[0])
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
        last_price_raw = (item.get("lastPrice") or item.get("last") or item.get("close") or 
                        item.get("price") or item.get("latestPrice") or item.get("markPrice"))
        bid_raw = (item.get("bidPrice") or item.get("bid1") or item.get("bid") or 
                  item.get("bestBid") or item.get("buy"))
        ask_raw = (item.get("askPrice") or item.get("ask1") or item.get("ask") or 
                  item.get("bestAsk") or item.get("sell"))
        
        if not last_price_raw:
            logger.warning(f"LBank: нет цены для {coin} в ответе. Доступные поля: {list(item.keys()) if isinstance(item, dict) else 'N/A'}")
            return None
        
        try:
            price = float(last_price_raw)
            bid = float(bid_raw) if bid_raw else price
            ask = float(ask_raw) if ask_raw else price
            
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
            symbol = self._normalize_symbol(coin)  # IOTAUSDT
            
            # Сначала получаем список всех инструментов, чтобы найти правильный формат символа
            url = "/cfd/openApi/v1/pub/instrument"
            instruments_data = await self._request_json("GET", url, params={"productGroup": self.PRODUCT_GROUP})
            
            correct_symbol = None
            if instruments_data and instruments_data.get("data"):
                instruments_list = instruments_data.get("data")
                if isinstance(instruments_list, list):
                    # Ищем наш символ в списке инструментов
                    for instrument in instruments_list:
                        if isinstance(instrument, dict):
                            inst_symbol = instrument.get("symbol") or instrument.get("instrumentId") or instrument.get("instrument_id")
                            if inst_symbol:
                                # Проверяем точное совпадение (приоритет) или точное окончание
                                inst_symbol_upper = inst_symbol.upper()
                                symbol_upper = symbol.upper()
                                if symbol_upper == inst_symbol_upper:
                                    # Точное совпадение - это то, что нужно
                                    correct_symbol = inst_symbol
                                    logger.debug(f"LBank: найден точный символ {correct_symbol} для {coin} (фандинг)")
                                    break
                                elif inst_symbol_upper.endswith(symbol_upper) and len(inst_symbol_upper) == len(symbol_upper):
                                    # Точное окончание без дополнительных символов
                                    correct_symbol = inst_symbol
                                    logger.debug(f"LBank: найден символ {correct_symbol} для {coin} (фандинг, по окончанию)")
                                    # Не break, продолжаем искать точное совпадение
            
            # Используем найденный символ или исходный
            symbol_to_use = correct_symbol or symbol
            
            # Пробуем получить фандинг через marketData с правильным символом
            url = "/cfd/openApi/v1/pub/marketData"
            params = {"productGroup": self.PRODUCT_GROUP, "symbol": symbol_to_use}
            data = await self._request_json("GET", url, params=params)
            
            item = self._pick_market_item(data, symbol_to_use)
            if item:
                item_symbol = item.get('symbol')
                if item_symbol and item_symbol.upper() != symbol_to_use.upper():
                    logger.warning(f"LBank: ВНИМАНИЕ! Выбран неправильный символ: {item_symbol} вместо {symbol_to_use}")
                parsed = self._parse_funding_response({"data": item}, coin)
                if parsed is not None:
                    return parsed
            
            # fallback: полный список
            data = await self._request_json("GET", url, params={"productGroup": self.PRODUCT_GROUP})
            item = self._pick_market_item(data, symbol_to_use)
            if item:
                item_symbol = item.get('symbol')
                if item_symbol and item_symbol.upper() != symbol_to_use.upper():
                    logger.warning(f"LBank: ВНИМАНИЕ! Выбран неправильный символ в fallback: {item_symbol} вместо {symbol_to_use}")
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
        position_fee_rate_raw = item.get("positionFeeRate")
        
        # Используем fundingRate если есть, иначе positionFeeRate
        if funding_rate_raw is not None:
            funding_rate_raw = funding_rate_raw
        elif position_fee_rate_raw is not None:
            funding_rate_raw = position_fee_rate_raw
        else:
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

