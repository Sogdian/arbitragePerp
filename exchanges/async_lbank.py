"""
Асинхронная реализация LBank для фьючерсов на базе AsyncBaseExchange

ВАЖНО: LBank, похоже, требует API ключи для доступа к данным о фьючерсах.
Публичный API для фьючерсов может быть недоступен или ограничен.

Если запросы не работают:
1. Проверьте официальную документацию LBank API
2. Возможно, требуется аутентификация через API ключи
3. Рассмотрите использование других бирж (Bybit, Gate.io, MEXC) для арбитража
"""
from typing import Dict, Optional
import logging
from .async_base_exchange import AsyncBaseExchange

logger = logging.getLogger(__name__)


class AsyncLbankExchange(AsyncBaseExchange):
    # LBank может использовать разные базовые URL для фьючерсов
    # Пробуем разные варианты базовых URL
    BASE_URL = "https://api.lbank.com"
    
    def __init__(self, pool_limit: int = 100):
        super().__init__("LBank", pool_limit)
        # Альтернативные базовые URL для фьючерсов
        self.alternative_base_urls = [
            "https://www.lbank.com",
            "https://api.lbank.info",
        ]

    def _normalize_symbol(self, coin: str) -> str:
        """Преобразует монету в формат LBank для фьючерсов (например, CVC -> CVCUSDT)"""
        # LBank может использовать формат без подчеркивания
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
            # Пробуем только основной URL и один альтернативный
            # LBank, похоже, не предоставляет публичный API для фьючерсов
            base_urls_to_try = [self.BASE_URL, "https://www.lbank.com"]
            
            # Основной эндпоинт
            url = "/v2/futures/ticker"
            param_name = "symbol"
            
            data = None
            for base_url in base_urls_to_try:
                # Временно меняем базовый URL клиента
                original_base_url = self.client.base_url
                try:
                    self.client.base_url = base_url
                    # Пробуем только основной формат символа
                    params = {param_name: symbol}
                    data = await self._request_json("GET", url, params=params)
                    if data:
                        logger.info(f"LBank: успешный запрос к {base_url}{url}")
                        # Парсим данные тикера
                        parsed = self._parse_ticker_response(data, coin)
                        if parsed:
                            return parsed
                finally:
                    self.client.base_url = original_base_url
                
                if data:
                    break
            
            if not data:
                logger.warning(f"LBank: не удалось получить тикер для {coin}")
                logger.warning(f"LBank: публичный API для фьючерсов недоступен. LBank не предоставляет публичный API для получения данных о фьючерсах или требует аутентификацию.")
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
                item = data["data"]
            elif "result" in data:
                item = data["result"]
            else:
                item = data
        elif isinstance(data, list) and data:
            item = data[0]
        
        if not item:
            logger.warning(f"LBank: тикер для {coin} не найден в ответе")
            return None
        
        # Извлекаем цены (проверяем разные возможные поля)
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
            Ставка фандинга (например, 0.0001 = 0.01%) или None если ошибка
        """
        try:
            symbol = self._normalize_symbol(coin)
            # Пробуем только основной URL и один альтернативный
            # LBank, похоже, не предоставляет публичный API для фьючерсов
            base_urls_to_try = [self.BASE_URL, "https://www.lbank.com"]
            
            url = "/v2/futures/fundingRate"
            param_name = "symbol"
            
            data = None
            for base_url in base_urls_to_try:
                original_base_url = self.client.base_url
                try:
                    self.client.base_url = base_url
                    # Пробуем только основной формат символа
                    params = {param_name: symbol}
                    data = await self._request_json("GET", url, params=params)
                    if data:
                        logger.info(f"LBank: успешный запрос к {base_url}{url}")
                        parsed = self._parse_funding_response(data, coin)
                        if parsed is not None:
                            return parsed
                finally:
                    self.client.base_url = original_base_url
                
                if data:
                    break
            
            if not data:
                logger.warning(f"LBank: не удалось получить фандинг для {coin}")
                logger.warning(f"LBank: публичный API для фьючерсов недоступен. LBank не предоставляет публичный API для получения данных о фьючерсах или требует аутентификацию.")
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
                if isinstance(data_list, list) and data_list:
                    item = data_list[0]
                elif isinstance(data_list, dict):
                    item = data_list
            elif "result" in data:
                result = data["result"]
                if isinstance(result, list) and result:
                    item = result[0]
                elif isinstance(result, dict):
                    item = result
            else:
                item = data
        elif isinstance(data, list) and data:
            item = data[0]
        
        if not item:
            logger.warning(f"LBank: фандинг для {coin} не найден в ответе")
            return None
        
        # LBank может возвращать ставку в разных полях
        funding_rate_raw = item.get("fundingRate") or item.get("rate") or item.get("r") or item.get("funding_rate")
        
        if funding_rate_raw is None:
            logger.warning(f"LBank: нет ставки фандинга для {coin} в ответе: {item}")
            return None
        
        try:
            funding_rate = float(funding_rate_raw)
            return funding_rate
        except (ValueError, TypeError) as e:
            logger.warning(f"LBank: ошибка парсинга фандинга для {coin}: {e}")
            return None

