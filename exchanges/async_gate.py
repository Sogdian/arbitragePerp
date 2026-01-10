"""
Асинхронная реализация Gate.io для фьючерсов на базе AsyncBaseExchange
"""
from typing import Dict, Optional
import logging
from .async_base_exchange import AsyncBaseExchange

logger = logging.getLogger(__name__)


class AsyncGateExchange(AsyncBaseExchange):
    BASE_URL = "https://api.gateio.ws"

    def __init__(self):
        super().__init__("Gate")

    def _normalize_symbol(self, coin: str) -> str:
        """Преобразует монету в формат Gate.io для фьючерсов (например, CVC -> CVC_USDT)"""
        return f"{coin.upper()}_USDT"

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
            url = "/api/v4/futures/usdt/tickers"
            params = {"contract": symbol}
            data = await self._request_json("GET", url, params=params)
            
            if not data:
                logger.warning(f"Gate: не удалось получить тикер для {coin}")
                return None
            
            # Gate.io возвращает список или словарь
            # Если список - ищем точный контракт, иначе берем первый элемент
            if isinstance(data, list) and data:
                # Попробуем найти точный контракт
                item = next((x for x in data if isinstance(x, dict) and x.get("contract") == symbol), data[0])
            elif isinstance(data, dict):
                item = data
            else:
                item = None
            
            if not item:
                logger.warning(f"Gate: тикер для {coin} не найден")
                return None
            
            last_raw = item.get("last")
            if last_raw is None:
                logger.warning(f"Gate: нет last для {coin}: {item}")
                return None
            
            last = float(last_raw)
            
            # Проверка, что last_price тоже валиден (не мусор)
            if last <= 0:
                return None
            
            def _safe_px(raw: object, fallback: float, sanity_mult: float = 20.0) -> float:
                """
                Безопасное преобразование цены с проверками на разумность
                
                Args:
                    raw: Сырое значение цены (может быть строкой, числом, None, или мусором)
                    fallback: Значение по умолчанию (обычно last_price)
                    sanity_mult: Множитель для sanity-check (по умолчанию 20x)
                                 Для микро-альтов с очень маленькой ценой можно увеличить или отключить
                    
                Returns:
                    Валидная цена или fallback
                """
                try:
                    v = float(raw)
                except (TypeError, ValueError):
                    return fallback
                
                if v <= 0:
                    return fallback
                
                # Sanity check: если отличается от fallback больше чем в sanity_mult раз — считаем мусором
                # Для микро-альтов с ценой < 0.0001 отключаем sanity-check (могут быть реальные скачки)
                # Порог по умолчанию 20x (можно увеличить для волатильных инструментов)
                if fallback >= 0.0001:  # Для нормальных цен применяем sanity-check
                    if v > fallback * sanity_mult or v < fallback / sanity_mult:
                        return fallback
                
                return v
            
            # Поддерживаем разные названия bid/ask
            bid_raw = item.get("bid") or item.get("highest_bid")
            ask_raw = item.get("ask") or item.get("lowest_ask")
            
            bid = _safe_px(bid_raw, last)
            ask = _safe_px(ask_raw, last)
            
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
            logger.error(f"Gate: ошибка при получении тикера для {coin}: {e}", exc_info=True)
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
            url = "/api/v4/futures/usdt/funding_rate"
            params = {"contract": symbol, "limit": 1}
            
            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"Gate: не удалось получить фандинг для {coin}")
                return None
            
            # ВАЖНО: API возвращает list вида [{"t": 1543968000, "r": "0.000157"}]
            if isinstance(data, list) and data:
                item = data[0]
            elif isinstance(data, dict) and "r" in data:
                # на всякий случай, если вдруг вернули dict
                item = data
            else:
                logger.warning(f"Gate: неожиданный формат funding_rate ответа для {coin}: {type(data)}")
                return None
            
            r = item.get("r") or item.get("funding_rate") or item.get("rate")
            if r is None:
                logger.warning(f"Gate: нет поля funding rate в ответе: {item}")
                return None
            
            return float(r)
                
        except Exception as e:
            logger.error(f"Gate: ошибка при получении фандинга для {coin}: {e}", exc_info=True)
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
            symbol = self._normalize_symbol(coin)
            url = "/api/v4/futures/usdt/order_book"
            params = {"contract": symbol, "limit": limit}
            
            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"Gate: не удалось получить orderbook для {coin}")
                return None
            
            # Gate.io возвращает словарь с bids и asks
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            
            if not bids or not asks:
                logger.warning(f"Gate: пустой orderbook для {coin}")
                return None
            
            # Валидация формата уровней: убеждаемся, что bids[0] и asks[0] - это список/кортеж длины 2
            if bids and (not isinstance(bids[0], (list, tuple)) or len(bids[0]) != 2):
                logger.warning(f"Gate: неверный формат bids для {coin}: ожидается [[price, size], ...]")
                return None
            
            if asks and (not isinstance(asks[0], (list, tuple)) or len(asks[0]) != 2):
                logger.warning(f"Gate: неверный формат asks для {coin}: ожидается [[price, size], ...]")
                return None
            
            # Проверяем, что элементы можно конвертировать в float (защита от объектов/невалидных типов)
            try:
                float(bids[0][0])
                float(bids[0][1])
                float(asks[0][0])
                float(asks[0][1])
            except (TypeError, ValueError, IndexError) as e:
                logger.warning(f"Gate: неверный тип данных в orderbook для {coin}: элементы не конвертируются в float: {e}")
                return None
            
            return {"bids": bids, "asks": asks}
                
        except Exception as e:
            logger.error(f"Gate: ошибка при получении orderbook для {coin}: {e}", exc_info=True)
            return None

