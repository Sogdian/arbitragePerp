"""
Асинхронная реализация XT.com для фьючерсов на базе AsyncBaseExchange

Фиксированная логика:
- Рынок: Futures
- Тип: Perpetual
- Символ: coin_usdt (нижний регистр, с подчеркиванием)
- Если coin_usdt не найден → считаем, что инструмента нет
"""
from typing import Dict, Optional, Set
import logging
from .async_base_exchange import AsyncBaseExchange
from .coin_list_fetchers import fetch_xt_coins

logger = logging.getLogger(__name__)


class AsyncXtExchange(AsyncBaseExchange):
    BASE_URL = "https://fapi.xt.com"

    def __init__(self, pool_limit: int = 100):
        super().__init__("XT", pool_limit)

    def _normalize_symbol(self, coin: str) -> str:
        """Преобразует монету в формат XT для фьючерсов (например, CVC -> cvc_usdt)"""
        return f"{coin.lower()}_usdt"

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
            url = "/future/market/v1/public/q/ticker"
            params = {"symbol": symbol}
            
            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"XT: не удалось получить ответ для тикера {coin} (символ: {symbol})")
                return None
            
            return_code = data.get("returnCode")
            if return_code != 0:
                msg = data.get("msgInfo", "Unknown error")
                logger.warning(f"XT: API вернул ошибку для тикера {coin} (символ: {symbol}): returnCode={return_code}, msg={msg}")
                return None
            
            result = data.get("result")
            if not result or not isinstance(result, dict):
                logger.warning(f"XT: тикер для {coin} не найден (символ: {symbol}, result пустой или не словарь)")
                return None
            
            # XT возвращает: c = last price, b = bid, a = ask
            last_price = result.get("c")
            if last_price is None:
                return None
            
            last = float(last_price)
            bid_raw = result.get("b")
            ask_raw = result.get("a")
            
            # Проверка на разумность значений: если bid/ask сильно отличаются от last_price (> 10x), вероятно ошибка
            # Используем last_price как fallback
            if bid_raw:
                bid_val = float(bid_raw)
                if bid_val > 0 and (bid_val > last * 10 or bid_val < last / 10):
                    bid = last
                else:
                    bid = bid_val
            else:
                bid = last
            
            if ask_raw:
                ask_val = float(ask_raw)
                if ask_val > 0 and (ask_val > last * 10 or ask_val < last / 10):
                    ask = last
                else:
                    ask = ask_val
            else:
                ask = last
            
            return {
                "price": last,
                "bid": bid,
                "ask": ask,
            }
                
        except Exception as e:
            logger.error(f"XT: ошибка при получении тикера для {coin}: {e}", exc_info=True)
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
            url = "/future/market/v1/public/q/funding-rate"
            params = {"symbol": symbol}
            
            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"XT: не удалось получить ответ для фандинга {coin} (символ: {symbol})")
                return None
            
            return_code = data.get("returnCode")
            if return_code != 0:
                msg = data.get("msgInfo", "Unknown error")
                logger.warning(f"XT: API вернул ошибку для фандинга {coin} (символ: {symbol}): returnCode={return_code}, msg={msg}")
                return None
            
            result = data.get("result")
            if not result or not isinstance(result, dict):
                logger.warning(f"XT: фандинг для {coin} не найден (символ: {symbol}, result пустой или не словарь)")
                return None
            
            funding_rate_raw = result.get("fundingRate")
            if funding_rate_raw is None:
                return None
            
            return float(funding_rate_raw)
                
        except Exception as e:
            logger.error(f"XT: ошибка при получении фандинга для {coin}: {e}", exc_info=True)
            return None

    async def get_funding_info(self, coin: str) -> Optional[Dict]:
        """
        Получить информацию о фандинге (ставка и время до следующей выплаты)
        
        Args:
            coin: Название монеты без /USDT (например, "CVC")
            
        Returns:
            Словарь с данными:
            {
                "funding_rate": float,  # Ставка фандинга (например, 0.0001 = 0.01%)
                "next_funding_time": int,  # Timestamp следующей выплаты (может быть None, если API не предоставляет)
            }
            или None если ошибка
        """
        try:
            symbol = self._normalize_symbol(coin)
            url = "/future/market/v1/public/q/funding-rate"
            params = {"symbol": symbol}
            
            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"XT: не удалось получить ответ для фандинга {coin} (символ: {symbol})")
                return None
            
            return_code = data.get("returnCode")
            if return_code != 0:
                msg = data.get("msgInfo", "Unknown error")
                logger.warning(f"XT: API вернул ошибку для фандинга {coin} (символ: {symbol}): returnCode={return_code}, msg={msg}")
                return None
            
            result = data.get("result")
            if not result or not isinstance(result, dict):
                logger.warning(f"XT: фандинг для {coin} не найден (символ: {symbol}, result пустой или не словарь)")
                return None
            
            funding_rate_raw = result.get("fundingRate")
            if funding_rate_raw is None:
                return None
            
            funding_rate = float(funding_rate_raw)
            
            # XT API может не предоставлять время следующей выплаты в этом эндпоинте
            # Проверяем возможные поля для времени
            next_funding_time = None
            
            # Логируем все поля для отладки (только на уровне DEBUG)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"XT funding data for {coin}: {result}")
            
            # XT API возвращает время следующей выплаты в поле nextCollectionTime (мс)
            for field in ["nextCollectionTime", "nextFundingTime", "nextFundingTimeMs", "fundingTime", "nextFunding", "nextSettleTime", "settleTime", "nextSettleTimestamp", "settleTimestamp", "nextFundingTimestamp", "fundingTimestamp"]:
                time_val = result.get(field)
                if time_val is not None and time_val != "" and time_val != 0:
                    try:
                        next_funding_time = int(time_val)
                        logger.debug(f"XT found next_funding_time in field '{field}': {next_funding_time} for {coin}")
                        break
                    except (TypeError, ValueError):
                        continue
            
            # Пробуем получить из ticker эндпоинта
            if next_funding_time is None:
                try:
                    ticker_url = "/future/market/v1/public/q/ticker"
                    ticker_params = {"symbol": symbol}
                    ticker_data = await self._request_json("GET", ticker_url, params=ticker_params)
                    
                    if ticker_data and ticker_data.get("returnCode") == 0:
                        ticker_result = ticker_data.get("result")
                        if isinstance(ticker_result, dict):
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug(f"XT ticker data for {coin}: {ticker_result}")
                            for field in ["nextCollectionTime", "nextFundingTime", "nextFundingTimeMs", "fundingTime", "nextFunding", "nextSettleTime", "settleTime", "nextSettleTimestamp", "settleTimestamp", "nextSettleTimeMs", "nextFundingTimestamp", "fundingTimestamp"]:
                                time_val = ticker_result.get(field)
                                if time_val is not None and time_val != "" and time_val != 0:
                                    try:
                                        next_funding_time = int(time_val)
                                        logger.debug(f"XT found next_funding_time in ticker field '{field}': {next_funding_time} for {coin}")
                                        break
                                    except (TypeError, ValueError):
                                        continue
                except Exception as e:
                    logger.debug(f"XT: error getting ticker for {coin}: {e}")
                    pass
            
            # Если API не предоставило время, возвращаем None (не вычисляем расписание)
            if next_funding_time is None:
                logger.debug(f"XT: API не предоставило next_funding_time для {coin}, возвращаем None")
            
            return {
                "funding_rate": funding_rate,
                "next_funding_time": next_funding_time,
            }
                
        except Exception as e:
            logger.error(f"XT: ошибка при получении funding info для {coin}: {e}", exc_info=True)
            return None

    async def get_orderbook(self, coin: str, limit: int = 50) -> Optional[Dict]:
        """
        XT Futures orderbook (cg API)
        
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
            symbol = self._normalize_symbol(coin)  # gps_usdt
            url = "/future/market/v1/public/cg/orderbook"
            level = max(1, min(int(limit), 200))
            params = {
                "symbol": symbol,
                "level": level,
            }
            
            data = await self._request_json("GET", url, params=params)
            if not data or not isinstance(data, dict):
                logger.warning(f"XT: empty orderbook response for {coin}")
                return None
            
            bids = data.get("bids") or []
            asks = data.get("asks") or []
            
            if not bids or not asks:
                logger.warning(f"XT: empty bids/asks for {coin}")
                return None
            
            return {
                "bids": bids,  # [["price","size"], ...]
                "asks": asks,
            }
                
        except Exception as e:
            logger.error(f"XT: orderbook error for {coin}: {e}", exc_info=True)
            return None

    async def get_all_futures_coins(self) -> Set[str]:
        """
        Возвращает множество монет, доступных во фьючерсах на XT.com.
        
        Returns:
            Множество монет без суффиксов (например, {"BTC", "ETH", "SOL", ...})
        """
        return await fetch_xt_coins(self)

