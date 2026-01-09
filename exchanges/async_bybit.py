"""
Асинхронная реализация Bybit для фьючерсов на базе AsyncBaseExchange

Фиксированная логика:
- Рынок: Derivatives
- Тип: Perpetual
- Категория: linear (только USDT пары)
- Символ: COINUSDT (без подчеркивания)
- USDC / inverse / альтернативные категории - НЕ поддерживаются
- Если COINUSDT не найден → считаем, что инструмента нет
"""
from typing import Dict, Optional, List, Tuple
import logging
from .async_base_exchange import AsyncBaseExchange

logger = logging.getLogger(__name__)


class AsyncBybitExchange(AsyncBaseExchange):
    BASE_URL = "https://api.bybit.com"

    def __init__(self, pool_limit: int = 100):
        super().__init__("Bybit", pool_limit)

    def _normalize_symbol(self, coin: str) -> str:
        """Преобразует монету в формат Bybit для фьючерсов (например, CVC -> CVCUSDT)"""
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
            url = "/v5/market/tickers"
            params = {
                "category": "linear",
                "symbol": symbol,
            }
            
            data = await self._request_json("GET", url, params=params)
            if not data or data.get("retCode") != 0:
                logger.warning(f"Bybit: тикер для {coin} не найден")
                return None
            
            items = (data.get("result") or {}).get("list") or []
            if not items:
                logger.warning(f"Bybit: тикер для {coin} не найден")
                return None
            
            item = items[0]
            last_price = item.get("lastPrice")
            if last_price is None:
                return None
            
            return {
                "price": float(last_price),
                "bid": float(item.get("bid1Price", last_price)),
                "ask": float(item.get("ask1Price", last_price)),
            }
                
        except Exception as e:
            logger.error(f"Bybit: ошибка при получении тикера для {coin}: {e}", exc_info=True)
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
            url = "/v5/market/funding/history"
            params = {
                "category": "linear",
                "symbol": symbol,
                "limit": 1,
            }
            
            data = await self._request_json("GET", url, params=params)
            if not data or data.get("retCode") != 0:
                logger.warning(f"Bybit: фандинг для {coin} не найден")
                return None
            
            items = (data.get("result") or {}).get("list") or []
            if not items:
                logger.warning(f"Bybit: фандинг для {coin} не найден")
                return None
            
            funding_rate_raw = items[0].get("fundingRate")
            if funding_rate_raw is None:
                return None
            
            return float(funding_rate_raw)
                
        except Exception as e:
            logger.error(f"Bybit: ошибка при получении фандинга для {coin}: {e}", exc_info=True)
            return None
    
    async def get_orderbook(self, coin: str, limit: int = 50) -> Optional[Dict]:
        """
        Получить книгу заявок (orderbook) для монеты
        
        Args:
            coin: Название монеты без /USDT (например, "GPS")
            limit: Количество уровней (по умолчанию 50)
            
        Returns:
            Словарь с данными книги заявок:
            {
                "bids": [[price, size], ...],  # Заявки на покупку
                "asks": [[price, size], ...]    # Заявки на продажу
            }
            или None если ошибка
        """
        try:
            symbol = self._normalize_symbol(coin)
            url = "/v5/market/orderbook"
            params = {
                "category": "linear",
                "symbol": symbol,
                "limit": limit,
            }
            
            data = await self._request_json("GET", url, params=params)
            if not data or data.get("retCode") != 0:
                logger.warning(f"Bybit: orderbook error for {coin}: {data}")
                return None
            
            result = data.get("result") or {}
            bids = result.get("b") or []  # [[price, size], ...]
            asks = result.get("a") or []
            
            if not bids or not asks:
                return None
            
            return {"bids": bids, "asks": asks}
                
        except Exception as e:
            logger.error(f"Bybit: ошибка при получении orderbook для {coin}: {e}", exc_info=True)
            return None
    
    @staticmethod
    def _vwap_for_notional(
        levels: List[List[str]],  # [[price, size], ...] as strings
        target_usdt: float,
        side: str,  # "buy" uses asks, "sell" uses bids
    ) -> Tuple[Optional[float], float]:
        """
        Вычисляет VWAP для заданного номинала (notional) в USDT
        
        Args:
            levels: Список уровней [[price, size], ...] как строки
            target_usdt: Целевой номинал в USDT
            side: "buy" или "sell" (для логики)
            
        Returns:
            Кортеж (vwap_price, filled_usdt). Если глубины не хватило -> (None, filled_usdt)
        """
        remaining = target_usdt
        return_cost = 0.0
        return_base = 0.0
        
        for p_raw, sz_raw in levels:
            p = float(p_raw)
            sz = float(sz_raw)
            level_notional = p * sz
            take = level_notional if level_notional <= remaining else remaining
            take_sz = take / p
            return_cost += take
            return_base += take_sz
            remaining -= take
            
            if remaining <= 1e-9:
                break
        
        if return_base <= 0:
            return None, 0.0
        
        if remaining > 1e-6:
            # не хватило глубины
            return None, return_cost
        
        vwap = return_cost / return_base
        return vwap, target_usdt
    
    async def check_liquidity(
        self,
        coin: str,
        notional_usdt: float,
        ob_limit: int = 50,
        max_spread_bps: float = 30.0,
        max_impact_bps: float = 50.0,
    ) -> Optional[Dict]:
        """
        Проверка ликвидности под сделку notional_usdt
        
        Args:
            coin: Название монеты без /USDT (например, "GPS")
            notional_usdt: Размер сделки в USDT (например, 50, 100, 150)
            ob_limit: Количество уровней в orderbook (по умолчанию 50)
            max_spread_bps: Максимальный спред в базисных пунктах (по умолчанию 30)
            max_impact_bps: Максимальный проскальзывание в базисных пунктах (по умолчанию 50)
            
        Returns:
            Словарь с метриками ликвидности:
            {
                "coin": str,
                "symbol": str,
                "mid": float,
                "bid1": float,
                "ask1": float,
                "spread_bps": float,
                "notional_usdt": float,
                "buy_vwap": float | None,
                "sell_vwap": float | None,
                "buy_impact_bps": float | None,
                "sell_impact_bps": float | None,
                "ok": bool,
                "reasons": List[str]
            }
            или None если ошибка
        """
        try:
            ob = await self.get_orderbook(coin, limit=ob_limit)
            if not ob:
                return None
            
            bids = ob["bids"]
            asks = ob["asks"]
            
            bid1 = float(bids[0][0])
            ask1 = float(asks[0][0])
            mid = (bid1 + ask1) / 2.0
            
            spread_bps = (ask1 - bid1) / mid * 10_000
            
            # BUY uses asks (чтобы оценить вход в лонг по рынку)
            buy_vwap, buy_filled = self._vwap_for_notional(asks, notional_usdt, "buy")
            # SELL uses bids (чтобы оценить вход в шорт по рынку)
            sell_vwap, sell_filled = self._vwap_for_notional(bids, notional_usdt, "sell")
            
            # если глубины не хватило
            enough_depth = (buy_vwap is not None) and (sell_vwap is not None)
            
            buy_impact_bps = None
            sell_impact_bps = None
            if buy_vwap is not None:
                buy_impact_bps = abs(buy_vwap - mid) / mid * 10_000
            if sell_vwap is not None:
                sell_impact_bps = abs(mid - sell_vwap) / mid * 10_000
            
            ok = True
            reasons = []
            
            if spread_bps > max_spread_bps:
                ok = False
                reasons.append(f"spread {spread_bps:.1f} bps > {max_spread_bps:.1f}")
            
            if not enough_depth:
                ok = False
                reasons.append(f"not enough depth for {notional_usdt} USDT (buy_filled={buy_filled:.2f}, sell_filled={sell_filled:.2f})")
            else:
                if buy_impact_bps is not None and buy_impact_bps > max_impact_bps:
                    ok = False
                    reasons.append(f"buy impact {buy_impact_bps:.1f} bps > {max_impact_bps:.1f}")
                if sell_impact_bps is not None and sell_impact_bps > max_impact_bps:
                    ok = False
                    reasons.append(f"sell impact {sell_impact_bps:.1f} bps > {max_impact_bps:.1f}")
            
            return {
                "coin": coin.upper(),
                "symbol": self._normalize_symbol(coin),
                "mid": mid,
                "bid1": bid1,
                "ask1": ask1,
                "spread_bps": spread_bps,
                "notional_usdt": notional_usdt,
                "buy_vwap": buy_vwap,
                "sell_vwap": sell_vwap,
                "buy_impact_bps": buy_impact_bps,
                "sell_impact_bps": sell_impact_bps,
                "ok": ok,
                "reasons": reasons,
            }
                
        except Exception as e:
            logger.error(f"Bybit: ошибка при проверке ликвидности для {coin}: {e}", exc_info=True)
            return None

