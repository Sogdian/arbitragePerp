"""
Асинхронный базовый класс для бирж.
Использует httpx.AsyncClient, должен наследоваться всеми async-* биржами.
"""
from abc import ABC, abstractmethod
from typing import Dict, Optional, List, Tuple
import httpx
import asyncio
import logging

logger = logging.getLogger(__name__)


class AsyncBaseExchange(ABC):
    """Базовый класс для всех асинхронных бирж"""

    BASE_URL: str = ""

    def __init__(self, name: str, pool_limit: int = 100):
        self.name = name
        # Один AsyncClient на биржу – повторно используем TCP-соединения
        self.client = httpx.AsyncClient(
            base_url=self.BASE_URL,
            limits=httpx.Limits(max_connections=pool_limit, max_keepalive_connections=pool_limit),
            timeout=httpx.Timeout(5.0, connect=3.0)
        )

    async def close(self):
        """Закрывает HTTP клиент"""
        await self.client.aclose()

    async def _request_json(self, method: str, url: str, *, params: Optional[dict] = None) -> Optional[dict]:
        """Обертка с обработкой ошибок и логированием"""
        try:
            resp = await self.client.request(method, url, params=params)
            resp.raise_for_status()
            result = resp.json()
            return result
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            # Для LBank и 404 ошибок - это означает, что публичный API недоступен
            if self.name == "LBank" and status == 404:
                # Не логируем каждую 404 ошибку отдельно, чтобы не засорять лог
                # Основное сообщение будет в методах get_futures_ticker/get_funding_rate
                pass
            else:
                try:
                    error_body = e.response.text[:200]
                    logger.debug(f"{self.name}: HTTP {status} для {url} с params {params}: {error_body}")
                except:
                    logger.debug(f"{self.name}: HTTP {status} для {url} с params {params}")
        except (httpx.RequestError, asyncio.TimeoutError) as e:
            logger.warning(f"{self.name}: Ошибка соединения для {url} с params {params}: {e}")
        except Exception as e:
            logger.warning(f"{self.name}: Неожиданная ошибка для {url} с params {params}: {e}")
        return None

    @staticmethod
    def _vwap_for_notional(
        levels: List[List[str]],  # [[price, size], ...] as strings
        target_usdt: float,
    ) -> Tuple[Optional[float], float]:
        """
        Вычисляет VWAP для заданного номинала (notional) в USDT
        
        Args:
            levels: Список уровней [[price, size], ...] как строки
            target_usdt: Целевой номинал в USDT
            
        Returns:
            Кортеж (vwap_price, filled_usdt). Если глубины не хватило -> (None, filled_usdt)
        """
        remaining = target_usdt
        filled_usdt = 0.0
        filled_base = 0.0
        
        for p_raw, sz_raw in levels:
            p = float(p_raw)
            sz = float(sz_raw)
            level_notional = p * sz
            take = level_notional if level_notional <= remaining else remaining
            take_sz = take / p
            filled_usdt += take
            filled_base += take_sz
            remaining -= take
            
            if remaining <= 1e-9:
                break
        
        if filled_base <= 0:
            return None, 0.0
        
        if remaining > 1e-6:
            # не хватило глубины
            return None, filled_usdt
        
        vwap = filled_usdt / filled_base
        return vwap, target_usdt

    async def check_liquidity(
        self,
        coin: str,
        notional_usdt: float,
        ob_limit: int = 50,
        max_spread_bps: float = 30.0,
        max_impact_bps: float = 50.0,
        mode: str = "roundtrip",
    ) -> Optional[Dict]:
        """
        Проверка ликвидности под сделку notional_usdt
        
        Args:
            coin: Название монеты без /USDT (например, "GPS")
            notional_usdt: Размер сделки в USDT (например, 50, 100, 150)
            ob_limit: Количество уровней в orderbook (по умолчанию 50)
            max_spread_bps: Максимальный спред в базисных пунктах (по умолчанию 30)
            max_impact_bps: Максимальный проскальзывание в базисных пунктах (по умолчанию 50)
            mode: Режим проверки (по умолчанию "roundtrip"):
                - "entry_long" - проверка для входа в Long (важен только buy_vwap)
                - "entry_short" - проверка для входа в Short (важен только sell_vwap)
                - "roundtrip" - проверка для полного цикла (нужны оба buy_vwap и sell_vwap)
            
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
            
            # Валидация mode
            valid_modes = ("entry_long", "entry_short", "roundtrip")
            if mode not in valid_modes:
                logger.warning(f"{self.name}: неверный mode '{mode}', используется 'roundtrip'")
                mode = "roundtrip"
            
            # BUY uses asks (чтобы оценить вход в лонг по рынку)
            buy_vwap, buy_filled = self._vwap_for_notional(asks, notional_usdt)
            # SELL uses bids (чтобы оценить вход в шорт по рынку)
            sell_vwap, sell_filled = self._vwap_for_notional(bids, notional_usdt)
            
            # Проверяем достаточность глубины в зависимости от режима
            if mode == "entry_long":
                enough_depth = buy_vwap is not None
            elif mode == "entry_short":
                enough_depth = sell_vwap is not None
            else:  # "roundtrip"
                enough_depth = (buy_vwap is not None) and (sell_vwap is not None)
            
            buy_impact_bps = None
            sell_impact_bps = None
            if buy_vwap is not None:
                # Impact считается относительно ask1 (top-of-book для покупки)
                buy_impact_bps = abs(buy_vwap - ask1) / mid * 10_000
            if sell_vwap is not None:
                # Impact считается относительно bid1 (top-of-book для продажи)
                sell_impact_bps = abs(bid1 - sell_vwap) / mid * 10_000
            
            ok = True
            reasons = []
            
            if spread_bps > max_spread_bps:
                ok = False
                reasons.append(f"spread {spread_bps:.1f} bps > {max_spread_bps:.1f}")
            
            if not enough_depth:
                ok = False
                # Формируем сообщение в зависимости от режима
                if mode == "entry_long":
                    reasons.append(f"not enough depth for {notional_usdt} USDT (buy_filled={buy_filled:.2f})")
                elif mode == "entry_short":
                    reasons.append(f"not enough depth for {notional_usdt} USDT (sell_filled={sell_filled:.2f})")
                else:  # "roundtrip"
                    reasons.append(f"not enough depth for {notional_usdt} USDT (buy_filled={buy_filled:.2f}, sell_filled={sell_filled:.2f})")
            else:
                # Проверяем impact только для нужной стороны в зависимости от режима
                if mode in ("entry_long", "roundtrip"):
                    if buy_impact_bps is not None and buy_impact_bps > max_impact_bps:
                        ok = False
                        reasons.append(f"buy impact {buy_impact_bps:.1f} bps > {max_impact_bps:.1f}")
                if mode in ("entry_short", "roundtrip"):
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
            logger.error(f"{self.name}: ошибка при проверке ликвидности для {coin}: {e}", exc_info=True)
            return None

    @abstractmethod
    def _normalize_symbol(self, coin: str) -> str:
        """Преобразует монету в формат биржи для фьючерсов"""
        pass

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
            
        Примечание: Должен быть реализован в каждом классе биржи
        """
        logger.warning(f"{self.name}: get_orderbook не реализован")
        return None

