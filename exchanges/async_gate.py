"""
Асинхронная реализация Gate.io для фьючерсов на базе AsyncBaseExchange
"""
from typing import Dict, Optional, Set, Tuple, Any, List
import logging
from .async_base_exchange import AsyncBaseExchange
from .coin_list_fetchers import fetch_gate_coins

logger = logging.getLogger(__name__)

class AsyncGateExchange(AsyncBaseExchange):
    BASE_URL = "https://api.gateio.ws"

    def __init__(self):
        super().__init__("Gate")

    def _normalize_symbol(self, coin: str) -> str:
        """Преобразует монету в формат Gate.io для фьючерсов (например, CVC -> CVC_USDT)"""
        return f"{coin.upper()}_USDT"

    def _safe_px(self, raw: object, fallback: float, sanity_mult: float = 20.0) -> float:
        """
        Безопасное преобразование цены с проверками на разумность
        """
        try:
            v = float(raw)
        except (TypeError, ValueError):
            return fallback

        if v <= 0:
            return fallback

        # Для нормальных цен применяем sanity-check
        if fallback >= 0.0001:
            if v > fallback * sanity_mult or v < fallback / sanity_mult:
                return fallback

        return v

    def _parse_ob_levels(
        self,
        levels: Any,
        side: str,
        coin: str,
    ) -> Optional[List[List[float]]]:
        """
        Gate может вернуть уровни в разных форматах:
        1) REST/WS-legacy: [{"p":"97.1","s":2245}, ...]
        2) REST (часто): [["97.1","2245"], ...] или [[price, size], ...]
        3) Иногда может быть tuple/list длины >= 2
        Возвращаем нормализованный формат: [[price(float), size(float)], ...]
        """
        if not isinstance(levels, list) or not levels:
            return None

        out: List[List[float]] = []

        for i, lvl in enumerate(levels):
            price_raw = None
            size_raw = None

            if isinstance(lvl, dict):
                # WS legacy формат: {"p": "...", "s": ...}
                price_raw = lvl.get("p") or lvl.get("price")
                size_raw = lvl.get("s") or lvl.get("size")
            elif isinstance(lvl, (list, tuple)):
                if len(lvl) < 2:
                    logger.warning(f"Gate: level too short in {side} for {coin}: {lvl}")
                    return None
                price_raw, size_raw = lvl[0], lvl[1]
            else:
                logger.warning(f"Gate: unexpected level type in {side} for {coin}: {type(lvl)}")
                return None

            try:
                p = float(price_raw)
                s = float(size_raw)
            except (TypeError, ValueError):
                logger.warning(f"Gate: non-numeric level in {side} for {coin}: {lvl}")
                return None

            if p <= 0:
                continue

            # size в ордербуке должен быть положительным; на всякий случай нормализуем
            if s < 0:
                s = abs(s)

            out.append([p, s])

            # лёгкая защита от безумных ответов
            if i > 5000:
                break

        return out if out else None

    async def get_futures_ticker(self, coin: str) -> Optional[Dict]:
        """
        Получить тикер фьючерса для монеты
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
            if isinstance(data, list) and data:
                item = next((x for x in data if isinstance(x, dict) and x.get("contract") == symbol), data[0])
            elif isinstance(data, dict):
                item = data
            else:
                item = None

            if not item or not isinstance(item, dict):
                logger.warning(f"Gate: тикер для {coin} не найден / неверный формат")
                return None

            last_raw = item.get("last")
            if last_raw is None:
                logger.warning(f"Gate: нет last для {coin}: {item}")
                return None

            last = float(last_raw)
            if last <= 0:
                return None

            bid_raw = item.get("bid") or item.get("highest_bid")
            ask_raw = item.get("ask") or item.get("lowest_ask")

            bid = self._safe_px(bid_raw, last)
            ask = self._safe_px(ask_raw, last)

            if bid > ask:
                bid = last
                ask = last

            return {"price": last, "bid": bid, "ask": ask}

        except Exception as e:
            logger.error(f"Gate: ошибка при получении тикера для {coin}: {e}", exc_info=True)
            return None

    async def get_funding_rate(self, coin: str) -> Optional[float]:
        """
        Получить текущую ставку фандинга для монеты
        
        Gate:
        - /futures/usdt/contracts/{contract} -> текущая/индикативная ставка (UI "Текущая")
        - /futures/usdt/funding_rate -> история (UI "Предыдущая")
        """
        try:
            symbol = self._normalize_symbol(coin)

            # 1) CURRENT (contract info)
            url = f"/api/v4/futures/usdt/contracts/{symbol}"
            data = await self._request_json("GET", url)

            if isinstance(data, dict):
                # приоритет: funding_rate (обычно то, что UI показывает как текущую)
                r = data.get("funding_rate")
                if r is None:
                    # иногда полезно взять indicative
                    r = data.get("funding_rate_indicative")
                if r is not None:
                    return float(r)

            # 2) FALLBACK: HISTORY (previous applied)
            url = "/api/v4/futures/usdt/funding_rate"
            params = {"contract": symbol, "limit": 1}
            data = await self._request_json("GET", url, params=params)

            if isinstance(data, list) and data:
                item = data[0]
                r = item.get("r") or item.get("funding_rate") or item.get("rate")
                if r is not None:
                    return float(r)

            logger.warning(f"Gate: не удалось получить funding_rate для {coin} (symbol={symbol})")
            return None

        except Exception as e:
            logger.error(f"Gate: ошибка при получении фандинга для {coin}: {e}", exc_info=True)
            return None

    async def get_orderbook(self, coin: str, limit: int = 50) -> Optional[Dict]:
        """
        Получить orderbook (книгу заявок) для монеты
        """
        try:
            symbol = self._normalize_symbol(coin)
            url = "/api/v4/futures/usdt/order_book"

            # Gate принимает limit, но иногда удобнее дать небольшой sanity
            limit_i = max(1, min(int(limit), 200))

            # with_id=true полезен, если будешь синхронизировать по id, но для разовой ликвидности не обязателен
            params = {"contract": symbol, "limit": limit_i}

            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"Gate: не удалось получить orderbook для {coin}")
                return None

            if not isinstance(data, dict):
                logger.warning(f"Gate: неожиданный формат orderbook для {coin}: {type(data)}")
                return None

            bids_raw = data.get("bids")
            asks_raw = data.get("asks")

            bids = self._parse_ob_levels(bids_raw, "bids", coin)
            asks = self._parse_ob_levels(asks_raw, "asks", coin)

            if not bids or not asks:
                logger.warning(f"Gate: пустой/невалидный orderbook для {coin} (bids={type(bids_raw)}, asks={type(asks_raw)})")
                return None

            return {"bids": bids, "asks": asks}

        except Exception as e:
            logger.error(f"Gate: ошибка при получении orderbook для {coin}: {e}", exc_info=True)
            return None

    async def get_all_futures_coins(self) -> Set[str]:
        """
        Возвращает множество монет, доступных во фьючерсах на Gate.io.
        
        Returns:
            Множество монет без суффиксов (например, {"BTC", "ETH", "SOL", ...})
        """
        return await fetch_gate_coins(self)
