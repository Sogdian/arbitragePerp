"""
Асинхронная реализация MEXC для фьючерсов на базе AsyncBaseExchange
"""
from typing import Dict, Optional, Any, List
import logging
import asyncio
import httpx
from .async_base_exchange import AsyncBaseExchange

logger = logging.getLogger(__name__)


class AsyncMexcExchange(AsyncBaseExchange):
    # Основной домен
    BASE_URL = "https://contract.mexc.com"
    # Часто более стабильный домен для futures у MEXC
    BASE_URL_FALLBACK = "https://futures.mexc.com"

    def __init__(self, pool_limit: int = 100):
        super().__init__("MEXC", pool_limit)

    def _normalize_symbol(self, coin: str) -> str:
        """MEXC perpetual обычно использует формат COIN_USDT"""
        return f"{coin.upper()}_USDT"

    def _canon(self, sym: str) -> str:
        """GPS_USDT, GPS-USDT, GPSUSDT -> GPSUSDT"""
        return (sym or "").replace("_", "").replace("-", "").upper()

    def _safe_px(self, raw: object, fallback: float) -> float:
        try:
            v = float(raw)
        except (TypeError, ValueError):
            return fallback

        if v <= 0:
            return fallback

        # sanity-check 10x
        if v > fallback * 10 or v < fallback / 10:
            return fallback

        return v

    def _looks_empty_top(self, data: object) -> bool:
        """Пустой/странный dict без ключей верхнего уровня data/result/code."""
        return isinstance(data, dict) and not any(k in data for k in ("data", "result", "code"))

    async def _request_json_with_domain_fallback(
        self,
        method: str,
        url: str,
        params: Optional[dict] = None,
        *,
        try_domains: Optional[List[str]] = None,
        retries_per_domain: int = 1,
        backoff_s: float = 0.2,
    ) -> Any:
        """
        Обёртка, которая пробует два домена и короткие ретраи.
        ВАЖНО: тут мы сами делаем httpx запрос, чтобы реально сменить base_url.
        """
        domains = try_domains or [self.BASE_URL, self.BASE_URL_FALLBACK]

        headers = {
            # иногда WAF'ы режут пустой UA на некоторых эндпоинтах
            "User-Agent": "Mozilla/5.0 (compatible; arbitragePerp/1.0)",
            "Accept": "application/json",
        }

        for d_i, domain in enumerate(domains):
            for r in range(max(1, int(retries_per_domain))):
                client = httpx.AsyncClient(
                    base_url=domain,
                    headers=headers,
                    limits=httpx.Limits(max_connections=10, max_keepalive_connections=10),
                    timeout=httpx.Timeout(8.0, connect=3.0),
                )
                try:
                    resp = await client.request(method, url, params=params)
                    resp.raise_for_status()
                    return resp.json()
                except (httpx.RequestError, httpx.HTTPStatusError, asyncio.TimeoutError) as e:
                    # на последней попытке просто отдадим None
                    is_last = (d_i == len(domains) - 1) and (r == max(1, int(retries_per_domain)) - 1)
                    if is_last:
                        logger.debug(f"MEXC: domain fallback failed for {domain}{url}: {e}")
                    else:
                        await asyncio.sleep(backoff_s * (1 + r + d_i))
                except Exception as e:
                    is_last = (d_i == len(domains) - 1) and (r == max(1, int(retries_per_domain)) - 1)
                    logger.debug(f"MEXC: unexpected error for {domain}{url}: {e}")
                    if not is_last:
                        await asyncio.sleep(backoff_s * (1 + r + d_i))
                finally:
                    await client.aclose()

        return None

    def _parse_ob_levels(self, levels: Any, side: str, coin: str) -> Optional[List[List[float]]]:
        """
        Нормализация форматов orderbook:
        - [[price, size], ...]
        - [["price","size"], ...]
        - [{"price":"...","quantity":"..."}, ...] (реже)
        """
        if not isinstance(levels, list) or not levels:
            return None

        out: List[List[float]] = []

        for lvl in levels:
            if isinstance(lvl, dict):
                price_raw = lvl.get("price") or lvl.get("p")
                size_raw = lvl.get("quantity") or lvl.get("q") or lvl.get("size") or lvl.get("s")
            elif isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                price_raw, size_raw = lvl[0], lvl[1]
            else:
                logger.warning(f"MEXC: unexpected level type in {side} for {coin}: {type(lvl)}")
                return None

            try:
                p = float(price_raw)
                s = float(size_raw)
            except (TypeError, ValueError):
                logger.warning(f"MEXC: non-numeric level in {side} for {coin}: {lvl}")
                return None

            if p <= 0:
                continue
            if s < 0:
                s = abs(s)

            out.append([p, s])

        return out if out else None

    async def get_futures_ticker(self, coin: str) -> Optional[Dict]:
        """
        Тикер: GET /api/v1/contract/ticker?symbol=...
        """
        try:
            c = coin.upper()
            symbol = f"{c}_USDT"
            url = "/api/v1/contract/ticker"
            params = {"symbol": symbol}

            data = await self._request_json("GET", url, params=params)

            bad_code = isinstance(data, dict) and ("code" in data) and data.get("code") != 0
            looks_empty = self._looks_empty_top(data)

            if not data or bad_code or looks_empty:
                symbol_fallback = f"{c}USDT"
                logger.debug(f"MEXC: ticker fallback symbol {symbol_fallback} for {coin}")
                data = await self._request_json("GET", url, params={"symbol": symbol_fallback})
                if data and isinstance(data, dict) and data.get("code") == 0:
                    symbol = symbol_fallback

            if not data:
                logger.warning(f"MEXC: не удалось получить тикер для {coin}")
                return None

            if isinstance(data, dict) and "code" in data and data.get("code") != 0:
                logger.warning(f"MEXC: ticker API error {coin}: code={data.get('code')} msg={data.get('msg')}")
                return None

            item = None
            if isinstance(data, dict):
                item = data.get("data") if data.get("data") is not None else data.get("result")
                if isinstance(item, list) and item:
                    item = item[0]
            elif isinstance(data, list) and data:
                item = data[0]

            if not isinstance(item, dict):
                logger.warning(f"MEXC: тикер для {coin} не найден/не dict")
                return None

            last_price_raw = item.get("lastPrice") or item.get("last")
            if not last_price_raw:
                logger.warning(f"MEXC: нет lastPrice/last для {coin}")
                return None

            price = float(last_price_raw)
            if price <= 0:
                return None

            bid = self._safe_px(item.get("bid1") or item.get("bid"), price)
            ask = self._safe_px(item.get("ask1") or item.get("ask"), price)

            if bid > ask:
                bid = price
                ask = price

            return {"price": price, "bid": bid, "ask": ask}

        except Exception as e:
            logger.error(f"MEXC: ошибка при получении тикера для {coin}: {e}", exc_info=True)
            return None

    async def get_funding_rate(self, coin: str) -> Optional[float]:
        """
        ВАЖНО: по Contract V1 funding_rate идёт как:
        GET /api/v1/contract/funding_rate/{symbol}
        """
        try:
            c = coin.upper()

            # 1) пробуем COIN_USDT
            symbol1 = f"{c}_USDT"
            url1 = f"/api/v1/contract/funding_rate/{symbol1}"
            data = await self._request_json_with_domain_fallback("GET", url1, params=None)

            # 2) fallback COINUSDT
            if not data or (isinstance(data, dict) and ("code" in data) and data.get("code") != 0) or self._looks_empty_top(data):
                symbol2 = f"{c}USDT"
                url2 = f"/api/v1/contract/funding_rate/{symbol2}"
                data = await self._request_json_with_domain_fallback("GET", url2, params=None)

            if not data:
                logger.warning(f"MEXC: не удалось получить фандинг для {coin}")
                return None

            if isinstance(data, dict) and "code" in data and data.get("code") != 0:
                logger.warning(f"MEXC: funding API error {coin}: code={data.get('code')} msg={data.get('msg')}")
                return None

            # данные могут лежать в data/result или быть прямым dict
            item = None
            if isinstance(data, dict):
                item = data.get("data") if data.get("data") is not None else data.get("result")
                if item is None:
                    item = data
                if isinstance(item, list) and item:
                    item = item[0]
            elif isinstance(data, list) and data:
                item = data[0]

            if not isinstance(item, dict):
                logger.warning(f"MEXC: funding для {coin} не найден/не dict")
                return None

            funding_rate_raw = item.get("fundingRate") or item.get("rate") or item.get("r")

            if funding_rate_raw is None:
                logger.warning(f"MEXC: нет поля fundingRate/rate/r для {coin}: {item}")
                return None

            return float(funding_rate_raw)

        except Exception as e:
            logger.error(f"MEXC: ошибка при получении фандинга для {coin}: {e}", exc_info=True)
            return None

    async def get_orderbook(self, coin: str, limit: int = 50) -> Optional[Dict]:
        """
        ВАЖНО: по Contract V1 depth идёт как:
        GET /api/v1/contract/depth/{symbol}?limit=...
        """
        try:
            c = coin.upper()
            limit_i = max(1, min(int(limit), 200))

            # 1) COIN_USDT
            symbol1 = f"{c}_USDT"
            url1 = f"/api/v1/contract/depth/{symbol1}"
            data = await self._request_json_with_domain_fallback(
                "GET",
                url1,
                params={"limit": limit_i},
                retries_per_domain=1,
                backoff_s=0.25,
            )

            # 2) fallback COINUSDT
            if not data or (isinstance(data, dict) and ("code" in data) and data.get("code") != 0) or self._looks_empty_top(data):
                symbol2 = f"{c}USDT"
                url2 = f"/api/v1/contract/depth/{symbol2}"
                data = await self._request_json_with_domain_fallback(
                    "GET",
                    url2,
                    params={"limit": limit_i},
                    retries_per_domain=1,
                    backoff_s=0.25,
                )

            if not data:
                logger.warning(f"MEXC: не удалось получить orderbook для {coin}")
                return None

            if isinstance(data, dict) and "code" in data and data.get("code") != 0:
                logger.warning(f"MEXC: orderbook API error {coin}: code={data.get('code')} msg={data.get('msg')}")
                return None

            # MEXC: data/result или напрямую
            result = None
            if isinstance(data, dict):
                result = data.get("data") if data.get("data") is not None else data.get("result")
                if result is None:
                    result = data
            else:
                result = data

            if not isinstance(result, dict):
                logger.warning(f"MEXC: неожиданный формат orderbook для {coin}: {type(result)}")
                return None

            bids_raw = result.get("bids", [])
            asks_raw = result.get("asks", [])

            bids = self._parse_ob_levels(bids_raw, "bids", coin)
            asks = self._parse_ob_levels(asks_raw, "asks", coin)

            if not bids or not asks:
                logger.warning(
                    f"MEXC: пустой/невалидный orderbook для {coin} "
                    f"(bids_raw={type(bids_raw)}, asks_raw={type(asks_raw)})"
                )
                return None

            return {"bids": bids, "asks": asks}

        except Exception as e:
            logger.error(f"MEXC: ошибка при получении orderbook для {coin}: {e}", exc_info=True)
            return None
