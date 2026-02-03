"""
Асинхронная реализация MEXC для фьючерсов на базе AsyncBaseExchange
"""
from typing import Dict, Optional, Any, Set, List
import logging
import asyncio
import json
import httpx
from .async_base_exchange import AsyncBaseExchange
from .coin_list_fetchers import fetch_mexc_coins

logger = logging.getLogger(__name__)

# Эти коды считаем "нет инструмента/тикера" => не WARNING
_MEXC_NOT_FOUND_CODES = {510, 1001}


def _to_int(x) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None


def _mexc_coin_from_contract(it: dict) -> tuple:
    """Как в scripts/mexc_alias_check.py: (coin для списка, API symbol)."""
    sym = it.get("symbol") or ""
    if not isinstance(sym, str) or not sym.endswith("_USDT"):
        return ("", "")
    sym_u = sym.upper()
    if sym_u == "FUN_USDT":
        return ("FUN", sym)
    if sym_u == "SPORTFUN_USDT":
        return ("SPORTFUN", sym)
    disp = it.get("displayName") or it.get("display_name") or it.get("displayNameEn") or it.get("display_name_en")
    coin_from_disp = None
    if isinstance(disp, str):
        up = disp.upper()
        idx = up.find("_USDT")
        if idx > 0:
            coin_from_disp = up[:idx].strip()
            coin_from_disp = "".join(ch for ch in coin_from_disp if ch.isalnum())
    coin = (coin_from_disp or sym.replace("_USDT", "")).upper()
    return (coin, sym)


class AsyncMexcExchange(AsyncBaseExchange):
    # Основной домен
    BASE_URL = "https://contract.mexc.com"
    # Часто более стабильный домен для futures у MEXC
    BASE_URL_FALLBACK = "https://futures.mexc.com"

    def __init__(self, pool_limit: int = 100):
        super().__init__("MEXC", pool_limit)
        self._mexc_dynamic_aliases: Optional[Dict[str, str]] = None

    async def _ensure_mexc_aliases_loaded(self) -> None:
        """Один раз загрузить алиасы из API contract/detail (логика как в scripts/mexc_alias_check.py)."""
        if self._mexc_dynamic_aliases is not None:
            return
        try:
            data = await self._request_json_with_domain_fallback(
                "GET", "/api/v1/contract/detail", params={},
                retries_per_domain=1, backoff_s=0.25,
            )
            if not isinstance(data, dict) or data.get("code") != 0:
                self._mexc_dynamic_aliases = {}
                return
            items = data.get("data") or []
            if not isinstance(items, list):
                self._mexc_dynamic_aliases = {}
                return
            aliases: Dict[str, str] = {}
            for it in items:
                if not isinstance(it, dict):
                    continue
                sym = it.get("symbol") or ""
                if not sym.endswith("_USDT"):
                    continue
                settle = (it.get("settleCoin") or "").upper()
                if settle != "USDT":
                    continue
                state = str(it.get("state", ""))
                if state in ("3", "4", "5"):
                    continue
                coin, symbol = _mexc_coin_from_contract(it)
                if coin and symbol:
                    aliases[coin] = symbol
            self._mexc_dynamic_aliases = aliases
            logger.debug(f"MEXC: загружено {len(aliases)} алиасов из contract/detail")
        except Exception as e:
            logger.debug(f"MEXC: не удалось загрузить алиасы: {e}")
            self._mexc_dynamic_aliases = {}

    def _normalize_symbol(self, coin: str) -> str:
        """
        MEXC perpetual обычно использует формат COIN_USDT, но иногда у MEXC есть алиасы,
        когда "отображаемый" тикер (как в UI) не совпадает с API symbol.

        Пример:
        - UI: FUNUSDT (монета FUN, цена ~0.07)
        - API symbol: SPORTFUN_USDT
        - При этом API symbol FUN_USDT — это другой контракт (FUNTOKEN_USDT) с другой ценой (~0.002)
        """
        c = coin.upper()
        # Алиасы: coin (из списка монет / displayName) -> API symbol. Только бессрочные USDT.
        # Источник: scripts/mexc_alias_check.py (contract/detail, settleCoin=USDT, coin != symbol_base)
        aliases = {
            "AAPL": "AAPLSTOCK_USDT",
            "ACN": "ACNSTOCK_USDT",
            "ADBE": "ADBESTOCK_USDT",
            "AMAT": "AMATSTOCK_USDT",
            "AMD": "AMDSTOCK_USDT",
            "AMZN": "AMZNSTOCK_USDT",
            "APE": "APE_USDT",
            "APECOIN": "APE_USDT",
            "AIOZ": "AIOZ_USDT",
            "IRYS": "IRYS_USDT",
            "ARM": "ARMSTOCK_USDT",
            "ASML": "ASMLSTOCK_USDT",
            "AVGO": "AVGOSTOCK_USDT",
            "BA": "BASTOCK_USDT",
            "BABA": "BABASTOCK_USDT",
            "BAC": "BACSTOCK_USDT",
            "BOB": "BUILDONBOB_USDT",
            "CAT": "CATSTOCK_USDT",
            "COIN": "COINBASE_USDT",
            "COPPERXCU": "COPPER_USDT",
            "COST": "COSTSTOCK_USDT",
            "CRCL": "CRCLSTOCK_USDT",
            "CRM": "CRMSTOCK_USDT",
            "CSCO": "CSCOSTOCK_USDT",
            "FIG": "FIGSTOCK_USDT",
            "FIL": "FILECOIN_USDT",
            "FUN": "SPORTFUN_USDT",
            "FUNTOKEN": "FUN_USDT",
            "FUTU": "FUTUSTOCK_USDT",
            "GE": "GESTOCK_USDT",
            "GOLDPAXG": "PAXG_USDT",
            "GOLDXAUT": "XAUT_USDT",
            "GOOGL": "GOOGLSTOCK_USDT",
            "GS": "GSSTOCK_USDT",
            "HOOD": "ROBINHOOD_USDT",
            "IBM": "IBMSTOCK_USDT",
            "INTC": "INTCSTOCK_USDT",
            "JD": "JDSTOCK_USDT",
            "JNJ": "JNJSTOCK_USDT",
            "JPM": "JPMSTOCK_USDT",
            "LLY": "LLYSTOCK_USDT",
            "LRCX": "LRCXSTOCK_USDT",
            "LUNA": "LUNANEW_USDT",
            "MA": "MASTOCK_USDT",
            "MCD": "MCDSTOCK_USDT",
            "META": "METASTOCK_USDT",
            "MON": "MONAD_USDT",
            "MRVL": "MRVLSTOCK_USDT",
            "MSFT": "MSFTSTOCK_USDT",
            "MSTR": "MSTRSTOCK_USDT",
            "MU": "MUSTOCK_USDT",
            "NFLX": "NFLXSTOCK_USDT",
            "NKE": "NKESTOCK_USDT",
            "NOW": "NOWSTOCK_USDT",
            "NVDA": "NVIDIA_USDT",
            "OPEN": "OPENLEDGER_USDT",
            "ORCL": "ORCLSTOCK_USDT",
            "PALLADIUMXPD": "XPD_USDT",
            "PEP": "PEPSTOCK_USDT",
            "PLATINUMXPT": "XPT_USDT",
            "PLTR": "PLTRSTOCK_USDT",
            "PUMP": "PUMPFUN_USDT",
            "QCOM": "QCOMSTOCK_USDT",
            "QQQ": "QQQSTOCK_USDT",
            "RDDT": "RDDTSTOCK_USDT",
            "SILVERXAG": "SILVER_USDT",
            "SLEEPLESSAI": "AI_USDT",
            "SOON": "SOONNETWORK_USDT",
            "SP500": "SPX500_USDT",
            "TON": "TONCOIN_USDT",
            "TRUMP": "TRUMPOFFICIAL_USDT",
            "TSLA": "TESLA_USDT",
            "UBER": "UBERSTOCK_USDT",
            "UNH": "UNHSTOCK_USDT",
            "V": "VSTOCK_USDT",
            "WMT": "WMTSTOCK_USDT",
            "XEMPIRE": "X_USDT",
            "XOM": "XOMSTOCK_USDT",
            "ZK": "ZKSYNC_USDT",
            # displayName в API (латиница в symbol)
            "\u8001\u5b50": "LAOZI_USDT",           # 老子
            "\u9ed1\u9a6c": "HEIMA_USDT",           # 黑马
            "\u6211\u8e0f\u9a6c\u6765\u4e86": "WOTAMALAILE_USDT",  # 我踏马来了
            "\u5e01\u5b89\u4eba\u751f": "BIANRENSHENG_USDT",       # 币安人生
        }
        # Сначала алиасы из API (contract/detail), затем статический словарь
        if self._mexc_dynamic_aliases is not None and c in self._mexc_dynamic_aliases:
            return self._mexc_dynamic_aliases[c]
        return aliases.get(c, f"{c}_USDT")

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
                try:
                    async with httpx.AsyncClient(
                        base_url=domain,
                        headers=headers,
                        limits=httpx.Limits(max_connections=10, max_keepalive_connections=10),
                        timeout=httpx.Timeout(15.0, connect=5.0),  # MEXC часто отвечает медленно
                    ) as client:
                        resp = await client.request(method, url, params=params)
                        resp.raise_for_status()
                        try:
                            return resp.json()
                        except json.JSONDecodeError:
                            is_last = (d_i == len(domains) - 1) and (r == max(1, int(retries_per_domain)) - 1)
                            msg = f"MEXC: non-JSON response for {domain}{url} status={resp.status_code}"
                            if is_last:
                                logger.warning(msg)
                                return None
                            logger.debug(msg)
                            await asyncio.sleep(backoff_s * (1 + r + d_i))
                            continue
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
            await self._ensure_mexc_aliases_loaded()
            url = "/api/v1/contract/ticker"

            # 1) основной формат COIN_USDT (с учетом алиасов)
            symbol = self._normalize_symbol(coin)
            data = await self._request_json_with_domain_fallback("GET", url, params={"symbol": symbol})

            # 2) если ответ странный/ошибочный — пробуем COINUSDT
            code_int = _to_int(data.get("code")) if isinstance(data, dict) else None
            looks_empty = self._looks_empty_top(data)

            if (not data) or looks_empty or (code_int is not None and code_int != 0):
                symbol_fallback = symbol.replace("_", "")
                logger.debug(f"MEXC: ticker fallback symbol {symbol_fallback} for {coin}")
                data2 = await self._request_json_with_domain_fallback("GET", url, params={"symbol": symbol_fallback})

                # принимаем fallback только если он успешен
                code2 = _to_int(data2.get("code")) if isinstance(data2, dict) else None
                if data2 and isinstance(data2, dict) and code2 == 0 and not self._looks_empty_top(data2):
                    data = data2
                    symbol = symbol_fallback
                    code_int = code2
                else:
                    # оставляем исходный data (он нужен для корректной диагностики ниже)
                    data = data2 if data2 is not None else data
                    code_int = _to_int(data.get("code")) if isinstance(data, dict) else code_int

            if not data:
                # это может быть сеть/домен/HTTP — но это уже залогировано в domain_fallback как debug,
                # здесь дадим аккуратный WARNING
                logger.warning(f"MEXC: не удалось получить тикер для {coin}")
                return None

            if isinstance(data, dict) and "code" in data:
                code_int = _to_int(data.get("code"))

                if code_int in _MEXC_NOT_FOUND_CODES:
                    # это "нет тикера/нет инструмента" — шум не нужен
                    logger.debug(f"MEXC: ticker not found for {coin} (symbol={symbol}) code={code_int} msg={data.get('msg')}")
                    return None

                if code_int is not None and code_int != 0:
                    # прочие коды — это уже реально интересно
                    logger.warning(f"MEXC: ticker API error {coin}: code={data.get('code')} msg={data.get('msg')}")
                    return None

            # данные тикера
            item = None
            if isinstance(data, dict):
                item = data.get("data") if data.get("data") is not None else data.get("result")
                if isinstance(item, list) and item:
                    item = item[0]
            elif isinstance(data, list) and data:
                item = data[0]

            if not isinstance(item, dict):
                logger.debug(f"MEXC: ticker for {coin} not dict (symbol={symbol})")
                return None

            last_price_raw = item.get("lastPrice") or item.get("last")
            if not last_price_raw:
                # тоже не надо шуметь WARN — это частая ситуация для невалидных/пустых тикеров
                logger.debug(f"MEXC: no lastPrice/last for {coin} (symbol={symbol}) item_keys={list(item.keys())[:10]}")
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
            await self._ensure_mexc_aliases_loaded()
            # 1) пробуем COIN_USDT (с учетом алиасов)
            symbol1 = self._normalize_symbol(coin)
            url1 = f"/api/v1/contract/funding_rate/{symbol1}"
            data = await self._request_json_with_domain_fallback("GET", url1, params=None)

            # 2) fallback COINUSDT
            code_int = _to_int(data.get("code")) if isinstance(data, dict) else None
            if not data or (code_int is not None and code_int != 0) or self._looks_empty_top(data):
                symbol2 = symbol1.replace("_", "")
                url2 = f"/api/v1/contract/funding_rate/{symbol2}"
                data = await self._request_json_with_domain_fallback("GET", url2, params=None)

            if not data:
                logger.warning(f"MEXC: не удалось получить фандинг для {coin}")
                return None

            if isinstance(data, dict) and "code" in data:
                code_int = _to_int(data.get("code"))
                if code_int in _MEXC_NOT_FOUND_CODES:
                    logger.debug(f"MEXC: funding not found for {coin}: code={code_int} msg={data.get('msg')}")
                    return None
                if code_int is not None and code_int != 0:
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
            await self._ensure_mexc_aliases_loaded()
            # 1) пробуем COIN_USDT (с учетом алиасов)
            symbol1 = self._normalize_symbol(coin)
            url1 = f"/api/v1/contract/funding_rate/{symbol1}"
            data = await self._request_json_with_domain_fallback("GET", url1, params=None)

            # 2) fallback COINUSDT
            code_int = _to_int(data.get("code")) if isinstance(data, dict) else None
            if not data or (code_int is not None and code_int != 0) or self._looks_empty_top(data):
                symbol2 = symbol1.replace("_", "")
                url2 = f"/api/v1/contract/funding_rate/{symbol2}"
                data = await self._request_json_with_domain_fallback("GET", url2, params=None)

            if not data:
                logger.warning(f"MEXC: не удалось получить фандинг для {coin}")
                return None

            if isinstance(data, dict) and "code" in data:
                code_int = _to_int(data.get("code"))
                if code_int in _MEXC_NOT_FOUND_CODES:
                    logger.debug(f"MEXC: funding not found for {coin}: code={code_int} msg={data.get('msg')}")
                    return None
                if code_int is not None and code_int != 0:
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

            funding_rate = float(funding_rate_raw)
            
            # MEXC API может не предоставлять время следующей выплаты в этом эндпоинте
            # Проверяем возможные поля для времени
            next_funding_time = None
            # Пробуем найти время в разных полях
            for field in ["nextFundingTime", "nextFundingTimeMs", "fundingTime", "nextFunding", "nextSettleTime", "settleTime", "nextFundingTimestamp", "settleTimestamp"]:
                time_val = item.get(field)
                if time_val is not None:
                    try:
                        next_funding_time = int(time_val)
                        break
                    except (TypeError, ValueError):
                        continue

            return {
                "funding_rate": funding_rate,
                "next_funding_time": next_funding_time,
            }

        except Exception as e:
            logger.error(f"MEXC: ошибка при получении funding info для {coin}: {e}", exc_info=True)
            return None

    async def get_orderbook(self, coin: str, limit: int = 50) -> Optional[Dict]:
        """
        ВАЖНО: по Contract V1 depth идёт как:
        GET /api/v1/contract/depth/{symbol}?limit=...
        """
        try:
            await self._ensure_mexc_aliases_loaded()
            limit_i = max(1, min(int(limit), 200))

            # 1) COIN_USDT (с учетом алиасов)
            symbol1 = self._normalize_symbol(coin)
            url1 = f"/api/v1/contract/depth/{symbol1}"
            data = await self._request_json_with_domain_fallback(
                "GET",
                url1,
                params={"limit": limit_i},
                retries_per_domain=1,
                backoff_s=0.25,
            )

            # 2) fallback COINUSDT
            code_int = _to_int(data.get("code")) if isinstance(data, dict) else None
            if not data or (code_int is not None and code_int != 0) or self._looks_empty_top(data):
                symbol2 = symbol1.replace("_", "")
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

            if isinstance(data, dict) and "code" in data:
                code_int = _to_int(data.get("code"))
                if code_int in _MEXC_NOT_FOUND_CODES:
                    logger.debug(f"MEXC: orderbook not found for {coin}: code={code_int} msg={data.get('msg')}")
                    return None
                if code_int is not None and code_int != 0:
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

    async def get_all_futures_coins(self) -> Set[str]:
        """
        Возвращает множество монет, доступных во фьючерсах на MEXC.
        
        Returns:
            Множество монет без суффиксов (например, {"BTC", "ETH", "SOL", ...})
        """
        return await fetch_mexc_coins(self)
