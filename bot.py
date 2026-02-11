"""
Бот для арбитража фьючерсов между биржами
"""
import asyncio
import base64
import logging
import os
import re
import hmac
import hashlib
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any, Tuple
from exchanges.async_bybit import AsyncBybitExchange
from exchanges.async_gate import AsyncGateExchange
from exchanges.async_mexc import AsyncMexcExchange
from exchanges.async_lbank import AsyncLbankExchange
from exchanges.async_xt import AsyncXtExchange
from exchanges.async_binance import AsyncBinanceExchange
from exchanges.async_bitget import AsyncBitgetExchange
from exchanges.async_okx import AsyncOkxExchange
from exchanges.async_bingx import AsyncBingxExchange
from input_parser import parse_input
from news_monitor import NewsMonitor
from announcements_monitor import AnnouncementsMonitor
from telegram_sender import TelegramSender
import config
from position_opener import (
    OpenLegResult,
    _binance_private_request,
    _bingx_private_request,
    _bitget_private_request,
    _bybit_private_request,
    _gate_private_request,
    _mexc_private_request,
    _xt_private_request,
    _plan_one_leg,
    close_long_short_positions,
    open_long_short_positions,
)

# Настройка логирования
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(config.LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Отключаем логирование HTTP запросов от httpx
logging.getLogger("httpx").setLevel(logging.WARNING)


def format_number(value: Optional[float], precision: int = 3) -> str:
    """
    Форматирует число до указанной точности и убирает нули на конце.
    
    Args:
        value: Число для форматирования (может быть None)
        precision: Количество знаков после запятой (по умолчанию 3)
    
    Returns:
        Отформатированная строка или "N/A" если value is None
    """
    if value is None:
        return "N/A"
    
    formatted = f"{value:.{precision}f}"
    # Убираем нули на конце
    if '.' in formatted:
        formatted = formatted.rstrip('0').rstrip('.')
    
    return formatted


class PerpArbitrageBot:
    """Бот для анализа арбитража фьючерсов"""
    
    def __init__(self):
        self.bybit = AsyncBybitExchange()
        self.gate = AsyncGateExchange()
        self.mexc = AsyncMexcExchange()
        self.lbank = AsyncLbankExchange()
        self.xt = AsyncXtExchange()
        self.binance = AsyncBinanceExchange()
        self.bitget = AsyncBitgetExchange()
        self.okx = AsyncOkxExchange()
        self.bingx = AsyncBingxExchange()
        self.exchanges = {
            "bybit": self.bybit,
            "gate": self.gate,
            "mexc": self.mexc,
            "lbank": self.lbank,
            "xt": self.xt,
            "binance": self.binance,
            "bitget": self.bitget,
            "okx": self.okx,
            "bingx": self.bingx
        }
        self.news_monitor = NewsMonitor()
        self.announcements_monitor = AnnouncementsMonitor(news_monitor=self.news_monitor)
    
    async def close(self):
        """Закрывает соединения с биржами"""
        await asyncio.gather(
            self.bybit.close(),
            self.gate.close(),
            self.mexc.close(),
            self.lbank.close(),
            self.xt.close(),
            self.binance.close(),
            self.bitget.close(),
            self.okx.close(),
            self.bingx.close(),
            return_exceptions=True
        )
    
    async def get_futures_data(self, exchange_name: str, coin: str, need_funding: bool = True) -> Optional[Dict]:
        """
        Получить данные о фьючерсе (цена и фандинг) для монеты на бирже
        
        Args:
            exchange_name: Название биржи ("bybit" или "gate")
            coin: Название монеты (например, "CVC")
            need_funding: Запрашивать ли фандинг (по умолчанию True)
            
        Returns:
            Словарь с данными:
            {
                "price": float,
                "bid": float,
                "ask": float,
                "funding_rate": float (если need_funding=True)
            }
            или None если ошибка
        """
        exchange = self.exchanges.get(exchange_name)
        if not exchange:
            logger.error(f"Неизвестная биржа: {exchange_name}")
            return None
        
        # Всегда тянем bid/ask
        ticker = await exchange.get_futures_ticker(coin)
        
        if isinstance(ticker, Exception):
            logger.error(f"{exchange_name}: ошибка при получении тикера для {coin}: {ticker}")
            ticker = None
        
        if not ticker:
            return None
        
        out = {
            "price": ticker.get("price"),
            "bid": ticker.get("bid"),
            "ask": ticker.get("ask"),
        }
        
        # Funding — только если нужно
        if need_funding:
            funding_rate = await exchange.get_funding_rate(coin)
            
            if isinstance(funding_rate, Exception):
                logger.error(f"{exchange_name}: ошибка при получении фандинга для {coin}: {funding_rate}")
                funding_rate = None
            
            if funding_rate is not None:
                out["funding_rate"] = funding_rate
        
        return out
    
    def calculate_spread(self, price_short: Optional[float], price_long: Optional[float]) -> Optional[float]:
        """
        Вычислить спред на цену для арбитража (в процентах)
        
        Формула: (price_short - price_long) / price_long * 100
        
        Для схемы Long (A) / Short (B):
        - Положительный спред = хорошо (цена на Short бирже выше)
        - Отрицательный спред = плохо (цена на Short бирже ниже)
        
        Args:
            price_short: Цена на бирже Short
            price_long: Цена на бирже Long
            
        Returns:
            Спред в процентах или None если невозможно вычислить
        """
        if price_short is None or price_long is None:
            return None
        
        if price_long == 0:
            return None
        
        spread = ((price_short - price_long) / price_long) * 100
        return spread
    
    def calculate_funding_spread(self, funding_long: Optional[float], funding_short: Optional[float]) -> Optional[float]:
        """
        Спред фандинга в процентах.

        Идея:
        - на Long нас интересует «модуль дохода/расхода» → берём abs(funding_long);
        - на Short — такой же модуль → берём abs(funding_short);
        - спред = |funding_long| - |funding_short|.

        Args:
            funding_long: Ставка фандинга на бирже Long (десятичный формат, например -0.00824 = -0.824%)
            funding_short: Ставка фандинга на бирже Short (десятичный формат, например 0.00005 = 0.005%)

        Returns:
            (abs(funding_long) - abs(funding_short)) * 100, в процентах.
        """
        if funding_long is None or funding_short is None:
            return None

        funding_long_abs = abs(funding_long)
        funding_short_abs = abs(funding_short)
        return (funding_long_abs - funding_short_abs) * 100.0

    def _extract_usdt_available(self, payload: Any) -> Optional[float]:
        """
        Best-effort extraction of available USDT balance from heterogeneous API payloads.
        """
        target_keys = ("coin", "currency", "asset", "marginCoin", "settleCoin", "ccy")
        value_keys = (
            "availableToWithdraw",
            "availableBalance",
            "available",
            "availBal",
            "availEq",
            "cashBal",
            "walletBalance",
            "equity",
            "balance",
        )
        best: Optional[float] = None
        stack: List[Any] = [payload]
        seen_ids = set()
        max_nodes = 1500
        n = 0

        while stack and n < max_nodes:
            n += 1
            cur = stack.pop()
            oid = id(cur)
            if oid in seen_ids:
                continue
            seen_ids.add(oid)

            if isinstance(cur, dict):
                coin_val = None
                for k in target_keys:
                    if cur.get(k) is not None:
                        coin_val = str(cur.get(k)).upper().strip()
                        break
                if coin_val == "USDT":
                    for vk in value_keys:
                        raw = cur.get(vk)
                        if raw is None:
                            continue
                        try:
                            val = float(raw)
                        except Exception:
                            continue
                        if val >= 0 and (best is None or val > best):
                            best = val
                for v in cur.values():
                    if isinstance(v, (dict, list)):
                        stack.append(v)
            elif isinstance(cur, list):
                for item in cur:
                    if isinstance(item, (dict, list)):
                        stack.append(item)

        return best

    async def _get_exchange_usdt_available(self, exchange_name: str, exchange_obj: Any) -> Tuple[Optional[float], str]:
        ex = (exchange_name or "").lower().strip()
        try:
            if ex == "bybit":
                api_key = os.getenv("BYBIT_API_KEY", "").strip()
                api_secret = os.getenv("BYBIT_API_SECRET", "").strip()
                if not api_key or not api_secret:
                    return None, "missing BYBIT_API_KEY/BYBIT_API_SECRET"
                for account_type in ("UNIFIED", "CONTRACT"):
                    data = await _bybit_private_request(
                        exchange_obj=exchange_obj,
                        api_key=api_key,
                        api_secret=api_secret,
                        method="GET",
                        path="/v5/account/wallet-balance",
                        params={"accountType": account_type, "coin": "USDT"},
                    )
                    if isinstance(data, dict) and data.get("retCode") not in (None, 0):
                        msg = str(data.get("retMsg") or data.get("retCode"))
                        return None, f"wallet-balance error: {msg}"
                    val = self._extract_usdt_available(data)
                    if val is not None:
                        return val, f"accountType={account_type}"
                return None, "USDT not found in wallet-balance"

            if ex == "mexc":
                api_key = os.getenv("MEXC_API_KEY", "").strip()
                api_secret = os.getenv("MEXC_API_SECRET", "").strip()
                if not api_key or not api_secret:
                    return None, "missing MEXC_API_KEY/MEXC_API_SECRET"
                data = await _mexc_private_request(
                    exchange_obj=exchange_obj,
                    api_key=api_key,
                    api_secret=api_secret,
                    method="GET",
                    path="/api/v1/private/account/assets",
                    params={},
                )
                if isinstance(data, dict) and data.get("_error"):
                    return None, f"account/assets error: {data.get('_error')}"
                if isinstance(data, dict) and data.get("success") is False:
                    msg = str(data.get("msg") or data.get("code") or "unknown error")
                    return None, f"account/assets error: {msg}"
                val = self._extract_usdt_available(data)
                if val is not None:
                    return val, "account/assets"
                return None, "USDT not found in account/assets"

            if ex == "binance":
                api_key = os.getenv("BINANCE_API_KEY", "").strip()
                api_secret = os.getenv("BINANCE_API_SECRET", "").strip()
                if not api_key or not api_secret:
                    return None, "missing BINANCE_API_KEY/BINANCE_API_SECRET"
                data = await _binance_private_request(
                    exchange_obj=exchange_obj,
                    api_key=api_key,
                    api_secret=api_secret,
                    method="GET",
                    path="/fapi/v2/balance",
                    params={},
                )
                if isinstance(data, dict) and data.get("_error"):
                    return None, f"fapi/v2/balance error: {data.get('_error')}"
                val = self._extract_usdt_available(data)
                if val is not None:
                    return val, "fapi/v2/balance"
                return None, "USDT not found in fapi/v2/balance"

            if ex == "gate":
                api_key = os.getenv("GATEIO_API_KEY", "").strip()
                api_secret = os.getenv("GATEIO_API_SECRET", "").strip()
                if not api_key or not api_secret:
                    return None, "missing GATEIO_API_KEY/GATEIO_API_SECRET"
                data = await _gate_private_request(
                    exchange_obj=exchange_obj,
                    api_key=api_key,
                    api_secret=api_secret,
                    method="GET",
                    path="/api/v4/futures/usdt/accounts",
                    params={},
                )
                if isinstance(data, dict) and data.get("_error"):
                    return None, f"futures/usdt/accounts error: {data.get('_error')}"
                val = self._extract_usdt_available(data)
                if val is not None:
                    return val, "futures/usdt/accounts"
                return None, "USDT not found in futures/usdt/accounts"

            if ex == "bitget":
                api_key = os.getenv("BITGET_API_KEY", "").strip()
                api_secret = os.getenv("BITGET_API_SECRET", "").strip()
                api_passphrase = os.getenv("BITGET_API_PASSPHRASE", "").strip()
                if not api_key or not api_secret:
                    return None, "missing BITGET_API_KEY/BITGET_API_SECRET"
                if not api_passphrase:
                    return None, "missing BITGET_API_PASSPHRASE"

                data = await _bitget_private_request(
                    exchange_obj=exchange_obj,
                    api_key=api_key,
                    api_secret=api_secret,
                    api_passphrase=api_passphrase,
                    method="GET",
                    path="/api/v2/mix/account/accounts",
                    params={"productType": "USDT-FUTURES"},
                )
                if isinstance(data, dict) and data.get("_error"):
                    return None, f"mix/account/accounts error: {data.get('_error')}"
                if isinstance(data, dict) and str(data.get("code")) not in ("00000", "0", ""):
                    msg = str(data.get("msg") or data.get("code") or "unknown error")
                    return None, f"mix/account/accounts error: {msg}"
                val = self._extract_usdt_available(data)
                if val is not None:
                    return val, "mix/account/accounts"
                return None, "USDT not found in mix/account/accounts"

            if ex == "bingx":
                api_key = os.getenv("BINGX_API_KEY", "").strip()
                api_secret = os.getenv("BINGX_API_SECRET", "").strip()
                if not api_key or not api_secret:
                    return None, "missing BINGX_API_KEY/BINGX_API_SECRET"
                data = await _bingx_private_request(
                    exchange_obj=exchange_obj,
                    api_key=api_key,
                    api_secret=api_secret,
                    method="GET",
                    path="/openApi/swap/v2/user/balance",
                    params={},
                )
                if isinstance(data, dict) and data.get("_error"):
                    return None, f"user/balance error: {data.get('_error')}"
                if isinstance(data, dict) and str(data.get("code")) not in ("0", "00000", ""):
                    msg = str(data.get("msg") or data.get("code") or "unknown error")
                    return None, f"user/balance error: {msg}"
                val = self._extract_usdt_available(data)
                if val is not None:
                    return val, "user/balance"
                return None, "USDT not found in user/balance"

            if ex == "xt":
                api_key = os.getenv("XT_API_KEY", "").strip()
                api_secret = os.getenv("XT_API_SECRET", "").strip()
                if not api_key or not api_secret:
                    return None, "missing XT_API_KEY/XT_API_SECRET"
                data = await _xt_private_request(
                    exchange_obj=exchange_obj,
                    api_key=api_key,
                    api_secret=api_secret,
                    method="GET",
                    path="/future/user/v1/balance/list",
                    params={},
                )
                if isinstance(data, dict) and data.get("_error"):
                    return None, f"balance/list error: {data.get('_error')}"
                if isinstance(data, dict) and data.get("returnCode") not in (None, 0):
                    msg = str(data.get("msg") or data.get("returnCode") or "unknown error")
                    return None, f"balance/list error: {msg}"
                val = self._extract_usdt_available(data)
                if val is not None:
                    return val, "balance/list"
                return None, "USDT not found in balance/list"

            if ex == "okx":
                api_key = os.getenv("OKX_API_KEY", "").strip()
                api_secret = os.getenv("OKX_API_SECRET", "").strip()
                api_passphrase = (os.getenv("OKX_API_PASSPHRASE", "").strip() or os.getenv("OKX_API_PASS", "").strip())
                if not api_key or not api_secret:
                    return None, "missing OKX_API_KEY/OKX_API_SECRET"
                if not api_passphrase:
                    return None, "missing OKX_API_PASSPHRASE"

                method = "GET"
                path = "/api/v5/account/balance"
                params = {"ccy": "USDT"}
                ts = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
                prehash = f"{ts}{method}{path}?ccy=USDT"
                sign = base64.b64encode(
                    hmac.new(api_secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()
                ).decode("utf-8")
                headers = {
                    "OK-ACCESS-KEY": api_key,
                    "OK-ACCESS-SIGN": sign,
                    "OK-ACCESS-TIMESTAMP": ts,
                    "OK-ACCESS-PASSPHRASE": api_passphrase,
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                }
                try:
                    resp = await exchange_obj.client.request(method, path, params=params, headers=headers)
                except Exception as e:
                    return None, f"account/balance http error: {type(e).__name__}: {e}"
                if resp.status_code < 200 or resp.status_code >= 300:
                    return None, f"account/balance http {resp.status_code}"
                try:
                    data = resp.json()
                except Exception:
                    return None, "account/balance bad json"
                if isinstance(data, dict) and str(data.get("code")) not in ("0", ""):
                    msg = str(data.get("msg") or data.get("code") or "unknown error")
                    return None, f"account/balance error: {msg}"
                val = self._extract_usdt_available(data)
                if val is not None:
                    return val, "account/balance"
                return None, "USDT not found in account/balance"

            return None, f"balance check not implemented for {exchange_name}"
        except Exception as e:
            return None, str(e)

    async def check_balances_for_coin(
        self,
        coin: str,
        long_exchange: str,
        short_exchange: str,
        coin_amount: float,
        price_long: Optional[float],
        price_short: Optional[float],
    ) -> Dict[str, Any]:
        """
        Проверка достаточности USDT баланса для открытия обеих ног.
        """
        px_long = float(price_long) if price_long is not None and price_long > 0 else None
        px_short = float(price_short) if price_short is not None and price_short > 0 else None
        fallback_px = px_long or px_short
        req_long = float(coin_amount) * float(px_long or fallback_px or 0.0)
        req_short = float(coin_amount) * float(px_short or fallback_px or 0.0)

        out: Dict[str, Any] = {
            "ok": True,
            "long_ok": True,
            "short_ok": True,
            "required_long_usdt": req_long,
            "required_short_usdt": req_short,
        }

        async def _check_one(ex_name: str, required_usdt: float) -> Tuple[bool, Optional[float], str]:
            ex_obj = self.exchanges.get(ex_name)
            if ex_obj is None:
                return False, None, "exchange not found"
            if required_usdt <= 0:
                return True, None, "required_notional_not_available"
            available, source = await self._get_exchange_usdt_available(ex_name, ex_obj)
            if available is None:
                # Не блокируем анализ, если баланс не удалось прочитать (best-effort).
                src_l = str(source or "").lower()
                if ex_name.lower() == "mexc" and ("http 401" in src_l or "not logged in" in src_l):
                    logger.info(
                        f"ℹ️ Баланс {ex_name} ({coin}): приватный endpoint недоступен (401), проверка баланса пропущена."
                    )
                else:
                    logger.warning(
                        f"⚠️ Баланс {ex_name} ({coin}): не удалось проверить доступный USDT (причина: {source})."
                    )
                return True, None, source
            ok = available + 1e-9 >= required_usdt
            status = "✅" if ok else "❌"
            logger.info(
                f"{status} Баланс {ex_name} ({coin}): доступно {available:.3f} USDT | "
                f"требуется ~{required_usdt:.3f} USDT"
            )
            if not ok:
                logger.warning(
                    f"Недостаточно баланса на {ex_name}: {available:.3f} < {required_usdt:.3f} USDT"
                )
            return ok, available, source

        long_ok, long_available, _ = await _check_one(long_exchange, req_long)
        short_ok, short_available, _ = await _check_one(short_exchange, req_short)
        out["long_ok"] = bool(long_ok)
        out["short_ok"] = bool(short_ok)
        out["long_available_usdt"] = long_available
        out["short_available_usdt"] = short_available
        out["ok"] = bool(long_ok and short_ok)
        return out

    async def check_min_order_constraints_for_coin(
        self,
        coin: str,
        long_exchange: str,
        short_exchange: str,
        coin_amount: float,
    ) -> Dict[str, Any]:
        """
        Проверка минимальных ограничений открытия ордеров (step/minQty/minNotional и т.п.)
        через preflight planner из position_opener.py.
        """
        out: Dict[str, Any] = {"ok": True, "long_ok": True, "short_ok": True}

        async def _check_one(ex_name: str, direction: str) -> Tuple[bool, str]:
            ex_obj = self.exchanges.get(ex_name)
            if ex_obj is None:
                return False, "exchange not found"
            try:
                plan = await _plan_one_leg(
                    exchange_name=ex_name,
                    exchange_obj=ex_obj,
                    coin=coin,
                    direction=direction,
                    coin_amount=float(coin_amount),
                )
            except Exception as e:
                return False, f"preflight exception: {e}"

            if isinstance(plan, OpenLegResult):
                if plan.ok:
                    return True, "ok"
                msg = str(plan.error or "preflight failed")
                if "trading not implemented for this exchange" in msg.lower():
                    logger.warning(
                        f"⚠️ Минимальные ограничения {ex_name} ({direction}): проверка не реализована."
                    )
                    return True, msg
                msg_l = msg.lower()
                if ex_name.lower() == "mexc" and (
                    "401" in msg_l or "not logged in" in msg_l or "авториз" in msg_l
                ):
                    logger.info(
                        f"ℹ️ Минимальные ограничения {ex_name} ({direction}): API недоступен для preflight, проверка пропущена."
                    )
                    return True, msg
                return False, msg
            if isinstance(plan, dict):
                return True, "ok"
            return False, f"unexpected preflight result type: {type(plan).__name__}"

        long_ok, long_msg = await _check_one(long_exchange, "long")
        short_ok, short_msg = await _check_one(short_exchange, "short")
        out["long_ok"] = bool(long_ok)
        out["short_ok"] = bool(short_ok)
        out["ok"] = bool(long_ok and short_ok)
        out["long_msg"] = long_msg
        out["short_msg"] = short_msg

        if not long_ok:
            logger.warning(f"❌ Минимальные ограничения {long_exchange} Long ({coin}): {long_msg}")

        if not short_ok:
            logger.warning(f"❌ Минимальные ограничения {short_exchange} Short ({coin}): {short_msg}")

        return out
    
    async def process_input(self, input_text: str):
        """
        Обработать вводные данные и вывести информацию о фьючерсах и фандингах
        
        Args:
            input_text: Строка с вводными данными (например, "CVC Long (bybit), Short (gate)")
        """
        # Парсим вводные данные
        parsed = parse_input(input_text)
        if not parsed:
            logger.error("Не удалось распарсить вводные данные")
            return
        
        coin = parsed["coin"]
        long_exchange = parsed["long_exchange"]
        short_exchange = parsed["short_exchange"]
        coin_amount = parsed.get("coin_amount")
        if coin_amount is None:
            logger.error("Не указано количество монет (пример: 'DASH Long (bybit), Short (gate) 1')")
            return
        
        # Получаем данные с обеих бирж параллельно
        long_data_task = self.get_futures_data(long_exchange, coin)
        short_data_task = self.get_futures_data(short_exchange, coin)
        
        long_data, short_data = await asyncio.gather(
            long_data_task,
            short_data_task,
            return_exceptions=True
        )
        
        if isinstance(long_data, Exception):
            logger.error(f"Ошибка при получении данных с {long_exchange}: {long_data}")
            long_data = None
        
        if isinstance(short_data, Exception):
            logger.error(f"Ошибка при получении данных с {short_exchange}: {short_data}")
            short_data = None
        
        # Проверяем, доступна ли монета на биржах
        logger.info("=" * 60)
        logger.info(f"Анализ арбитража для {coin}")
        logger.info("=" * 60)
        
        # Если тикер не найден на бирже, монета недоступна/делистирована
        if long_data is None:
            logger.warning(f"⚠️ {coin} недоступна/делистирована на {long_exchange}")
            logger.warning("Арбитраж невозможен: тикер не найден на бирже Long")
            logger.info("=" * 60)
            return None
        
        if short_data is None:
            logger.warning(f"⚠️ {coin} недоступна/делистирована на {short_exchange}")
            logger.warning("Арбитраж невозможен: тикер не найден на бирже Short")
            logger.info("=" * 60)
            return None
        
        # Данные Long биржи
        if long_data:
            price_long = long_data.get("price")
            funding_long = long_data.get("funding_rate")
            if price_long is not None:
                notional_long = coin_amount * price_long
                price_str_long = f"Цена: {price_long:.5f} (qty: {coin_amount:.3f} {coin} | ~{notional_long:.3f} USDT)"
            else:
                price_str_long = "Цена: недоступно"
            funding_str_long = f"Фандинг: {funding_long * 100:.3f}%" if funding_long is not None else "Фандинг: недоступно"
            logger.info(f"(Long {long_exchange}) ({coin}) {price_str_long} {funding_str_long}")
        else:
            logger.error(f"Не удалось получить данные с {long_exchange}")
            price_long = None
            funding_long = None
        
        # Данные Short биржи
        if short_data:
            price_short = short_data.get("price")
            funding_short = short_data.get("funding_rate")
            if price_short is not None:
                notional_short = coin_amount * price_short
                price_str_short = f"Цена: {price_short:.5f} (qty: {coin_amount:.3f} {coin} | ~{notional_short:.3f} USDT)"
            else:
                price_str_short = "Цена: недоступно"
            funding_str_short = f"Фандинг: {funding_short * 100:.3f}%" if funding_short is not None else "Фандинг: недоступно"
            logger.info(f"(Short {short_exchange}) ({coin}) {price_str_short} {funding_str_short}")
        else:
            logger.error(f"Не удалось получить данные с {short_exchange}")
            price_short = None
            funding_short = None
        
        # Вычисляем спреды
        price_spread = None
        if price_long is not None and price_short is not None:
            # Формула: (price_short - price_long) / price_long * 100
            # Положительный спред = хорошо (цена на Short бирже выше)
            price_spread = self.calculate_spread(price_short, price_long)
            if price_spread is not None:
                logger.info(f"({long_exchange} и {short_exchange}) Спред на цену: {price_spread:.3f}%")
            else:
                logger.info(f"({long_exchange} и {short_exchange}) Спред на цену: невозможно вычислить")
        else:
            logger.info(f"({long_exchange} и {short_exchange}) Спред на цену: недоступно")
        
        if funding_long is not None and funding_short is not None:
            funding_spread = self.calculate_funding_spread(funding_long, funding_short)
            if funding_spread is not None:
                logger.info(f"({long_exchange} и {short_exchange}) Спред на фандинги: {funding_spread:.3f}%")
            else:
                logger.info(f"({long_exchange} и {short_exchange}) Спред на фандинги: невозможно вычислить")
                funding_spread = None
        else:
            logger.info(f"({long_exchange} и {short_exchange}) Спред на фандинги: недоступно")
            funding_spread = None
        
        # Вычисляем общий спред (спред на цену + спред на фандинги)
        if price_spread is not None and funding_spread is not None:
            total_spread = price_spread + funding_spread
            logger.info(f"({long_exchange} и {short_exchange}) Спред общий: {total_spread:.3f}%")
        elif price_spread is not None:
            logger.info(f"({long_exchange} и {short_exchange}) Спред общий: недоступно (нет данных по фандингам)")
        elif funding_spread is not None:
            logger.info(f"({long_exchange} и {short_exchange}) Спред общий: недоступно (нет данных по цене)")
        else:
            logger.info(f"({long_exchange} и {short_exchange}) Спред общий: недоступно")
        
        logger.info("=" * 60)
        
        # Проверка баланса на обеих биржах перед анализом ликвидности
        balances_check = await self.check_balances_for_coin(
            coin=coin,
            long_exchange=long_exchange,
            short_exchange=short_exchange,
            coin_amount=float(coin_amount),
            price_long=price_long,
            price_short=price_short,
        )

        # Проверка минимальных ограничений на открытие ордеров перед анализом ликвидности
        min_constraints_check = await self.check_min_order_constraints_for_coin(
            coin=coin,
            long_exchange=long_exchange,
            short_exchange=short_exchange,
            coin_amount=float(coin_amount),
        )

        # Проверяем ликвидность на обеих биржах для указанного размера инвестиций
        # Оценка в USDT для ликвидности: используем last price как приближение
        approx_price = None
        if price_long is not None and price_long > 0:
            approx_price = price_long
        elif price_short is not None and price_short > 0:
            approx_price = price_short
        approx_notional_usdt = float(coin_amount) * float(approx_price) if approx_price else 0.0
        if not balances_check.get("ok", True):
            logger.warning("Недостаточно баланса для открытия обеих ног (анализ ликвидности продолжается).")
        if approx_notional_usdt > 0:
            await self.check_liquidity_for_coin(coin, long_exchange, short_exchange, approx_notional_usdt)
        else:
            logger.warning("Не удалось оценить notional в USDT для проверки ликвидности")
        
        # Проверяем делистинг на обеих биржах
        await self.check_delisting_for_coin(coin, exchanges=[long_exchange, short_exchange])
        
        # Сохраняем данные для мониторинга/трейдинга
        return {
            "coin": coin,
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "coin_amount": coin_amount,
            "long_data": long_data,
            "short_data": short_data
        }
    
    async def check_liquidity_for_coin(self, coin: str, long_exchange: str, short_exchange: str, notional_usdt: float) -> Dict[str, Any]:
        """
        Проверяет ликвидность на обеих биржах для указанного размера инвестиций
        
        Args:
            coin: Символ монеты
            long_exchange: Биржа для Long позиции
            short_exchange: Биржа для Short позиции
            notional_usdt: Размер инвестиций в USDT (для каждой позиции: Long и Short)
        """
        size = float(notional_usdt)

        long_liquidity: Optional[Dict[str, Any]] = None
        short_liquidity: Optional[Dict[str, Any]] = None
        
        # Проверяем ликвидность на Long бирже (для покупки)
        long_exchange_obj = self.exchanges.get(long_exchange)
        if long_exchange_obj:
            long_liquidity = await long_exchange_obj.check_liquidity(
                coin, 
                notional_usdt=size,
                ob_limit=50,
                max_spread_bps=30.0,
                max_impact_bps=50.0,
                mode="entry_long" # Проверяем только глубину на покупку
            )
            if long_liquidity:
                status = "✅" if long_liquidity["ok"] else "❌"
                buy_impact_str = f"{long_liquidity['buy_impact_bps']:.1f}bps" if long_liquidity['buy_impact_bps'] is not None else "N/A"
                reasons_str = f" (Причины: {', '.join(long_liquidity['reasons'])})" if not long_liquidity["ok"] else ""
                logger.info(f"{status} Ликвидность {long_exchange} Long ({coin}): {size:.3f} USDT | "
                          f"spread={long_liquidity['spread_bps']:.1f}bps, buy_impact={buy_impact_str}{reasons_str}")
            else:
                logger.warning(f"Не удалось проверить ликвидность {long_exchange} Long ({coin}) для {size} USDT")
        
        # Проверяем ликвидность на Short бирже (для продажи)
        short_exchange_obj = self.exchanges.get(short_exchange)
        if short_exchange_obj:
            short_liquidity = await short_exchange_obj.check_liquidity(
                coin,
                notional_usdt=size,
                ob_limit=50,
                max_spread_bps=30.0,
                max_impact_bps=50.0,
                mode="entry_short" # Проверяем только глубину на продажу
            )
            if short_liquidity:
                status = "✅" if short_liquidity["ok"] else "❌"
                sell_impact_str = f"{short_liquidity['sell_impact_bps']:.1f}bps" if short_liquidity['sell_impact_bps'] is not None else "N/A"
                reasons_str = f" (Причины: {', '.join(short_liquidity['reasons'])})" if not short_liquidity["ok"] else ""
                logger.info(f"{status} Ликвидность {short_exchange} Short ({coin}): {size:.3f} USDT | "
                          f"spread={short_liquidity['spread_bps']:.1f}bps, sell_impact={sell_impact_str}{reasons_str}")
            else:
                logger.warning(f"Не удалось проверить ликвидность {short_exchange} Short ({coin}) для {size} USDT")

        long_ok = bool(long_liquidity and long_liquidity.get("ok") is True)
        short_ok = bool(short_liquidity and short_liquidity.get("ok") is True)
        return {
            "ok": bool(long_ok and short_ok),
            "long_ok": long_ok,
            "short_ok": short_ok,
            "long": long_liquidity,
            "short": short_liquidity,
            "notional_usdt": size,
        }
    
    async def check_delisting_for_coin(self, coin: str, exchanges: Optional[List[str]] = None, days_back: int = 60):
        """
        Проверяет наличие новостей о делистинге монеты на указанных биржах
        
        Args:
            coin: Символ монеты
            exchanges: Список бирж для проверки (например, ["bybit", "gate"]). Если None, проверка не выполняется.
            days_back: Количество дней назад для поиска (по умолчанию 60)
        """
        try:
            if not exchanges:
                logger.warning(f"Укажите биржи для проверки делистинга {coin}")
                return
            
            # 1) Exchange announcements
            delisting_news = await self.news_monitor.check_delisting(coin, exchanges=exchanges, days_back=days_back)
            
            # Формируем строку с биржами для вывода
            exchanges_str = ", ".join(exchanges)
            
            if not delisting_news:
                logger.info(f"✅ Новостей о делистинге {coin} ({exchanges_str}) за последние {days_back} дней не найдено")

                # Доп. проверка: security/hack новости по монете на тех же биржах
                security_news = await self.announcements_monitor.check_security_for_coin(
                    coin_symbol=coin,
                    exchanges=exchanges,
                    days_back=days_back,
                )
                if not security_news:
                    logger.info(
                        f"✅ Новостей о взломах/безопасности {coin} ({exchanges_str}) за последние {days_back} дней не найдено"
                    )
                else:
                    for n in security_news[:5]:
                        title = (n.get("title") or "")[:120]
                        url = n.get("url") or "N/A"
                        logger.warning(f"⚠️ Security news {coin}: {title} | URL: {url}")
        except Exception as e:
            logger.warning(f"Ошибка при проверке делистинга для {coin}: {e}")
    
    def calculate_opening_spread(self, ask_long: Optional[float], bid_short: Optional[float]) -> Optional[float]:
        """
        Вычислить спред открытия позиции (max)
        
        Формула: (bid_short - ask_long) / ask_long * 100
        Положительное значение = выгодный арбитраж (bid_short > ask_long)
        
        Args:
            ask_long: Цена ask на бирже Long
            bid_short: Цена bid на бирже Short
            
        Returns:
            Спред открытия в процентах или None
            Положительное значение = выгодно открывать, отрицательное = невыгодно
        """
        if ask_long is None or bid_short is None:
            return None
        
        if ask_long == 0:
            return None
        
        spread = ((bid_short - ask_long) / ask_long) * 100
        return spread
    
    def calculate_closing_spread(self, bid_long: Optional[float], ask_short: Optional[float]) -> Optional[float]:
        """
        Вычислить спред закрытия позиции (min)
        
        Формула: (bid_long - ask_short) / ask_short * 100
        
        Args:
            bid_long: Цена bid на бирже Long
            ask_short: Цена ask на бирже Short
            
        Returns:
            Спред закрытия в процентах или None
        """
        if bid_long is None or ask_short is None:
            return None
        
        if ask_short == 0:
            return None
        
        spread = ((bid_long - ask_short) / ask_short) * 100
        return spread
    
    def get_exit_threshold_pct(self) -> float:
        """
        Порог выхода в процентах.
        Используем дефолтные значения:
        - закрытие long: 0.04%
        - закрытие short: 0.04%
        - запас: 0.10%
        """
        close_long_fee_pct = 0.04
        close_short_fee_pct = 0.04
        buffer_pct = 0.10

        return close_long_fee_pct + close_short_fee_pct + buffer_pct
    
    async def monitor_spreads(
        self,
        coin: str,
        long_exchange: str,
        short_exchange: str,
        close_threshold_pct: Optional[float] = None,
        coin_amount: Optional[float] = None,
    ):
        """
        Мониторинг спредов открытия и закрытия каждую секунду
        
        Args:
            coin: Название монеты
            long_exchange: Биржа для Long позиции
            short_exchange: Биржа для Short позиции
            close_threshold_pct: Порог закрытия в процентах (если указан, отправляет сообщение в Telegram при достижении)
            coin_amount: Количество монет (base qty), нужно для авто-закрытия после N уведомлений
        """
        logger.info("=" * 60)
        logger.info(f"Начало мониторинга спредов для {coin}")
        if close_threshold_pct is not None:
            logger.info(f"Порог закрытия для уведомлений: {close_threshold_pct:.2f}%")
        else:
            logger.info("Порог закрытия не установлен, уведомления отключены")
        logger.info("=" * 60)
        
        # Отслеживание времени последней отправки сообщения (ключ: (coin, long_exchange, short_exchange))
        last_sent_time: Dict[tuple, float] = {}
        # Интервал между отправками сообщений о закрытии (секунды), читается из .env
        SEND_INTERVAL_SEC = float(os.getenv("CLOSE_INTERVAL", "60"))  # По умолчанию 60 секунд (1 минута)
        # Окно авто-закрытия: если за 1 минуту (60 секунд) отправились 3 Telegram-сообщения "закрытие при спреде" — закрываем позиции.
        close_alert_window_sec = 60.0  # Фиксированное окно: 1 минута
        close_alert_times: List[float] = []
        
        try:
            while True:
                # Получаем данные с обеих бирж параллельно
                long_data_task = self.get_futures_data(long_exchange, coin)
                short_data_task = self.get_futures_data(short_exchange, coin)
                
                long_data, short_data = await asyncio.gather(
                    long_data_task,
                    short_data_task,
                    return_exceptions=True
                )
                
                if isinstance(long_data, Exception):
                    logger.error(f"Ошибка при получении данных с {long_exchange}: {long_data}")
                    long_data = None
                
                if isinstance(short_data, Exception):
                    logger.error(f"Ошибка при получении данных с {short_exchange}: {short_data}")
                    short_data = None
                
                if long_data and short_data:
                    # Извлекаем данные
                    ask_long = long_data.get("ask")
                    bid_long = long_data.get("bid")
                    funding_long = long_data.get("funding_rate")
                    
                    bid_short = short_data.get("bid")
                    ask_short = short_data.get("ask")
                    funding_short = short_data.get("funding_rate")
                    
                    # Рассчитываем спреды
                    opening_spread = self.calculate_opening_spread(ask_long, bid_short)
                    closing_spread = self.calculate_closing_spread(bid_long, ask_short)
                    
                    # Форматируем фандинги в проценты
                    funding_long_pct = funding_long * 100 if funding_long is not None else None
                    funding_short_pct = funding_short * 100 if funding_short is not None else None
                    
                    # Рассчитываем спред на фандинг (используем тот же метод, что и в process_input)
                    fr_spread = self.calculate_funding_spread(funding_long, funding_short)
                    
                    # Рассчитываем общий спред (спред на цену + спред на фандинг)
                    total_spread = None
                    if opening_spread is not None and fr_spread is not None:
                        total_spread = opening_spread + fr_spread
                    
                    # Формируем строку вывода
                    exit_threshold = self.get_exit_threshold_pct()
                    # Инвертируем знак closing_spread для отображения
                    closing_spread_display = -closing_spread if closing_spread is not None else None
                    if closing_spread_display is not None:
                        if close_threshold_pct is not None:
                            closing_str = f"🚩 Закр: {format_number(closing_spread_display)}% (min: {format_number(exit_threshold)}% цель: {format_number(close_threshold_pct)}%)"
                        else:
                            closing_str = f"🚩 Закр: {format_number(closing_spread_display)}% (min: {format_number(exit_threshold)}%)"
                    else:
                        if close_threshold_pct is not None:
                            closing_str = f"🚩 Закр: N/A (min: {format_number(exit_threshold)}% цель: {format_number(close_threshold_pct)}%)"
                        else:
                            closing_str = f"🚩 Закр: N/A (min: {format_number(exit_threshold)}%)"
                    opening_str = f"⛳ Откр: {format_number(opening_spread)}%" if opening_spread is not None else "⛳ Откр: N/A"
                    
                    fr_spread_str = format_number(fr_spread)
                    total_spread_str = format_number(total_spread)
                    
                    # Формируем информацию о биржах и монете
                    long_ex_str = f"Long {long_exchange}"
                    short_ex_str = f"Short {short_exchange}"
                    coin_str = coin
                    
                    # Выводим одной строкой
                    logger.info(f"{closing_str} | {opening_str} | 💰 fr_spread: {fr_spread_str} | 🎯 total_spread: {total_spread_str} ⚙️  {long_ex_str} | {short_ex_str} | {coin_str}")
                    
                    # Проверяем порог закрытия и отправляем сообщение в Telegram
                    # Для положительных порогов: отправляем, когда closing_spread_display <= close_threshold_pct
                    # (т.е. когда убыток при закрытии становится приемлемым)
                    # Для отрицательных порогов: отправляем, когда closing_spread <= close_threshold_pct
                    threshold_met = False
                    if close_threshold_pct is not None and closing_spread is not None:
                        if close_threshold_pct < 0:
                            # Для отрицательных порогов: спред хуже (меньше) порога
                            threshold_met = closing_spread <= close_threshold_pct
                        else:
                            # Для положительных порогов: используем closing_spread_display (уже инвертированное значение)
                            # Закрываем, когда closing_spread_display <= close_threshold_pct
                            threshold_met = closing_spread_display is not None and closing_spread_display <= close_threshold_pct
                    
                    # Дополнительная проверка (Telegram) по fr_spread:
                    # ВАЖНО: если порог закрытия не задан — уведомления ДОЛЖНЫ быть отключены полностью.
                    fr_threshold_met = False
                    if close_threshold_pct is not None and fr_spread is not None:
                        fr_threshold_met = fr_spread <= 0.05
                    
                    # Проверяем, выполняются ли оба условия одновременно
                    both_conditions_met = threshold_met and fr_threshold_met
                    current_time = time.time()
                    
                    # Если оба условия выполняются одновременно — отправляем только одно сообщение
                    if both_conditions_met:
                        # Используем общий ключ для отслеживания времени отправки при выполнении обоих условий
                        key_both = (coin, long_exchange, short_exchange, "both")
                        last_sent_both = last_sent_time.get(key_both, 0)
                        
                        if current_time - last_sent_both >= SEND_INTERVAL_SEC:
                            try:
                                telegram = TelegramSender()
                                if telegram.enabled:
                                    channel_id = config.FREE_CHANNEL_ID
                                    if channel_id:
                                        long_ex_capitalized = long_exchange.capitalize()
                                        short_ex_capitalized = short_exchange.capitalize()
                                        
                                        message_lines = [
                                            f"⏰ <b>Time to close {coin}:</b> Long ({long_ex_capitalized}) / Short ({short_ex_capitalized})",
                                        ]
                                        
                                        exit_threshold = self.get_exit_threshold_pct()
                                        if closing_spread_display is not None:
                                            if close_threshold_pct is not None:
                                                message_lines.append(f"🚩 <b>Close price:</b> {format_number(closing_spread_display)}% (min: {format_number(exit_threshold)}% цель: {format_number(close_threshold_pct)}%)")
                                            else:
                                                message_lines.append(f"🚩 <b>Close price:</b> {format_number(closing_spread_display)}% (min: {format_number(exit_threshold)}%)")
                                        else:
                                            message_lines.append(f"🚩 <b>Close price:</b> N/A (min: {format_number(exit_threshold)}%)")
                                        
                                        fr_spread_formatted = format_number(fr_spread)
                                        total_spread_formatted = format_number(total_spread)
                                        message_lines.append(f"💰 fr_spread: {fr_spread_formatted} | 🎯 total_spread: {total_spread_formatted}")
                                        
                                        telegram_message = "\n".join(message_lines)
                                        await telegram.send_message(telegram_message, channel_id=channel_id)
                                        
                                        # Обновляем время последней отправки для всех ключей
                                        key = (coin, long_exchange, short_exchange)
                                        key_fr = (coin, long_exchange, short_exchange, "fr_spread")
                                        last_sent_time[key] = current_time
                                        last_sent_time[key_fr] = current_time
                                        last_sent_time[key_both] = current_time

                                        # Учет "закрытие при спреде" для авто-закрытия
                                        close_alert_times.append(current_time)
                                        cutoff = current_time - close_alert_window_sec
                                        close_alert_times[:] = [t for t in close_alert_times if t >= cutoff]
                                        if len(close_alert_times) >= 3:
                                            if coin_amount is None:
                                                logger.error("❌ Авто-закрытие: неизвестно количество монет (coin_amount=None), закрытие пропущено")
                                                close_alert_times.clear()
                                            else:
                                                logger.warning(f"🧯 Авто-закрытие: 3 уведомления о закрытии за {close_alert_window_sec:.0f}с — закрываем позиции")
                                                ok_closed = await close_long_short_positions(
                                                    bot=self,
                                                    coin=coin,
                                                    long_exchange=long_exchange,
                                                    short_exchange=short_exchange,
                                                    coin_amount=coin_amount,
                                                )
                                                if ok_closed:
                                                    logger.info("✅ Авто-закрытие выполнено, мониторинг остановлен")
                                                    return
                                                logger.error("❌ Авто-закрытие не удалось, мониторинг остановлен")
                                                return
                                        
                                        closing_display_log = format_number(closing_spread_display) if closing_spread_display is not None else "N/A"
                                        threshold_log = format_number(close_threshold_pct) if close_threshold_pct is not None else "N/A"
                                        fr_spread_log = format_number(fr_spread)
                                        logger.info(f"📱 Отправлено сообщение в Telegram: закрытие при спреде {closing_display_log}% <= {threshold_log}% и fr_spread {fr_spread_log}% <= 0.05%")
                                    else:
                                        logger.warning(f"📱 Telegram включен, но канал не настроен для режима {config.ENV_MODE}")
                            except Exception as e:
                                logger.warning(f"Ошибка отправки в Telegram: {e}", exc_info=True)
                    else:
                        # Если только одно условие выполняется — отправляем сообщения как обычно
                        if threshold_met:
                            # Проверяем интервал между отправками (раз в минуту)
                            key = (coin, long_exchange, short_exchange)
                            last_sent = last_sent_time.get(key, 0)
                            
                            if current_time - last_sent >= SEND_INTERVAL_SEC:
                                try:
                                    telegram = TelegramSender()
                                    if telegram.enabled:
                                        # bot.py всегда использует FREE_CHANNEL_ID
                                        channel_id = config.FREE_CHANNEL_ID
                                        if channel_id:
                                            # Формируем сообщение в новом формате
                                            long_ex_capitalized = long_exchange.capitalize()
                                            short_ex_capitalized = short_exchange.capitalize()
                                            
                                            message_lines = [
                                                f"⏰ <b>Time to close {coin}:</b> Long ({long_ex_capitalized}) / Short ({short_ex_capitalized})",
                                            ]
                                            
                                            exit_threshold = self.get_exit_threshold_pct()
                                            # Используем closing_spread_display из лога (уже инвертированное значение)
                                            if closing_spread_display is not None:
                                                if close_threshold_pct is not None:
                                                    message_lines.append(f"🚩 <b>Close price:</b> {format_number(closing_spread_display)}% (min: {format_number(exit_threshold)}% цель: {format_number(close_threshold_pct)}%)")
                                                else:
                                                    message_lines.append(f"🚩 <b>Close price:</b> {format_number(closing_spread_display)}% (min: {format_number(exit_threshold)}%)")
                                            else:
                                                message_lines.append(f"🚩 <b>Close price:</b> N/A (min: {format_number(exit_threshold)}%)")
                                            
                                            # Используем fr_spread и total_spread из лога, форматируем через format_number
                                            fr_spread_formatted = format_number(fr_spread)
                                            total_spread_formatted = format_number(total_spread)
                                            message_lines.append(f"💰 fr_spread: {fr_spread_formatted} | 🎯 total_spread: {total_spread_formatted}")
                                            
                                            telegram_message = "\n".join(message_lines)
                                            await telegram.send_message(telegram_message, channel_id=channel_id)
                                            
                                            # Обновляем время последней отправки
                                            last_sent_time[key] = current_time

                                            # Учет "закрытие при спреде" для авто-закрытия
                                            close_alert_times.append(current_time)
                                            cutoff = current_time - close_alert_window_sec
                                            close_alert_times[:] = [t for t in close_alert_times if t >= cutoff]
                                            if len(close_alert_times) >= 3:
                                                if coin_amount is None:
                                                    logger.error("❌ Авто-закрытие: неизвестно количество монет (coin_amount=None), закрытие пропущено")
                                                    close_alert_times.clear()
                                                else:
                                                    logger.warning(f"🧯 Авто-закрытие: 3 уведомления о закрытии за {close_alert_window_sec:.0f}с — закрываем позиции")
                                                    ok_closed = await close_long_short_positions(
                                                        bot=self,
                                                        coin=coin,
                                                        long_exchange=long_exchange,
                                                        short_exchange=short_exchange,
                                                        coin_amount=coin_amount,
                                                    )
                                                    if ok_closed:
                                                        logger.info("✅ Авто-закрытие выполнено, мониторинг остановлен")
                                                        return
                                                    logger.error("❌ Авто-закрытие не удалось, мониторинг остановлен")
                                                    return
                                            
                                            # Используем closing_spread_display для лога (уже инвертированное значение)
                                            closing_display_log = format_number(closing_spread_display) if closing_spread_display is not None else "N/A"
                                            threshold_log = format_number(close_threshold_pct) if close_threshold_pct is not None else "N/A"
                                            logger.info(f"📱 Отправлено сообщение в Telegram: закрытие при спреде {closing_display_log}% <= {threshold_log}%")
                                        else:
                                            logger.warning(f"📱 Telegram включен, но канал не настроен для режима {config.ENV_MODE}")
                                except Exception as e:
                                    logger.warning(f"Ошибка отправки в Telegram: {e}", exc_info=True)

                        if fr_threshold_met:
                            # Проверяем интервал между отправками (раз в минуту) - используем отдельный ключ для fr_spread
                            key_fr = (coin, long_exchange, short_exchange, "fr_spread")
                            last_sent_fr = last_sent_time.get(key_fr, 0)
                            
                            if current_time - last_sent_fr >= SEND_INTERVAL_SEC:
                                try:
                                    telegram = TelegramSender()
                                    if telegram.enabled:
                                        # bot.py всегда использует FREE_CHANNEL_ID
                                        channel_id = config.FREE_CHANNEL_ID
                                        if channel_id:
                                            # Формируем сообщение в новом формате
                                            long_ex_capitalized = long_exchange.capitalize()
                                            short_ex_capitalized = short_exchange.capitalize()
                                            
                                            message_lines = [
                                                f"⏰ <b>Time to close {coin}:</b> Long ({long_ex_capitalized}) / Short ({short_ex_capitalized})",
                                            ]
                                            
                                            exit_threshold = self.get_exit_threshold_pct()
                                            # Используем closing_spread_display из лога (уже инвертированное значение)
                                            if closing_spread_display is not None:
                                                if close_threshold_pct is not None:
                                                    message_lines.append(f"🚩 <b>Close price:</b> {format_number(closing_spread_display)}% (min: {format_number(exit_threshold)}% цель: {format_number(close_threshold_pct)}%)")
                                                else:
                                                    message_lines.append(f"🚩 <b>Close price:</b> {format_number(closing_spread_display)}% (min: {format_number(exit_threshold)}%)")
                                            else:
                                                message_lines.append(f"🚩 <b>Close price:</b> N/A (min: {format_number(exit_threshold)}%)")
                                            
                                            # Используем fr_spread и total_spread из лога, форматируем через format_number
                                            fr_spread_formatted = format_number(fr_spread)
                                            total_spread_formatted = format_number(total_spread)
                                            message_lines.append(f"💰 fr_spread: {fr_spread_formatted} | 🎯 total_spread: {total_spread_formatted}")
                                            
                                            telegram_message = "\n".join(message_lines)
                                            await telegram.send_message(telegram_message, channel_id=channel_id)
                                            
                                            # Обновляем время последней отправки
                                            last_sent_time[key_fr] = current_time
                                            
                                            # Логируем отправку
                                            fr_spread_log = format_number(fr_spread)
                                            logger.info(f"📱 Отправлено сообщение в Telegram: fr_spread {fr_spread_log}% <= 0.05%")
                                        else:
                                            logger.warning(f"📱 Telegram включен, но канал не настроен для режима {config.ENV_MODE}")
                                except Exception as e:
                                    logger.warning(f"Ошибка отправки в Telegram: {e}", exc_info=True)
                            else:
                                # Интервал не прошел, пропускаем отправку
                                remaining = SEND_INTERVAL_SEC - (current_time - last_sent_fr)
                                logger.debug(f"Пропуск отправки: интервал не прошел (осталось {remaining:.1f}с)")
                
                # Ждем 1 секунду перед следующей итерацией
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("=" * 60)
            logger.info("Мониторинг прерван пользователем")
            logger.info("=" * 60)
        except Exception as e:
            logger.error(f"Ошибка в мониторинге: {e}", exc_info=True)


async def main():
    """Главная функция"""
    bot = PerpArbitrageBot()
    
    try:
        # Читаем вводные данные из командной строки или stdin
        raw_args = [a.strip() for a in sys.argv[1:]]
        # Флаги управления интерактивностью/мониторингом
        monitor_forced = ("--monitor" in raw_args)
        monitor_disabled = ("--no-monitor" in raw_args) or ("--no-prompt" in raw_args)
        filtered_args = [a for a in raw_args if a not in ("--monitor", "--no-monitor", "--no-prompt")]

        if filtered_args:
            # Вводные данные переданы как аргументы командной строки
            input_text = " ".join(filtered_args)
        else:
            # Читаем из stdin
            print("Введите данные в формате: 'монета Long (биржа), Short (биржа) количество_монет'")
            print("Пример: DASH Long (bybit), Short (gate) 1")
            input_text = input().strip()
        
        if not input_text:
            logger.error("Не указаны вводные данные")
            return
        
        # Обрабатываем вводные данные и получаем информацию для мониторинга
        monitoring_data = await bot.process_input(input_text)
        
        if monitoring_data:
            should_monitor = False

            if monitor_forced:
                should_monitor = True
            elif monitor_disabled:
                should_monitor = False
            else:
                # Спрашиваем про открытие позиций
                print("\nОткрыть позиции в лонг и шорт?")
                print("\nВедите 'Да' или 'Нет': если 'Да', то позиции будут открыты и введите min цену (через .) закр, для отправки сообщения в тг")
                # Если запуск не интерактивный — не блокируемся.
                if not sys.stdin or not sys.stdin.isatty() or os.getenv("BOT_NO_PROMPT") == "1":
                    should_monitor = False
                    close_threshold_pct = None
                else:
                    answer1 = input().strip()
                    answer1_lower = answer1.lower()
                    open_positions = answer1_lower.startswith("да") or answer1_lower.startswith("yes") or answer1_lower.startswith("y")
                    
                    close_threshold_pct = None
                    should_monitor = False
                    
                    if open_positions:
                        # Парсим порог закрытия из ввода (формат: "Да, 0.05" или "Да 0.05" или "да, 0.05")
                        match = re.search(r'([-]?\d+\.?\d*)', answer1)
                        if match:
                            try:
                                close_threshold_pct = float(match.group(1))
                            except ValueError:
                                close_threshold_pct = None
                        else:
                            # Если цена не указана, выдаем ошибку
                            logger.error(f"❌ Не указана min цена закрытия. Ожидается формат: 'Да, 0.05' или 'Да 0.05'")
                            logger.error("Мониторинг не запущен")
                            return
                        
                        # Автоматическое открытие позиций (лонг+шорт) по API, затем мониторинг как обычно
                        open_result = await open_long_short_positions(
                            bot=bot,
                            coin=monitoring_data["coin"],
                            long_exchange=monitoring_data["long_exchange"],
                            short_exchange=monitoring_data["short_exchange"],
                            coin_amount=monitoring_data["coin_amount"],
                        )
                        opened_ok = open_result[0] if isinstance(open_result, (tuple, list)) else bool(open_result)
                        if opened_ok:
                            # Если ноги открылись на разный объём — для мониторинга используем хеджируемую часть (min)
                            if isinstance(open_result, (tuple, list)) and len(open_result) >= 5:
                                try:
                                    long_q = float(open_result[3] or 0.0)
                                    short_q = float(open_result[4] or 0.0)
                                    if long_q > 0 and short_q > 0:
                                        monitoring_data["coin_amount"] = min(long_q, short_q)
                                except Exception:
                                    pass
                            should_monitor = True
                            # После успешного открытия позиций мониторинг запускается с указанным порогом закрытия
                            # close_threshold_pct уже установлен выше
                    else:
                        # Если ответ "Нет" на открытие позиций, спрашиваем про мониторинг
                        print("\nВключить мониторинг?")
                        print("Введите 'Да' или 'Нет': если 'Да', то введите min цену (через .) закр, для отправки сообщения в тг")
                        answer2 = input().strip()
                        answer2_lower = answer2.lower()
                        monitor_yes = answer2_lower.startswith("да") or answer2_lower.startswith("yes") or answer2_lower.startswith("y")
                        
                        if monitor_yes:
                            should_monitor = True
                            # Парсим порог закрытия из ввода (формат: "Да, 2%" или "Да, 2.5%" или "Да, -1%" или "Да 3")
                            match = re.search(r'([-]?\d+\.?\d*)', answer2)
                            if match:
                                try:
                                    close_threshold_pct = float(match.group(1))
                                except ValueError:
                                    close_threshold_pct = None
                                    logger.warning(f"Не удалось распарсить порог закрытия из '{answer2}', мониторинг без уведомлений")
                            else:
                                logger.warning(f"Не найден порог закрытия в '{answer2}', мониторинг без уведомлений")
                        else:
                            should_monitor = False
                            close_threshold_pct = None
            
            if should_monitor:
                # Запускаем мониторинг
                await bot.monitor_spreads(
                    monitoring_data["coin"],
                    monitoring_data["long_exchange"],
                    monitoring_data["short_exchange"],
                    close_threshold_pct=close_threshold_pct,
                    coin_amount=monitoring_data.get("coin_amount"),
                )
            else:
                logger.info("Мониторинг не запущен")
        
    except KeyboardInterrupt:
        logger.info("Прервано пользователем")
    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
    finally:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())
