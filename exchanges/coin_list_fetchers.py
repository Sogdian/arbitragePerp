"""
Модуль для получения списка всех доступных монет на фьючерсах для каждой биржи.
Все методы возвращают множество монет без суффиксов (например, {"BTC", "ETH", "SOL", ...}).
"""
from typing import Set, Optional
import logging

logger = logging.getLogger(__name__)


async def fetch_bybit_coins(exchange) -> Set[str]:
    """Получить список монет с Bybit с пагинацией"""
    coins: Set[str] = set()
    cursor: Optional[str] = None
    
    try:
        while True:
            params = {"category": "linear", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            
            data = await exchange._request_json("GET", "/v5/market/instruments-info", params=params)
            if not data or str(data.get("retCode")) != "0":
                break
            
            result = data.get("result") or {}
            items = result.get("list") or []
            
            for it in items:
                # фильтруем именно USDT-perp
                if it.get("quoteCoin") != "USDT":
                    continue
                if it.get("settleCoin") != "USDT":
                    continue
                if it.get("status") and it.get("status") != "Trading":
                    continue
                
                # Фильтруем только PERP (LinearPerpetual), исключаем фьючерсы с датой экспирации
                ct = it.get("contractType")
                if ct and ct != "LinearPerpetual":
                    continue
                
                sym = it.get("symbol")  # BTCUSDT
                if isinstance(sym, str) and sym.endswith("USDT"):
                    coins.add(sym[:-4].upper())
            
            next_cursor = result.get("nextPageCursor")
            if not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor
        
        return coins
    except Exception as e:
        logger.error(f"Bybit: ошибка при получении списка монет: {e}", exc_info=True)
        return set()


async def fetch_gate_coins(exchange) -> Set[str]:
    """Получить список монет с Gate.io"""
    coins: Set[str] = set()
    
    try:
        data = await exchange._request_json("GET", "/api/v4/futures/usdt/contracts", params={})
        if not isinstance(data, list):
            return coins
        
        for it in data:
            if not isinstance(it, dict):
                continue
            
            # контракт может называться "name" или "contract"
            c = it.get("name") or it.get("contract")
            if not isinstance(c, str):
                continue
            
            # берем только *_USDT
            if not c.endswith("_USDT"):
                continue
            
            base = c.replace("_USDT", "")
            if not base:
                continue
            
            # опционально: отсев делистинга/паузы (если такие поля есть)
            if it.get("in_delisting") is True:
                continue
            ts = str(it.get("trade_status", "")).lower()
            if ts in ("delisting", "suspend", "suspended", "closed"):
                continue
            
            base_u = base.upper()
            # ВАЖНО: на Gate контракт FUN_USDT — это SPORTFUN (Sport.Fun), а FUN (FUNTOKEN) там нет.
            if base_u == "FUN":
                coins.add("SPORTFUN")
            else:
                coins.add(base_u)
        
        return coins
    except Exception as e:
        logger.error(f"Gate: ошибка при получении списка монет: {e}", exc_info=True)
        return set()


async def fetch_mexc_coins(exchange) -> Set[str]:
    """Получить список монет с MEXC"""
    coins: Set[str] = set()
    
    try:
        data = await exchange._request_json_with_domain_fallback(
            "GET",
            "/api/v1/contract/detail",
            params={},
            retries_per_domain=1,
            backoff_s=0.25,
        )
        if not isinstance(data, dict):
            return coins
        
        items = data.get("data") or []
        if not isinstance(items, list):
            return coins
        
        for it in items:
            sym = it.get("symbol")  # BTC_USDT
            if not isinstance(sym, str) or not sym.endswith("_USDT"):
                continue
            
            # если есть состояние — отсеиваем только явно неактивные
            # Не делаем жесткий фильтр, чтобы не потерять валидные контракты
            state = str(it.get("state", ""))
            if state in ("3", "4", "5"):  # если знаешь точно неактивные
                continue

            sym_u = sym.upper()
            # ВАЖНО (MEXC): у монеты FUN есть два разных контракта:
            # - FUN_USDT -> FUN (FUNTOKEN)
            # - SPORTFUN_USDT -> SPORTFUN (Sport.Fun)
            # Не смешиваем их. Для этих двух контрактов используем именно symbol, а не displayName.
            if sym_u == "FUN_USDT":
                coins.add("FUN")
                continue
            if sym_u == "SPORTFUN_USDT":
                coins.add("SPORTFUN")
                continue

            # Для остальных контрактов: если есть displayName вида "{COIN}_USDT..." — используем его для имени монеты.
            disp = it.get("displayName") or it.get("display_name") or it.get("displayNameEn") or it.get("display_name_en")
            coin_from_disp = None
            if isinstance(disp, str):
                up = disp.upper()
                idx = up.find("_USDT")
                if idx > 0:
                    coin_from_disp = up[:idx].strip()
                    # небольшая нормализация (убираем пробелы/мусорные символы)
                    coin_from_disp = "".join(ch for ch in coin_from_disp if ch.isalnum())

            coins.add((coin_from_disp or sym.replace("_USDT", "")).upper())
        
        return coins
    except Exception as e:
        logger.error(f"MEXC: ошибка при получении списка монет: {e}", exc_info=True)
        return set()


async def fetch_xt_coins(exchange) -> Set[str]:
    """Получить список монет с XT.com (USDT-M Perpetual)"""
    coins: Set[str] = set()
    
    try:
        # Важно: у XT для фьючерсов часто используется fapi.xt.com и endpoint cg/contracts
        # https://fapi.xt.com/future/market/v1/public/cg/contracts
        data = await exchange._request_json(
            "GET",
            "/future/market/v1/public/cg/contracts",
            params={}
        )
        
        # Вариант A: API вернул список (как часто и бывает)
        if isinstance(data, list):
            items = data
        
        # Вариант B: API вернул dict-обертку
        elif isinstance(data, dict):
            # некоторые ответы: {"returnCode":0,"result":[...]}
            if data.get("returnCode") not in (0, "0", None):
                logger.warning(f"XT: не удалось получить список инструментов (returnCode={data.get('returnCode')}, msg={data.get('msg')})")
                return coins
            items = data.get("result") or data.get("data") or []
            if not isinstance(items, list):
                return coins
        else:
            return coins
        
        for it in items:
            if not isinstance(it, dict):
                continue
            symbol = it.get("symbol") or it.get("contractCode") or ""
            if not isinstance(symbol, str):
                continue
            
            s = symbol.lower()
            # типично: "btc_usdt"
            if s.endswith("_usdt"):
                coin = s[:-5].upper()
                if coin:
                    coins.add(coin)
        
        return coins
    except Exception as e:
        logger.error(f"XT: ошибка при получении списка монет: {e}", exc_info=True)
        return set()


async def fetch_binance_coins(exchange) -> Set[str]:
    """Получить список монет с Binance"""
    coins: Set[str] = set()
    
    try:
        data = await exchange._request_json("GET", "/fapi/v1/exchangeInfo", params={})
        if not isinstance(data, dict):
            return coins
        
        items = data.get("symbols") or []
        if not isinstance(items, list):
            return coins
        
        for it in items:
            if it.get("contractType") != "PERPETUAL":
                continue
            if it.get("quoteAsset") != "USDT":
                continue
            if it.get("status") != "TRADING":
                continue
            
            sym = it.get("symbol")  # BTCUSDT
            if isinstance(sym, str) and sym.endswith("USDT"):
                coins.add(sym[:-4].upper())
        
        return coins
    except Exception as e:
        logger.error(f"Binance: ошибка при получении списка монет: {e}", exc_info=True)
        return set()


async def fetch_bitget_coins(exchange) -> Set[str]:
    """Получить список монет с Bitget (USDT-M Perpetual) через tickers"""
    coins: Set[str] = set()
    
    try:
        url = "/api/v2/mix/market/tickers"
        params = {"productType": "USDT-FUTURES"}
        
        data = await exchange._request_json("GET", url, params=params)
        if not data or exchange._is_api_error(data):
            return coins
        
        items = data.get("data") or []
        if not isinstance(items, list):
            return coins
        
        for it in items:
            if not isinstance(it, dict):
                continue
            sym = it.get("symbol")
            if not isinstance(sym, str):
                continue
            
            s = sym.upper()
            # иногда встречается суффикс старого формата
            if s.endswith("_UMCBL"):
                s = s[:-6]
            
            # основной формат: BTCUSDT
            if s.endswith("USDT") and len(s) > 4:
                base = s[:-4]
                # ВАЖНО (Bitget): FUNUSDT -> SPORTFUN, FUNTOKENUSDT -> FUN (FUNTOKEN)
                if base == "FUN":
                    coins.add("SPORTFUN")
                elif base == "FUNTOKEN":
                    coins.add("FUN")
                else:
                    coins.add(base)
        
        return coins
    except Exception:
        logger.exception("Bitget: ошибка при получении списка монет")
        return set()


async def fetch_okx_coins(exchange) -> Set[str]:
    """Получить список монет с OKX"""
    coins: Set[str] = set()
    
    try:
        data = await exchange._request_json("GET", "/api/v5/public/instruments", params={"instType": "SWAP"})
        if not isinstance(data, dict) or str(data.get("code")) != "0":
            return coins
        
        items = data.get("data") or []
        if not isinstance(items, list):
            return coins

        # --- Временное логирование для отладки SENT ---
        found_sent = False
        for it in items:
            inst_id = it.get("instId")
            state = it.get("state")
            settle = it.get("settleCcy")
            if inst_id and "SENT" in inst_id.upper():
                logger.info(f"DEBUG OKX Instrument: instId={inst_id}, state={state}, settleCcy={settle}")
                found_sent = True
        if not found_sent:
            logger.info("DEBUG OKX: SENT-USDT-SWAP не найден в списке инструментов SWAP от API.")
        # --- Конец временного логирования ---

        for it in items:
            inst_id = it.get("instId")  # BTC-USDT-SWAP
            settle = it.get("settleCcy")
            if settle != "USDT":
                continue
            if not isinstance(inst_id, str) or not inst_id.endswith("-USDT-SWAP"):
                continue

            # Фильтруем только активные инструменты (state == "live")
            state = it.get("state")
            if state and state != "live":
                continue

            coins.add(inst_id.split("-")[0].upper())
        
        return coins
    except Exception as e:
        logger.error(f"OKX: ошибка при получении списка монет: {e}", exc_info=True)
        return set()


async def fetch_bingx_coins(exchange) -> Set[str]:
    """Получить список монет с BingX"""
    coins: Set[str] = set()
    
    try:
        data = await exchange._request_json("GET", "/openApi/swap/v2/quote/contracts", params={})
        if not isinstance(data, dict) or str(data.get("code")) != "0":
            return coins
        
        items = data.get("data") or []
        if not isinstance(items, list):
            return coins
        
        for it in items:
            sym = it.get("symbol")  # BTC-USDT
            if isinstance(sym, str) and sym.endswith("-USDT"):
                base = sym.replace("-USDT", "").upper()
                # ВАЖНО (BingX): FUNTOKEN-USDT -> FUN (FUNTOKEN)
                if base == "FUNTOKEN":
                    coins.add("FUN")
                else:
                    coins.add(base)
        
        return coins
    except Exception as e:
        logger.error(f"BingX: ошибка при получении списка монет: {e}", exc_info=True)
        return set()


async def fetch_lbank_coins(exchange) -> Set[str]:
    """Получить список монет с LBank"""
    coins: Set[str] = set()
    
    try:
        instruments_list = await exchange._get_instruments_with_cache()
        if not instruments_list:
            return coins
        
        for instrument in instruments_list:
            symbol = instrument.get("symbol", "")
            symbol_canon = exchange._canon(symbol)
            if symbol_canon.endswith("USDT"):
                coin = symbol_canon[:-4].upper()
                if coin:
                    coins.add(coin)
        
        return coins
    except Exception as e:
        logger.error(f"LBank: ошибка при получении списка монет: {e}", exc_info=True)
        return set()
