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
            name = it.get("name")  # BTC_USDT
            if not isinstance(name, str) or not name.endswith("_USDT"):
                continue
            
            # если есть флаги делистинга/торгового статуса — фильтруем мягко
            if it.get("in_delisting") is True:
                continue
            ts = it.get("trade_status")
            if ts and str(ts).lower() not in ("tradable", "trading"):
                continue
            
            coins.add(name.replace("_USDT", "").upper())
        
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
            
            # если есть состояние — отсеиваем неактивные
            state = it.get("state")
            if state is not None and str(state) not in ("0", "1"):
                continue
            
            coins.add(sym.replace("_USDT", "").upper())
        
        return coins
    except Exception as e:
        logger.error(f"MEXC: ошибка при получении списка монет: {e}", exc_info=True)
        return set()


async def fetch_xt_coins(exchange) -> Set[str]:
    """Получить список монет с XT.com"""
    coins: Set[str] = set()
    
    try:
        data = await exchange._request_json("GET", "/future/market/v1/public/q/instruments", params={})
        if not data or data.get("returnCode") != 0:
            logger.warning("XT: не удалось получить список инструментов")
            return coins
        
        result = data.get("result")
        if not result or not isinstance(result, list):
            return coins
        
        for item in result:
            symbol = item.get("symbol", "")
            if symbol.endswith("_usdt"):
                coin = symbol[:-5].upper()  # "_usdt" длина 5, убираем и приводим к верхнему регистру
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
    """Получить список монет с Bitget"""
    coins: Set[str] = set()
    
    try:
        url = "/api/v2/mix/market/contracts"
        params = {"productType": exchange.PRODUCT_TYPE}  # "umcbl" для USDT-M Futures (Perpetual)
        
        data = await exchange._request_json("GET", url, params=params)
        if not data or exchange._is_api_error(data):
            return coins
        
        items = data.get("data")
        if not isinstance(items, list):
            return coins
        
        for it in items:
            sym = it.get("symbol")  # часто "BTCUSDT_UMCBL" или "BTCUSDT"
            if isinstance(sym, str):
                sym = sym.upper()
                # нормализация под оба варианта
                if sym.endswith("_UMCBL"):
                    sym = sym.replace("_UMCBL", "")
                if sym.endswith("USDT"):
                    coins.add(sym[:-4])  # sym уже upper(), второй .upper() не нужен
        
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
        
        for it in items:
            inst_id = it.get("instId")  # BTC-USDT-SWAP
            settle = it.get("settleCcy")
            if settle != "USDT":
                continue
            if not isinstance(inst_id, str) or not inst_id.endswith("-USDT-SWAP"):
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
                coins.add(sym.replace("-USDT", "").upper())
        
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
