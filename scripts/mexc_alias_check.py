#!/usr/bin/env python3
"""
Запрос MEXC contract/detail: только бессрочные USDT-фьючерсы (perpetual).
Строит алиасы так же, как fetch_mexc_coins: coin (из displayName или symbol) -> API symbol.
Если coin != symbol_base, нужен алиас. Результат выводится для вставки в async_mexc.py.
"""
import json
import sys

import httpx

MEXC_CONTRACT_URL = "https://contract.mexc.com/api/v1/contract/detail"


def coin_from_contract(it: dict) -> tuple[str, str]:
    """Как в fetch_mexc_coins: возвращает (coin_в_списке, symbol)."""
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


def main() -> int:
    print("Request MEXC contract/detail...", flush=True)
    try:
        r = httpx.get(MEXC_CONTRACT_URL, timeout=30.0)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if data.get("code") != 0 or "data" not in data:
        print("Bad API response", file=sys.stderr)
        return 1

    items = data["data"]
    if not isinstance(items, list):
        return 1

    # Только бессрочные USDT-фьючерсы: symbol *_USDT, settleCoin USDT
    aliases: dict[str, str] = {}
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
        coin, symbol = coin_from_contract(it)
        if not coin or not symbol:
            continue
        symbol_base = symbol.replace("_USDT", "").upper()
        if coin != symbol_base:
            aliases[coin] = symbol

    print("\nAliases (coin -> MEXC symbol), only where coin != symbol base:\n")
    for k in sorted(aliases.keys()):
        v = aliases[k]
        try:
            print(f"  {k!r}: {v!r},")
        except UnicodeEncodeError:
            print(f"  {k.encode('ascii', 'replace').decode('ascii')!r}: {v!r},")
    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
