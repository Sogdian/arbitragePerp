"""
Парсер вводных данных для бота арбитража фьючерсов
"""
import re
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)


def parse_input(input_text: str) -> Optional[Dict]:
    """
    Парсит вводные данные в формате: "монета Long (биржа), Short (биржа) [размер]"
    Пример: "CVC Long (bybit), Short (gate) 100"
    Пример без размера: "CVC Long (bybit), Short (gate)"
    
    Args:
        input_text: Строка с вводными данными
        
    Returns:
        Словарь с распарсенными данными:
        {
            "coin": str,           # Название монеты (например, "CVC")
            "long_exchange": str,  # Биржа для Long позиции (например, "bybit")
            "short_exchange": str, # Биржа для Short позиции (например, "gate")
            "notional_usdt": float | None # Размер инвестиций в USDT (например, 100.0) или None если не указан
        }
        или None если ошибка парсинга
    """
    if not input_text:
        logger.error("Пустая строка ввода")
        return None
    
    # Нормализуем строку: убираем лишние пробелы
    normalized = input_text.strip()
    
    # Паттерн для поиска: монета, затем Long (биржа), затем Short (биржа), затем опционально размер инвестиций
    # Примеры:
    # "CVC Long (bybit), Short (gate) 100"
    # "BTC Long (bybit), Short (gate) 50"
    # "ETH Long (gate), Short (bybit) 150"
    # "CVC Long (bybit), Short (gate)" - без размера
    
    # Размер может быть числом ("30", "30.5") или токеном SCAN_COIN_INVEST
    pattern = r'^(\w+)\s+Long\s*\((\w+)\)\s*,\s*Short\s*\((\w+)\)(?:\s+(\d+(?:\.\d+)?|SCAN_COIN_INVEST))?$'
    match = re.match(pattern, normalized, re.IGNORECASE)
    
    if not match:
        logger.error(f"Неверный формат ввода: {input_text}. Ожидается: 'монета Long (биржа), Short (биржа) [размер]'")
        return None
    
    coin = match.group(1).upper()
    long_exchange = match.group(2).lower()
    short_exchange = match.group(3).lower()
    
    # Извлекаем размер инвестиций (опционально)
    notional_usdt = None
    if match.group(4):
        raw_size = str(match.group(4)).strip()
        if raw_size.upper() == "SCAN_COIN_INVEST":
            notional_usdt = None  # будет взято из env в bot.py
        else:
            try:
                notional_usdt = float(raw_size)
                if notional_usdt <= 0:
                    logger.error(f"Размер инвестиций должен быть положительным числом, получено: {notional_usdt}")
                    return None
            except (ValueError, IndexError):
                logger.error(f"Не удалось распарсить размер инвестиций из: {input_text}")
                return None
    
    # Проверяем, что биржи поддерживаются
    # LBank временно отключен для арбитража (код не удален)
    supported_exchanges = {"bybit", "gate", "mexc", "xt", "binance", "bitget", "okx", "bingx"}
    if long_exchange not in supported_exchanges:
        logger.error(f"Неподдерживаемая биржа для Long: {long_exchange}. Поддерживаются: {supported_exchanges}")
        return None
    
    if short_exchange not in supported_exchanges:
        logger.error(f"Неподдерживаемая биржа для Short: {short_exchange}. Поддерживаются: {supported_exchanges}")
        return None
    
    if long_exchange == short_exchange:
        logger.error(f"Long и Short позиции не могут быть на одной бирже: {long_exchange}")
        return None
    
    return {
        "coin": coin,
        "long_exchange": long_exchange,
        "short_exchange": short_exchange,
        "notional_usdt": notional_usdt
    }



