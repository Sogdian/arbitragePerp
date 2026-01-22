"""
Парсер вводных данных для бота арбитража фьючерсов
"""
import re
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)


def parse_input(input_text: str) -> Optional[Dict]:
    """
    Парсит вводные данные в формате: "монета Long (биржа), Short (биржа) количество_монет"
    Пример: "CVC Long (bybit), Short (gate) 100"
    Пример: "DASH Long (bybit), Short (gate) 1"
    
    Args:
        input_text: Строка с вводными данными
        
    Returns:
        Словарь с распарсенными данными:
        {
            "coin": str,           # Название монеты (например, "CVC")
            "long_exchange": str,  # Биржа для Long позиции (например, "bybit")
            "short_exchange": str, # Биржа для Short позиции (например, "gate")
            "coin_amount": float   # Количество монет (base), например 1.0 DASH
        }
        или None если ошибка парсинга
    """
    if not input_text:
        logger.error("Пустая строка ввода")
        return None
    
    # Нормализуем строку: убираем лишние пробелы
    normalized = input_text.strip()
    
    # Паттерн для поиска: монета, затем Long (биржа), затем Short (биржа), затем ОБЯЗАТЕЛЬНО количество монет
    # Примеры:
    # "CVC Long (bybit), Short (gate) 100"
    # "BTC Long (bybit), Short (gate) 50"
    # "ETH Long (gate), Short (bybit) 150"
    
    pattern = r'^(\w+)\s+Long\s*\((\w+)\)\s*,\s*Short\s*\((\w+)\)\s+(\d+(?:\.\d+)?)$'
    match = re.match(pattern, normalized, re.IGNORECASE)
    
    if not match:
        logger.error(f"Неверный формат ввода: {input_text}. Ожидается: 'монета Long (биржа), Short (биржа) количество_монет'")
        return None
    
    coin = match.group(1).upper()
    long_exchange = match.group(2).lower()
    short_exchange = match.group(3).lower()
    
    # Извлекаем количество монет (обязательно)
    try:
        coin_amount = float(match.group(4))
        if coin_amount <= 0:
            logger.error(f"Количество монет должно быть положительным числом, получено: {coin_amount}")
            return None
    except (ValueError, IndexError):
        logger.error(f"Не удалось распарсить количество монет из: {input_text}")
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
        "coin_amount": coin_amount
    }



