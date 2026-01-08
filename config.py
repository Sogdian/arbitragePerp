"""
Конфигурационный файл для бота арбитража фьючерсов
"""
import os
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv('.env', override=False)

# Настройки логирования
LOG_FILE = os.getenv('LOG_FILE', 'arbitrage_perp_bot.log')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()

