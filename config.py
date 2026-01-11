"""
Конфигурационный файл для бота арбитража фьючерсов
"""
import os
from dotenv import load_dotenv

# Определяем режим работы (test или prod)
ENV_MODE = os.getenv('ENV', 'test').lower()  # По умолчанию test

# Загружаем переменные окружения из .env файла
load_dotenv('.env', override=False)

# Настройки логирования
LOG_FILE = os.getenv('LOG_FILE', 'arbitrage_perp_bot.log')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()

# ========== Telegram настройки ==========
# Токен бота (получить у @BotFather)
BOT_TOKEN = os.getenv('BOT_TOKEN', '')

# ID тестового канала (для отладки)
TEST_CHANNEL_ID = os.getenv('TEST_CHANNEL_ID', '')

# ID публичного канала (для продакшена)
FREE_CHANNEL_ID = os.getenv('FREE_CHANNEL_ID', '')

# Минимальный спред для отправки в Telegram (в процентах)
TELEGRAM_MIN_SPREAD = float(os.getenv('TELEGRAM_MIN_SPREAD', 2.0))

# Включить отправку в Telegram (true/false)
# Читаем значение и нормализуем: 'true', 'True', 'TRUE', '1' -> True, остальное -> False
enable_telegram_raw = os.getenv('ENABLE_TELEGRAM', 'true').strip().lower()
ENABLE_TELEGRAM = enable_telegram_raw in ('true', '1', 'yes', 'on')

# Настройки антидублирования для Telegram
# Количество итераций между отправками одинакового спреда
# Схема: отправляем на 1-й раз, пропускаем N раз, затем снова отправляем
# Например, TELEGRAM_REPEAT_INTERVAL=3 означает: отправляем на 1-й и 4-й раз (пропускаем 2-й и 3-й)
TELEGRAM_REPEAT_INTERVAL = int(os.getenv('TELEGRAM_REPEAT_INTERVAL', 3))



