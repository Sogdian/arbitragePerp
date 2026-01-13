"""
Модуль для отправки сообщений в Telegram каналы
"""
import asyncio
import logging
import httpx
import time
from typing import List, Dict
import config

logger = logging.getLogger(__name__)

# Глобальный трекер для отслеживания спредов между вызовами
# Ключ: f"{coin}_{long_exchange}_{short_exchange}"
# Значение: {"spread": float, "count": int, "last_seen": float}
_global_spread_tracker: Dict[str, Dict] = {}


class TelegramSender:
    """Класс для отправки сообщений в Telegram каналы"""
    
    BASE_URL = "https://api.telegram.org/bot"
    
    def __init__(self):
        self.bot_token = config.BOT_TOKEN
        self.test_channel_id = config.TEST_CHANNEL_ID
        self.free_channel_id = config.FREE_CHANNEL_ID
        self.enabled = config.ENABLE_TELEGRAM and bool(self.bot_token)
        
        # Детальное логирование для диагностики
        if not config.ENABLE_TELEGRAM:
            logger.warning(f"Telegram интеграция отключена: ENABLE_TELEGRAM={config.ENABLE_TELEGRAM} (значение из config)")
        elif not self.bot_token:
            logger.warning(f"Telegram интеграция отключена: BOT_TOKEN не установлен (длина: {len(self.bot_token) if self.bot_token else 0})")
    
    def _get_channel_id(self) -> str:
        """Возвращает ID канала в зависимости от режима (test/prod)"""
        if config.ENV_MODE == 'test':
            return self.test_channel_id
        else:
            return self.free_channel_id
    
    async def _send_message(self, text: str, channel_id: str = None) -> bool:
        """Отправляет сообщение в канал"""
        if not self.enabled:
            return False
        
        if not channel_id:
            channel_id = self._get_channel_id()
        
        if not channel_id:
            logger.error("❌ Channel ID не установлен!")
            return False
        
        url = f"{self.BASE_URL}{self.bot_token}/sendMessage"
        data = {
            "chat_id": channel_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, json=data)
                response.raise_for_status()
                result = response.json()
                
                if result.get("ok"):
                    return True
                else:
                    logger.error(f"❌ Ошибка отправки: {result.get('description', 'Unknown')}")
                    return False
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке в Telegram: {e}")
            return False
    
    async def send_message(self, text: str, channel_id: str = None) -> bool:
        """
        Публичный метод для отправки произвольного сообщения
        
        Args:
            text: Текст сообщения
            channel_id: ID канала (опционально, если не указан - используется канал из конфига)
        
        Returns:
            True если сообщение отправлено успешно
        """
        return await self._send_message(text, channel_id)
    
    def _get_spread_key(self, coin: str, long_exchange: str, short_exchange: str) -> str:
        """Создает уникальный ключ для спреда (coin + биржи)"""
        return f"{coin}_{long_exchange}_{short_exchange}"
    
    def _should_send_spread(self, coin: str, long_exchange: str, short_exchange: str, spread: float) -> bool:
        """
        Определяет, нужно ли отправлять спред в Telegram.
        Логика:
        - 1-е обнаружение (новый спред) - отправляем
        - 2-е и 3-е подряд одинаковое - не отправляем
        - 4-е подряд одинаковое - отправляем
        """
        global _global_spread_tracker
        
        key = self._get_spread_key(coin, long_exchange, short_exchange)
        spread_rounded = round(spread, 2)  # Округляем для сравнения
        
        current_time = time.time()
        
        # Очищаем старые ключи (не встречались более 24 часов) для предотвращения утечки памяти
        max_age = 24 * 3600  # 24 часа
        keys_to_remove = [
            k for k, v in _global_spread_tracker.items()
            if current_time - v.get("last_seen", 0) > max_age
        ]
        for k in keys_to_remove:
            del _global_spread_tracker[k]
        
        # Если это новый спред или спред изменился
        if key not in _global_spread_tracker:
            # Новый спред - отправляем, счетчик = 1
            _global_spread_tracker[key] = {"spread": spread_rounded, "count": 1, "last_seen": current_time}
            logger.debug(f"Новый спред {key}: {spread_rounded}% (count=1) - отправляем")
            return True
        
        tracker = _global_spread_tracker[key]
        last_spread = tracker["spread"]
        
        # Обновляем время последнего обнаружения
        tracker["last_seen"] = current_time
        
        # Если спред изменился - это новый спред
        if spread_rounded != last_spread:
            # Обновляем трекер для нового спреда
            _global_spread_tracker[key] = {"spread": spread_rounded, "count": 1, "last_seen": current_time}
            logger.debug(f"Спред изменился {key}: {last_spread}% -> {spread_rounded}% (count=1) - отправляем")
            return True
        
        # Спред такой же - увеличиваем счетчик
        tracker["count"] += 1
        count = tracker["count"]
        
        # Получаем настройку интервала повторений из config
        repeat_interval = config.TELEGRAM_REPEAT_INTERVAL
        # Отправляем на 1-й раз и каждый (repeat_interval + 1)-й раз
        # Например, если repeat_interval=3: отправляем на 1, 4, 7, 10...
        if count == 1:
            logger.debug(f"Спред {key}: {spread_rounded}% (count=1) - отправляем")
            return True
        elif count <= repeat_interval:
            logger.debug(f"Спред {key}: {spread_rounded}% (count={count}) - НЕ отправляем (пропускаем до {repeat_interval})")
            return False
        elif count == repeat_interval + 1:
            # Сбрасываем счетчик, чтобы следующий раз снова был count = 1
            tracker["count"] = 1  # Сбрасываем в 1, так как на следующей итерации он увеличится до 2
            logger.debug(f"Спред {key}: {spread_rounded}% (count={count}) - отправляем, сброс счетчика")
            return True
        
        # Если count > (repeat_interval + 1), это не должно происходить, но на всякий случай сбрасываем
        if count > repeat_interval + 1:
            tracker["count"] = 1
            logger.warning(f"Спред {key}: неожиданное значение count={count}, сброс в 1")
            return True
        
        # Этот код недостижим, но оставлен для безопасности
        return False

