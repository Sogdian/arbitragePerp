"""
Асинхронный базовый класс для бирж.
Использует httpx.AsyncClient, должен наследоваться всеми async-* биржами.
"""
from abc import ABC, abstractmethod
from typing import Dict, Optional
import httpx
import asyncio
import logging

logger = logging.getLogger(__name__)


class AsyncBaseExchange(ABC):
    """Базовый класс для всех асинхронных бирж"""

    BASE_URL: str = ""

    def __init__(self, name: str, pool_limit: int = 100):
        self.name = name
        # Один AsyncClient на биржу – повторно используем TCP-соединения
        self.client = httpx.AsyncClient(
            base_url=self.BASE_URL,
            limits=httpx.Limits(max_connections=pool_limit, max_keepalive_connections=pool_limit),
            timeout=httpx.Timeout(5.0, connect=3.0)
        )

    async def close(self):
        """Закрывает HTTP клиент"""
        await self.client.aclose()

    async def _request_json(self, method: str, url: str, *, params: Optional[dict] = None) -> Optional[dict]:
        """Обертка с обработкой ошибок и логированием"""
        try:
            resp = await self.client.request(method, url, params=params)
            # Для LBank логируем только успешные ответы
            if self.name == "LBank" and resp.status_code == 200:
                logger.debug(f"{self.name}: HTTP {resp.status_code} для {url} с params {params}")
            
            resp.raise_for_status()
            result = resp.json()
            # Логируем для LBank для отладки
            if self.name == "LBank":
                logger.info(f"{self.name}: Успешный JSON ответ от {url}: {str(result)[:500]}")
            return result
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            # Для LBank и 404 ошибок - это означает, что публичный API недоступен
            if self.name == "LBank" and status == 404:
                # Не логируем каждую 404 ошибку отдельно, чтобы не засорять лог
                # Основное сообщение будет в методах get_futures_ticker/get_funding_rate
                pass
            else:
                try:
                    error_body = e.response.text[:200]
                    logger.debug(f"{self.name}: HTTP {status} для {url} с params {params}: {error_body}")
                except:
                    logger.debug(f"{self.name}: HTTP {status} для {url} с params {params}")
        except (httpx.RequestError, asyncio.TimeoutError) as e:
            logger.warning(f"{self.name}: Ошибка соединения для {url} с params {params}: {e}")
        except Exception as e:
            logger.warning(f"{self.name}: Неожиданная ошибка для {url} с params {params}: {e}")
        return None

