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
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            logger.debug(f"{self.name}: HTTP {status} для {url}")
        except (httpx.RequestError, asyncio.TimeoutError) as e:
            logger.debug(f"{self.name}: Ошибка соединения для {url}: {e}")
        return None

