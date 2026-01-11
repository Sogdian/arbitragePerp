"""
Мониторинг объявлений бирж по монете: взломы, проблемы с безопасностью и другие критические инциденты.

Этот модуль сделан для переиспользования подхода из проекта `E:\\Work\\arbitrage` (ai_system/news_monitor.py),
но адаптирован под текущий `arbitragePerp`:
- анализируем только монету и биржи из входного запроса (например "REPPO Long (bingx), Short (mexc)")
- используем уже существующий сбор announcements из `news_monitor.NewsMonitor`
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from news_monitor import NewsMonitor

logger = logging.getLogger(__name__)


class AnnouncementsMonitor:
    """
    Поиск риск-новостей (security/hack) по монете в announcements указанных бирж.
    """

    def __init__(self, news_monitor: Optional[NewsMonitor] = None):
        # Переиспользуем существующий NewsMonitor (там уже много фиксов: дедуп, сортировка, догруз статей и т.д.)
        self.news_monitor = news_monitor or NewsMonitor()

    @staticmethod
    def _coin_pattern(coin: str) -> re.Pattern:
        coin_upper = coin.upper()
        # Матчим COIN и COINUSDT (фьючи в проекте USDT-M), не ловим подстроки (FLOW vs FLOWER)
        return re.compile(
            rf"(?<![A-Z0-9]){re.escape(coin_upper)}(?:USDT)?(?![A-Z0-9])",
            re.IGNORECASE,
        )

    @staticmethod
    def _has_security_keywords(text_upper: str) -> bool:
        # Подход и наборы ключей — из логики старого проекта (arbitrage/ai_system/news_monitor.py),
        # но здесь фокус именно на security/hack.
        keywords = [
            # EN
            "SECURITY",
            "HACK",
            "HACKED",
            "EXPLOIT",
            "BREACH",
            "COMPROMISED",
            "UNAUTHORIZED",
            "PHISH",
            "SCAM",
            "MALWARE",
            "ATTACK",
            "VULNERAB",  # vulnerability/vulnerable
            "STOLEN",
            "FUNDS STOLEN",
            "SECURITY INCIDENT",
            "INCIDENT",
            "PRIVATE KEY",
            "KEY LEAK",
            # RU
            "ВЗЛОМ",
            "УЯЗВ",
            "ФИШИНГ",
            "КОМПРОМЕТ",
            "АТАК",
            "УКРАЛ",
            "КРАЖ",
            "УТЕЧК",
            "ВРЕДОНОС",
            "МОШЕННИЧ",
        ]
        return any(k in text_upper for k in keywords)

    async def find_security_news(
        self,
        news: List[Dict[str, Any]],
        coin_symbol: str,
        lookback: Optional[datetime],
    ) -> List[Dict[str, Any]]:
        """
        Возвращает релевантные объявления по безопасности/взломам для конкретной монеты.
        """
        pat = self._coin_pattern(coin_symbol)
        out: List[Dict[str, Any]] = []

        for it in news:
            title = str(it.get("title") or "")
            body = str(it.get("body") or "")
            title_body_upper = (title + " " + body).upper()

            # строгая фильтрация по монете
            if pat.search(title_body_upper) is None:
                continue

            if not self._has_security_keywords(title_body_upper):
                continue

            # финальная (строгая) фильтрация по дате, если она есть
            if lookback is not None:
                dt = it.get("published_at")
                if isinstance(dt, datetime):
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    if dt <= lookback:
                        continue

            tagged = it.copy()
            tags = list(tagged.get("tags", []) or [])
            if "security" not in tags:
                tags.append("security")
            tagged["tags"] = tags
            out.append(tagged)

        # дедуп по url как в основном NewsMonitor
        out = self.news_monitor._dedupe_by_url(out)
        return out

    async def check_security_for_coin(
        self,
        coin_symbol: str,
        exchanges: Optional[List[str]],
        days_back: int = 60,
    ) -> List[Dict[str, Any]]:
        """
        Проверяет announcements выбранных бирж на наличие security/hack новостей по монете.
        """
        if exchanges == []:
            return []
        if not exchanges:
            return []

        try:
            # Вытаскиваем только announcements нужных бирж (внутри NewsMonitor есть фильтрация по exchanges)
            anns = await self.news_monitor._fetch_exchange_announcements(
                limit=200,
                days_back=days_back,
                exchanges=exchanges,
            )
            now_utc = datetime.now(timezone.utc)
            lookback = now_utc - timedelta(days=days_back, hours=6) if days_back > 0 else None
            return await self.find_security_news(anns, coin_symbol, lookback=lookback)
        except Exception as e:
            logger.warning(f"AnnouncementsMonitor: ошибка при проверке security news для {coin_symbol}: {e}")
            return []


