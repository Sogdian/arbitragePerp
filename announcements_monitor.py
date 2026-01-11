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
from urllib.parse import urlsplit
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
import os

import httpx
from bs4 import BeautifulSoup

from news_monitor import NewsMonitor

logger = logging.getLogger(__name__)


class AnnouncementsMonitor:
    """
    Поиск риск-новостей (security/hack) по монете в announcements указанных бирж.
    """

    def __init__(self, news_monitor: Optional[NewsMonitor] = None):
        # Переиспользуем существующий NewsMonitor (там уже много фиксов: дедуп, сортировка, догруз статей и т.д.)
        self.news_monitor = news_monitor or NewsMonitor()
        self._binance_cookie = (os.getenv("BINANCE_COOKIE") or "").strip()
        self._warned_binance_waf = False

    def _extra_headers_for_url(self, url: str) -> Optional[Dict[str, str]]:
        try:
            netloc = urlsplit(url).netloc.lower()
        except Exception:
            netloc = ""
        if self._binance_cookie and netloc.endswith("binance.com"):
            return {"Cookie": self._binance_cookie}
        return None

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
            "RISK WARNING",
            "DYOR",
            "PROTOCOL",
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
            "ПРЕДУПРЕЖДЕНИЕ О РИСК",
            "ИНЦИДЕНТ БЕЗОПАСНОСТ",
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

        # Догруз full-article контента (как в делистингах): помогает, когда на листинге нет текста,
        # но в самой статье есть coin/security-индикаторы.
        timeout = httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=5.0)
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        fetch_cache: Dict[str, Optional[str]] = {}  # URL(normalized) -> body_full or None (sentinel)
        fetch_limit = 20

        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as client:
            fetch_count = 0
            for it in news:
                # Ранний skip по дате, если она "твёрдая" (не inferred)
                if lookback is not None and not it.get("published_at_inferred", False):
                    dt = it.get("published_at")
                    if isinstance(dt, datetime):
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        if dt <= lookback:
                            continue

                title = str(it.get("title") or "")
                body = str(it.get("body") or "")
                title_body_upper = (title + " " + body).upper()

                coin_mentioned = pat.search(title_body_upper) is not None
                has_security = self._has_security_keywords(title_body_upper)

                # Если дата inferred и нет ни coin, ни security — не тратим запросы
                if it.get("published_at_inferred", False) and (not coin_mentioned) and (not has_security):
                    continue

                # Условный догруз: если есть coin ИЛИ есть security, но второй сигнал отсутствует
                should_fetch = (coin_mentioned and not has_security) or (has_security and not coin_mentioned)
                if should_fetch and fetch_count < fetch_limit:
                    article_url = str(it.get("url") or "").strip()
                    if article_url.startswith("http"):
                        url_norm = self.news_monitor._normalize_url(article_url)
                        if url_norm in fetch_cache:
                            cached = fetch_cache[url_norm]
                            if cached is not None:
                                title_body_upper = (title + " " + cached).upper()
                                coin_mentioned = pat.search(title_body_upper) is not None
                                has_security = self._has_security_keywords(title_body_upper)
                        else:
                            try:
                                r = await client.get(article_url, headers=self._extra_headers_for_url(article_url))
                                fetch_count += 1

                                # fallback: если original 4xx/5xx — пробуем normalized (без query/fragment)
                                if (
                                    r.status_code >= 400
                                    and fetch_count < fetch_limit
                                    and url_norm != article_url
                                ):
                                    r = await client.get(url_norm, headers=self._extra_headers_for_url(url_norm))
                                    fetch_count += 1

                                # Binance может отдать AWS WAF challenge (часто 202) — логируем 1 раз.
                                if (not self._warned_binance_waf) and r.status_code in (202, 403):
                                    txt = (r.text or "")[:3000]
                                    if "awsWafCookieDomainList" in txt or "gokuProps" in txt or r.status_code == 403:
                                        # эвристика: если нет cookie, почти наверняка это WAF
                                        if not self._binance_cookie:
                                            self._warned_binance_waf = True
                                            logger.warning(
                                                "Binance/Square недоступны из-за AWS WAF (status=%s). "
                                                "Security новости по Binance могут быть неполными. "
                                                "Чтобы включить, добавьте BINANCE_COOKIE в .env (cookie из браузера).",
                                                r.status_code,
                                            )

                                if r.status_code == 200 and r.text:
                                    soup = BeautifulSoup(r.text, "html.parser")
                                    main = (
                                        soup.find("main")
                                        or soup.find("article")
                                        or soup.find("div", class_=re.compile(r"content|article|body|post", re.I))
                                    )
                                    # Для security лучше брать максимум текста: risk-warning может быть вне main/article.
                                    body_full = None
                                    if main is not None:
                                        body_full = main.get_text(" ", strip=True)[:8000]
                                    if not body_full:
                                        body_full = soup.get_text(" ", strip=True)[:8000]
                                    fetch_cache[url_norm] = body_full if body_full else None

                                    if body_full:
                                        title_body_upper = (title + " " + body_full).upper()
                                        coin_mentioned = pat.search(title_body_upper) is not None
                                        has_security = self._has_security_keywords(title_body_upper)
                                else:
                                    fetch_cache[url_norm] = None
                            except Exception as e:
                                logger.debug(f"AnnouncementsMonitor: не удалось догрузить статью {article_url}: {e}")
                                fetch_cache[url_norm] = None

                # Финальная фильтрация (после возможного догруза)
                if not coin_mentioned:
                    continue
                if not has_security:
                    continue

                # финальная (строгая) фильтрация по дате, если она есть
                if lookback is not None:
                    dt2 = it.get("published_at")
                    if isinstance(dt2, datetime):
                        if dt2.tzinfo is None:
                            dt2 = dt2.replace(tzinfo=timezone.utc)
                        if dt2 <= lookback:
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


