"""
Мониторинг новостей о делистинге монет с бирж
Поддерживает: Bybit, Gate, MEXC, LBank
"""
import asyncio
import httpx
from typing import List, Dict, Optional, Union
from datetime import datetime, timedelta, timezone
import logging
import re
from bs4 import BeautifulSoup
from urllib.parse import urlsplit, urlunsplit

logger = logging.getLogger(__name__)


class NewsMonitor:
    """Класс для мониторинга и получения новостей о делистинге монет"""
    
    def __init__(self):
        # URL страниц поддержки/объявлений бирж
        # Для бирж с API (Bybit) используем API. Для остальных - HTML-скрапинг.
        self.exchange_announcement_urls = {
            # Bybit: используем официальный API announcements
            # Документация: https://bybit-exchange.github.io/docs/v5/announcement
            "Bybit": "https://api.bybit.com/v5/announcements/index",
            # MEXC: HTML-скрапинг нескольких категорий объявлений
            "MEXC": [
                "https://www.mexc.com/ru-RU/announcements/help-faq/deposits-withdrawals-36",
                "https://www.mexc.com/ru-RU/announcements/delistings",
                "https://www.mexc.com/ru-RU/announcements/tag/deposits-withdrawals-36",
            ],
            # Gate.io: HTML-скрапинг нескольких категорий объявлений
            "Gate": [
                "https://www.gate.com/ru/announcements/deposit-withdrawal",
                "https://www.gate.com/ru/announcements/delisted",
            ],
        }
    
    @staticmethod
    def _dedupe_by_url(items: List[Dict]) -> List[Dict]:
        """Дедупликация новостей по URL (с сохранением порядка)."""
        out: List[Dict] = []
        seen = set()
        for it in items:
            url = (it.get("url") or "").strip()
            key = url.split("?")[0] if url else None
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            out.append(it)
        return out
    
    @staticmethod
    def _normalize_url(url: str) -> str:
        """Убираем querystring (utm_* и т.п.) чтобы дедупликация/сравнение работали стабильно."""
        if not url:
            return url
        parts = urlsplit(url)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    
    async def _fetch_bybit_announcements(
        self,
        limit: int = 100,
        locale: str = "en-US",
        ann_type: Optional[str] = None,
        tag: Optional[str] = None,
        max_pages: int = 50,
        days_back: int = 60,
    ) -> List[Dict]:
        """
        Bybit official announcements API:
        GET /v5/announcements/index
        
        Документация: https://bybit-exchange.github.io/docs/v5/announcement
        """
        timeout = httpx.Timeout(connect=5.0, read=8.0, write=8.0, pool=5.0)
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        base_url = self.exchange_announcement_urls.get("Bybit")
        if not base_url:
            return []
        
        try:
            out: List[Dict] = []
            # Используем UTC для корректного сравнения дат
            now_utc = datetime.now(timezone.utc)
            # Буфер 6 часов, чтобы не терять события на границе "ровно 30 дней назад"
            cutoff_date = now_utc - timedelta(days=days_back, hours=6)
            stop_early = False
            page_limit = min(50, limit)
            
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as client:
                for page in range(1, max_pages + 1):
                    if stop_early:
                        break
                    params: Dict[str, str] = {
                        "locale": locale,
                        "page": str(page),
                        "limit": str(page_limit),  # Bybit нормально переваривает 50
                    }
                    if ann_type:
                        params["type"] = str(ann_type)
                    if tag:
                        params["tag"] = str(tag)
                    
                    r = await client.get(base_url, params=params)
                    if r.status_code != 200:
                        logger.warning("Bybit announcements API вернул статус %s", r.status_code)
                        break
                    
                    data = r.json()
                    if not isinstance(data, dict) or data.get("retCode") != 0:
                        logger.warning("Bybit announcements API вернул ошибку: %s", data.get("retMsg") if isinstance(data, dict) else "bad_json")
                        break
                    
                    result = data.get("result", {})
                    items = result.get("list", []) or []
                    if not items:
                        break
                    
                    for it in items:
                        try:
                            title = (it.get("title") or "").strip()
                            url = self._normalize_url((it.get("url") or "").strip())
                            description = (it.get("description") or "").strip()
                            
                            if not title or not url:
                                continue
                            
                            # Парсим дату публикации в UTC
                            published_at = now_utc
                            publish_time = it.get("publishTime")
                            if publish_time:
                                try:
                                    published_at = datetime.fromtimestamp(int(str(publish_time)) / 1000, tz=timezone.utc)
                                except Exception:
                                    published_at = now_utc
                            
                            # Фильтруем по дате (если элемент старше cutoff_date, можно останавливаться)
                            if published_at < cutoff_date:
                                # Список отсортирован по времени (новые -> старые), можно останавливаться
                                stop_early = True
                                break
                            
                            # Извлекаем тип и теги
                            ann_type_obj = it.get("type", {})
                            type_key = ann_type_obj.get("key", "") if isinstance(ann_type_obj, dict) else ""
                            type_title = ann_type_obj.get("title", "") if isinstance(ann_type_obj, dict) else ""
                            tags_list = it.get("tags", []) or []
                            
                            out.append({
                                "title": title,
                                "body": description[:1000],
                                "url": url,
                                "source": "Bybit",
                                "published_at": published_at,
                                "tags": ["Bybit", "exchange", "announcement", type_key, type_title] + (tags_list if isinstance(tags_list, list) else []),
                            })
                        except Exception:
                            continue
                    
                    out = self._dedupe_by_url(out)
                    if len(out) >= limit:
                        break
                    
                    # Проверяем, есть ли еще страницы
                    total = result.get("total", 0)
                    if page * page_limit >= total:
                        break
                    
                    # Мягкая задержка
                    await asyncio.sleep(0.05)
            
            out.sort(key=lambda x: x["published_at"], reverse=True)
            out = self._dedupe_by_url(out)[:limit]
            return out
        except Exception as e:
            logger.warning("Bybit announcements API ошибка: %s", e)
            return []
    
    async def _fetch_exchange_announcements(self, limit: int = 100, days_back: int = 60) -> List[Dict]:
        """
        Получает объявления с бирж
        
        Args:
            limit: Максимальное количество новостей
            days_back: Количество дней назад для поиска
            
        Returns:
            Список новостей с бирж
        """
        all_news = []
        # Используем UTC для корректного сравнения дат
        now_utc = datetime.now(timezone.utc)
        # Буфер 6 часов, чтобы не терять события на границе
        lookback = now_utc - timedelta(days=days_back, hours=6)
        
        timeout = httpx.Timeout(connect=5.0, read=8.0, write=8.0, pool=5.0)
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        
        async def _fetch_one(exchange_name: str, base_url: Optional[Union[str, List[str]]]) -> List[Dict]:
            local: List[Dict] = []
            try:
                # Bybit — официальный JSON API
                if exchange_name == "Bybit":
                    result = await self._fetch_bybit_announcements(
                        limit=min(limit, 100),
                        days_back=days_back,
                        ann_type="delistings",
                        tag="Derivatives"
                    )
                    return result
                
                # Для бирж без публичного API и без URL для скрапинга пропускаем
                if base_url is None:
                    logger.debug("⏭️ %s: пропущен (нет публичного REST API для announcements)", exchange_name)
                    return []
                
                # Обрабатываем список URL категорий
                if isinstance(base_url, str):
                    urls_to_fetch = [base_url]
                elif isinstance(base_url, list):
                    urls_to_fetch = base_url
                else:
                    urls_to_fetch = []
                
                if not urls_to_fetch:
                    return []
                
                async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as client:
                    for url in urls_to_fetch:
                        try:
                            r = await client.get(url)
                            if r.status_code != 200:
                                logger.debug("🔍 %s: announcements %s вернул статус %s", exchange_name, url, r.status_code)
                                continue
                            
                            soup = BeautifulSoup(r.text, "html.parser")
                            articles: List = []
                            
                            # Общая логика для других бирж
                            articles.extend(soup.find_all("a", href=re.compile(r"article|announcement|support|help", re.I)))
                            articles.extend(soup.find_all(["article", "div"], class_=re.compile(r"article|announcement|news|support", re.I)))
                
                            seen_urls = set()
                            for article in articles[: max(10, limit * 2)]:
                                try:
                                    url_elem = article if getattr(article, "name", None) == "a" else article.find("a")
                                    if not url_elem:
                                        continue
                                    href = url_elem.get("href", "") or ""
                                    if not href:
                                        continue
                                    if not href.startswith("http"):
                                        if href.startswith("/"):
                                            # Извлекаем базовый домен из текущего URL
                                            url_parts = url.split("/")
                                            base_domain = f"{url_parts[0]}//{url_parts[2]}"
                                            href = base_domain + href
                                        else:
                                            href = url.rstrip("/") + "/" + href.lstrip("/")
                                    href = href.split("?")[0]
                                    if href in seen_urls:
                                        continue
                                    seen_urls.add(href)
                        
                                    title_elem = article.find(["h1", "h2", "h3", "h4", "span", "div", "a"], class_=re.compile(r"title|heading|name", re.I))
                                    if not title_elem:
                                        title_elem = url_elem
                                    title = (title_elem.get_text(strip=True) if title_elem else "").strip()
                                    if not title or len(title) < 5:
                                        continue
                                    
                                    body_elem = article.find(["p", "div", "span"], class_=re.compile(r"content|body|description|text|summary", re.I))
                                    body = body_elem.get_text(strip=True)[:500] if body_elem else ""
                                    
                                    # Используем текущее время как приблизительную дату (UTC)
                                    published_at = datetime.now(timezone.utc)
                                    
                                    local.append(
                                        {
                                            "title": title,
                                            "body": body,
                                            "url": href,
                                            "source": exchange_name,
                                            "published_at": published_at,
                                            "tags": [exchange_name, "exchange", "announcement"],
                                        }
                                    )
                                    if len(local) >= limit:
                                        break
                                except Exception:
                                    continue
                        except Exception as e:
                            logger.debug(f"Ошибка при обработке URL {url} для {exchange_name}: {e}")
                            continue
                
                if local:
                    logger.debug("  ✓ %s: загружено %s объявлений", exchange_name, len(local))
                return local[:limit]
            except Exception as e:
                logger.warning("❌ %s: ошибка загрузки announcements: %s", exchange_name, e)
                return []
        
        tasks = [_fetch_one(name, url) for name, url in self.exchange_announcement_urls.items() if url is not None]
        chunks = await asyncio.gather(*tasks, return_exceptions=False)
        for chunk in chunks:
            all_news.extend(chunk)
        
        # Фильтруем по времени
        all_news = [n for n in all_news if n["published_at"] > lookback]
        
        return all_news[:limit]
    
    def find_delisting_news(self, news: List[Dict], coin_symbol: str) -> List[Dict]:
        """
        Находит новости о делистинге монеты на биржах
        
        Args:
            news: Список новостей
            coin_symbol: Символ монеты (например, "FLOW", "BTC")
            
        Returns:
            Список релевантных новостей о делистинге
        """
        coin_upper = coin_symbol.upper()
        relevant_news = []
        
        # Ключевые слова, указывающие на делистинг
        delisting_keywords = [
            "delist", "delisting", "removal", "removed", "discontinued", "terminated",
            "trading suspended", "trading halt", "will be delisted", "to be delisted",
            "delisting announcement", "removal from trading", "cease trading",
            "удаление", "делистинг", "прекращение торговли", "удаление с биржи",
            "прекращение листинга", "исключение из торговли"
        ]
        
        # Компилируем регулярное выражение для поиска монеты
        # Фьючерсы только к USDT, поэтому ищем OBOL и OBOLUSDT
        coin_pattern = re.compile(
            rf"(?<![A-Z0-9]){re.escape(coin_upper)}(?:USDT)?(?![A-Z0-9])",
            re.IGNORECASE
        )
        
        for article in news:
            title_body = (article.get("title", "") + " " + article.get("body", "")).upper()
            tags_upper = [str(t).upper() for t in article.get("tags", [])]
            
            # Проверяем упоминание монеты (с учетом суффикса USDT, так как фьючерсы только к USDT)
            # Находит OBOL как отдельное слово, и OBOLUSDT
            coin_mentioned = coin_pattern.search(title_body) is not None
            
            # Проверяем наличие ключевых слов о делистинге или явного annType=symbol_delisting
            has_delisting_keywords = any(keyword.upper() in title_body for keyword in delisting_keywords) or ("SYMBOL_DELISTING" in tags_upper)
            
            # Логируем для отладки, если монета упомянута, но делистинг не найден
            if coin_mentioned and not has_delisting_keywords:
                logger.info(f"Монета {coin_symbol} найдена в '{article.get('title', '')[:60]}...', но нет ключевых слов делистинга")
            
            if coin_mentioned and has_delisting_keywords:
                # Добавляем тег о делистинге
                article_with_tag = article.copy()
                if "delisting" not in article_with_tag.get("tags", []):
                    tags = article_with_tag.get("tags", [])
                    tags.append("delisting")
                    article_with_tag["tags"] = tags
                relevant_news.append(article_with_tag)
                # Логируем найденный делистинг с URL
                url = article.get('url', 'N/A')
                logger.warning(f"⚠️ Найден делистинг {coin_symbol}: {article.get('title', '')[:80]}... | URL: {url}")
        
        return relevant_news
    
    async def check_delisting(self, coin_symbol: str, days_back: int = 60) -> List[Dict]:
        """
        Проверяет наличие новостей о делистинге монеты за последние N дней
        
        Args:
            coin_symbol: Символ монеты (например, "DGRAM", "IOTA")
            days_back: Количество дней назад для поиска (по умолчанию 60)
            
        Returns:
            Список новостей о делистинге
        """
        logger.info(f"Проверка делистинга для {coin_symbol} за последние {days_back} дней...")
        
        # Получаем объявления с бирж
        all_announcements = await self._fetch_exchange_announcements(limit=200, days_back=days_back)
        logger.info(f"Получено {len(all_announcements)} объявлений с бирж")
        
        # Ищем новости о делистинге
        delisting_news = self.find_delisting_news(all_announcements, coin_symbol)
        
        if not delisting_news:
            # Логируем для отладки - проверяем все объявления Bybit на наличие монеты
            bybit_announcements = [a for a in all_announcements if a.get("source") == "Bybit"]
            logger.info(f"Делистинг не найден. Проверено {len(bybit_announcements)} объявлений Bybit. Проверяем первые 10 для отладки:")
            coin_pattern_debug = re.compile(
                rf"(?<![A-Z0-9]){re.escape(coin_symbol.upper())}(?:USDT)?(?![A-Z0-9])",
                re.IGNORECASE
            )
            for i, ann in enumerate(bybit_announcements[:10]):
                title = ann.get("title", "")[:100]
                body = ann.get("body", "")[:100]
                title_body = (title + " " + body).upper()
                coin_found = coin_pattern_debug.search(title_body) is not None
                logger.info(f"  {i+1}. [{ann.get('source', 'Unknown')}] {title} | Монета найдена: {coin_found}")
        
        return delisting_news

