"""
Мониторинг новостей о делистинге монет с бирж
Поддерживает: Bybit, Gate, MEXC, XT, Binance, Bitget, OKX, BingX
"""
import asyncio
import httpx
from typing import List, Dict, Optional, Union
from datetime import datetime, timedelta, timezone
import logging
import re
from bs4 import BeautifulSoup
from urllib.parse import urlsplit, urlunsplit, urlparse, urljoin

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
            # XT.com: HTML-скрапинг объявлений
            "XT": [
                "https://xtsupport.zendesk.com/hc/en-us/sections/360000106872-Announcements",
                "https://www.xt.com/en/support/articles/announcements",
            ],
            # Binance: HTML-скрапинг объявлений
            "Binance": [
                "https://www.binance.com/en/support/announcement",
                "https://www.binance.com/en/support/announcement/c-48",
            ],
            # Bitget: HTML-скрапинг объявлений
            "Bitget": [
                "https://www.bitgetapp.com/support/articles",
                "https://www.bitgetapp.com/support/articles/category/delisting",
            ],
            # OKX: HTML-скрапинг объявлений
            "OKX": [
                "https://www.okx.com/support/hc/en-us/sections/360000030652-Latest-Announcements",
                "https://www.okx.com/support/hc/en-us/categories/115000275432-Announcements",
            ],
            # BingX: HTML-скрапинг объявлений
            "BingX": [
                "https://support.bingx.com/hc/en-us/sections/360000197872-Announcements",
                "https://support.bingx.com/hc/en-us/categories/360000197872-Announcements",
            ],
        }
        
        # Маппинг названий бирж из запроса на названия в системе
        self.exchange_name_mapping = {
            "bybit": "Bybit",
            "gate": "Gate",
            "mexc": "MEXC",
            "xt": "XT",
            "binance": "Binance",
            "bitget": "Bitget",
            "okx": "OKX",
            "bingx": "BingX",
        }
    
    @staticmethod
    def _dedupe_by_url(items: List[Dict]) -> List[Dict]:
        """Дедупликация новостей по URL (с сохранением порядка)."""
        out: List[Dict] = []
        seen = set()
        for it in items:
            url = (it.get("url") or "").strip()
            key = NewsMonitor._normalize_url(url) if url else None
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            out.append(it)
        return out
    
    @staticmethod
    def _normalize_url(url: str) -> str:
        """Убираем querystring и fragment (utm_*, #hash и т.п.) чтобы дедупликация/сравнение работали стабильно."""
        if not url:
            return url
        parts = urlsplit(url)
        # Убираем query и fragment, нормализуем trailing slash
        path = parts.path.rstrip("/") or "/"
        return urlunsplit((parts.scheme, parts.netloc, path, "", ""))
    
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
    
    async def _fetch_exchange_announcements(self, limit: int = 100, days_back: int = 60, exchanges: Optional[List[str]] = None) -> List[Dict]:
        """
        Получает объявления с бирж
        
        Args:
            limit: Максимальное количество новостей
            days_back: Количество дней назад для поиска
            exchanges: Список бирж для проверки (например, ["bybit", "gate"]). Если None, проверяются все биржи.
            
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
        
        # Фильтруем биржи, если указаны
        exchanges_to_check = self.exchange_announcement_urls
        if exchanges:
            # Преобразуем названия бирж из запроса в названия в системе
            mapped_exchanges = [self.exchange_name_mapping.get(ex.lower(), ex.capitalize()) for ex in exchanges]
            exchanges_to_check = {name: url for name, url in self.exchange_announcement_urls.items() if name in mapped_exchanges}
        
        async def _fetch_one(exchange_name: str, base_url: Optional[Union[str, List[str]]]) -> List[Dict]:
            local: List[Dict] = []
            try:
                # Bybit — официальный JSON API
                if exchange_name == "Bybit":
                    result = await self._fetch_bybit_announcements(
                        limit=min(limit, 200),
                        days_back=days_back,
                        ann_type=None,
                        tag=None,
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
                
                # Паттерны для отсеивания мусорных ссылок (категории, секции, поиск и т.п.)
                # Применяем только к path, не к полному URL
                deny_patterns = [
                    r"/categories?/",
                    r"/sections?/",
                    r"/tag/",
                    r"/search",
                    r"/login",
                    r"/register",
                ]
                deny_re = re.compile("|".join(deny_patterns), re.I)
                
                # seen_urls на всю биржу (все категории), чтобы не дублировать работу
                seen_urls = set()
                
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
                            
                            # Жёсткий потолок для обработки статей (производительность)
                            max_articles = min(2000, max(200, limit * 10))
                            for article in articles[:max_articles]:
                                try:
                                    url_elem = article if getattr(article, "name", None) == "a" else article.find("a")
                                    if not url_elem:
                                        continue
                                    href = url_elem.get("href", "") or ""
                                    if not href:
                                        continue
                                    if not href.startswith("http"):
                                        # Используем urljoin для надёжной сборки URL (обрабатывает edge-cases)
                                        href = urljoin(url, href)
                                    href = href.split("?")[0]
                                    href = self._normalize_url(href)
                                    
                                    # Фильтруем мусорные ссылки (категории, секции, поиск и т.п.)
                                    # Применяем deny только к path, чтобы не выкинуть валидные /support/... или /help/...
                                    parsed = urlparse(href)
                                    if deny_re.search(parsed.path):
                                        continue
                                    
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
                                    
                                    # Пытаемся извлечь дату публикации из статьи на странице списка
                                    published_at = None
                                    # Пробуем найти дату в time элементе рядом с article
                                    time_elem = article.find("time")
                                    if time_elem:
                                        datetime_attr = time_elem.get("datetime")
                                        if datetime_attr:
                                            try:
                                                # Парсим ISO формат: 2024-01-15T10:30:00Z или 2024-01-15T10:30:00+00:00
                                                if "T" in datetime_attr:
                                                    published_at = datetime.fromisoformat(datetime_attr.replace("Z", "+00:00"))
                                                else:
                                                    published_at = datetime.strptime(datetime_attr, "%Y-%m-%d")
                                                    published_at = published_at.replace(tzinfo=timezone.utc)
                                            except Exception:
                                                pass
                                    
                                    # Если не нашли в time, ищем в тексте рядом (многие биржи показывают дату в span/div)
                                    if published_at is None:
                                        date_elem = article.find(["span", "div", "p"], class_=re.compile(r"date|time|published|created", re.I))
                                        if date_elem:
                                            date_text = date_elem.get_text(strip=True)
                                            # Пробуем распарсить различные форматы дат
                                            for fmt in ["%Y-%m-%d", "%d.%m.%Y", "%m/%d/%Y", "%Y/%m/%d"]:
                                                try:
                                                    published_at = datetime.strptime(date_text[:10], fmt)
                                                    published_at = published_at.replace(tzinfo=timezone.utc)
                                                    break
                                                except Exception:
                                                    continue
                                    
                                    # Нормализуем дату к UTC
                                    published_at_inferred = False
                                    if published_at is not None:
                                        if published_at.tzinfo is not None:
                                            published_at = published_at.astimezone(timezone.utc)
                                        else:
                                            # Если дата без timezone, считаем что UTC
                                            published_at = published_at.replace(tzinfo=timezone.utc)
                                    else:
                                        # нет даты => оставляем, иначе days_back не работает на биржах без дат в листинге
                                        # ставим now_utc чтобы элемент прошёл фильтр, дату попробуем получить при догрузе
                                        published_at = datetime.now(timezone.utc)
                                        published_at_inferred = True
                                    
                                    # Фильтруем по lookback сразу (оптимизация)
                                    # Но пропускаем фильтр для inferred дат, чтобы не потерять новости без даты
                                    if not published_at_inferred and published_at <= lookback:
                                        continue
                                    
                                    local.append(
                                        {
                                            "title": title,
                                            "body": body,
                                            "url": href,
                                            "source": exchange_name,
                                            "published_at": published_at,
                                            "published_at_inferred": published_at_inferred,
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
                # Дедуплицируем по URL перед возвратом
                local = self._dedupe_by_url(local)
                return local[:limit]
            except Exception as e:
                logger.warning("❌ %s: ошибка загрузки announcements: %s", exchange_name, e)
                return []
        
        tasks = [_fetch_one(name, url) for name, url in exchanges_to_check.items() if url is not None]
        chunks = await asyncio.gather(*tasks, return_exceptions=False)
        for chunk in chunks:
            all_news.extend(chunk)
        
        # Фильтруем по времени
        all_news = [n for n in all_news if n["published_at"] > lookback]
        
        return all_news[:limit]
    
    async def find_delisting_news(self, news: List[Dict], coin_symbol: str) -> List[Dict]:
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
        
        # Ключевые слова делистинга: разделяем на hard (реальный делистинг) и soft (временная пауза)
        hard_delisting_keywords = [
            "delist", "delisting", "removal", "removed", "discontinued", "terminated",
            "will be delisted", "to be delisted", "delisting announcement",
            "removal from trading", "cease trading", "termination",
            "удаление", "делистинг", "прекращение торговли", "удаление с биржи",
            "прекращение листинга", "исключение из торговли"
        ]
        # soft_keywords (suspend/halt/pause) - временные паузы, не считаем делистингом
        # Используем только hard-набор для поиска делистинга
        delisting_keywords = hard_delisting_keywords
        
        # Компилируем регулярное выражение для поиска монеты
        # Фьючерсы только к USDT, поэтому ищем OBOL и OBOLUSDT
        coin_pattern = re.compile(
            rf"(?<![A-Z0-9]){re.escape(coin_upper)}(?:USDT)?(?![A-Z0-9])",
            re.IGNORECASE
        )
        
        # Условный догруз статей: догружаем если монета упомянута или есть delist-ключи в карточке
        timeout = httpx.Timeout(connect=5.0, read=8.0, write=8.0, pool=5.0)
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        
        # Защитные меры для догруза: лимит, кеш (хранит body или None если не удалось извлечь)
        fetch_cache: Dict[str, Optional[str]] = {}  # кеш по URL: body или None (sentinel для "не удалось извлечь")
        fetch_limit = 20  # догружать максимум 20 статей на монету
        
        # Один клиент на всю функцию для переиспользования соединений
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as client:
            fetch_count = 0
            for article in news:
                title_body = (article.get("title", "") + " " + article.get("body", "")).upper()
                tags_upper = [str(t).upper() for t in article.get("tags", [])]
                
                # Проверяем упоминание монеты (с учетом суффикса USDT, так как фьючерсы только к USDT)
                # Находит OBOL как отдельное слово, и OBOLUSDT
                coin_mentioned = coin_pattern.search(title_body) is not None
                
                # Проверяем наличие ключевых слов о делистинге в карточке
                has_delisting_keywords_in_card = any(keyword.upper() in title_body for keyword in delisting_keywords)
                
                # Условный догруз: если монета упомянута ИЛИ есть delist-ключи в карточке (даже без монеты)
                # Это позволяет находить "batch delisting" новости, где монета только внутри статьи
                should_fetch = (coin_mentioned and not has_delisting_keywords_in_card) or (has_delisting_keywords_in_card and not coin_mentioned)
                
                if should_fetch and fetch_count < fetch_limit:
                    article_url = article.get("url", "")
                    if article_url and article_url.startswith("http"):
                        # Нормализуем URL для кеша (как в дедупликаторе)
                        article_url_normalized = self._normalize_url(article_url)
                        
                        # Проверяем кеш
                        if article_url_normalized in fetch_cache:
                            cached_body = fetch_cache[article_url_normalized]
                            if cached_body is not None:  # None означает "не удалось извлечь", не повторяем запрос
                                title_body = (article.get("title", "") + " " + cached_body).upper()
                                # Пересчитываем coin_mentioned после догруза
                                coin_mentioned = coin_pattern.search(title_body) is not None
                        else:
                            try:
                                r = await client.get(article_url)
                                fetch_count += 1  # Инкрементируем после любого запроса, независимо от результата
                                
                                if r.status_code == 200:
                                    soup_article = BeautifulSoup(r.text, "html.parser")
                                    
                                    # Пытаемся извлечь дату из статьи (для улучшения days_back фильтрации)
                                    published_at_updated = None
                                    time_elem = soup_article.find("time")
                                    if time_elem:
                                        datetime_attr = time_elem.get("datetime")
                                        if datetime_attr:
                                            try:
                                                if "T" in datetime_attr:
                                                    published_at_updated = datetime.fromisoformat(datetime_attr.replace("Z", "+00:00"))
                                                else:
                                                    published_at_updated = datetime.strptime(datetime_attr, "%Y-%m-%d")
                                                    published_at_updated = published_at_updated.replace(tzinfo=timezone.utc)
                                            except Exception:
                                                pass
                                    
                                    # Если не нашли в time, пробуем meta теги
                                    if published_at_updated is None:
                                        meta_published = soup_article.find("meta", property="article:published_time") or soup_article.find("meta", attrs={"name": "article:published_time"})
                                        if meta_published:
                                            content = meta_published.get("content", "")
                                            if content:
                                                try:
                                                    published_at_updated = datetime.fromisoformat(content.replace("Z", "+00:00"))
                                                except Exception:
                                                    pass
                                    
                                    # Обновляем published_at в статье, если нашли дату
                                    if published_at_updated is not None:
                                        if published_at_updated.tzinfo is not None:
                                            published_at_updated = published_at_updated.astimezone(timezone.utc)
                                        else:
                                            published_at_updated = published_at_updated.replace(tzinfo=timezone.utc)
                                        article["published_at"] = published_at_updated
                                        article["published_at_inferred"] = False
                                    
                                    # Извлекаем полный текст статьи
                                    main_content = soup_article.find("main") or soup_article.find("article") or soup_article.find("div", class_=re.compile(r"content|article|body", re.I))
                                    if main_content:
                                        body_full = main_content.get_text(strip=True)[:2000]  # ограничиваем размер
                                        # Сохраняем в кеш
                                        fetch_cache[article_url_normalized] = body_full
                                        # НЕ мутируем article, используем локально для проверки
                                        title_body = (article.get("title", "") + " " + body_full).upper()
                                        # Пересчитываем coin_mentioned после догруза
                                        coin_mentioned = coin_pattern.search(title_body) is not None
                                    else:
                                        # Сохраняем None как sentinel - контент не найден, не повторяем запрос
                                        fetch_cache[article_url_normalized] = None
                            except Exception as e:
                                logger.debug(f"Не удалось догрузить статью {article_url}: {e}")
                                # Сохраняем None в кеш, чтобы не повторять запрос
                                fetch_cache[article_url_normalized] = None
                
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
    
    async def check_delisting(self, coin_symbol: str, exchanges: Optional[List[str]] = None, days_back: int = 60) -> List[Dict]:
        """
        Проверяет наличие новостей о делистинге монеты за последние N дней
        
        Args:
            coin_symbol: Символ монеты (например, "DGRAM", "IOTA")
            exchanges: Список бирж для проверки (например, ["bybit", "gate"]). Если None, проверяются все биржи. Если [], проверка не выполняется.
            days_back: Количество дней назад для поиска (по умолчанию 60)
            
        Returns:
            Список новостей о делистинге
        """
        # [] => явно ничего не проверять
        if exchanges == []:
            return []
        
        # Получаем объявления с бирж (None => все биржи)
        all_announcements = await self._fetch_exchange_announcements(limit=200, days_back=days_back, exchanges=exchanges)
        
        # Ищем новости о делистинге (теперь async для условного догруза статей)
        delisting_news = await self.find_delisting_news(all_announcements, coin_symbol)
        
        return delisting_news

