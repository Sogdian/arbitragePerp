"""
X (Twitter) news integration (optional).

Purpose:
- Provide additional "delisting" and "security/hack" signals from X that can be used
  alongside exchange announcements when deciding verdicts.

This module is intentionally optional:
- If `X_BEARER_TOKEN` is not set, all methods return [] and do not make network calls.
"""

from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _clamp_recent_search_lookback(lookback: Optional[datetime]) -> Optional[datetime]:
    """
    Twitter/X v2 recent search is typically limited to ~7 days (depending on access tier).
    We clamp lookback to now-7d to avoid useless requests with too-old start_time.
    """
    if lookback is None:
        return None
    if lookback.tzinfo is None:
        lookback = lookback.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    min_dt = now - timedelta(days=7)
    return lookback if lookback > min_dt else min_dt


def _dedupe_by_url(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[str] = set()
    out: List[Dict[str, Any]] = []
    for it in items:
        url = str(it.get("url") or "").strip()
        key = url or (str(it.get("title") or "").strip()[:200])
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


class XNewsMonitor:
    """
    Minimal X(Twitter) recent search client.

    Env:
    - X_BEARER_TOKEN: required to enable
    - X_NEWS_CACHE_TTL_SEC: cache TTL per query (default 180)
    - X_NEWS_MAX_RESULTS: max tweets per query (default 25, capped to [10..100])
    """

    API_URL = "https://api.twitter.com/2/tweets/search/recent"

    def __init__(self) -> None:
        self.bearer_token = (os.getenv("X_BEARER_TOKEN") or "").strip()
        self.cache_ttl_sec = float(os.getenv("X_NEWS_CACHE_TTL_SEC", "180"))
        self.max_results = int(os.getenv("X_NEWS_MAX_RESULTS", "25"))
        self.max_results = max(10, min(100, self.max_results))
        # key -> (expires_monotonic, items)
        self._cache: Dict[Tuple[str, str], Tuple[float, List[Dict[str, Any]]]] = {}

    @property
    def enabled(self) -> bool:
        return bool(self.bearer_token)

    def _coin_query_terms(self, coin: str) -> str:
        c = coin.upper()
        # Prefer more specific patterns to reduce false positives for short tickers (e.g. ALL, ONE, IN).
        # Keep both cashtag and USDT-pair variants.
        return f'(${c} OR {c}USDT OR "{c}/USDT" OR "{c} USDT")'

    def _exchange_query_terms(self, exchanges: Optional[List[str]]) -> str:
        if not exchanges:
            return ""
        # Keep it simple: exchange names are often mentioned in posts about delists/incidents.
        # (If this is too noisy, we can later add X_NEWS_ACCOUNTS / handle filters.)
        ex_terms = [re.sub(r"[^a-z0-9_]+", "", (e or "").lower()) for e in exchanges]
        ex_terms = [e for e in ex_terms if e]
        if not ex_terms:
            return ""
        joined = " OR ".join(ex_terms)
        return f"({joined})"

    async def _search_recent(self, query: str, start_time: Optional[datetime]) -> List[Dict[str, Any]]:
        if not self.enabled:
            return []

        cache_key = (query, _iso(start_time) if start_time else "")
        now_m = time.monotonic()
        cached = self._cache.get(cache_key)
        if cached and cached[0] > now_m:
            return cached[1]

        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        params: Dict[str, Any] = {
            "query": query,
            "max_results": self.max_results,
            "tweet.fields": "created_at",
            "expansions": "author_id",
            "user.fields": "username",
        }
        if start_time is not None:
            params["start_time"] = _iso(start_time)

        timeout = httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=5.0)
        try:
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
                r = await client.get(self.API_URL, headers=headers, params=params)
            if r.status_code != 200:
                # Keep this quiet for scanners; the caller can decide logger level.
                logger.debug(f"X search failed: status={r.status_code} body={(r.text or '')[:250]}")
                items: List[Dict[str, Any]] = []
            else:
                payload = r.json() or {}
                data = payload.get("data") or []
                includes = payload.get("includes") or {}
                users = includes.get("users") or []
                id_to_user: Dict[str, str] = {str(u.get("id")): str(u.get("username") or "") for u in users if u}

                items = []
                for tw in data:
                    tid = str(tw.get("id") or "").strip()
                    text = str(tw.get("text") or "").strip()
                    created_at_raw = tw.get("created_at")
                    created_at = None
                    if isinstance(created_at_raw, str):
                        try:
                            created_at = datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))
                        except Exception:
                            created_at = None
                    author_id = str(tw.get("author_id") or "").strip()
                    username = id_to_user.get(author_id, "")
                    if username and tid:
                        url = f"https://x.com/{username}/status/{tid}"
                    elif tid:
                        url = f"https://x.com/i/web/status/{tid}"
                    else:
                        url = ""

                    title = text.replace("\n", " ").strip()
                    items.append(
                        {
                            "title": title[:280],
                            "url": url,
                            "published_at": created_at,
                            "source": "x",
                        }
                    )

            items = _dedupe_by_url(items)
            self._cache[cache_key] = (now_m + self.cache_ttl_sec, items)
            return items
        except Exception as e:
            logger.debug(f"X search error: {e}", exc_info=True)
            return []

    async def find_delisting_news(
        self,
        coin_symbol: str,
        exchanges: Optional[List[str]],
        lookback: Optional[datetime],
    ) -> List[Dict[str, Any]]:
        """
        Returns X posts that look like delisting / suspension / removal announcements for the coin.
        """
        if not self.enabled:
            return []

        start_time = _clamp_recent_search_lookback(lookback)
        coin_terms = self._coin_query_terms(coin_symbol)
        ex_terms = self._exchange_query_terms(exchanges)

        delist_terms = (
            "(delist OR delisting OR \"will delist\" OR \"to delist\" OR \"remove\" OR "
            "\"trading will be suspended\" OR \"suspend trading\" OR \"terminate\" OR \"remove trading\" OR "
            "\"perpetual\" OR \"futures\" OR \"contract\")"
        )

        parts = [coin_terms, delist_terms]
        if ex_terms:
            parts.append(ex_terms)
        query = " ".join(parts) + " -is:retweet"

        items = await self._search_recent(query=query, start_time=start_time)
        # Tag them like exchange announcements so existing code can treat them the same way.
        out: List[Dict[str, Any]] = []
        for it in items:
            tagged = it.copy()
            tags = list(tagged.get("tags", []) or [])
            if "delisting" not in tags:
                tags.append("delisting")
            if "x" not in tags:
                tags.append("x")
            tagged["tags"] = tags
            out.append(tagged)
        return out

    async def find_security_news(
        self,
        coin_symbol: str,
        exchanges: Optional[List[str]],
        lookback: Optional[datetime],
    ) -> List[Dict[str, Any]]:
        """
        Returns X posts that look like security/hack/exploit news for the coin.
        """
        if not self.enabled:
            return []

        start_time = _clamp_recent_search_lookback(lookback)
        coin_terms = self._coin_query_terms(coin_symbol)
        ex_terms = self._exchange_query_terms(exchanges)

        sec_terms = (
            "(security OR hack OR hacked OR exploit OR exploited OR breach OR compromised OR "
            "vulnerab* OR phishing OR scam OR rug OR \"funds stolen\" OR stolen OR drain OR drained OR attacker OR "
            "взлом OR уязв* OR фишинг OR мошенн* OR украл* OR краж*)"
        )

        parts = [coin_terms, sec_terms]
        if ex_terms:
            parts.append(ex_terms)
        query = " ".join(parts) + " -is:retweet"

        items = await self._search_recent(query=query, start_time=start_time)
        out: List[Dict[str, Any]] = []
        for it in items:
            tagged = it.copy()
            tags = list(tagged.get("tags", []) or [])
            if "security" not in tags:
                tags.append("security")
            if "x" not in tags:
                tags.append("x")
            tagged["tags"] = tags
            out.append(tagged)
        return out


