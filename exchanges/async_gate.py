"""
Асинхронная реализация Gate.io для фьючерсов на базе AsyncBaseExchange
"""
from typing import Dict, Optional, Set, Tuple, Any, List
import logging
from .async_base_exchange import AsyncBaseExchange
from .coin_list_fetchers import fetch_gate_coins

logger = logging.getLogger(__name__)

class AsyncGateExchange(AsyncBaseExchange):
    BASE_URL = "https://api.gateio.ws"

    def __init__(self):
        super().__init__("Gate")

    def _normalize_symbol(self, coin: str) -> str:
        """
        Преобразует монету в формат Gate.io для фьючерсов (например, CVC -> CVC_USDT)
        
        ВАЖНО: На Gate.io FUN_USDT соответствует SPORTFUN (Sport.Fun), а не FUNTOKEN.
        FUNTOKEN на Gate.io отсутствует.
        """
        c = coin.upper()
        # Алиасы для известных коллизий
        aliases = {
            "SPORTFUN": "FUN_USDT",  # SPORTFUN -> FUN_USDT (на Gate FUN_USDT это SPORTFUN)
            # FUN (FUNTOKEN) на Gate отсутствует — возвращаем несуществующий символ, чтобы получить N/A
            "FUN": "FUNTOKEN_USDT",
        }
        return aliases.get(c, f"{c}_USDT")

    def _safe_px(self, raw: object, fallback: float, sanity_mult: float = 20.0) -> float:
        """
        Безопасное преобразование цены с проверками на разумность
        """
        try:
            v = float(raw)
        except (TypeError, ValueError):
            return fallback

        if v <= 0:
            return fallback

        # Для нормальных цен применяем sanity-check
        if fallback >= 0.0001:
            if v > fallback * sanity_mult or v < fallback / sanity_mult:
                return fallback

        return v

    def _parse_ob_levels(
        self,
        levels: Any,
        side: str,
        coin: str,
    ) -> Optional[List[List[float]]]:
        """
        Gate может вернуть уровни в разных форматах:
        1) REST/WS-legacy: [{"p":"97.1","s":2245}, ...]
        2) REST (часто): [["97.1","2245"], ...] или [[price, size], ...]
        3) Иногда может быть tuple/list длины >= 2
        Возвращаем нормализованный формат: [[price(float), size(float)], ...]
        """
        if not isinstance(levels, list) or not levels:
            return None

        out: List[List[float]] = []

        for i, lvl in enumerate(levels):
            price_raw = None
            size_raw = None

            if isinstance(lvl, dict):
                # WS legacy формат: {"p": "...", "s": ...}
                price_raw = lvl.get("p") or lvl.get("price")
                size_raw = lvl.get("s") or lvl.get("size")
            elif isinstance(lvl, (list, tuple)):
                if len(lvl) < 2:
                    logger.warning(f"Gate: level too short in {side} for {coin}: {lvl}")
                    return None
                price_raw, size_raw = lvl[0], lvl[1]
            else:
                logger.warning(f"Gate: unexpected level type in {side} for {coin}: {type(lvl)}")
                return None

            try:
                p = float(price_raw)
                s = float(size_raw)
            except (TypeError, ValueError):
                logger.warning(f"Gate: non-numeric level in {side} for {coin}: {lvl}")
                return None

            if p <= 0:
                continue

            # size в ордербуке должен быть положительным; на всякий случай нормализуем
            if s < 0:
                s = abs(s)

            out.append([p, s])

            # лёгкая защита от безумных ответов
            if i > 5000:
                break

        return out if out else None

    async def get_futures_ticker(self, coin: str) -> Optional[Dict]:
        """
        Получить тикер фьючерса для монеты
        """
        try:
            symbol = self._normalize_symbol(coin)  # e.g. TUT_USDT
            url = "/api/v4/futures/usdt/tickers"
            params = {"contract": symbol}

            data = await self._request_json("GET", url, params=params)

            # 1) Реальная проблема сети/HTTP/парсинга -> data == None (или Falsey не-list/dict)
            if data is None:
                # Причина чаще всего уже залогирована в AsyncBaseExchange._request_json (timeout/connection/HTTP status),
                # поэтому здесь не дублируем WARNING.
                logger.debug(f"Gate: тикер запрос вернул None для {coin} (contract={symbol})")
                return None

            # 2) Нормальная ситуация "контракт не найден / не активен": API вернул пустой список
            if isinstance(data, list):
                if not data:
                    logger.debug(f"Gate: тикер не найден для {coin} (contract={symbol})")
                    return None

                # если contract-параметр сработал, обычно будет один элемент;
                # но на всякий случай ищем точное совпадение и НЕ берем data[0], если совпадения нет
                item = next((x for x in data if isinstance(x, dict) and x.get("contract") == symbol), None)
                if item is None:
                    logger.debug(f"Gate: тикер список без нужного contract для {coin} (contract={symbol})")
                    return None

            elif isinstance(data, dict):
                item = data
                # Иногда Gate может вернуть dict с ошибкой вида {"label": "...", "message": "..."}
                # Если видишь такие поля — лучше логировать warning.
                if "message" in item and "label" in item and "contract" not in item:
                    logger.warning(f"Gate: ticker API error для {coin}: {item}")
                    return None
            else:
                logger.warning(f"Gate: неожиданный формат тикера для {coin}: {type(data)}")
                return None

            # дальше парсинг цены
            last_raw = item.get("last")
            if last_raw is None:
                logger.debug(f"Gate: нет last для {coin} (contract={symbol})")
                return None

            last = float(last_raw)
            if last <= 0:
                return None

            bid_raw = item.get("bid") or item.get("highest_bid")
            ask_raw = item.get("ask") or item.get("lowest_ask")

            bid = self._safe_px(bid_raw, last)
            ask = self._safe_px(ask_raw, last)

            if bid > ask:
                bid = last
                ask = last

            return {"price": last, "bid": bid, "ask": ask}

        except Exception as e:
            logger.error(f"Gate: ошибка при получении тикера для {coin}: {e}", exc_info=True)
            return None

    async def get_funding_rate(self, coin: str) -> Optional[float]:
        """
        Получить текущую ставку фандинга для монеты
        
        Gate:
        - /futures/usdt/contracts/{contract} -> текущая/индикативная ставка (UI "Текущая")
        - /futures/usdt/funding_rate -> история (UI "Предыдущая")
        """
        try:
            symbol = self._normalize_symbol(coin)

            # 1) CURRENT (contract info)
            url = f"/api/v4/futures/usdt/contracts/{symbol}"
            data = await self._request_json("GET", url)

            if isinstance(data, dict):
                # приоритет: funding_rate (обычно то, что UI показывает как текущую)
                r = data.get("funding_rate")
                if r is None:
                    # иногда полезно взять indicative
                    r = data.get("funding_rate_indicative")
                if r is not None:
                    return float(r)

            # 2) FALLBACK: HISTORY (previous applied)
            url = "/api/v4/futures/usdt/funding_rate"
            params = {"contract": symbol, "limit": 1}
            data = await self._request_json("GET", url, params=params)

            if isinstance(data, list) and data:
                item = data[0]
                r = item.get("r") or item.get("funding_rate") or item.get("rate")
                if r is not None:
                    return float(r)

            logger.warning(f"Gate: не удалось получить funding_rate для {coin} (symbol={symbol})")
            return None

        except Exception as e:
            logger.error(f"Gate: ошибка при получении фандинга для {coin}: {e}", exc_info=True)
            return None

    async def get_funding_info(self, coin: str) -> Optional[Dict]:
        """
        Получить информацию о фандинге (ставка и время до следующей выплаты)
        
        Gate:
        - /futures/usdt/contracts/{contract} -> текущая ставка и funding_next_apply_time
        - Также пробуем получить время из истории фандингов
        
        Returns:
            Словарь с данными:
            {
                "funding_rate": float,  # Ставка фандинга (например, 0.0001 = 0.01%)
                "next_funding_time": int,  # Timestamp следующей выплаты в секундах
            }
            или None если ошибка
        """
        try:
            symbol = self._normalize_symbol(coin)
            url = f"/api/v4/futures/usdt/contracts/{symbol}"
            data = await self._request_json("GET", url)

            if not isinstance(data, dict):
                return None

            # приоритет: funding_rate (обычно то, что UI показывает как текущую)
            r = data.get("funding_rate")
            if r is None:
                # иногда полезно взять indicative
                r = data.get("funding_rate_indicative")
            if r is None:
                return None

            # Gate контракт-эндпоинт может возвращать время следующей выплаты в разных полях.
            # На практике встречается `funding_next_apply` (unix seconds), а не `funding_next_apply_time`.
            # Также может быть в других полях: funding_next_apply_time, next_funding_time, funding_time, ...
            # Проверяем все возможные поля для времени следующей выплаты
            # Важно: проверяем не только на None, но и на 0/пустую строку
            def _get_time_field(field_name):
                val = data.get(field_name)
                if val is None or val == "" or val == 0:
                    return None
                return val
            
            next_funding_time_raw = (
                _get_time_field("funding_next_apply_time") or
                _get_time_field("funding_next_apply") or
                _get_time_field("next_funding_time") or
                _get_time_field("funding_time") or
                _get_time_field("next_funding_apply_time") or
                _get_time_field("funding_next_time") or
                _get_time_field("next_funding") or
                _get_time_field("funding_apply_time")
            )
            
            # Если не нашли в contracts, пробуем получить из тикера (может содержать время следующей выплаты)
            if next_funding_time_raw is None:
                try:
                    ticker_data = await self.get_futures_ticker(coin)
                    # Тикер обычно не содержит время следующей выплаты, но проверим на всякий случай
                    # (это маловероятно, но не помешает)
                except Exception:
                    pass
            
            # Логируем для диагностики, если время не найдено
            if next_funding_time_raw is None:
                # Логируем все поля, связанные с funding/time/next
                available_keys = [k for k in data.keys() if "funding" in k.lower() or "time" in k.lower() or "next" in k.lower()]
                # Также логируем все ключи для полной диагностики (первые 30)
                all_keys = list(data.keys())[:30]
                logger.warning(
                    f"Gate: не найдено время следующей выплаты для {coin} (symbol={symbol}). "
                    f"Релевантные поля: {available_keys if available_keys else 'нет'}. "
                    f"Все поля (первые 30): {all_keys}"
                )
                # Логируем значения релевантных полей
                if available_keys:
                    relevant_values = {k: data.get(k) for k in available_keys[:10]}
                    logger.warning(f"Gate: значения релевантных полей для {coin}: {relevant_values}")

            # Если не нашли в contracts, пробуем получить из истории фандингов
            if next_funding_time_raw is None:
                try:
                    url_history = "/api/v4/futures/usdt/funding_rate"
                    params_history = {"contract": symbol, "limit": 1}
                    data_history = await self._request_json("GET", url_history, params=params_history)
                    
                    if isinstance(data_history, list) and data_history:
                        item = data_history[0]
                        # В истории может быть время следующей выплаты или время последней выплаты
                        # Проверяем все возможные поля
                        last_funding_time_raw = (
                            item.get("t") or  # timestamp последней выплаты
                            item.get("time") or
                            item.get("funding_time") or
                            item.get("apply_time")
                        )
                        
                        # Также проверяем, может быть есть поле для следующей выплаты
                        next_funding_time_raw = (
                            item.get("next_time") or
                            item.get("next_funding_time") or
                            item.get("next_apply_time")
                        )
                        
                        # Если нашли время следующей выплаты напрямую - используем его
                        if next_funding_time_raw is not None:
                            try:
                                # Может быть в секундах или миллисекундах
                                next_time = int(float(next_funding_time_raw))
                                # Если значение очень большое (> 1e10), вероятно это миллисекунды
                                if next_time > 1e10:
                                    next_funding_time_raw = next_time // 1000
                                else:
                                    next_funding_time_raw = next_time
                            except (TypeError, ValueError):
                                next_funding_time_raw = None
                        # Если получили время последней выплаты, добавляем 8 часов (28800 секунд)
                        elif last_funding_time_raw is not None:
                            try:
                                last_time = int(float(last_funding_time_raw))
                                # Если значение очень большое (> 1e10), вероятно это миллисекунды
                                if last_time > 1e10:
                                    last_time = last_time // 1000
                                # Добавляем 8 часов для следующей выплаты
                                next_funding_time_raw = last_time + 28800
                            except (TypeError, ValueError):
                                next_funding_time_raw = None
                except Exception as e:
                    # Если не удалось получить из истории, оставляем None
                    logger.debug(f"Gate: ошибка при получении истории фандинга для {coin}: {e}")
                    pass
            
            # Если всё ещё не нашли время, вычисляем fallback-ом.
            # ВАЖНО: НЕ хардкодим 8ч, потому что на Gate встречаются контракты с другим интервалом
            # (например, 1 час: funding_interval=3600).
            # Если время вычислено через fallback, логируем предупреждение.
            fallback_used = False
            if next_funding_time_raw is None:
                from datetime import datetime, timezone, timedelta

                now_utc = datetime.now(timezone.utc)
                now_s = int(now_utc.timestamp())

                # 1) Prefer contract-specified interval/offset if present
                interval_raw = data.get("funding_interval")
                offset_raw = data.get("funding_offset")
                try:
                    interval_s = int(float(interval_raw)) if interval_raw not in (None, "", 0) else 0
                except Exception:
                    interval_s = 0
                try:
                    offset_s = int(float(offset_raw)) if offset_raw not in (None, "", 0) else 0
                except Exception:
                    offset_s = 0

                next_funding_dt = None
                if interval_s > 0:
                    # next = ceil((now - offset) / interval) * interval + offset
                    k = (max(0, now_s - offset_s) // interval_s) + 1
                    next_s = (k * interval_s) + offset_s
                    next_funding_time_raw = int(next_s)
                    next_funding_dt = datetime.fromtimestamp(next_s, tz=timezone.utc)
                else:
                    # 2) Ultimate fallback: 8 hours at 00:00, 08:00, 16:00 UTC
                    current_hour = now_utc.hour
                    if current_hour < 8:
                        next_funding_dt = now_utc.replace(hour=8, minute=0, second=0, microsecond=0)
                    elif current_hour < 16:
                        next_funding_dt = now_utc.replace(hour=16, minute=0, second=0, microsecond=0)
                    else:
                        next_day = now_utc + timedelta(days=1)
                        next_funding_dt = next_day.replace(hour=0, minute=0, second=0, microsecond=0)
                    next_funding_time_raw = int(next_funding_dt.timestamp())

                fallback_used = True
                logger.warning(
                    f"Gate: используем fallback для вычисления времени выплаты {coin} (symbol={symbol}). "
                    f"Вычислено: {next_funding_dt.strftime('%Y-%m-%d %H:%M:%S UTC') if next_funding_dt else next_funding_time_raw}. "
                    f"Поля: funding_next_apply/funding_next_apply_time отсутствуют; interval={data.get('funding_interval')} offset={data.get('funding_offset')}."
                )

            try:
                funding_rate = float(r)
                next_funding_time = None
                if next_funding_time_raw is not None:
                    try:
                        # Может быть строка или число
                        if isinstance(next_funding_time_raw, str):
                            next_time_val = int(float(next_funding_time_raw))
                        else:
                            next_time_val = int(next_funding_time_raw)
                        
                        # Gate API возвращает время в секундах (Unix timestamp)
                        # Но если значение очень большое (> 1e10), возможно это миллисекунды
                        # Конвертируем в секунды, если нужно
                        original_val = next_time_val
                        if next_time_val > 1e10:
                            next_funding_time = next_time_val // 1000
                            logger.debug(f"Gate: конвертировали время из миллисекунд в секунды для {coin}: {original_val} -> {next_funding_time}")
                        else:
                            next_funding_time = next_time_val
                        
                        # Логируем для диагностики
                        from datetime import datetime, timezone
                        if next_funding_time:
                            next_dt = datetime.fromtimestamp(next_funding_time, tz=timezone.utc)
                            now_dt = datetime.now(timezone.utc)
                            diff_seconds = next_funding_time - int(now_dt.timestamp())
                            diff_minutes = diff_seconds // 60
                            logger.debug(
                                f"Gate: время следующей выплаты для {coin}: "
                                f"raw={original_val}, seconds={next_funding_time}, "
                                f"next_dt={next_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}, "
                                f"diff={diff_minutes} мин (fallback_used={fallback_used})"
                            )
                    except (TypeError, ValueError) as e:
                        # Если не удалось распарсить, оставляем None
                        logger.debug(f"Gate: ошибка парсинга времени для {coin}: {e}")
                        pass
                
                return {
                    "funding_rate": funding_rate,
                    "next_funding_time": next_funding_time,
                }
            except (TypeError, ValueError):
                return None

        except Exception as e:
            logger.error(f"Gate: ошибка при получении funding info для {coin}: {e}", exc_info=True)
            return None

    async def get_orderbook(self, coin: str, limit: int = 50) -> Optional[Dict]:
        """
        Получить orderbook (книгу заявок) для монеты
        """
        try:
            symbol = self._normalize_symbol(coin)
            url = "/api/v4/futures/usdt/order_book"

            # Gate принимает limit, но иногда удобнее дать небольшой sanity
            limit_i = max(1, min(int(limit), 200))

            # with_id=true полезен, если будешь синхронизировать по id, но для разовой ликвидности не обязателен
            params = {"contract": symbol, "limit": limit_i}

            data = await self._request_json("GET", url, params=params)
            if not data:
                logger.warning(f"Gate: не удалось получить orderbook для {coin}")
                return None

            if not isinstance(data, dict):
                logger.warning(f"Gate: неожиданный формат orderbook для {coin}: {type(data)}")
                return None

            bids_raw = data.get("bids")
            asks_raw = data.get("asks")

            bids = self._parse_ob_levels(bids_raw, "bids", coin)
            asks = self._parse_ob_levels(asks_raw, "asks", coin)

            if not bids or not asks:
                logger.warning(f"Gate: пустой/невалидный orderbook для {coin} (bids={type(bids_raw)}, asks={type(asks_raw)})")
                return None

            return {"bids": bids, "asks": asks}

        except Exception as e:
            logger.error(f"Gate: ошибка при получении orderbook для {coin}: {e}", exc_info=True)
            return None

    async def get_all_futures_coins(self) -> Set[str]:
        """
        Возвращает множество монет, доступных во фьючерсах на Gate.io.
        
        Returns:
            Множество монет без суффиксов (например, {"BTC", "ETH", "SOL", ...})
        """
        return await fetch_gate_coins(self)
