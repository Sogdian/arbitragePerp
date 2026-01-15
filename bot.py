"""
Бот для арбитража фьючерсов между биржами
"""
import asyncio
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any
from exchanges.async_bybit import AsyncBybitExchange
from exchanges.async_gate import AsyncGateExchange
from exchanges.async_mexc import AsyncMexcExchange
from exchanges.async_lbank import AsyncLbankExchange
from exchanges.async_xt import AsyncXtExchange
from exchanges.async_binance import AsyncBinanceExchange
from exchanges.async_bitget import AsyncBitgetExchange
from exchanges.async_okx import AsyncOkxExchange
from exchanges.async_bingx import AsyncBingxExchange
from input_parser import parse_input
from news_monitor import NewsMonitor
from announcements_monitor import AnnouncementsMonitor
from x_news_monitor import XNewsMonitor
from telegram_sender import TelegramSender
import config

# Настройка логирования
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(config.LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Отключаем логирование HTTP запросов от httpx
logging.getLogger("httpx").setLevel(logging.WARNING)


class PerpArbitrageBot:
    """Бот для анализа арбитража фьючерсов"""
    
    def __init__(self):
        self.bybit = AsyncBybitExchange()
        self.gate = AsyncGateExchange()
        self.mexc = AsyncMexcExchange()
        self.lbank = AsyncLbankExchange()
        self.xt = AsyncXtExchange()
        self.binance = AsyncBinanceExchange()
        self.bitget = AsyncBitgetExchange()
        self.okx = AsyncOkxExchange()
        self.bingx = AsyncBingxExchange()
        self.exchanges = {
            "bybit": self.bybit,
            "gate": self.gate,
            "mexc": self.mexc,
            "lbank": self.lbank,
            "xt": self.xt,
            "binance": self.binance,
            "bitget": self.bitget,
            "okx": self.okx,
            "bingx": self.bingx
        }
        self.news_monitor = NewsMonitor()
        self.announcements_monitor = AnnouncementsMonitor(news_monitor=self.news_monitor)
        self.x_news_monitor = XNewsMonitor()
    
    async def close(self):
        """Закрывает соединения с биржами"""
        await asyncio.gather(
            self.bybit.close(),
            self.gate.close(),
            self.mexc.close(),
            self.lbank.close(),
            self.xt.close(),
            self.binance.close(),
            self.bitget.close(),
            self.okx.close(),
            self.bingx.close(),
            return_exceptions=True
        )
    
    async def get_futures_data(self, exchange_name: str, coin: str, need_funding: bool = True) -> Optional[Dict]:
        """
        Получить данные о фьючерсе (цена и фандинг) для монеты на бирже
        
        Args:
            exchange_name: Название биржи ("bybit" или "gate")
            coin: Название монеты (например, "CVC")
            need_funding: Запрашивать ли фандинг (по умолчанию True)
            
        Returns:
            Словарь с данными:
            {
                "price": float,
                "bid": float,
                "ask": float,
                "funding_rate": float (если need_funding=True)
            }
            или None если ошибка
        """
        exchange = self.exchanges.get(exchange_name)
        if not exchange:
            logger.error(f"Неизвестная биржа: {exchange_name}")
            return None
        
        # Всегда тянем bid/ask
        ticker = await exchange.get_futures_ticker(coin)
        
        if isinstance(ticker, Exception):
            logger.error(f"{exchange_name}: ошибка при получении тикера для {coin}: {ticker}")
            ticker = None
        
        if not ticker:
            return None
        
        out = {
            "price": ticker.get("price"),
            "bid": ticker.get("bid"),
            "ask": ticker.get("ask"),
        }
        
        # Funding — только если нужно
        if need_funding:
            funding_rate = await exchange.get_funding_rate(coin)
            
            if isinstance(funding_rate, Exception):
                logger.error(f"{exchange_name}: ошибка при получении фандинга для {coin}: {funding_rate}")
                funding_rate = None
            
            if funding_rate is not None:
                out["funding_rate"] = funding_rate
        
        return out
    
    def calculate_spread(self, price_short: Optional[float], price_long: Optional[float]) -> Optional[float]:
        """
        Вычислить спред на цену для арбитража (в процентах)
        
        Формула: (price_short - price_long) / price_long * 100
        
        Для схемы Long (A) / Short (B):
        - Положительный спред = хорошо (цена на Short бирже выше)
        - Отрицательный спред = плохо (цена на Short бирже ниже)
        
        Args:
            price_short: Цена на бирже Short
            price_long: Цена на бирже Long
            
        Returns:
            Спред в процентах или None если невозможно вычислить
        """
        if price_short is None or price_long is None:
            return None
        
        if price_long == 0:
            return None
        
        spread = ((price_short - price_long) / price_long) * 100
        return spread
    
    def calculate_funding_spread(self, funding_long: Optional[float], funding_short: Optional[float]) -> Optional[float]:
        """
        Вычислить чистый эффект по фандингу для арбитража (Long и Short позиции)
        
        Экономическая логика funding:
        - Если funding > 0: Long платит, Short получает
        - Если funding < 0: Long получает, Short платит
        
        PnL для позиций:
        - PnL Long = -funding_long (если funding положительный, платим; если отрицательный, получаем)
        - PnL Short = +funding_short (если funding положительный, получаем; если отрицательный, платим)
        
        Формула: Net funding = PnL_long + PnL_short = (-funding_long) + (+funding_short) = funding_short - funding_long
        
        Args:
            funding_long: Ставка фандинга на бирже Long (в десятичном формате, например, -0.02 = -2%)
            funding_short: Ставка фандинга на бирже Short (в десятичном формате, например, -0.025 = -2.5%)
            
        Returns:
            Чистый эффект по фандингу в процентах или None если невозможно вычислить
            Положительное значение = прибыль, отрицательное значение = убыток
            Пример: -0.5% означает, что за один funding-период будет убыток 0.5%
        """
        if funding_long is None or funding_short is None:
            return None
        
        # Конвертируем в проценты (funding rate обычно в формате 0.0001 = 0.01%)
        funding_long_pct = funding_long * 100
        funding_short_pct = funding_short * 100
        
        # Net funding PnL: funding_short - funding_long
        net_funding = funding_short_pct - funding_long_pct
        return net_funding
    
    async def process_input(self, input_text: str):
        """
        Обработать вводные данные и вывести информацию о фьючерсах и фандингах
        
        Args:
            input_text: Строка с вводными данными (например, "CVC Long (bybit), Short (gate)")
        """
        # Парсим вводные данные
        parsed = parse_input(input_text)
        if not parsed:
            logger.error("Не удалось распарсить вводные данные")
            return
        
        coin = parsed["coin"]
        long_exchange = parsed["long_exchange"]
        short_exchange = parsed["short_exchange"]
        notional_usdt = parsed.get("notional_usdt")
        
        if notional_usdt is None:
            logger.error("Размер инвестиций не указан. Формат: 'монета Long (биржа), Short (биржа) размер'")
            return None
        
        # Получаем данные с обеих бирж параллельно
        long_data_task = self.get_futures_data(long_exchange, coin)
        short_data_task = self.get_futures_data(short_exchange, coin)
        
        long_data, short_data = await asyncio.gather(
            long_data_task,
            short_data_task,
            return_exceptions=True
        )
        
        if isinstance(long_data, Exception):
            logger.error(f"Ошибка при получении данных с {long_exchange}: {long_data}")
            long_data = None
        
        if isinstance(short_data, Exception):
            logger.error(f"Ошибка при получении данных с {short_exchange}: {short_data}")
            short_data = None
        
        # Проверяем, доступна ли монета на биржах
        logger.info("=" * 60)
        logger.info(f"Анализ арбитража для {coin}")
        logger.info("=" * 60)
        
        # Если тикер не найден на бирже, монета недоступна/делистирована
        if long_data is None:
            logger.warning(f"⚠️ {coin} недоступна/делистирована на {long_exchange}")
            logger.warning("Арбитраж невозможен: тикер не найден на бирже Long")
            logger.info("=" * 60)
            return None
        
        if short_data is None:
            logger.warning(f"⚠️ {coin} недоступна/делистирована на {short_exchange}")
            logger.warning("Арбитраж невозможен: тикер не найден на бирже Short")
            logger.info("=" * 60)
            return None
        
        # Данные Long биржи
        if long_data:
            price_long = long_data.get("price")
            funding_long = long_data.get("funding_rate")
            
            # Вычисляем количество монет, которое можно купить за notional_usdt
            coins_long = None
            if price_long is not None and price_long > 0:
                coins_long = notional_usdt / price_long
            
            if price_long is not None:
                if coins_long is not None:
                    logger.info(f"(Long {long_exchange}) ({coin}) Цена: {price_long:.3f} ({coins_long:.3f} {coin})")
                else:
                    logger.info(f"(Long {long_exchange}) ({coin}) Цена: {price_long:.3f}")
            else:
                logger.info(f"(Long {long_exchange}) ({coin}) Цена: недоступно")
            
            if funding_long is not None:
                funding_long_pct = funding_long * 100
                logger.info(f"(Long {long_exchange}) ({coin}) Фандинг: {funding_long_pct:.3f}%")
            else:
                logger.info(f"(Long {long_exchange}) ({coin}) Фандинг: недоступно")
        else:
            logger.error(f"Не удалось получить данные с {long_exchange}")
            price_long = None
            funding_long = None
        
        # Данные Short биржи
        if short_data:
            price_short = short_data.get("price")
            funding_short = short_data.get("funding_rate")
            
            # Вычисляем количество монет, которое можно купить за notional_usdt
            coins_short = None
            if price_short is not None and price_short > 0:
                coins_short = notional_usdt / price_short
            
            if price_short is not None:
                if coins_short is not None:
                    logger.info(f"(Short {short_exchange}) ({coin}) Цена: {price_short:.3f} ({coins_short:.3f} {coin})")
                else:
                    logger.info(f"(Short {short_exchange}) ({coin}) Цена: {price_short:.3f}")
            else:
                logger.info(f"(Short {short_exchange}) ({coin}) Цена: недоступно")
            
            if funding_short is not None:
                funding_short_pct = funding_short * 100
                logger.info(f"(Short {short_exchange}) ({coin}) Фандинг: {funding_short_pct:.3f}%")
            else:
                logger.info(f"(Short {short_exchange}) ({coin}) Фандинг: недоступно")
        else:
            logger.error(f"Не удалось получить данные с {short_exchange}")
            price_short = None
            funding_short = None
        
        # Вычисляем спреды
        price_spread = None
        if price_long is not None and price_short is not None:
            # Формула: (price_short - price_long) / price_long * 100
            # Положительный спред = хорошо (цена на Short бирже выше)
            price_spread = self.calculate_spread(price_short, price_long)
            if price_spread is not None:
                logger.info(f"({long_exchange} и {short_exchange}) Спред на цену: {price_spread:.3f}%")
            else:
                logger.info(f"({long_exchange} и {short_exchange}) Спред на цену: невозможно вычислить")
        else:
            logger.info(f"({long_exchange} и {short_exchange}) Спред на цену: недоступно")
        
        if funding_long is not None and funding_short is not None:
            funding_spread = self.calculate_funding_spread(funding_long, funding_short)
            if funding_spread is not None:
                logger.info(f"({long_exchange} и {short_exchange}) Спред на фандинги: {funding_spread:.3f}% (откр: ≥0.18%, закр: ≤0.05%)")
            else:
                logger.info(f"({long_exchange} и {short_exchange}) Спред на фандинги: невозможно вычислить")
                funding_spread = None
        else:
            logger.info(f"({long_exchange} и {short_exchange}) Спред на фандинги: недоступно")
            funding_spread = None
        
        # Вычисляем общий спред (спред на цену + спред на фандинги)
        if price_spread is not None and funding_spread is not None:
            total_spread = price_spread + funding_spread
            logger.info(f"({long_exchange} и {short_exchange}) Спред общий: {total_spread:.3f}%")
        elif price_spread is not None:
            logger.info(f"({long_exchange} и {short_exchange}) Спред общий: недоступно (нет данных по фандингам)")
        elif funding_spread is not None:
            logger.info(f"({long_exchange} и {short_exchange}) Спред общий: недоступно (нет данных по цене)")
        else:
            logger.info(f"({long_exchange} и {short_exchange}) Спред общий: недоступно")
        
        logger.info("=" * 60)
        
        # Проверяем ликвидность на обеих биржах для указанного размера инвестиций
        await self.check_liquidity_for_coin(coin, long_exchange, short_exchange, notional_usdt)
        
        # Проверяем делистинг на обеих биржах
        await self.check_delisting_for_coin(coin, exchanges=[long_exchange, short_exchange])
        
        # Сохраняем данные для мониторинга
        return {
            "coin": coin,
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "long_data": long_data,
            "short_data": short_data
        }
    
    async def check_liquidity_for_coin(self, coin: str, long_exchange: str, short_exchange: str, notional_usdt: float) -> Dict[str, Any]:
        """
        Проверяет ликвидность на обеих биржах для указанного размера инвестиций
        
        Args:
            coin: Символ монеты
            long_exchange: Биржа для Long позиции
            short_exchange: Биржа для Short позиции
            notional_usdt: Размер инвестиций в USDT (для каждой позиции: Long и Short)
        """
        size = notional_usdt

        long_liquidity: Optional[Dict[str, Any]] = None
        short_liquidity: Optional[Dict[str, Any]] = None
        
        # Проверяем ликвидность на Long бирже (для покупки)
        long_exchange_obj = self.exchanges.get(long_exchange)
        if long_exchange_obj:
            long_liquidity = await long_exchange_obj.check_liquidity(
                coin, 
                notional_usdt=size,
                ob_limit=50,
                max_spread_bps=30.0,
                max_impact_bps=50.0,
                mode="entry_long" # Проверяем только глубину на покупку
            )
            if long_liquidity:
                status = "✓" if long_liquidity["ok"] else "✗"
                buy_impact_str = f"{long_liquidity['buy_impact_bps']:.1f}bps" if long_liquidity['buy_impact_bps'] is not None else "N/A"
                reasons_str = f" (Причины: {', '.join(long_liquidity['reasons'])})" if not long_liquidity["ok"] else ""
                logger.info(f"{status} Ликвидность {long_exchange} Long ({coin}): {size} USDT | "
                          f"spread={long_liquidity['spread_bps']:.1f}bps, buy_impact={buy_impact_str}{reasons_str}")
            else:
                logger.warning(f"Не удалось проверить ликвидность {long_exchange} Long ({coin}) для {size} USDT")
        
        # Проверяем ликвидность на Short бирже (для продажи)
        short_exchange_obj = self.exchanges.get(short_exchange)
        if short_exchange_obj:
            short_liquidity = await short_exchange_obj.check_liquidity(
                coin,
                notional_usdt=size,
                ob_limit=50,
                max_spread_bps=30.0,
                max_impact_bps=50.0,
                mode="entry_short" # Проверяем только глубину на продажу
            )
            if short_liquidity:
                status = "✓" if short_liquidity["ok"] else "✗"
                sell_impact_str = f"{short_liquidity['sell_impact_bps']:.1f}bps" if short_liquidity['sell_impact_bps'] is not None else "N/A"
                reasons_str = f" (Причины: {', '.join(short_liquidity['reasons'])})" if not short_liquidity["ok"] else ""
                logger.info(f"{status} Ликвидность {short_exchange} Short ({coin}): {size} USDT | "
                          f"spread={short_liquidity['spread_bps']:.1f}bps, sell_impact={sell_impact_str}{reasons_str}")
            else:
                logger.warning(f"Не удалось проверить ликвидность {short_exchange} Short ({coin}) для {size} USDT")

        long_ok = bool(long_liquidity and long_liquidity.get("ok") is True)
        short_ok = bool(short_liquidity and short_liquidity.get("ok") is True)
        return {
            "ok": bool(long_ok and short_ok),
            "long_ok": long_ok,
            "short_ok": short_ok,
            "long": long_liquidity,
            "short": short_liquidity,
            "notional_usdt": size,
        }
    
    async def check_delisting_for_coin(self, coin: str, exchanges: Optional[List[str]] = None, days_back: int = 60):
        """
        Проверяет наличие новостей о делистинге монеты на указанных биржах
        
        Args:
            coin: Символ монеты
            exchanges: Список бирж для проверки (например, ["bybit", "gate"]). Если None, проверка не выполняется.
            days_back: Количество дней назад для поиска (по умолчанию 60)
        """
        try:
            if not exchanges:
                logger.warning(f"Укажите биржи для проверки делистинга {coin}")
                return
            
            # 1) Exchange announcements (existing)
            delisting_news = await self.news_monitor.check_delisting(coin, exchanges=exchanges, days_back=days_back)

            # 2) X(Twitter) (optional)
            now_utc = datetime.now(timezone.utc)
            lookback = now_utc - timedelta(days=days_back, hours=6) if days_back > 0 else None
            x_delisting_news: List[Dict[str, Any]] = []
            if getattr(self, "x_news_monitor", None) is not None and self.x_news_monitor.enabled:
                x_delisting_news = await self.x_news_monitor.find_delisting_news(
                    coin_symbol=coin,
                    exchanges=exchanges,
                    lookback=lookback,
                )
                # Логируем найденные X-делистинги (в отличие от exchange announcements, они иначе не логируются)
                for n in x_delisting_news[:5]:
                    title = (n.get("title") or "")[:120]
                    url = n.get("url") or "N/A"
                    logger.warning(f"⚠️ X delisting {coin}: {title} | URL: {url}")

            # Dedupe by URL/title
            if x_delisting_news:
                seen = set()
                merged: List[Dict[str, Any]] = []
                for it in (delisting_news or []) + x_delisting_news:
                    url = str(it.get("url") or "").strip()
                    key = url or (str(it.get("title") or "").strip()[:200])
                    if not key or key in seen:
                        continue
                    seen.add(key)
                    merged.append(it)
                delisting_news = merged
            
            # Формируем строку с биржами для вывода
            exchanges_str = ", ".join(exchanges)
            
            if not delisting_news:
                logger.info(f"✓ Новостей о делистинге {coin} ({exchanges_str}) за последние {days_back} дней не найдено")

                # Доп. проверка: security/hack новости по монете на тех же биржах
                security_news = await self.announcements_monitor.check_security_for_coin(
                    coin_symbol=coin,
                    exchanges=exchanges,
                    days_back=days_back,
                )
                # X security (optional) — добавляем к exchange-security
                x_security_news: List[Dict[str, Any]] = []
                if getattr(self, "x_news_monitor", None) is not None and self.x_news_monitor.enabled:
                    x_security_news = await self.x_news_monitor.find_security_news(
                        coin_symbol=coin,
                        exchanges=exchanges,
                        lookback=lookback,
                    )
                    for n in x_security_news[:5]:
                        title = (n.get("title") or "")[:120]
                        url = n.get("url") or "N/A"
                        logger.warning(f"⚠️ X security {coin}: {title} | URL: {url}")

                if x_security_news:
                    seen2 = set()
                    merged2: List[Dict[str, Any]] = []
                    for it in (security_news or []) + x_security_news:
                        url = str(it.get("url") or "").strip()
                        key = url or (str(it.get("title") or "").strip()[:200])
                        if not key or key in seen2:
                            continue
                        seen2.add(key)
                        merged2.append(it)
                    security_news = merged2
                if not security_news:
                    logger.info(
                        f"✓ Новостей о взломах/безопасности {coin} ({exchanges_str}) за последние {days_back} дней не найдено"
                    )
                else:
                    for n in security_news[:5]:
                        title = (n.get("title") or "")[:120]
                        url = n.get("url") or "N/A"
                        logger.warning(f"⚠️ Security news {coin}: {title} | URL: {url}")
        except Exception as e:
            logger.warning(f"Ошибка при проверке делистинга для {coin}: {e}")
    
    def calculate_opening_spread(self, ask_long: Optional[float], bid_short: Optional[float]) -> Optional[float]:
        """
        Вычислить спред открытия позиции (max)
        
        Формула: (bid_short - ask_long) / ask_long * 100
        Положительное значение = выгодный арбитраж (bid_short > ask_long)
        
        Args:
            ask_long: Цена ask на бирже Long
            bid_short: Цена bid на бирже Short
            
        Returns:
            Спред открытия в процентах или None
            Положительное значение = выгодно открывать, отрицательное = невыгодно
        """
        if ask_long is None or bid_short is None:
            return None
        
        if ask_long == 0:
            return None
        
        spread = ((bid_short - ask_long) / ask_long) * 100
        return spread
    
    def calculate_closing_spread(self, bid_long: Optional[float], ask_short: Optional[float]) -> Optional[float]:
        """
        Вычислить спред закрытия позиции (min)
        
        Формула: (bid_long - ask_short) / ask_short * 100
        
        Args:
            bid_long: Цена bid на бирже Long
            ask_short: Цена ask на бирже Short
            
        Returns:
            Спред закрытия в процентах или None
        """
        if bid_long is None or ask_short is None:
            return None
        
        if ask_short == 0:
            return None
        
        spread = ((bid_long - ask_short) / ask_short) * 100
        return spread
    
    def get_exit_threshold_pct(self) -> float:
        """
        Порог выхода в процентах.
        Используем дефолтные значения:
        - закрытие long: 0.04%
        - закрытие short: 0.04%
        - запас: 0.10%
        """
        close_long_fee_pct = 0.04
        close_short_fee_pct = 0.04
        buffer_pct = 0.10

        return close_long_fee_pct + close_short_fee_pct + buffer_pct
    
    async def monitor_spreads(self, coin: str, long_exchange: str, short_exchange: str, close_threshold_pct: Optional[float] = None):
        """
        Мониторинг спредов открытия и закрытия каждую секунду
        
        Args:
            coin: Название монеты
            long_exchange: Биржа для Long позиции
            short_exchange: Биржа для Short позиции
            close_threshold_pct: Порог закрытия в процентах (если указан, отправляет сообщение в Telegram при достижении)
        """
        logger.info("=" * 60)
        logger.info(f"Начало мониторинга спредов для {coin}")
        if close_threshold_pct is not None:
            logger.info(f"Порог закрытия для уведомлений: {close_threshold_pct:.2f}%")
        else:
            logger.info("Порог закрытия не установлен, уведомления отключены")
        logger.info("=" * 60)
        
        # Отслеживание времени последней отправки сообщения (ключ: (coin, long_exchange, short_exchange))
        last_sent_time: Dict[tuple, float] = {}
        # Интервал между отправками сообщений о закрытии (секунды), читается из .env
        SEND_INTERVAL_SEC = float(os.getenv("CLOSE_INTERVAL", "60"))  # По умолчанию 60 секунд (1 минута)
        
        try:
            while True:
                # Получаем данные с обеих бирж параллельно
                long_data_task = self.get_futures_data(long_exchange, coin)
                short_data_task = self.get_futures_data(short_exchange, coin)
                
                long_data, short_data = await asyncio.gather(
                    long_data_task,
                    short_data_task,
                    return_exceptions=True
                )
                
                if isinstance(long_data, Exception):
                    logger.error(f"Ошибка при получении данных с {long_exchange}: {long_data}")
                    long_data = None
                
                if isinstance(short_data, Exception):
                    logger.error(f"Ошибка при получении данных с {short_exchange}: {short_data}")
                    short_data = None
                
                if long_data and short_data:
                    # Извлекаем данные
                    ask_long = long_data.get("ask")
                    bid_long = long_data.get("bid")
                    funding_long = long_data.get("funding_rate")
                    
                    bid_short = short_data.get("bid")
                    ask_short = short_data.get("ask")
                    funding_short = short_data.get("funding_rate")
                    
                    # Рассчитываем спреды
                    opening_spread = self.calculate_opening_spread(ask_long, bid_short)
                    closing_spread = self.calculate_closing_spread(bid_long, ask_short)
                    
                    # Форматируем фандинги в проценты
                    funding_long_pct = funding_long * 100 if funding_long is not None else None
                    funding_short_pct = funding_short * 100 if funding_short is not None else None
                    
                    # Рассчитываем спред на фандинг (используем тот же метод, что и в process_input)
                    fr_spread = self.calculate_funding_spread(funding_long, funding_short)
                    
                    # Рассчитываем общий спред (спред на цену + спред на фандинг)
                    total_spread = None
                    if opening_spread is not None and fr_spread is not None:
                        total_spread = opening_spread + fr_spread
                    
                    # Формируем строку вывода
                    exit_threshold = self.get_exit_threshold_pct()
                    # Инвертируем знак closing_spread для отображения
                    closing_spread_display = -closing_spread if closing_spread is not None else None
                    if closing_spread_display is not None:
                        if close_threshold_pct is not None:
                            closing_str = f"🚩 Закр: {closing_spread_display:.3f}% (min: {exit_threshold:.3f}% цель: {close_threshold_pct:.3f}%)"
                        else:
                            closing_str = f"🚩 Закр: {closing_spread_display:.3f}% (min: {exit_threshold:.3f}%)"
                    else:
                        if close_threshold_pct is not None:
                            closing_str = f"🚩 Закр: N/A (min: {exit_threshold:.3f}% цель: {close_threshold_pct:.3f}%)"
                        else:
                            closing_str = f"🚩 Закр: N/A (min: {exit_threshold:.3f}%)"
                    opening_str = f"⛳ Откр: {opening_spread:.3f}%" if opening_spread is not None else "⛳ Откр: N/A"
                    
                    if fr_spread is not None:
                        fr_spread_str = f"{fr_spread:.3f}"
                    else:
                        fr_spread_str = "N/A"
                    
                    total_spread_str = f"{total_spread:.3f}" if total_spread is not None else "N/A"
                    
                    # Формируем информацию о биржах и монете
                    long_ex_str = f"Long {long_exchange}"
                    short_ex_str = f"Short {short_exchange}"
                    coin_str = coin
                    
                    # Выводим одной строкой
                    logger.info(f"{closing_str} | {opening_str} | 💰 fr_spread: {fr_spread_str} | 🎯 total_spread: {total_spread_str}⚙️  {long_ex_str} | {short_ex_str} | {coin_str}")
                    
                    # Проверяем порог закрытия и отправляем сообщение в Telegram
                    # Для отрицательных порогов: отправляем, когда спред становится хуже (меньше) порога
                    # Для положительных порогов: отправляем, когда спред становится лучше (больше) порога
                    threshold_met = False
                    if close_threshold_pct is not None and closing_spread is not None:
                        if close_threshold_pct < 0:
                            # Для отрицательных порогов: спред хуже (меньше) порога
                            threshold_met = closing_spread <= close_threshold_pct
                        else:
                            # Для положительных порогов: спред лучше (больше или равен) порога
                            threshold_met = closing_spread >= close_threshold_pct
                    
                    if threshold_met:
                        # Проверяем интервал между отправками (раз в минуту)
                        key = (coin, long_exchange, short_exchange)
                        current_time = time.time()
                        last_sent = last_sent_time.get(key, 0)
                        
                        if current_time - last_sent >= SEND_INTERVAL_SEC:
                            try:
                                telegram = TelegramSender()
                                if telegram.enabled:
                                    channel_id = telegram._get_channel_id()
                                    if channel_id:
                                        # Формируем сообщение в новом формате
                                        long_ex_capitalized = long_exchange.capitalize()
                                        short_ex_capitalized = short_exchange.capitalize()
                                        
                                        message_lines = [
                                            f"⏰ <b>Time to close {coin}:</b> Long ({long_ex_capitalized}) / Short ({short_ex_capitalized})",
                                        ]
                                        
                                        exit_threshold = self.get_exit_threshold_pct()
                                        if closing_spread is not None:
                                            if close_threshold_pct is not None:
                                                message_lines.append(f"🚩 <b>Close:</b> {closing_spread:.2f}% (min: {exit_threshold:.2f}% цель: {close_threshold_pct:.2f}%)")
                                            else:
                                                message_lines.append(f"🚩 <b>Close:</b> {closing_spread:.2f}% (min: {exit_threshold:.2f}%)")
                                        else:
                                            message_lines.append(f"🚩 <b>Close:</b> N/A (min: {exit_threshold:.2f}%)")
                                        
                                        if fr_spread is not None:
                                            message_lines.append(f"💰 <b>Funding Spread:</b> {fr_spread:.6f}%")
                                        else:
                                            message_lines.append("💰 <b>Funding Spread:</b> N/A")
                                        
                                        telegram_message = "\n".join(message_lines)
                                        await telegram.send_message(telegram_message, channel_id=channel_id)
                                        
                                        # Обновляем время последней отправки
                                        last_sent_time[key] = current_time
                                        
                                        if close_threshold_pct < 0:
                                            logger.info(f"📱 Отправлено сообщение в Telegram: закрытие при спреде {closing_spread:.2f}% <= {close_threshold_pct:.2f}%")
                                        else:
                                            logger.info(f"📱 Отправлено сообщение в Telegram: закрытие при спреде {closing_spread:.2f}% >= {close_threshold_pct:.2f}%")
                                    else:
                                        logger.warning(f"📱 Telegram включен, но канал не настроен для режима {config.ENV_MODE}")
                            except Exception as e:
                                logger.warning(f"Ошибка отправки в Telegram: {e}", exc_info=True)
                        else:
                            # Интервал не прошел, пропускаем отправку
                            remaining = SEND_INTERVAL_SEC - (current_time - last_sent)
                            logger.debug(f"Пропуск отправки: интервал не прошел (осталось {remaining:.1f}с)")
                
                # Ждем 1 секунду перед следующей итерацией
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("=" * 60)
            logger.info("Мониторинг прерван пользователем")
            logger.info("=" * 60)
        except Exception as e:
            logger.error(f"Ошибка в мониторинге: {e}", exc_info=True)


async def main():
    """Главная функция"""
    bot = PerpArbitrageBot()
    
    try:
        # Читаем вводные данные из командной строки или stdin
        raw_args = [a.strip() for a in sys.argv[1:]]
        # Флаги управления интерактивностью/мониторингом
        monitor_forced = ("--monitor" in raw_args)
        monitor_disabled = ("--no-monitor" in raw_args) or ("--no-prompt" in raw_args)
        filtered_args = [a for a in raw_args if a not in ("--monitor", "--no-monitor", "--no-prompt")]

        if filtered_args:
            # Вводные данные переданы как аргументы командной строки
            input_text = " ".join(filtered_args)
        else:
            # Читаем из stdin
            print("Введите данные в формате: 'монета Long (биржа), Short (биржа) размер'")
            print("Пример: CVC Long (bybit), Short (gate) 100")
            input_text = input().strip()
        
        if not input_text:
            logger.error("Не указаны вводные данные")
            return
        
        # Обрабатываем вводные данные и получаем информацию для мониторинга
        monitoring_data = await bot.process_input(input_text)
        
        if monitoring_data:
            should_monitor = False

            if monitor_forced:
                should_monitor = True
            elif monitor_disabled:
                should_monitor = False
            else:
                # Спрашиваем про ручное открытие позиций
                print("\nБыло ли ручное открытие позиций (long и short)?")
                print("Введите 'Да' или 'Нет': если 'Да', то введите min цену закр, для отправки сообщения в тг")
                # Если запуск не интерактивный — не блокируемся.
                if not sys.stdin or not sys.stdin.isatty() or os.getenv("BOT_NO_PROMPT") == "1":
                    should_monitor = False
                    close_threshold_pct = None
                else:
                    answer = input().strip()
                    answer_lower = answer.lower()
                    should_monitor = answer_lower.startswith("да") or answer_lower.startswith("yes") or answer_lower.startswith("y")
                    
                    # Парсим порог закрытия из ввода (формат: "Да, 2%" или "Да, 2.5%" или "Да, -1%")
                    close_threshold_pct = None
                    if should_monitor:
                        # Ищем паттерн: число (может быть отрицательным) с опциональным знаком процента
                        # Ищем после запятой или в любом месте строки
                        match = re.search(r'([-]?\d+\.?\d*)\s*%?', answer)
                        if match:
                            try:
                                close_threshold_pct = float(match.group(1))
                                logger.info(f"Распарсен порог закрытия: {close_threshold_pct:.2f}% из ввода '{answer}'")
                            except ValueError:
                                close_threshold_pct = None
                                logger.warning(f"Не удалось распарсить порог закрытия из '{answer}', мониторинг без уведомлений")
                        else:
                            logger.warning(f"Не найден порог закрытия в '{answer}', мониторинг без уведомлений")
            
            if should_monitor:
                # Запускаем мониторинг
                await bot.monitor_spreads(
                    monitoring_data["coin"],
                    monitoring_data["long_exchange"],
                    monitoring_data["short_exchange"],
                    close_threshold_pct=close_threshold_pct
                )
            else:
                logger.info("Мониторинг не запущен")
        
    except KeyboardInterrupt:
        logger.info("Прервано пользователем")
    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
    finally:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())



