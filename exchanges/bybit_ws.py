"""
WebSocket клиент для Bybit public market data.

Реализует подписки на:
- orderbook (best_bid/best_ask)
- trades (last_trade_price)
- tickers (backup для lastPrice/bid1/ask1)

Использует asyncio.Lock для безопасного обновления состояния.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

logger = logging.getLogger(__name__)


@dataclass
class MarketState:
    """Состояние рынка в памяти, обновляемое из WebSocket сообщений."""
    
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    last_trade: Optional[float] = None
    last_ticker: Optional[float] = None
    
    # Временные метки обновлений (time.monotonic())
    ts_bidask_monotonic: float = field(default_factory=time.monotonic)
    ts_trade_monotonic: float = field(default_factory=time.monotonic)
    ts_ticker_monotonic: float = field(default_factory=time.monotonic)
    
    def is_ready(self, max_age_ms: float = 5000.0) -> bool:
        """
        Проверяет готовность данных.
        
        Args:
            max_age_ms: Максимальный возраст данных в миллисекундах
            
        Returns:
            True если данные готовы и свежие
        """
        now = time.monotonic()
        max_age_sec = max_age_ms / 1000.0
        
        # Проверка bid/ask
        if self.best_bid is None or self.best_ask is None:
            return False
        if (now - self.ts_bidask_monotonic) > max_age_sec:
            return False
        
        # Проверка наличия хотя бы одной цены сделки
        if self.last_trade is None and self.last_ticker is None:
            return False
        
        # Проверка свежести trade или ticker
        trade_fresh = (now - self.ts_trade_monotonic) <= max_age_sec if self.last_trade is not None else False
        ticker_fresh = (now - self.ts_ticker_monotonic) <= max_age_sec if self.last_ticker is not None else False
        
        return trade_fresh or ticker_fresh
    
    def get_snapshot(self) -> 'MarketState':
        """Возвращает копию текущего состояния."""
        return MarketState(
            best_bid=self.best_bid,
            best_ask=self.best_ask,
            last_trade=self.last_trade,
            last_ticker=self.last_ticker,
            ts_bidask_monotonic=self.ts_bidask_monotonic,
            ts_trade_monotonic=self.ts_trade_monotonic,
            ts_ticker_monotonic=self.ts_ticker_monotonic,
        )


class BybitPublicWS:
    """
    WebSocket клиент для Bybit public market data.
    
    Подписывается на orderbook, trades и tickers для одного символа.
    """
    
    # Bybit V5 public WebSocket endpoint
    WS_URL = "wss://stream.bybit.com/v5/public/linear"
    
    def __init__(self, symbol: str):
        """
        Args:
            symbol: Символ в формате Bybit (например, "BTCUSDT")
        """
        self.symbol = symbol.upper()
        self.state = MarketState()
        self.lock = asyncio.Lock()
        self._ws = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._reconnect_delay = 0.5  # Начальная задержка для reconnect
        self._max_reconnect_delay = 15.0  # Максимальная задержка
    
    async def run(self) -> None:
        """
        Запускает WebSocket runner в бесконечном цикле с reconnect.
        Должен быть вызван как asyncio.create_task(ws.run()).
        """
        self._running = True
        reconnect_count = 0
        
        while self._running:
            try:
                # Закрываем старое соединение перед новым (защита от множественных подключений)
                await self._close_ws()
                
                await self._connect_and_subscribe()
                reconnect_count = 0  # Сброс счетчика при успешном подключении
                self._reconnect_delay = 0.5  # Сброс задержки при успешном подключении
                
                await self._read_messages()
            except asyncio.CancelledError:
                logger.info(f"Bybit WS: задача отменена для {self.symbol}")
                break
            except Exception as e:
                if self._running:
                    reconnect_count += 1
                    error_type = type(e).__name__
                    error_msg = str(e)
                    
                    # Детальное логирование причины закрытия/исключения
                    if isinstance(e, ConnectionClosed):
                        close_code = getattr(e, 'code', 'unknown')
                        close_reason = getattr(e, 'reason', 'unknown')
                        logger.warning(
                            f"Bybit WS: соединение закрыто для {self.symbol} | "
                            f"код={close_code} причина={close_reason} | "
                            f"reconnect #{reconnect_count} через {self._reconnect_delay:.1f}s"
                        )
                    elif isinstance(e, WebSocketException):
                        logger.warning(
                            f"Bybit WS: WebSocket исключение для {self.symbol} | "
                            f"тип={error_type} сообщение={error_msg} | "
                            f"reconnect #{reconnect_count} через {self._reconnect_delay:.1f}s"
                        )
                    else:
                        logger.warning(
                            f"Bybit WS: ошибка для {self.symbol} | "
                            f"тип={error_type} сообщение={error_msg} | "
                            f"reconnect #{reconnect_count} через {self._reconnect_delay:.1f}s"
                        )
                    
                    # Задержка перед reconnect (защита от tight loop)
                    await asyncio.sleep(self._reconnect_delay)
                    
                    # Exponential backoff: 0.5s → 1s → 2s → 4s → 8s → 15s (cap)
                    self._reconnect_delay = min(self._reconnect_delay * 2, self._max_reconnect_delay)
                else:
                    # Нормальное закрытие соединения - сброс задержки
                    self._reconnect_delay = 0.5
    
    async def _connect_and_subscribe(self) -> None:
        """Подключается к WebSocket и подписывается на темы."""
        logger.info(f"Bybit WS: подключение к {self.WS_URL} для {self.symbol}")
        
        try:
            self._ws = await websockets.connect(self.WS_URL)
            logger.info(f"Bybit WS: подключено для {self.symbol}")
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            logger.error(
                f"Bybit WS: ошибка подключения для {self.symbol} | "
                f"тип={error_type} сообщение={error_msg}"
            )
            raise
        
        # Подписки на темы
        topics = [
            f"orderbook.1.{self.symbol}",  # orderbook depth=1
            f"publicTrade.{self.symbol}",  # trades
            f"tickers.{self.symbol}",      # tickers
        ]
        
        subscribe_msg = {
            "op": "subscribe",
            "args": topics,
        }
        
        try:
            await self._ws.send(json.dumps(subscribe_msg))
            logger.info(f"Bybit WS: отправлена подписка для {self.symbol}: {topics}")
        except Exception as e:
            logger.error(f"Bybit WS: ошибка отправки подписки для {self.symbol}: {e}")
            await self._close_ws()
            raise
    
    async def _read_messages(self) -> None:
        """Читает сообщения из WebSocket и обновляет состояние."""
        while self._running:
            try:
                message = await asyncio.wait_for(self._ws.recv(), timeout=30.0)
            except asyncio.TimeoutError:
                # Ping для поддержания соединения
                try:
                    await self._ws.ping()
                except Exception:
                    pass
                continue
            except (ConnectionClosed, WebSocketException) as e:
                # Детальное логирование причины закрытия
                if isinstance(e, ConnectionClosed):
                    close_code = getattr(e, 'code', 'unknown')
                    close_reason = getattr(e, 'reason', 'unknown')
                    logger.warning(
                        f"Bybit WS: соединение закрыто при чтении для {self.symbol} | "
                        f"код={close_code} причина={close_reason}"
                    )
                else:
                    logger.warning(
                        f"Bybit WS: WebSocket исключение при чтении для {self.symbol} | "
                        f"тип={type(e).__name__} сообщение={str(e)}"
                    )
                break
            except Exception as e:
                logger.warning(
                    f"Bybit WS: ошибка чтения для {self.symbol} | "
                    f"тип={type(e).__name__} сообщение={str(e)}"
                )
                break
            
            try:
                data = json.loads(message)
                await self._handle_message(data)
            except json.JSONDecodeError:
                logger.warning(f"Bybit WS: невалидный JSON для {self.symbol}: {message[:200]}")
            except Exception as e:
                logger.warning(f"Bybit WS: ошибка обработки сообщения для {self.symbol}: {e}")
    
    async def _handle_message(self, data: dict) -> None:
        """Обрабатывает входящее сообщение и обновляет состояние."""
        if not isinstance(data, dict):
            return
        
        topic = data.get("topic", "")
        if not topic:
            # Может быть ответ на subscribe
            if data.get("success") is True:
                logger.debug(f"Bybit WS: подписка подтверждена для {self.symbol}")
            return
        
        try:
            if topic.startswith("orderbook."):
                await self._handle_orderbook(data)
            elif topic.startswith("publicTrade."):
                await self._handle_trade(data)
            elif topic.startswith("tickers."):
                await self._handle_ticker(data)
        except Exception as e:
            logger.warning(f"Bybit WS: ошибка обработки topic={topic} для {self.symbol}: {e}")
    
    async def _handle_orderbook(self, data: dict) -> None:
        """Обрабатывает обновление orderbook."""
        result = data.get("data", {})
        if not isinstance(result, dict):
            return
        
        bids = result.get("b", [])  # [[price, size], ...]
        asks = result.get("a", [])
        
        best_bid = None
        best_ask = None
        
        if bids and isinstance(bids, list) and len(bids) > 0:
            try:
                best_bid = float(bids[0][0])
            except (IndexError, ValueError, TypeError):
                pass
        
        if asks and isinstance(asks, list) and len(asks) > 0:
            try:
                best_ask = float(asks[0][0])
            except (IndexError, ValueError, TypeError):
                pass
        
        if best_bid is not None and best_ask is not None:
            async with self.lock:
                self.state.best_bid = best_bid
                self.state.best_ask = best_ask
                self.state.ts_bidask_monotonic = time.monotonic()
    
    async def _handle_trade(self, data: dict) -> None:
        """Обрабатывает обновление trades."""
        trades = data.get("data", [])
        if not isinstance(trades, list):
            return
        
        # Берем последнюю сделку из списка
        last_price = None
        for trade in trades:
            if not isinstance(trade, dict):
                continue
            try:
                price = float(trade.get("p", 0))
                if price > 0:
                    last_price = price
            except (ValueError, TypeError):
                continue
        
        if last_price is not None:
            async with self.lock:
                self.state.last_trade = last_price
                self.state.ts_trade_monotonic = time.monotonic()
    
    async def _handle_ticker(self, data: dict) -> None:
        """Обрабатывает обновление ticker."""
        result = data.get("data", {})
        if not isinstance(result, dict):
            return
        
        last_price = None
        try:
            last_price_raw = result.get("lastPrice")
            if last_price_raw is not None:
                last_price = float(last_price_raw)
        except (ValueError, TypeError):
            pass
        
        if last_price is not None and last_price > 0:
            async with self.lock:
                self.state.last_ticker = last_price
                self.state.ts_ticker_monotonic = time.monotonic()
    
    async def _close_ws(self) -> None:
        """Закрывает WebSocket соединение."""
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
    
    async def stop(self) -> None:
        """Останавливает WebSocket runner."""
        self._running = False
        await self._close_ws()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
    
    async def wait_ready(self, timeout: float = 10.0) -> bool:
        """
        Ждет готовности данных.
        
        Args:
            timeout: Таймаут в секундах
            
        Returns:
            True если данные готовы в течение timeout
        """
        start = time.monotonic()
        while (time.monotonic() - start) < timeout:
            async with self.lock:
                if self.state.is_ready():
                    return True
            await asyncio.sleep(0.1)
        return False
    
    async def get_snapshot(self) -> MarketState:
        """
        Возвращает снимок текущего состояния (thread-safe).
        
        Returns:
            Копия MarketState
        """
        async with self.lock:
            return self.state.get_snapshot()

