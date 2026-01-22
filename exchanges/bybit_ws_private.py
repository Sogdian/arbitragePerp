"""
Private WebSocket Stream для Bybit: order и execution updates.

Endpoint: wss://stream.bybit.com/v5/private
Подписки: order, execution
"""

import asyncio
import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

logger = logging.getLogger(__name__)


@dataclass
class OrderFinal:
    """Финальный статус ордера."""
    order_id: str
    status: str
    filled_qty: float
    avg_price: Optional[float]
    raw: Dict[str, Any]


class BybitPrivateWS:
    """
    Private WebSocket Stream:
      - connect: wss://stream.bybit.com/v5/private
      - auth: op=auth args=[api_key, expires, signature]
      - subscribe: order, execution
    """
    
    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        url: str = "wss://stream.bybit.com/v5/private"
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.url = url
        
        self._ws = None
        self._reader_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        
        self._authed = asyncio.Event()
        self._ready = asyncio.Event()
        self._stop = asyncio.Event()
        
        # order_id -> Future[OrderFinal]
        self._waiters: Dict[str, asyncio.Future] = {}

        # --- position cache (0-REST) ---
        # key: (symbol, positionIdx, side) where side is "Buy"/"Sell"
        self._positions: Dict[Tuple[str, int, str], float] = {}
        self._pos_last_ms: Dict[Tuple[str, int, str], int] = {}
        self._pos_events: Dict[Tuple[str, int, str], asyncio.Event] = {}
        self._pos_any = asyncio.Event()
        
        # last update timestamp (monotonic ms)
        self._last_msg_ms: Optional[int] = None
    
    @property
    def ready(self) -> bool:
        return self._ready.is_set()
    
    def staleness_ms(self) -> Optional[float]:
        """Возвращает staleness последнего сообщения в миллисекундах."""
        if self._last_msg_ms is None:
            return None
        now = time.perf_counter() * 1000.0
        return max(0.0, now - self._last_msg_ms)
    
    async def start(self) -> None:
        """Запускает Private WS и выполняет auth + subscribe."""
        self._stop.clear()
        self._authed.clear()
        self._ready.clear()
        
        try:
            self._ws = await websockets.connect(
                self.url,
                ping_interval=None,   # ping делаем сами по документации
                ping_timeout=None,
                close_timeout=1,
                max_queue=256,
            )
            logger.info("Bybit Private WS: подключено")
        except Exception as e:
            logger.error(f"Bybit Private WS: ошибка подключения: {e}")
            raise
        
        self._reader_task = asyncio.create_task(self._reader_loop())
        await self._auth()
        await self._subscribe(["order", "execution", "position"])
        
        self._ping_task = asyncio.create_task(self._ping_loop())
        self._ready.set()
        logger.info("Bybit Private WS: ready (authed + subscribed)")
    
    async def stop(self) -> None:
        """Останавливает Private WS."""
        self._stop.set()
        
        try:
            if self._ping_task:
                self._ping_task.cancel()
                try:
                    await self._ping_task
                except asyncio.CancelledError:
                    pass
        except Exception:
            pass
        
        try:
            if self._reader_task:
                self._reader_task.cancel()
                try:
                    await self._reader_task
                except asyncio.CancelledError:
                    pass
        except Exception:
            pass
        
        # fail all pending waiters
        for order_id, fut in list(self._waiters.items()):
            if not fut.done():
                fut.set_exception(RuntimeError("Private WS stopped"))
        self._waiters.clear()

        # clear position waiters/events
        try:
            for _k, ev in list(self._pos_events.items()):
                try:
                    ev.set()
                except Exception:
                    pass
        except Exception:
            pass
        self._pos_events.clear()
        self._positions.clear()
        self._pos_last_ms.clear()
        try:
            self._pos_any.set()
        except Exception:
            pass
        
        try:
            if self._ws:
                await self._ws.close()
        except Exception:
            pass
        finally:
            self._ws = None
    
    async def _send(self, payload: Dict[str, Any]) -> None:
        """Отправляет сообщение в WebSocket."""
        if not self._ws:
            raise RuntimeError("Private WS is not connected")
        await self._ws.send(json.dumps(payload, separators=(",", ":")))
    
    async def _auth(self) -> None:
        """Выполняет аутентификацию."""
        # signature = HMAC_SHA256(secret, f"GET/realtime{expires}")
        # Минимум +20 секунд для защиты от джиттеров (локальные часы, задержка сети, GC, планировщик)
        expires = int(time.time() * 1000) + 20_000
        sign_payload = f"GET/realtime{expires}".encode("utf-8")
        signature = hmac.new(self.api_secret.encode("utf-8"), sign_payload, hashlib.sha256).hexdigest()
        
        await self._send({"op": "auth", "args": [self.api_key, str(expires), signature]})
        
        try:
            ok = await asyncio.wait_for(self._authed.wait(), timeout=5.0)
            if not ok:
                raise RuntimeError("Bybit Private WS auth timeout")
        except asyncio.TimeoutError:
            raise RuntimeError("Bybit Private WS auth timeout")
    
    async def _subscribe(self, topics: list[str]) -> None:
        """Подписывается на темы."""
        # Stream WS subscribe ack uses success:true
        await self._send({"op": "subscribe", "args": topics})
        logger.info(f"Bybit Private WS: отправлена подписка на {topics}")
    
    async def _ping_loop(self) -> None:
        """Отправляет ping для поддержания соединения."""
        # docs recommend ping every ~20s
        try:
            while not self._stop.is_set():
                await asyncio.sleep(20)
                if self._stop.is_set():
                    return
                try:
                    await self._send({"op": "ping"})
                except (ConnectionClosed, WebSocketException) as e:
                    logger.warning(f"Bybit Private WS ping failed: {e}")
                    await self.stop()
                    return
                except Exception as e:
                    logger.warning(f"Bybit Private WS ping error: {e}")
                    # Останавливаем WS при ошибках ping, чтобы не висеть "живым трупом"
                    await self.stop()
                    return
        except asyncio.CancelledError:
            return
    
    async def _reader_loop(self) -> None:
        """Читает сообщения из WebSocket."""
        try:
            while not self._stop.is_set():
                if not self._ws:
                    return
                
                try:
                    raw = await self._ws.recv()
                except (ConnectionClosed, WebSocketException) as e:
                    logger.warning(f"Bybit Private WS: соединение закрыто: {e}")
                    break
                except Exception as e:
                    logger.warning(f"Bybit Private WS: ошибка чтения: {e}")
                    break
                
                self._last_msg_ms = int(time.perf_counter() * 1000)
                
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    logger.warning(f"Bybit Private WS: невалидный JSON: {raw[:200]}")
                    continue
                
                # auth ack: принимаем оба формата (success=True или retCode=0)
                if msg.get("op") == "auth":
                    # Bybit может возвращать success=True (Stream WS) или retCode=0 (как Trade WS)
                    ok = (msg.get("success") is True) or (msg.get("retCode") == 0)
                    if ok:
                        self._authed.set()
                        logger.info("Bybit Private WS: authenticated")
                    else:
                        raise RuntimeError(f"Bybit Private WS auth failed: {msg}")
                    continue
                
                # subscription ack (можно не ждать)
                if msg.get("op") == "subscribe":
                    # success:true
                    if msg.get("success") is True:
                        logger.debug("Bybit Private WS: подписка подтверждена")
                    continue
                
                topic = msg.get("topic")
                if not topic:
                    continue
                
                data = msg.get("data")
                if not isinstance(data, list):
                    continue
                
                if topic == "order":
                    self._handle_order_updates(data)
                elif topic == "execution":
                    # если захочешь точнее считать fee/avg — можно расширить
                    pass
                elif topic == "position":
                    self._handle_position_updates(data)
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error(f"Bybit Private WS reader error: {type(e).__name__}: {e}")

    # ----------------------------
    # Position cache helpers
    # ----------------------------
    @staticmethod
    def _norm_side(side: Any) -> str:
        s = str(side or "").strip()
        if s.lower() == "buy":
            return "Buy"
        if s.lower() == "sell":
            return "Sell"
        return s

    def get_position_size(self, *, symbol: str, position_idx: int, side: str) -> Optional[float]:
        """Returns cached position size (abs qty). None if not yet available."""
        key = (str(symbol), int(position_idx), self._norm_side(side))
        v = self._positions.get(key)
        if v is None:
            return None
        try:
            return float(abs(v))
        except Exception:
            return None

    def position_staleness_ms(self, *, symbol: str, position_idx: int, side: str) -> Optional[float]:
        key = (str(symbol), int(position_idx), self._norm_side(side))
        last = self._pos_last_ms.get(key)
        if last is None:
            return None
        now = int(time.perf_counter() * 1000)
        return float(max(0, now - int(last)))

    async def wait_position(self, *, symbol: str, position_idx: int, side: str, timeout: float = 2.0) -> bool:
        """Wait until we have at least one position update for given key."""
        key = (str(symbol), int(position_idx), self._norm_side(side))
        if key in self._positions:
            return True
        ev = self._pos_events.get(key)
        if ev is None or ev.is_set():
            ev = asyncio.Event()
            self._pos_events[key] = ev
        try:
            await asyncio.wait_for(ev.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return key in self._positions
        return key in self._positions

    async def wait_any_position(self, timeout: float = 2.0) -> bool:
        """Wait until any position update arrives (useful right after subscribe)."""
        if self._pos_any.is_set():
            return True
        try:
            await asyncio.wait_for(self._pos_any.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return self._pos_any.is_set()
        return True
    
    def _handle_order_updates(self, items: list[dict]) -> None:
        """Обрабатывает обновления ордеров."""
        for it in items:
            if not isinstance(it, dict):
                continue
            
            order_id = str(it.get("orderId") or "")
            if not order_id:
                continue
            
            status = str(it.get("orderStatus") or "")
            cum_exec = it.get("cumExecQty")
            avg = it.get("avgPrice") or it.get("avgPx") or it.get("avgFillPrice")
            
            try:
                filled = float(cum_exec) if cum_exec is not None else 0.0
            except Exception:
                filled = 0.0
            
            avg_px = None
            try:
                if avg is not None:
                    v = float(avg)
                    if v > 0:
                        avg_px = v
            except Exception:
                pass
            
            # финальные статусы
            if status.lower() in ("filled", "cancelled", "canceled", "rejected"):
                fut = self._waiters.get(order_id)
                if fut and not fut.done():
                    fut.set_result(OrderFinal(
                        order_id=order_id,
                        status=status,
                        filled_qty=filled,
                        avg_price=avg_px,
                        raw=it,
                    ))

    def _handle_position_updates(self, items: list[dict]) -> None:
        """Обрабатывает position updates и обновляет локальный кэш позиций."""
        now_ms = int(time.perf_counter() * 1000)
        for it in items:
            if not isinstance(it, dict):
                continue
            symbol = str(it.get("symbol") or "").strip()
            if not symbol:
                continue
            try:
                pidx = int(it.get("positionIdx") or 0)
            except Exception:
                pidx = 0
            if pidx <= 0:
                # fallback: one-way аккаунт может присылать 0; всё равно кэшируем под 0
                pidx = 0

            side = self._norm_side(it.get("side") or it.get("positionSide") or "")
            if not side:
                # fallback по positionIdx (hedge): 1=Buy, 2=Sell
                if pidx == 1:
                    side = "Buy"
                elif pidx == 2:
                    side = "Sell"
                else:
                    continue

            try:
                sz = float(it.get("size") or 0.0)
            except Exception:
                sz = 0.0

            key = (symbol, int(pidx), side)
            self._positions[key] = float(abs(sz))
            self._pos_last_ms[key] = now_ms

            ev = self._pos_events.get(key)
            if ev is not None:
                try:
                    ev.set()
                except Exception:
                    pass

            try:
                self._pos_any.set()
            except Exception:
                pass
    
    async def wait_final(self, order_id: str, timeout: float = 2.0) -> OrderFinal:
        """
        Ждет финального статуса ордера.
        
        Args:
            order_id: ID ордера
            timeout: Таймаут в секундах
            
        Returns:
            OrderFinal с финальным статусом
            
        Raises:
            asyncio.TimeoutError если таймаут
        """
        order_id = str(order_id)
        fut = self._waiters.get(order_id)
        if fut is None or fut.done():
            fut = asyncio.get_running_loop().create_future()
            self._waiters[order_id] = fut
        
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            # не копим мусор
            self._waiters.pop(order_id, None)

