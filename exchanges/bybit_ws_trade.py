"""
Bybit WebSocket Trade (v5): place/amend/cancel orders via websocket.

Endpoint: wss://stream.bybit.com/v5/trade (mainnet)
Docs: Websocket Trade Guideline
"""

import asyncio
import hashlib
import hmac
import json
import logging
import time
import uuid
from typing import Any, Dict, Optional

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

logger = logging.getLogger(__name__)


class BybitTradeWS:
    """
    Bybit WS Trade (v5): place/amend/cancel orders via websocket.
    
    Endpoint: wss://stream.bybit.com/v5/trade  (mainnet)
    Docs: Websocket Trade Guideline.
    """
    
    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        url: str = "wss://stream.bybit.com/v5/trade",
        recv_window_ms: int = 8000,
        referer: str = "arb-bot",
        ping_interval_sec: float = 20.0,
        logger=None,
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.url = url
        self.recv_window_ms = int(recv_window_ms)
        self.referer = referer
        self.ping_interval_sec = float(ping_interval_sec)
        self._ws = None
        self._reader_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._ready = asyncio.Event()
        self._pending: Dict[str, asyncio.Future] = {}
        self._log = logger or logging.getLogger(__name__)
    
    def _log_info(self, msg: str) -> None:
        if self._log:
            self._log.info(msg)
    
    def _log_warn(self, msg: str) -> None:
        if self._log:
            self._log.warning(msg)
    
    def _log_err(self, msg: str) -> None:
        if self._log:
            self._log.error(msg)
    
    def _sign_ws_auth(self, expires_ms: int) -> str:
        """
        signature = HMAC_SHA256(secret, f"GET/realtime{expires}")
        """
        payload = f"GET/realtime{int(expires_ms)}".encode("utf-8")
        return hmac.new(self.api_secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()
    
    async def start(self) -> None:
        """Запускает Trade WS и выполняет аутентификацию."""
        if self._ws is not None:
            return
        
        self._ready.clear()
        
        try:
            self._ws = await websockets.connect(self.url, ping_interval=None, max_queue=None)
            self._log_info("Bybit Trade WS: подключено")
        except Exception as e:
            self._log_err(f"Bybit Trade WS: ошибка подключения: {e}")
            raise
        
        self._reader_task = asyncio.create_task(self._reader_loop())
        self._ping_task = asyncio.create_task(self._ping_loop())
        
        # --- AUTH ---
        expires = int(time.time() * 1000) + 10_000
        sig = self._sign_ws_auth(expires)
        auth_msg = {"op": "auth", "args": [self.api_key, str(expires), sig]}
        
        try:
            await self._ws.send(json.dumps(auth_msg))
            self._log_info("Bybit Trade WS: отправлен auth запрос")
        except Exception as e:
            self._log_err(f"Bybit Trade WS: ошибка отправки auth: {e}")
            await self.stop()
            raise
        
        # ждём подтверждения auth
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=5.0)
            self._log_info("Bybit Trade WS: authenticated")
        except asyncio.TimeoutError:
            self._log_err("Bybit Trade WS: timeout ожидания auth")
            await self.stop()
            raise RuntimeError("Trade WS auth timeout")
    
    async def stop(self) -> None:
        """Останавливает Trade WS."""
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
        
        # fail all pending
        for k, fut in list(self._pending.items()):
            if not fut.done():
                fut.set_exception(RuntimeError("WS stopped"))
        self._pending.clear()
        
        try:
            if self._ws is not None:
                await self._ws.close()
        except Exception:
            pass
        finally:
            self._ws = None
    
    async def _ping_loop(self) -> None:
        """Отправляет ping для поддержания соединения."""
        try:
            while True:
                await asyncio.sleep(self.ping_interval_sec)
                if self._ws is None:
                    return
                try:
                    await self._ws.send(json.dumps({"op": "ping"}))
                except (ConnectionClosed, WebSocketException):
                    return
                except Exception:
                    return
        except asyncio.CancelledError:
            return
    
    async def _reader_loop(self) -> None:
        """Читает сообщения из WebSocket."""
        try:
            while True:
                if self._ws is None:
                    return
                
                try:
                    raw = await self._ws.recv()
                except (ConnectionClosed, WebSocketException) as e:
                    self._log_warn(f"Bybit Trade WS: соединение закрыто: {e}")
                    break
                except Exception as e:
                    self._log_err(f"Bybit Trade WS: ошибка чтения: {e}")
                    break
                
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    self._log_warn(f"Bybit Trade WS: невалидный JSON: {raw[:200]}")
                    continue
                
                # auth response
                if msg.get("op") == "auth":
                    # Trade WS: success критерий = retCode == 0
                    if msg.get("retCode") == 0:
                        self._ready.set()
                        self._log_info("Bybit Trade WS: authenticated")
                    else:
                        self._ready.set()
                        raise RuntimeError(f"Bybit Trade WS auth failed: {msg}")
                    continue
                
                # match reqId
                req_id = msg.get("reqId")
                if req_id and req_id in self._pending:
                    fut = self._pending.pop(req_id)
                    if not fut.done():
                        fut.set_result(msg)
        except asyncio.CancelledError:
            return
        except Exception as e:
            self._log_err(f"Bybit Trade WS reader error: {type(e).__name__}: {e}")
            # fail pending
            for k, fut in list(self._pending.items()):
                if not fut.done():
                    fut.set_exception(e)
            self._pending.clear()
    
    async def create_order(
        self,
        *,
        order: Dict[str, Any],
        server_ts_ms: int,
        recv_window_ms: Optional[int] = None,
        req_id: Optional[str] = None,
        timeout_sec: float = 2.0,
    ) -> Dict[str, Any]:
        """
        Sends WS 'order.create' and returns ACK response.
        
        Important: ACK != Filled. Use order-stream / position check later.
        
        Request example and header fields are from Bybit WS trade guideline.
        """
        if self._ws is None:
            raise RuntimeError("Trade WS not started")
        
        rid = req_id or str(uuid.uuid4())
        rw = int(self.recv_window_ms if recv_window_ms is None else recv_window_ms)
        
        msg = {
            "reqId": rid,
            "header": {
                "X-BAPI-TIMESTAMP": str(int(server_ts_ms)),
                "X-BAPI-RECV-WINDOW": str(rw),
                "Referer": self.referer,
            },
            "op": "order.create",
            "args": [order],
        }
        
        fut = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        
        try:
            await self._ws.send(json.dumps(msg))
        except Exception as e:
            self._pending.pop(rid, None)
            raise RuntimeError(f"Failed to send order.create: {e}")
        
        try:
            resp = await asyncio.wait_for(fut, timeout=timeout_sec)
        except asyncio.TimeoutError:
            self._pending.pop(rid, None)
            raise RuntimeError(f"order.create timeout after {timeout_sec}s")
        
        if not isinstance(resp, dict):
            raise RuntimeError(f"Bad WS response: {resp}")
        
        if resp.get("retCode") != 0:
            raise RuntimeError(
                f"order.create retCode={resp.get('retCode')} "
                f"retMsg={resp.get('retMsg')} resp={resp}"
            )
        
        return resp

