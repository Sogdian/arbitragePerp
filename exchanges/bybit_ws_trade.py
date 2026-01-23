# exchanges/bybit_ws_trade.py
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
        self._auth_future: Optional[asyncio.Future] = None
        self._pending: Dict[str, asyncio.Future] = {}

        self._log = logger or logging.getLogger(__name__)
        self._start_lock = asyncio.Lock()
        self._stop_lock = asyncio.Lock()

    @property
    def ready(self) -> bool:
        return self._ready.is_set()

    @property
    def is_ready(self) -> bool:
        """Alias for ready (Protocol compatibility)."""
        return self._ready.is_set()

    def _log_info(self, msg: str) -> None:
        try:
            self._log.info(msg)
        except Exception:
            pass

    def _log_warn(self, msg: str) -> None:
        try:
            self._log.warning(msg)
        except Exception:
            pass

    def _log_err(self, msg: str) -> None:
        try:
            self._log.error(msg)
        except Exception:
            pass

    def _sign_ws_auth(self, expires_ms: int) -> str:
        """
        signature = HMAC_SHA256(secret, f"GET/realtime{expires}")
        """
        payload = f"GET/realtime{int(expires_ms)}".encode("utf-8")
        return hmac.new(self.api_secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()

    async def start(self) -> None:
        """Запускает Trade WS и выполняет аутентификацию."""
        async with self._start_lock:
            if self._ws is not None and self._reader_task and not self._reader_task.done() and self.ready:
                return

            # reset state
            self._ready.clear()
            # close if half-open
            await self.stop()

            loop = asyncio.get_running_loop()
            self._auth_future = loop.create_future()

            try:
                self._ws = await websockets.connect(self.url, ping_interval=None, max_queue=None)
                self._log_info("Bybit Trade WS: подключено")
            except Exception as e:
                self._log_err(f"Bybit Trade WS: ошибка подключения: {e}")
                self._ws = None
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

            # wait for auth result (success or exception)
            try:
                await asyncio.wait_for(self._auth_future, timeout=5.0)
                self._ready.set()
                self._log_info("Bybit Trade WS: authenticated")
            except asyncio.TimeoutError:
                self._log_err("Bybit Trade WS: timeout ожидания auth")
                await self.stop()
                raise RuntimeError("Trade WS auth timeout")
            except Exception as e:
                self._log_err(f"Bybit Trade WS: auth failed: {e}")
                await self.stop()
                raise

    async def stop(self) -> None:
        """Останавливает Trade WS (idempotent)."""
        async with self._stop_lock:
            # fail pending immediately
            for _, fut in list(self._pending.items()):
                if not fut.done():
                    fut.set_exception(RuntimeError("WS stopped"))
            self._pending.clear()

            self._ready.clear()

            # cancel tasks (avoid awaiting a task from inside itself)
            cur = asyncio.current_task()

            if self._ping_task and self._ping_task is not cur:
                self._ping_task.cancel()
                try:
                    await self._ping_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
            self._ping_task = None

            if self._reader_task and self._reader_task is not cur:
                self._reader_task.cancel()
                try:
                    await self._reader_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
            self._reader_task = None

            # close socket
            try:
                if self._ws is not None:
                    await self._ws.close()
            except Exception:
                pass
            self._ws = None

            # auth future
            if self._auth_future is not None and not self._auth_future.done():
                self._auth_future.set_exception(RuntimeError("WS stopped"))
            self._auth_future = None

    async def _ping_loop(self) -> None:
        """Отправляет ping для поддержания соединения."""
        try:
            while True:
                await asyncio.sleep(self.ping_interval_sec)
                if self._ws is None:
                    return
                try:
                    await self._ws.send(json.dumps({"op": "ping"}))
                except (ConnectionClosed, WebSocketException) as e:
                    self._log_warn(f"Bybit Trade WS ping failed: {e}")
                    await self.stop()
                    return
                except Exception as e:
                    self._log_warn(f"Bybit Trade WS ping error: {e}")
                    await self.stop()
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
                    await self.stop()
                    return
                except Exception as e:
                    self._log_err(f"Bybit Trade WS: ошибка чтения: {e}")
                    await self.stop()
                    return

                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    self._log_warn(f"Bybit Trade WS: невалидный JSON: {str(raw)[:200]}")
                    continue

                # auth response
                if msg.get("op") == "auth":
                    if self._auth_future is not None and not self._auth_future.done():
                        if msg.get("retCode") == 0:
                            self._auth_future.set_result(True)
                        else:
                            self._auth_future.set_exception(RuntimeError(f"auth failed: {msg}"))
                    continue

                # pong / ping responses can be ignored
                if msg.get("op") in ("pong", "ping"):
                    continue

                # match reqId for request-response ops (order.create/amend/cancel)
                req_id = msg.get("reqId")
                if req_id and req_id in self._pending:
                    fut = self._pending.pop(req_id)
                    if not fut.done():
                        fut.set_result(msg)
        except asyncio.CancelledError:
            return
        except Exception as e:
            self._log_err(f"Bybit Trade WS reader error: {type(e).__name__}: {e}")
            # fail all pending
            for _, fut in list(self._pending.items()):
                if not fut.done():
                    fut.set_exception(e)
            self._pending.clear()
            # fail auth if waiting
            if self._auth_future is not None and not self._auth_future.done():
                self._auth_future.set_exception(e)

    async def create_order(
        self,
        *,
        order: Dict[str, Any],
        server_ts_ms: Optional[int] = None,
        recv_window_ms: Optional[int] = None,
        req_id: Optional[str] = None,
        timeout_sec: float = 0.5,  # Default 500ms (200-800ms range per contract)
    ) -> Dict[str, Any]:
        """
        Sends WS 'order.create' and returns standardized ACK response.

        Contract-compliant format:
          - Success: {"ok": True, "order_id": "...", "raw": {...}}
          - Failure: {"ok": False, "error": "...", "raw": {...}}

        Important: ACK != Filled. Use order-stream / executions later.

        Args:
            order: Order dict (category, symbol, side, orderType, qty, price, timeInForce, positionIdx)
            server_ts_ms: Server timestamp (defaults to current time + offset if not provided)
            recv_window_ms: Receive window (defaults to self.recv_window_ms)
            req_id: Request ID (auto-generated if not provided)
            timeout_sec: Timeout for ACK (default 0.5s, range 200-800ms per contract)

        Returns:
            {"ok": True, "order_id": str, "raw": dict} on success
            {"ok": False, "error": str, "raw": dict} on failure
        """
        if self._ws is None or not self.ready:
            return {
                "ok": False,
                "error": "Trade WS not started/authenticated",
                "raw": {},
            }

        # Auto-generate server_ts_ms if not provided (best-effort)
        if server_ts_ms is None:
            server_ts_ms = int(time.time() * 1000)

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
            return {
                "ok": False,
                "error": f"Failed to send order.create: {e}",
                "raw": {},
            }

        try:
            resp = await asyncio.wait_for(fut, timeout=timeout_sec)
        except asyncio.TimeoutError:
            self._pending.pop(rid, None)
            return {
                "ok": False,
                "error": f"order.create timeout after {timeout_sec}s",
                "raw": {},
            }
        except Exception as e:
            self._pending.pop(rid, None)
            return {
                "ok": False,
                "error": f"order.create error: {type(e).__name__}: {e}",
                "raw": {},
            }

        if not isinstance(resp, dict):
            return {
                "ok": False,
                "error": f"Bad WS response: {resp}",
                "raw": resp if isinstance(resp, dict) else {},
            }

        ret_code = resp.get("retCode")
        if ret_code != 0:
            ret_msg = resp.get("retMsg", "Unknown error")
            return {
                "ok": False,
                "error": f"order.create retCode={ret_code} retMsg={ret_msg}",
                "raw": resp,
            }

        # Extract order_id from response (multiple possible paths)
        order_id = None
        for path in (
            ("result", "orderId"),
            ("data", "orderId"),
            ("data", "result", "orderId"),
            ("result", "list", 0, "orderId"),
            ("data", "list", 0, "orderId"),
        ):
            cur = resp
            ok = True
            for key in path:
                try:
                    cur = cur[key] if isinstance(key, int) else cur.get(key)
                except Exception:
                    ok = False
                    break
                if cur is None:
                    ok = False
                    break
            if ok:
                try:
                    order_id = str(cur)
                    if order_id:
                        break
                except Exception:
                    pass

        if not order_id:
            return {
                "ok": False,
                "error": f"order.create succeeded but no order_id in response: {resp}",
                "raw": resp,
            }

        return {
            "ok": True,
            "order_id": order_id,
            "raw": resp,
        }
