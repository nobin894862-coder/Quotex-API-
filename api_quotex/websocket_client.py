"""Async WebSocket client for Quotex API."""
import asyncio
import json
import ssl
import time
import base64
from datetime import datetime
import pkg_resources
from collections import deque
from typing import Any, Callable, Dict, List, Optional, Union

import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedOK, ConnectionClosedError
from loguru import logger

from .models import ConnectionInfo, ConnectionStatus, ServerTime
from .constants import CONNECTION_SETTINGS, DEFAULT_HEADERS
from .exceptions import WebSocketError, ConnectionError, Base64DecodeError
from .monitoring import error_monitor, ErrorSeverity, ErrorCategory
from .config import Config

logger.remove()
log_filename = f"log-{time.strftime('%Y-%m-%d')}.txt"
logger.add(log_filename, level="INFO", encoding="utf-8", backtrace=True, diagnose=True)

def _now_ms() -> int:
    """Return current time in milliseconds."""
    return int(time.time() * 1000)

class MessageBatcher:
    """Batch outgoing messages to reduce WebSocket send calls under high load."""
    def __init__(self, batch_size: int = 10, batch_timeout: float = 0.03):
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.pending_messages: deque = deque()
        self._last_batch_time = time.time()
        self._batch_lock = asyncio.Lock()

    async def add_message(self, message: str) -> List[str]:
        async with self._batch_lock:
            self.pending_messages.append(message)
            current_time = time.time()
            if len(self.pending_messages) >= self.batch_size or (current_time - self._last_batch_time) >= self.batch_timeout:
                batch = list(self.pending_messages)
                self.pending_messages.clear()
                self._last_batch_time = current_time
                return batch
            return []

    async def flush_batch(self) -> List[str]:
        async with self._batch_lock:
            if not self.pending_messages:
                return []
            batch = list(self.pending_messages)
            self.pending_messages.clear()
            self._last_batch_time = time.time()
            return batch

class ConnectionPool:
    """Manages multiple WebSocket connections and tracks their performance statistics."""
    def __init__(self, max_connections: int = 3):
        self.max_connections = max_connections
        self.active_connections: Dict[str, websockets.WebSocketClientProtocol] = {}
        self.connection_stats: Dict[str, Dict[str, Any]] = {}
        self._pool_lock = asyncio.Lock()

    async def get_best_connection(self) -> Optional[str]:
        async with self._pool_lock:
            if not self.connection_stats:
                return None
            best_url = min(
                self.connection_stats.keys(),
                key=lambda url: (
                    self.connection_stats[url].get("avg_response_time", float("inf")),
                    -self.connection_stats[url].get("success_rate", 0.0),
                )
            )
            return best_url

    async def update_stats(self, url: str, response_time: float, success: bool) -> None:
        async with self._pool_lock:
            if url not in self.connection_stats:
                self.connection_stats[url] = {
                    "response_times": deque(maxlen=100),
                    "successes": 0,
                    "failures": 0,
                    "avg_response_time": 0.0,
                    "success_rate": 0.0,
                }
            stats = self.connection_stats[url]
            stats["response_times"].append(response_time)
            if success:
                stats["successes"] += 1
            else:
                stats["failures"] += 1
            if stats["response_times"]:
                stats["avg_response_time"] = sum(stats["response_times"]) / len(stats["response_times"])
            total_attempts = stats["successes"] + stats["failures"]
            if total_attempts > 0:
                stats["success_rate"] = stats["successes"] / total_attempts

class AsyncWebSocketClient:
    """Asynchronous WebSocket client for Quotex API (Engine.IO/Socket.IO v3)."""

    def __init__(self) -> None:
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.connection_info: Optional[ConnectionInfo] = None
        self.server_time: Optional[ServerTime] = None
        self._status: ConnectionStatus = ConnectionStatus.DISCONNECTED
        self._receiver_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._running: bool = False
        self._reconnect_attempts: int = 0
        self._max_reconnect_attempts: int = CONNECTION_SETTINGS["max_reconnect_attempts"]
        self._message_batcher = MessageBatcher()
        self._flush_task: Optional[asyncio.Task] = None  # delayed flush for small bursts
        self._connection_pool = ConnectionPool()
        self._rate_limiter = asyncio.Semaphore(10)
        self._message_cache: Dict[int, Any] = {}
        self._cache_ttl: float = 5.0
        self._event_handlers: Dict[str, List[Callable]] = {}
        # Handlers by prefix (Engine.IO/Socket.IO text frames)
        self._message_handlers: Dict[str, Callable[[str], None]] = {
            "0": self._handle_initial_message,
            "2": self._handle_ping_message,
            "3": self._handle_pong_message,
            "40": self._handle_connection_message,
            "41": self._handle_disconnect_message,
            "451-[": self._handle_json_message_wrapper,   # binary placeholder preface
            "42": self._handle_auth_message,
            "[": self._handle_candle_message,            # Engine.IO binary frame decoded to string (\x04JSON)
        }

        self.websocket_is_connected: bool = False
        self.ssl_mutual_exclusion: bool = False
        self.ssl_mutual_exclusion_write: bool = False
        self._last_auth_error: Optional[Dict] = None
        # Pending event from '451-["event",{"_placeholder":true}]'
        self._pending_binary_event: Optional[str] = None

    @property
    def is_connected(self) -> bool:
        return (
            self.websocket is not None
            and not self.websocket.closed
            and self.connection_info is not None
            and self.connection_info.status == ConnectionStatus.CONNECTED
            and self.websocket_is_connected
        )

    async def connect(self, urls: List[str], ssid: str) -> bool:
        for url in urls:
            try:
                logger.info(f"Attempting to connect to {url}")
                ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                ssl_context.minimum_version = ssl.TLSVersion.TLSv1_3
                ssl_context.options = (ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1) | ssl.OP_NO_TLSv1_2
                ws_version = pkg_resources.get_distribution("websockets").version
                connect_kwargs = {
                    "uri": url,
                    "ssl": ssl_context,
                    "ping_interval": 25,
                    "ping_timeout": 5,
                    "close_timeout": CONNECTION_SETTINGS["close_timeout"],
                }
                if float(ws_version.split('.')[0]) >= 8:
                    connect_kwargs["extra_headers"] = DEFAULT_HEADERS
                else:
                    connect_kwargs["headers"] = DEFAULT_HEADERS
                    logger.warning("Using older websockets version; headers parameter may not work as expected")

                self.websocket = await asyncio.wait_for(
                    websockets.connect(**connect_kwargs),
                    timeout=CONNECTION_SETTINGS.get("handshake_timeout", 10.0)
                )
                region = self._extract_region_from_url(url)
                self.connection_info = ConnectionInfo(
                    url=url,
                    region=region,
                    status=ConnectionStatus.CONNECTED,
                    connected_at=datetime.now(),
                    reconnect_attempts=self._reconnect_attempts,
                )
                logger.info(f"Connected to {region} region successfully")
                self._running = True
                self.websocket_is_connected = True
                await self._send_handshake(ssid)
                await self._start_background_tasks()
                self._reconnect_attempts = 0
                self._last_auth_error = None
                return True

            except Exception as e:
                logger.warning(f"Failed to connect to {url}: {str(e)}")
                await error_monitor.record_error(
                    error_type="websocket_connect_failed",
                    severity=ErrorSeverity.HIGH,
                    category=ErrorCategory.CONNECTION,
                    message=f"Failed to connect to {url}: {str(e)}",
                    context={"url": url, "exception": str(e)}
                )
                if self.websocket:
                    try:
                        await self.websocket.close()
                    except Exception:
                        pass
                    self.websocket = None
                self.websocket_is_connected = False
                continue

        raise ConnectionError("Failed to connect to any WebSocket endpoint")

    async def disconnect(self) -> None:
        logger.info("Disconnecting from WebSocket")

        self._running = False

        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass

        if self._receiver_task:
            self._receiver_task.cancel()
            self._receiver_task = None

        if self.websocket:
            try:
                await self.websocket.close()
            except Exception:
                pass
            self.websocket = None

        if self.connection_info:
            self.connection_info = ConnectionInfo(
                url=self.connection_info.url,
                region=self.connection_info.region,
                status=ConnectionStatus.DISCONNECTED,
                connected_at=self.connection_info.connected_at,
                last_ping=self.connection_info.last_ping,
                reconnect_attempts=self.connection_info.reconnect_attempts,
            )

        self.websocket_is_connected = False
        self.ssl_mutual_exclusion = False
        self.ssl_mutual_exclusion_write = False

    async def send_message(self, message: str) -> None:
        """
        Queue outgoing Socket.IO frame and send in small batches to reduce syscalls.
        Uses a short delayed flush to coalesce bursts; messages are sent as separate frames
        to preserve Socket.IO framing (no concatenation).
        """
        if not self.websocket or self.websocket.closed:
            self.websocket_is_connected = False
            raise WebSocketError("WebSocket is not connected")

        try:
            while self.ssl_mutual_exclusion or self.ssl_mutual_exclusion_write:
                await asyncio.sleep(0.1)

            self.ssl_mutual_exclusion_write = True

            # Add to batch; if threshold reached, send immediately (each as its own frame)
            batch = await self._message_batcher.add_message(message)
            if batch:
                for msg in batch:
                    await self.websocket.send(msg)
                    logger.debug(f"Sent batched message: {msg}")

            # Ensure a short delayed flush for small bursts
            if not self._flush_task or self._flush_task.done():
                self._flush_task = asyncio.create_task(self._delayed_flush())

            self.ssl_mutual_exclusion_write = False

        except Exception as e:
            logger.error(f"Failed to send message: {str(e)}")
            self.ssl_mutual_exclusion_write = False
            self.websocket_is_connected = False
            await error_monitor.record_error(
                error_type="websocket_send_failed",
                severity=ErrorSeverity.MEDIUM,
                category=ErrorCategory.CONNECTION,
                message=f"Failed to send message: {str(e)}",
                context={"message": message[:100]}
            )
            raise WebSocketError(f"Failed to send message: {str(e)}")

    async def _delayed_flush(self) -> None:
        """Flush pending messages after a short delay to coalesce bursts."""
        await asyncio.sleep(0.04)
        leftover = await self._message_batcher.flush_batch()
        if not leftover:
            return
        try:
            for msg in leftover:
                await self.websocket.send(msg)
                logger.debug(f"Sent delayed-flush message: {msg}")
        except Exception as e:
            logger.error(f"Failed to flush batched messages: {str(e)}")
            await error_monitor.record_error(
                error_type="websocket_flush_failed",
                severity=ErrorSeverity.MEDIUM,
                category=ErrorCategory.CONNECTION,
                message=f"Failed to flush batched messages: {str(e)}",
                context={}
            )

    async def send_event(self, event: str, data: Any) -> None:
        payload = json.dumps([event, data], separators=(",", ":"))
        await self.send_message("42" + payload)

    async def send_message_optimized(self, message: str) -> None:
        """Compatibility: rate-limited wrapper that delegates to the batched send_message."""
        async with self._rate_limiter:
            await self.send_message(message)

    async def receive_messages(self) -> None:
        try:
            while self._running and self.websocket:
                try:
                    message = await asyncio.wait_for(self.websocket.recv(), timeout=CONNECTION_SETTINGS["receive_timeout"])
                    await self._process_message(message)

                except asyncio.TimeoutError:
                    logger.warning("Message receive timeout")
                    await error_monitor.record_error(
                        error_type="websocket_receive_timeout",
                        severity=ErrorSeverity.MEDIUM,
                        category=ErrorCategory.CONNECTION,
                        message="Timeout waiting for WebSocket message",
                        context={}
                    )
                    continue

                except (ConnectionClosedOK, ConnectionClosed):
                    logger.info("WebSocket connection closed normally (code 1005 or OK)")
                    await self._handle_disconnect()
                    break

                except ConnectionClosedError as e:
                    logger.warning(f"WebSocket connection closed with error: {str(e)}")
                    await self._handle_disconnect()
                    break

        except Exception as e:
            logger.error(f"Error in message receiving: {str(e)}")
            await self._handle_disconnect()
            await error_monitor.record_error(
                error_type="websocket_receive_error",
                severity=ErrorSeverity.HIGH,
                category=ErrorCategory.CONNECTION,
                message=f"Error in message receiving: {str(e)}",
                context={}
            )

    def on(self, event: str, callback: Callable) -> None:
        self._event_handlers.setdefault(event, []).append(callback)

    def add_event_handler(self, event: str, handler: Callable) -> None:
        self.on(event, handler)

    def remove_event_handler(self, event: str, handler: Callable) -> None:
        if event in self._event_handlers:
            try:
                self._event_handlers[event].remove(handler)
            except ValueError:
                pass

    async def _on_unknown_event(self, event_data: Dict[str, Any]) -> None:
        logger.debug(f"Ignoring unknown event: {event_data}")
        await error_monitor.record_error(
            error_type="unknown_event",
            severity=ErrorSeverity.LOW,
            category=ErrorCategory.DATA,
            message=f"Ignoring unknown event: {event_data}",
            context={"event_data": str(event_data)[:100]}
        )

    async def _recv_until_sioconnect(self, timeout: float = 7.0) -> bool:
        """
        Wait until we see the Socket.IO '40' connect frame.
        Parse Engine.IO open packet '0{...}' safely (strip the leading '0').
        Reply to Engine.IO ping '2' with pong '3' while waiting.
        ALSO: proactively send '40' after receiving the Engine.IO open frame.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            msg = await asyncio.wait_for(self.websocket.recv(), timeout=timeout)
            if not isinstance(msg, str):
                continue

            if msg.startswith("0"):
                try:
                    data = json.loads(msg[1:])
                    self.server_time = ServerTime(
                        server_timestamp=data.get("sid_timestamp", time.time()),
                        local_timestamp=time.time(),
                        offset=0.0
                    )
                    await self._emit_event("connected", {"sid": data.get("sid")})
                    self.websocket_is_connected = True
                    # Important: open default namespace
                    await self.send_message("40")
                except json.JSONDecodeError:
                    logger.error("Failed to parse initial message (Engine.IO open)")
                    await error_monitor.record_error(
                        error_type="websocket_initial_message_parse_error",
                        severity=ErrorSeverity.MEDIUM,
                        category=ErrorCategory.DATA,
                        message="Failed to parse initial Engine.IO open",
                        context={}
                    )
                    continue

            elif msg == "40":
                return True

            elif msg == "2":
                await self.send_message("3")

            else:
                continue

        return False

    async def _send_handshake(self, ssid: str) -> None:
        try:
            logger.debug("Waiting for Engine.IO open and Socket.IO connect ('40')...")
            ok = await self._recv_until_sioconnect(timeout=CONNECTION_SETTINGS.get("handshake_timeout", 10.0))
            if not ok:
                raise WebSocketError("Handshake timeout (no '40' connect frame)")

            await self.send_message(ssid)
            logger.debug(f"Sent authorization message: {ssid}")

            auth_response = await asyncio.wait_for(
                self.websocket.recv(),
                timeout=CONNECTION_SETTINGS.get("handshake_timeout", 10.0)
            )
            logger.debug(f"Received authentication response: {auth_response}")

            if auth_response.startswith('42["s_authorization"') or auth_response.startswith('451-["instruments/list"'):
                # success by s_authorization or by server pushing instruments/list header immediately
                if auth_response.startswith('451-["instruments/list"'):
                    logger.info("Authentication successful (via instruments/list header)")
                else:
                    logger.info("Authentication successful")
                    # request instruments list explicitly only when not already pushed
                    await self.send_message('451-["instruments/list",{"_placeholder":true,"num":0}]')
                    logger.debug("Sent instruments/list request")

                await self._emit_event("authenticated", {})

            else:
                # Try to parse the frame to check if it's actually an auth reject
                is_auth_reject = False
                if auth_response.startswith("42"):
