"""Connection Keep-Alive Manager for Quotex API"""
import asyncio
import time
import json
import base64
from collections import defaultdict
from typing import Dict, List, Any, Callable, Optional, Union
from websockets.exceptions import ConnectionClosed
from datetime import datetime, timedelta
from loguru import logger
from .monitoring import error_monitor, ErrorSeverity, ErrorCategory
from .constants import REGIONS, CONNECTION_SETTINGS

logger.remove()
log_filename = f"log-{time.strftime('%Y-%m-%d')}.txt"
logger.add(log_filename, level="INFO", encoding="utf-8", backtrace=True, diagnose=True)

class ConnectionKeepAlive:
    def __init__(self, ssid: str, is_demo: bool = True):
        self.ssid = ssid
        self.is_demo = is_demo
        self.is_connected = False
        self._websocket = None
        self._event_handlers: Dict[str, List[Callable]] = defaultdict(list)
        self._ping_task = None
        self._reconnect_task = None
        self._health_task = None
        self._assets_request_task = None
        self._last_assets_request = None
        self._connection_stats = {
            "last_ping_time": None,
            "last_pong_time": None,
            "total_reconnections": 0,
            "messages_sent": 0,
            "messages_received": 0,
        }
        try:
            from .websocket_client import AsyncWebSocketClient
            self._websocket_client_class = AsyncWebSocketClient
        except ImportError:
            logger.error("Failed to import AsyncWebSocketClient")
            raise ImportError("AsyncWebSocketClient module not available")

    def add_event_handler(self, event: str, handler: Callable):
        self._event_handlers[event].append(handler)

    async def _trigger_event_async(self, event: str, data: Dict[str, Any]) -> None:
        try:
            for handler in self._event_handlers.get(event, []):
                if asyncio.iscoroutinefunction(handler):
                    await handler(data)
                else:
                    handler(data)
        except Exception as e:
            logger.error(f"Error in {event} handler: {e}")
            await error_monitor.record_error(
                error_type=f"{event}_handler_error",
                severity=ErrorSeverity.MEDIUM,
                category=ErrorCategory.SYSTEM,
                message=f"Error in {event} handler: {str(e)}",
                context={"event": event}
            )

    async def _trigger_event(self, event: str, *args, **kwargs):
        if not self._websocket or not getattr(self._websocket, "is_connected", False):
            logger.warning(f"Skipping {event} handler: WebSocket is not connected")
            return
        for handler in self._event_handlers.get(event, []):
            try:
                data = kwargs.get("data", {})
                if asyncio.iscoroutinefunction(handler):
                    await self._handle_async_callback(handler, (data,), {})
                else:
                    handler(data)
            except Exception as e:
                logger.error(f"Error in {event} handler: {e}")
                await error_monitor.record_error(
                    error_type=f"{event}_handler_error",
                    severity=ErrorSeverity.MEDIUM,
                    category=ErrorCategory.SYSTEM,
                    message=f"Error in {event} handler: {str(e)}",
                    context={"event": event}
                )

    async def _handle_async_callback(self, callback, args, kwargs):
        try:
            await callback(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in async callback: {e}")
            await error_monitor.record_error(
                error_type="async_callback_error",
                severity=ErrorSeverity.MEDIUM,
                category=ErrorCategory.SYSTEM,
                message=f"Error in async callback: {str(e)}",
                context={}
            )

    async def _forward_balance_data(self, data):
        await self._trigger_event_async("balance_data", data=data)

    async def _forward_balance_updated(self, data):
        await self._trigger_event_async("balance_updated", data=data)

    async def _forward_authenticated(self, data):
        await self._trigger_event_async("authenticated", data=data)

    async def _forward_order_opened(self, data):
        await self._trigger_event_async("order_opened", data=data)

    async def _forward_order_closed(self, data):
        await self._trigger_event_async("order_closed", data=data)

    async def _forward_candles_received(self, data):
        await self._trigger_event_async("candles_received", data=data)

    async def _forward_assets_updated(self, data):
        await self._trigger_event_async("assets_list", data=data)

    async def _forward_stream_update(self, data):
        await self._trigger_event_async("stream_update", data=data)

    async def _forward_quote_stream(self, data):
        await self._trigger_event_async("quote_stream", data=data)

    async def _forward_error(self, data):
        await self._trigger_event_async("error", data=data)

    async def _forward_json_data(self, data):
        await self._trigger_event_async("json_data", data=data)

    async def connect_with_keep_alive(self, regions: Optional[List[str]] = None) -> bool:
        if not self._websocket:
            self._websocket = self._websocket_client_class()
            # wire forwarders ...
            self._websocket.add_event_handler("balance_data", self._forward_balance_data)
            self._websocket.add_event_handler("balance_updated", self._forward_balance_updated)
            self._websocket.add_event_handler("authenticated", self._forward_authenticated)
            self._websocket.add_event_handler("order_opened", self._forward_order_opened)
            self._websocket.add_event_handler("order_closed", self._forward_order_closed)
            self._websocket.add_event_handler("candles_received", self._forward_candles_received)
            self._websocket.add_event_handler("assets_list", self._forward_assets_updated)
            self._websocket.add_event_handler("stream_update", self._forward_stream_update)
            self._websocket.add_event_handler("quote_stream", self._forward_quote_stream)
            self._websocket.add_event_handler("error", self._forward_error)
            self._websocket.add_event_handler("json_data", self._forward_json_data)

        # Use given frame if already complete, else wrap session only
        if self.ssid.startswith('42["authorization",'):
            ssid_message = self.ssid
        else:
            ssid_message = (
                f'42["authorization", {{"session":"{self.ssid}","isDemo":{1 if self.is_demo else 0},"tournamentId":0}}]'
            )

        if not regions:
            all_regions = REGIONS.get_all_regions()
            if self.is_demo:
                demo_urls = REGIONS.get_demo_regions()
                regions = [name for name, url in all_regions.items() if url in demo_urls]
            else:
                regions = [name for name, url in all_regions.items() if "DEMO" not in name.upper()]

        for region_name in regions:
            region_url = REGIONS.get_region(region_name)
            if not region_url:
                continue
            try:
                logger.info(f"Trying to connect to {region_name} ({region_url})")
                ok = await asyncio.wait_for(
                    self._websocket.connect([region_url], ssid_message),
                    timeout=CONNECTION_SETTINGS["handshake_timeout"]
                )
                if ok:
                    logger.info(f"Connected to {region_name}")
                    self.is_connected = True
                    self._start_keep_alive_tasks()
                    await self._trigger_event_async("connected", data={"region": region_name, "url": region_url})
                    await self._websocket.send_message('451-["instruments/list",{"_placeholder":true,"num":0}]')
                    return True
            except asyncio.TimeoutError:
                logger.warning(f"Connection timeout to {region_name}")
                await error_monitor.record_error(
                    error_type="connection_timeout",
                    severity=ErrorSeverity.HIGH,
                    category=ErrorCategory.CONNECTION,
                    message=f"Connection timeout to {region_name}",
                    context={"region": region_name}
                )
            except Exception as e:
                logger.warning(f"Failed to connect to {region_name}: {e}")
                await error_monitor.record_error(
                    error_type="connection_failed",
                    severity=ErrorSeverity.HIGH,
                    category=ErrorCategory.CONNECTION,
                    message=f"Failed to connect to {region_name}: {str(e)}",
                    context={"region": region_name, "exception": str(e)}
                )
        return False

    def _start_keep_alive_tasks(self):
        logger.info("Starting keep-alive tasks")
        if self._ping_task:
            self._ping_task.cancel()
        self._ping_task = asyncio.create_task(self._ping_loop())
        if self._reconnect_task:
            self._reconnect_task.cancel()
        self._reconnect_task = asyncio.create_task(self._reconnection_monitor())
        if self._health_task:
            self._health_task.cancel()
        self._health_task = asyncio.create_task(self._health_monitor_loop())
        if self._assets_request_task:
            self._assets_request_task.cancel()
        self._assets_request_task = asyncio.create_task(self._assets_request_loop())

    async def _ping_loop(self):
        """Engine.IO heartbeat: send '2' periodically; upstream will reply '3' (PONG)."""
        while self.is_connected and self._websocket:
            try:
                await self._websocket.send_message('2')
                self._connection_stats["last_ping_time"] = time.time()
                self._connection_stats["messages_sent"] += 1
                logger.debug("Ping sent")
                await asyncio.sleep(25)
            except Exception as e:
                logger.warning(f"Ping failed: {e}")
                self.is_connected = False
                await error_monitor.record_error(
                    error_type="ping_failed",
                    severity=ErrorSeverity.MEDIUM,
                    category=ErrorCategory.CONNECTION,
                    message=f"Ping failed: {str(e)}",
                    context={}
                )

    async def _health_monitor_loop(self):
        logger.info("Starting health monitor...")
        while True:
            try:
                await asyncio.sleep(30)
                if not self.is_connected or not self._websocket:
                    continue
                if self._connection_stats["last_ping_time"]:
                    time_since_ping = (
                        datetime.now() - datetime.fromtimestamp(self._connection_stats["last_ping_time"])
                    ).total_seconds()
                    if time_since_ping > 60:
                        logger.warning("No ping response, connection may be dead")
                        self.is_connected = False
                        await error_monitor.record_error(
                            error_type="no_ping_response",
                            severity=ErrorSeverity.HIGH,
                            category=ErrorCategory.CONNECTION,
                            message="No ping response, connection may be dead",
                            context={"time_since_ping": time_since_ping}
                        )
                if not self._websocket.is_connected:
                    logger.warning("WebSocket is closed")
                    self.is_connected = False
                    await error_monitor.record_error(
                        error_type="websocket_closed",
                        severity=ErrorSeverity.HIGH,
                        category=ErrorCategory.CONNECTION,
                        message="WebSocket is closed",
                        context={}
                    )
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
                self.is_connected = False
                await error_monitor.record_error(
                    error_type="health_monitor_error",
                    severity=ErrorSeverity.MEDIUM,
                    category=ErrorCategory.SYSTEM,
                    message=f"Health monitor error: {str(e)}",
                    context={}
                )

    async def _assets_request_loop(self):
        """
        Periodically request instruments list to keep local cache fresh.
        Uses a conservative cadence and guards against spamming the server.
        """
        logger.info("Starting assets request loop...")
        while self.is_connected and self._websocket:
            try:
                now = datetime.now()
                if self._last_assets_request:
                    if (now - self._last_assets_request).total_seconds() < 60:
                        await asyncio.sleep(30)
                        continue
                await self._websocket.send_message('451-["instruments/list",{"_placeholder":true,"num":0}]')
                self._last_assets_request = now
                self._connection_stats["messages_sent"] += 1
                logger.debug("Assets data request sent")
                await asyncio.sleep(60)
            except Exception as e:
                logger.warning(f"Assets request failed: {e}")
                self.is_connected = False
                await error_monitor.record_error(
                    error_type="assets_request_failed",
                    severity=ErrorSeverity.MEDIUM,
                    category=ErrorCategory.DATA,
                    message=f"Assets request failed: {str(e)}",
                    context={}
                )

    async def _reconnection_monitor(self):
        """
        Watchdog: if connection drops, attempt a clean reconnect and resubscribe to instruments.
        """
        logger.info("Starting reconnection monitor...")
        while True:
            await asyncio.sleep(30)
            if not self.is_connected or not self._websocket or not self._websocket.is_connected:
                logger.info("Connection lost, reconnecting...")
                self.is_connected = False
                self._connection_stats["total_reconnections"] += 1
                try:
                    success = await self.connect_with_keep_alive()
                    if success:
                        logger.info("Reconnection successful")
                        await self._trigger_event_async("reconnected", data={})
                        await self._websocket.send_message('451-["instruments/list",{"_placeholder":true,"num":0}]')
                    else:
                        logger.error("Reconnection failed")
                        await error_monitor.record_error(
                            error_type="reconnection_failed",
                            severity=ErrorSeverity.HIGH,
                            category=ErrorCategory.CONNECTION,
                            message="Reconnection failed",
                            context={"attempt": self._connection_stats["total_reconnections"]}
                        )
                except Exception as e:
                    logger.error(f"Reconnection error: {e}")
                    await error_monitor.record_error(
                        error_type="reconnection_error",
                        severity=ErrorSeverity.HIGH,
                        category=ErrorCategory.CONNECTION,
                        message=f"Reconnection error: {str(e)}",
                        context={"attempt": self._connection_stats["total_reconnections"]}
                    )

    async def disconnect(self):
        logger.info("Disconnecting...")
        try:
            if self._ping_task:
                self._ping_task.cancel()
                try:
                    await self._ping_task
                except asyncio.CancelledError:
                    pass
            if self._reconnect_task:
                self._reconnect_task.cancel()
                try:
                    await self._reconnect_task
                except asyncio.CancelledError:
                    pass
            if self._health_task:
                self._health_task.cancel()
                try:
                    await self._health_task
                except asyncio.CancelledError:
                    pass
            if self._assets_request_task:
                self._assets_request_task.cancel()
                try:
                    await self._assets_request_task
                except asyncio.CancelledError:
                    pass
            if self._websocket:
                await self._websocket.disconnect()
            self.is_connected = False
            logger.info("Disconnected")
            await self._trigger_event_async("disconnected", data={})
        except Exception as e:
            logger.error(f"Error during disconnection: {e}")
            await error_monitor.record_error(
                error_type="disconnect_error",
                severity=ErrorSeverity.MEDIUM,
                category=ErrorCategory.CONNECTION,
                message=f"Error during disconnection: {str(e)}",
                context={}
            )

    async def send_message(self, message):
        if not self.is_connected or not self._websocket:
            raise ConnectionError("Not connected")
        try:
            await self._websocket.send_message(message)
            self._connection_stats["messages_sent"] += 1
            logger.debug(f"Sent message: {message[:100]}...")
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            self.is_connected = False
            await error_monitor.record_error(
                error_type="send_message_failed",
                severity=ErrorSeverity.MEDIUM,
                category=ErrorCategory.CONNECTION,
                message=f"Failed to send message: {str(e)}",
                context={"message": message[:100]}
            )
            raise ConnectionError(f"Failed to send message: {e}")

    async def on_message(self, message: Union[str, bytes]):
        """
        Old-compat + New-compat Engine.IO / Socket.IO message handler.

        Old library behaviour preserved:
          - Send 'tick' at second==0 and after processing frames (best-effort).
          - Detect 'authorization/reject' and 's_authorization'.
          - Handle two-part headers '451-'/'51-' followed by a binary JSON frame starting with '\\x04'.
          - Interpret payloads by keys (liveBalance/demoBalance, index, id, ticket, deals ...).

        New library behaviour added:
          - Reply '3' to server PING '2'; just record server PONG '3'.
          - Decode 'BFtb' base64-wrapped frames when present.
          - Route topics to unified forwarders via _trigger_event_async(...).
        """
        # Old-style periodic tick: send when time.second == 0 (best-effort)
        try:
            if self._websocket and getattr(self._websocket, "is_connected", False):
                if int(time.time()) % 60 == 0:
                    try:
                        await self._websocket.send_message('42["tick"]')
                    except Exception:
                        pass
        except Exception:
            pass

        # Lazy init for old-style header bookkeeping
        if not hasattr(self, "_pending_binary_event"):
            self._pending_binary_event = None
        if not hasattr(self, "_temp_status"):
            self._temp_status = ""

        try:
            self._connection_stats["messages_received"] += 1

            # Bytes → UTF-8
            if isinstance(message, (bytes, bytearray)):
              
