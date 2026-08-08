"""Professional Async Quotex API Client – Unified & Cleaned."""
import asyncio
import os
import json
import time
import requests
import uuid
import base64
from typing import Dict, List, Any, Callable, Optional, Union, Tuple
from datetime import datetime
from collections import defaultdict, deque
import pandas as pd
from loguru import logger

from .monitoring import error_monitor, health_checker, ErrorCategory, ErrorSeverity
from .websocket_client import AsyncWebSocketClient
from .models import Balance, Candle, Order, OrderResult, OrderStatus, OrderDirection, ConnectionStatus, ServerTime
from .constants import ASSETS, REGIONS, TIMEFRAMES, API_LIMITS, CONNECTION_SETTINGS
from .exceptions import QuotexError, ConnectionError, AuthenticationError, OrderError, InvalidParameterError, WebSocketError, Base64DecodeError
from .utils import sanitize_symbol, format_session_id, retry_async, candles_to_dataframe
from .login import get_ssid
from .config import Config
from .connection_keep_alive import ConnectionKeepAlive

# Logging
logger.remove()
log_filename = f"log-{time.strftime('%Y-%m-%d')}.txt"
logger.add(log_filename, level="INFO", encoding="utf-8", backtrace=True, diagnose=True)

# Fast path: ultra-low-latency candle store
class FastCandleStore:
    """Lock-free ring buffer per (asset, period) for near-instant candle reads."""
    def __init__(self, maxlen: int = 4096):
        self._bufs: Dict[Tuple[str, int], deque] = {}
        self._last_ts: Dict[Tuple[str, int], int] = {}
        self._maxlen = maxlen

    def _key(self, asset: str, period: int) -> Tuple[str, int]:
        return (asset, int(period))

    def add_many(self, asset: str, period: int, candles: List["Candle"]) -> None:
        if not candles:
            return
        k = self._key(asset, period)
        buf = self._bufs.get(k)
        if buf is None:
            buf = deque(maxlen=self._maxlen)
            self._bufs[k] = buf

        # De-duplicate while preserving order (oldest -> newest)
        last_ts = self._last_ts.get(k, 0)
        appended_any = False
        for c in sorted(candles, key=lambda x: x.timestamp):
            ts_i = int(c.timestamp.timestamp())
            if ts_i <= last_ts:
                continue
            buf.append(c)
            last_ts = ts_i
            appended_any = True

        if appended_any:
            self._last_ts[k] = last_ts

    def get_tail(self, asset: str, period: int, count: int) -> List["Candle"]:
        k = self._key(asset, period)
        buf = self._bufs.get(k)
        if not buf:
            return []
        if count <= 0 or count >= len(buf):
            return list(buf)
        return list(buf)[-count:]

    def size(self, asset: str, period: int) -> int:
        k = self._key(asset, period)
        buf = self._bufs.get(k)
        return len(buf) if buf else 0

class AsyncQuotexClient:
    """Professional async Quotex client with modern Python practices"""
    # region Initialization and Setup
    def __init__(self, ssid: str, is_demo: bool = True, uid: int = 0, region: Optional[str] = None,
                 is_fast_history: bool = True, persistent_connection: bool = False, auto_reconnect: bool = True,
                 enable_logging: bool = True, max_reconnect_attempts: int = CONNECTION_SETTINGS["max_reconnect_attempts"]):
        from .config import Config

        self.raw_ssid = ssid
        self.is_demo = is_demo
        self.preferred_region = region
        self.uid = uid
        self.is_fast_history = is_fast_history
        self.persistent_connection = persistent_connection
        self.auto_reconnect = auto_reconnect
        self.enable_logging = enable_logging
        self.max_reconnect_attempts = max_reconnect_attempts
        self._config = Config()
        self._reconnect_attempts = 0
        self.balance_id: Optional[int] = None
        self.websocket_is_connected: bool = False
        self.ssl_mutual_exclusion: bool = False
        self.ssl_mutual_exclusion_write: bool = False
        if not enable_logging:
            logger.remove()
            logger.add(lambda msg: None, level="CRITICAL")
        self._original_demo = None
        if ssid.startswith('42["authorization",'):
            self._parse_complete_ssid(ssid)
        else:
            self.session_id = ssid
            self._complete_ssid = None
        self._websocket = AsyncWebSocketClient()
        # --- runtime state ---
        self._balance: Optional[Balance] = None
        self._orders: Dict[str, OrderResult] = {}
        self._active_orders: Dict[str, OrderResult] = {}          # key = requestId
        self._order_results: Dict[str, OrderResult] = {}          # key = requestId
        self._pending_order_requests: Dict[str, Order] = {}       # key = requestId
        self._candles_cache: Dict[str, List[Candle]] = {}
        self._assets_data: Dict[str, Dict[str, Any]] = {}
        self._assets_requests: Dict[str, asyncio.Future] = {}
        self._candle_requests: Dict[str, asyncio.Future] = {}
        self._balance_requests: Dict[str, asyncio.Future] = {}
        self._payout_data: Dict[str, float] = {}
        self._server_time: Optional[ServerTime] = None
        self._event_callbacks: Dict[str, List[Callable]] = defaultdict(list)
        # Mapping between requestId and server order id; plus reverse index
        self._request_id_to_server_id: Dict[str, str] = {}        # requestId -> server_order_id
        self._server_order_index: Dict[str, str] = {}             # server_order_id -> requestId
        self._setup_event_handlers()
        self._error_monitor = error_monitor
        self._health_checker = health_checker
        self._operation_metrics: Dict[str, List[float]] = defaultdict(list)
        self._last_health_check = time.time()
        self._last_assets_update = 0
        self._keep_alive_manager = None
        self._ping_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._assets_update_task: Optional[asyncio.Task] = None
        self._is_persistent = False
        self._connection_stats = {
            "total_connections": 0,
            "successful_connections": 0,
            "total_reconnects": 0,
            "last_ping_time": None,
            "messages_sent": 0,
            "messages_received": 0,
            "connection_start_time": None,
        }
        # Fast store for instant candle reads
        self._fast_store = FastCandleStore()

        logger.info(
            f"Initialized Quotex client (demo={is_demo}, persistent={persistent_connection}) with enhanced monitoring"
            if enable_logging else ""
        )

    def _setup_event_handlers(self):
        self._websocket.add_event_handler('authenticated', self._on_authenticated)
        self._websocket.add_event_handler('s_authorization', self._on_authenticated)
        self._websocket.add_event_handler('balance_updated', self._on_balance_updated)
        self._websocket.add_event_handler('balance_data', self._on_balance_data)
        self._websocket.add_event_handler('balance_list', self._on_balance_list)
        self._websocket.add_event_handler('settings_list', self._on_settings_list)
        self._websocket.add_event_handler('orders_opened_list', self._on_orders_opened_list)
        self._websocket.add_event_handler('orders_closed_list', self._on_orders_closed_list)
        self._websocket.add_event_handler('order_opened', self._on_order_opened)
        self._websocket.add_event_handler('order_closed', self._on_order_closed)
        self._websocket.add_event_handler('drawing_load', self._on_drawing_load)
        self._websocket.add_event_handler('stream_update', self._on_stream_update)
        self._websocket.add_event_handler('candles_received', self._on_candles_received)
        self._websocket.add_event_handler('assets_list', self._on_assets_updated)
        self._websocket.add_event_handler('quote_stream', self._on_quote_stream)
        self._websocket.add_event_handler('depth_change', self._on_depth_change)
        self._websocket.add_event_handler('error', self._on_error)
        self._websocket.add_event_handler('json_data', self._on_json_data)
        self._websocket.add_event_handler('unknown_event', self._on_unknown_event)
        self._websocket.add_event_handler('auth_error', self._on_auth_error)
    # endregion

    # region Connection Management
    async def connect(self, regions: Optional[List[str]] = None, persistent: bool = None) -> bool:
        logger.info("Connecting to Quotex...")

        if persistent is not None:
            self.persistent_connection = persistent
        try:
            self.ssl_mutual_exclusion = False
            self.ssl_mutual_exclusion_write = False
            self.websocket_is_connected = False

            if self.persistent_connection:
                return await self._start_persistent_connection(regions)
            else:
                return await self._start_regular_connection(regions)
        except Exception as e:
            logger.error(f"Connection failed: {str(e)}")
            await self._error_monitor.record_error(
                error_type="connection_failed",
                severity=ErrorSeverity.HIGH,
                category=ErrorCategory.CONNECTION,
                message=f"Connection failed: {str(e)}",
                context={"exception": str(e)}
            )
            return False

    async def _start_regular_connection(self, regions: Optional[List[str]] = None) -> bool:
        logger.info("Starting regular connection...")

        if not regions:
            if self.is_demo:
                demo_urls = REGIONS.get_demo_regions()
                regions = [name for name, url in REGIONS.get_all_regions().items() if url in demo_urls]
                logger.info(f"Demo mode: Using demo regions: {regions}")
            else:
                regions = [name for name, url in REGIONS.get_all_regions().items() if "DEMO" not in name.upper()]
                logger.info(f"Live mode: Using non-demo regions: {regions}")

        self._connection_stats["total_connections"] += 1
        self._connection_stats["connection_start_time"] = time.time()

        for region in regions:
            if self._reconnect_attempts >= self.max_reconnect_attempts:
                logger.error(f"Max reconnect attempts ({self.max_reconnect_attempts}) reached. Aborting connection.")
                raise ConnectionError(f"Failed to connect after {self.max_reconnect_attempts} attempts")
            try:
                region_url = REGIONS.get_region(region)
                if not region_url:
                    logger.warning(f"No URL found for region {region}")
                    continue

                import socket
                host = region_url.split("//")[1].split("/")[0]
                socket.getaddrinfo(host, 443)  # DNS resolution check

                urls = [region_url]
                logger.info(f"Trying region: {region} with URL: {region_url}")

                ssid_message = self._format_session_message()
                success = await self._websocket.connect(urls, ssid_message)
                if success:
                    logger.info(f"Connected to region: {region}")
                    await self._wait_for_authentication(timeout=30.0)
                    await self._initialize_data()
                    await self._start_keep_alive_tasks()

                    self._connection_stats["successful_connections"] += 1
                    self.websocket_is_connected = True
                    logger.info("Successfully connected and authenticated")

                    self._reconnect_attempts = 0
                    return True

            except socket.gaierror as e:
                logger.warning(f"DNS resolution failed for {region_url}: {str(e)}")
                await self._error_monitor.record_error(
                    error_type="dns_resolution_failed",
                    severity=ErrorSeverity.HIGH,
                    category=ErrorCategory.CONNECTION,
                    message=f"DNS resolution failed for {region_url}: {str(e)}",
                    context={"region": region, "url": region_url}
                )
                self._reconnect_attempts += 1
                continue

            except Exception as e:
                logger.warning(f"Failed to connect to region {region}: {str(e)}")
                self._reconnect_attempts += 1
                await self._error_monitor.record_error(
                    error_type="connection_failed",
                    severity=ErrorSeverity.HIGH,
                    category=ErrorCategory.CONNECTION,
                    message=f"Failed to connect to {region}: {str(e)}",
                    context={"region": region, "exception": str(e)}
                )

                delay = min(
                    CONNECTION_SETTINGS["reconnect_initial_delay"] * (CONNECTION_SETTINGS["reconnect_factor"] ** self._reconnect_attempts),
                    CONNECTION_SETTINGS["reconnect_max_delay"]
                )
                logger.info(f"Waiting {delay:.2f} seconds before next reconnect attempt")
                await asyncio.sleep(delay)
                continue

        raise ConnectionError(f"Failed to connect to any region after {self._reconnect_attempts} attempts")

    async def _start_persistent_connection(self, regions: Optional[List[str]] = None) -> bool:
        logger.info("Starting persistent connection with automatic keep-alive...")
        from .connection_keep_alive import ConnectionKeepAlive
        complete_ssid = self.raw_ssid
        self._keep_alive_manager = ConnectionKeepAlive(complete_ssid, self.is_demo)
        self._keep_alive_manager.add_event_handler('connected', self._on_keep_alive_connected)
        self._keep_alive_manager.add_event_handler('reconnected', self._on_keep_alive_reconnected)
        self._keep_alive_manager.add_event_handler('message_received', self._on_keep_alive_message)
        self._keep_alive_manager.add_event_handler('balance_data', self._on_balance_data)
        self._keep_alive_manager.add_event_handler('balance_updated', self._on_balance_updated)
        self._keep_alive_manager.add_event_handler('authenticated', self._on_authenticated)
        self._keep_alive_manager.add_event_handler('order_opened', self._on_order_opened)
        self._keep_alive_manager.add_event_handler('order_closed', self._on_order_closed)
        self._keep_alive_manager.add_event_handler('candles_received', self._on_candles_received)
        self._keep_alive_manager.add_event_handler('assets_list', self._on_assets_updated)
        self._keep_alive_manager.add_event_handler('quote_stream', self._on_quote_stream)
        self._keep_alive_manager.add_event_handler('depth_change', self._on_depth_change)
        self._keep_alive_manager.add_event_handler('error', self._on_error)
        self._keep_alive_manager.add_event_handler('auth_error', self._on_auth_error)
        success = await self._keep_alive_manager.connect_with_keep_alive(regions)
        if success:
            self._is_persistent = True
            self.websocket_is_connected = True
            logger.info("Persistent connection established successfully")
            return True
        else:
            logger.error("Failed to establish persistent connection")
            return False

    async def disconnect(self) -> None:
        logger.info("Disconnecting from Quotex...")

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

        if self._assets_update_task:
            self._assets_update_task.cancel()
            try:
                await self._assets_update_task
            except asyncio.CancelledError:
                pass

        if self._is_persistent and self._keep_alive_manager:
            await self._keep_alive_manager.disconnect()
        else:
            await self._websocket.disconnect()

        self._is_persistent = False
        self.websocket_is_connected = False

        self._balance = None
        self._active_orders.clear()
        self._order_results.clear()
        self._pending_order_requests.clear()

        logger.info("Disconnected successfully")

    async def _start_keep_alive_tasks(self):
        logger.info("Starting keep-alive tasks for regular connection...")

        self._ping_task = asyncio.create_task(self._ping_loop())
        if self.auto_reconnect:
            self._reconnect_task = asyncio.create_task(self._reconnection_monitor())
        self._assets_update_task = asyncio.create_task(self._update_raw_assets())

    async def _ping_loop(self):
        while self.is_connected and not self._is_persistent:
            try:
                await self._websocket.send_message('2')
                self._connection_stats["last_ping_time"] = time.time()
                await asyncio.sleep(25)
            except Exception as e:
                logger.warning(f"Ping failed: {str(e)}")
                await self._error_monitor.record_error(
                    error_type="ping_failed",
                    severity=ErrorSeverity.MEDIUM,
                    category=ErrorCategory.CONNECTION,
                    message=f"Ping failed: {str(e)}",
                    context={}
                )
                break

    async def _reconnection_monitor(self):
        while not self._is_persistent:
            await asyncio.sleep(1)
            if not self.is_connected:
                logger.info("Connection lost, attempting reconnection...")
                self._connection_stats["total_reconnects"] += 1
                try:
                    await self._start_regular_connection()
                    self.websocket_is_connected = True
                    logger.info("Reconnection successful")
                except Exception as e:
                    logger.error(f"Reconnection error: {str(e)}")
                    delay = min(
                        CONNECTION_SETTINGS["reconnect_initial_delay"] * (CONNECTION_SETTINGS["reconnect_factor"] ** self._connection_stats["total_reconnects"]),
                        CONNECTION_SETTINGS["reconnect_max_delay"]
                    )
                    logger.info(f"Waiting {delay:.2f} seconds before next reconnect attempt")
                    await asyncio.sleep(delay)
                    await self._error_monitor.record_error(
                        error_type="reconnection_failed",
                        severity=ErrorSeverity.HIGH,
                        category=ErrorCategory.CONNECTION,
                        message=f"Reconnection error: {str(e)}",
                        context={"attempt": self._connection_stats["total_reconnects"]}
                    )

    @property
    def is_connected(self) -> bool:
        if self._is_persistent and self._keep_alive_manager:
            return self._keep_alive_manager.is_connected
        else:
            return self._websocket.is_connected and self.websocket_is_connected

    @property
    def connection_info(self):
        if self._is_persistent and self._keep_alive_manager:
            return self._keep_alive_manager.connection_info
        else:
            return self._websocket.connection_info

    async def send_message(self, message: str) -> bool:
        try:
            while self.ssl_mutual_exclusion or self.ssl_mutual_exclusion_write:
                await asyncio.sleep(0.1)
            self.ssl_mutual_exclusion_write = True

            if self._is_persistent and self._keep_alive_manager:
                success = await self._keep_alive_manager.send_message(message)
            else:
                await self._websocket.send_message(message)
                success = True

 
