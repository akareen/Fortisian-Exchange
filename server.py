"""
WebSocket API Server for the Trading Exchange.

Features:
- WebSocket-only trading (no REST API for orders)
- Session-based authentication with secure cookies
- Aggressive rate limiting (token bucket)
- Anomaly detection for bot behavior
- Comprehensive audit logging
- Admin controls

Anti-Gaming Measures:
1. No documented REST API - WebSocket only
2. Rate limiting: 2 orders/sec, 30/min per user
3. Timing anomaly detection
4. Session fingerprinting
5. Origin validation
"""

from __future__ import annotations

import asyncio
import json
import time
import secrets
import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, UTC, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Callable
from collections import deque
import statistics

from aiohttp import web, WSMsgType
import aiohttp_session
from aiohttp_session import setup as setup_session
from aiohttp_session.cookie_storage import EncryptedCookieStorage
from cryptography.fernet import Fernet

from matching_engine import (
    Exchange, ExchangeConfig, Side, TimeInForce, MarketStatus,
    EngineEvent, OrderAccepted, OrderRejected, OrderCancelled,
    OrderFilled, OrderExpired, TradeExecuted, BookSnapshot, BookDelta,
    MarketStatusChanged, UserId, filter_events_for_user,
    is_public_event, is_private_event,
)

from book_cache import BookCacheManager, BookCacheConfig


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ServerConfig:
    """Server configuration."""
    host: str = "0.0.0.0"
    port: int = 8080
    
    # Session - Fernet key must be 32 url-safe base64-encoded bytes
    session_secret: bytes = field(default_factory=Fernet.generate_key)
    session_max_age: int = 86400  # 24 hours
    
    # Rate limiting
    rate_limit_orders_per_second: float = 2.0
    rate_limit_orders_per_minute: int = 30
    rate_limit_burst: int = 5
    
    # Anomaly detection
    anomaly_window_size: int = 20  # Number of requests to analyze
    anomaly_timing_threshold: float = 0.05  # Suspiciously consistent timing (50ms std dev)
    
    # Security
    allowed_origins: list[str] = field(default_factory=lambda: ["*"])
    require_origin: bool = False
    
    # Admin
    admin_token: str = field(default_factory=lambda: secrets.token_urlsafe(32))


# ─────────────────────────────────────────────────────────────────────────────
# Rate Limiter (Token Bucket)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TokenBucket:
    """Token bucket rate limiter."""
    capacity: float
    refill_rate: float  # tokens per second
    tokens: float = field(default=None)
    last_update: float = field(default_factory=time.monotonic)
    
    def __post_init__(self):
        if self.tokens is None:
            self.tokens = self.capacity
    
    def consume(self, tokens: float = 1.0) -> bool:
        """Try to consume tokens. Returns True if allowed."""
        now = time.monotonic()
        elapsed = now - self.last_update
        self.last_update = now
        
        # Refill tokens
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False
    
    @property
    def tokens_available(self) -> float:
        """Get current token count (without consuming)."""
        now = time.monotonic()
        elapsed = now - self.last_update
        return min(self.capacity, self.tokens + elapsed * self.refill_rate)


@dataclass
class RateLimiter:
    """Per-user rate limiter with multiple windows."""
    user_id: str
    
    # Token bucket for burst control
    bucket: TokenBucket = field(default=None)
    
    # Sliding window for per-minute limit
    minute_window: deque = field(default_factory=deque)
    minute_limit: int = 30
    
    # Request timestamps for anomaly detection
    request_times: deque = field(default_factory=deque)
    window_size: int = 20
    
    def __post_init__(self):
        if self.bucket is None:
            self.bucket = TokenBucket(capacity=5.0, refill_rate=2.0)
    
    def check_and_consume(self) -> tuple[bool, str]:
        """
        Check if request is allowed and consume quota.
        Returns (allowed, reason).
        """
        now = time.monotonic()
        
        # Check token bucket (burst/second limit)
        if not self.bucket.consume():
            return False, "Rate limit exceeded (too fast)"
        
        # Check minute window
        cutoff = now - 60
        while self.minute_window and self.minute_window[0] < cutoff:
            self.minute_window.popleft()
        
        if len(self.minute_window) >= self.minute_limit:
            return False, "Rate limit exceeded (per-minute limit)"
        
        self.minute_window.append(now)
        
        # Record for anomaly detection
        self.request_times.append(now)
        if len(self.request_times) > self.window_size:
            self.request_times.popleft()
        
        return True, ""
    
    def get_timing_stats(self) -> dict:
        """Get timing statistics for anomaly detection."""
        if len(self.request_times) < 3:
            return {"intervals": [], "std_dev": None, "mean": None}
        
        times = list(self.request_times)
        intervals = [times[i+1] - times[i] for i in range(len(times) - 1)]
        
        return {
            "intervals": intervals,
            "std_dev": statistics.stdev(intervals) if len(intervals) > 1 else None,
            "mean": statistics.mean(intervals) if intervals else None,
        }
    
    def is_suspicious(self, threshold: float = 0.05) -> bool:
        """Check if timing pattern is suspiciously consistent (bot-like)."""
        stats = self.get_timing_stats()
        if stats["std_dev"] is None:
            return False
        
        # Very low standard deviation suggests automated trading
        # Human traders have irregular timing
        return stats["std_dev"] < threshold and len(self.request_times) >= 10


# ─────────────────────────────────────────────────────────────────────────────
# Session & User Management
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class UserSession:
    """Active user session."""
    user_id: str
    session_id: str
    created_at: datetime
    last_active: datetime
    ip_address: str
    user_agent: str
    
    # Connection state
    websocket: web.WebSocketResponse | None = None
    subscribed_markets: set = field(default_factory=set)
    
    # Rate limiting
    rate_limiter: RateLimiter = field(default=None)
    
    # Flags
    is_admin: bool = False
    is_banned: bool = False
    ban_reason: str | None = None
    
    # Anomaly tracking
    anomaly_flags: int = 0
    
    def __post_init__(self):
        if self.rate_limiter is None:
            self.rate_limiter = RateLimiter(user_id=self.user_id)
    
    def touch(self):
        """Update last active time."""
        self.last_active = datetime.now(UTC)


class SessionManager:
    """Manages user sessions."""
    
    def __init__(self):
        self._sessions: dict[str, UserSession] = {}  # session_id -> session
        self._user_sessions: dict[str, str] = {}  # user_id -> session_id
        self._banned_users: set[str] = set()
    
    def create_session(
        self,
        user_id: str,
        ip_address: str,
        user_agent: str,
        is_admin: bool = False,
    ) -> UserSession:
        """Create a new session for a user."""
        # Invalidate existing session if any
        if user_id in self._user_sessions:
            old_session_id = self._user_sessions[user_id]
            if old_session_id in self._sessions:
                del self._sessions[old_session_id]
        
        session_id = secrets.token_urlsafe(32)
        now = datetime.now(UTC)
        
        session = UserSession(
            user_id=user_id,
            session_id=session_id,
            created_at=now,
            last_active=now,
            ip_address=ip_address,
            user_agent=user_agent,
            is_admin=is_admin,
            is_banned=user_id in self._banned_users,
        )
        
        self._sessions[session_id] = session
        self._user_sessions[user_id] = session_id
        
        return session
    
    def get_session(self, session_id: str) -> UserSession | None:
        """Get session by ID."""
        return self._sessions.get(session_id)
    
    def get_user_session(self, user_id: str) -> UserSession | None:
        """Get session by user ID."""
        session_id = self._user_sessions.get(user_id)
        if session_id:
            return self._sessions.get(session_id)
        return None
    
    def invalidate_session(self, session_id: str) -> bool:
        """Invalidate a session."""
        session = self._sessions.get(session_id)
        if session:
            del self._sessions[session_id]
            if self._user_sessions.get(session.user_id) == session_id:
                del self._user_sessions[session.user_id]
            return True
        return False
    
    def ban_user(self, user_id: str, reason: str = "") -> None:
        """Ban a user."""
        self._banned_users.add(user_id)
        session = self.get_user_session(user_id)
        if session:
            session.is_banned = True
            session.ban_reason = reason
    
    def unban_user(self, user_id: str) -> None:
        """Unban a user."""
        self._banned_users.discard(user_id)
        session = self.get_user_session(user_id)
        if session:
            session.is_banned = False
            session.ban_reason = None
    
    def is_banned(self, user_id: str) -> bool:
        """Check if user is banned."""
        return user_id in self._banned_users
    
    def get_all_sessions(self) -> list[UserSession]:
        """Get all active sessions."""
        return list(self._sessions.values())
    
    def get_connected_users(self) -> list[str]:
        """Get list of connected user IDs."""
        return [s.user_id for s in self._sessions.values() if s.websocket is not None]


# ─────────────────────────────────────────────────────────────────────────────
# Audit Logger
# ─────────────────────────────────────────────────────────────────────────────

class AuditEventType(Enum):
    """Types of audit events."""
    LOGIN = "login"
    LOGOUT = "logout"
    ORDER_SUBMIT = "order_submit"
    ORDER_CANCEL = "order_cancel"
    RATE_LIMITED = "rate_limited"
    ANOMALY_DETECTED = "anomaly_detected"
    BANNED = "banned"
    ADMIN_ACTION = "admin_action"
    ERROR = "error"


@dataclass
class AuditEvent:
    """An audit log entry."""
    timestamp: datetime
    event_type: AuditEventType
    user_id: str | None
    ip_address: str | None
    user_agent: str | None
    details: dict
    
    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp.isoformat(),
            "event_type": self.event_type.value,
            "user_id": self.user_id,
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
            "details": self.details,
        }


class AuditLogger:
    """Logs security-relevant events."""
    
    def __init__(self, max_entries: int = 10000):
        self._events: deque[AuditEvent] = deque(maxlen=max_entries)
        self._logger = logging.getLogger("audit")
    
    def log(
        self,
        event_type: AuditEventType,
        user_id: str | None = None,
        ip_address: str | None = None,
        user_agent: str | None = None,
        **details,
    ) -> None:
        """Log an audit event."""
        event = AuditEvent(
            timestamp=datetime.now(UTC),
            event_type=event_type,
            user_id=user_id,
            ip_address=ip_address,
            user_agent=user_agent,
            details=details,
        )
        self._events.append(event)
        self._logger.info(f"{event_type.value}: user={user_id} ip={ip_address} {details}")
    
    def get_events(
        self,
        user_id: str | None = None,
        event_type: AuditEventType | None = None,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[AuditEvent]:
        """Query audit events."""
        events = list(self._events)
        
        if user_id:
            events = [e for e in events if e.user_id == user_id]
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        if since:
            events = [e for e in events if e.timestamp >= since]
        
        return events[-limit:]
    
    def get_suspicious_users(self) -> list[dict]:
        """Get users with anomaly flags."""
        anomalies = [e for e in self._events if e.event_type == AuditEventType.ANOMALY_DETECTED]
        
        user_counts = {}
        for event in anomalies:
            if event.user_id:
                user_counts[event.user_id] = user_counts.get(event.user_id, 0) + 1
        
        return [
            {"user_id": uid, "anomaly_count": count}
            for uid, count in sorted(user_counts.items(), key=lambda x: -x[1])
        ]


# ─────────────────────────────────────────────────────────────────────────────
# Message Protocol
# ─────────────────────────────────────────────────────────────────────────────

class MessageType(Enum):
    """WebSocket message types."""
    # Client -> Server
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    ORDER_SUBMIT = "order_submit"
    ORDER_CANCEL = "order_cancel"
    ORDER_CANCEL_ALL = "order_cancel_all"
    GET_ORDERS = "get_orders"
    GET_POSITION = "get_position"
    GET_BOOK = "get_book"
    PING = "ping"
    
    # Server -> Client
    SUBSCRIBED = "subscribed"
    UNSUBSCRIBED = "unsubscribed"
    ORDER_ACCEPTED = "order_accepted"
    ORDER_REJECTED = "order_rejected"
    ORDER_CANCELLED = "order_cancelled"
    ORDER_FILLED = "order_filled"
    ORDER_EXPIRED = "order_expired"
    TRADE = "trade"
    BOOK_SNAPSHOT = "book_snapshot"
    BOOK_DELTA = "book_delta"
    MARKET_STATUS = "market_status"
    ORDERS = "orders"
    POSITION = "position"
    PONG = "pong"
    ERROR = "error"


def make_response(msg_type: MessageType, data: dict | None = None, request_id: str | None = None) -> str:
    """Create a JSON response message."""
    msg = {"type": msg_type.value}
    if data:
        msg["data"] = data
    if request_id:
        msg["request_id"] = request_id
    return json.dumps(msg)


def make_error(message: str, code: str = "error", request_id: str | None = None) -> str:
    """Create an error response."""
    return make_response(MessageType.ERROR, {"message": message, "code": code}, request_id)


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket Server
# ─────────────────────────────────────────────────────────────────────────────

class TradingServer:
    """WebSocket trading server."""
    
    def __init__(
        self,
        exchange: Exchange,
        config: ServerConfig | None = None,
        book_cache_config: BookCacheConfig | None = None,
    ):
        self.exchange = exchange
        self.config = config or ServerConfig()
        self.sessions = SessionManager()
        self.audit = AuditLogger()
        
        # Book cache for fast reads without impacting matching engine
        self.book_cache = BookCacheManager(book_cache_config or BookCacheConfig())
        
        # Initialize cache from current exchange state
        self.book_cache.initialize_from_exchange(exchange)
        
        # Set up book update callback to push to clients
        self.book_cache.set_update_callback(self._on_book_cache_update)
        
        # WebSocket connections by user
        self._connections: dict[str, web.WebSocketResponse] = {}
        
        # Market subscriptions
        self._market_subscribers: dict[str, set[str]] = {}  # market_id -> set of user_ids
        
        # Set up exchange event handler
        self.exchange._on_event = self._on_exchange_event
        
        # App
        self.app = web.Application()
        self._setup_routes()
        self._setup_session()
    
    def _setup_routes(self):
        """Set up HTTP routes."""
        self.app.router.add_get("/ws", self._websocket_handler)
        self.app.router.add_post("/auth/login", self._login_handler)
        self.app.router.add_post("/auth/logout", self._logout_handler)
        self.app.router.add_get("/health", self._health_handler)
        
        # Admin routes
        self.app.router.add_get("/admin/stats", self._admin_stats_handler)
        self.app.router.add_get("/admin/audit", self._admin_audit_handler)
        self.app.router.add_post("/admin/ban", self._admin_ban_handler)
        self.app.router.add_post("/admin/unban", self._admin_unban_handler)
        self.app.router.add_post("/admin/market/{action}", self._admin_market_handler)
    
    def _setup_session(self):
        """Set up session middleware."""
        # aiohttp_session expects string, not bytes
        secret = self.config.session_secret
        if isinstance(secret, bytes):
            secret = secret.decode('utf-8')
        
        storage = EncryptedCookieStorage(
            secret,
            cookie_name="session",
            max_age=self.config.session_max_age,
            httponly=True,
            secure=False,  # Set True in production with HTTPS
            samesite="Strict",
        )
        setup_session(self.app, storage)
    
    # ─────────────────────────────────────────────────────────────────────────
    # Authentication Handlers
    # ─────────────────────────────────────────────────────────────────────────
    
    async def _login_handler(self, request: web.Request) -> web.Response:
        """Handle login requests."""
        try:
            data = await request.json()
        except json.JSONDecodeError:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        
        user_id = data.get("user_id", "").strip()
        password = data.get("password", "")  # In production, validate against DB
        
        if not user_id:
            return web.json_response({"error": "user_id required"}, status=400)
        
        # Validate user_id format
        if not user_id.replace("_", "").replace("-", "").isalnum():
            return web.json_response({"error": "Invalid user_id format"}, status=400)
        
        if len(user_id) > 32:
            return web.json_response({"error": "user_id too long"}, status=400)
        
        # Check if banned
        if self.sessions.is_banned(user_id):
            self.audit.log(
                AuditEventType.LOGIN,
                user_id=user_id,
                ip_address=request.remote,
                user_agent=request.headers.get("User-Agent"),
                success=False,
                reason="banned",
            )
            return web.json_response({"error": "User is banned"}, status=403)
        
        # Create session
        ip_address = request.remote or "unknown"
        user_agent = request.headers.get("User-Agent", "unknown")
        
        # Check if admin (simple token check - use proper auth in production)
        is_admin = data.get("admin_token") == self.config.admin_token
        
        session = self.sessions.create_session(
            user_id=user_id,
            ip_address=ip_address,
            user_agent=user_agent,
            is_admin=is_admin,
        )
        
        # Store session ID in cookie session
        aio_session = await aiohttp_session.get_session(request)
        aio_session["session_id"] = session.session_id
        aio_session["user_id"] = user_id
        
        self.audit.log(
            AuditEventType.LOGIN,
            user_id=user_id,
            ip_address=ip_address,
            user_agent=user_agent,
            success=True,
            is_admin=is_admin,
        )
        
        return web.json_response({
            "success": True,
            "user_id": user_id,
            "session_id": session.session_id,
            "is_admin": is_admin,
        })
    
    async def _logout_handler(self, request: web.Request) -> web.Response:
        """Handle logout requests."""
        aio_session = await aiohttp_session.get_session(request)
        session_id = aio_session.get("session_id")
        user_id = aio_session.get("user_id")
        
        if session_id:
            self.sessions.invalidate_session(session_id)
            aio_session.invalidate()
            
            self.audit.log(
                AuditEventType.LOGOUT,
                user_id=user_id,
                ip_address=request.remote,
            )
        
        return web.json_response({"success": True})
    
    async def _get_session(self, request: web.Request) -> UserSession | None:
        """Get current user session from request."""
        aio_session = await aiohttp_session.get_session(request)
        session_id = aio_session.get("session_id")
        if session_id:
            return self.sessions.get_session(session_id)
        return None
    
    # ─────────────────────────────────────────────────────────────────────────
    # WebSocket Handler
    # ─────────────────────────────────────────────────────────────────────────
    
    async def _websocket_handler(self, request: web.Request) -> web.WebSocketResponse:
        """Handle WebSocket connections."""
        # Check origin
        if self.config.require_origin:
            origin = request.headers.get("Origin", "")
            if "*" not in self.config.allowed_origins and origin not in self.config.allowed_origins:
                return web.Response(status=403, text="Forbidden origin")
        
        # Get session
        session = await self._get_session(request)
        if not session:
            return web.Response(status=401, text="Not authenticated")
        
        if session.is_banned:
            return web.Response(status=403, text="User is banned")
        
        # Set up WebSocket
        ws = web.WebSocketResponse(heartbeat=30)
        await ws.prepare(request)
        
        # Register connection
        session.websocket = ws
        session.touch()
        self._connections[session.user_id] = ws
        
        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    await self._handle_message(session, msg.data)
                elif msg.type == WSMsgType.ERROR:
                    self.audit.log(
                        AuditEventType.ERROR,
                        user_id=session.user_id,
                        error=str(ws.exception()),
                    )
        finally:
            # Clean up
            session.websocket = None
            if self._connections.get(session.user_id) is ws:
                del self._connections[session.user_id]
            
            # Remove from all subscriptions
            for market_id in list(session.subscribed_markets):
                self._unsubscribe_user(session.user_id, market_id)
        
        return ws
    
    async def _handle_message(self, session: UserSession, raw_data: str) -> None:
        """Handle incoming WebSocket message."""
        session.touch()
        
        try:
            data = json.loads(raw_data)
        except json.JSONDecodeError:
            await self._send(session, make_error("Invalid JSON"))
            return
        
        msg_type = data.get("type", "")
        request_id = data.get("request_id")
        payload = data.get("data", {})
        
        # Handle ping immediately (no rate limit)
        if msg_type == "ping":
            await self._send(session, make_response(MessageType.PONG, request_id=request_id))
            return
        
        # Rate limit trading operations
        if msg_type in ("order_submit", "order_cancel", "order_cancel_all"):
            allowed, reason = session.rate_limiter.check_and_consume()
            
            if not allowed:
                self.audit.log(
                    AuditEventType.RATE_LIMITED,
                    user_id=session.user_id,
                    ip_address=session.ip_address,
                    reason=reason,
                    message_type=msg_type,
                )
                await self._send(session, make_error(reason, "rate_limited", request_id))
                return
            
            # Check for anomaly
            if session.rate_limiter.is_suspicious(self.config.anomaly_timing_threshold):
                session.anomaly_flags += 1
                self.audit.log(
                    AuditEventType.ANOMALY_DETECTED,
                    user_id=session.user_id,
                    ip_address=session.ip_address,
                    stats=session.rate_limiter.get_timing_stats(),
                    flags=session.anomaly_flags,
                )
        
        # Route message
        handlers = {
            "subscribe": self._handle_subscribe,
            "unsubscribe": self._handle_unsubscribe,
            "order_submit": self._handle_order_submit,
            "order_cancel": self._handle_order_cancel,
            "order_cancel_all": self._handle_order_cancel_all,
            "get_orders": self._handle_get_orders,
            "get_position": self._handle_get_position,
            "get_book": self._handle_get_book,
            "get_level2": self._handle_get_level2,
            "get_bbo": self._handle_get_bbo,
        }
        
        handler = handlers.get(msg_type)
        if handler:
            await handler(session, payload, request_id)
        else:
            await self._send(session, make_error(f"Unknown message type: {msg_type}", request_id=request_id))
    
    async def _send(self, session: UserSession, message: str) -> None:
        """Send message to user's WebSocket."""
        if session.websocket and not session.websocket.closed:
            await session.websocket.send_str(message)
    
    async def _broadcast_to_market(self, market_id: str, message: str) -> None:
        """Broadcast message to all subscribers of a market."""
        subscribers = self._market_subscribers.get(market_id, set())
        for user_id in subscribers:
            ws = self._connections.get(user_id)
            if ws and not ws.closed:
                try:
                    await ws.send_str(message)
                except Exception:
                    pass
    
    async def _send_to_user(self, user_id: str, message: str) -> None:
        """Send message to a specific user."""
        ws = self._connections.get(user_id)
        if ws and not ws.closed:
            try:
                await ws.send_str(message)
            except Exception:
                pass
    
    # ─────────────────────────────────────────────────────────────────────────
    # Message Handlers
    # ─────────────────────────────────────────────────────────────────────────
    
    async def _handle_subscribe(self, session: UserSession, data: dict, request_id: str | None) -> None:
        """Handle market subscription."""
        market_id = data.get("market_id", "")
        
        market = self.exchange.get_market(market_id)
        if not market:
            await self._send(session, make_error(f"Market not found: {market_id}", request_id=request_id))
            return
        
        # Add to subscriptions
        session.subscribed_markets.add(market_id)
        if market_id not in self._market_subscribers:
            self._market_subscribers[market_id] = set()
        self._market_subscribers[market_id].add(session.user_id)
        
        # Send subscription confirmation
        await self._send(session, make_response(MessageType.SUBSCRIBED, {"market_id": market_id}, request_id))
        
        # Send current book snapshot from cache (no rate limit for initial subscribe)
        snapshot, _ = self.book_cache.get_book_snapshot(
            market_id=market_id,
            user_id=session.user_id,
            check_rate_limit=False,  # Initial subscribe is free
        )
        if snapshot:
            await self._send(session, make_response(MessageType.BOOK_SNAPSHOT, snapshot))
        
        # Send user's open orders
        orders = self.exchange.get_user_orders(market_id, session.user_id)
        await self._send(session, make_response(MessageType.ORDERS, {
            "market_id": market_id,
            "orders": [o.to_dict() for o in orders],
        }))
        
        # Send user's position
        position = self.exchange.get_user_position(market_id, session.user_id)
        await self._send(session, make_response(MessageType.POSITION, {
            "market_id": market_id,
            "position": position.to_dict(),
        }))
    
    async def _handle_unsubscribe(self, session: UserSession, data: dict, request_id: str | None) -> None:
        """Handle market unsubscription."""
        market_id = data.get("market_id", "")
        self._unsubscribe_user(session.user_id, market_id)
        session.subscribed_markets.discard(market_id)
        await self._send(session, make_response(MessageType.UNSUBSCRIBED, {"market_id": market_id}, request_id))
    
    def _unsubscribe_user(self, user_id: str, market_id: str) -> None:
        """Remove user from market subscription."""
        if market_id in self._market_subscribers:
            self._market_subscribers[market_id].discard(user_id)
    
    async def _handle_order_submit(self, session: UserSession, data: dict, request_id: str | None) -> None:
        """Handle order submission."""
        # Handle case where data is not a dict
        if not isinstance(data, dict):
            await self._send(session, make_error("Invalid order data format", request_id=request_id))
            return
        
        market_id = data.get("market_id", "")
        side = data.get("side", "")
        price = data.get("price")
        qty = data.get("qty")
        tif = data.get("time_in_force", "gtc")
        client_order_id = data.get("client_order_id")
        
        # Validate
        if not market_id:
            await self._send(session, make_error("market_id required", request_id=request_id))
            return
        
        # Check market exists
        if not self.exchange.get_market(market_id):
            await self._send(session, make_error(f"Market not found: {market_id}", request_id=request_id))
            return
        
        if side not in ("buy", "sell"):
            await self._send(session, make_error("side must be 'buy' or 'sell'", request_id=request_id))
            return
        
        try:
            price = Decimal(str(price))
            if price <= 0:
                raise ValueError("Price must be positive")
        except (InvalidOperation, TypeError, ValueError) as e:
            await self._send(session, make_error(f"Invalid price: {e}", request_id=request_id))
            return
        
        try:
            qty = int(qty)
            if qty <= 0:
                raise ValueError("Qty must be positive")
        except (ValueError, TypeError) as e:
            await self._send(session, make_error(f"Invalid qty: {e}", request_id=request_id))
            return
        
        # Log
        self.audit.log(
            AuditEventType.ORDER_SUBMIT,
            user_id=session.user_id,
            ip_address=session.ip_address,
            market_id=market_id,
            side=side,
            price=str(price),
            qty=qty,
            tif=tif,
        )
        
        # Submit order
        try:
            order = self.exchange.submit_order(
                market_id=market_id,
                user_id=session.user_id,
                side=side,
                price=price,
                qty=qty,
                time_in_force=tif,
                client_order_id=client_order_id,
            )
        except Exception as e:
            await self._send(session, make_error(f"Order submission failed: {e}", request_id=request_id))
        
        # Response is sent via exchange event handler
    
    async def _handle_order_cancel(self, session: UserSession, data: dict, request_id: str | None) -> None:
        """Handle order cancellation."""
        market_id = data.get("market_id", "")
        order_id = data.get("order_id", "")
        
        if not market_id or not order_id:
            await self._send(session, make_error("market_id and order_id required", request_id=request_id))
            return
        
        self.audit.log(
            AuditEventType.ORDER_CANCEL,
            user_id=session.user_id,
            ip_address=session.ip_address,
            market_id=market_id,
            order_id=order_id,
        )
        
        success = self.exchange.cancel_order(market_id, order_id, session.user_id)
        
        if not success:
            await self._send(session, make_error("Order not found or not owned by you", request_id=request_id))
    
    async def _handle_order_cancel_all(self, session: UserSession, data: dict, request_id: str | None) -> None:
        """Handle cancel all orders."""
        market_id = data.get("market_id")
        
        self.audit.log(
            AuditEventType.ORDER_CANCEL,
            user_id=session.user_id,
            ip_address=session.ip_address,
            market_id=market_id,
            cancel_all=True,
        )
        
        if market_id:
            count = self.exchange.cancel_all_orders(market_id, session.user_id)
        else:
            count = self.exchange.cancel_all_user_orders_all_markets(session.user_id)
        
        await self._send(session, make_response(
            MessageType.ORDER_CANCELLED,
            {"cancelled_count": count, "market_id": market_id},
            request_id,
        ))
    
    async def _handle_get_orders(self, session: UserSession, data: dict, request_id: str | None) -> None:
        """Handle get orders request."""
        market_id = data.get("market_id")
        
        if market_id:
            orders = self.exchange.get_user_orders(market_id, session.user_id)
            await self._send(session, make_response(MessageType.ORDERS, {
                "market_id": market_id,
                "orders": [o.to_dict() for o in orders],
            }, request_id))
        else:
            all_orders = self.exchange.get_all_user_orders(session.user_id)
            await self._send(session, make_response(MessageType.ORDERS, {
                "orders": {mid: [o.to_dict() for o in orders] for mid, orders in all_orders.items()},
            }, request_id))
    
    async def _handle_get_position(self, session: UserSession, data: dict, request_id: str | None) -> None:
        """Handle get position request."""
        market_id = data.get("market_id")
        
        if market_id:
            try:
                position = self.exchange.get_user_position(market_id, session.user_id)
                await self._send(session, make_response(MessageType.POSITION, {
                    "market_id": market_id,
                    "position": position.to_dict(),
                }, request_id))
            except ValueError as e:
                await self._send(session, make_error(str(e), request_id=request_id))
        else:
            positions = self.exchange.get_user_positions_all_markets(session.user_id)
            await self._send(session, make_response(MessageType.POSITION, {
                "positions": {mid: p.to_dict() for mid, p in positions.items()},
            }, request_id))
    
    async def _handle_get_book(self, session: UserSession, data: dict, request_id: str | None) -> None:
        """Handle get book request - uses cached data with rate limiting."""
        market_id = data.get("market_id", "")
        depth = data.get("depth", 50)  # Max depth to return
        
        if not market_id:
            await self._send(session, make_error("market_id required", request_id=request_id))
            return
        
        # Use book cache with rate limiting (separate from order rate limits)
        snapshot, error = self.book_cache.get_book_snapshot(
            market_id=market_id,
            user_id=session.user_id,
            check_rate_limit=True,
        )
        
        if error:
            # Rate limited or not found
            await self._send(session, make_error(error, "rate_limited", request_id))
            return
        
        await self._send(session, make_response(MessageType.BOOK_SNAPSHOT, snapshot, request_id))
    
    async def _handle_get_level2(self, session: UserSession, data: dict, request_id: str | None) -> None:
        """Handle get level2 book request - aggregated price levels."""
        market_id = data.get("market_id", "")
        depth = data.get("depth", 10)
        
        if not market_id:
            await self._send(session, make_error("market_id required", request_id=request_id))
            return
        
        level2, error = self.book_cache.get_level2(
            market_id=market_id,
            user_id=session.user_id,
            depth=min(depth, 50),
            check_rate_limit=True,
        )
        
        if error:
            await self._send(session, make_error(error, "rate_limited", request_id))
            return
        
        await self._send(session, make_response(MessageType.BOOK_SNAPSHOT, level2, request_id))
    
    async def _handle_get_bbo(self, session: UserSession, data: dict, request_id: str | None) -> None:
        """Handle get best bid/offer request - Level 1 data."""
        market_id = data.get("market_id", "")
        
        if not market_id:
            await self._send(session, make_error("market_id required", request_id=request_id))
            return
        
        bbo, error = self.book_cache.get_best_bid_ask(
            market_id=market_id,
            user_id=session.user_id,
            check_rate_limit=True,
        )
        
        if error:
            await self._send(session, make_error(error, "rate_limited", request_id))
            return
        
        await self._send(session, make_response("bbo", bbo, request_id))
    
    # ─────────────────────────────────────────────────────────────────────────
    # Exchange Event Handler
    # ─────────────────────────────────────────────────────────────────────────
    
    def _on_exchange_event(self, event: EngineEvent) -> None:
        """Handle events from the exchange."""
        # Update book cache (synchronous, fast)
        self.book_cache.on_event(event)
        
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._broadcast_event(event))
        except RuntimeError:
            # No running event loop - this is fine during testing/setup
            pass
    
    def _on_book_cache_update(self, market_id: str) -> None:
        """Handle book cache update - push to subscribers."""
        # This is called from book cache when book changes
        # We'll use the existing broadcast mechanism from _broadcast_event
        # The book updates are already pushed via BookSnapshot/BookDelta events
        pass
    
    async def _broadcast_event(self, event: EngineEvent) -> None:
        """Broadcast exchange event to relevant users."""
        # Determine market ID
        market_id = None
        if hasattr(event, 'market_id'):
            market_id = str(event.market_id)
        elif hasattr(event, 'trade'):
            market_id = str(event.trade.market_id)
        
        # Convert event to message
        if isinstance(event, OrderAccepted):
            msg = make_response(MessageType.ORDER_ACCEPTED, event.to_dict())
            await self._send_to_user(str(event.user_id), msg)
        
        elif isinstance(event, OrderRejected):
            msg = make_response(MessageType.ORDER_REJECTED, event.to_dict())
            await self._send_to_user(str(event.user_id), msg)
        
        elif isinstance(event, OrderCancelled):
            msg = make_response(MessageType.ORDER_CANCELLED, event.to_dict())
            await self._send_to_user(str(event.user_id), msg)
        
        elif isinstance(event, OrderExpired):
            msg = make_response(MessageType.ORDER_EXPIRED, {
                "order_id": str(event.order_id),
                "reason": event.reason,
                "expired_qty": event.expired_qty,
                "filled_qty": event.filled_qty,
            })
            await self._send_to_user(str(event.user_id), msg)
        
        elif isinstance(event, OrderFilled):
            msg = make_response(MessageType.ORDER_FILLED, event.to_dict())
            await self._send_to_user(str(event.user_id), msg)
        
        elif isinstance(event, TradeExecuted):
            # Broadcast trade to market subscribers (public info only)
            if market_id:
                msg = make_response(MessageType.TRADE, event.to_public_dict())
                await self._broadcast_to_market(market_id, msg)
        
        elif isinstance(event, BookSnapshot):
            if market_id:
                msg = make_response(MessageType.BOOK_SNAPSHOT, event.to_dict())
                await self._broadcast_to_market(market_id, msg)
        
        elif isinstance(event, BookDelta):
            if market_id:
                msg = make_response(MessageType.BOOK_DELTA, event.to_dict())
                await self._broadcast_to_market(market_id, msg)
        
        elif isinstance(event, MarketStatusChanged):
            if market_id:
                msg = make_response(MessageType.MARKET_STATUS, event.to_dict())
                await self._broadcast_to_market(market_id, msg)
    
    # ─────────────────────────────────────────────────────────────────────────
    # Admin Handlers
    # ─────────────────────────────────────────────────────────────────────────
    
    async def _require_admin(self, request: web.Request) -> UserSession | None:
        """Require admin authentication."""
        # Check header token
        token = request.headers.get("X-Admin-Token")
        if token == self.config.admin_token:
            return True
        
        # Check session
        session = await self._get_session(request)
        if session and session.is_admin:
            return session
        
        return None
    
    async def _admin_stats_handler(self, request: web.Request) -> web.Response:
        """Get exchange statistics."""
        if not await self._require_admin(request):
            return web.json_response({"error": "Unauthorized"}, status=401)
        
        stats = self.exchange.get_stats()
        stats["connected_users"] = len(self._connections)
        stats["active_sessions"] = len(self.sessions.get_all_sessions())
        stats["suspicious_users"] = self.audit.get_suspicious_users()
        stats["book_cache"] = self.book_cache.get_stats()
        
        return web.json_response(stats)
    
    async def _admin_audit_handler(self, request: web.Request) -> web.Response:
        """Get audit log."""
        if not await self._require_admin(request):
            return web.json_response({"error": "Unauthorized"}, status=401)
        
        user_id = request.query.get("user_id")
        event_type = request.query.get("event_type")
        limit = int(request.query.get("limit", 100))
        
        et = AuditEventType(event_type) if event_type else None
        events = self.audit.get_events(user_id=user_id, event_type=et, limit=limit)
        
        return web.json_response({"events": [e.to_dict() for e in events]})
    
    async def _admin_ban_handler(self, request: web.Request) -> web.Response:
        """Ban a user."""
        if not await self._require_admin(request):
            return web.json_response({"error": "Unauthorized"}, status=401)
        
        data = await request.json()
        user_id = data.get("user_id")
        reason = data.get("reason", "")
        
        if not user_id:
            return web.json_response({"error": "user_id required"}, status=400)
        
        self.sessions.ban_user(user_id, reason)
        self.exchange.cancel_all_user_orders_all_markets(user_id)
        
        self.audit.log(
            AuditEventType.BANNED,
            user_id=user_id,
            reason=reason,
        )
        
        # Disconnect user
        ws = self._connections.get(user_id)
        if ws and not ws.closed:
            await ws.close()
        
        return web.json_response({"success": True, "user_id": user_id})
    
    async def _admin_unban_handler(self, request: web.Request) -> web.Response:
        """Unban a user."""
        if not await self._require_admin(request):
            return web.json_response({"error": "Unauthorized"}, status=401)
        
        data = await request.json()
        user_id = data.get("user_id")
        
        if not user_id:
            return web.json_response({"error": "user_id required"}, status=400)
        
        self.sessions.unban_user(user_id)
        
        self.audit.log(
            AuditEventType.ADMIN_ACTION,
            user_id=user_id,
            action="unban",
        )
        
        return web.json_response({"success": True, "user_id": user_id})
    
    async def _admin_market_handler(self, request: web.Request) -> web.Response:
        """Control market status."""
        if not await self._require_admin(request):
            return web.json_response({"error": "Unauthorized"}, status=401)
        
        action = request.match_info.get("action")
        data = await request.json()
        market_id = data.get("market_id")
        
        if not market_id:
            return web.json_response({"error": "market_id required"}, status=400)
        
        actions = {
            "start": lambda: self.exchange.start_market(market_id),
            "stop": lambda: self.exchange.stop_market(market_id),
            "halt": lambda: self.exchange.halt_market(market_id),
            "resume": lambda: self.exchange.resume_market(market_id),
        }
        
        if action not in actions:
            return web.json_response({"error": f"Unknown action: {action}"}, status=400)
        
        success = actions[action]()
        
        self.audit.log(
            AuditEventType.ADMIN_ACTION,
            action=f"market_{action}",
            market_id=market_id,
            success=success,
        )
        
        return web.json_response({"success": success, "market_id": market_id, "action": action})
    
    async def _health_handler(self, request: web.Request) -> web.Response:
        """Health check endpoint."""
        return web.json_response({
            "status": "healthy",
            "timestamp": datetime.now(UTC).isoformat(),
            "markets": len(self.exchange.list_markets()),
            "connections": len(self._connections),
        })
    
    # ─────────────────────────────────────────────────────────────────────────
    # Server Control
    # ─────────────────────────────────────────────────────────────────────────
    
    def run(self):
        """Run the server."""
        print(f"Starting trading server on {self.config.host}:{self.config.port}")
        print(f"Admin token: {self.config.admin_token}")
        web.run_app(self.app, host=self.config.host, port=self.config.port)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def create_demo_exchange() -> Exchange:
    """Create a demo exchange with sample markets."""
    exchange = Exchange()
    
    # Create some markets
    exchange.create_market(
        "AAPL",
        "Apple Inc.",
        description="Tech giant stock",
        tick_size="0.01",
        max_position=1000,
    )
    
    exchange.create_market(
        "BTCUSD",
        "Bitcoin/USD",
        description="Cryptocurrency pair",
        tick_size="0.01",
        max_position=10,
    )
    
    exchange.create_market(
        "GOLD",
        "Gold Futures",
        description="Precious metal",
        tick_size="0.10",
        lot_size=10,
        max_position=500,
    )
    
    return exchange


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Trading Exchange Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8080, help="Port to bind to")
    parser.add_argument("--admin-token", help="Admin token (generated if not provided)")
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    
    # Create exchange
    exchange = create_demo_exchange()
    
    # Create server config
    config = ServerConfig(
        host=args.host,
        port=args.port,
    )
    if args.admin_token:
        config.admin_token = args.admin_token
    
    # Create and run server
    server = TradingServer(exchange, config)
    
    # Start all markets
    exchange.start_all_markets()
    
    server.run()
