"""
Book Cache and API Layer.

This module provides a cached book state layer that:
1. Maintains a copy of order book state for fast reads
2. Updates in real-time from exchange events
3. Provides rate-limited API access
4. Does NOT impact the matching engine

The matching engine focuses on matching; this layer handles reads.

Architecture:
    Exchange (matching) -> Events -> BookCache (reads)
                                         |
                                         v
                                   Frontend clients
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Callable
from collections import defaultdict
import threading
import copy

from matching_engine import (
    Exchange, MarketId, Side,
    EngineEvent, OrderAccepted, OrderRejected, OrderCancelled,
    OrderFilled, OrderExpired, TradeExecuted, BookSnapshot, BookDelta,
    BookDeltaEntry, BookDeltaAction, PriceLevel, MarketStatusChanged,
)


# ─────────────────────────────────────────────────────────────────────────────
# Book Cache Configuration
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class BookCacheConfig:
    """Configuration for book cache."""
    # Rate limiting for book requests
    book_requests_per_second: float = 10.0  # Per user
    book_requests_burst: int = 20
    
    # Cache settings
    max_depth: int = 50  # Maximum price levels to track per side
    snapshot_interval: float = 5.0  # Send full snapshot every N seconds
    
    # Aggregation
    aggregate_by_tick: bool = False  # Aggregate multiple price levels


# ─────────────────────────────────────────────────────────────────────────────
# Token Bucket Rate Limiter
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class BookRateLimiter:
    """Rate limiter for book requests."""
    capacity: float
    refill_rate: float  # tokens per second
    tokens: float = field(default=None)
    last_update: float = field(default_factory=time.monotonic)
    
    def __post_init__(self):
        if self.tokens is None:
            self.tokens = self.capacity
    
    def check_and_consume(self, tokens: float = 1.0) -> tuple[bool, str]:
        """Check if request is allowed and consume tokens."""
        now = time.monotonic()
        elapsed = now - self.last_update
        self.last_update = now
        
        # Refill tokens
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True, ""
        return False, "Book request rate limit exceeded"


# ─────────────────────────────────────────────────────────────────────────────
# Cached Price Level
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CachedPriceLevel:
    """A price level in the cached book."""
    price: Decimal
    qty: int
    order_count: int
    updated_at: float = field(default_factory=time.monotonic)
    
    def to_dict(self) -> dict:
        return {
            "price": str(self.price),
            "qty": self.qty,
            "order_count": self.order_count,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Cached Order Book
# ─────────────────────────────────────────────────────────────────────────────

class CachedOrderBook:
    """
    Cached order book state for a single market.
    
    Updated from exchange events, provides fast reads.
    Thread-safe for concurrent access.
    """
    
    def __init__(self, market_id: str, max_depth: int = 50):
        self.market_id = market_id
        self.max_depth = max_depth
        
        # Price levels: price -> CachedPriceLevel
        self._bids: dict[Decimal, CachedPriceLevel] = {}
        self._asks: dict[Decimal, CachedPriceLevel] = {}
        
        # Sequence tracking
        self._sequence = 0
        self._last_update = time.monotonic()
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Event callbacks
        self._on_update: Optional[Callable] = None
    
    def set_update_callback(self, callback: Callable) -> None:
        """Set callback to be called on book updates."""
        self._on_update = callback
    
    def apply_snapshot(self, snapshot: BookSnapshot) -> None:
        """Apply a full book snapshot."""
        with self._lock:
            self._bids.clear()
            self._asks.clear()
            
            for level in snapshot.bids:
                self._bids[level.price] = CachedPriceLevel(
                    price=level.price,
                    qty=level.qty,
                    order_count=level.order_count,
                )
            
            for level in snapshot.asks:
                self._asks[level.price] = CachedPriceLevel(
                    price=level.price,
                    qty=level.qty,
                    order_count=level.order_count,
                )
            
            self._sequence = snapshot.sequence
            self._last_update = time.monotonic()
        
        self._notify_update()
    
    def apply_delta(self, delta: BookDelta) -> None:
        """Apply incremental book update."""
        with self._lock:
            for change in delta.changes:
                book = self._bids if change.side == Side.BUY else self._asks
                
                if change.action == BookDeltaAction.REMOVE or change.qty == 0:
                    book.pop(change.price, None)
                else:
                    book[change.price] = CachedPriceLevel(
                        price=change.price,
                        qty=change.qty,
                        order_count=change.order_count,
                    )
            
            self._sequence = delta.sequence
            self._last_update = time.monotonic()
        
        self._notify_update()
    
    def get_snapshot(self) -> dict:
        """Get current book state as dictionary."""
        with self._lock:
            # Sort bids descending (highest first)
            sorted_bids = sorted(self._bids.values(), key=lambda x: x.price, reverse=True)
            # Sort asks ascending (lowest first)
            sorted_asks = sorted(self._asks.values(), key=lambda x: x.price)
            
            # Limit depth
            bids = sorted_bids[:self.max_depth]
            asks = sorted_asks[:self.max_depth]
            
            best_bid = bids[0].price if bids else None
            best_ask = asks[0].price if asks else None
            
            spread = None
            mid_price = None
            if best_bid and best_ask:
                spread = best_ask - best_bid
                mid_price = (best_bid + best_ask) / 2
            
            return {
                "type": "book_snapshot",
                "market_id": self.market_id,
                "bids": [b.to_dict() for b in bids],
                "asks": [a.to_dict() for a in asks],
                "best_bid": str(best_bid) if best_bid else None,
                "best_ask": str(best_ask) if best_ask else None,
                "spread": str(spread) if spread else None,
                "mid_price": str(mid_price) if mid_price else None,
                "sequence": self._sequence,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
    
    def get_level2(self, depth: int = 10) -> dict:
        """Get Level 2 (aggregated) book data."""
        with self._lock:
            sorted_bids = sorted(self._bids.values(), key=lambda x: x.price, reverse=True)
            sorted_asks = sorted(self._asks.values(), key=lambda x: x.price)
            
            bids = sorted_bids[:depth]
            asks = sorted_asks[:depth]
            
            return {
                "market_id": self.market_id,
                "bids": [{"price": str(b.price), "qty": b.qty} for b in bids],
                "asks": [{"price": str(a.price), "qty": a.qty} for a in asks],
                "sequence": self._sequence,
            }
    
    def get_best_bid_ask(self) -> dict:
        """Get just the best bid/ask (Level 1 data)."""
        with self._lock:
            best_bid = max(self._bids.keys()) if self._bids else None
            best_ask = min(self._asks.keys()) if self._asks else None
            
            bid_qty = self._bids[best_bid].qty if best_bid else 0
            ask_qty = self._asks[best_ask].qty if best_ask else 0
            
            return {
                "market_id": self.market_id,
                "best_bid": str(best_bid) if best_bid else None,
                "best_bid_qty": bid_qty,
                "best_ask": str(best_ask) if best_ask else None,
                "best_ask_qty": ask_qty,
                "sequence": self._sequence,
            }
    
    @property
    def sequence(self) -> int:
        return self._sequence
    
    def _notify_update(self) -> None:
        """Notify listeners of book update."""
        if self._on_update:
            try:
                self._on_update(self.market_id)
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────────────────────
# Book Cache Manager
# ─────────────────────────────────────────────────────────────────────────────

class BookCacheManager:
    """
    Manages cached book state for all markets.
    
    Listens to exchange events and maintains up-to-date book state.
    Provides rate-limited access for API layer.
    """
    
    def __init__(self, config: BookCacheConfig = None):
        self.config = config or BookCacheConfig()
        
        # Cached books per market
        self._books: dict[str, CachedOrderBook] = {}
        
        # Rate limiters per user
        self._rate_limiters: dict[str, BookRateLimiter] = {}
        
        # Update callbacks
        self._on_book_update: Optional[Callable[[str], None]] = None
        
        # Statistics
        self._cache_hits = 0
        self._cache_misses = 0
        self._rate_limited_requests = 0
    
    def set_update_callback(self, callback: Callable[[str], None]) -> None:
        """
        Set callback for book updates.
        
        Callback receives market_id when book changes.
        Use this to push updates to WebSocket clients.
        """
        self._on_book_update = callback
        
        # Set on existing books
        for book in self._books.values():
            book.set_update_callback(self._handle_book_update)
    
    def _handle_book_update(self, market_id: str) -> None:
        """Internal handler for book updates."""
        if self._on_book_update:
            self._on_book_update(market_id)
    
    def get_or_create_book(self, market_id: str) -> CachedOrderBook:
        """Get or create a cached book for a market."""
        if market_id not in self._books:
            self._books[market_id] = CachedOrderBook(
                market_id=market_id,
                max_depth=self.config.max_depth,
            )
            self._books[market_id].set_update_callback(self._handle_book_update)
        return self._books[market_id]
    
    def get_rate_limiter(self, user_id: str) -> BookRateLimiter:
        """Get or create rate limiter for a user."""
        if user_id not in self._rate_limiters:
            self._rate_limiters[user_id] = BookRateLimiter(
                capacity=self.config.book_requests_burst,
                refill_rate=self.config.book_requests_per_second,
            )
        return self._rate_limiters[user_id]
    
    def on_event(self, event: EngineEvent) -> None:
        """
        Handle exchange event.
        
        Call this from the exchange event handler to keep cache updated.
        """
        # Get market ID from event
        market_id = None
        if hasattr(event, 'market_id'):
            market_id = str(event.market_id)
        elif hasattr(event, 'trade'):
            market_id = str(event.trade.market_id)
        
        if not market_id:
            return
        
        book = self.get_or_create_book(market_id)
        
        # Apply event to cache
        if isinstance(event, BookSnapshot):
            book.apply_snapshot(event)
        
        elif isinstance(event, BookDelta):
            book.apply_delta(event)
    
    def get_book_snapshot(
        self, 
        market_id: str, 
        user_id: str,
        check_rate_limit: bool = True,
    ) -> tuple[dict | None, str]:
        """
        Get book snapshot with rate limiting.
        
        Returns:
            (snapshot_dict, error_message)
            If error_message is non-empty, request was denied.
        """
        # Check rate limit
        if check_rate_limit:
            limiter = self.get_rate_limiter(user_id)
            allowed, reason = limiter.check_and_consume()
            if not allowed:
                self._rate_limited_requests += 1
                return None, reason
        
        # Get from cache
        book = self._books.get(market_id)
        if not book:
            self._cache_misses += 1
            return None, f"Market {market_id} not found in cache"
        
        self._cache_hits += 1
        return book.get_snapshot(), ""
    
    def get_level2(
        self,
        market_id: str,
        user_id: str,
        depth: int = 10,
        check_rate_limit: bool = True,
    ) -> tuple[dict | None, str]:
        """Get Level 2 book data with rate limiting."""
        if check_rate_limit:
            limiter = self.get_rate_limiter(user_id)
            allowed, reason = limiter.check_and_consume()
            if not allowed:
                self._rate_limited_requests += 1
                return None, reason
        
        book = self._books.get(market_id)
        if not book:
            return None, f"Market {market_id} not found"
        
        return book.get_level2(depth), ""
    
    def get_best_bid_ask(
        self,
        market_id: str,
        user_id: str,
        check_rate_limit: bool = True,
    ) -> tuple[dict | None, str]:
        """Get Level 1 data (best bid/ask) with rate limiting."""
        if check_rate_limit:
            limiter = self.get_rate_limiter(user_id)
            allowed, reason = limiter.check_and_consume(0.5)  # Less costly
            if not allowed:
                self._rate_limited_requests += 1
                return None, reason
        
        book = self._books.get(market_id)
        if not book:
            return None, f"Market {market_id} not found"
        
        return book.get_best_bid_ask(), ""
    
    def get_all_books(self) -> dict[str, dict]:
        """Get snapshots of all books (admin only, no rate limit)."""
        return {
            market_id: book.get_snapshot()
            for market_id, book in self._books.items()
        }
    
    def get_stats(self) -> dict:
        """Get cache statistics."""
        return {
            "markets_cached": len(self._books),
            "users_tracked": len(self._rate_limiters),
            "cache_hits": self._cache_hits,
            "cache_misses": self._cache_misses,
            "rate_limited_requests": self._rate_limited_requests,
        }
    
    def initialize_from_exchange(self, exchange: Exchange) -> None:
        """Initialize cache from exchange state."""
        for market_id in exchange._markets.keys():
            snapshot = exchange.get_book_snapshot(str(market_id))
            if snapshot:
                book = self.get_or_create_book(str(market_id))
                book.apply_snapshot(snapshot)


# ─────────────────────────────────────────────────────────────────────────────
# API Response Builders
# ─────────────────────────────────────────────────────────────────────────────

def make_book_response(data: dict, request_id: str | None = None) -> dict:
    """Build book response message."""
    msg = {
        "type": "book_snapshot",
        "data": data,
    }
    if request_id:
        msg["request_id"] = request_id
    return msg


def make_book_update(market_id: str, changes: list[dict], sequence: int) -> dict:
    """Build book update message for push to clients."""
    return {
        "type": "book_update",
        "data": {
            "market_id": market_id,
            "changes": changes,
            "sequence": sequence,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    }


# ─────────────────────────────────────────────────────────────────────────────
# Integration Helper
# ─────────────────────────────────────────────────────────────────────────────

def create_book_cache_handler(
    cache: BookCacheManager,
    broadcast_fn: Callable[[str, dict], None],
) -> Callable[[EngineEvent], None]:
    """
    Create an event handler that updates cache and broadcasts changes.
    
    Args:
        cache: The BookCacheManager instance
        broadcast_fn: Function to broadcast updates. Takes (market_id, message_dict)
    
    Returns:
        Event handler function to connect to exchange
    """
    def handler(event: EngineEvent) -> None:
        # Update cache
        cache.on_event(event)
        
        # Broadcast book changes to subscribers
        if isinstance(event, (BookSnapshot, BookDelta)):
            market_id = str(event.market_id)
            book = cache._books.get(market_id)
            if book:
                snapshot = book.get_snapshot()
                broadcast_fn(market_id, {
                    "type": "book_snapshot",
                    "data": snapshot,
                })
    
    return handler


# ─────────────────────────────────────────────────────────────────────────────
# Example Usage
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Example: Setup book cache with exchange
    from matching_engine import Exchange
    
    # Create exchange with event handler
    cache = BookCacheManager()
    
    def on_event(event):
        cache.on_event(event)
        print(f"Event: {type(event).__name__}")
    
    exchange = Exchange(on_event=on_event)
    exchange.create_market("TEST", "Test Market", tick_size="0.01")
    exchange.start_market("TEST")
    
    # Initialize cache
    cache.initialize_from_exchange(exchange)
    
    # Set update callback
    def on_book_update(market_id: str):
        book = cache._books[market_id]
        print(f"Book updated: {market_id}, seq={book.sequence}")
    
    cache.set_update_callback(on_book_update)
    
    # Simulate trading
    from decimal import Decimal
    exchange.submit_order("TEST", "user1", "buy", Decimal("100.00"), 100)
    exchange.submit_order("TEST", "user2", "sell", Decimal("101.00"), 50)
    exchange.submit_order("TEST", "user3", "buy", Decimal("99.00"), 200)
    
    # Get cached book (with rate limiting)
    snapshot, error = cache.get_book_snapshot("TEST", "user1")
    if snapshot:
        print(f"\nBook snapshot:")
        print(f"  Best bid: {snapshot['best_bid']}")
        print(f"  Best ask: {snapshot['best_ask']}")
        print(f"  Bids: {len(snapshot['bids'])} levels")
        print(f"  Asks: {len(snapshot['asks'])} levels")
    
    # Test rate limiting
    print("\nTesting rate limiting...")
    for i in range(25):
        _, error = cache.get_book_snapshot("TEST", "user1")
        if error:
            print(f"  Request {i+1}: RATE LIMITED - {error}")
            break
        print(f"  Request {i+1}: OK")
    
    print(f"\nCache stats: {cache.get_stats()}")
