"""
In-Memory Persistence Layer for Testing.

Provides the same interface as MongoRepository but stores everything in memory.
Useful for:
1. Testing without MongoDB
2. Development and local testing
3. Unit tests
"""

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional
from collections import defaultdict
import hashlib

from matching_engine import (
    Exchange, Order, Trade, Position, Market,
    EngineEvent, OrderId, UserId, MarketId,
)

from persistence import (
    serialize_event, serialize_order, serialize_trade, 
    serialize_position, serialize_market, DecimalEncoder,
)


class InMemoryRepository:
    """
    In-memory implementation of the persistence repository.
    
    Provides the same interface as MongoRepository for testing
    and development without requiring MongoDB.
    """
    
    def __init__(self):
        self.events: list[dict] = []
        self.trades: dict[str, dict] = {}
        self.orders: dict[str, dict] = {}
        self.positions: dict[str, dict] = {}
        self.markets: dict[str, dict] = {}
        self.snapshots: list[dict] = []
        self._event_count = 0
        self._last_snapshot_at = 0
        self.snapshot_interval = 1000
    
    async def connect(self) -> None:
        """No-op for in-memory."""
        print("Using in-memory persistence (no MongoDB)")
    
    async def disconnect(self) -> None:
        """No-op for in-memory."""
        pass
    
    # ─────────────────────────────────────────────────────────────────────────
    # Event Storage
    # ─────────────────────────────────────────────────────────────────────────
    
    async def store_event(self, event: EngineEvent) -> str:
        """Store an exchange event."""
        doc = serialize_event(event)
        doc["_id"] = f"event_{len(self.events)}"
        self.events.append(doc)
        self._event_count += 1
        return doc["_id"]
    
    async def store_events(self, events: list[EngineEvent]) -> list[str]:
        """Store multiple events."""
        ids = []
        for event in events:
            id = await self.store_event(event)
            ids.append(id)
        return ids
    
    async def get_events(
        self,
        start_sequence: int = 0,
        end_sequence: int = None,
        market_id: str = None,
        user_id: str = None,
        event_types: list[str] = None,
        limit: int = 10000,
    ) -> list[dict]:
        """Retrieve events with filters."""
        results = []
        
        for event in self.events:
            # Apply filters
            seq = event.get("sequence", 0) or 0
            if seq < start_sequence:
                continue
            if end_sequence and seq > end_sequence:
                continue
            if market_id and event.get("market_id") != market_id:
                continue
            if user_id and event.get("user_id") != user_id:
                continue
            if event_types and event.get("event_type") not in event_types:
                continue
            
            results.append(event)
            
            if len(results) >= limit:
                break
        
        return results
    
    async def get_event_count(self) -> int:
        """Get total event count."""
        return len(self.events)
    
    async def get_latest_sequence(self) -> int:
        """Get the latest sequence number."""
        if not self.events:
            return 0
        return max(e.get("sequence", 0) or 0 for e in self.events)
    
    # ─────────────────────────────────────────────────────────────────────────
    # Trade Storage
    # ─────────────────────────────────────────────────────────────────────────
    
    async def store_trade(self, trade: Trade) -> None:
        """Store a trade."""
        doc = serialize_trade(trade)
        self.trades[doc["_id"]] = doc
    
    async def get_trades(
        self,
        market_id: str = None,
        user_id: str = None,
        start_time: datetime = None,
        end_time: datetime = None,
        limit: int = 100,
    ) -> list[dict]:
        """Retrieve trades with filters."""
        results = []
        
        for trade in self.trades.values():
            if market_id and trade.get("market_id") != market_id:
                continue
            if user_id and trade.get("buyer_id") != user_id and trade.get("seller_id") != user_id:
                continue
            
            results.append(trade)
            
            if len(results) >= limit:
                break
        
        return sorted(results, key=lambda x: x.get("timestamp", datetime.min), reverse=True)
    
    # ─────────────────────────────────────────────────────────────────────────
    # Order Storage
    # ─────────────────────────────────────────────────────────────────────────
    
    async def store_order(self, order: Order) -> None:
        """Store or update an order."""
        doc = serialize_order(order)
        self.orders[doc["_id"]] = doc
    
    async def get_order(self, order_id: str) -> Optional[dict]:
        """Get an order by ID."""
        return self.orders.get(order_id)
    
    async def get_orders(
        self,
        market_id: str = None,
        user_id: str = None,
        status: str = None,
        limit: int = 100,
    ) -> list[dict]:
        """Retrieve orders with filters."""
        results = []
        
        for order in self.orders.values():
            if market_id and order.get("market_id") != market_id:
                continue
            if user_id and order.get("user_id") != user_id:
                continue
            if status and order.get("status") != status:
                continue
            
            results.append(order)
            
            if len(results) >= limit:
                break
        
        return results
    
    # ─────────────────────────────────────────────────────────────────────────
    # Position Storage
    # ─────────────────────────────────────────────────────────────────────────
    
    async def store_position(self, user_id: str, market_id: str, position: Position) -> None:
        """Store or update a position."""
        doc = serialize_position(user_id, market_id, position)
        self.positions[doc["_id"]] = doc
    
    async def get_position(self, user_id: str, market_id: str) -> Optional[dict]:
        """Get a position."""
        return self.positions.get(f"{user_id}:{market_id}")
    
    async def get_user_positions(self, user_id: str) -> list[dict]:
        """Get all positions for a user."""
        return [p for p in self.positions.values() if p.get("user_id") == user_id]
    
    async def get_all_positions(self) -> list[dict]:
        """Get all positions."""
        return list(self.positions.values())
    
    # ─────────────────────────────────────────────────────────────────────────
    # Market Storage
    # ─────────────────────────────────────────────────────────────────────────
    
    async def store_market(self, market: Market) -> None:
        """Store or update a market."""
        doc = serialize_market(market)
        self.markets[doc["_id"]] = doc
    
    async def get_market(self, market_id: str) -> Optional[dict]:
        """Get a market."""
        return self.markets.get(market_id)
    
    async def get_all_markets(self) -> list[dict]:
        """Get all markets."""
        return list(self.markets.values())
    
    # ─────────────────────────────────────────────────────────────────────────
    # Snapshots
    # ─────────────────────────────────────────────────────────────────────────
    
    async def create_snapshot(self, exchange: Exchange) -> str:
        """Create a state snapshot."""
        markets_data = []
        order_books_data = {}
        positions_data = []
        
        for market_id, market in exchange._markets.items():
            markets_data.append(serialize_market(market))
            
            engine = exchange._engines.get(market_id)
            if engine:
                book = engine.book
                
                # Get bids - book._bids is a dict of Decimal -> OrderQueue
                bids_list = []
                for price, queue in book._bids.items():
                    bids_list.append({
                        "price": str(price), 
                        "orders": [serialize_order(o) for o in queue.orders]
                    })
                
                # Get asks - book._asks is a dict of Decimal -> OrderQueue
                asks_list = []
                for price, queue in book._asks.items():
                    asks_list.append({
                        "price": str(price),
                        "orders": [serialize_order(o) for o in queue.orders]
                    })
                
                order_books_data[str(market_id)] = {
                    "bids": bids_list,
                    "asks": asks_list,
                }
                
                # Get positions from engine's position tracker
                for position in engine.positions.get_all():
                    positions_data.append(serialize_position(
                        str(position.user_id), str(position.market_id), position
                    ))
        
        snapshot = {
            "_id": f"snapshot_{len(self.snapshots)}",
            "created_at": datetime.now(timezone.utc),
            "event_sequence": await self.get_latest_sequence(),
            "event_count": await self.get_event_count(),
            "total_orders": exchange.total_orders,
            "total_trades": exchange.total_trades,
            "markets": markets_data,
            "order_books": order_books_data,
            "positions": positions_data,
        }
        
        snapshot["checksum"] = hashlib.sha256(
            json.dumps(snapshot, cls=DecimalEncoder, sort_keys=True, default=str).encode()
        ).hexdigest()[:16]
        
        self.snapshots.append(snapshot)
        self._last_snapshot_at = self._event_count
        
        return snapshot["_id"]
    
    async def get_latest_snapshot(self) -> Optional[dict]:
        """Get most recent snapshot."""
        if not self.snapshots:
            return None
        return self.snapshots[-1]
    
    async def should_snapshot(self) -> bool:
        """Check if snapshot needed."""
        return (self._event_count - self._last_snapshot_at) >= self.snapshot_interval
    
    # ─────────────────────────────────────────────────────────────────────────
    # Cleanup
    # ─────────────────────────────────────────────────────────────────────────
    
    async def clear_all(self) -> None:
        """Clear all data."""
        self.events = []
        self.trades = {}
        self.orders = {}
        self.positions = {}
        self.markets = {}
        self.snapshots = []
        self._event_count = 0
        self._last_snapshot_at = 0
        print("Cleared in-memory data")
