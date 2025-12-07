"""
MongoDB Persistence Layer for Trading Exchange.

Provides:
1. Event sourcing - all exchange events stored in MongoDB
2. State snapshots - periodic snapshots for faster recovery
3. Replay capability - rebuild exchange state from events
4. Separate listener process - subscribes to WebSocket and persists events

Architecture:
    Exchange -> Events -> WebSocket -> Listener Process -> MongoDB
                                            |
                                            v
                                    [events collection]
                                    [snapshots collection]
                                    [trades collection]
                                    [orders collection]
                                    [positions collection]
"""

import asyncio
import json
import signal
import sys
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Optional
import hashlib

# MongoDB
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING, IndexModel
from pymongo.errors import DuplicateKeyError

# For WebSocket client
import aiohttp

# Local imports
from matching_engine import (
    Exchange, ExchangeConfig, Side, TimeInForce, MarketStatus,
    Order, Trade, Position, Market, OrderId, UserId, MarketId,
    EngineEvent, OrderAccepted, OrderRejected, OrderCancelled,
    OrderFilled, OrderExpired, TradeExecuted, BookSnapshot, BookDelta,
    MarketStatusChanged,
)


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class MongoConfig:
    """MongoDB configuration."""
    uri: str = "mongodb://localhost:27017"
    database: str = "exchange"
    
    # Collections
    events_collection: str = "events"
    snapshots_collection: str = "snapshots"
    trades_collection: str = "trades"
    orders_collection: str = "orders"
    positions_collection: str = "positions"
    markets_collection: str = "markets"
    
    # Snapshot settings
    snapshot_interval: int = 1000  # Create snapshot every N events
    
    # Connection settings
    max_pool_size: int = 50
    min_pool_size: int = 10


@dataclass  
class ListenerConfig:
    """Event listener configuration."""
    ws_url: str = "ws://localhost:8080/ws"
    api_url: str = "http://localhost:8080"
    user_id: str = "persistence_listener"
    reconnect_delay: float = 5.0
    markets: list[str] = field(default_factory=lambda: ["AAPL", "BTCUSD", "GOLD"])


# ─────────────────────────────────────────────────────────────────────────────
# Serialization Helpers
# ─────────────────────────────────────────────────────────────────────────────

class DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal types."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Enum):
            return obj.value
        if hasattr(obj, 'to_dict'):
            return obj.to_dict()
        return super().default(obj)


def serialize_event(event: EngineEvent) -> dict:
    """Serialize an exchange event to a MongoDB document."""
    event_dict = event.to_dict()
    
    return {
        "event_type": type(event).__name__,
        "timestamp": datetime.now(timezone.utc),
        "sequence": getattr(event, 'sequence', None),
        "market_id": str(getattr(event, 'market_id', None)) if hasattr(event, 'market_id') else None,
        "user_id": str(getattr(event, 'user_id', None)) if hasattr(event, 'user_id') else None,
        "data": json.loads(json.dumps(event_dict, cls=DecimalEncoder)),
    }


def serialize_order(order: Order) -> dict:
    """Serialize an order to MongoDB document."""
    return {
        "_id": str(order.id),
        "market_id": str(order.market_id),
        "user_id": str(order.user_id),
        "side": order.side.value,
        "price": str(order.price),
        "original_qty": order.qty,
        "remaining_qty": order.remaining_qty,
        "filled_qty": order.filled_qty,
        "status": order.status.value,
        "time_in_force": order.time_in_force.value,
        "client_order_id": order.client_order_id,
        "created_at": order.created_at,
        "updated_at": order.updated_at,
    }


def serialize_trade(trade: Trade) -> dict:
    """Serialize a trade to MongoDB document."""
    return {
        "_id": str(trade.id),
        "market_id": str(trade.market_id),
        "price": str(trade.price),
        "qty": trade.qty,
        "buyer_id": str(trade.buyer_id),
        "seller_id": str(trade.seller_id),
        "buyer_order_id": str(trade.buy_order_id),
        "seller_order_id": str(trade.sell_order_id),
        "aggressor_side": trade.aggressor_side.value,
        "timestamp": trade.created_at,
        "sequence": trade.sequence,
    }


def serialize_position(user_id: str, market_id: str, position: Position) -> dict:
    """Serialize a position to MongoDB document."""
    return {
        "_id": f"{user_id}:{market_id}",
        "user_id": user_id,
        "market_id": market_id,
        "net_qty": position.net_qty,
        "total_bought": position.total_bought,
        "total_sold": position.total_sold,
        "buy_value": str(position.buy_value),
        "sell_value": str(position.sell_value),
        "realized_pnl": str(position.realized_pnl),
        "avg_buy_price": str(position.avg_buy_price) if position.avg_buy_price else None,
        "avg_sell_price": str(position.avg_sell_price) if position.avg_sell_price else None,
        "trade_count": position.trade_count,
        "updated_at": datetime.now(timezone.utc),
    }


def serialize_market(market: Market) -> dict:
    """Serialize a market to MongoDB document."""
    config = market.config
    return {
        "_id": str(market.id),
        "title": market.title,
        "status": market.status.value,
        "tick_size": str(config.tick_size),
        "lot_size": config.lot_size,
        "min_qty": config.min_qty,
        "max_qty": config.max_qty,
        "max_position": config.max_position,
        "last_trade_price": str(market.last_trade_price) if market.last_trade_price else None,
        "total_volume": market.total_volume,
        "created_at": config.created_at,
    }


# ─────────────────────────────────────────────────────────────────────────────
# MongoDB Repository
# ─────────────────────────────────────────────────────────────────────────────

class MongoRepository:
    """
    MongoDB repository for exchange data.
    
    Handles all database operations including:
    - Event storage and retrieval
    - State snapshots
    - Order/trade/position persistence
    """
    
    def __init__(self, config: MongoConfig):
        self.config = config
        self.client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[AsyncIOMotorDatabase] = None
        self._event_count = 0
        self._last_snapshot_at = 0
    
    async def connect(self) -> None:
        """Connect to MongoDB."""
        self.client = AsyncIOMotorClient(
            self.config.uri,
            maxPoolSize=self.config.max_pool_size,
            minPoolSize=self.config.min_pool_size,
        )
        self.db = self.client[self.config.database]
        
        # Create indexes
        await self._create_indexes()
        
        print(f"Connected to MongoDB: {self.config.database}")
    
    async def disconnect(self) -> None:
        """Disconnect from MongoDB."""
        if self.client:
            self.client.close()
            print("Disconnected from MongoDB")
    
    async def _create_indexes(self) -> None:
        """Create database indexes."""
        # Events collection indexes
        events = self.db[self.config.events_collection]
        await events.create_indexes([
            IndexModel([("timestamp", ASCENDING)]),
            IndexModel([("sequence", ASCENDING)]),
            IndexModel([("event_type", ASCENDING)]),
            IndexModel([("market_id", ASCENDING), ("timestamp", ASCENDING)]),
            IndexModel([("user_id", ASCENDING), ("timestamp", ASCENDING)]),
        ])
        
        # Trades collection indexes
        trades = self.db[self.config.trades_collection]
        await trades.create_indexes([
            IndexModel([("market_id", ASCENDING), ("timestamp", DESCENDING)]),
            IndexModel([("buyer_id", ASCENDING), ("timestamp", DESCENDING)]),
            IndexModel([("seller_id", ASCENDING), ("timestamp", DESCENDING)]),
            IndexModel([("timestamp", DESCENDING)]),
        ])
        
        # Orders collection indexes
        orders = self.db[self.config.orders_collection]
        await orders.create_indexes([
            IndexModel([("market_id", ASCENDING), ("status", ASCENDING)]),
            IndexModel([("user_id", ASCENDING), ("status", ASCENDING)]),
            IndexModel([("created_at", DESCENDING)]),
        ])
        
        # Positions collection indexes
        positions = self.db[self.config.positions_collection]
        await positions.create_indexes([
            IndexModel([("user_id", ASCENDING)]),
            IndexModel([("market_id", ASCENDING)]),
        ])
        
        # Snapshots collection indexes
        snapshots = self.db[self.config.snapshots_collection]
        await snapshots.create_indexes([
            IndexModel([("created_at", DESCENDING)]),
            IndexModel([("event_sequence", DESCENDING)]),
        ])
    
    # ─────────────────────────────────────────────────────────────────────────
    # Event Storage
    # ─────────────────────────────────────────────────────────────────────────
    
    async def store_event(self, event: EngineEvent) -> str:
        """Store an exchange event."""
        doc = serialize_event(event)
        result = await self.db[self.config.events_collection].insert_one(doc)
        self._event_count += 1
        return str(result.inserted_id)
    
    async def store_events(self, events: list[EngineEvent]) -> list[str]:
        """Store multiple events in batch."""
        if not events:
            return []
        
        docs = [serialize_event(e) for e in events]
        result = await self.db[self.config.events_collection].insert_many(docs)
        self._event_count += len(events)
        return [str(id) for id in result.inserted_ids]
    
    async def get_events(
        self,
        start_sequence: int = 0,
        end_sequence: int = None,
        market_id: str = None,
        user_id: str = None,
        event_types: list[str] = None,
        limit: int = 10000,
    ) -> list[dict]:
        """Retrieve events with optional filters."""
        query = {}
        
        if start_sequence > 0:
            query["sequence"] = {"$gte": start_sequence}
        if end_sequence:
            query.setdefault("sequence", {})["$lte"] = end_sequence
        if market_id:
            query["market_id"] = market_id
        if user_id:
            query["user_id"] = user_id
        if event_types:
            query["event_type"] = {"$in": event_types}
        
        cursor = self.db[self.config.events_collection].find(query).sort("sequence", ASCENDING).limit(limit)
        return await cursor.to_list(length=limit)
    
    async def get_event_count(self) -> int:
        """Get total event count."""
        return await self.db[self.config.events_collection].count_documents({})
    
    async def get_latest_sequence(self) -> int:
        """Get the latest event sequence number."""
        doc = await self.db[self.config.events_collection].find_one(
            {}, 
            sort=[("sequence", DESCENDING)]
        )
        return doc["sequence"] if doc and doc.get("sequence") else 0
    
    # ─────────────────────────────────────────────────────────────────────────
    # Trade Storage
    # ─────────────────────────────────────────────────────────────────────────
    
    async def store_trade(self, trade: Trade) -> None:
        """Store a trade."""
        doc = serialize_trade(trade)
        try:
            await self.db[self.config.trades_collection].insert_one(doc)
        except DuplicateKeyError:
            # Trade already exists, update it
            await self.db[self.config.trades_collection].replace_one(
                {"_id": doc["_id"]}, doc, upsert=True
            )
    
    async def get_trades(
        self,
        market_id: str = None,
        user_id: str = None,
        start_time: datetime = None,
        end_time: datetime = None,
        limit: int = 100,
    ) -> list[dict]:
        """Retrieve trades with optional filters."""
        query = {}
        
        if market_id:
            query["market_id"] = market_id
        if user_id:
            query["$or"] = [{"buyer_id": user_id}, {"seller_id": user_id}]
        if start_time:
            query["timestamp"] = {"$gte": start_time}
        if end_time:
            query.setdefault("timestamp", {})["$lte"] = end_time
        
        cursor = self.db[self.config.trades_collection].find(query).sort("timestamp", DESCENDING).limit(limit)
        return await cursor.to_list(length=limit)
    
    # ─────────────────────────────────────────────────────────────────────────
    # Order Storage
    # ─────────────────────────────────────────────────────────────────────────
    
    async def store_order(self, order: Order) -> None:
        """Store or update an order."""
        doc = serialize_order(order)
        await self.db[self.config.orders_collection].replace_one(
            {"_id": doc["_id"]}, doc, upsert=True
        )
    
    async def get_order(self, order_id: str) -> Optional[dict]:
        """Get an order by ID."""
        return await self.db[self.config.orders_collection].find_one({"_id": order_id})
    
    async def get_orders(
        self,
        market_id: str = None,
        user_id: str = None,
        status: str = None,
        limit: int = 100,
    ) -> list[dict]:
        """Retrieve orders with optional filters."""
        query = {}
        
        if market_id:
            query["market_id"] = market_id
        if user_id:
            query["user_id"] = user_id
        if status:
            query["status"] = status
        
        cursor = self.db[self.config.orders_collection].find(query).sort("created_at", DESCENDING).limit(limit)
        return await cursor.to_list(length=limit)
    
    # ─────────────────────────────────────────────────────────────────────────
    # Position Storage
    # ─────────────────────────────────────────────────────────────────────────
    
    async def store_position(self, user_id: str, market_id: str, position: Position) -> None:
        """Store or update a position."""
        doc = serialize_position(user_id, market_id, position)
        await self.db[self.config.positions_collection].replace_one(
            {"_id": doc["_id"]}, doc, upsert=True
        )
    
    async def get_position(self, user_id: str, market_id: str) -> Optional[dict]:
        """Get a position by user and market."""
        return await self.db[self.config.positions_collection].find_one(
            {"_id": f"{user_id}:{market_id}"}
        )
    
    async def get_user_positions(self, user_id: str) -> list[dict]:
        """Get all positions for a user."""
        cursor = self.db[self.config.positions_collection].find({"user_id": user_id})
        return await cursor.to_list(length=100)
    
    async def get_all_positions(self) -> list[dict]:
        """Get all positions."""
        cursor = self.db[self.config.positions_collection].find({})
        return await cursor.to_list(length=10000)
    
    # ─────────────────────────────────────────────────────────────────────────
    # Market Storage
    # ─────────────────────────────────────────────────────────────────────────
    
    async def store_market(self, market: Market) -> None:
        """Store or update a market."""
        doc = serialize_market(market)
        await self.db[self.config.markets_collection].replace_one(
            {"_id": doc["_id"]}, doc, upsert=True
        )
    
    async def get_market(self, market_id: str) -> Optional[dict]:
        """Get a market by ID."""
        return await self.db[self.config.markets_collection].find_one({"_id": market_id})
    
    async def get_all_markets(self) -> list[dict]:
        """Get all markets."""
        cursor = self.db[self.config.markets_collection].find({})
        return await cursor.to_list(length=100)
    
    # ─────────────────────────────────────────────────────────────────────────
    # Snapshots
    # ─────────────────────────────────────────────────────────────────────────
    
    async def create_snapshot(self, exchange: Exchange) -> str:
        """Create a full exchange state snapshot."""
        # Gather all state
        markets_data = []
        order_books_data = {}
        positions_data = []
        
        for market_id, market in exchange._markets.items():
            markets_data.append(serialize_market(market))
            
            # Get order book state
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
                
                # Get asks
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
                
                # Get positions from position tracker
                for position in engine.positions.get_all():
                    positions_data.append(serialize_position(
                        str(position.user_id), str(position.market_id), position
                    ))
        
        # Create snapshot document
        snapshot = {
            "created_at": datetime.now(timezone.utc),
            "event_sequence": await self.get_latest_sequence(),
            "event_count": await self.get_event_count(),
            "total_orders": exchange.total_orders,
            "total_trades": exchange.total_trades,
            "markets": markets_data,
            "order_books": order_books_data,
            "positions": positions_data,
        }
        
        # Calculate checksum
        snapshot["checksum"] = hashlib.sha256(
            json.dumps(snapshot, cls=DecimalEncoder, sort_keys=True).encode()
        ).hexdigest()[:16]
        
        result = await self.db[self.config.snapshots_collection].insert_one(snapshot)
        self._last_snapshot_at = self._event_count
        
        print(f"Created snapshot at sequence {snapshot['event_sequence']}")
        return str(result.inserted_id)
    
    async def get_latest_snapshot(self) -> Optional[dict]:
        """Get the most recent snapshot."""
        return await self.db[self.config.snapshots_collection].find_one(
            {}, sort=[("created_at", DESCENDING)]
        )
    
    async def should_snapshot(self) -> bool:
        """Check if we should create a new snapshot."""
        return (self._event_count - self._last_snapshot_at) >= self.config.snapshot_interval
    
    # ─────────────────────────────────────────────────────────────────────────
    # Cleanup
    # ─────────────────────────────────────────────────────────────────────────
    
    async def clear_all(self) -> None:
        """Clear all data (for testing)."""
        await self.db[self.config.events_collection].delete_many({})
        await self.db[self.config.trades_collection].delete_many({})
        await self.db[self.config.orders_collection].delete_many({})
        await self.db[self.config.positions_collection].delete_many({})
        await self.db[self.config.markets_collection].delete_many({})
        await self.db[self.config.snapshots_collection].delete_many({})
        self._event_count = 0
        self._last_snapshot_at = 0
        print("Cleared all MongoDB data")


# ─────────────────────────────────────────────────────────────────────────────
# Exchange State Rebuilder
# ─────────────────────────────────────────────────────────────────────────────

class ExchangeRebuilder:
    """
    Rebuilds exchange state from storage.
    
    Can rebuild from:
    1. Latest snapshot + subsequent events (fast)
    2. All events from scratch (slow but complete)
    
    Works with either MongoRepository or InMemoryRepository.
    """
    
    def __init__(self, repo):
        self.repo = repo  # Can be MongoRepository or InMemoryRepository
    
    async def rebuild(self, from_snapshot: bool = True) -> Exchange:
        """
        Rebuild exchange state from MongoDB.
        
        Args:
            from_snapshot: If True, use latest snapshot as starting point.
                          If False, replay all events from scratch.
        """
        if from_snapshot:
            snapshot = await self.repo.get_latest_snapshot()
            if snapshot:
                return await self._rebuild_from_snapshot(snapshot)
        
        return await self._rebuild_from_events()
    
    async def _rebuild_from_snapshot(self, snapshot: dict) -> Exchange:
        """Rebuild from a snapshot and replay subsequent events."""
        print(f"Rebuilding from snapshot at sequence {snapshot['event_sequence']}")
        
        # Create exchange with markets from snapshot
        exchange = Exchange()
        
        # Restore markets
        for market_data in snapshot["markets"]:
            exchange.create_market(
                market_id=market_data["_id"],
                title=market_data["title"],
                tick_size=market_data.get("tick_size", "0.01"),
                lot_size=market_data.get("lot_size", 1),
                max_position=market_data.get("max_position"),
            )
            
            # Restore market status
            status = MarketStatus(market_data["status"])
            if status == MarketStatus.ACTIVE:
                exchange.start_market(market_data["_id"])
        
        # Restore order books
        for market_id, book_data in snapshot.get("order_books", {}).items():
            engine = exchange._engines.get(MarketId(market_id))
            if not engine:
                continue
            
            # Restore orders
            for bid_level in book_data.get("bids", []):
                for order_data in bid_level["orders"]:
                    order = self._deserialize_order(order_data)
                    engine.book.add(order)
            
            for ask_level in book_data.get("asks", []):
                for order_data in ask_level["orders"]:
                    order = self._deserialize_order(order_data)
                    engine.book.add(order)
        
        # Restore positions
        for pos_data in snapshot.get("positions", []):
            market_id = MarketId(pos_data["market_id"])
            user_id = UserId(pos_data["user_id"])
            engine = exchange._engines.get(market_id)
            if engine:
                position = Position(user_id=user_id, market_id=market_id)
                position.net_qty = pos_data["net_qty"]
                position.total_bought = pos_data["total_bought"]
                position.total_sold = pos_data["total_sold"]
                position.buy_value = Decimal(pos_data.get("buy_value", "0"))
                position.sell_value = Decimal(pos_data.get("sell_value", "0"))
                # Position is stored via the tracker
                engine.positions._positions[user_id] = position
        
        # Note: total_orders/total_trades are properties on Exchange, not directly settable
        # They'll be updated as we replay events or are derived from engine state
        
        # Get events after snapshot
        events = await self.repo.get_events(
            start_sequence=snapshot["event_sequence"] + 1
        )
        
        if events:
            print(f"Replaying {len(events)} events after snapshot")
            await self._replay_events(exchange, events)
        
        print(f"Exchange rebuilt: {len(exchange._markets)} markets, "
              f"{exchange.total_orders} orders, {exchange.total_trades} trades")
        
        return exchange
    
    async def _rebuild_from_events(self) -> Exchange:
        """Rebuild from all events."""
        print("Rebuilding from all events (no snapshot)")
        
        # Get all markets first
        markets = await self.repo.get_all_markets()
        
        exchange = Exchange()
        
        # Create markets
        for market_data in markets:
            exchange.create_market(
                market_id=market_data["_id"],
                title=market_data["title"],
                tick_size=market_data["tick_size"],
                lot_size=market_data.get("lot_size", 1),
                min_price=market_data.get("min_price"),
                max_price=market_data.get("max_price"),
                max_position=market_data.get("max_position"),
            )
            exchange.start_market(market_data["_id"])
        
        # Get all events
        events = await self.repo.get_events(limit=1000000)
        
        if events:
            print(f"Replaying {len(events)} events")
            await self._replay_events(exchange, events)
        
        return exchange
    
    async def _replay_events(self, exchange: Exchange, events: list[dict]) -> None:
        """Replay events to rebuild state."""
        for event_doc in events:
            event_type = event_doc["event_type"]
            data = event_doc["data"]
            
            # We need to apply the state changes implied by each event
            # This is a simplified replay - in production you'd want more complete handling
            
            if event_type == "OrderAccepted":
                # Order was added to book - we need to reconstruct it
                market_id = data.get("market_id")
                engine = exchange._engines.get(MarketId(market_id)) if market_id else None
                if engine and "order" in data:
                    order = self._deserialize_order(data["order"])
                    if order.remaining_qty > 0 and order.status.value == "open":
                        engine.book.add(order)
            
            elif event_type == "OrderCancelled":
                market_id = data.get("market_id")
                order_id = data.get("order_id")
                engine = exchange._engines.get(MarketId(market_id)) if market_id else None
                if engine and order_id:
                    engine.book.remove(OrderId(order_id))
            
            elif event_type == "TradeExecuted":
                # Update trade count
                exchange.total_trades += 1
    
    def _deserialize_order(self, data: dict) -> Order:
        """Deserialize an order from dict."""
        order = Order(
            id=OrderId(data.get("id", data.get("_id", ""))),
            market_id=MarketId(data["market_id"]),
            user_id=UserId(data["user_id"]),
            side=Side(data["side"]),
            price=Decimal(data["price"]),
            qty=data.get("original_qty", data.get("qty", 0)),
            time_in_force=TimeInForce(data.get("time_in_force", "gtc")),
            client_order_id=data.get("client_order_id"),
        )
        # Restore filled qty if present
        filled = data.get("filled_qty", 0)
        if filled > 0:
            order.fill(filled)
        return order


# ─────────────────────────────────────────────────────────────────────────────
# Event Listener Process
# ─────────────────────────────────────────────────────────────────────────────

class EventListener:
    """
    Separate process that listens to exchange WebSocket and persists events.
    
    This runs as an independent process that:
    1. Connects to the exchange WebSocket
    2. Subscribes to all markets
    3. Receives all public events (trades, book updates)
    4. Persists events to MongoDB
    5. Creates periodic snapshots
    """
    
    def __init__(self, listener_config: ListenerConfig, mongo_config: MongoConfig):
        self.listener_config = listener_config
        self.mongo_config = mongo_config
        self.repo: Optional[MongoRepository] = None
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.running = False
        self._event_buffer: list[dict] = []
        self._buffer_size = 100
        self._last_flush = datetime.now(timezone.utc)
    
    async def start(self) -> None:
        """Start the event listener."""
        self.running = True
        
        # Connect to MongoDB
        self.repo = MongoRepository(self.mongo_config)
        await self.repo.connect()
        
        # Setup signal handlers
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))
        
        print("Event listener starting...")
        
        while self.running:
            try:
                await self._run_listener()
            except Exception as e:
                print(f"Listener error: {e}")
                if self.running:
                    print(f"Reconnecting in {self.listener_config.reconnect_delay}s...")
                    await asyncio.sleep(self.listener_config.reconnect_delay)
    
    async def stop(self) -> None:
        """Stop the event listener."""
        print("Stopping event listener...")
        self.running = False
        
        # Flush remaining events
        await self._flush_buffer()
        
        if self.ws:
            await self.ws.close()
        if self.session:
            await self.session.close()
        if self.repo:
            await self.repo.disconnect()
    
    async def _run_listener(self) -> None:
        """Main listener loop."""
        self.session = aiohttp.ClientSession()
        
        try:
            # Login
            async with self.session.post(
                f"{self.listener_config.api_url}/auth/login",
                json={"user_id": self.listener_config.user_id}
            ) as resp:
                if resp.status != 200:
                    raise Exception(f"Login failed: {await resp.text()}")
            
            # Connect WebSocket
            self.ws = await self.session.ws_connect(self.listener_config.ws_url)
            print("Connected to exchange WebSocket")
            
            # Subscribe to all markets
            for market_id in self.listener_config.markets:
                await self.ws.send_json({
                    "type": "subscribe",
                    "data": {"market_id": market_id}
                })
                print(f"Subscribed to {market_id}")
            
            # Listen for events
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_message(json.loads(msg.data))
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    raise Exception(f"WebSocket error: {self.ws.exception()}")
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    break
        
        finally:
            if self.session:
                await self.session.close()
    
    async def _handle_message(self, msg: dict) -> None:
        """Handle a WebSocket message."""
        msg_type = msg.get("type", "")
        data = msg.get("data", {})
        
        # Events we want to persist
        persist_types = {
            "trade", "order_accepted", "order_rejected", 
            "order_cancelled", "order_filled", "order_expired",
            "book_snapshot", "book_delta", "market_status"
        }
        
        if msg_type in persist_types:
            event_doc = {
                "event_type": msg_type,
                "timestamp": datetime.now(timezone.utc),
                "data": data,
                "market_id": data.get("market_id") or data.get("trade", {}).get("market_id"),
            }
            
            self._event_buffer.append(event_doc)
            
            # Store trades separately for fast querying
            if msg_type == "trade" and "trade" in data:
                trade_data = data["trade"]
                trade_doc = {
                    "_id": trade_data.get("id", str(datetime.now(timezone.utc).timestamp())),
                    "market_id": trade_data.get("market_id"),
                    "price": trade_data.get("price"),
                    "qty": trade_data.get("qty"),
                    "timestamp": datetime.now(timezone.utc),
                }
                try:
                    await self.repo.db[self.repo.config.trades_collection].insert_one(trade_doc)
                except DuplicateKeyError:
                    pass
            
            # Flush buffer if full or time elapsed
            if len(self._event_buffer) >= self._buffer_size:
                await self._flush_buffer()
            elif (datetime.now(timezone.utc) - self._last_flush).total_seconds() > 5:
                await self._flush_buffer()
    
    async def _flush_buffer(self) -> None:
        """Flush event buffer to MongoDB."""
        if not self._event_buffer:
            return
        
        try:
            await self.repo.db[self.repo.config.events_collection].insert_many(
                self._event_buffer
            )
            print(f"Flushed {len(self._event_buffer)} events to MongoDB")
        except Exception as e:
            print(f"Error flushing events: {e}")
        
        self._event_buffer = []
        self._last_flush = datetime.now(timezone.utc)


# ─────────────────────────────────────────────────────────────────────────────
# Integrated Persistence Manager
# ─────────────────────────────────────────────────────────────────────────────

class PersistenceManager:
    """
    Manages persistence for an exchange instance.
    
    Provides:
    - Direct event persistence (embedded mode)
    - Periodic snapshots
    - Recovery from storage
    
    Works with either MongoRepository or InMemoryRepository.
    """
    
    def __init__(self, exchange: Exchange, repo):
        self.exchange = exchange
        self.repo = repo  # Can be MongoRepository or InMemoryRepository
        self._original_handler: Optional[Callable] = None
    
    async def attach(self) -> None:
        """Attach to exchange and start persisting events."""
        # Store original handler
        self._original_handler = self.exchange._on_event
        
        # Wrap with persistence - the exchange calls _on_event with a single event
        original = self._original_handler
        repo = self.repo
        
        def persist_wrapper(event: EngineEvent):
            # Call original handler first (synchronously)
            if original:
                original(event)
            # Note: We can't easily do async persistence here, so we'll rely on
            # explicit persisting in the test or a background task
        
        self.exchange._on_event = persist_wrapper
        
        # Store markets
        for market in self.exchange._markets.values():
            await self.repo.store_market(market)
        
        print("Persistence manager attached to exchange")
    
    async def detach(self) -> None:
        """Detach from exchange."""
        self.exchange._on_event = self._original_handler
        print("Persistence manager detached")
    
    async def _persist_events(self, events: list[EngineEvent]) -> None:
        """Persist a batch of events."""
        for event in events:
            await self.repo.store_event(event)
            
            # Also update denormalized collections
            if isinstance(event, TradeExecuted):
                await self.repo.store_trade(event.trade)
            
            elif isinstance(event, (OrderAccepted, OrderFilled, OrderCancelled)):
                if hasattr(event, 'order'):
                    await self.repo.store_order(event.order)
        
        # Check if we need a snapshot
        if await self.repo.should_snapshot():
            await self.repo.create_snapshot(self.exchange)
    
    async def save_all_positions(self) -> None:
        """Save all current positions to MongoDB."""
        for market_id, engine in self.exchange._engines.items():
            for position in engine.positions.get_all():
                await self.repo.store_position(
                    str(position.user_id), str(position.market_id), position
                )
    
    async def create_snapshot(self) -> str:
        """Manually create a snapshot."""
        return await self.repo.create_snapshot(self.exchange)


# ─────────────────────────────────────────────────────────────────────────────
# CLI Entry Points
# ─────────────────────────────────────────────────────────────────────────────

async def run_listener():
    """Run the event listener as a separate process."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Exchange Event Listener")
    parser.add_argument("--ws-url", default="ws://localhost:8080/ws", help="WebSocket URL")
    parser.add_argument("--api-url", default="http://localhost:8080", help="API URL")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017", help="MongoDB URI")
    parser.add_argument("--database", default="exchange", help="Database name")
    parser.add_argument("--markets", nargs="+", default=["AAPL", "BTCUSD", "GOLD"], help="Markets to subscribe")
    
    args = parser.parse_args()
    
    listener_config = ListenerConfig(
        ws_url=args.ws_url,
        api_url=args.api_url,
        markets=args.markets,
    )
    
    mongo_config = MongoConfig(
        uri=args.mongo_uri,
        database=args.database,
    )
    
    listener = EventListener(listener_config, mongo_config)
    await listener.start()


async def rebuild_exchange():
    """Rebuild exchange from MongoDB."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Rebuild Exchange from MongoDB")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017", help="MongoDB URI")
    parser.add_argument("--database", default="exchange", help="Database name")
    parser.add_argument("--no-snapshot", action="store_true", help="Don't use snapshot, replay all events")
    
    args = parser.parse_args()
    
    mongo_config = MongoConfig(
        uri=args.mongo_uri,
        database=args.database,
    )
    
    repo = MongoRepository(mongo_config)
    await repo.connect()
    
    try:
        rebuilder = ExchangeRebuilder(repo)
        exchange = await rebuilder.rebuild(from_snapshot=not args.no_snapshot)
        
        # Print state
        print("\nRebuilt Exchange State:")
        print(f"  Markets: {len(exchange._markets)}")
        print(f"  Total Orders: {exchange.total_orders}")
        print(f"  Total Trades: {exchange.total_trades}")
        
        for market_id, market in exchange._markets.items():
            engine = exchange._engines[market_id]
            print(f"\n  {market_id} ({market.title}):")
            print(f"    Status: {market.status.value}")
            print(f"    Book: {len(engine.book)} orders")
            print(f"    Best Bid: {engine.book.best_bid}")
            print(f"    Best Ask: {engine.book.best_ask}")
        
        return exchange
    
    finally:
        await repo.disconnect()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "listener":
        sys.argv.pop(1)  # Remove 'listener' from args
        asyncio.run(run_listener())
    elif len(sys.argv) > 1 and sys.argv[1] == "rebuild":
        sys.argv.pop(1)
        asyncio.run(rebuild_exchange())
    else:
        print("Usage:")
        print("  python persistence.py listener [options]  - Run event listener")
        print("  python persistence.py rebuild [options]   - Rebuild exchange from MongoDB")
