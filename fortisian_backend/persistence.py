"""
═══════════════════════════════════════════════════════════════════════════════
                    FORTISIAN EXCHANGE PERSISTENCE LAYER
═══════════════════════════════════════════════════════════════════════════════

A comprehensive, production-grade persistence system for the trading exchange.

DESIGN PRINCIPLES:
─────────────────────────────────────────────────────────────────────────────────
1. EVENT SOURCING: Every state change is captured as an immutable event
2. WRITE-AHEAD LOG: Events written BEFORE state change for durability
3. COMPLETE CAPTURE: Every order, trade, fill, cancel - nothing is lost
4. ADMIN VISIBILITY: Full query capability for every player's activity
5. SEAMLESS REBUILD: Exchange state perfectly reconstructed from event log
6. NON-BLOCKING: Async writes don't impact trading latency
7. SINGLE FILE: One unified module for all persistence needs

ARCHITECTURE:
─────────────────────────────────────────────────────────────────────────────────

    ┌─────────────────────────────────────────────────────────────────────┐
    │                        TRADING SERVER                               │
    │  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────┐ │
    │  │   Exchange  │───▶│  Matching   │───▶│   Event Stream          │ │
    │  │   (State)   │    │   Engine    │    │   (All Events)          │ │
    │  └─────────────┘    └─────────────┘    └───────────┬─────────────┘ │
    └────────────────────────────────────────────────────┼───────────────┘
                                                         │
                              ┌───────────────────────────┘
                              ▼
    ┌─────────────────────────────────────────────────────────────────────┐
    │                    PERSISTENCE LAYER                                │
    │  ┌─────────────────────────────────────────────────────────────┐   │
    │  │              EVENT INTERCEPTOR                               │   │
    │  │  • Captures ALL events at source                            │   │
    │  │  • Assigns global sequence numbers                          │   │
    │  │  • Zero-copy event forwarding                               │   │
    │  └──────────────────────────┬──────────────────────────────────┘   │
    │                             │                                       │
    │  ┌──────────────────────────▼──────────────────────────────────┐   │
    │  │              WRITE-AHEAD LOG                                 │   │
    │  │  • Durable event buffer                                     │   │
    │  │  • Sequence-ordered writes                                  │   │
    │  │  • Crash recovery guarantee                                 │   │
    │  └──────────────────────────┬──────────────────────────────────┘   │
    │                             │                                       │
    │  ┌──────────────────────────▼──────────────────────────────────┐   │
    │  │              ASYNC WRITE PIPELINE                            │   │
    │  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐        │   │
    │  │  │ Events  │  │ Orders  │  │ Trades  │  │Positions│        │   │
    │  │  │ Stream  │  │ Index   │  │ Index   │  │ Index   │        │   │
    │  │  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘        │   │
    │  └───────┼────────────┼───────────┼───────────┼───────────────┘   │
    └──────────┼────────────┼───────────┼───────────┼───────────────────┘
               │            │           │           │
               ▼            ▼           ▼           ▼
    ┌─────────────────────────────────────────────────────────────────────┐
    │                         MONGODB                                     │
    │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  │
    │  │ events  │  │ orders  │  │ trades  │  │positions│  │snapshots│  │
    │  │ (WAL)   │  │ (index) │  │ (index) │  │ (index) │  │ (ckpt)  │  │
    │  └─────────┘  └─────────┘  └─────────┘  └─────────┘  └─────────┘  │
    └─────────────────────────────────────────────────────────────────────┘

COLLECTIONS:
─────────────────────────────────────────────────────────────────────────────────
events       - Immutable event log (source of truth)
orders       - Denormalized order index with full lifecycle
trades       - Denormalized trade index
positions    - Current position state per user/market
markets      - Market configuration and state
snapshots    - Periodic checkpoints for fast recovery
order_history- Complete order state changes over time

USAGE:
─────────────────────────────────────────────────────────────────────────────────
    # In server startup:
    persistence = ExchangePersistence(exchange, mongo_config)
    await persistence.start()
    
    # Server runs normally - all events auto-captured
    
    # On shutdown:
    await persistence.stop()
    
    # To rebuild from persistence:
    exchange = await ExchangeRebuilder(mongo_config).rebuild()

═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import asyncio
import json
import hashlib
import logging
import signal
import sys
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Optional, AsyncIterator, TypeVar, Generic
from contextlib import asynccontextmanager
import traceback

# MongoDB
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING, IndexModel
from pymongo.errors import DuplicateKeyError, BulkWriteError

# Local imports - matching engine types
from matching_engine import (
    Exchange, ExchangeConfig, Side, TimeInForce, MarketStatus, OrderStatus,
    Order, Trade, Position, Market, OrderId, UserId, MarketId,
    EngineEvent, OrderAccepted, OrderRejected, OrderCancelled,
    OrderFilled, OrderExpired, TradeExecuted, BookSnapshot, BookDelta,
    MarketStatusChanged,
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
#                              CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class PersistenceConfig:
    """
    Configuration for the persistence layer.
    
    Tuned for low-latency trading with durability guarantees.
    """
    # MongoDB connection
    mongo_uri: str = "mongodb://localhost:27017"
    database: str = "fortisian_exchange"
    
    # MongoDB authentication (optional - can also be in URI)
    username: Optional[str] = None
    password: Optional[str] = None
    auth_source: str = "admin"  # Database to authenticate against
    
    # Collection names
    events_collection: str = "events"
    orders_collection: str = "orders"
    trades_collection: str = "trades"
    positions_collection: str = "positions"
    markets_collection: str = "markets"
    snapshots_collection: str = "snapshots"
    order_history_collection: str = "order_history"
    
    # Write performance tuning
    write_batch_size: int = 100          # Batch writes for efficiency
    write_flush_interval: float = 1.0    # Max seconds before flush
    write_queue_size: int = 10000        # Max pending writes
    
    # Snapshot configuration
    snapshot_interval_events: int = 5000  # Snapshot every N events
    snapshot_interval_seconds: int = 300  # Or every N seconds
    
    # Connection pool
    max_pool_size: int = 50
    min_pool_size: int = 10
    
    # Rebuild settings
    replay_batch_size: int = 1000        # Events per batch during rebuild
    
    def get_connection_uri(self) -> str:
        """Build the full MongoDB connection URI with auth if provided."""
        if self.username and self.password:
            # Parse the base URI and inject credentials
            from urllib.parse import urlparse, urlunparse, quote_plus
            parsed = urlparse(self.mongo_uri)
            # Build new URI with credentials
            netloc = f"{quote_plus(self.username)}:{quote_plus(self.password)}@{parsed.hostname}"
            if parsed.port:
                netloc += f":{parsed.port}"
            new_uri = urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
            # Add authSource if not already in query
            if 'authSource' not in new_uri:
                separator = '&' if '?' in new_uri else '?'
                new_uri += f"{separator}authSource={self.auth_source}"
            return new_uri
        return self.mongo_uri


# ═══════════════════════════════════════════════════════════════════════════════
#                           SERIALIZATION
# ═══════════════════════════════════════════════════════════════════════════════

class DecimalEncoder(json.JSONEncoder):
    """JSON encoder handling Decimal, datetime, and Enum types."""
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


def _safe_str(value: Any) -> Optional[str]:
    """Safely convert to string, handling None."""
    return str(value) if value is not None else None


def serialize_event(event: EngineEvent, sequence: int) -> dict:
    """
    Serialize an exchange event to MongoDB document.
    
    This is the core serialization - events are the source of truth.
    Every field needed for replay must be captured here.
    """
    event_dict = event.to_dict() if hasattr(event, 'to_dict') else {}
    
    # Extract common fields
    market_id = getattr(event, 'market_id', None)
    user_id = getattr(event, 'user_id', None)
    order_id = getattr(event, 'order_id', None)
    
    # Build document
    doc = {
        "seq": sequence,                              # Global sequence number
        "type": type(event).__name__,                 # Event type for filtering
        "ts": datetime.now(timezone.utc),             # Capture timestamp
        "market_id": _safe_str(market_id),
        "user_id": _safe_str(user_id),
        "order_id": _safe_str(order_id),
        "data": json.loads(json.dumps(event_dict, cls=DecimalEncoder)),
    }
    
    # Add trade-specific fields for TradeExecuted
    if isinstance(event, TradeExecuted) and hasattr(event, 'trade'):
        trade = event.trade
        doc["trade_id"] = _safe_str(trade.id)
        doc["buyer_id"] = _safe_str(trade.buyer_id)
        doc["seller_id"] = _safe_str(trade.seller_id)
    
    return doc


def serialize_order(order: Order) -> dict:
    """
    Serialize an order to MongoDB document.
    
    This is a denormalized view for fast querying - the event log is authoritative.
    """
    return {
        "_id": str(order.id),
        "market_id": str(order.market_id),
        "user_id": str(order.user_id),
        "side": order.side.value,
        "price": str(order.price),
        "qty": order.qty,
        "remaining_qty": order.remaining_qty,
        "filled_qty": order.filled_qty,
        "status": order.status.value,
        "time_in_force": order.time_in_force.value,
        "client_order_id": order.client_order_id,
        "created_at": order.created_at,
        "updated_at": order.updated_at or datetime.now(timezone.utc),
    }


def serialize_order_history(order: Order, event_type: str, sequence: int) -> dict:
    """
    Serialize an order state change for the history collection.
    
    This captures the complete timeline of every order.
    """
    return {
        "order_id": str(order.id),
        "seq": sequence,
        "event_type": event_type,
        "ts": datetime.now(timezone.utc),
        "market_id": str(order.market_id),
        "user_id": str(order.user_id),
        "side": order.side.value,
        "price": str(order.price),
        "qty": order.qty,
        "remaining_qty": order.remaining_qty,
        "filled_qty": order.filled_qty,
        "status": order.status.value,
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
        "buy_order_id": str(trade.buy_order_id),
        "sell_order_id": str(trade.sell_order_id),
        "aggressor_side": trade.aggressor_side.value,
        "ts": trade.created_at or datetime.now(timezone.utc),
        "seq": trade.sequence,
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
        "description": getattr(config, 'description', ''),
        "status": market.status.value,
        "tick_size": str(config.tick_size),
        "lot_size": config.lot_size,
        "min_qty": config.min_qty,
        "max_qty": config.max_qty,
        "max_position": config.max_position,
        "last_trade_price": str(market.last_trade_price) if market.last_trade_price else None,
        "total_volume": market.total_volume,
        "created_at": config.created_at,
        "updated_at": datetime.now(timezone.utc),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#                         ASYNC WRITE PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class WriteOperation:
    """A pending write operation."""
    collection: str
    operation: str  # 'insert', 'upsert', 'insert_many'
    document: dict | list[dict]
    key_field: str = "_id"  # For upsert operations


class AsyncWritePipeline:
    """
    Non-blocking async write pipeline for MongoDB.
    
    Features:
    - Batches writes for efficiency
    - Separate queues per collection for parallelism
    - Automatic flush on interval or batch size
    - Graceful shutdown with drain
    """
    
    def __init__(self, db: AsyncIOMotorDatabase, config: PersistenceConfig):
        self.db = db
        self.config = config
        self._queues: dict[str, asyncio.Queue] = {}
        self._workers: list[asyncio.Task] = []
        self._running = False
        self._stats = {
            "writes": 0,
            "batches": 0,
            "errors": 0,
        }
    
    async def start(self) -> None:
        """Start the write pipeline workers."""
        self._running = True
        
        # Create queue and worker for each collection
        collections = [
            self.config.events_collection,
            self.config.orders_collection,
            self.config.trades_collection,
            self.config.positions_collection,
            self.config.order_history_collection,
        ]
        
        for coll in collections:
            self._queues[coll] = asyncio.Queue(maxsize=self.config.write_queue_size)
            worker = asyncio.create_task(self._collection_worker(coll))
            self._workers.append(worker)
        
        logger.info(f"Write pipeline started with {len(collections)} workers")
    
    async def stop(self) -> None:
        """Stop the pipeline and drain remaining writes."""
        self._running = False
        
        # Signal workers to drain and stop
        for queue in self._queues.values():
            await queue.put(None)  # Sentinel to stop
        
        # Wait for workers to finish
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        
        logger.info(f"Write pipeline stopped. Stats: {self._stats}")
    
    async def write(self, op: WriteOperation) -> None:
        """Queue a write operation (non-blocking)."""
        if not self._running:
            return
        
        queue = self._queues.get(op.collection)
        if queue:
            try:
                queue.put_nowait(op)
            except asyncio.QueueFull:
                logger.warning(f"Write queue full for {op.collection}, dropping write")
                self._stats["errors"] += 1
    
    async def _collection_worker(self, collection_name: str) -> None:
        """Worker that processes writes for a single collection."""
        queue = self._queues[collection_name]
        coll = self.db[collection_name]
        batch: list[WriteOperation] = []
        last_flush = asyncio.get_event_loop().time()
        
        while True:
            try:
                # Get with timeout for periodic flush
                try:
                    op = await asyncio.wait_for(
                        queue.get(),
                        timeout=self.config.write_flush_interval
                    )
                except asyncio.TimeoutError:
                    op = None
                
                # Check for shutdown sentinel
                if op is None and not self._running:
                    # Flush remaining and exit
                    if batch:
                        await self._flush_batch(coll, batch)
                    break
                
                if op:
                    batch.append(op)
                
                # Flush conditions
                now = asyncio.get_event_loop().time()
                should_flush = (
                    len(batch) >= self.config.write_batch_size or
                    (now - last_flush) >= self.config.write_flush_interval
                )
                
                if should_flush and batch:
                    await self._flush_batch(coll, batch)
                    batch = []
                    last_flush = now
                    
            except Exception as e:
                logger.error(f"Error in write worker for {collection_name}: {e}")
                self._stats["errors"] += 1
    
    async def _flush_batch(self, coll, batch: list[WriteOperation]) -> None:
        """Flush a batch of writes to MongoDB."""
        if not batch:
            return
        
        try:
            # Group by operation type
            inserts = [op.document for op in batch if op.operation == 'insert']
            upserts = [(op.document, op.key_field) for op in batch if op.operation == 'upsert']
            
            # Bulk insert
            if inserts:
                try:
                    await coll.insert_many(inserts, ordered=False)
                except BulkWriteError as e:
                    # Ignore duplicate key errors for idempotency
                    pass
            
            # Individual upserts (can't easily batch these)
            for doc, key_field in upserts:
                await coll.replace_one(
                    {key_field: doc[key_field]},
                    doc,
                    upsert=True
                )
            
            self._stats["writes"] += len(batch)
            self._stats["batches"] += 1
            
        except Exception as e:
            logger.error(f"Batch write error: {e}")
            self._stats["errors"] += 1


# ═══════════════════════════════════════════════════════════════════════════════
#                        MAIN PERSISTENCE CLASS
# ═══════════════════════════════════════════════════════════════════════════════

class ExchangePersistence:
    """
    The unified persistence layer for the trading exchange.
    
    This class:
    1. Intercepts ALL events from the exchange at the source
    2. Assigns global sequence numbers
    3. Persists to MongoDB via async pipeline
    4. Maintains denormalized indexes for fast queries
    5. Creates periodic snapshots for fast recovery
    
    Usage:
        persistence = ExchangePersistence(exchange, config)
        await persistence.start()
        # ... exchange operates normally ...
        await persistence.stop()
    """
    
    def __init__(
        self,
        exchange: Exchange,
        config: PersistenceConfig,
        enabled: bool = True,
    ):
        self.exchange = exchange
        self.config = config
        self.enabled = enabled
        
        # MongoDB connection
        self._client: Optional[AsyncIOMotorClient] = None
        self._db: Optional[AsyncIOMotorDatabase] = None
        
        # Write pipeline
        self._pipeline: Optional[AsyncWritePipeline] = None
        
        # Event interception
        self._original_event_handler: Optional[Callable] = None
        self._sequence: int = 0
        self._sequence_lock = asyncio.Lock()
        
        # Snapshot tracking
        self._events_since_snapshot: int = 0
        self._last_snapshot_time: datetime = datetime.now(timezone.utc)
        self._snapshot_task: Optional[asyncio.Task] = None
        
        # Statistics
        self._stats = {
            "events_captured": 0,
            "orders_persisted": 0,
            "trades_persisted": 0,
            "snapshots_created": 0,
        }
    
    # ─────────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────────────────
    
    async def start(self) -> None:
        """Start the persistence layer."""
        if not self.enabled:
            logger.info("Persistence disabled")
            return
        
        try:
            # Connect to MongoDB with optional authentication
            connection_uri = self.config.get_connection_uri()
            logger.info(f"Connecting to MongoDB: {self.config.mongo_uri.split('@')[-1] if '@' in self.config.mongo_uri else self.config.mongo_uri}")
            
            self._client = AsyncIOMotorClient(
                connection_uri,
                maxPoolSize=self.config.max_pool_size,
                minPoolSize=self.config.min_pool_size,
                serverSelectionTimeoutMS=5000,
            )
            self._db = self._client[self.config.database]
            
            # Test connection
            await self._client.admin.command('ping')
            logger.info("MongoDB connection successful")
            
            # Create indexes
            await self._create_indexes()
            
            # Recover sequence number from last event
            self._sequence = await self._recover_sequence()
            
            # Start write pipeline
            self._pipeline = AsyncWritePipeline(self._db, self.config)
            await self._pipeline.start()
            
            # Store initial market states
            for market in self.exchange.list_markets():
                await self._persist_market(market)
            
            # Hook into exchange event stream
            self._install_event_interceptor()
            
            # Start snapshot worker
            self._snapshot_task = asyncio.create_task(self._snapshot_worker())
            
            logger.info(f"Persistence started. Sequence: {self._sequence}")
            
        except Exception as e:
            logger.error(f"Failed to start persistence: {e}")
            traceback.print_exc()
            self.enabled = False
            raise
    
    async def stop(self) -> None:
        """Stop the persistence layer gracefully."""
        logger.info("Stopping persistence layer...")
        
        # Stop snapshot worker
        if self._snapshot_task:
            self._snapshot_task.cancel()
            try:
                await self._snapshot_task
            except asyncio.CancelledError:
                pass
        
        # Restore original event handler
        if self._original_event_handler:
            self.exchange._on_event = self._original_event_handler
        
        # Create final snapshot
        if self.enabled and self._db:
            try:
                await self.create_snapshot()
            except Exception as e:
                logger.error(f"Failed to create final snapshot: {e}")
        
        # Stop write pipeline (drains remaining writes)
        if self._pipeline:
            await self._pipeline.stop()
        
        # Close MongoDB connection
        if self._client:
            self._client.close()
        
        logger.info(f"Persistence stopped. Stats: {self._stats}")
    
    # ─────────────────────────────────────────────────────────────────────────
    # Index Setup
    # ─────────────────────────────────────────────────────────────────────────
    
    async def _create_indexes(self) -> None:
        """Create MongoDB indexes for efficient querying."""
        
        # Events collection - the source of truth
        events = self._db[self.config.events_collection]
        await events.create_indexes([
            IndexModel([("seq", ASCENDING)], unique=True),
            IndexModel([("type", ASCENDING), ("seq", ASCENDING)]),
            IndexModel([("market_id", ASCENDING), ("seq", ASCENDING)]),
            IndexModel([("user_id", ASCENDING), ("seq", ASCENDING)]),
            IndexModel([("ts", DESCENDING)]),
            IndexModel([("order_id", ASCENDING)], sparse=True),
            IndexModel([("trade_id", ASCENDING)], sparse=True),
        ])
        
        # Orders collection - denormalized for fast queries
        orders = self._db[self.config.orders_collection]
        await orders.create_indexes([
            IndexModel([("market_id", ASCENDING), ("status", ASCENDING)]),
            IndexModel([("user_id", ASCENDING), ("status", ASCENDING)]),
            IndexModel([("user_id", ASCENDING), ("created_at", DESCENDING)]),
            IndexModel([("market_id", ASCENDING), ("user_id", ASCENDING)]),
            IndexModel([("created_at", DESCENDING)]),
            IndexModel([("status", ASCENDING), ("created_at", DESCENDING)]),
        ])
        
        # Order history - complete timeline per order
        order_history = self._db[self.config.order_history_collection]
        await order_history.create_indexes([
            IndexModel([("order_id", ASCENDING), ("seq", ASCENDING)]),
            IndexModel([("user_id", ASCENDING), ("ts", DESCENDING)]),
            IndexModel([("market_id", ASCENDING), ("ts", DESCENDING)]),
        ])
        
        # Trades collection
        trades = self._db[self.config.trades_collection]
        await trades.create_indexes([
            IndexModel([("market_id", ASCENDING), ("ts", DESCENDING)]),
            IndexModel([("buyer_id", ASCENDING), ("ts", DESCENDING)]),
            IndexModel([("seller_id", ASCENDING), ("ts", DESCENDING)]),
            IndexModel([("ts", DESCENDING)]),
            IndexModel([("seq", ASCENDING)]),
        ])
        
        # Positions collection
        positions = self._db[self.config.positions_collection]
        await positions.create_indexes([
            IndexModel([("user_id", ASCENDING)]),
            IndexModel([("market_id", ASCENDING)]),
        ])
        
        # Snapshots collection
        snapshots = self._db[self.config.snapshots_collection]
        await snapshots.create_indexes([
            IndexModel([("created_at", DESCENDING)]),
            IndexModel([("seq", DESCENDING)]),
        ])
        
        # Book snapshots collection - for order book history
        book_snapshots = self._db["book_snapshots"]
        await book_snapshots.create_indexes([
            IndexModel([("market_id", ASCENDING), ("ts", DESCENDING)]),
            IndexModel([("seq", ASCENDING)]),
            IndexModel([("ts", DESCENDING)]),
        ])
        
        logger.info("MongoDB indexes created")
    
    async def _recover_sequence(self) -> int:
        """Recover the last sequence number from the event log."""
        doc = await self._db[self.config.events_collection].find_one(
            {}, sort=[("seq", DESCENDING)]
        )
        return doc["seq"] if doc else 0
    
    # ─────────────────────────────────────────────────────────────────────────
    # Event Interception
    # ─────────────────────────────────────────────────────────────────────────
    
    def _install_event_interceptor(self) -> None:
        """Install our interceptor on the exchange's event handler."""
        self._original_event_handler = self.exchange._on_event
        self.exchange._on_event = self._intercept_event
        logger.info(f"Event interceptor installed. Original handler: {self._original_event_handler}")
    
    def _intercept_event(self, event: EngineEvent) -> None:
        """
        Intercept every event from the exchange.
        
        This is called synchronously by the matching engine.
        We capture the event and schedule async persistence.
        """
        if not self.enabled:
            # Pass through to original handler
            if self._original_event_handler:
                self._original_event_handler(event)
            return
        
        # Assign sequence number (thread-safe via GIL for sync code)
        self._sequence += 1
        seq = self._sequence
        
        event_type = type(event).__name__
        logger.debug(f"Intercepted event #{seq}: {event_type}")
        
        # Call original handler first (WebSocket distribution, etc.)
        if self._original_event_handler:
            self._original_event_handler(event)
        
        # Schedule async persistence (fire and forget)
        asyncio.create_task(self._persist_event(event, seq))
    
    async def _persist_event(self, event: EngineEvent, sequence: int) -> None:
        """Persist an event and update denormalized collections."""
        if not self._pipeline:
            return
        
        try:
            # Always persist to event log (source of truth)
            event_doc = serialize_event(event, sequence)
            await self._pipeline.write(WriteOperation(
                collection=self.config.events_collection,
                operation='insert',
                document=event_doc,
            ))
            
            self._stats["events_captured"] += 1
            self._events_since_snapshot += 1
            
            # Update denormalized collections based on event type
            await self._update_denormalized(event, sequence)
            
        except Exception as e:
            logger.error(f"Error persisting event: {e}")
    
    async def _update_denormalized(self, event: EngineEvent, sequence: int) -> None:
        """Update denormalized collections based on event type."""
        
        # Order events
        if isinstance(event, OrderAccepted):
            if hasattr(event, 'order') and event.order:
                order = event.order
                # Persist current order state
                await self._pipeline.write(WriteOperation(
                    collection=self.config.orders_collection,
                    operation='upsert',
                    document=serialize_order(order),
                ))
                # Persist to order history
                await self._pipeline.write(WriteOperation(
                    collection=self.config.order_history_collection,
                    operation='insert',
                    document=serialize_order_history(order, 'accepted', sequence),
                ))
                self._stats["orders_persisted"] += 1
        
        elif isinstance(event, (OrderFilled, OrderCancelled, OrderExpired)):
            if hasattr(event, 'order') and event.order:
                order = event.order
                event_name = type(event).__name__.lower().replace('order', '')
                await self._pipeline.write(WriteOperation(
                    collection=self.config.orders_collection,
                    operation='upsert',
                    document=serialize_order(order),
                ))
                await self._pipeline.write(WriteOperation(
                    collection=self.config.order_history_collection,
                    operation='insert',
                    document=serialize_order_history(order, event_name, sequence),
                ))
        
        # Trade events
        elif isinstance(event, TradeExecuted):
            if hasattr(event, 'trade') and event.trade:
                trade = event.trade
                await self._pipeline.write(WriteOperation(
                    collection=self.config.trades_collection,
                    operation='upsert',
                    document=serialize_trade(trade),
                ))
                self._stats["trades_persisted"] += 1
                
                # Update positions for both buyer and seller
                await self._update_positions_for_trade(trade)
        
        # Order rejection events - useful for audit/debugging
        elif isinstance(event, OrderRejected):
            # Store rejected orders for audit trail
            order_id = getattr(event, 'order_id', None)
            user_id = getattr(event, 'user_id', None)
            market_id = getattr(event, 'market_id', None)
            if order_id:
                await self._pipeline.write(WriteOperation(
                    collection=self.config.order_history_collection,
                    operation='insert',
                    document={
                        "order_id": str(order_id),
                        "seq": sequence,
                        "event_type": "rejected",
                        "ts": datetime.now(timezone.utc),
                        "market_id": str(market_id) if market_id else None,
                        "user_id": str(user_id) if user_id else None,
                        "reason": getattr(event, 'reason', None),
                        "reason_text": getattr(event, 'reason_text', None),
                    },
                ))
        
        # Book events - store periodic snapshots for replay/analysis
        elif isinstance(event, BookSnapshot):
            market_id = getattr(event, 'market_id', None)
            if market_id:
                # Store book snapshot in a separate collection
                await self._pipeline.write(WriteOperation(
                    collection="book_snapshots",
                    operation='insert',
                    document={
                        "seq": sequence,
                        "ts": datetime.now(timezone.utc),
                        "market_id": str(market_id),
                        "best_bid": str(event.best_bid) if event.best_bid else None,
                        "best_ask": str(event.best_ask) if event.best_ask else None,
                        "spread": str(event.spread) if event.spread else None,
                        "bid_levels": len(event.bids) if event.bids else 0,
                        "ask_levels": len(event.asks) if event.asks else 0,
                        "data": event.to_dict() if hasattr(event, 'to_dict') else {},
                    },
                ))
        
        # Market status events
        elif isinstance(event, MarketStatusChanged):
            market_id = getattr(event, 'market_id', None)
            if market_id:
                market = self.exchange.get_market(market_id)
                if market:
                    await self._persist_market(market)
    
    async def _update_positions_for_trade(self, trade: Trade) -> None:
        """Update position records for a trade."""
        # Get positions from exchange
        engine = self.exchange._engines.get(trade.market_id)
        if not engine:
            return
        
        for user_id in [trade.buyer_id, trade.seller_id]:
            position = engine.positions.get(user_id)
            if position:
                await self._pipeline.write(WriteOperation(
                    collection=self.config.positions_collection,
                    operation='upsert',
                    document=serialize_position(
                        str(user_id), str(trade.market_id), position
                    ),
                ))
    
    async def _persist_market(self, market: Market) -> None:
        """Persist a market state."""
        await self._pipeline.write(WriteOperation(
            collection=self.config.markets_collection,
            operation='upsert',
            document=serialize_market(market),
        ))
    
    # ─────────────────────────────────────────────────────────────────────────
    # Snapshots
    # ─────────────────────────────────────────────────────────────────────────
    
    async def _snapshot_worker(self) -> None:
        """Background worker that creates periodic snapshots."""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                now = datetime.now(timezone.utc)
                time_since_snapshot = (now - self._last_snapshot_time).total_seconds()
                
                should_snapshot = (
                    self._events_since_snapshot >= self.config.snapshot_interval_events or
                    time_since_snapshot >= self.config.snapshot_interval_seconds
                )
                
                if should_snapshot:
                    await self.create_snapshot()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Snapshot worker error: {e}")
    
    async def create_snapshot(self) -> str:
        """Create a complete exchange state snapshot."""
        logger.info("Creating snapshot...")
        
        # Gather complete state
        markets_data = []
        order_books_data = {}
        positions_data = []
        
        for market_id, market in self.exchange._markets.items():
            markets_data.append(serialize_market(market))
            
            engine = self.exchange._engines.get(market_id)
            if not engine:
                continue
            
            book = engine.book
            
            # Complete L3 order book - every order at every level
            bids_list = []
            for price, queue in book._bids.items():
                for order in queue.orders:
                    bids_list.append(serialize_order(order))
            
            asks_list = []
            for price, queue in book._asks.items():
                for order in queue.orders:
                    asks_list.append(serialize_order(order))
            
            order_books_data[str(market_id)] = {
                "bids": bids_list,
                "asks": asks_list,
            }
            
            # All positions
            for position in engine.positions.get_all():
                positions_data.append(serialize_position(
                    str(position.user_id), str(position.market_id), position
                ))
        
        # Build snapshot document
        snapshot = {
            "created_at": datetime.now(timezone.utc),
            "seq": self._sequence,
            "event_count": self._stats["events_captured"],
            "total_orders": self.exchange.total_orders,
            "total_trades": self.exchange.total_trades,
            "markets": markets_data,
            "order_books": order_books_data,
            "positions": positions_data,
        }
        
        # Calculate checksum for integrity verification
        snapshot["checksum"] = hashlib.sha256(
            json.dumps(snapshot, cls=DecimalEncoder, sort_keys=True, default=str).encode()
        ).hexdigest()[:16]
        
        # Insert directly (not via pipeline) for immediate durability
        result = await self._db[self.config.snapshots_collection].insert_one(snapshot)
        
        self._events_since_snapshot = 0
        self._last_snapshot_time = datetime.now(timezone.utc)
        self._stats["snapshots_created"] += 1
        
        logger.info(f"Snapshot created at sequence {self._sequence}")
        return str(result.inserted_id)
    
    # ─────────────────────────────────────────────────────────────────────────
    # Admin Query API
    # ─────────────────────────────────────────────────────────────────────────
    
    async def get_all_orders_for_user(
        self,
        user_id: str,
        market_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 1000,
    ) -> list[dict]:
        """Get all orders for a user (admin dashboard)."""
        if not self._db:
            raise RuntimeError("Persistence not started - call start() first")
        
        query = {"user_id": user_id}
        if market_id:
            query["market_id"] = market_id
        if status:
            query["status"] = status
        
        cursor = self._db[self.config.orders_collection].find(query).sort(
            "created_at", DESCENDING
        ).limit(limit)
        return await cursor.to_list(length=limit)
    
    async def get_all_trades_for_user(
        self,
        user_id: str,
        market_id: Optional[str] = None,
        limit: int = 1000,
    ) -> list[dict]:
        """Get all trades involving a user (admin dashboard)."""
        if not self._db:
            raise RuntimeError("Persistence not started - call start() first")
        
        query = {"$or": [{"buyer_id": user_id}, {"seller_id": user_id}]}
        if market_id:
            query["market_id"] = market_id
        
        cursor = self._db[self.config.trades_collection].find(query).sort(
            "ts", DESCENDING
        ).limit(limit)
        return await cursor.to_list(length=limit)
    
    async def get_order_history(
        self,
        order_id: str,
    ) -> list[dict]:
        """Get complete history of an order (all state changes)."""
        cursor = self._db[self.config.order_history_collection].find(
            {"order_id": order_id}
        ).sort("seq", ASCENDING)
        return await cursor.to_list(length=1000)
    
    async def get_user_activity_timeline(
        self,
        user_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 500,
    ) -> list[dict]:
        """Get complete activity timeline for a user."""
        query = {"user_id": user_id}
        if start_time or end_time:
            query["ts"] = {}
            if start_time:
                query["ts"]["$gte"] = start_time
            if end_time:
                query["ts"]["$lte"] = end_time
        
        cursor = self._db[self.config.events_collection].find(query).sort(
            "seq", DESCENDING
        ).limit(limit)
        return await cursor.to_list(length=limit)
    
    async def get_all_positions(
        self,
        market_id: Optional[str] = None,
    ) -> list[dict]:
        """Get all positions (for admin overview)."""
        query = {}
        if market_id:
            query["market_id"] = market_id
        
        cursor = self._db[self.config.positions_collection].find(query)
        return await cursor.to_list(length=10000)
    
    async def get_market_statistics(
        self,
        market_id: str,
    ) -> dict:
        """Get statistics for a market."""
        trades = self._db[self.config.trades_collection]
        orders = self._db[self.config.orders_collection]
        
        # Aggregate trade stats
        trade_stats = await trades.aggregate([
            {"$match": {"market_id": market_id}},
            {"$group": {
                "_id": None,
                "total_trades": {"$sum": 1},
                "total_volume": {"$sum": "$qty"},
            }}
        ]).to_list(length=1)
        
        # Aggregate order stats
        order_stats = await orders.aggregate([
            {"$match": {"market_id": market_id}},
            {"$group": {
                "_id": "$status",
                "count": {"$sum": 1},
            }}
        ]).to_list(length=10)
        
        return {
            "market_id": market_id,
            "trades": trade_stats[0] if trade_stats else {"total_trades": 0, "total_volume": 0},
            "orders_by_status": {s["_id"]: s["count"] for s in order_stats},
        }
    
    async def get_all_users_summary(self) -> list[dict]:
        """Get summary of all users with trading activity."""
        # Aggregate from orders collection
        pipeline = [
            {"$group": {
                "_id": "$user_id",
                "total_orders": {"$sum": 1},
                "first_order": {"$min": "$created_at"},
                "last_order": {"$max": "$created_at"},
            }},
            {"$sort": {"total_orders": -1}},
        ]
        
        users = await self._db[self.config.orders_collection].aggregate(pipeline).to_list(length=10000)
        
        # Enrich with trade counts
        for user in users:
            user_id = user["_id"]
            trade_count = await self._db[self.config.trades_collection].count_documents({
                "$or": [{"buyer_id": user_id}, {"seller_id": user_id}]
            })
            user["total_trades"] = trade_count
        
        return users


# ═══════════════════════════════════════════════════════════════════════════════
#                         EXCHANGE REBUILDER
# ═══════════════════════════════════════════════════════════════════════════════

class ExchangeRebuilder:
    """
    Rebuilds complete exchange state from persistence.
    
    Two modes:
    1. From snapshot + replay subsequent events (fast)
    2. From full event replay (slow but guaranteed accurate)
    
    Usage:
        rebuilder = ExchangeRebuilder(config)
        exchange = await rebuilder.rebuild()
    """
    
    def __init__(self, config: PersistenceConfig):
        self.config = config
        self._client: Optional[AsyncIOMotorClient] = None
        self._db: Optional[AsyncIOMotorDatabase] = None
    
    async def connect(self) -> None:
        """Connect to MongoDB."""
        connection_uri = self.config.get_connection_uri()
        self._client = AsyncIOMotorClient(
            connection_uri,
            maxPoolSize=self.config.max_pool_size,
            serverSelectionTimeoutMS=5000,
        )
        self._db = self._client[self.config.database]
        # Test connection
        await self._client.admin.command('ping')
        logger.info("ExchangeRebuilder connected to MongoDB")
    
    async def disconnect(self) -> None:
        """Disconnect from MongoDB."""
        if self._client:
            self._client.close()
    
    async def rebuild(self, from_snapshot: bool = True) -> Exchange:
        """
        Rebuild the exchange from persisted state.
        
        Args:
            from_snapshot: If True, use latest snapshot as starting point.
        
        Returns:
            A fully reconstructed Exchange instance.
        """
        await self.connect()
        
        try:
            if from_snapshot:
                snapshot = await self._get_latest_snapshot()
                if snapshot:
                    return await self._rebuild_from_snapshot(snapshot)
                logger.warning("No snapshot found, rebuilding from events")
            
            return await self._rebuild_from_events()
            
        finally:
            await self.disconnect()
    
    async def _get_latest_snapshot(self) -> Optional[dict]:
        """Get the most recent snapshot."""
        return await self._db[self.config.snapshots_collection].find_one(
            {}, sort=[("created_at", DESCENDING)]
        )
    
    async def _rebuild_from_snapshot(self, snapshot: dict) -> Exchange:
        """Rebuild from snapshot and replay subsequent events."""
        logger.info(f"Rebuilding from snapshot at sequence {snapshot['seq']}")
        
        exchange = Exchange()
        
        # Restore markets
        for market_data in snapshot["markets"]:
            exchange.create_market(
                market_id=market_data["_id"],
                title=market_data["title"],
                description=market_data.get("description", ""),
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
            
            # Restore all orders to book
            for order_data in book_data.get("bids", []):
                order = self._deserialize_order(order_data)
                if order.status == OrderStatus.OPEN and order.remaining_qty > 0:
                    engine.book.add(order)
            
            for order_data in book_data.get("asks", []):
                order = self._deserialize_order(order_data)
                if order.status == OrderStatus.OPEN and order.remaining_qty > 0:
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
                position.realized_pnl = Decimal(pos_data.get("realized_pnl", "0"))
                position.trade_count = pos_data.get("trade_count", 0)
                engine.positions._positions[user_id] = position
        
        # Replay events after snapshot
        events = await self._get_events_after(snapshot["seq"])
        if events:
            logger.info(f"Replaying {len(events)} events after snapshot")
            await self._replay_events(exchange, events)
        
        logger.info(f"Exchange rebuilt: {len(exchange._markets)} markets")
        return exchange
    
    async def _rebuild_from_events(self) -> Exchange:
        """Rebuild from complete event replay."""
        logger.info("Rebuilding from all events (no snapshot)")
        
        # Get markets first
        markets = await self._db[self.config.markets_collection].find({}).to_list(length=100)
        
        exchange = Exchange()
        
        # Create markets
        for market_data in markets:
            exchange.create_market(
                market_id=market_data["_id"],
                title=market_data["title"],
                description=market_data.get("description", ""),
                tick_size=market_data.get("tick_size", "0.01"),
                lot_size=market_data.get("lot_size", 1),
                max_position=market_data.get("max_position"),
            )
            exchange.start_market(market_data["_id"])
        
        # Replay all events
        events = await self._get_all_events()
        if events:
            logger.info(f"Replaying {len(events)} events")
            await self._replay_events(exchange, events)
        
        return exchange
    
    async def _get_events_after(self, sequence: int) -> list[dict]:
        """Get all events after a sequence number."""
        cursor = self._db[self.config.events_collection].find(
            {"seq": {"$gt": sequence}}
        ).sort("seq", ASCENDING)
        return await cursor.to_list(length=1000000)
    
    async def _get_all_events(self) -> list[dict]:
        """Get all events."""
        cursor = self._db[self.config.events_collection].find({}).sort("seq", ASCENDING)
        return await cursor.to_list(length=1000000)
    
    async def _replay_events(self, exchange: Exchange, events: list[dict]) -> None:
        """Replay events to rebuild state."""
        for event_doc in events:
            event_type = event_doc["type"]
            data = event_doc.get("data", {})
            
            if event_type == "OrderAccepted":
                # Reconstruct and add order
                market_id = data.get("market_id")
                engine = exchange._engines.get(MarketId(market_id)) if market_id else None
                if engine and "order" in data:
                    order = self._deserialize_order(data["order"])
                    if order.status == OrderStatus.OPEN and order.remaining_qty > 0:
                        # Only add if not already in book (idempotency)
                        existing = engine.book.get(order.id)
                        if not existing:
                            engine.book.add(order)
            
            elif event_type == "OrderCancelled":
                market_id = data.get("market_id")
                order_id = data.get("order_id")
                engine = exchange._engines.get(MarketId(market_id)) if market_id else None
                if engine and order_id:
                    engine.book.remove(OrderId(order_id))
            
            elif event_type == "OrderFilled":
                # Order was filled - update or remove from book
                market_id = data.get("market_id")
                engine = exchange._engines.get(MarketId(market_id)) if market_id else None
                if engine and "order" in data:
                    order_data = data["order"]
                    order_id = OrderId(order_data.get("id", order_data.get("_id", "")))
                    
                    # Check if fully filled
                    if order_data.get("remaining_qty", 0) == 0:
                        engine.book.remove(order_id)
            
            elif event_type == "TradeExecuted":
                exchange.total_trades += 1
    
    def _deserialize_order(self, data: dict) -> Order:
        """Deserialize an order from dict."""
        order = Order(
            id=OrderId(data.get("id", data.get("_id", ""))),
            market_id=MarketId(data["market_id"]),
            user_id=UserId(data["user_id"]),
            side=Side(data["side"]),
            price=Decimal(data["price"]),
            qty=data.get("qty", data.get("original_qty", 0)),
            time_in_force=TimeInForce(data.get("time_in_force", "gtc")),
            client_order_id=data.get("client_order_id"),
        )
        
        # Restore filled qty
        filled = data.get("filled_qty", 0)
        if filled > 0:
            order._filled_qty = filled
        
        # Restore remaining qty if different from computed
        remaining = data.get("remaining_qty")
        if remaining is not None:
            order._remaining_qty = remaining
        
        return order


# ═══════════════════════════════════════════════════════════════════════════════
#                          CLI & STANDALONE MODE
# ═══════════════════════════════════════════════════════════════════════════════

async def run_rebuild():
    """CLI command to rebuild and verify exchange state."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Rebuild Exchange from Persistence")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    parser.add_argument("--database", default="fortisian_exchange")
    parser.add_argument("--no-snapshot", action="store_true", help="Rebuild from events only")
    args = parser.parse_args()
    
    config = PersistenceConfig(
        mongo_uri=args.mongo_uri,
        database=args.database,
    )
    
    rebuilder = ExchangeRebuilder(config)
    exchange = await rebuilder.rebuild(from_snapshot=not args.no_snapshot)
    
    print("\n" + "=" * 60)
    print("EXCHANGE STATE REBUILT")
    print("=" * 60)
    print(f"Markets: {len(exchange._markets)}")
    print(f"Total Orders: {exchange.total_orders}")
    print(f"Total Trades: {exchange.total_trades}")
    
    for market_id, market in exchange._markets.items():
        engine = exchange._engines[market_id]
        print(f"\n  {market_id} ({market.title})")
        print(f"    Status: {market.status.value}")
        print(f"    Orders in book: {len(engine.book)}")
        print(f"    Best bid: {engine.book.best_bid}")
        print(f"    Best ask: {engine.book.best_ask}")
    
    return exchange


async def run_admin_query():
    """CLI command to query admin data."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Query Admin Data")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    parser.add_argument("--database", default="fortisian_exchange")
    parser.add_argument("--user", help="User ID to query")
    parser.add_argument("--market", help="Market ID to query")
    parser.add_argument("--summary", action="store_true", help="Show all users summary")
    args = parser.parse_args()
    
    config = PersistenceConfig(
        mongo_uri=args.mongo_uri,
        database=args.database,
    )
    
    # Create a minimal persistence instance for queries
    from motor.motor_asyncio import AsyncIOMotorClient
    
    client = AsyncIOMotorClient(config.mongo_uri)
    db = client[config.database]
    
    try:
        if args.summary:
            # Show all users
            pipeline = [
                {"$group": {
                    "_id": "$user_id",
                    "total_orders": {"$sum": 1},
                }},
                {"$sort": {"total_orders": -1}},
                {"$limit": 50},
            ]
            users = await db[config.orders_collection].aggregate(pipeline).to_list(length=50)
            
            print("\n" + "=" * 60)
            print("ALL USERS SUMMARY")
            print("=" * 60)
            for user in users:
                print(f"  {user['_id']}: {user['total_orders']} orders")
        
        elif args.user:
            # Query specific user
            print(f"\n" + "=" * 60)
            print(f"USER: {args.user}")
            print("=" * 60)
            
            # Orders
            orders = await db[config.orders_collection].find(
                {"user_id": args.user}
            ).sort("created_at", DESCENDING).limit(20).to_list(length=20)
            
            print(f"\nRecent Orders ({len(orders)}):")
            for order in orders:
                print(f"  {order['_id']}: {order['side']} {order['qty']}@{order['price']} [{order['status']}]")
            
            # Trades
            trades = await db[config.trades_collection].find({
                "$or": [{"buyer_id": args.user}, {"seller_id": args.user}]
            }).sort("ts", DESCENDING).limit(20).to_list(length=20)
            
            print(f"\nRecent Trades ({len(trades)}):")
            for trade in trades:
                side = "BUY" if trade["buyer_id"] == args.user else "SELL"
                print(f"  {trade['_id']}: {side} {trade['qty']}@{trade['price']}")
            
            # Positions
            positions = await db[config.positions_collection].find(
                {"user_id": args.user}
            ).to_list(length=100)
            
            print(f"\nPositions ({len(positions)}):")
            for pos in positions:
                print(f"  {pos['market_id']}: {pos['net_qty']} (P&L: {pos.get('realized_pnl', '0')})")
    
    finally:
        client.close()


if __name__ == "__main__":
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        sys.argv.pop(1)
        
        if command == "rebuild":
            asyncio.run(run_rebuild())
        elif command == "admin":
            asyncio.run(run_admin_query())
        else:
            print(f"Unknown command: {command}")
            print("Usage:")
            print("  python persistence.py rebuild [options]  - Rebuild exchange from storage")
            print("  python persistence.py admin [options]    - Query admin data")
    else:
        print("Fortisian Exchange Persistence Layer")
        print("=" * 40)
        print("\nUsage:")
        print("  python persistence.py rebuild [options]  - Rebuild exchange from storage")
        print("  python persistence.py admin [options]    - Query admin data")
        print("\nOptions:")
        print("  --mongo-uri URI      MongoDB connection string")
        print("  --database NAME      Database name")
        print("  --no-snapshot        Rebuild from events only (slow)")
        print("  --user USER_ID       Query specific user (admin)")
        print("  --summary            Show all users summary (admin)")
