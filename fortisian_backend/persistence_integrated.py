"""
Integrated Persistence Layer for Trading Server.

This module provides direct integration with the trading server to capture
ALL L3 (Level 3) order book data and events in real-time.

Key Features:
1. Direct event capture at the source (before WebSocket distribution)
2. Complete L3 order book snapshots (every order detail)
3. Automatic persistence of all order lifecycle events
4. Background task for non-blocking writes
5. Periodic full order book snapshots
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Callable
from collections import deque
import logging

from matching_engine import (
    Exchange, EngineEvent, Order, Trade, Position, Market,
    OrderAccepted, OrderRejected, OrderCancelled, OrderFilled,
    OrderExpired, TradeExecuted, BookSnapshot, BookDelta,
    MarketStatusChanged, OrderId, UserId, MarketId,
)

from persistence import (
    MongoRepository, MongoConfig, serialize_event, serialize_order,
    serialize_trade, serialize_position, serialize_market,
)

logger = logging.getLogger(__name__)


class IntegratedPersistence:
    """
    Integrated persistence that captures ALL events and L3 data directly from the exchange.
    
    This runs as part of the server process and captures:
    - Every event at the source (before WebSocket distribution)
    - Complete order book state (L3 data) with every order detail
    - All order lifecycle changes
    - Market state changes
    - Position updates
    
    Uses background tasks to avoid blocking the main server loop.
    """
    
    def __init__(
        self,
        exchange: Exchange,
        mongo_config: MongoConfig,
        enabled: bool = True,
    ):
        self.exchange = exchange
        self.mongo_config = mongo_config
        self.enabled = enabled
        
        self.repo: Optional[MongoRepository] = None
        self._original_event_handler: Optional[Callable] = None
        
        # Background task for async writes
        self._write_queue: asyncio.Queue = asyncio.Queue()
        self._write_task: Optional[asyncio.Task] = None
        self._running = False
        
        # L3 order book snapshots - store complete state periodically
        self._l3_snapshot_interval = 60.0  # Every 60 seconds
        self._last_l3_snapshot: dict[MarketId, float] = {}
        
        # Event buffer for batching
        self._event_buffer: deque = deque(maxlen=1000)
        self._buffer_flush_interval = 5.0  # Flush every 5 seconds
        self._last_flush = datetime.now(timezone.utc)
    
    async def start(self) -> None:
        """Start the persistence system."""
        if not self.enabled:
            logger.info("Persistence disabled")
            return
        
        try:
            # Connect to MongoDB
            self.repo = MongoRepository(self.mongo_config)
            await self.repo.connect()
            
            # Store initial market states
            for market in self.exchange.list_markets():
                await self._store_market_state(market)
            
            # Hook into exchange event stream
            self._original_event_handler = self.exchange._on_event
            self.exchange._on_event = self._wrap_event_handler(self._original_event_handler)
            
            # Start background write task
            self._running = True
            self._write_task = asyncio.create_task(self._write_worker())
            
            # Start L3 snapshot task
            asyncio.create_task(self._l3_snapshot_worker())
            
            logger.info("Integrated persistence started")
            
        except Exception as e:
            logger.error(f"Failed to start persistence: {e}")
            self.enabled = False
    
    async def stop(self) -> None:
        """Stop the persistence system."""
        self._running = False
        
        # Flush remaining events
        await self._flush_event_buffer()
        
        # Stop write task
        if self._write_task:
            self._write_task.cancel()
            try:
                await self._write_task
            except asyncio.CancelledError:
                pass
        
        # Restore original event handler
        if self._original_event_handler:
            self.exchange._on_event = self._original_event_handler
        
        # Disconnect from MongoDB
        if self.repo:
            await self.repo.disconnect()
        
        logger.info("Integrated persistence stopped")
    
    def _wrap_event_handler(self, original_handler: Optional[Callable]) -> Callable:
        """Wrap the exchange event handler to capture all events."""
        def wrapped_handler(event: EngineEvent):
            # Call original handler first (for WebSocket distribution, etc.)
            if original_handler:
                original_handler(event)
            
            # Capture event for persistence (non-blocking, fire-and-forget)
            # Use create_task to avoid blocking
            asyncio.create_task(self._capture_event(event))
        
        return wrapped_handler
    
    async def _capture_event(self, event: EngineEvent) -> None:
        """Capture an event for persistence."""
        if not self.enabled or not self.repo:
            return
        
        # Add to buffer
        self._event_buffer.append(event)
        
        # Check if we need to flush
        now = datetime.now(timezone.utc)
        if (now - self._last_flush).total_seconds() >= self._buffer_flush_interval:
            await self._flush_event_buffer()
        
        # Also handle specific event types immediately for critical data
        if isinstance(event, (OrderAccepted, OrderFilled, OrderCancelled, OrderExpired)):
            if hasattr(event, 'order') and event.order:
                await self._queue_write('order', serialize_order(event.order))
        
        elif isinstance(event, TradeExecuted):
            if hasattr(event, 'trade') and event.trade:
                await self._queue_write('trade', serialize_trade(event.trade))
    
    async def _flush_event_buffer(self) -> None:
        """Flush event buffer to MongoDB."""
        if not self._event_buffer:
            return
        
        events_to_store = list(self._event_buffer)
        self._event_buffer.clear()
        self._last_flush = datetime.now(timezone.utc)
        
        # Store events in background
        for event in events_to_store:
            await self._queue_write('event', serialize_event(event))
    
    async def _queue_write(self, doc_type: str, doc: dict) -> None:
        """Queue a document for async write."""
        if not self.enabled:
            return
        
        try:
            self._write_queue.put_nowait((doc_type, doc))
        except asyncio.QueueFull:
            logger.warning("Write queue full, dropping document")
    
    async def _write_worker(self) -> None:
        """Background worker that writes to MongoDB."""
        while self._running:
            try:
                # Get item from queue with timeout
                try:
                    doc_type, doc = await asyncio.wait_for(
                        self._write_queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                
                # Write to appropriate collection
                if doc_type == 'event':
                    await self.repo.db[self.repo.config.events_collection].insert_one(doc)
                elif doc_type == 'order':
                    await self.repo.db[self.repo.config.orders_collection].replace_one(
                        {"_id": doc["_id"]}, doc, upsert=True
                    )
                elif doc_type == 'trade':
                    try:
                        await self.repo.db[self.repo.config.trades_collection].insert_one(doc)
                    except Exception:
                        # Trade might already exist, update it
                        await self.repo.db[self.repo.config.trades_collection].replace_one(
                            {"_id": doc["_id"]}, doc, upsert=True
                        )
                elif doc_type == 'position':
                    await self.repo.db[self.repo.config.positions_collection].replace_one(
                        {"_id": doc["_id"]}, doc, upsert=True
                    )
                elif doc_type == 'market':
                    await self.repo.db[self.repo.config.markets_collection].replace_one(
                        {"_id": doc["_id"]}, doc, upsert=True
                    )
                elif doc_type == 'l3_snapshot':
                    # Store L3 snapshot in a separate collection
                    await self.repo.db["l3_snapshots"].insert_one(doc)
                
            except Exception as e:
                logger.error(f"Error in write worker: {e}")
    
    async def _l3_snapshot_worker(self) -> None:
        """Periodically capture complete L3 order book snapshots."""
        while self._running:
            try:
                await asyncio.sleep(self._l3_snapshot_interval)
                
                if not self.enabled or not self.repo:
                    continue
                
                # Capture L3 snapshot for each market
                for market_id, market in self.exchange._markets.items():
                    engine = self.exchange._engines.get(market_id)
                    if not engine:
                        continue
                    
                    # Get complete order book state
                    book = engine.book
                    book_state = book.get_state()
                    
                    # Build L3 snapshot with every order detail
                    l3_snapshot = {
                        "market_id": str(market_id),
                        "timestamp": datetime.now(timezone.utc),
                        "sequence": engine._sequence,
                        "bids": [],
                        "asks": [],
                        "best_bid": str(book_state.best_bid) if book_state.best_bid else None,
                        "best_ask": str(book_state.best_ask) if book_state.best_ask else None,
                        "spread": str(book_state.spread) if book_state.spread else None,
                    }
                    
                    # Capture all bid orders (L3 detail)
                    for price, queue in book._bids.items():
                        for order in queue.orders:
                            l3_snapshot["bids"].append({
                                "price": str(price),
                                "order_id": str(order.id),
                                "user_id": str(order.user_id),
                                "qty": order.remaining_qty,
                                "original_qty": order.qty,
                                "filled_qty": order.filled_qty,
                                "time_in_force": order.time_in_force.value,
                                "created_at": order.created_at.isoformat() if order.created_at else None,
                                "client_order_id": order.client_order_id,
                            })
                    
                    # Capture all ask orders (L3 detail)
                    for price, queue in book._asks.items():
                        for order in queue.orders:
                            l3_snapshot["asks"].append({
                                "price": str(price),
                                "order_id": str(order.id),
                                "user_id": str(order.user_id),
                                "qty": order.remaining_qty,
                                "original_qty": order.qty,
                                "filled_qty": order.filled_qty,
                                "time_in_force": order.time_in_force.value,
                                "created_at": order.created_at.isoformat() if order.created_at else None,
                                "client_order_id": order.client_order_id,
                            })
                    
                    # Store L3 snapshot
                    await self._queue_write('l3_snapshot', l3_snapshot)
                    
                    logger.debug(f"Captured L3 snapshot for {market_id}: {len(l3_snapshot['bids'])} bids, {len(l3_snapshot['asks'])} asks")
                
            except Exception as e:
                logger.error(f"Error in L3 snapshot worker: {e}")
    
    async def _store_market_state(self, market: Market) -> None:
        """Store complete market state."""
        if not self.enabled or not self.repo:
            return
        
        # Store market
        market_doc = serialize_market(market)
        await self._queue_write('market', market_doc)
        
        # Store all positions for this market
        engine = self.exchange._engines.get(market.id)
        if engine:
            for position in engine.positions.get_all():
                pos_doc = serialize_position(
                    str(position.user_id), str(position.market_id), position
                )
                await self._queue_write('position', pos_doc)
    
    async def store_position_update(self, user_id: UserId, market_id: MarketId, position: Position) -> None:
        """Store a position update."""
        if not self.enabled:
            return
        
        pos_doc = serialize_position(str(user_id), str(market_id), position)
        await self._queue_write('position', pos_doc)
    
    async def capture_order_book_state(self, market_id: MarketId) -> None:
        """Manually trigger L3 snapshot for a market."""
        market = self.exchange.get_market(market_id)
        if market:
            await self._store_market_state(market)

