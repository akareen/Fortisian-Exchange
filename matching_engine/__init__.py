"""
Matching Engine for Limit Order Book Exchange.

A clean, efficient implementation of a price-time priority matching engine
suitable for trading games and simulations.

Features:
- Price-time priority matching
- Self-trade prevention
- IOC/FOK order types
- Position tracking with limits
- Circuit breakers (price bands)
- Sequence numbers for ordering
- Event-driven architecture

Example usage:

    from matching_engine import Exchange, Side, TimeInForce
    
    # Create exchange with event handler
    def on_event(event):
        print(f"Event: {event.type}")
    
    exchange = Exchange(on_event=on_event)
    
    # Create a market
    exchange.create_market(
        "AAPL", 
        "Apple Inc.", 
        description="Tech stock",
        tick_size="0.01",
        max_position=1000,
    )
    exchange.start_market("AAPL")
    
    # Submit orders
    exchange.submit_order("AAPL", "user1", "buy", "100.00", 10)
    exchange.submit_order("AAPL", "user2", "sell", "100.00", 5)
    
    # Check book state
    snapshot = exchange.get_book_snapshot("AAPL")
    print(snapshot.to_dict())
    
    # Check positions
    position = exchange.get_user_position("AAPL", "user1")
    print(position.to_dict())
"""

from .models import (
    # Enums
    Side,
    OrderStatus,
    MarketStatus,
    TimeInForce,
    RejectReason,
    # IDs
    OrderId,
    TradeId,
    UserId,
    MarketId,
    SequenceNumber,
    # Domain objects
    Order,
    Trade,
    Market,
    MarketConfig,
    PriceBands,
)

from .events import (
    # Type alias
    EngineEvent,
    # Order events
    OrderAccepted,
    OrderRejected,
    OrderCancelled,
    OrderExpired,
    OrderFilled,
    # Trade events
    TradeExecuted,
    # Book events
    PriceLevel,
    BookSnapshot,
    BookDelta,
    BookDeltaEntry,
    BookDeltaAction,
    # Market events
    MarketStatusChanged,
    # Batching
    EventBatch,
    # Utilities
    filter_events_for_user,
    is_public_event,
    is_private_event,
)

from .order_book import (
    OrderBook,
    OrderQueue,
    BookState,
)

from .engine import (
    MatchingEngine,
    MatchResult,
    Position,
    PositionTracker,
)

from .exchange import (
    Exchange,
    ExchangeConfig,
)


__all__ = [
    # Enums
    "Side",
    "OrderStatus",
    "MarketStatus",
    "TimeInForce",
    "RejectReason",
    # IDs
    "OrderId",
    "TradeId",
    "UserId",
    "MarketId",
    "SequenceNumber",
    # Domain objects
    "Order",
    "Trade",
    "Market",
    "MarketConfig",
    "PriceBands",
    # Events
    "EngineEvent",
    "OrderAccepted",
    "OrderRejected",
    "OrderCancelled",
    "OrderExpired",
    "OrderFilled",
    "TradeExecuted",
    "PriceLevel",
    "BookSnapshot",
    "BookDelta",
    "BookDeltaEntry",
    "BookDeltaAction",
    "MarketStatusChanged",
    "EventBatch",
    "filter_events_for_user",
    "is_public_event",
    "is_private_event",
    # Order Book
    "OrderBook",
    "OrderQueue",
    "BookState",
    # Engine
    "MatchingEngine",
    "MatchResult",
    "Position",
    "PositionTracker",
    # Exchange
    "Exchange",
    "ExchangeConfig",
]

__version__ = "0.2.0"
