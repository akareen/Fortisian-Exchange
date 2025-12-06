"""
Event types emitted by the matching engine.

Events are immutable records of state changes. Uses discriminated union 
pattern for type-safe event handling.

Design principles:
- All events are immutable (frozen dataclasses)
- Each event has a 'type' discriminator field
- Events include sequence numbers for ordering
- Supports both full snapshots and delta updates
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, UTC
from decimal import Decimal
from typing import Literal

from .models import (
    MarketId, OrderId, TradeId, UserId, Side, OrderStatus, 
    MarketStatus, Trade, RejectReason, SequenceNumber, TimeInForce
)


# ─────────────────────────────────────────────────────────────────────────────
# Type alias for all events
# ─────────────────────────────────────────────────────────────────────────────

type EngineEvent = (
    OrderAccepted
    | OrderRejected
    | OrderCancelled
    | OrderExpired
    | OrderFilled
    | TradeExecuted
    | BookSnapshot
    | BookDelta
    | MarketStatusChanged
)


# ─────────────────────────────────────────────────────────────────────────────
# Order Events
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class OrderAccepted:
    """Emitted when an order is accepted into the book."""
    type: Literal["order_accepted"] = field(default="order_accepted", init=False)
    order_id: OrderId
    market_id: MarketId
    user_id: UserId
    side: Side
    price: Decimal
    qty: int
    time_in_force: TimeInForce
    sequence: SequenceNumber
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    client_order_id: str | None = None
    
    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "order_id": str(self.order_id),
            "market_id": str(self.market_id),
            "user_id": str(self.user_id),
            "side": self.side.value,
            "price": str(self.price),
            "qty": self.qty,
            "time_in_force": self.time_in_force.value,
            "sequence": self.sequence,
            "timestamp": self.timestamp.isoformat(),
            "client_order_id": self.client_order_id,
        }


@dataclass(frozen=True, slots=True)
class OrderRejected:
    """Emitted when an order is rejected."""
    type: Literal["order_rejected"] = field(default="order_rejected", init=False)
    market_id: MarketId
    user_id: UserId
    reason: RejectReason
    reason_text: str  # Human readable explanation
    sequence: SequenceNumber
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    client_order_id: str | None = None
    
    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "market_id": str(self.market_id),
            "user_id": str(self.user_id),
            "reason": self.reason.value,
            "reason_text": self.reason_text,
            "sequence": self.sequence,
            "timestamp": self.timestamp.isoformat(),
            "client_order_id": self.client_order_id,
        }


@dataclass(frozen=True, slots=True)
class OrderCancelled:
    """Emitted when an order is cancelled."""
    type: Literal["order_cancelled"] = field(default="order_cancelled", init=False)
    order_id: OrderId
    market_id: MarketId
    user_id: UserId
    side: Side
    price: Decimal
    cancelled_qty: int  # Quantity that was cancelled (remaining at cancel time)
    filled_qty: int     # Quantity that was filled before cancel
    sequence: SequenceNumber
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    
    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "order_id": str(self.order_id),
            "market_id": str(self.market_id),
            "user_id": str(self.user_id),
            "side": self.side.value,
            "price": str(self.price),
            "cancelled_qty": self.cancelled_qty,
            "filled_qty": self.filled_qty,
            "sequence": self.sequence,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class OrderExpired:
    """Emitted when an order expires (e.g., IOC unfilled portion)."""
    type: Literal["order_expired"] = field(default="order_expired", init=False)
    order_id: OrderId
    market_id: MarketId
    user_id: UserId
    side: Side
    price: Decimal
    expired_qty: int
    filled_qty: int
    reason: str  # e.g., "IOC unfilled", "FOK cannot fill"
    sequence: SequenceNumber
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    
    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "order_id": str(self.order_id),
            "market_id": str(self.market_id),
            "user_id": str(self.user_id),
            "side": self.side.value,
            "price": str(self.price),
            "expired_qty": self.expired_qty,
            "filled_qty": self.filled_qty,
            "reason": self.reason,
            "sequence": self.sequence,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class OrderFilled:
    """Emitted when an order receives a fill (partial or complete)."""
    type: Literal["order_filled"] = field(default="order_filled", init=False)
    order_id: OrderId
    market_id: MarketId
    user_id: UserId
    side: Side
    fill_price: Decimal
    fill_qty: int
    remaining_qty: int
    total_filled_qty: int
    is_complete: bool
    trade_id: TradeId
    is_maker: bool  # True if this was the resting order
    sequence: SequenceNumber
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    
    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "order_id": str(self.order_id),
            "market_id": str(self.market_id),
            "user_id": str(self.user_id),
            "side": self.side.value,
            "fill_price": str(self.fill_price),
            "fill_qty": self.fill_qty,
            "remaining_qty": self.remaining_qty,
            "total_filled_qty": self.total_filled_qty,
            "is_complete": self.is_complete,
            "trade_id": str(self.trade_id),
            "is_maker": self.is_maker,
            "sequence": self.sequence,
            "timestamp": self.timestamp.isoformat(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Trade Events
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class TradeExecuted:
    """Emitted when a trade occurs. Contains full trade details."""
    type: Literal["trade_executed"] = field(default="trade_executed", init=False)
    trade: Trade
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    
    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "trade": self.trade.to_dict(),
            "timestamp": self.timestamp.isoformat(),
        }
    
    def to_public_dict(self) -> dict:
        """Public version that hides user identities."""
        return {
            "type": self.type,
            "trade": self.trade.to_public_dict(),
            "timestamp": self.timestamp.isoformat(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Book Events
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class PriceLevel:
    """A single price level in the book."""
    price: Decimal
    qty: int
    order_count: int
    
    def to_dict(self) -> dict:
        return {
            "price": str(self.price),
            "qty": self.qty,
            "order_count": self.order_count,
        }


@dataclass(frozen=True, slots=True)
class BookSnapshot:
    """
    Full order book snapshot.
    
    Sent on initial subscription and periodically for sync.
    """
    type: Literal["book_snapshot"] = field(default="book_snapshot", init=False)
    market_id: MarketId
    bids: tuple[PriceLevel, ...]
    asks: tuple[PriceLevel, ...]
    best_bid: Decimal | None
    best_ask: Decimal | None
    spread: Decimal | None
    mid_price: Decimal | None
    sequence: SequenceNumber
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    
    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "market_id": str(self.market_id),
            "bids": [level.to_dict() for level in self.bids],
            "asks": [level.to_dict() for level in self.asks],
            "best_bid": str(self.best_bid) if self.best_bid else None,
            "best_ask": str(self.best_ask) if self.best_ask else None,
            "spread": str(self.spread) if self.spread else None,
            "mid_price": str(self.mid_price) if self.mid_price else None,
            "sequence": self.sequence,
            "timestamp": self.timestamp.isoformat(),
        }


class BookDeltaAction:
    """Actions for book delta updates."""
    ADD = "add"
    REMOVE = "remove"
    UPDATE = "update"


@dataclass(frozen=True, slots=True)
class BookDeltaEntry:
    """Single change to the order book."""
    action: str  # add, remove, update
    side: Side
    price: Decimal
    qty: int  # New quantity (0 for remove)
    order_count: int
    
    def to_dict(self) -> dict:
        return {
            "action": self.action,
            "side": self.side.value,
            "price": str(self.price),
            "qty": self.qty,
            "order_count": self.order_count,
        }


@dataclass(frozen=True, slots=True)
class BookDelta:
    """
    Incremental book update.
    
    More efficient than full snapshots for frequent updates.
    """
    type: Literal["book_delta"] = field(default="book_delta", init=False)
    market_id: MarketId
    changes: tuple[BookDeltaEntry, ...]
    best_bid: Decimal | None
    best_ask: Decimal | None
    sequence: SequenceNumber
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    
    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "market_id": str(self.market_id),
            "changes": [c.to_dict() for c in self.changes],
            "best_bid": str(self.best_bid) if self.best_bid else None,
            "best_ask": str(self.best_ask) if self.best_ask else None,
            "sequence": self.sequence,
            "timestamp": self.timestamp.isoformat(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Market Events
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class MarketStatusChanged:
    """Emitted when market status changes (e.g., game starts/ends)."""
    type: Literal["market_status_changed"] = field(default="market_status_changed", init=False)
    market_id: MarketId
    old_status: MarketStatus
    new_status: MarketStatus
    sequence: SequenceNumber
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    
    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "market_id": str(self.market_id),
            "old_status": self.old_status.value,
            "new_status": self.new_status.value,
            "sequence": self.sequence,
            "timestamp": self.timestamp.isoformat(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Event Batch
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class EventBatch:
    """
    Batch of related events from a single operation.
    
    Useful for reducing WebSocket message count - send one batch
    instead of many individual events.
    """
    events: tuple[EngineEvent, ...]
    sequence_start: SequenceNumber
    sequence_end: SequenceNumber
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    
    def to_dict(self) -> dict:
        return {
            "type": "event_batch",
            "events": [e.to_dict() for e in self.events],
            "sequence_start": self.sequence_start,
            "sequence_end": self.sequence_end,
            "timestamp": self.timestamp.isoformat(),
        }
    
    def __len__(self) -> int:
        return len(self.events)
    
    def __iter__(self):
        return iter(self.events)


# ─────────────────────────────────────────────────────────────────────────────
# Event Filtering
# ─────────────────────────────────────────────────────────────────────────────

def filter_events_for_user(events: list[EngineEvent], user_id: UserId) -> list[EngineEvent]:
    """Filter events to only those relevant to a specific user."""
    result = []
    for event in events:
        match event:
            case OrderAccepted(user_id=uid) | OrderRejected(user_id=uid) | \
                 OrderCancelled(user_id=uid) | OrderExpired(user_id=uid) | \
                 OrderFilled(user_id=uid) if uid == user_id:
                result.append(event)
            case TradeExecuted(trade=trade) if trade.buyer_id == user_id or trade.seller_id == user_id:
                result.append(event)
            case BookSnapshot() | BookDelta() | MarketStatusChanged():
                # Public events go to everyone
                result.append(event)
    return result


def is_public_event(event: EngineEvent) -> bool:
    """Check if an event should be broadcast publicly."""
    match event:
        case BookSnapshot() | BookDelta() | MarketStatusChanged() | TradeExecuted():
            return True
        case _:
            return False


def is_private_event(event: EngineEvent) -> bool:
    """Check if an event is user-specific."""
    return not is_public_event(event)
