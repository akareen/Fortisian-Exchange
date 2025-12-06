"""
Core domain models for the matching engine.

Design principles:
- Immutable where possible (frozen dataclasses)
- Strongly typed identifiers prevent mixing up strings
- Decimal for prices to avoid floating point issues
- Sequence numbers for deterministic ordering and replay
- Clear separation between configuration and state
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, UTC
from decimal import Decimal, ROUND_DOWN
from enum import Enum, auto
from typing import Self, NewType
from uuid import uuid4
import re


# ─────────────────────────────────────────────────────────────────────────────
# Strongly Typed Identifiers
# ─────────────────────────────────────────────────────────────────────────────

def _validate_id(value: str, name: str) -> None:
    """Validate identifier format."""
    if not value or not value.strip():
        raise ValueError(f"{name} cannot be empty")
    if len(value) > 64:
        raise ValueError(f"{name} cannot exceed 64 characters")


@dataclass(frozen=True, slots=True)
class OrderId:
    """Strongly typed order identifier."""
    value: str
    
    def __post_init__(self):
        _validate_id(self.value, "OrderId")
    
    @classmethod
    def generate(cls) -> Self:
        return cls(uuid4().hex)  # Shorter than str(uuid4())
    
    def __str__(self) -> str:
        return self.value
    
    def __hash__(self) -> int:
        return hash(self.value)


@dataclass(frozen=True, slots=True)
class TradeId:
    """Strongly typed trade identifier."""
    value: str
    
    def __post_init__(self):
        _validate_id(self.value, "TradeId")
    
    @classmethod
    def generate(cls) -> Self:
        return cls(uuid4().hex)
    
    def __str__(self) -> str:
        return self.value
    
    def __hash__(self) -> int:
        return hash(self.value)


@dataclass(frozen=True, slots=True)
class UserId:
    """Strongly typed user identifier."""
    value: str
    
    def __post_init__(self):
        _validate_id(self.value, "UserId")
        # Additional validation for user IDs
        if not re.match(r'^[a-zA-Z0-9_-]+$', self.value):
            raise ValueError("UserId can only contain alphanumeric, underscore, and hyphen")
    
    def __str__(self) -> str:
        return self.value
    
    def __hash__(self) -> int:
        return hash(self.value)


@dataclass(frozen=True, slots=True)
class MarketId:
    """Strongly typed market identifier."""
    value: str
    
    def __post_init__(self):
        _validate_id(self.value, "MarketId")
        if not re.match(r'^[A-Za-z0-9_-]+$', self.value):
            raise ValueError("MarketId must be alphanumeric with underscores/hyphens")
    
    def __str__(self) -> str:
        return self.value
    
    def __hash__(self) -> int:
        return hash(self.value)


# Type alias for sequence numbers
SequenceNumber = NewType('SequenceNumber', int)


# ─────────────────────────────────────────────────────────────────────────────
# Enumerations
# ─────────────────────────────────────────────────────────────────────────────

class Side(Enum):
    """Order side."""
    BUY = "buy"
    SELL = "sell"
    
    @property
    def opposite(self) -> Side:
        return Side.SELL if self == Side.BUY else Side.BUY
    
    @property
    def sign(self) -> int:
        """Returns +1 for BUY, -1 for SELL. Useful for position calculations."""
        return 1 if self == Side.BUY else -1


class OrderStatus(Enum):
    """Order lifecycle status."""
    PENDING = "pending"      # Created but not yet processed
    OPEN = "open"            # Resting in book
    PARTIAL = "partial"      # Partially filled, still in book
    FILLED = "filled"        # Completely filled
    CANCELLED = "cancelled"  # Cancelled by user
    REJECTED = "rejected"    # Rejected by system
    EXPIRED = "expired"      # Time-in-force expired


class MarketStatus(Enum):
    """Market operational status."""
    PENDING = "pending"      # Created but not started
    PRE_OPEN = "pre_open"    # Accepting orders but not matching
    ACTIVE = "active"        # Normal trading
    HALTED = "halted"        # Temporarily halted
    CLOSED = "closed"        # Trading ended
    
    @property
    def accepts_orders(self) -> bool:
        return self in (MarketStatus.PRE_OPEN, MarketStatus.ACTIVE)
    
    @property
    def matches_orders(self) -> bool:
        return self == MarketStatus.ACTIVE


class TimeInForce(Enum):
    """Order time-in-force instruction."""
    GTC = "gtc"              # Good til cancelled
    IOC = "ioc"              # Immediate or cancel (fill what you can, cancel rest)
    FOK = "fok"              # Fill or kill (all or nothing)


class RejectReason(Enum):
    """Standardized rejection reasons."""
    INVALID_PRICE = "invalid_price"
    INVALID_QUANTITY = "invalid_quantity"
    INVALID_SIDE = "invalid_side"
    MARKET_CLOSED = "market_closed"
    MARKET_HALTED = "market_halted"
    SELF_TRADE = "self_trade_prevented"
    PRICE_OUT_OF_BANDS = "price_out_of_bands"
    RATE_LIMITED = "rate_limited"
    INSUFFICIENT_PERMISSIONS = "insufficient_permissions"
    ORDER_NOT_FOUND = "order_not_found"
    NOT_ORDER_OWNER = "not_order_owner"
    TICK_SIZE_VIOLATION = "tick_size_violation"
    MAX_ORDER_SIZE = "max_order_size_exceeded"
    MAX_POSITION = "max_position_exceeded"
    LOT_SIZE_VIOLATION = "lot_size_violation"
    FOK_CANNOT_FILL = "fok_cannot_fill"


# ─────────────────────────────────────────────────────────────────────────────
# Order
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class Order:
    """
    A limit order in the book.
    
    Mutable fields: filled_qty, status, updated_at
    All other fields are effectively immutable after creation.
    
    Sequence number provides total ordering of all orders across the exchange.
    """
    id: OrderId
    market_id: MarketId
    user_id: UserId
    side: Side
    price: Decimal
    qty: int
    time_in_force: TimeInForce = TimeInForce.GTC
    filled_qty: int = 0
    status: OrderStatus = OrderStatus.OPEN
    sequence: SequenceNumber = SequenceNumber(0)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    
    # Optional metadata
    client_order_id: str | None = None  # User-provided ID for correlation
    
    def __post_init__(self):
        if self.qty <= 0:
            raise ValueError("Order quantity must be positive")
        if self.price <= 0:
            raise ValueError("Order price must be positive")
        if self.filled_qty < 0:
            raise ValueError("Filled quantity cannot be negative")
        if self.filled_qty > self.qty:
            raise ValueError("Filled quantity cannot exceed order quantity")
    
    @property
    def remaining_qty(self) -> int:
        return self.qty - self.filled_qty
    
    @property
    def is_complete(self) -> bool:
        return self.status in (
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        )
    
    @property
    def is_open(self) -> bool:
        return self.status in (OrderStatus.OPEN, OrderStatus.PARTIAL)
    
    @property
    def fill_ratio(self) -> float:
        """Percentage of order filled (0.0 to 1.0)."""
        return self.filled_qty / self.qty if self.qty > 0 else 0.0
    
    @property
    def notional_value(self) -> Decimal:
        """Total notional value of order."""
        return self.price * self.qty
    
    @property
    def filled_value(self) -> Decimal:
        """Notional value of filled portion (approximate, uses order price)."""
        return self.price * self.filled_qty
    
    def fill(self, qty: int) -> int:
        """
        Record a fill against this order.
        
        Returns the actual fill quantity (may be less if order nearly complete).
        """
        if qty <= 0:
            raise ValueError("Fill quantity must be positive")
        
        actual_fill = min(qty, self.remaining_qty)
        if actual_fill == 0:
            return 0
        
        self.filled_qty += actual_fill
        self.status = OrderStatus.FILLED if self.remaining_qty == 0 else OrderStatus.PARTIAL
        self.updated_at = datetime.now(UTC)
        
        return actual_fill
    
    def cancel(self) -> bool:
        """
        Cancel this order.
        
        Returns True if cancelled, False if already complete.
        """
        if self.is_complete:
            return False
        
        self.status = OrderStatus.CANCELLED
        self.updated_at = datetime.now(UTC)
        return True
    
    def reject(self, reason: RejectReason) -> None:
        """Mark order as rejected."""
        self.status = OrderStatus.REJECTED
        self.updated_at = datetime.now(UTC)
    
    def expire(self) -> bool:
        """Mark order as expired. Returns True if expired, False if already complete."""
        if self.is_complete:
            return False
        self.status = OrderStatus.EXPIRED
        self.updated_at = datetime.now(UTC)
        return True
    
    def to_dict(self) -> dict:
        return {
            "id": str(self.id),
            "market_id": str(self.market_id),
            "user_id": str(self.user_id),
            "side": self.side.value,
            "price": str(self.price),
            "qty": self.qty,
            "filled_qty": self.filled_qty,
            "remaining_qty": self.remaining_qty,
            "status": self.status.value,
            "time_in_force": self.time_in_force.value,
            "sequence": self.sequence,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "client_order_id": self.client_order_id,
        }
    
    def __repr__(self) -> str:
        return (
            f"Order({self.side.value} {self.remaining_qty}/{self.qty} "
            f"@ {self.price}, {self.status.value})"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Trade
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class Trade:
    """
    An executed trade between two orders.
    
    Immutable record of a match. Trade price is always the resting order's
    price (price improvement for the aggressor).
    """
    id: TradeId
    market_id: MarketId
    price: Decimal
    qty: int
    
    # Buyer info
    buyer_id: UserId
    buy_order_id: OrderId
    
    # Seller info
    seller_id: UserId
    sell_order_id: OrderId
    
    # Match metadata
    aggressor_side: Side
    sequence: SequenceNumber
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    
    @classmethod
    def from_match(
        cls,
        market_id: MarketId,
        price: Decimal,
        qty: int,
        buy_order: Order,
        sell_order: Order,
        aggressor_side: Side,
        sequence: SequenceNumber,
    ) -> Self:
        return cls(
            id=TradeId.generate(),
            market_id=market_id,
            price=price,
            qty=qty,
            buyer_id=buy_order.user_id,
            seller_id=sell_order.user_id,
            buy_order_id=buy_order.id,
            sell_order_id=sell_order.id,
            aggressor_side=aggressor_side,
            sequence=sequence,
        )
    
    @property
    def notional_value(self) -> Decimal:
        """Trade notional value."""
        return self.price * self.qty
    
    @property
    def maker_side(self) -> Side:
        """Side of the resting (maker) order."""
        return self.aggressor_side.opposite
    
    @property
    def maker_id(self) -> UserId:
        """User ID of the maker."""
        return self.seller_id if self.aggressor_side == Side.BUY else self.buyer_id
    
    @property
    def taker_id(self) -> UserId:
        """User ID of the taker (aggressor)."""
        return self.buyer_id if self.aggressor_side == Side.BUY else self.seller_id
    
    def to_dict(self) -> dict:
        return {
            "id": str(self.id),
            "market_id": str(self.market_id),
            "price": str(self.price),
            "qty": self.qty,
            "notional": str(self.notional_value),
            "buyer_id": str(self.buyer_id),
            "seller_id": str(self.seller_id),
            "buy_order_id": str(self.buy_order_id),
            "sell_order_id": str(self.sell_order_id),
            "aggressor_side": self.aggressor_side.value,
            "sequence": self.sequence,
            "created_at": self.created_at.isoformat(),
        }
    
    def to_public_dict(self) -> dict:
        """Public trade info (hides user identities until game end)."""
        return {
            "id": str(self.id),
            "market_id": str(self.market_id),
            "price": str(self.price),
            "qty": self.qty,
            "aggressor_side": self.aggressor_side.value,
            "sequence": self.sequence,
            "created_at": self.created_at.isoformat(),
        }
    
    def __repr__(self) -> str:
        return f"Trade({self.qty} @ {self.price}, {self.aggressor_side.value} aggressor)"


# ─────────────────────────────────────────────────────────────────────────────
# Market Configuration
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class PriceBands:
    """Price band configuration for circuit breakers."""
    reference_price: Decimal
    lower_band: Decimal   # Absolute lower limit
    upper_band: Decimal   # Absolute upper limit
    
    def __post_init__(self):
        if self.lower_band >= self.upper_band:
            raise ValueError("Lower band must be less than upper band")
        if self.reference_price < self.lower_band or self.reference_price > self.upper_band:
            raise ValueError("Reference price must be within bands")
    
    def is_within_bands(self, price: Decimal) -> bool:
        return self.lower_band <= price <= self.upper_band
    
    @classmethod
    def from_percentage(
        cls,
        reference_price: Decimal,
        band_percentage: Decimal,
    ) -> Self:
        """Create bands as percentage around reference price."""
        band = reference_price * band_percentage / 100
        return cls(
            reference_price=reference_price,
            lower_band=reference_price - band,
            upper_band=reference_price + band,
        )


@dataclass
class MarketConfig:
    """
    Immutable market configuration.
    
    Separated from runtime state for clarity.
    """
    id: MarketId
    title: str
    description: str = ""
    
    # Price configuration
    tick_size: Decimal = Decimal("0.01")
    price_bands: PriceBands | None = None  # Optional circuit breaker
    
    # Quantity limits
    min_qty: int = 1
    max_qty: int = 10000
    lot_size: int = 1  # Orders must be multiples of lot size
    
    # Position limits (per user)
    max_position: int | None = None  # Max absolute position
    max_order_value: Decimal | None = None  # Max notional per order
    
    # Trading rules
    allow_self_trade: bool = False
    
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    
    def validate_price(self, price: Decimal) -> RejectReason | None:
        """Validate price against market rules. Returns None if valid."""
        if price <= 0:
            return RejectReason.INVALID_PRICE
        
        # Check tick size
        if price % self.tick_size != 0:
            return RejectReason.TICK_SIZE_VIOLATION
        
        # Check price bands
        if self.price_bands and not self.price_bands.is_within_bands(price):
            return RejectReason.PRICE_OUT_OF_BANDS
        
        return None
    
    def validate_qty(self, qty: int) -> RejectReason | None:
        """Validate quantity against market rules. Returns None if valid."""
        if qty <= 0:
            return RejectReason.INVALID_QUANTITY
        
        if qty < self.min_qty:
            return RejectReason.INVALID_QUANTITY
        
        if qty > self.max_qty:
            return RejectReason.MAX_ORDER_SIZE
        
        if qty % self.lot_size != 0:
            return RejectReason.LOT_SIZE_VIOLATION
        
        return None
    
    def validate_order_value(self, price: Decimal, qty: int) -> RejectReason | None:
        """Validate order notional value."""
        if self.max_order_value is not None:
            if price * qty > self.max_order_value:
                return RejectReason.MAX_ORDER_SIZE
        return None
    
    def to_dict(self) -> dict:
        return {
            "id": str(self.id),
            "title": self.title,
            "description": self.description,
            "tick_size": str(self.tick_size),
            "min_qty": self.min_qty,
            "max_qty": self.max_qty,
            "lot_size": self.lot_size,
            "max_position": self.max_position,
            "max_order_value": str(self.max_order_value) if self.max_order_value else None,
            "allow_self_trade": self.allow_self_trade,
            "price_bands": {
                "reference": str(self.price_bands.reference_price),
                "lower": str(self.price_bands.lower_band),
                "upper": str(self.price_bands.upper_band),
            } if self.price_bands else None,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class Market:
    """
    Market runtime state.
    
    Combines configuration with mutable status.
    """
    config: MarketConfig
    status: MarketStatus = MarketStatus.PENDING
    
    # Runtime stats
    last_trade_price: Decimal | None = None
    high_price: Decimal | None = None
    low_price: Decimal | None = None
    open_price: Decimal | None = None
    total_volume: int = 0
    trade_count: int = 0
    
    @property
    def id(self) -> MarketId:
        return self.config.id
    
    @property
    def title(self) -> str:
        return self.config.title
    
    def update_from_trade(self, trade: Trade) -> None:
        """Update market stats from a trade."""
        price = trade.price
        
        if self.open_price is None:
            self.open_price = price
        
        self.last_trade_price = price
        
        if self.high_price is None or price > self.high_price:
            self.high_price = price
        
        if self.low_price is None or price < self.low_price:
            self.low_price = price
        
        self.total_volume += trade.qty
        self.trade_count += 1
    
    def to_dict(self) -> dict:
        return {
            **self.config.to_dict(),
            "status": self.status.value,
            "last_trade_price": str(self.last_trade_price) if self.last_trade_price else None,
            "open_price": str(self.open_price) if self.open_price else None,
            "high_price": str(self.high_price) if self.high_price else None,
            "low_price": str(self.low_price) if self.low_price else None,
            "total_volume": self.total_volume,
            "trade_count": self.trade_count,
        }
