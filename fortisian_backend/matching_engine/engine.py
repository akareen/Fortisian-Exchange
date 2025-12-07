"""
Matching engine for a single market.

Implements price-time priority matching with:
- Self-trade prevention
- IOC/FOK order types
- Position tracking
- Circuit breakers
- Comprehensive event emission

Design principles:
- Single-threaded, synchronous matching for correctness
- All state changes emit events
- Sequence numbers for total ordering
- Clear separation of concerns
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, UTC
from decimal import Decimal
from typing import Callable
from collections import defaultdict

from .models import (
    Order, OrderId, UserId, MarketId, Side, Trade, Market, MarketConfig,
    MarketStatus, OrderStatus, TimeInForce, RejectReason, SequenceNumber
)
from .order_book import OrderBook
from .events import (
    EngineEvent, OrderAccepted, OrderRejected, OrderCancelled, OrderExpired,
    OrderFilled, TradeExecuted, BookSnapshot, BookDelta, MarketStatusChanged,
    EventBatch
)


# ─────────────────────────────────────────────────────────────────────────────
# Position Tracking
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Position:
    """User's position in a market."""
    user_id: UserId
    market_id: MarketId
    net_qty: int = 0           # Positive = long, negative = short
    total_bought: int = 0
    total_sold: int = 0
    buy_value: Decimal = Decimal(0)   # Total spent on buys
    sell_value: Decimal = Decimal(0)  # Total received from sells
    trade_count: int = 0
    
    @property
    def avg_buy_price(self) -> Decimal | None:
        return self.buy_value / self.total_bought if self.total_bought > 0 else None
    
    @property
    def avg_sell_price(self) -> Decimal | None:
        return self.sell_value / self.total_sold if self.total_sold > 0 else None
    
    @property
    def realized_pnl(self) -> Decimal:
        """Simplified realized P&L (sell value - buy value for closed portion)."""
        closed_qty = min(self.total_bought, self.total_sold)
        if closed_qty == 0:
            return Decimal(0)
        
        avg_buy = self.buy_value / self.total_bought if self.total_bought > 0 else Decimal(0)
        avg_sell = self.sell_value / self.total_sold if self.total_sold > 0 else Decimal(0)
        
        return (avg_sell - avg_buy) * closed_qty
    
    def apply_fill(self, side: Side, qty: int, price: Decimal) -> None:
        """Apply a fill to update position."""
        if side == Side.BUY:
            self.net_qty += qty
            self.total_bought += qty
            self.buy_value += price * qty
        else:
            self.net_qty -= qty
            self.total_sold += qty
            self.sell_value += price * qty
        
        self.trade_count += 1
    
    def to_dict(self) -> dict:
        return {
            "user_id": str(self.user_id),
            "market_id": str(self.market_id),
            "net_qty": self.net_qty,
            "total_bought": self.total_bought,
            "total_sold": self.total_sold,
            "avg_buy_price": str(self.avg_buy_price) if self.avg_buy_price else None,
            "avg_sell_price": str(self.avg_sell_price) if self.avg_sell_price else None,
            "realized_pnl": str(self.realized_pnl),
            "trade_count": self.trade_count,
        }


class PositionTracker:
    """Tracks positions for all users in a market."""
    
    def __init__(self, market_id: MarketId):
        self.market_id = market_id
        self._positions: dict[UserId, Position] = {}
    
    def get(self, user_id: UserId) -> Position:
        if user_id not in self._positions:
            self._positions[user_id] = Position(user_id=user_id, market_id=self.market_id)
        return self._positions[user_id]
    
    def apply_trade(self, trade: Trade) -> None:
        """Update positions from a trade."""
        buyer_pos = self.get(trade.buyer_id)
        seller_pos = self.get(trade.seller_id)
        
        buyer_pos.apply_fill(Side.BUY, trade.qty, trade.price)
        seller_pos.apply_fill(Side.SELL, trade.qty, trade.price)
    
    def get_all(self) -> list[Position]:
        return list(self._positions.values())
    
    def clear(self) -> None:
        self._positions.clear()


# ─────────────────────────────────────────────────────────────────────────────
# Match Result
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class MatchResult:
    """Result of attempting to match an order."""
    trades: list[Trade] = field(default_factory=list)
    filled_qty: int = 0
    remaining_qty: int = 0
    
    @property
    def fully_filled(self) -> bool:
        return self.remaining_qty == 0
    
    @property
    def partially_filled(self) -> bool:
        return self.filled_qty > 0 and self.remaining_qty > 0


# ─────────────────────────────────────────────────────────────────────────────
# Matching Engine
# ─────────────────────────────────────────────────────────────────────────────

class MatchingEngine:
    """
    Matching engine for a single market.
    
    Features:
    - Price-time priority matching
    - Self-trade prevention
    - IOC/FOK order types
    - Position tracking with limits
    - Sequence numbers for ordering
    - Event batching
    """
    
    def __init__(
        self,
        market: Market,
        on_event: Callable[[EngineEvent], None] | None = None,
    ):
        self.market = market
        self.book = OrderBook()
        self.positions = PositionTracker(market.id)
        
        self._on_event = on_event or (lambda e: None)
        self._sequence: int = 0
        self._trade_count: int = 0
        
        # Event batching
        self._event_buffer: list[EngineEvent] = []
        self._batching: bool = False
    
    def _next_sequence(self) -> SequenceNumber:
        """Get next sequence number."""
        self._sequence += 1
        return SequenceNumber(self._sequence)
    
    def _emit(self, event: EngineEvent) -> None:
        """Emit or buffer an event."""
        if self._batching:
            self._event_buffer.append(event)
        else:
            self._on_event(event)
    
    def _start_batch(self) -> None:
        """Start batching events."""
        self._batching = True
        self._event_buffer.clear()
    
    def _end_batch(self) -> EventBatch | None:
        """End batching and emit batch if non-empty."""
        self._batching = False
        
        if not self._event_buffer:
            return None
        
        events = tuple(self._event_buffer)
        self._event_buffer.clear()
        
        # Find sequence range
        sequences = []
        for e in events:
            if hasattr(e, 'sequence'):
                sequences.append(e.sequence)
        
        batch = EventBatch(
            events=events,
            sequence_start=SequenceNumber(min(sequences)) if sequences else SequenceNumber(0),
            sequence_end=SequenceNumber(max(sequences)) if sequences else SequenceNumber(0),
        )
        
        # Emit individual events
        for event in events:
            self._on_event(event)
        
        return batch
    
    def _emit_book_snapshot(self) -> None:
        """Emit full book snapshot."""
        state = self.book.get_state()
        self._emit(BookSnapshot(
            market_id=self.market.id,
            bids=state.bids,
            asks=state.asks,
            best_bid=state.best_bid,
            best_ask=state.best_ask,
            spread=state.spread,
            mid_price=state.mid_price,
            sequence=self._next_sequence(),
        ))
    
    def _emit_book_delta(self) -> None:
        """Emit book delta if there are changes."""
        deltas = self.book.pop_deltas()
        if deltas:
            self._emit(BookDelta(
                market_id=self.market.id,
                changes=deltas,
                best_bid=self.book.best_bid,
                best_ask=self.book.best_ask,
                sequence=self._next_sequence(),
            ))
    
    def _reject(
        self, 
        user_id: UserId, 
        reason: RejectReason, 
        reason_text: str,
        client_order_id: str | None = None,
    ) -> None:
        """Emit rejection event."""
        self._emit(OrderRejected(
            market_id=self.market.id,
            user_id=user_id,
            reason=reason,
            reason_text=reason_text,
            sequence=self._next_sequence(),
            client_order_id=client_order_id,
        ))
    
    # ─────────────────────────────────────────────────────────────────────────
    # Order Submission
    # ─────────────────────────────────────────────────────────────────────────
    
    def submit_order(
        self,
        user_id: UserId,
        side: Side,
        price: Decimal,
        qty: int,
        time_in_force: TimeInForce = TimeInForce.GTC,
        client_order_id: str | None = None,
    ) -> Order | None:
        """
        Submit a new limit order.
        
        Returns the order if accepted (may be partially/fully filled),
        None if rejected.
        """
        config = self.market.config
        
        # ─────────────────────────────────────────────────────────────────────
        # Validation
        # ─────────────────────────────────────────────────────────────────────
        
        # Check market status
        if not self.market.status.accepts_orders:
            reason = RejectReason.MARKET_CLOSED if self.market.status == MarketStatus.CLOSED else RejectReason.MARKET_HALTED
            self._reject(user_id, reason, f"Market is {self.market.status.value}", client_order_id)
            return None
        
        # Validate price
        price_error = config.validate_price(price)
        if price_error:
            self._reject(user_id, price_error, f"Invalid price: {price}", client_order_id)
            return None
        
        # Validate quantity
        qty_error = config.validate_qty(qty)
        if qty_error:
            self._reject(user_id, qty_error, f"Invalid quantity: {qty}", client_order_id)
            return None
        
        # Validate order value
        value_error = config.validate_order_value(price, qty)
        if value_error:
            self._reject(user_id, value_error, f"Order value exceeds limit", client_order_id)
            return None
        
        # Check position limits
        if config.max_position is not None:
            current_pos = self.positions.get(user_id).net_qty
            potential_pos = current_pos + (qty if side == Side.BUY else -qty)
            if abs(potential_pos) > config.max_position:
                self._reject(user_id, RejectReason.MAX_POSITION, 
                           f"Would exceed position limit of {config.max_position}", client_order_id)
                return None
        
        # Check self-trade (if prevention enabled)
        if not config.allow_self_trade and self.book.crosses(side, price):
            if self.book.would_self_trade(user_id, side, price):
                self._reject(user_id, RejectReason.SELF_TRADE,
                           "Order would result in self-trade", client_order_id)
                return None
        
        # FOK validation - check if full quantity is available
        if time_in_force == TimeInForce.FOK:
            if not config.allow_self_trade:
                available = self.book.get_matchable_qty_excluding_user(side, price, user_id)
            else:
                available = self.book.get_matchable_qty(side, price)
            
            if available < qty:
                self._reject(user_id, RejectReason.FOK_CANNOT_FILL,
                           f"FOK order cannot be fully filled (available: {available})", client_order_id)
                return None
        
        # ─────────────────────────────────────────────────────────────────────
        # Create Order
        # ─────────────────────────────────────────────────────────────────────
        
        order = Order(
            id=OrderId.generate(),
            market_id=self.market.id,
            user_id=user_id,
            side=side,
            price=price,
            qty=qty,
            time_in_force=time_in_force,
            sequence=self._next_sequence(),
            client_order_id=client_order_id,
        )
        
        # ─────────────────────────────────────────────────────────────────────
        # Matching
        # ─────────────────────────────────────────────────────────────────────
        
        self._start_batch()
        
        result = MatchResult(remaining_qty=qty)
        
        if self.market.status.matches_orders:
            result = self._match(order)
        
        # ─────────────────────────────────────────────────────────────────────
        # Post-Match Processing
        # ─────────────────────────────────────────────────────────────────────
        
        # Handle remaining quantity based on TIF
        if result.remaining_qty > 0:
            if time_in_force == TimeInForce.GTC:
                # Add to book
                self.book.add(order)
                self._emit(OrderAccepted(
                    order_id=order.id,
                    market_id=self.market.id,
                    user_id=user_id,
                    side=side,
                    price=price,
                    qty=result.remaining_qty,
                    time_in_force=time_in_force,
                    sequence=self._next_sequence(),
                    client_order_id=client_order_id,
                ))
            
            elif time_in_force == TimeInForce.IOC:
                # Cancel remaining
                order.expire()
                self._emit(OrderExpired(
                    order_id=order.id,
                    market_id=self.market.id,
                    user_id=user_id,
                    side=side,
                    price=price,
                    expired_qty=result.remaining_qty,
                    filled_qty=result.filled_qty,
                    reason="IOC order - unfilled portion cancelled",
                    sequence=self._next_sequence(),
                ))
            
            # FOK should never reach here due to pre-validation
        
        # Emit book update
        self._emit_book_delta()
        
        self._end_batch()
        
        return order
    
    def _match(self, incoming: Order) -> MatchResult:
        """
        Match incoming order against the book.
        
        Uses price-time priority: matches at best price first,
        within a price level matches oldest orders first.
        """
        result = MatchResult(remaining_qty=incoming.qty)
        config = self.market.config
        
        while incoming.remaining_qty > 0:
            # Get opposite side's best queue
            if incoming.side == Side.BUY:
                queue = self.book.best_ask_queue()
                if not queue or incoming.price < queue.price:
                    break
            else:
                queue = self.book.best_bid_queue()
                if not queue or incoming.price > queue.price:
                    break
            
            resting = queue.peek()
            if not resting:
                break
            
            # Self-trade prevention: skip orders from same user
            if not config.allow_self_trade and resting.user_id == incoming.user_id:
                # Remove the resting order (cancel it)
                self.book.remove(resting.id)
                resting.cancel()
                self._emit(OrderCancelled(
                    order_id=resting.id,
                    market_id=self.market.id,
                    user_id=resting.user_id,
                    side=resting.side,
                    price=resting.price,
                    cancelled_qty=resting.remaining_qty,
                    filled_qty=resting.filled_qty,
                    sequence=self._next_sequence(),
                ))
                continue
            
            # Execute trade
            trade = self._execute_trade(incoming, resting)
            result.trades.append(trade)
            result.filled_qty += trade.qty
        
        result.remaining_qty = incoming.remaining_qty
        return result
    
    def _execute_trade(self, aggressor: Order, resting: Order) -> Trade:
        """Execute a trade between aggressor and resting order."""
        
        # Determine buy/sell orders
        if aggressor.side == Side.BUY:
            buy_order, sell_order = aggressor, resting
        else:
            buy_order, sell_order = resting, aggressor
        
        # Trade at resting order's price (price improvement for aggressor)
        trade_price = resting.price
        trade_qty = min(aggressor.remaining_qty, resting.remaining_qty)
        
        # Create trade
        trade = Trade.from_match(
            market_id=self.market.id,
            price=trade_price,
            qty=trade_qty,
            buy_order=buy_order,
            sell_order=sell_order,
            aggressor_side=aggressor.side,
            sequence=self._next_sequence(),
        )
        
        # Update order states
        aggressor.fill(trade_qty)
        resting.fill(trade_qty)
        
        # Update book
        self.book.update_after_fill(resting, trade_qty)
        
        # Update positions
        self.positions.apply_trade(trade)
        
        # Update market stats
        self.market.update_from_trade(trade)
        self._trade_count += 1
        
        # Emit events
        self._emit(TradeExecuted(trade=trade))
        
        # Emit fill for buyer
        self._emit(OrderFilled(
            order_id=buy_order.id,
            market_id=self.market.id,
            user_id=buy_order.user_id,
            side=Side.BUY,
            fill_price=trade_price,
            fill_qty=trade_qty,
            remaining_qty=buy_order.remaining_qty,
            total_filled_qty=buy_order.filled_qty,
            is_complete=buy_order.remaining_qty == 0,
            trade_id=trade.id,
            is_maker=buy_order is resting,
            sequence=self._next_sequence(),
        ))
        
        # Emit fill for seller
        self._emit(OrderFilled(
            order_id=sell_order.id,
            market_id=self.market.id,
            user_id=sell_order.user_id,
            side=Side.SELL,
            fill_price=trade_price,
            fill_qty=trade_qty,
            remaining_qty=sell_order.remaining_qty,
            total_filled_qty=sell_order.filled_qty,
            is_complete=sell_order.remaining_qty == 0,
            trade_id=trade.id,
            is_maker=sell_order is resting,
            sequence=self._next_sequence(),
        ))
        
        return trade
    
    # ─────────────────────────────────────────────────────────────────────────
    # Order Cancellation
    # ─────────────────────────────────────────────────────────────────────────
    
    def cancel_order(self, order_id: OrderId, user_id: UserId) -> bool:
        """
        Cancel an order.
        
        Only the order owner can cancel. Returns True if cancelled.
        """
        order = self.book.get_order(order_id)
        
        if not order:
            return False
        
        if order.user_id != user_id:
            return False
        
        cancelled_qty = order.remaining_qty
        filled_qty = order.filled_qty
        
        order.cancel()
        self.book.remove(order_id)
        
        self._emit(OrderCancelled(
            order_id=order_id,
            market_id=self.market.id,
            user_id=user_id,
            side=order.side,
            price=order.price,
            cancelled_qty=cancelled_qty,
            filled_qty=filled_qty,
            sequence=self._next_sequence(),
        ))
        
        self._emit_book_delta()
        
        return True
    
    def cancel_all_user_orders(self, user_id: UserId) -> int:
        """Cancel all orders for a user. Returns count cancelled."""
        orders = self.book.get_user_orders(user_id)
        count = 0
        
        self._start_batch()
        
        for order in orders:
            cancelled_qty = order.remaining_qty
            filled_qty = order.filled_qty
            
            order.cancel()
            self.book.remove(order.id)
            
            self._emit(OrderCancelled(
                order_id=order.id,
                market_id=self.market.id,
                user_id=user_id,
                side=order.side,
                price=order.price,
                cancelled_qty=cancelled_qty,
                filled_qty=filled_qty,
                sequence=self._next_sequence(),
            ))
            count += 1
        
        if count > 0:
            self._emit_book_delta()
        
        self._end_batch()
        
        return count
    
    # ─────────────────────────────────────────────────────────────────────────
    # Market Control
    # ─────────────────────────────────────────────────────────────────────────
    
    def set_status(self, new_status: MarketStatus) -> None:
        """Change market status."""
        old_status = self.market.status
        
        if old_status == new_status:
            return
        
        self.market.status = new_status
        
        self._emit(MarketStatusChanged(
            market_id=self.market.id,
            old_status=old_status,
            new_status=new_status,
            sequence=self._next_sequence(),
        ))
        
        # If closing, cancel all orders
        if new_status == MarketStatus.CLOSED:
            self._start_batch()
            
            for order in self.book.clear():
                order.cancel()
                self._emit(OrderCancelled(
                    order_id=order.id,
                    market_id=self.market.id,
                    user_id=order.user_id,
                    side=order.side,
                    price=order.price,
                    cancelled_qty=order.remaining_qty,
                    filled_qty=order.filled_qty,
                    sequence=self._next_sequence(),
                ))
            
            self._emit_book_snapshot()
            self._end_batch()
    
    # ─────────────────────────────────────────────────────────────────────────
    # Queries
    # ─────────────────────────────────────────────────────────────────────────
    
    def get_order(self, order_id: OrderId) -> Order | None:
        """Get order by ID."""
        return self.book.get_order(order_id)
    
    def get_user_orders(self, user_id: UserId) -> list[Order]:
        """Get all open orders for a user."""
        return self.book.get_user_orders(user_id)
    
    def get_user_position(self, user_id: UserId) -> Position:
        """Get user's position."""
        return self.positions.get(user_id)
    
    def get_all_positions(self) -> list[Position]:
        """Get all positions."""
        return self.positions.get_all()
    
    def get_book_snapshot(self) -> BookSnapshot:
        """Get current book state."""
        state = self.book.get_state()
        return BookSnapshot(
            market_id=self.market.id,
            bids=state.bids,
            asks=state.asks,
            best_bid=state.best_bid,
            best_ask=state.best_ask,
            spread=state.spread,
            mid_price=state.mid_price,
            sequence=SequenceNumber(self._sequence),
        )
    
    @property
    def trade_count(self) -> int:
        return self._trade_count
    
    @property
    def sequence(self) -> int:
        return self._sequence
    
    def __repr__(self) -> str:
        return (
            f"MatchingEngine(market={self.market.title!r}, "
            f"orders={len(self.book)}, trades={self._trade_count}, "
            f"seq={self._sequence})"
        )
