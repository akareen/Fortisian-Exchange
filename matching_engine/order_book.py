"""
Order Book implementation with price-time priority.

Design principles:
- O(log n) price level operations using sorted containers
- O(1) order lookup by ID
- O(1) user order lookup (indexed)
- Efficient cancel operations
- Immutable snapshots for thread-safe reads
- Delta tracking for efficient updates
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Iterator, Callable
from collections import defaultdict

from sortedcontainers import SortedDict

from .models import Order, OrderId, UserId, Side, SequenceNumber
from .events import PriceLevel, BookDeltaEntry, BookDeltaAction


@dataclass
class OrderQueue:
    """
    FIFO queue of orders at a single price level.
    
    Maintains O(1) append and O(1) popleft for price-time priority.
    Tracks total quantity for quick book depth queries.
    """
    price: Decimal
    orders: deque[Order] = field(default_factory=deque)
    _total_qty: int = 0
    
    @property
    def total_qty(self) -> int:
        return self._total_qty
    
    @property
    def order_count(self) -> int:
        return len(self.orders)
    
    @property
    def is_empty(self) -> bool:
        return len(self.orders) == 0
    
    def append(self, order: Order) -> None:
        """Add order to back of queue (time priority)."""
        self.orders.append(order)
        self._total_qty += order.remaining_qty
    
    def peek(self) -> Order | None:
        """View front order without removing."""
        return self.orders[0] if self.orders else None
    
    def pop(self) -> Order | None:
        """Remove and return front order."""
        if not self.orders:
            return None
        order = self.orders.popleft()
        self._total_qty -= order.remaining_qty
        return order
    
    def remove(self, order_id: OrderId) -> Order | None:
        """
        Remove specific order by ID. 
        O(n) but cancels are less frequent than matches.
        """
        for i, order in enumerate(self.orders):
            if order.id == order_id:
                removed_qty = order.remaining_qty
                del self.orders[i]
                self._total_qty -= removed_qty
                return order
        return None
    
    def reduce_qty(self, amount: int) -> None:
        """Reduce total qty after a partial fill on front order."""
        self._total_qty = max(0, self._total_qty - amount)
    
    def recalculate_qty(self) -> None:
        """Recalculate total quantity (use after modifications)."""
        self._total_qty = sum(o.remaining_qty for o in self.orders)
    
    def to_price_level(self) -> PriceLevel:
        return PriceLevel(
            price=self.price,
            qty=self._total_qty,
            order_count=self.order_count,
        )
    
    def __repr__(self) -> str:
        return f"OrderQueue(price={self.price}, qty={self._total_qty}, orders={self.order_count})"


@dataclass(frozen=True, slots=True)
class BookState:
    """
    Immutable snapshot of order book state.
    
    Safe to pass around without worrying about mutations.
    """
    bids: tuple[PriceLevel, ...]
    asks: tuple[PriceLevel, ...]
    best_bid: Decimal | None
    best_ask: Decimal | None
    
    @property
    def spread(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid
    
    @property
    def mid_price(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2
    
    @property
    def is_crossed(self) -> bool:
        """Check if book is crossed (best bid >= best ask)."""
        if self.best_bid is None or self.best_ask is None:
            return False
        return self.best_bid >= self.best_ask


class OrderBook:
    """
    Limit order book with price-time priority.
    
    Bids sorted descending (best bid = highest price first).
    Asks sorted ascending (best ask = lowest price first).
    
    Features:
    - O(log n) price level operations
    - O(1) order lookup by ID
    - O(1) user order enumeration
    - Delta tracking for efficient updates
    """
    
    def __init__(self):
        # Bids: price -> OrderQueue, sorted descending (negate keys)
        self._bids: SortedDict[Decimal, OrderQueue] = SortedDict()
        
        # Asks: price -> OrderQueue, sorted ascending (natural order)
        self._asks: SortedDict[Decimal, OrderQueue] = SortedDict()
        
        # Order ID -> Order for fast lookup
        self._orders: dict[OrderId, Order] = {}
        
        # User ID -> set of order IDs for fast user order queries
        self._user_orders: dict[UserId, set[OrderId]] = defaultdict(set)
        
        # Delta tracking
        self._pending_deltas: list[BookDeltaEntry] = []
        self._delta_tracking_enabled: bool = True
    
    def _get_book(self, side: Side) -> SortedDict[Decimal, OrderQueue]:
        return self._bids if side == Side.BUY else self._asks
    
    def _price_key(self, side: Side, price: Decimal) -> Decimal:
        """Convert price to sort key. Bids negated for descending order."""
        return -price if side == Side.BUY else price
    
    def _track_delta(self, action: str, side: Side, price: Decimal, qty: int, order_count: int) -> None:
        """Record a book change for delta updates."""
        if self._delta_tracking_enabled:
            self._pending_deltas.append(BookDeltaEntry(
                action=action,
                side=side,
                price=price,
                qty=qty,
                order_count=order_count,
            ))
    
    def pop_deltas(self) -> tuple[BookDeltaEntry, ...]:
        """Get and clear pending deltas."""
        deltas = tuple(self._pending_deltas)
        self._pending_deltas.clear()
        return deltas
    
    def add(self, order: Order) -> None:
        """Add order to the book at its price level."""
        book = self._get_book(order.side)
        key = self._price_key(order.side, order.price)
        
        is_new_level = key not in book
        if is_new_level:
            book[key] = OrderQueue(price=order.price)
        
        book[key].append(order)
        self._orders[order.id] = order
        self._user_orders[order.user_id].add(order.id)
        
        # Track delta
        queue = book[key]
        action = BookDeltaAction.ADD if is_new_level else BookDeltaAction.UPDATE
        self._track_delta(action, order.side, order.price, queue.total_qty, queue.order_count)
    
    def remove(self, order_id: OrderId) -> Order | None:
        """Remove order from book by ID."""
        order = self._orders.get(order_id)
        if not order:
            return None
        
        book = self._get_book(order.side)
        key = self._price_key(order.side, order.price)
        
        if key in book:
            book[key].remove(order_id)
            queue = book[key]
            
            # Track delta
            if queue.is_empty:
                del book[key]
                self._track_delta(BookDeltaAction.REMOVE, order.side, order.price, 0, 0)
            else:
                self._track_delta(BookDeltaAction.UPDATE, order.side, order.price, 
                                 queue.total_qty, queue.order_count)
        
        del self._orders[order_id]
        self._user_orders[order.user_id].discard(order_id)
        
        return order
    
    def update_after_fill(self, order: Order, fill_qty: int) -> None:
        """Update book state after a fill on a resting order."""
        book = self._get_book(order.side)
        key = self._price_key(order.side, order.price)
        
        if key not in book:
            return
        
        queue = book[key]
        queue.reduce_qty(fill_qty)
        
        # If order is complete, remove it
        if order.remaining_qty == 0:
            queue.orders.popleft()  # Should be the front order
            del self._orders[order.id]
            self._user_orders[order.user_id].discard(order.id)
            
            if queue.is_empty:
                del book[key]
                self._track_delta(BookDeltaAction.REMOVE, order.side, order.price, 0, 0)
            else:
                self._track_delta(BookDeltaAction.UPDATE, order.side, order.price,
                                 queue.total_qty, queue.order_count)
        else:
            self._track_delta(BookDeltaAction.UPDATE, order.side, order.price,
                             queue.total_qty, queue.order_count)
    
    def get_order(self, order_id: OrderId) -> Order | None:
        """Get order by ID. O(1)."""
        return self._orders.get(order_id)
    
    def get_user_orders(self, user_id: UserId) -> list[Order]:
        """Get all open orders for a user. O(k) where k = user's order count."""
        order_ids = self._user_orders.get(user_id, set())
        return [self._orders[oid] for oid in order_ids if oid in self._orders]
    
    def get_user_order_count(self, user_id: UserId) -> int:
        """Get count of open orders for a user. O(1)."""
        return len(self._user_orders.get(user_id, set()))
    
    def has_orders_from_user(self, user_id: UserId) -> bool:
        """Check if user has any orders in book. O(1)."""
        return bool(self._user_orders.get(user_id))
    
    @property
    def best_bid(self) -> Decimal | None:
        """Best (highest) bid price."""
        if not self._bids:
            return None
        return -self._bids.peekitem(0)[0]
    
    @property
    def best_ask(self) -> Decimal | None:
        """Best (lowest) ask price."""
        if not self._asks:
            return None
        return self._asks.peekitem(0)[0]
    
    @property
    def spread(self) -> Decimal | None:
        """Spread between best bid and ask."""
        bb, ba = self.best_bid, self.best_ask
        return ba - bb if bb is not None and ba is not None else None
    
    @property
    def mid_price(self) -> Decimal | None:
        """Mid price between best bid and ask."""
        bb, ba = self.best_bid, self.best_ask
        return (bb + ba) / 2 if bb is not None and ba is not None else None
    
    def best_bid_queue(self) -> OrderQueue | None:
        """Get the order queue at best bid."""
        if not self._bids:
            return None
        return self._bids.peekitem(0)[1]
    
    def best_ask_queue(self) -> OrderQueue | None:
        """Get the order queue at best ask."""
        if not self._asks:
            return None
        return self._asks.peekitem(0)[1]
    
    def iter_bids(self, depth: int | None = None) -> Iterator[OrderQueue]:
        """Iterate bid levels best-first (descending price)."""
        count = 0
        for queue in self._bids.values():
            if depth is not None and count >= depth:
                break
            yield queue
            count += 1
    
    def iter_asks(self, depth: int | None = None) -> Iterator[OrderQueue]:
        """Iterate ask levels best-first (ascending price)."""
        count = 0
        for queue in self._asks.values():
            if depth is not None and count >= depth:
                break
            yield queue
            count += 1
    
    def get_bids(self, depth: int | None = None) -> tuple[PriceLevel, ...]:
        """Get bid levels as PriceLevel tuples."""
        return tuple(q.to_price_level() for q in self.iter_bids(depth))
    
    def get_asks(self, depth: int | None = None) -> tuple[PriceLevel, ...]:
        """Get ask levels as PriceLevel tuples."""
        return tuple(q.to_price_level() for q in self.iter_asks(depth))
    
    def get_state(self, depth: int | None = None) -> BookState:
        """Get immutable snapshot of current book state."""
        return BookState(
            bids=self.get_bids(depth),
            asks=self.get_asks(depth),
            best_bid=self.best_bid,
            best_ask=self.best_ask,
        )
    
    def crosses(self, side: Side, price: Decimal) -> bool:
        """Check if an order at this price would cross the spread."""
        if side == Side.BUY:
            return self.best_ask is not None and price >= self.best_ask
        else:
            return self.best_bid is not None and price <= self.best_bid
    
    def would_self_trade(self, user_id: UserId, side: Side, price: Decimal) -> bool:
        """
        Check if order would result in self-trade.
        
        Returns True if there's a matchable order from the same user
        on the opposite side.
        """
        opposite_book = self._asks if side == Side.BUY else self._bids
        
        for key, queue in opposite_book.items():
            actual_price = key if side == Side.BUY else -key
            
            # Check if price would match
            if side == Side.BUY and price < actual_price:
                break  # No more matchable prices
            if side == Side.SELL and price > actual_price:
                break
            
            # Check if any order at this level is from the same user
            for order in queue.orders:
                if order.user_id == user_id:
                    return True
        
        return False
    
    def get_matchable_qty(self, side: Side, price: Decimal) -> int:
        """
        Get total quantity available to match at or better than price.
        
        Useful for FOK order validation.
        """
        opposite_book = self._asks if side == Side.BUY else self._bids
        total = 0
        
        for key, queue in opposite_book.items():
            actual_price = key if side == Side.BUY else -key
            
            # Check if price would match
            if side == Side.BUY and price < actual_price:
                break
            if side == Side.SELL and price > actual_price:
                break
            
            total += queue.total_qty
        
        return total
    
    def get_matchable_qty_excluding_user(
        self, 
        side: Side, 
        price: Decimal, 
        exclude_user: UserId
    ) -> int:
        """
        Get matchable quantity excluding orders from a specific user.
        
        Used for self-trade prevention with FOK orders.
        """
        opposite_book = self._asks if side == Side.BUY else self._bids
        total = 0
        
        for key, queue in opposite_book.items():
            actual_price = key if side == Side.BUY else -key
            
            if side == Side.BUY and price < actual_price:
                break
            if side == Side.SELL and price > actual_price:
                break
            
            for order in queue.orders:
                if order.user_id != exclude_user:
                    total += order.remaining_qty
        
        return total
    
    def clear(self) -> list[Order]:
        """Clear all orders, return list of removed orders."""
        orders = list(self._orders.values())
        self._bids.clear()
        self._asks.clear()
        self._orders.clear()
        self._user_orders.clear()
        self._pending_deltas.clear()
        return orders
    
    @property
    def bid_depth(self) -> int:
        """Number of bid price levels."""
        return len(self._bids)
    
    @property
    def ask_depth(self) -> int:
        """Number of ask price levels."""
        return len(self._asks)
    
    @property
    def total_bid_qty(self) -> int:
        """Total quantity on bid side."""
        return sum(q.total_qty for q in self._bids.values())
    
    @property
    def total_ask_qty(self) -> int:
        """Total quantity on ask side."""
        return sum(q.total_qty for q in self._asks.values())
    
    @property
    def order_count(self) -> int:
        """Total number of orders in book."""
        return len(self._orders)
    
    @property
    def user_count(self) -> int:
        """Number of unique users with orders."""
        return sum(1 for orders in self._user_orders.values() if orders)
    
    def __len__(self) -> int:
        return self.order_count
    
    def __repr__(self) -> str:
        return (
            f"OrderBook(bids={self.bid_depth}L/{self.total_bid_qty}Q, "
            f"asks={self.ask_depth}L/{self.total_ask_qty}Q, "
            f"spread={self.spread})"
        )
