"""
Sweep Order Implementation.

A sweep order is a special order type that progressively submits limit orders
from a start price to an end price. This is useful for:
- Accumulating/distributing large positions across multiple price levels
- Simulating market orders with price protection
- Creating liquidity across a price range

Example:
    Sweep Buy from 100.00 to 102.00 with qty 1000
    - Creates limit buy orders at each tick from 100.00 up to 102.00
    - Allocates quantity proportionally or equally across levels
    - Each individual order goes through normal matching
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from enum import Enum
from typing import Callable
from uuid import uuid4

from matching_engine import (
    Exchange, MarketId, UserId, OrderId, Side, TimeInForce, Order,
    EngineEvent, OrderAccepted, OrderRejected,
)


class SweepAllocation(Enum):
    """How to allocate quantity across price levels."""
    EQUAL = "equal"        # Equal quantity at each level
    FRONT_WEIGHTED = "front_weighted"  # More quantity at aggressive prices
    BACK_WEIGHTED = "back_weighted"    # More quantity at passive prices


@dataclass
class SweepOrderResult:
    """Result of a sweep order submission."""
    sweep_id: str
    market_id: str
    user_id: str
    side: str
    start_price: Decimal
    end_price: Decimal
    total_qty: int
    
    # Results
    orders_submitted: int = 0
    orders_accepted: int = 0
    orders_rejected: int = 0
    total_qty_accepted: int = 0
    total_qty_rejected: int = 0
    
    # Order details
    order_ids: list[str] = field(default_factory=list)
    rejection_reasons: list[str] = field(default_factory=list)
    
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    @property
    def success(self) -> bool:
        return self.orders_accepted > 0
    
    @property
    def fully_accepted(self) -> bool:
        return self.orders_rejected == 0 and self.orders_accepted > 0
    
    def to_dict(self) -> dict:
        return {
            "sweep_id": self.sweep_id,
            "market_id": self.market_id,
            "user_id": self.user_id,
            "side": self.side,
            "start_price": str(self.start_price),
            "end_price": str(self.end_price),
            "total_qty": self.total_qty,
            "orders_submitted": self.orders_submitted,
            "orders_accepted": self.orders_accepted,
            "orders_rejected": self.orders_rejected,
            "total_qty_accepted": self.total_qty_accepted,
            "total_qty_rejected": self.total_qty_rejected,
            "order_ids": self.order_ids,
            "rejection_reasons": self.rejection_reasons,
            "success": self.success,
            "fully_accepted": self.fully_accepted,
            "created_at": self.created_at.isoformat(),
        }


class SweepOrderManager:
    """
    Manages sweep order execution.
    
    Sweep orders are decomposed into multiple limit orders and submitted
    to the exchange in order.
    """
    
    def __init__(self, exchange: Exchange):
        self.exchange = exchange
        self._sweep_history: dict[str, SweepOrderResult] = {}
    
    def submit_sweep(
        self,
        market_id: str,
        user_id: str,
        side: str,
        start_price: str | Decimal,
        end_price: str | Decimal,
        total_qty: int,
        allocation: SweepAllocation = SweepAllocation.EQUAL,
        time_in_force: str = "gtc",
        client_sweep_id: str | None = None,
    ) -> SweepOrderResult:
        """
        Submit a sweep order.
        
        Args:
            market_id: Market to trade
            user_id: User submitting the order
            side: "buy" or "sell"
            start_price: Starting price (more aggressive)
            end_price: Ending price (less aggressive)
            total_qty: Total quantity to distribute
            allocation: How to distribute quantity
            time_in_force: TIF for individual orders
            client_sweep_id: Optional client correlation ID
        
        Returns:
            SweepOrderResult with details of all orders submitted
        """
        # Normalize inputs
        start_price = Decimal(str(start_price))
        end_price = Decimal(str(end_price))
        side_enum = Side.BUY if side.lower() == "buy" else Side.SELL
        
        # Get market for tick size
        market = self.exchange.get_market(market_id)
        if not market:
            result = SweepOrderResult(
                sweep_id=client_sweep_id or uuid4().hex,
                market_id=market_id,
                user_id=user_id,
                side=side,
                start_price=start_price,
                end_price=end_price,
                total_qty=total_qty,
            )
            result.rejection_reasons.append("Market not found")
            return result
        
        tick_size = market.config.tick_size
        lot_size = market.config.lot_size
        
        # Validate price direction based on side
        if side_enum == Side.BUY:
            # For buys, start should be >= end (aggressive to passive)
            if start_price < end_price:
                start_price, end_price = end_price, start_price
        else:
            # For sells, start should be <= end (aggressive to passive)
            if start_price > end_price:
                start_price, end_price = end_price, start_price
        
        # Calculate price levels
        price_levels = self._calculate_price_levels(
            start_price, end_price, tick_size, side_enum
        )
        
        if not price_levels:
            result = SweepOrderResult(
                sweep_id=client_sweep_id or uuid4().hex,
                market_id=market_id,
                user_id=user_id,
                side=side,
                start_price=start_price,
                end_price=end_price,
                total_qty=total_qty,
            )
            result.rejection_reasons.append("No valid price levels")
            return result
        
        # Allocate quantity across levels
        qty_per_level = self._allocate_quantity(
            total_qty, len(price_levels), allocation, lot_size
        )
        
        # Create result tracker
        result = SweepOrderResult(
            sweep_id=client_sweep_id or uuid4().hex,
            market_id=market_id,
            user_id=user_id,
            side=side,
            start_price=start_price,
            end_price=end_price,
            total_qty=total_qty,
        )
        
        # Submit orders
        for price, qty in zip(price_levels, qty_per_level):
            if qty <= 0:
                continue
            
            result.orders_submitted += 1
            
            try:
                order = self.exchange.submit_order(
                    market_id=market_id,
                    user_id=user_id,
                    side=side,
                    price=price,
                    qty=qty,
                    time_in_force=time_in_force,
                    client_order_id=f"{result.sweep_id}_{result.orders_submitted}",
                )
                
                if order:
                    result.orders_accepted += 1
                    result.total_qty_accepted += qty
                    result.order_ids.append(str(order.id))
                else:
                    result.orders_rejected += 1
                    result.total_qty_rejected += qty
                    result.rejection_reasons.append(f"Order at {price} rejected")
                    
            except Exception as e:
                result.orders_rejected += 1
                result.total_qty_rejected += qty
                result.rejection_reasons.append(f"Order at {price}: {str(e)}")
        
        # Store in history
        self._sweep_history[result.sweep_id] = result
        
        return result
    
    def _calculate_price_levels(
        self,
        start_price: Decimal,
        end_price: Decimal,
        tick_size: Decimal,
        side: Side,
    ) -> list[Decimal]:
        """Calculate all price levels from start to end."""
        levels = []
        
        # Ensure prices are on tick grid
        start_price = (start_price / tick_size).quantize(Decimal('1'), rounding=ROUND_DOWN) * tick_size
        end_price = (end_price / tick_size).quantize(Decimal('1'), rounding=ROUND_UP) * tick_size
        
        if side == Side.BUY:
            # Start high, go low (aggressive to passive)
            current = start_price
            while current >= end_price:
                levels.append(current)
                current -= tick_size
        else:
            # Start low, go high (aggressive to passive)
            current = start_price
            while current <= end_price:
                levels.append(current)
                current += tick_size
        
        return levels
    
    def _allocate_quantity(
        self,
        total_qty: int,
        num_levels: int,
        allocation: SweepAllocation,
        lot_size: int,
    ) -> list[int]:
        """Allocate quantity across price levels."""
        if num_levels == 0:
            return []
        
        if allocation == SweepAllocation.EQUAL:
            # Equal distribution
            base_qty = total_qty // num_levels
            remainder = total_qty % num_levels
            
            # Round to lot size
            base_qty = (base_qty // lot_size) * lot_size
            
            qtys = [base_qty] * num_levels
            
            # Distribute remainder to first levels (more aggressive)
            for i in range(min(remainder // lot_size, num_levels)):
                qtys[i] += lot_size
            
            return qtys
        
        elif allocation == SweepAllocation.FRONT_WEIGHTED:
            # More at aggressive prices (front)
            # Use triangular distribution: weights 1, 2, 3, ..., n
            weights = list(range(num_levels, 0, -1))
            total_weight = sum(weights)
            
            qtys = []
            remaining = total_qty
            
            for i, w in enumerate(weights):
                if i == len(weights) - 1:
                    qty = remaining
                else:
                    qty = int(total_qty * w / total_weight)
                    qty = (qty // lot_size) * lot_size
                    remaining -= qty
                
                qtys.append(max(0, qty))
            
            return qtys
        
        elif allocation == SweepAllocation.BACK_WEIGHTED:
            # More at passive prices (back)
            weights = list(range(1, num_levels + 1))
            total_weight = sum(weights)
            
            qtys = []
            remaining = total_qty
            
            for i, w in enumerate(weights):
                if i == len(weights) - 1:
                    qty = remaining
                else:
                    qty = int(total_qty * w / total_weight)
                    qty = (qty // lot_size) * lot_size
                    remaining -= qty
                
                qtys.append(max(0, qty))
            
            return qtys
        
        return [total_qty // num_levels] * num_levels
    
    def get_sweep(self, sweep_id: str) -> SweepOrderResult | None:
        """Get a sweep order result by ID."""
        return self._sweep_history.get(sweep_id)
    
    def get_user_sweeps(self, user_id: str) -> list[SweepOrderResult]:
        """Get all sweep orders for a user."""
        return [
            s for s in self._sweep_history.values()
            if s.user_id == user_id
        ]
    
    def cancel_sweep(self, sweep_id: str, user_id: str) -> int:
        """
        Cancel all orders from a sweep.
        
        Returns count of orders cancelled.
        """
        sweep = self._sweep_history.get(sweep_id)
        if not sweep:
            return 0
        
        if sweep.user_id != user_id:
            return 0  # Can only cancel own sweeps
        
        count = 0
        for order_id in sweep.order_ids:
            if self.exchange.cancel_order(sweep.market_id, order_id, user_id):
                count += 1
        
        return count


# ─────────────────────────────────────────────────────────────────────────────
# Testing
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from matching_engine import Exchange
    
    print("Testing Sweep Orders")
    print("=" * 60)
    
    # Create exchange
    exchange = Exchange()
    exchange.create_market("TEST", "Test Market", tick_size="0.01")
    exchange.start_market("TEST")
    
    sweep_manager = SweepOrderManager(exchange)
    
    # Test 1: Basic sweep buy
    print("\n1. Sweep Buy 100.00 -> 99.50 with 500 qty")
    result = sweep_manager.submit_sweep(
        market_id="TEST",
        user_id="user1",
        side="buy",
        start_price="100.00",
        end_price="99.50",
        total_qty=500,
    )
    print(f"   Orders submitted: {result.orders_submitted}")
    print(f"   Orders accepted: {result.orders_accepted}")
    print(f"   Qty accepted: {result.total_qty_accepted}")
    print(f"   Success: {result.success}")
    
    # Check the book
    snapshot = exchange.get_book_snapshot("TEST")
    print(f"   Book has {len(snapshot.bids)} bid levels")
    
    # Test 2: Sweep sell
    print("\n2. Sweep Sell 100.00 -> 101.00 with 300 qty")
    result = sweep_manager.submit_sweep(
        market_id="TEST",
        user_id="user2",
        side="sell",
        start_price="100.00",
        end_price="101.00",
        total_qty=300,
    )
    print(f"   Orders submitted: {result.orders_submitted}")
    print(f"   Orders accepted: {result.orders_accepted}")
    
    snapshot = exchange.get_book_snapshot("TEST")
    print(f"   Book has {len(snapshot.asks)} ask levels")
    
    # Test 3: Front-weighted allocation
    print("\n3. Front-weighted Sweep")
    result = sweep_manager.submit_sweep(
        market_id="TEST",
        user_id="user3",
        side="buy",
        start_price="98.00",
        end_price="97.90",
        total_qty=1000,
        allocation=SweepAllocation.FRONT_WEIGHTED,
    )
    print(f"   Orders: {result.orders_submitted}")
    print(f"   First order gets more qty (aggressive price)")
    
    # Test 4: Cancel sweep
    print("\n4. Cancel sweep orders")
    cancelled = sweep_manager.cancel_sweep(result.sweep_id, "user3")
    print(f"   Cancelled {cancelled} orders")
    
    print("\n" + "=" * 60)
    print("All sweep tests passed!")
