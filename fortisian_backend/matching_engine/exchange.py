"""
Exchange manager - coordinates multiple markets.

This is the top-level API for the matching engine. It manages multiple
markets, routes orders, and provides administrative functions.

Design principles:
- Clean, type-safe API
- Flexible input types for ease of use
- Comprehensive admin capabilities
- Thread-safe where possible
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, UTC
from decimal import Decimal
from typing import Callable

from .models import (
    Market, MarketConfig, MarketId, MarketStatus, Order, OrderId, UserId, 
    Side, TimeInForce, PriceBands, SequenceNumber
)
from .engine import MatchingEngine, Position
from .events import EngineEvent, BookSnapshot


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ExchangeConfig:
    """Exchange-wide configuration."""
    max_markets: int = 100
    default_tick_size: Decimal = Decimal("0.01")
    default_min_qty: int = 1
    default_max_qty: int = 10000
    default_lot_size: int = 1
    default_allow_self_trade: bool = False
    default_max_position: int | None = None


# ─────────────────────────────────────────────────────────────────────────────
# Exchange
# ─────────────────────────────────────────────────────────────────────────────

class Exchange:
    """
    Exchange managing multiple markets.
    
    Provides a unified API for order submission, market management,
    and administrative functions.
    """
    
    def __init__(
        self,
        config: ExchangeConfig | None = None,
        on_event: Callable[[EngineEvent], None] | None = None,
    ):
        self.config = config or ExchangeConfig()
        self._on_event = on_event or (lambda e: None)
        self._engines: dict[MarketId, MatchingEngine] = {}
        self._markets: dict[MarketId, Market] = {}
        self._created_at = datetime.now(UTC)
        self._global_sequence: int = 0
    
    def _emit(self, event: EngineEvent) -> None:
        """Propagate event to consumers."""
        self._on_event(event)
    
    def _normalize_market_id(self, market_id: str | MarketId) -> MarketId:
        """Normalize market_id to MarketId type."""
        return market_id if isinstance(market_id, MarketId) else MarketId(market_id)
    
    def _normalize_user_id(self, user_id: str | UserId) -> UserId:
        """Normalize user_id to UserId type."""
        return user_id if isinstance(user_id, UserId) else UserId(user_id)
    
    def _normalize_order_id(self, order_id: str | OrderId) -> OrderId:
        """Normalize order_id to OrderId type."""
        return order_id if isinstance(order_id, OrderId) else OrderId(order_id)
    
    def _normalize_side(self, side: str | Side) -> Side:
        """Normalize side to Side enum."""
        if isinstance(side, Side):
            return side
        return Side(side.lower())
    
    def _normalize_price(self, price: Decimal | float | str | int) -> Decimal:
        """Normalize price to Decimal."""
        if isinstance(price, Decimal):
            return price
        return Decimal(str(price))
    
    def _normalize_tif(self, tif: str | TimeInForce | None) -> TimeInForce:
        """Normalize time_in_force to TimeInForce enum."""
        if tif is None:
            return TimeInForce.GTC
        if isinstance(tif, TimeInForce):
            return tif
        return TimeInForce(tif.lower())
    
    # ─────────────────────────────────────────────────────────────────────────
    # Market Management
    # ─────────────────────────────────────────────────────────────────────────
    
    def create_market(
        self,
        market_id: str,
        title: str,
        description: str = "",
        tick_size: Decimal | float | str | None = None,
        min_qty: int | None = None,
        max_qty: int | None = None,
        lot_size: int | None = None,
        max_position: int | None = None,
        max_order_value: Decimal | float | str | None = None,
        allow_self_trade: bool | None = None,
        price_bands: PriceBands | None = None,
    ) -> Market:
        """
        Create a new market.
        
        Returns the created Market object.
        """
        if len(self._markets) >= self.config.max_markets:
            raise ValueError(f"Maximum markets ({self.config.max_markets}) reached")
        
        mid = MarketId(market_id)
        if mid in self._markets:
            raise ValueError(f"Market {market_id} already exists")
        
        # Build config with defaults
        market_config = MarketConfig(
            id=mid,
            title=title,
            description=description,
            tick_size=self._normalize_price(tick_size) if tick_size else self.config.default_tick_size,
            min_qty=min_qty if min_qty is not None else self.config.default_min_qty,
            max_qty=max_qty if max_qty is not None else self.config.default_max_qty,
            lot_size=lot_size if lot_size is not None else self.config.default_lot_size,
            max_position=max_position if max_position is not None else self.config.default_max_position,
            max_order_value=self._normalize_price(max_order_value) if max_order_value else None,
            allow_self_trade=allow_self_trade if allow_self_trade is not None else self.config.default_allow_self_trade,
            price_bands=price_bands,
        )
        
        market = Market(config=market_config)
        engine = MatchingEngine(market=market, on_event=self._emit)
        
        self._markets[mid] = market
        self._engines[mid] = engine
        
        return market
    
    def get_market(self, market_id: str | MarketId) -> Market | None:
        """Get market by ID."""
        mid = self._normalize_market_id(market_id)
        return self._markets.get(mid)
    
    def get_engine(self, market_id: str | MarketId) -> MatchingEngine | None:
        """Get matching engine for a market."""
        mid = self._normalize_market_id(market_id)
        return self._engines.get(mid)
    
    def list_markets(self) -> list[Market]:
        """List all markets."""
        return list(self._markets.values())
    
    def set_market_status(self, market_id: str | MarketId, status: MarketStatus | str) -> bool:
        """Change market status. Returns True if successful."""
        engine = self.get_engine(market_id)
        if not engine:
            return False
        
        if isinstance(status, str):
            status = MarketStatus(status.lower())
        
        engine.set_status(status)
        return True
    
    def start_market(self, market_id: str | MarketId) -> bool:
        """Start a market (set to ACTIVE)."""
        return self.set_market_status(market_id, MarketStatus.ACTIVE)
    
    def stop_market(self, market_id: str | MarketId) -> bool:
        """Stop a market (set to CLOSED)."""
        return self.set_market_status(market_id, MarketStatus.CLOSED)
    
    def halt_market(self, market_id: str | MarketId) -> bool:
        """Halt a market temporarily."""
        return self.set_market_status(market_id, MarketStatus.HALTED)
    
    def resume_market(self, market_id: str | MarketId) -> bool:
        """Resume a halted market."""
        return self.set_market_status(market_id, MarketStatus.ACTIVE)
    
    def pre_open_market(self, market_id: str | MarketId) -> bool:
        """Set market to pre-open (accepts orders but no matching)."""
        return self.set_market_status(market_id, MarketStatus.PRE_OPEN)
    
    # ─────────────────────────────────────────────────────────────────────────
    # Order Operations
    # ─────────────────────────────────────────────────────────────────────────
    
    def submit_order(
        self,
        market_id: str | MarketId,
        user_id: str | UserId,
        side: str | Side,
        price: Decimal | float | str | int,
        qty: int,
        time_in_force: str | TimeInForce | None = None,
        client_order_id: str | None = None,
    ) -> Order | None:
        """
        Submit a limit order to a market.
        
        Accepts flexible types for ease of use.
        Returns the order if accepted, None if rejected.
        """
        engine = self.get_engine(market_id)
        if not engine:
            raise ValueError(f"Market {market_id} not found")
        
        return engine.submit_order(
            user_id=self._normalize_user_id(user_id),
            side=self._normalize_side(side),
            price=self._normalize_price(price),
            qty=qty,
            time_in_force=self._normalize_tif(time_in_force),
            client_order_id=client_order_id,
        )
    
    def cancel_order(
        self,
        market_id: str | MarketId,
        order_id: str | OrderId,
        user_id: str | UserId,
    ) -> bool:
        """Cancel a specific order."""
        engine = self.get_engine(market_id)
        if not engine:
            return False
        
        return engine.cancel_order(
            order_id=self._normalize_order_id(order_id),
            user_id=self._normalize_user_id(user_id),
        )
    
    def cancel_all_orders(
        self, 
        market_id: str | MarketId, 
        user_id: str | UserId,
    ) -> int:
        """Cancel all orders for a user in a market. Returns count cancelled."""
        engine = self.get_engine(market_id)
        if not engine:
            return 0
        
        return engine.cancel_all_user_orders(self._normalize_user_id(user_id))
    
    def cancel_all_user_orders_all_markets(self, user_id: str | UserId) -> int:
        """Cancel all orders for a user across all markets."""
        uid = self._normalize_user_id(user_id)
        total = 0
        for engine in self._engines.values():
            total += engine.cancel_all_user_orders(uid)
        return total
    
    # ─────────────────────────────────────────────────────────────────────────
    # Order Queries
    # ─────────────────────────────────────────────────────────────────────────
    
    def get_order(
        self, 
        market_id: str | MarketId, 
        order_id: str | OrderId,
    ) -> Order | None:
        """Get a specific order."""
        engine = self.get_engine(market_id)
        if not engine:
            return None
        return engine.get_order(self._normalize_order_id(order_id))
    
    def get_user_orders(
        self, 
        market_id: str | MarketId, 
        user_id: str | UserId,
    ) -> list[Order]:
        """Get all open orders for a user in a market."""
        engine = self.get_engine(market_id)
        if not engine:
            return []
        return engine.get_user_orders(self._normalize_user_id(user_id))
    
    def get_all_user_orders(self, user_id: str | UserId) -> dict[str, list[Order]]:
        """Get all open orders for a user across all markets."""
        uid = self._normalize_user_id(user_id)
        return {
            str(mid): engine.get_user_orders(uid)
            for mid, engine in self._engines.items()
        }
    
    # ─────────────────────────────────────────────────────────────────────────
    # Position Queries
    # ─────────────────────────────────────────────────────────────────────────
    
    def get_user_position(
        self, 
        market_id: str | MarketId, 
        user_id: str | UserId,
    ) -> Position:
        """Get user's position in a market."""
        engine = self.get_engine(market_id)
        if not engine:
            raise ValueError(f"Market {market_id} not found")
        return engine.get_user_position(self._normalize_user_id(user_id))
    
    def get_all_positions(self, market_id: str | MarketId) -> list[Position]:
        """Get all positions in a market."""
        engine = self.get_engine(market_id)
        if not engine:
            return []
        return engine.get_all_positions()
    
    def get_user_positions_all_markets(self, user_id: str | UserId) -> dict[str, Position]:
        """Get user's positions across all markets."""
        uid = self._normalize_user_id(user_id)
        return {
            str(mid): engine.get_user_position(uid)
            for mid, engine in self._engines.items()
        }
    
    # ─────────────────────────────────────────────────────────────────────────
    # Book Queries
    # ─────────────────────────────────────────────────────────────────────────
    
    def get_book_snapshot(self, market_id: str | MarketId) -> BookSnapshot | None:
        """Get current order book state for a market."""
        engine = self.get_engine(market_id)
        if not engine:
            return None
        return engine.get_book_snapshot()
    
    def get_all_book_snapshots(self) -> dict[str, BookSnapshot]:
        """Get order book state for all markets."""
        return {
            str(mid): engine.get_book_snapshot()
            for mid, engine in self._engines.items()
        }
    
    # ─────────────────────────────────────────────────────────────────────────
    # Game Control
    # ─────────────────────────────────────────────────────────────────────────
    
    def start_all_markets(self) -> None:
        """Start all markets (begin game)."""
        for market_id in self._markets:
            self.set_market_status(market_id, MarketStatus.ACTIVE)
    
    def stop_all_markets(self) -> None:
        """Stop all markets (end game)."""
        for market_id in self._markets:
            self.set_market_status(market_id, MarketStatus.CLOSED)
    
    def halt_all_markets(self) -> None:
        """Halt all markets temporarily."""
        for market_id in self._markets:
            self.set_market_status(market_id, MarketStatus.HALTED)
    
    def resume_all_markets(self) -> None:
        """Resume all halted markets."""
        for market_id in self._markets:
            self.set_market_status(market_id, MarketStatus.ACTIVE)
    
    # ─────────────────────────────────────────────────────────────────────────
    # Statistics & Leaderboard
    # ─────────────────────────────────────────────────────────────────────────
    
    @property
    def total_orders(self) -> int:
        """Total open orders across all markets."""
        return sum(len(e.book) for e in self._engines.values())
    
    @property
    def total_trades(self) -> int:
        """Total trades executed across all markets."""
        return sum(e.trade_count for e in self._engines.values())
    
    def get_leaderboard(self, market_id: str | MarketId) -> list[dict]:
        """
        Get leaderboard for a market.
        
        Returns positions sorted by realized P&L.
        """
        positions = self.get_all_positions(market_id)
        
        leaderboard = []
        for pos in positions:
            if pos.trade_count > 0:  # Only include users who traded
                leaderboard.append({
                    "user_id": str(pos.user_id),
                    "net_position": pos.net_qty,
                    "total_bought": pos.total_bought,
                    "total_sold": pos.total_sold,
                    "avg_buy_price": str(pos.avg_buy_price) if pos.avg_buy_price else None,
                    "avg_sell_price": str(pos.avg_sell_price) if pos.avg_sell_price else None,
                    "realized_pnl": str(pos.realized_pnl),
                    "trade_count": pos.trade_count,
                })
        
        # Sort by realized P&L descending
        leaderboard.sort(key=lambda x: Decimal(x["realized_pnl"]), reverse=True)
        
        # Add rank
        for i, entry in enumerate(leaderboard, 1):
            entry["rank"] = i
        
        return leaderboard
    
    def get_stats(self) -> dict:
        """Get exchange statistics."""
        return {
            "markets": len(self._markets),
            "total_orders": self.total_orders,
            "total_trades": self.total_trades,
            "created_at": self._created_at.isoformat(),
            "market_stats": {
                str(mid): {
                    "title": market.title,
                    "status": market.status.value,
                    "orders": len(self._engines[mid].book),
                    "trades": self._engines[mid].trade_count,
                    "best_bid": str(self._engines[mid].book.best_bid) if self._engines[mid].book.best_bid else None,
                    "best_ask": str(self._engines[mid].book.best_ask) if self._engines[mid].book.best_ask else None,
                    "last_price": str(market.last_trade_price) if market.last_trade_price else None,
                    "volume": market.total_volume,
                }
                for mid, market in self._markets.items()
            },
        }
    
    # ─────────────────────────────────────────────────────────────────────────
    # Admin Operations
    # ─────────────────────────────────────────────────────────────────────────
    
    def admin_cancel_user_everywhere(self, user_id: str | UserId) -> int:
        """[Admin] Cancel all orders for a user across all markets."""
        return self.cancel_all_user_orders_all_markets(user_id)
    
    def admin_get_user_summary(self, user_id: str | UserId) -> dict:
        """[Admin] Get comprehensive summary for a user."""
        uid = self._normalize_user_id(user_id)
        
        summary = {
            "user_id": str(uid),
            "markets": {},
            "total_orders": 0,
            "total_trades": 0,
            "total_realized_pnl": Decimal(0),
        }
        
        for mid, engine in self._engines.items():
            orders = engine.get_user_orders(uid)
            position = engine.get_user_position(uid)
            
            market_summary = {
                "open_orders": len(orders),
                "position": position.to_dict(),
            }
            
            summary["markets"][str(mid)] = market_summary
            summary["total_orders"] += len(orders)
            summary["total_trades"] += position.trade_count
            summary["total_realized_pnl"] += position.realized_pnl
        
        summary["total_realized_pnl"] = str(summary["total_realized_pnl"])
        
        return summary
    
    def __repr__(self) -> str:
        return (
            f"Exchange(markets={len(self._markets)}, "
            f"orders={self.total_orders}, trades={self.total_trades})"
        )
