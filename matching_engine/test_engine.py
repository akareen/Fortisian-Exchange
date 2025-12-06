"""
Tests for the improved matching engine.

Run with: pytest test_engine.py -v
"""

import pytest
from decimal import Decimal
from collections import defaultdict

from matching_engine import (
    Exchange,
    ExchangeConfig,
    Side,
    OrderStatus,
    MarketStatus,
    TimeInForce,
    RejectReason,
    OrderAccepted,
    OrderRejected,
    OrderCancelled,
    OrderExpired,
    OrderFilled,
    TradeExecuted,
    BookSnapshot,
    BookDelta,
    MarketStatusChanged,
    PriceBands,
)


class EventCollector:
    """Collects events for testing."""
    
    def __init__(self):
        self.events: list = []
        self.by_type: dict = defaultdict(list)
    
    def __call__(self, event):
        self.events.append(event)
        self.by_type[type(event).__name__].append(event)
    
    def clear(self):
        self.events.clear()
        self.by_type.clear()
    
    @property
    def trades(self) -> list[TradeExecuted]:
        return self.by_type["TradeExecuted"]
    
    @property
    def fills(self) -> list[OrderFilled]:
        return self.by_type["OrderFilled"]
    
    @property
    def accepts(self) -> list[OrderAccepted]:
        return self.by_type["OrderAccepted"]
    
    @property
    def rejects(self) -> list[OrderRejected]:
        return self.by_type["OrderRejected"]
    
    @property
    def cancels(self) -> list[OrderCancelled]:
        return self.by_type["OrderCancelled"]
    
    @property
    def expirations(self) -> list[OrderExpired]:
        return self.by_type["OrderExpired"]


@pytest.fixture
def collector():
    return EventCollector()


@pytest.fixture
def exchange(collector):
    ex = Exchange(on_event=collector)
    ex.create_market("TEST", "Test Market", description="A test market")
    ex.start_market("TEST")
    return ex


class TestOrderBook:
    """Test basic order book operations."""
    
    def test_add_bid(self, exchange, collector):
        order = exchange.submit_order("TEST", "user1", "buy", "100.00", 10)
        
        assert order is not None
        assert order.side == Side.BUY
        assert order.price == Decimal("100.00")
        assert order.qty == 10
        assert order.status == OrderStatus.OPEN
        assert len(collector.accepts) == 1
    
    def test_add_ask(self, exchange, collector):
        order = exchange.submit_order("TEST", "user1", "sell", "105.00", 10)
        
        assert order is not None
        assert order.side == Side.SELL
        
        snapshot = exchange.get_book_snapshot("TEST")
        assert snapshot.best_bid is None
        assert snapshot.best_ask == Decimal("105.00")
    
    def test_multiple_price_levels(self, exchange, collector):
        exchange.submit_order("TEST", "user1", "buy", "100.00", 10)
        exchange.submit_order("TEST", "user2", "buy", "99.00", 20)
        exchange.submit_order("TEST", "user3", "buy", "101.00", 5)
        
        snapshot = exchange.get_book_snapshot("TEST")
        
        assert snapshot.best_bid == Decimal("101.00")
        assert len(snapshot.bids) == 3
        
        prices = [level.price for level in snapshot.bids]
        assert prices == [Decimal("101.00"), Decimal("100.00"), Decimal("99.00")]


class TestMatching:
    """Test order matching logic."""
    
    def test_exact_match(self, exchange, collector):
        exchange.submit_order("TEST", "buyer", "buy", "100.00", 10)
        collector.clear()
        
        exchange.submit_order("TEST", "seller", "sell", "100.00", 10)
        
        assert len(collector.trades) == 1
        trade = collector.trades[0].trade
        assert trade.price == Decimal("100.00")
        assert trade.qty == 10
        
        snapshot = exchange.get_book_snapshot("TEST")
        assert snapshot.best_bid is None
        assert snapshot.best_ask is None
    
    def test_price_improvement(self, exchange, collector):
        exchange.submit_order("TEST", "buyer", "buy", "100.00", 10)
        collector.clear()
        
        exchange.submit_order("TEST", "seller", "sell", "99.00", 10)
        
        trade = collector.trades[0].trade
        assert trade.price == Decimal("100.00")  # Price improvement
    
    def test_partial_fill(self, exchange, collector):
        exchange.submit_order("TEST", "buyer", "buy", "100.00", 10)
        collector.clear()
        
        exchange.submit_order("TEST", "seller", "sell", "100.00", 3)
        
        trade = collector.trades[0].trade
        assert trade.qty == 3
        
        snapshot = exchange.get_book_snapshot("TEST")
        assert snapshot.best_bid == Decimal("100.00")
        assert snapshot.bids[0].qty == 7
    
    def test_sweep_multiple_levels(self, exchange, collector):
        exchange.submit_order("TEST", "seller1", "sell", "100.00", 5)
        exchange.submit_order("TEST", "seller2", "sell", "101.00", 5)
        exchange.submit_order("TEST", "seller3", "sell", "102.00", 5)
        collector.clear()
        
        exchange.submit_order("TEST", "buyer", "buy", "102.00", 12)
        
        assert len(collector.trades) == 3
        assert collector.trades[0].trade.price == Decimal("100.00")
        assert collector.trades[1].trade.price == Decimal("101.00")
        assert collector.trades[2].trade.price == Decimal("102.00")
    
    def test_time_priority(self, exchange, collector):
        order1 = exchange.submit_order("TEST", "buyer1", "buy", "100.00", 10)
        order2 = exchange.submit_order("TEST", "buyer2", "buy", "100.00", 10)
        collector.clear()
        
        exchange.submit_order("TEST", "seller", "sell", "100.00", 5)
        
        trade = collector.trades[0].trade
        assert str(trade.buy_order_id) == str(order1.id)


class TestSelfTradePrevention:
    """Test self-trade prevention."""
    
    def test_self_trade_rejected(self, exchange, collector):
        exchange.submit_order("TEST", "user1", "buy", "100.00", 10)
        collector.clear()
        
        order = exchange.submit_order("TEST", "user1", "sell", "100.00", 10)
        
        assert order is None
        assert len(collector.rejects) == 1
        assert collector.rejects[0].reason == RejectReason.SELF_TRADE
    
    def test_self_trade_allowed_when_configured(self, collector):
        ex = Exchange(on_event=collector)
        ex.create_market("SELF", "Self Trade Market", allow_self_trade=True)
        ex.start_market("SELF")
        
        ex.submit_order("SELF", "user1", "buy", "100.00", 10)
        collector.clear()
        
        order = ex.submit_order("SELF", "user1", "sell", "100.00", 10)
        
        assert order is not None
        assert len(collector.trades) == 1


class TestTimeInForce:
    """Test IOC and FOK order types."""
    
    def test_ioc_partial_fill(self, exchange, collector):
        exchange.submit_order("TEST", "seller", "sell", "100.00", 5)
        collector.clear()
        
        order = exchange.submit_order(
            "TEST", "buyer", "buy", "100.00", 10,
            time_in_force=TimeInForce.IOC
        )
        
        assert order is not None
        assert len(collector.trades) == 1
        assert len(collector.expirations) == 1
        assert collector.expirations[0].expired_qty == 5
    
    def test_ioc_no_fill(self, exchange, collector):
        order = exchange.submit_order(
            "TEST", "buyer", "buy", "100.00", 10,
            time_in_force=TimeInForce.IOC
        )
        
        assert order is not None
        assert len(collector.trades) == 0
        assert len(collector.expirations) == 1
        assert collector.expirations[0].expired_qty == 10
    
    def test_fok_success(self, exchange, collector):
        exchange.submit_order("TEST", "seller", "sell", "100.00", 10)
        collector.clear()
        
        order = exchange.submit_order(
            "TEST", "buyer", "buy", "100.00", 10,
            time_in_force=TimeInForce.FOK
        )
        
        assert order is not None
        assert len(collector.trades) == 1
    
    def test_fok_rejected_insufficient_qty(self, exchange, collector):
        exchange.submit_order("TEST", "seller", "sell", "100.00", 5)
        collector.clear()
        
        order = exchange.submit_order(
            "TEST", "buyer", "buy", "100.00", 10,
            time_in_force=TimeInForce.FOK
        )
        
        assert order is None
        assert len(collector.rejects) == 1
        assert collector.rejects[0].reason == RejectReason.FOK_CANNOT_FILL


class TestCancellation:
    """Test order cancellation."""
    
    def test_cancel_own_order(self, exchange, collector):
        order = exchange.submit_order("TEST", "user1", "buy", "100.00", 10)
        collector.clear()
        
        result = exchange.cancel_order("TEST", str(order.id), "user1")
        
        assert result is True
        assert len(collector.cancels) == 1
    
    def test_cannot_cancel_others_order(self, exchange, collector):
        order = exchange.submit_order("TEST", "user1", "buy", "100.00", 10)
        
        result = exchange.cancel_order("TEST", str(order.id), "user2")
        
        assert result is False
    
    def test_cancel_all_user_orders(self, exchange, collector):
        exchange.submit_order("TEST", "user1", "buy", "100.00", 10)
        exchange.submit_order("TEST", "user1", "buy", "99.00", 10)
        exchange.submit_order("TEST", "user1", "sell", "105.00", 10)
        collector.clear()
        
        count = exchange.cancel_all_orders("TEST", "user1")
        
        assert count == 3


class TestMarketStatus:
    """Test market status transitions."""
    
    def test_cannot_order_closed_market(self, exchange, collector):
        exchange.stop_market("TEST")
        collector.clear()
        
        order = exchange.submit_order("TEST", "user1", "buy", "100.00", 10)
        
        assert order is None
        assert len(collector.rejects) == 1
        assert collector.rejects[0].reason == RejectReason.MARKET_CLOSED
    
    def test_close_market_cancels_orders(self, exchange, collector):
        exchange.submit_order("TEST", "user1", "buy", "100.00", 10)
        exchange.submit_order("TEST", "user2", "sell", "105.00", 10)
        collector.clear()
        
        exchange.stop_market("TEST")
        
        assert len(collector.cancels) == 2


class TestValidation:
    """Test order validation."""
    
    def test_invalid_quantity(self, exchange, collector):
        order = exchange.submit_order("TEST", "user1", "buy", "100.00", 0)
        assert order is None
        assert len(collector.rejects) == 1
    
    def test_tick_size_validation(self, collector):
        ex = Exchange(on_event=collector)
        ex.create_market("STRICT", "Strict Market", tick_size=Decimal("0.25"))
        ex.start_market("STRICT")
        
        order = ex.submit_order("STRICT", "user1", "buy", "100.25", 10)
        assert order is not None
        
        order = ex.submit_order("STRICT", "user1", "buy", "100.10", 10)
        assert order is None
        assert collector.rejects[-1].reason == RejectReason.TICK_SIZE_VIOLATION


class TestPositionLimits:
    """Test position limit enforcement."""
    
    def test_max_position_buy(self, collector):
        ex = Exchange(on_event=collector)
        ex.create_market("POS", "Position Limited", max_position=100)
        ex.start_market("POS")
        
        order = ex.submit_order("POS", "user1", "buy", "100.00", 50)
        assert order is not None
        
        ex.submit_order("POS", "user2", "sell", "100.00", 50)
        
        collector.clear()
        order = ex.submit_order("POS", "user1", "buy", "100.00", 60)
        assert order is None
        assert collector.rejects[-1].reason == RejectReason.MAX_POSITION


class TestPositionTracking:
    """Test position and P&L tracking."""
    
    def test_position_updates(self, exchange, collector):
        exchange.submit_order("TEST", "buyer", "buy", "100.00", 10)
        exchange.submit_order("TEST", "seller", "sell", "100.00", 10)
        
        buyer_pos = exchange.get_user_position("TEST", "buyer")
        seller_pos = exchange.get_user_position("TEST", "seller")
        
        assert buyer_pos.net_qty == 10
        assert seller_pos.net_qty == -10
    
    def test_realized_pnl(self, exchange, collector):
        exchange.submit_order("TEST", "mm", "sell", "100.00", 10)
        exchange.submit_order("TEST", "trader", "buy", "100.00", 10)
        
        exchange.submit_order("TEST", "mm", "buy", "110.00", 10)
        exchange.submit_order("TEST", "trader", "sell", "110.00", 10)
        
        pos = exchange.get_user_position("TEST", "trader")
        
        assert pos.net_qty == 0
        assert pos.realized_pnl == Decimal("100")


class TestLeaderboard:
    """Test leaderboard functionality."""
    
    def test_leaderboard_ranking(self, exchange, collector):
        exchange.submit_order("TEST", "mm", "sell", "100.00", 10)
        exchange.submit_order("TEST", "user1", "buy", "100.00", 10)
        exchange.submit_order("TEST", "mm", "buy", "110.00", 10)
        exchange.submit_order("TEST", "user1", "sell", "110.00", 10)
        
        exchange.submit_order("TEST", "mm", "sell", "110.00", 10)
        exchange.submit_order("TEST", "user2", "buy", "110.00", 10)
        exchange.submit_order("TEST", "mm", "buy", "100.00", 10)
        exchange.submit_order("TEST", "user2", "sell", "100.00", 10)
        
        leaderboard = exchange.get_leaderboard("TEST")
        
        user_ranks = {entry["user_id"]: entry["rank"] for entry in leaderboard}
        assert user_ranks["user1"] < user_ranks["user2"]


class TestMultipleMarkets:
    """Test multiple market scenarios."""
    
    def test_multiple_markets(self, collector):
        ex = Exchange(on_event=collector)
        ex.create_market("AAPL", "Apple Inc.")
        ex.create_market("GOOG", "Alphabet Inc.")
        ex.start_all_markets()
        
        ex.submit_order("AAPL", "user1", "buy", "150.00", 10)
        ex.submit_order("GOOG", "user1", "buy", "100.00", 10)
        
        all_orders = ex.get_all_user_orders("user1")
        assert len(all_orders["AAPL"]) == 1
        assert len(all_orders["GOOG"]) == 1


class TestEdgeCases:
    """Test edge cases."""
    
    def test_large_order_book(self, exchange, collector):
        for i in range(100):
            exchange.submit_order("TEST", f"buyer{i}", "buy", str(100 - i * 0.01), 10)
        
        snapshot = exchange.get_book_snapshot("TEST")
        assert len(snapshot.bids) == 100
    
    def test_decimal_precision(self, exchange, collector):
        exchange.submit_order("TEST", "buyer", "buy", "100.01", 10)
        exchange.submit_order("TEST", "seller", "sell", "100.01", 10)
        
        trade = collector.trades[0].trade
        assert trade.price == Decimal("100.01")
        assert trade.notional_value == Decimal("1000.10")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
