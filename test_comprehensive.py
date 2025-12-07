"""
Comprehensive Edge Case Test Suite.

Tests all edge cases across the exchange system.
"""

import sys
from decimal import Decimal
from datetime import datetime, timezone
import time

from matching_engine import (
    Exchange, Side, TimeInForce, MarketStatus,
    OrderRejected, OrderCancelled, OrderFilled, TradeExecuted, OrderExpired,
)
from auth import UserStore, SessionManager, create_auth_system
from sweep_orders import SweepOrderManager, SweepAllocation
from analytics import AdminAnalytics
from book_cache import BookCacheManager


class TestRunner:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.current_suite = ""
    
    def suite(self, name: str):
        print(f"\n{'=' * 60}")
        print(f"TEST SUITE: {name}")
        print("=" * 60)
        self.current_suite = name
    
    def test(self, condition: bool, name: str, msg: str = "") -> bool:
        if condition:
            self.passed += 1
            print(f"   ✓ {name}")
        else:
            self.failed += 1
            print(f"   ✗ {name}" + (f": {msg}" if msg else ""))
        return condition
    
    def summary(self) -> bool:
        print(f"\n{'=' * 60}")
        print("FINAL RESULTS")
        print("=" * 60)
        total = self.passed + self.failed
        print(f"\nTotal: {total} tests")
        print(f"Passed: {self.passed}")
        print(f"Failed: {self.failed}")
        print(f"Pass rate: {100 * self.passed / total:.1f}%" if total > 0 else "N/A")
        return self.failed == 0


def main():
    t = TestRunner()
    
    # ─────────────────────────────────────────────────────────────────────
    # Order Validation Edge Cases
    # ─────────────────────────────────────────────────────────────────────
    t.suite("Order Validation Edge Cases")
    
    exchange = Exchange()
    exchange.create_market("TEST", "Test Market", tick_size="0.01", lot_size=10, max_qty=10000)
    exchange.start_market("TEST")
    
    events = []
    exchange._on_event = lambda e: events.append(e)
    
    # Zero quantity - engine rejects and returns None
    events.clear()
    order = exchange.submit_order("TEST", "user1", "buy", Decimal("100.00"), 0)
    rejected = order is None or any(isinstance(e, OrderRejected) for e in events)
    t.test(rejected, "Zero quantity rejected")
    
    # Negative quantity
    events.clear()
    order = exchange.submit_order("TEST", "user1", "buy", Decimal("100.00"), -10)
    rejected = order is None or any(isinstance(e, OrderRejected) for e in events)
    t.test(rejected, "Negative quantity rejected")
    
    # Zero price
    events.clear()
    order = exchange.submit_order("TEST", "user1", "buy", Decimal("0.00"), 100)
    rejected = order is None or any(isinstance(e, OrderRejected) for e in events)
    t.test(rejected, "Zero price rejected")
    
    # Negative price
    events.clear()
    order = exchange.submit_order("TEST", "user1", "buy", Decimal("-10.00"), 100)
    rejected = order is None or any(isinstance(e, OrderRejected) for e in events)
    t.test(rejected, "Negative price rejected")
    
    # Tick size violation
    events.clear()
    exchange.submit_order("TEST", "user1", "buy", Decimal("100.001"), 10)
    t.test(any(isinstance(e, OrderRejected) for e in events), "Tick size violation rejected")
    
    # Lot size violation
    events.clear()
    exchange.submit_order("TEST", "user1", "buy", Decimal("100.00"), 5)
    t.test(any(isinstance(e, OrderRejected) for e in events), "Lot size violation rejected")
    
    # Valid order accepted
    events.clear()
    order = exchange.submit_order("TEST", "user1", "buy", Decimal("100.00"), 10)
    t.test(order is not None, "Valid order accepted")
    
    # Non-existent market
    try:
        exchange.submit_order("FAKE", "user1", "buy", Decimal("100.00"), 10)
        t.test(False, "Non-existent market rejected")
    except:
        t.test(True, "Non-existent market rejected")
    
    # Empty user ID
    try:
        exchange.submit_order("TEST", "", "buy", Decimal("100.00"), 10)
        t.test(False, "Empty user ID rejected")
    except:
        t.test(True, "Empty user ID rejected")
    
    # Max quantity
    events.clear()
    exchange.submit_order("TEST", "user1", "buy", Decimal("100.00"), 10001)
    t.test(any(isinstance(e, OrderRejected) for e in events), "Exceeds max qty rejected")
    
    # Extreme price precision
    events.clear()
    exchange.submit_order("TEST", "user1", "buy", Decimal("100.00"), 10)
    t.test(not any(isinstance(e, OrderRejected) for e in events), "Normal precision accepted")
    
    # ─────────────────────────────────────────────────────────────────────
    # Matching Edge Cases
    # ─────────────────────────────────────────────────────────────────────
    t.suite("Matching Edge Cases")
    
    trades = []
    def on_trade(e):
        if isinstance(e, TradeExecuted):
            trades.append(e.trade)
    
    exchange = Exchange(on_event=on_trade)
    exchange.create_market("TEST", "Test", tick_size="0.01")
    exchange.start_market("TEST")
    
    # Exact match
    exchange.submit_order("TEST", "seller", "sell", Decimal("100.00"), 100)
    exchange.submit_order("TEST", "buyer", "buy", Decimal("100.00"), 100)
    t.test(len(trades) == 1 and trades[0].qty == 100, "Exact quantity match")
    trades.clear()
    
    # Partial match - buyer has more
    exchange.submit_order("TEST", "seller", "sell", Decimal("100.00"), 50)
    exchange.submit_order("TEST", "buyer", "buy", Decimal("100.00"), 100)
    t.test(len(trades) == 1 and trades[0].qty == 50, "Partial match buyer more")
    orders = exchange.get_user_orders("TEST", "buyer")
    t.test(len(orders) == 1 and orders[0].remaining_qty == 50, "Remaining order in book")
    exchange.cancel_all_orders("TEST", "buyer")
    trades.clear()
    
    # Multiple fills from single order
    exchange.submit_order("TEST", "s1", "sell", Decimal("100.00"), 30)
    exchange.submit_order("TEST", "s2", "sell", Decimal("100.00"), 30)
    exchange.submit_order("TEST", "s3", "sell", Decimal("100.00"), 30)
    tc = len(trades)
    exchange.submit_order("TEST", "buyer", "buy", Decimal("100.00"), 90)
    t.test(len(trades) - tc == 3, "Multiple fills from single order")
    trades.clear()
    
    # Price improvement for buyer
    exchange.submit_order("TEST", "seller", "sell", Decimal("99.00"), 100)
    exchange.submit_order("TEST", "buyer", "buy", Decimal("100.00"), 100)
    t.test(trades[-1].price == Decimal("99.00"), "Price improvement for buyer")
    trades.clear()
    
    # Price improvement for seller
    exchange.submit_order("TEST", "buyer", "buy", Decimal("101.00"), 100)
    exchange.submit_order("TEST", "seller", "sell", Decimal("100.00"), 100)
    t.test(trades[-1].price == Decimal("101.00"), "Price improvement for seller")
    trades.clear()
    
    # Self-trade prevention
    exchange.submit_order("TEST", "user1", "buy", Decimal("100.00"), 100)
    tc = len(trades)
    exchange.submit_order("TEST", "user1", "sell", Decimal("100.00"), 100)
    t.test(len(trades) == tc, "Self-trade prevented")
    exchange.cancel_all_orders("TEST", "user1")
    
    # Price-time priority
    exchange.submit_order("TEST", "s1", "sell", Decimal("100.00"), 100)
    time.sleep(0.001)
    exchange.submit_order("TEST", "s2", "sell", Decimal("100.00"), 100)
    tc = len(trades)
    exchange.submit_order("TEST", "buyer", "buy", Decimal("100.00"), 100)
    t.test(len(trades) > tc and str(trades[-1].seller_id) == "s1", "Price-time priority")
    exchange.cancel_all_orders("TEST", "s2")
    trades.clear()
    
    # ─────────────────────────────────────────────────────────────────────
    # Time In Force Edge Cases
    # ─────────────────────────────────────────────────────────────────────
    t.suite("Time In Force Edge Cases")
    
    events = []
    def on_event(e):
        events.append(e)
        if isinstance(e, TradeExecuted):
            trades.append(e.trade)
    
    exchange = Exchange(on_event=on_event)
    exchange.create_market("TEST", "Test", tick_size="0.01")
    exchange.start_market("TEST")
    
    # IOC partial fill + expire
    exchange.submit_order("TEST", "seller", "sell", Decimal("100.00"), 50)
    events.clear()
    exchange.submit_order("TEST", "buyer", "buy", Decimal("100.00"), 100, time_in_force="ioc")
    fills = [e for e in events if isinstance(e, OrderFilled)]
    expires = [e for e in events if isinstance(e, OrderExpired)]
    t.test(len(fills) > 0 and fills[0].fill_qty == 50, "IOC partial fill")
    t.test(len(expires) > 0 and expires[0].expired_qty == 50, "IOC remainder expired")
    trades.clear()
    
    # IOC no match - all expires
    events.clear()
    exchange.submit_order("TEST", "buyer", "buy", Decimal("99.00"), 100, time_in_force="ioc")
    expires = [e for e in events if isinstance(e, OrderExpired)]
    t.test(len(expires) > 0 and expires[0].expired_qty == 100, "IOC no match expires all")
    
    # FOK success
    exchange.submit_order("TEST", "seller", "sell", Decimal("100.00"), 100)
    tc = len(trades)
    order = exchange.submit_order("TEST", "buyer", "buy", Decimal("100.00"), 100, time_in_force="fok")
    t.test(order is not None and len(trades) > tc, "FOK full fill succeeds")
    trades.clear()
    
    # FOK failure
    exchange.submit_order("TEST", "seller", "sell", Decimal("100.00"), 50)
    events.clear()
    order = exchange.submit_order("TEST", "buyer", "buy", Decimal("100.00"), 100, time_in_force="fok")
    t.test(order is None, "FOK insufficient liquidity returns None")
    t.test(any(isinstance(e, OrderRejected) for e in events), "FOK rejection event")
    exchange.cancel_all_orders("TEST", "seller")
    
    # ─────────────────────────────────────────────────────────────────────
    # Position Edge Cases
    # ─────────────────────────────────────────────────────────────────────
    t.suite("Position Edge Cases")
    
    exchange = Exchange()
    exchange.create_market("TEST", "Test", tick_size="0.01")
    exchange.start_market("TEST")
    
    # Initial position
    pos = exchange.get_user_position("TEST", "user1")
    t.test(pos.net_qty == 0, "Initial position zero")
    t.test(pos.realized_pnl == Decimal("0"), "Initial PnL zero")
    
    # Long position
    exchange.submit_order("TEST", "user1", "buy", Decimal("100.00"), 100)
    exchange.submit_order("TEST", "user2", "sell", Decimal("100.00"), 100)
    pos = exchange.get_user_position("TEST", "user1")
    t.test(pos.net_qty == 100, "Long position after buy")
    
    # Close for profit
    exchange.submit_order("TEST", "user3", "buy", Decimal("110.00"), 100)
    exchange.submit_order("TEST", "user1", "sell", Decimal("110.00"), 100)
    pos = exchange.get_user_position("TEST", "user1")
    t.test(pos.net_qty == 0, "Position closed")
    t.test(pos.realized_pnl == Decimal("1000"), "Profit calculated correctly")
    
    # Short position
    exchange.submit_order("TEST", "user4", "buy", Decimal("100.00"), 100)
    exchange.submit_order("TEST", "user5", "sell", Decimal("100.00"), 100)
    pos = exchange.get_user_position("TEST", "user5")
    t.test(pos.net_qty == -100, "Short position after sell")
    
    # Close short for loss
    exchange.submit_order("TEST", "user5", "buy", Decimal("110.00"), 100)
    exchange.submit_order("TEST", "user6", "sell", Decimal("110.00"), 100)
    pos = exchange.get_user_position("TEST", "user5")
    t.test(pos.net_qty == 0, "Short position closed")
    t.test(pos.realized_pnl == Decimal("-1000"), "Loss calculated correctly")
    
    # ─────────────────────────────────────────────────────────────────────
    # Cancel Order Edge Cases
    # ─────────────────────────────────────────────────────────────────────
    t.suite("Cancel Order Edge Cases")
    
    exchange = Exchange()
    exchange.create_market("TEST", "Test", tick_size="0.01")
    exchange.start_market("TEST")
    
    # Cancel own order
    order = exchange.submit_order("TEST", "user1", "buy", Decimal("100.00"), 100)
    success = exchange.cancel_order("TEST", str(order.id), "user1")
    t.test(success, "Cancel own order succeeds")
    
    # Cancel non-existent order
    success = exchange.cancel_order("TEST", "nonexistent", "user1")
    t.test(not success, "Cancel non-existent order fails")
    
    # Cancel other user's order
    order = exchange.submit_order("TEST", "user1", "buy", Decimal("100.00"), 100)
    success = exchange.cancel_order("TEST", str(order.id), "user2")
    t.test(not success, "Cancel other user's order fails")
    
    # Cancel already filled order
    exchange.submit_order("TEST", "user2", "sell", Decimal("100.00"), 100)
    success = exchange.cancel_order("TEST", str(order.id), "user1")
    t.test(not success, "Cancel filled order fails")
    
    # Cancel all orders
    exchange.submit_order("TEST", "user3", "buy", Decimal("99.00"), 100)
    exchange.submit_order("TEST", "user3", "buy", Decimal("98.00"), 100)
    exchange.submit_order("TEST", "user3", "buy", Decimal("97.00"), 100)
    count = exchange.cancel_all_orders("TEST", "user3")
    t.test(count == 3, "Cancel all orders")
    
    # ─────────────────────────────────────────────────────────────────────
    # Market Status Edge Cases
    # ─────────────────────────────────────────────────────────────────────
    t.suite("Market Status Edge Cases")
    
    events = []
    exchange = Exchange(on_event=lambda e: events.append(e))
    exchange.create_market("TEST", "Test", tick_size="0.01")
    
    # Order on pending market
    events.clear()
    exchange.submit_order("TEST", "user1", "buy", Decimal("100.00"), 100)
    t.test(any(isinstance(e, OrderRejected) for e in events), "Order on pending market rejected")
    
    # Start market
    exchange.start_market("TEST")
    order = exchange.submit_order("TEST", "user1", "buy", Decimal("100.00"), 100)
    t.test(order is not None, "Order on active market accepted")
    
    # Halt market
    exchange.halt_market("TEST")
    events.clear()
    exchange.submit_order("TEST", "user2", "buy", Decimal("100.00"), 100)
    t.test(any(isinstance(e, OrderRejected) for e in events), "Order on halted market rejected")
    
    # Resume market
    exchange.resume_market("TEST")
    order = exchange.submit_order("TEST", "user2", "buy", Decimal("100.00"), 100)
    t.test(order is not None, "Order on resumed market accepted")
    
    # Close market - cancels all orders
    exchange.stop_market("TEST")
    orders = exchange.get_user_orders("TEST", "user1")
    t.test(len(orders) == 0, "Close market cancels orders")
    
    # ─────────────────────────────────────────────────────────────────────
    # Authentication Edge Cases
    # ─────────────────────────────────────────────────────────────────────
    t.suite("Authentication Edge Cases")
    
    user_store, sessions = create_auth_system(create_default_admin=False)
    
    # Create user
    user = user_store.create_user("test_user", "password123", "Test User")
    t.test(user is not None, "Create user")
    
    # Duplicate user
    try:
        user_store.create_user("test_user", "password456")
        t.test(False, "Duplicate user rejected")
    except ValueError:
        t.test(True, "Duplicate user rejected")
    
    # Empty password
    try:
        user_store.create_user("user2", "")
        t.test(False, "Empty password rejected")
    except ValueError:
        t.test(True, "Empty password rejected")
    
    # Short password
    try:
        user_store.create_user("user3", "abc")
        t.test(False, "Short password rejected")
    except ValueError:
        t.test(True, "Short password rejected")
    
    # Successful login
    result = sessions.login("test_user", "password123")
    t.test(not isinstance(result, tuple), "Successful login")
    
    # Wrong password
    result = sessions.login("test_user", "wrongpass")
    t.test(isinstance(result, tuple) and result[0] is None, "Wrong password rejected")
    
    # Non-existent user
    result = sessions.login("nonexistent", "password")
    t.test(isinstance(result, tuple) and result[0] is None, "Non-existent user rejected")
    
    # Ban user
    sessions.ban_user("test_user", "Testing")
    result = sessions.login("test_user", "password123")
    t.test(isinstance(result, tuple) and "banned" in result[1].lower(), "Banned user rejected")
    
    # Unban user
    sessions.unban_user("test_user")
    result = sessions.login("test_user", "password123")
    t.test(not isinstance(result, tuple), "Unbanned user can login")
    
    # Admin flag
    user_store.create_user("admin_user", "adminpass", is_admin=True)
    result = sessions.login("admin_user", "adminpass")
    t.test(not isinstance(result, tuple) and result.is_admin, "Admin flag preserved")
    
    # ─────────────────────────────────────────────────────────────────────
    # Sweep Order Edge Cases
    # ─────────────────────────────────────────────────────────────────────
    t.suite("Sweep Order Edge Cases")
    
    exchange = Exchange()
    exchange.create_market("TEST", "Test", tick_size="0.01")
    exchange.start_market("TEST")
    
    sweep = SweepOrderManager(exchange)
    
    # Basic sweep buy
    result = sweep.submit_sweep("TEST", "user1", "buy", "100.00", "99.50", 500)
    t.test(result.success, "Sweep buy succeeds")
    t.test(result.orders_submitted == 51, "Correct number of price levels")
    
    # Basic sweep sell
    result = sweep.submit_sweep("TEST", "user2", "sell", "100.00", "100.50", 300)
    t.test(result.success, "Sweep sell succeeds")
    
    # Non-existent market
    result = sweep.submit_sweep("FAKE", "user1", "buy", "100.00", "99.00", 100)
    t.test(not result.success, "Sweep on non-existent market fails")
    
    # Zero quantity
    result = sweep.submit_sweep("TEST", "user1", "buy", "100.00", "99.00", 0)
    t.test(result.total_qty_accepted == 0, "Zero quantity sweep")
    
    # Inverted price range (should auto-correct)
    result = sweep.submit_sweep("TEST", "user3", "buy", "99.00", "100.00", 500)
    t.test(result.success, "Inverted price range handled")
    
    # Cancel sweep
    cancelled = sweep.cancel_sweep(result.sweep_id, "user3")
    t.test(cancelled > 0, "Cancel sweep orders")
    
    # Cannot cancel other user's sweep
    result2 = sweep.submit_sweep("TEST", "user4", "buy", "98.00", "97.50", 100)
    cancelled = sweep.cancel_sweep(result2.sweep_id, "user1")
    t.test(cancelled == 0, "Cannot cancel other user's sweep")
    
    # ─────────────────────────────────────────────────────────────────────
    # Book Cache Edge Cases
    # ─────────────────────────────────────────────────────────────────────
    t.suite("Book Cache Edge Cases")
    
    exchange = Exchange()
    exchange.create_market("TEST", "Test", tick_size="0.01")
    exchange.start_market("TEST")
    
    cache = BookCacheManager()
    cache.initialize_from_exchange(exchange)
    
    # Get non-existent market
    snapshot, error = cache.get_book_snapshot("FAKE", "user1")
    t.test(snapshot is None and error != "", "Non-existent market returns error")
    
    # Rate limiting
    for i in range(25):
        _, error = cache.get_book_snapshot("TEST", "user1")
    t.test(error != "", "Rate limiting enforced")
    
    # Different users have separate limits
    snapshot, error = cache.get_book_snapshot("TEST", "user2")
    t.test(error == "", "Different user not rate limited")
    
    # ─────────────────────────────────────────────────────────────────────
    # Analytics Edge Cases
    # ─────────────────────────────────────────────────────────────────────
    t.suite("Analytics Edge Cases")
    
    trades = []
    
    def on_event(e):
        if isinstance(e, TradeExecuted):
            trades.append(e.trade)
            analytics.record_trade(e.trade)
    
    exchange = Exchange(on_event=on_event)
    analytics = AdminAnalytics(exchange)
    
    exchange.create_market("TEST", "Test", tick_size="0.01")
    exchange.start_market("TEST")
    
    # Analysis with no trades
    analysis = analytics.get_user_trade_analysis("user1")
    t.test(analysis.total_trades == 0, "Empty analysis for no trades")
    
    # Generate trades
    exchange.submit_order("TEST", "user1", "buy", Decimal("100.00"), 100)
    exchange.submit_order("TEST", "user2", "sell", Decimal("100.00"), 100)
    exchange.submit_order("TEST", "user2", "buy", Decimal("99.00"), 50)
    exchange.submit_order("TEST", "user1", "sell", Decimal("99.00"), 50)
    
    # User analysis
    analysis = analytics.get_user_trade_analysis("user1")
    t.test(analysis.total_trades == 2, "User trade count")
    t.test(analysis.total_volume == 150, "User volume")
    
    # Market analysis
    market = analytics.get_market_analysis("TEST")
    t.test(market.total_trades == 2, "Market trade count")
    t.test(market.unique_traders == 2, "Unique traders")
    
    # Leaderboard
    leaderboard = analytics.get_leaderboard("TEST")
    t.test(len(leaderboard) > 0, "Leaderboard generated")
    t.test(leaderboard[0].rank == 1, "Leaderboard ranked")
    
    # Anomaly detection
    anomalies = analytics.detect_anomalies()
    t.test(isinstance(anomalies, list), "Anomaly detection runs")
    
    # ─────────────────────────────────────────────────────────────────────
    # Concurrent Operations
    # ─────────────────────────────────────────────────────────────────────
    t.suite("Concurrent Operations")
    
    exchange = Exchange()
    exchange.create_market("TEST", "Test", tick_size="0.01")
    exchange.start_market("TEST")
    
    import threading
    errors = []
    
    def submit_orders(user_id, count):
        for i in range(count):
            try:
                exchange.submit_order("TEST", user_id, "buy", Decimal(f"{100 + i * 0.01:.2f}"), 10)
            except Exception as e:
                errors.append(str(e))
    
    # Submit from multiple threads
    threads = [
        threading.Thread(target=submit_orders, args=(f"user{i}", 20))
        for i in range(5)
    ]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    
    t.test(len(errors) == 0, f"Concurrent order submission (errors: {len(errors)})")
    
    # Check book integrity
    snapshot = exchange.get_book_snapshot("TEST")
    t.test(snapshot is not None, "Book intact after concurrent access")
    
    # ─────────────────────────────────────────────────────────────────────
    # Summary
    # ─────────────────────────────────────────────────────────────────────
    
    success = t.summary()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
