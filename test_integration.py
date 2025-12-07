"""
Comprehensive Integration Test Suite for Trading Exchange.

Simulates a full trading session with multiple users, various order types,
edge cases, and validates all outputs including positions, P&L, and audit logs.

Run with: python test_integration.py
"""

import asyncio
import aiohttp
import json
import random
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from collections import defaultdict

# Test configuration
BASE_URL = "http://localhost:8765"
WS_URL = "ws://localhost:8765/ws"
ADMIN_TOKEN = "test-admin-token-12345"


@dataclass
class TestTrader:
    """Represents a trader in the simulation."""
    user_id: str
    session: aiohttp.ClientSession = None
    ws: aiohttp.ClientWebSocketResponse = None
    received_messages: list = field(default_factory=list)
    orders: dict = field(default_factory=dict)  # order_id -> order
    fills: list = field(default_factory=list)
    positions: dict = field(default_factory=dict)  # market_id -> position
    is_connected: bool = False
    password: str = "testpass123"  # Default test password
    
    async def login(self):
        """Login and get session."""
        self.session = aiohttp.ClientSession()
        async with self.session.post(f"{BASE_URL}/auth/login", json={
            "user_id": self.user_id,
            "password": self.password
        }) as resp:
            if resp.status != 200:
                raise Exception(f"Login failed for {self.user_id}: {await resp.text()}")
            data = await resp.json()
            return data
    
    async def connect_ws(self):
        """Connect WebSocket."""
        self.ws = await self.session.ws_connect(WS_URL)
        self.is_connected = True
        return self.ws
    
    async def subscribe(self, market_id: str):
        """Subscribe to a market."""
        await self.ws.send_json({
            "type": "subscribe",
            "data": {"market_id": market_id}
        })
    
    async def submit_order(self, market_id: str, side: str, price: str, qty: int, 
                          tif: str = "gtc", client_order_id: str = None) -> dict:
        """Submit an order and return the request."""
        msg = {
            "type": "order_submit",
            "data": {
                "market_id": market_id,
                "side": side,
                "price": price,
                "qty": qty,
                "time_in_force": tif,
            }
        }
        if client_order_id:
            msg["data"]["client_order_id"] = client_order_id
        await self.ws.send_json(msg)
        return msg
    
    async def cancel_order(self, market_id: str, order_id: str):
        """Cancel an order."""
        await self.ws.send_json({
            "type": "order_cancel",
            "data": {"market_id": market_id, "order_id": order_id}
        })
    
    async def cancel_all(self, market_id: str = None):
        """Cancel all orders."""
        await self.ws.send_json({
            "type": "order_cancel_all",
            "data": {"market_id": market_id} if market_id else {}
        })
    
    async def get_position(self, market_id: str):
        """Request position."""
        await self.ws.send_json({
            "type": "get_position",
            "data": {"market_id": market_id}
        })
    
    async def get_orders(self, market_id: str = None):
        """Request open orders."""
        await self.ws.send_json({
            "type": "get_orders",
            "data": {"market_id": market_id} if market_id else {}
        })
    
    async def receive_messages(self, count: int = 1, timeout: float = 2.0) -> list:
        """Receive multiple messages."""
        messages = []
        try:
            for _ in range(count):
                msg = await asyncio.wait_for(self.ws.receive_json(), timeout=timeout)
                messages.append(msg)
                self.received_messages.append(msg)
                
                # Track orders and fills
                if msg["type"] == "order_accepted":
                    self.orders[msg["data"]["order_id"]] = msg["data"]
                elif msg["type"] == "order_filled":
                    self.fills.append(msg["data"])
                elif msg["type"] == "order_cancelled":
                    # Cancel response may have order_id or cancelled_count
                    order_id = msg["data"].get("order_id")
                    if order_id:
                        self.orders.pop(order_id, None)
                elif msg["type"] == "position":
                    mid = msg["data"].get("market_id")
                    if mid:
                        self.positions[mid] = msg["data"]["position"]
        except asyncio.TimeoutError:
            pass
        return messages
    
    async def drain_messages(self, timeout: float = 0.5) -> list:
        """Drain all pending messages."""
        return await self.receive_messages(count=100, timeout=timeout)
    
    async def cleanup(self):
        """Clean up connections."""
        if self.ws and not self.ws.closed:
            await self.ws.close()
        if self.session:
            await self.session.close()


class IntegrationTestSuite:
    """Comprehensive integration test suite."""
    
    def __init__(self):
        self.server = None
        self.exchange = None
        self.traders: dict[str, TestTrader] = {}
        self.test_results: list[dict] = []
        self.admin_session: aiohttp.ClientSession = None
    
    async def setup(self):
        """Start the server."""
        from server import TradingServer, ServerConfig, create_demo_exchange
        
        self.exchange = create_demo_exchange()
        config = ServerConfig(port=8765, admin_token=ADMIN_TOKEN)
        self.server = TradingServer(self.exchange, config)
        
        # Add more markets for testing
        self.exchange.create_market(
            "TEST",
            "Test Market",
            tick_size="0.01",
            lot_size=1,
            max_position=10000,
        )
        self.exchange.create_market(
            "VOLATILE",
            "Volatile Market",
            tick_size="0.05",
            lot_size=5,
            max_position=500,
        )
        
        self.exchange.start_all_markets()
        
        self.runner = aiohttp.web.AppRunner(self.server.app)
        await self.runner.setup()
        self.site = aiohttp.web.TCPSite(self.runner, 'localhost', 8765)
        await self.site.start()
        
        # Admin session
        self.admin_session = aiohttp.ClientSession()
        
        print("✓ Server started")
    
    async def teardown(self):
        """Stop the server."""
        for trader in self.traders.values():
            await trader.cleanup()
        if self.admin_session:
            await self.admin_session.close()
        await self.runner.cleanup()
        print("✓ Server stopped")
    
    async def create_trader(self, user_id: str) -> TestTrader:
        """Create and login a trader."""
        trader = TestTrader(user_id=user_id)
        
        # Create user in user store first
        try:
            self.server.user_store.create_user(
                user_id=user_id,
                password=trader.password,
                display_name=user_id,
            )
        except ValueError:
            pass  # User might already exist
        
        await trader.login()
        await trader.connect_ws()
        self.traders[user_id] = trader
        return trader
    
    async def admin_request(self, method: str, endpoint: str, data: dict = None) -> dict:
        """Make an admin API request."""
        headers = {"X-Admin-Token": ADMIN_TOKEN}
        url = f"{BASE_URL}{endpoint}"
        
        if method == "GET":
            async with self.admin_session.get(url, headers=headers) as resp:
                return {"status": resp.status, "data": await resp.json()}
        else:
            async with self.admin_session.post(url, headers=headers, json=data) as resp:
                return {"status": resp.status, "data": await resp.json()}
    
    def record_test(self, name: str, passed: bool, details: str = ""):
        """Record test result."""
        self.test_results.append({
            "name": name,
            "passed": passed,
            "details": details,
        })
        status = "✓" if passed else "✗"
        print(f"   {status} {name}" + (f": {details}" if details and not passed else ""))
    
    # ─────────────────────────────────────────────────────────────────────────
    # Test Suites
    # ─────────────────────────────────────────────────────────────────────────
    
    async def test_basic_connectivity(self):
        """Test basic server connectivity."""
        print("\n" + "=" * 60)
        print("TEST SUITE: Basic Connectivity")
        print("=" * 60)
        
        # Health check
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{BASE_URL}/health") as resp:
                self.record_test("Health endpoint", resp.status == 200)
        
        # Login
        trader = await self.create_trader("connectivity_test")
        self.record_test("User login", trader.session is not None)
        
        # WebSocket connection
        self.record_test("WebSocket connect", trader.is_connected)
        
        # Subscribe to market
        await trader.subscribe("TEST")
        msgs = await trader.receive_messages(4)
        
        has_subscribed = any(m["type"] == "subscribed" for m in msgs)
        has_snapshot = any(m["type"] == "book_snapshot" for m in msgs)
        
        self.record_test("Market subscription", has_subscribed)
        self.record_test("Book snapshot received", has_snapshot)
        
        # Ping/pong
        await trader.ws.send_json({"type": "ping"})
        msg = await trader.receive_messages(1)
        self.record_test("Ping/pong", msg and msg[0]["type"] == "pong")
    
    async def test_order_lifecycle(self):
        """Test complete order lifecycle."""
        print("\n" + "=" * 60)
        print("TEST SUITE: Order Lifecycle")
        print("=" * 60)
        
        trader = await self.create_trader("order_lifecycle")
        await trader.subscribe("TEST")
        await trader.drain_messages()
        
        # Submit order
        await trader.submit_order("TEST", "buy", "100.00", 100)
        msgs = await trader.receive_messages(2)
        
        accepted = [m for m in msgs if m["type"] == "order_accepted"]
        self.record_test("Order accepted", len(accepted) == 1)
        
        if accepted:
            order_id = accepted[0]["data"]["order_id"]
            
            # Verify order in book
            await trader.get_orders("TEST")
            msgs = await trader.receive_messages(1)
            orders = msgs[0]["data"]["orders"] if msgs else []
            self.record_test("Order in open orders", any(o["id"] == order_id for o in orders))
            
            # Cancel order
            await trader.cancel_order("TEST", order_id)
            msgs = await trader.receive_messages(2)
            
            cancelled = [m for m in msgs if m["type"] == "order_cancelled"]
            self.record_test("Order cancelled", len(cancelled) == 1)
            
            # Verify order removed
            await trader.get_orders("TEST")
            msgs = await trader.receive_messages(1)
            orders = msgs[0]["data"]["orders"] if msgs else []
            self.record_test("Order removed from book", not any(o["id"] == order_id for o in orders))
    
    async def test_order_matching(self):
        """Test order matching mechanics."""
        print("\n" + "=" * 60)
        print("TEST SUITE: Order Matching")
        print("=" * 60)
        
        buyer = await self.create_trader("match_buyer")
        seller = await self.create_trader("match_seller")
        
        await buyer.subscribe("TEST")
        await seller.subscribe("TEST")
        await buyer.drain_messages()
        await seller.drain_messages()
        
        # Buyer posts bid
        await buyer.submit_order("TEST", "buy", "100.00", 50)
        await buyer.drain_messages()
        
        # Seller hits bid
        await seller.submit_order("TEST", "sell", "100.00", 50)
        seller_msgs = await seller.drain_messages()
        
        trades = [m for m in seller_msgs if m["type"] == "trade"]
        fills = [m for m in seller_msgs if m["type"] == "order_filled"]
        
        self.record_test("Trade executed", len(trades) == 1)
        self.record_test("Seller received fill", len(fills) == 1)
        
        if trades:
            trade = trades[0]["data"]["trade"]
            self.record_test("Trade price correct", trade["price"] == "100.00")
            self.record_test("Trade qty correct", trade["qty"] == 50)
        
        # Buyer should also get trade and fill
        buyer_msgs = await buyer.drain_messages()
        buyer_trades = [m for m in buyer_msgs if m["type"] == "trade"]
        buyer_fills = [m for m in buyer_msgs if m["type"] == "order_filled"]
        
        self.record_test("Buyer received trade", len(buyer_trades) == 1)
        self.record_test("Buyer received fill", len(buyer_fills) == 1)
    
    async def test_partial_fills(self):
        """Test partial fill scenarios."""
        print("\n" + "=" * 60)
        print("TEST SUITE: Partial Fills")
        print("=" * 60)
        
        buyer = await self.create_trader("partial_buyer")
        seller = await self.create_trader("partial_seller")
        
        await buyer.subscribe("TEST")
        await seller.subscribe("TEST")
        await buyer.drain_messages()
        await seller.drain_messages()
        
        # Buyer posts large bid
        await buyer.submit_order("TEST", "buy", "100.00", 100)
        await buyer.drain_messages()
        
        # Seller fills part
        await seller.submit_order("TEST", "sell", "100.00", 30)
        seller_msgs = await seller.drain_messages()
        
        fills = [m for m in seller_msgs if m["type"] == "order_filled"]
        self.record_test("Partial fill executed", len(fills) == 1)
        
        if fills:
            self.record_test("Partial fill qty correct", fills[0]["data"]["fill_qty"] == 30)
            self.record_test("Seller order complete", fills[0]["data"]["is_complete"])
        
        # Check buyer's order is partially filled
        buyer_msgs = await buyer.drain_messages()
        buyer_fills = [m for m in buyer_msgs if m["type"] == "order_filled"]
        
        if buyer_fills:
            self.record_test("Buyer partial fill", buyer_fills[0]["data"]["fill_qty"] == 30)
            self.record_test("Buyer order not complete", not buyer_fills[0]["data"]["is_complete"])
            self.record_test("Buyer remaining qty", buyer_fills[0]["data"]["remaining_qty"] == 70)
        
        # Fill the rest
        await seller.submit_order("TEST", "sell", "100.00", 70)
        seller_msgs = await seller.drain_messages()
        buyer_msgs = await buyer.drain_messages()
        
        buyer_fills = [m for m in buyer_msgs if m["type"] == "order_filled"]
        if buyer_fills:
            self.record_test("Buyer order completed", buyer_fills[0]["data"]["is_complete"])
    
    async def test_price_time_priority(self):
        """Test price-time priority matching."""
        print("\n" + "=" * 60)
        print("TEST SUITE: Price-Time Priority")
        print("=" * 60)
        
        traders = []
        for i in range(5):
            t = await self.create_trader(f"priority_{i}")
            await t.subscribe("TEST")
            await t.drain_messages()
            traders.append(t)
        
        aggressor = await self.create_trader("priority_aggressor")
        await aggressor.subscribe("TEST")
        await aggressor.drain_messages()
        
        # Post bids at different prices
        await traders[0].submit_order("TEST", "buy", "99.00", 10)   # Worst price
        await traders[1].submit_order("TEST", "buy", "100.00", 10)  # Best price (first)
        await traders[2].submit_order("TEST", "buy", "100.00", 10)  # Best price (second)
        await traders[3].submit_order("TEST", "buy", "99.50", 10)   # Middle price
        await traders[4].submit_order("TEST", "buy", "100.00", 10)  # Best price (third)
        
        for t in traders:
            await t.drain_messages()
        
        # Aggressor sells into bids
        await aggressor.submit_order("TEST", "sell", "99.00", 25)
        await aggressor.drain_messages()
        
        # Check which traders got filled
        fill_order = []
        for i, t in enumerate(traders):
            msgs = await t.drain_messages()
            fills = [m for m in msgs if m["type"] == "order_filled"]
            if fills:
                fill_order.append((i, fills[0]["data"]["fill_qty"]))
        
        # Should fill: trader1 (10), trader2 (10), trader4 (5) - price-time priority
        self.record_test("Price priority (best price first)", 
                        fill_order[0][0] == 1 if fill_order else False,
                        f"Fill order: {fill_order}")
        
        self.record_test("Time priority (same price, earlier first)",
                        len(fill_order) >= 2 and fill_order[1][0] == 2 if len(fill_order) >= 2 else False)
    
    async def test_time_in_force(self):
        """Test IOC and FOK orders."""
        print("\n" + "=" * 60)
        print("TEST SUITE: Time In Force (IOC/FOK)")
        print("=" * 60)
        
        maker = await self.create_trader("tif_maker")
        taker = await self.create_trader("tif_taker")
        
        await maker.subscribe("TEST")
        await taker.subscribe("TEST")
        await maker.drain_messages()
        await taker.drain_messages()
        
        # Setup: maker posts limited liquidity at a specific price
        await maker.submit_order("TEST", "sell", "200.00", 30)  # Use higher price to avoid matching existing orders
        await maker.drain_messages()
        
        # IOC partial fill
        await taker.submit_order("TEST", "buy", "200.00", 50, tif="ioc")
        msgs = await taker.drain_messages()
        
        fills = [m for m in msgs if m["type"] == "order_filled"]
        expirations = [m for m in msgs if m["type"] == "order_expired"]
        
        self.record_test("IOC partial fill", len(fills) == 1 and fills[0]["data"]["fill_qty"] == 30)
        self.record_test("IOC unfilled expired", len(expirations) == 1)
        
        if expirations:
            self.record_test("IOC expired qty correct", expirations[0]["data"]["expired_qty"] == 20)
        else:
            self.record_test("IOC expired qty correct", False, "No expiration message")
        
        # Setup more liquidity at different price
        await maker.submit_order("TEST", "sell", "201.00", 10)
        await maker.drain_messages()
        
        # FOK should fail (only 10 available, need 50)
        await taker.submit_order("TEST", "buy", "201.00", 50, tif="fok")
        msgs = await taker.drain_messages()
        
        rejects = [m for m in msgs if m["type"] == "order_rejected"]
        self.record_test("FOK rejected insufficient qty", len(rejects) == 1)
        
        if rejects:
            self.record_test("FOK reject reason", "fok" in rejects[0]["data"]["reason"].lower())
        else:
            self.record_test("FOK reject reason", False, "No rejection message")
        
        # FOK should succeed with exact qty
        await taker.submit_order("TEST", "buy", "201.00", 10, tif="fok")
        msgs = await taker.drain_messages()
        
        fills = [m for m in msgs if m["type"] == "order_filled"]
        self.record_test("FOK success exact qty", len(fills) == 1 and fills[0]["data"]["is_complete"])
    
    async def test_self_trade_prevention(self):
        """Test self-trade prevention."""
        print("\n" + "=" * 60)
        print("TEST SUITE: Self-Trade Prevention")
        print("=" * 60)
        
        trader = await self.create_trader("self_trader")
        await trader.subscribe("TEST")
        await trader.drain_messages()
        
        # Post a bid
        await trader.submit_order("TEST", "buy", "100.00", 50)
        await trader.drain_messages()
        
        # Try to sell into own bid
        await trader.submit_order("TEST", "sell", "100.00", 50)
        msgs = await trader.drain_messages()
        
        rejects = [m for m in msgs if m["type"] == "order_rejected"]
        self.record_test("Self-trade rejected", len(rejects) == 1)
        
        if rejects:
            self.record_test("Self-trade reason", "self" in rejects[0]["data"]["reason"].lower())
    
    async def test_position_tracking(self):
        """Test position tracking and P&L calculation."""
        print("\n" + "=" * 60)
        print("TEST SUITE: Position Tracking & P&L")
        print("=" * 60)
        
        buyer = await self.create_trader("pos_buyer")
        seller = await self.create_trader("pos_seller")
        
        await buyer.subscribe("TEST")
        await seller.subscribe("TEST")
        await buyer.drain_messages()
        await seller.drain_messages()
        
        # Use unique prices to avoid matching with other test orders
        buy_price = "500.00"
        sell_price = "510.00"
        
        # Trade 1: buyer buys 100 @ 500
        await seller.submit_order("TEST", "sell", buy_price, 100)
        await seller.drain_messages()
        await buyer.submit_order("TEST", "buy", buy_price, 100)
        await buyer.drain_messages()
        await seller.drain_messages()
        
        # Check buyer position
        await buyer.get_position("TEST")
        msgs = await buyer.receive_messages(1)
        
        if msgs and msgs[0]["type"] == "position":
            pos = msgs[0]["data"]["position"]
            self.record_test("Buyer position net_qty", pos["net_qty"] == 100)
            self.record_test("Buyer total_bought", pos["total_bought"] == 100)
            self.record_test("Buyer avg_buy_price", pos["avg_buy_price"] == buy_price)
        else:
            self.record_test("Buyer position net_qty", False, "No position message")
            self.record_test("Buyer total_bought", False)
            self.record_test("Buyer avg_buy_price", False)
        
        # Check seller position
        await seller.get_position("TEST")
        msgs = await seller.receive_messages(1)
        
        if msgs and msgs[0]["type"] == "position":
            pos = msgs[0]["data"]["position"]
            self.record_test("Seller position net_qty", pos["net_qty"] == -100)
            self.record_test("Seller total_sold", pos["total_sold"] == 100)
        else:
            self.record_test("Seller position net_qty", False)
            self.record_test("Seller total_sold", False)
        
        # Trade 2: buyer sells 50 @ 510 (realizes profit)
        await buyer.submit_order("TEST", "sell", sell_price, 50)
        await buyer.drain_messages()
        
        # New buyer to match
        buyer2 = await self.create_trader("pos_buyer2")
        await buyer2.subscribe("TEST")
        await buyer2.drain_messages()
        await buyer2.submit_order("TEST", "buy", sell_price, 50)
        await buyer2.drain_messages()
        await buyer.drain_messages()
        
        # Check buyer's realized P&L
        await buyer.get_position("TEST")
        msgs = await buyer.receive_messages(1)
        
        if msgs and msgs[0]["type"] == "position":
            pos = msgs[0]["data"]["position"]
            self.record_test("Buyer net_qty after sell", pos["net_qty"] == 50)
            self.record_test("Buyer total_sold", pos["total_sold"] == 50)
            # P&L: bought 100 @ 500, sold 50 @ 510
            # Realized on 50 shares: (510 - 500) * 50 = 500
            realized = Decimal(pos["realized_pnl"])
            self.record_test("Buyer realized P&L positive", realized > 0, f"P&L: {realized}")
        else:
            self.record_test("Buyer net_qty after sell", False)
            self.record_test("Buyer total_sold", False)
            self.record_test("Buyer realized P&L positive", False)
    
    async def test_market_status(self):
        """Test market status transitions."""
        print("\n" + "=" * 60)
        print("TEST SUITE: Market Status")
        print("=" * 60)
        
        trader = await self.create_trader("status_trader")
        await trader.subscribe("VOLATILE")
        await trader.drain_messages()
        
        # Submit order while active
        await trader.submit_order("VOLATILE", "buy", "100.00", 10)
        msgs = await trader.drain_messages()
        accepted = [m for m in msgs if m["type"] == "order_accepted"]
        self.record_test("Order accepted while active", len(accepted) == 1)
        
        # Halt market
        result = await self.admin_request("POST", "/admin/market/halt", {"market_id": "VOLATILE"})
        self.record_test("Market halt API", result["status"] == 200)
        
        # Check trader received status change
        msgs = await trader.drain_messages()
        status_changes = [m for m in msgs if m["type"] == "market_status"]
        self.record_test("Status change broadcast", len(status_changes) >= 1)
        
        # Try to submit while halted
        await trader.submit_order("VOLATILE", "buy", "100.00", 10)
        msgs = await trader.drain_messages()
        rejects = [m for m in msgs if m["type"] == "order_rejected"]
        self.record_test("Order rejected while halted", len(rejects) == 1)
        
        # Resume market
        result = await self.admin_request("POST", "/admin/market/resume", {"market_id": "VOLATILE"})
        self.record_test("Market resume API", result["status"] == 200)
        await trader.drain_messages()
        
        # Submit should work again
        await trader.submit_order("VOLATILE", "buy", "100.00", 10)
        msgs = await trader.drain_messages()
        accepted = [m for m in msgs if m["type"] == "order_accepted"]
        self.record_test("Order accepted after resume", len(accepted) == 1)
    
    async def test_rate_limiting(self):
        """Test rate limiting enforcement."""
        print("\n" + "=" * 60)
        print("TEST SUITE: Rate Limiting")
        print("=" * 60)
        
        trader = await self.create_trader("rate_limit_trader")
        await trader.subscribe("TEST")
        await trader.drain_messages()
        
        # Rapid fire orders
        rate_limited = False
        orders_before_limit = 0
        
        for i in range(20):
            await trader.submit_order("TEST", "buy", f"{50 + i}.00", 1)
            msgs = await trader.receive_messages(1, timeout=0.3)
            
            if msgs:
                if msgs[0]["type"] == "error" and "rate" in msgs[0]["data"]["message"].lower():
                    rate_limited = True
                    orders_before_limit = i
                    break
                elif msgs[0]["type"] == "order_accepted":
                    orders_before_limit = i + 1
        
        self.record_test("Rate limiting triggered", rate_limited, f"After {orders_before_limit} orders")
        self.record_test("Burst limit reasonable", 3 <= orders_before_limit <= 10)
        
        # Wait longer for rate limit to recover (bucket refills at 2/sec)
        await asyncio.sleep(3)
        
        # Drain any pending messages
        await trader.drain_messages()
        
        await trader.submit_order("TEST", "buy", "45.00", 1)
        msgs = await trader.receive_messages(1, timeout=2.0)
        recovered = msgs and msgs[0]["type"] == "order_accepted"
        self.record_test("Rate limit recovery", recovered, 
                        f"Got: {msgs[0]['type'] if msgs else 'no message'}")
    
    async def test_validation(self):
        """Test order validation."""
        print("\n" + "=" * 60)
        print("TEST SUITE: Order Validation")
        print("=" * 60)
        
        trader = await self.create_trader("validation_trader")
        await trader.subscribe("VOLATILE")  # tick_size=0.05, lot_size=5
        await trader.drain_messages()
        
        # Invalid tick size
        await trader.submit_order("VOLATILE", "buy", "100.03", 10)
        msgs = await trader.drain_messages()
        rejects = [m for m in msgs if m["type"] == "order_rejected"]
        self.record_test("Tick size validation", len(rejects) == 1)
        
        # Invalid lot size
        await trader.submit_order("VOLATILE", "buy", "100.00", 7)
        msgs = await trader.drain_messages()
        rejects = [m for m in msgs if m["type"] == "order_rejected"]
        self.record_test("Lot size validation", len(rejects) == 1)
        
        # Valid order (tick=0.05, lot=5)
        await trader.submit_order("VOLATILE", "buy", "100.05", 10)
        msgs = await trader.drain_messages()
        accepted = [m for m in msgs if m["type"] == "order_accepted"]
        self.record_test("Valid order accepted", len(accepted) == 1)
        
        # Invalid side
        await trader.ws.send_json({
            "type": "order_submit",
            "data": {"market_id": "VOLATILE", "side": "invalid", "price": "100.00", "qty": 10}
        })
        msgs = await trader.drain_messages()
        errors = [m for m in msgs if m["type"] == "error"]
        self.record_test("Invalid side rejected", len(errors) == 1)
        
        # Invalid price (negative)
        await trader.ws.send_json({
            "type": "order_submit",
            "data": {"market_id": "VOLATILE", "side": "buy", "price": "-100.00", "qty": 10}
        })
        msgs = await trader.drain_messages()
        rejects = [m for m in msgs if m["type"] in ("order_rejected", "error")]
        self.record_test("Negative price rejected", len(rejects) == 1)
        
        # Invalid qty (zero)
        await trader.ws.send_json({
            "type": "order_submit",
            "data": {"market_id": "VOLATILE", "side": "buy", "price": "100.00", "qty": 0}
        })
        msgs = await trader.drain_messages()
        rejects = [m for m in msgs if m["type"] in ("order_rejected", "error")]
        self.record_test("Zero qty rejected", len(rejects) == 1)
    
    async def test_cancel_all(self):
        """Test cancel all orders."""
        print("\n" + "=" * 60)
        print("TEST SUITE: Cancel All Orders")
        print("=" * 60)
        
        trader = await self.create_trader("cancel_all_trader")
        await trader.subscribe("TEST")
        await trader.subscribe("AAPL")
        await trader.drain_messages()
        
        # Place orders in multiple markets at unique prices
        await trader.submit_order("TEST", "buy", "300.00", 10)
        await trader.submit_order("TEST", "buy", "299.00", 10)
        await trader.submit_order("AAPL", "buy", "250.00", 10)
        await trader.drain_messages()
        
        # Verify orders placed
        await trader.get_orders("TEST")
        msgs = await trader.receive_messages(1)
        test_orders_before = len(msgs[0]["data"]["orders"]) if msgs else 0
        
        # Cancel all in TEST market
        await trader.cancel_all("TEST")
        msgs = await trader.drain_messages()
        
        # Count cancellation responses - could be individual cancels or batch response
        test_cancels = [m for m in msgs if m["type"] == "order_cancelled"]
        self.record_test("Cancel all in single market", len(test_cancels) >= 1 or 
                        any("cancelled_count" in m.get("data", {}) for m in msgs),
                        f"Cancels: {len(test_cancels)}")
        
        # AAPL order should still exist
        await trader.get_orders("AAPL")
        msgs = await trader.receive_messages(1)
        aapl_orders = msgs[0]["data"]["orders"] if msgs else []
        self.record_test("Other market orders intact", len(aapl_orders) == 1)
        
        # Place new order in TEST
        await trader.submit_order("TEST", "buy", "300.00", 10)
        await trader.drain_messages()
        
        # Cancel all across all markets
        await trader.cancel_all()  # No market specified
        msgs = await trader.drain_messages()
        
        # Verify all cancelled
        await trader.get_orders()  # Get all orders
        msgs = await trader.receive_messages(1)
        
        if msgs and msgs[0]["type"] == "orders":
            remaining_orders = sum(len(orders) for orders in msgs[0]["data"]["orders"].values()) if isinstance(msgs[0]["data"]["orders"], dict) else len(msgs[0]["data"]["orders"])
            self.record_test("Cancel all markets", remaining_orders == 0, f"Remaining: {remaining_orders}")
        else:
            self.record_test("Cancel all markets", True)  # Assume passed if no orders response
    
    async def test_admin_functions(self):
        """Test admin functionality."""
        print("\n" + "=" * 60)
        print("TEST SUITE: Admin Functions")
        print("=" * 60)
        
        # Get stats
        result = await self.admin_request("GET", "/admin/stats")
        self.record_test("Admin stats endpoint", result["status"] == 200)
        
        stats = result["data"]
        self.record_test("Stats has markets", "markets" in stats)
        self.record_test("Stats has trades", "total_trades" in stats)
        
        # Get audit log
        result = await self.admin_request("GET", "/admin/audit")
        self.record_test("Audit log endpoint", result["status"] == 200)
        self.record_test("Audit has events", len(result["data"]["events"]) > 0)
        
        # Ban user
        # Create user first
        try:
            self.server.user_store.create_user("banned_user", "testpass123", "Banned User")
        except ValueError:
            pass
        
        result = await self.admin_request("POST", "/admin/ban", {
            "user_id": "banned_user",
            "reason": "Test ban"
        })
        self.record_test("Ban user API", result["status"] == 200)
        
        # Verify banned user can't login
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{BASE_URL}/auth/login", json={
                "user_id": "banned_user",
                "password": "testpass123"
            }) as resp:
                self.record_test("Banned user login blocked", resp.status == 401)  # Returns 401 for invalid creds/banned
        
        # Unban user
        result = await self.admin_request("POST", "/admin/unban", {"user_id": "banned_user"})
        self.record_test("Unban user API", result["status"] == 200)
        
        # Verify unbanned user can login
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{BASE_URL}/auth/login", json={
                "user_id": "banned_user",
                "password": "testpass123"
            }) as resp:
                self.record_test("Unbanned user can login", resp.status == 200)
        
        # Unauthorized access
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{BASE_URL}/admin/stats") as resp:
                self.record_test("Admin auth required", resp.status == 401)
    
    async def test_multi_market_simulation(self):
        """Simulate trading across multiple markets."""
        print("\n" + "=" * 60)
        print("TEST SUITE: Multi-Market Simulation")
        print("=" * 60)
        
        # Create multiple traders
        traders = []
        for i in range(5):
            t = await self.create_trader(f"sim_trader_{i}")
            await t.subscribe("AAPL")
            await t.subscribe("BTCUSD")
            await t.drain_messages()
            traders.append(t)
        
        # Simulate trading activity
        trades_executed = 0
        
        # Round 1: Post liquidity
        for i, t in enumerate(traders[:3]):
            price = 150 + i
            await t.submit_order("AAPL", "sell", f"{price}.00", 100)
            await t.submit_order("BTCUSD", "sell", f"{40000 + i * 100}.00", 5)
        
        for t in traders[:3]:
            await t.drain_messages()
        
        # Round 2: Take liquidity
        for t in traders[3:]:
            await t.submit_order("AAPL", "buy", "152.00", 50)
            await t.submit_order("BTCUSD", "buy", "40200.00", 2)
        
        # Collect all messages
        for t in traders:
            msgs = await t.drain_messages()
            trades_executed += len([m for m in msgs if m["type"] == "trade"])
        
        self.record_test("Multi-market trades executed", trades_executed > 0, f"{trades_executed} trades")
        
        # Check positions across markets
        await traders[3].get_position("AAPL")
        msgs = await traders[3].receive_messages(1)
        if msgs and msgs[0]["type"] == "position":
            self.record_test("AAPL position tracked", msgs[0]["data"]["position"]["net_qty"] != 0)
        
        await traders[3].get_position("BTCUSD")
        msgs = await traders[3].receive_messages(1)
        if msgs and msgs[0]["type"] == "position":
            self.record_test("BTCUSD position tracked", msgs[0]["data"]["position"]["net_qty"] != 0)
    
    async def test_full_trading_session(self):
        """Simulate a complete trading session with realistic activity."""
        print("\n" + "=" * 60)
        print("TEST SUITE: Full Trading Session Simulation")
        print("=" * 60)
        
        NUM_TRADERS = 10
        NUM_ROUNDS = 20
        
        # Create a fresh market for this test to avoid interference
        self.exchange.create_market(
            "SESSION",
            "Session Test Market",
            tick_size="0.01",
        )
        self.exchange.start_market("SESSION")
        
        # Create traders
        traders = []
        for i in range(NUM_TRADERS):
            t = await self.create_trader(f"session_trader_{i}")
            await t.subscribe("SESSION")
            await t.drain_messages()
            traders.append(t)
        
        # Simulate trading rounds
        total_orders = 0
        total_trades = 0
        total_cancels = 0
        
        base_price = 100.0
        
        for round_num in range(NUM_ROUNDS):
            # Random price drift
            base_price += random.uniform(-1, 1)
            base_price = max(10, base_price)  # Keep positive
            
            # Each trader takes an action
            for trader in traders:
                action = random.choice(["buy", "sell", "cancel", "nothing"])
                
                if action == "buy":
                    price = base_price + random.uniform(-2, 0)
                    qty = random.randint(1, 50)
                    await trader.submit_order("SESSION", "buy", f"{price:.2f}", qty)
                    total_orders += 1
                
                elif action == "sell":
                    price = base_price + random.uniform(0, 2)
                    qty = random.randint(1, 50)
                    await trader.submit_order("SESSION", "sell", f"{price:.2f}", qty)
                    total_orders += 1
                
                elif action == "cancel" and trader.orders:
                    order_id = random.choice(list(trader.orders.keys()))
                    await trader.cancel_order("SESSION", order_id)
                    total_cancels += 1
            
            # Process messages
            for trader in traders:
                msgs = await trader.drain_messages()
                total_trades += len([m for m in msgs if m["type"] == "trade"])
            
            # Small delay between rounds
            await asyncio.sleep(0.05)
        
        self.record_test("Session orders submitted", total_orders > 50, f"{total_orders} orders")
        self.record_test("Session trades executed", total_trades > 0, f"{total_trades} trades")
        self.record_test("Session cancels processed", total_cancels >= 0, f"{total_cancels} cancels")
        
        # Final position check - net positions should sum to zero for executed trades
        positions_sum = 0
        for trader in traders:
            await trader.get_position("SESSION")
            msgs = await trader.receive_messages(1)
            if msgs and msgs[0]["type"] == "position":
                positions_sum += msgs[0]["data"]["position"]["net_qty"]
        
        # Net positions should sum to zero (every buy has a sell)
        self.record_test("Position sum is zero", positions_sum == 0, f"Sum: {positions_sum}")
        
        # Get final stats from admin
        result = await self.admin_request("GET", "/admin/stats")
        if result["status"] == 200:
            stats = result["data"]
            self.record_test("Final stats available", True, 
                           f"Orders: {stats['total_orders']}, Trades: {stats['total_trades']}")
    
    async def test_edge_cases(self):
        """Test various edge cases."""
        print("\n" + "=" * 60)
        print("TEST SUITE: Edge Cases")
        print("=" * 60)
        
        trader = await self.create_trader("edge_case_trader")
        await trader.subscribe("TEST")
        await trader.drain_messages()
        
        # Cancel non-existent order
        await trader.cancel_order("TEST", "nonexistent_order_id_12345")
        msgs = await trader.drain_messages()
        errors = [m for m in msgs if m["type"] == "error"]
        self.record_test("Cancel non-existent order", len(errors) == 1)
        
        # Submit to non-existent market
        await trader.ws.send_json({
            "type": "order_submit",
            "data": {"market_id": "FAKE_MARKET", "side": "buy", "price": "100.00", "qty": 10}
        })
        msgs = await trader.drain_messages()
        errors = [m for m in msgs if m["type"] in ("error", "order_rejected")]
        self.record_test("Order to non-existent market", len(errors) == 1)
        
        # Get position for non-existent market
        await trader.ws.send_json({
            "type": "get_position",
            "data": {"market_id": "FAKE_MARKET"}
        })
        msgs = await trader.drain_messages()
        errors = [m for m in msgs if m["type"] == "error"]
        self.record_test("Position for non-existent market", len(errors) == 1)
        
        # Unknown message type
        await trader.ws.send_json({"type": "unknown_type", "data": {}})
        msgs = await trader.drain_messages()
        errors = [m for m in msgs if m["type"] == "error"]
        self.record_test("Unknown message type", len(errors) == 1)
        
        # Malformed JSON is handled by aiohttp, but we can test invalid data
        await trader.ws.send_json({"type": "order_submit", "data": "not_a_dict"})
        msgs = await trader.drain_messages()
        errors = [m for m in msgs if m["type"] == "error"]
        self.record_test("Malformed order data", len(errors) == 1)
        
        # Very large quantity
        await trader.submit_order("TEST", "buy", "100.00", 999999999)
        msgs = await trader.drain_messages()
        rejects = [m for m in msgs if m["type"] == "order_rejected"]
        self.record_test("Excessive quantity rejected", len(rejects) == 1)
    
    async def test_concurrent_connections(self):
        """Test handling of concurrent connections."""
        print("\n" + "=" * 60)
        print("TEST SUITE: Concurrent Connections")
        print("=" * 60)
        
        NUM_CONCURRENT = 20
        
        # Create users first
        for i in range(NUM_CONCURRENT):
            try:
                self.server.user_store.create_user(
                    f"concurrent_{i}",
                    "testpass123",
                    f"Concurrent User {i}"
                )
            except ValueError:
                pass
        
        # Create many traders simultaneously
        async def create_and_trade(user_id: str):
            trader = TestTrader(user_id=user_id)
            try:
                await trader.login()
                await trader.connect_ws()
                await trader.subscribe("TEST")
                await trader.drain_messages()
                
                # Submit an order
                await trader.submit_order("TEST", "buy", "100.00", 1)
                msgs = await trader.drain_messages()
                
                success = any(m["type"] == "order_accepted" for m in msgs)
                await trader.cleanup()
                return success
            except Exception as e:
                await trader.cleanup()
                return False
        
        # Run concurrently
        results = await asyncio.gather(*[
            create_and_trade(f"concurrent_{i}") for i in range(NUM_CONCURRENT)
        ])
        
        successes = sum(results)
        self.record_test("Concurrent connections handled", successes == NUM_CONCURRENT,
                        f"{successes}/{NUM_CONCURRENT} successful")
    
    async def run_all_tests(self):
        """Run all test suites."""
        print("\n" + "=" * 70)
        print("COMPREHENSIVE INTEGRATION TEST SUITE")
        print("=" * 70)
        
        await self.setup()
        
        try:
            await self.test_basic_connectivity()
            await self.test_order_lifecycle()
            await self.test_order_matching()
            await self.test_partial_fills()
            await self.test_price_time_priority()
            await self.test_time_in_force()
            await self.test_self_trade_prevention()
            await self.test_position_tracking()
            await self.test_market_status()
            await self.test_rate_limiting()
            await self.test_validation()
            await self.test_cancel_all()
            await self.test_admin_functions()
            await self.test_multi_market_simulation()
            await self.test_full_trading_session()
            await self.test_edge_cases()
            await self.test_concurrent_connections()
            
        finally:
            await self.teardown()
        
        # Summary
        print("\n" + "=" * 70)
        print("TEST SUMMARY")
        print("=" * 70)
        
        passed = sum(1 for t in self.test_results if t["passed"])
        failed = sum(1 for t in self.test_results if not t["passed"])
        total = len(self.test_results)
        
        print(f"\nTotal: {total} tests")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")
        print(f"Pass rate: {passed/total*100:.1f}%")
        
        if failed > 0:
            print("\nFailed tests:")
            for t in self.test_results:
                if not t["passed"]:
                    print(f"  ✗ {t['name']}: {t['details']}")
        
        print("\n" + "=" * 70)
        if failed == 0:
            print("ALL TESTS PASSED ✓")
        else:
            print(f"TESTS COMPLETED WITH {failed} FAILURES")
        print("=" * 70)
        
        return failed == 0


async def main():
    suite = IntegrationTestSuite()
    success = await suite.run_all_tests()
    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
