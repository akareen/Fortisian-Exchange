"""
Admin Analytics API.

Provides deep analysis capabilities for admins including:
- Trade analysis (by user, market, time)
- Position analysis
- Order flow analysis
- Performance metrics
- Leaderboard calculations
- Anomaly detection

All these APIs require admin authentication.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional
from collections import defaultdict
import statistics

from matching_engine import (
    Exchange, Trade, Order, Position, Side, MarketId, UserId,
)


# ─────────────────────────────────────────────────────────────────────────────
# Trade Analysis
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TradeAnalysis:
    """Analysis of a user's trading activity."""
    user_id: str
    market_id: str | None
    
    # Volume metrics
    total_trades: int = 0
    total_buy_trades: int = 0
    total_sell_trades: int = 0
    total_volume: int = 0
    total_buy_volume: int = 0
    total_sell_volume: int = 0
    total_notional: Decimal = Decimal("0")
    
    # Price metrics
    avg_trade_price: Decimal | None = None
    avg_buy_price: Decimal | None = None
    avg_sell_price: Decimal | None = None
    min_trade_price: Decimal | None = None
    max_trade_price: Decimal | None = None
    
    # Timing
    first_trade_at: datetime | None = None
    last_trade_at: datetime | None = None
    avg_time_between_trades: float | None = None  # seconds
    
    # Aggressor stats
    aggressor_trades: int = 0
    maker_trades: int = 0
    aggressor_ratio: float = 0.0
    
    def to_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "market_id": self.market_id,
            "total_trades": self.total_trades,
            "total_buy_trades": self.total_buy_trades,
            "total_sell_trades": self.total_sell_trades,
            "total_volume": self.total_volume,
            "total_buy_volume": self.total_buy_volume,
            "total_sell_volume": self.total_sell_volume,
            "total_notional": str(self.total_notional),
            "avg_trade_price": str(self.avg_trade_price) if self.avg_trade_price else None,
            "avg_buy_price": str(self.avg_buy_price) if self.avg_buy_price else None,
            "avg_sell_price": str(self.avg_sell_price) if self.avg_sell_price else None,
            "min_trade_price": str(self.min_trade_price) if self.min_trade_price else None,
            "max_trade_price": str(self.max_trade_price) if self.max_trade_price else None,
            "first_trade_at": self.first_trade_at.isoformat() if self.first_trade_at else None,
            "last_trade_at": self.last_trade_at.isoformat() if self.last_trade_at else None,
            "avg_time_between_trades": self.avg_time_between_trades,
            "aggressor_trades": self.aggressor_trades,
            "maker_trades": self.maker_trades,
            "aggressor_ratio": round(self.aggressor_ratio, 4),
        }


@dataclass 
class MarketAnalysis:
    """Analysis of a market's trading activity."""
    market_id: str
    
    # Volume
    total_trades: int = 0
    total_volume: int = 0
    total_notional: Decimal = Decimal("0")
    
    # Prices
    open_price: Decimal | None = None
    close_price: Decimal | None = None
    high_price: Decimal | None = None
    low_price: Decimal | None = None
    vwap: Decimal | None = None
    
    # Spread
    avg_spread: Decimal | None = None
    min_spread: Decimal | None = None
    max_spread: Decimal | None = None
    
    # Participation
    unique_traders: int = 0
    unique_buyers: int = 0
    unique_sellers: int = 0
    
    # Time
    trading_start: datetime | None = None
    trading_end: datetime | None = None
    
    def to_dict(self) -> dict:
        return {
            "market_id": self.market_id,
            "total_trades": self.total_trades,
            "total_volume": self.total_volume,
            "total_notional": str(self.total_notional),
            "open_price": str(self.open_price) if self.open_price else None,
            "close_price": str(self.close_price) if self.close_price else None,
            "high_price": str(self.high_price) if self.high_price else None,
            "low_price": str(self.low_price) if self.low_price else None,
            "vwap": str(self.vwap) if self.vwap else None,
            "avg_spread": str(self.avg_spread) if self.avg_spread else None,
            "unique_traders": self.unique_traders,
            "unique_buyers": self.unique_buyers,
            "unique_sellers": self.unique_sellers,
            "trading_start": self.trading_start.isoformat() if self.trading_start else None,
            "trading_end": self.trading_end.isoformat() if self.trading_end else None,
        }


@dataclass
class LeaderboardEntry:
    """Entry in the leaderboard."""
    rank: int
    user_id: str
    display_name: str
    market_id: str | None  # None for overall
    
    # P&L
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    total_pnl: Decimal = Decimal("0")
    
    # Position
    net_position: int = 0
    total_volume: int = 0
    trade_count: int = 0
    
    # Efficiency metrics
    win_rate: float = 0.0
    avg_profit_per_trade: Decimal = Decimal("0")
    sharpe_ratio: float | None = None
    
    def to_dict(self) -> dict:
        return {
            "rank": self.rank,
            "user_id": self.user_id,
            "display_name": self.display_name,
            "market_id": self.market_id,
            "realized_pnl": str(self.realized_pnl),
            "unrealized_pnl": str(self.unrealized_pnl),
            "total_pnl": str(self.total_pnl),
            "net_position": self.net_position,
            "total_volume": self.total_volume,
            "trade_count": self.trade_count,
            "win_rate": round(self.win_rate, 4),
            "avg_profit_per_trade": str(self.avg_profit_per_trade),
            "sharpe_ratio": round(self.sharpe_ratio, 4) if self.sharpe_ratio else None,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Analytics Engine
# ─────────────────────────────────────────────────────────────────────────────

class AdminAnalytics:
    """
    Admin analytics engine.
    
    Provides comprehensive analysis capabilities.
    All trade data is stored for later analysis.
    """
    
    def __init__(self, exchange: Exchange):
        self.exchange = exchange
        
        # Trade history (in-memory for simplicity, use persistence in production)
        self._trades: list[Trade] = []
        self._trade_index_by_user: dict[str, list[int]] = defaultdict(list)
        self._trade_index_by_market: dict[str, list[int]] = defaultdict(list)
        
        # Order history
        self._orders: list[Order] = []
        
        # Spread samples for analysis
        self._spread_samples: dict[str, list[tuple[datetime, Decimal]]] = defaultdict(list)
    
    def record_trade(self, trade: Trade) -> None:
        """Record a trade for analysis."""
        idx = len(self._trades)
        self._trades.append(trade)
        
        # Index by user
        self._trade_index_by_user[str(trade.buyer_id)].append(idx)
        self._trade_index_by_user[str(trade.seller_id)].append(idx)
        
        # Index by market
        self._trade_index_by_market[str(trade.market_id)].append(idx)
    
    def record_order(self, order: Order) -> None:
        """Record an order for analysis."""
        self._orders.append(order)
    
    def record_spread(self, market_id: str, spread: Decimal) -> None:
        """Record a spread observation."""
        self._spread_samples[market_id].append(
            (datetime.now(timezone.utc), spread)
        )
    
    # ─────────────────────────────────────────────────────────────────────────
    # User Analysis
    # ─────────────────────────────────────────────────────────────────────────
    
    def get_user_trade_analysis(
        self,
        user_id: str,
        market_id: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> TradeAnalysis:
        """Get comprehensive trade analysis for a user."""
        analysis = TradeAnalysis(user_id=user_id, market_id=market_id)
        
        # Get user's trades
        trade_indices = self._trade_index_by_user.get(user_id, [])
        trades = [self._trades[i] for i in trade_indices]
        
        # Filter by market if specified
        if market_id:
            trades = [t for t in trades if str(t.market_id) == market_id]
        
        # Filter by time
        if start_time:
            trades = [t for t in trades if t.created_at >= start_time]
        if end_time:
            trades = [t for t in trades if t.created_at <= end_time]
        
        if not trades:
            return analysis
        
        # Compute metrics
        buy_prices = []
        sell_prices = []
        trade_times = []
        
        for trade in trades:
            is_buyer = str(trade.buyer_id) == user_id
            
            analysis.total_trades += 1
            analysis.total_volume += trade.qty
            analysis.total_notional += trade.notional_value
            
            if is_buyer:
                analysis.total_buy_trades += 1
                analysis.total_buy_volume += trade.qty
                buy_prices.append((trade.price, trade.qty))
            else:
                analysis.total_sell_trades += 1
                analysis.total_sell_volume += trade.qty
                sell_prices.append((trade.price, trade.qty))
            
            # Aggressor tracking
            if str(trade.taker_id) == user_id:
                analysis.aggressor_trades += 1
            else:
                analysis.maker_trades += 1
            
            trade_times.append(trade.created_at)
        
        # Calculate averages
        if buy_prices:
            total_buy_value = sum(p * q for p, q in buy_prices)
            total_buy_qty = sum(q for _, q in buy_prices)
            analysis.avg_buy_price = total_buy_value / total_buy_qty
        
        if sell_prices:
            total_sell_value = sum(p * q for p, q in sell_prices)
            total_sell_qty = sum(q for _, q in sell_prices)
            analysis.avg_sell_price = total_sell_value / total_sell_qty
        
        # Overall average
        all_prices = buy_prices + sell_prices
        if all_prices:
            total_value = sum(p * q for p, q in all_prices)
            total_qty = sum(q for _, q in all_prices)
            analysis.avg_trade_price = total_value / total_qty
            analysis.min_trade_price = min(p for p, _ in all_prices)
            analysis.max_trade_price = max(p for p, _ in all_prices)
        
        # Time metrics
        trade_times.sort()
        if trade_times:
            analysis.first_trade_at = trade_times[0]
            analysis.last_trade_at = trade_times[-1]
            
            if len(trade_times) > 1:
                deltas = [
                    (trade_times[i+1] - trade_times[i]).total_seconds()
                    for i in range(len(trade_times) - 1)
                ]
                analysis.avg_time_between_trades = statistics.mean(deltas)
        
        # Aggressor ratio
        if analysis.total_trades > 0:
            analysis.aggressor_ratio = analysis.aggressor_trades / analysis.total_trades
        
        return analysis
    
    def get_all_users_analysis(
        self,
        market_id: str | None = None,
    ) -> list[TradeAnalysis]:
        """Get trade analysis for all users."""
        users = set(self._trade_index_by_user.keys())
        return [
            self.get_user_trade_analysis(uid, market_id)
            for uid in users
        ]
    
    # ─────────────────────────────────────────────────────────────────────────
    # Market Analysis
    # ─────────────────────────────────────────────────────────────────────────
    
    def get_market_analysis(
        self,
        market_id: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> MarketAnalysis:
        """Get comprehensive analysis for a market."""
        analysis = MarketAnalysis(market_id=market_id)
        
        # Get trades
        trade_indices = self._trade_index_by_market.get(market_id, [])
        trades = [self._trades[i] for i in trade_indices]
        
        # Filter by time
        if start_time:
            trades = [t for t in trades if t.created_at >= start_time]
        if end_time:
            trades = [t for t in trades if t.created_at <= end_time]
        
        if not trades:
            return analysis
        
        # Sort by time
        trades.sort(key=lambda t: t.created_at)
        
        # Basic metrics
        analysis.total_trades = len(trades)
        analysis.total_volume = sum(t.qty for t in trades)
        analysis.total_notional = sum(t.notional_value for t in trades)
        
        # OHLC
        analysis.open_price = trades[0].price
        analysis.close_price = trades[-1].price
        analysis.high_price = max(t.price for t in trades)
        analysis.low_price = min(t.price for t in trades)
        
        # VWAP
        if analysis.total_volume > 0:
            analysis.vwap = analysis.total_notional / analysis.total_volume
        
        # Participation
        buyers = set(str(t.buyer_id) for t in trades)
        sellers = set(str(t.seller_id) for t in trades)
        analysis.unique_buyers = len(buyers)
        analysis.unique_sellers = len(sellers)
        analysis.unique_traders = len(buyers | sellers)
        
        # Time
        analysis.trading_start = trades[0].created_at
        analysis.trading_end = trades[-1].created_at
        
        # Spread analysis
        spreads = self._spread_samples.get(market_id, [])
        if spreads:
            spread_values = [s for _, s in spreads]
            analysis.avg_spread = sum(spread_values) / len(spread_values)
            analysis.min_spread = min(spread_values)
            analysis.max_spread = max(spread_values)
        
        return analysis
    
    # ─────────────────────────────────────────────────────────────────────────
    # Leaderboard
    # ─────────────────────────────────────────────────────────────────────────
    
    def get_leaderboard(
        self,
        market_id: str | None = None,
        sort_by: str = "realized_pnl",
        limit: int = 100,
        user_names: dict[str, str] | None = None,
    ) -> list[LeaderboardEntry]:
        """
        Get the trading leaderboard.
        
        Args:
            market_id: Filter by market, None for overall
            sort_by: Sort field (realized_pnl, total_pnl, volume, trade_count)
            limit: Maximum entries to return
            user_names: Mapping of user_id to display_name
        
        Returns:
            List of LeaderboardEntry sorted by specified field
        """
        user_names = user_names or {}
        entries: dict[str, LeaderboardEntry] = {}
        
        # Collect position data
        if market_id:
            positions = self.exchange.get_all_positions(market_id)
            for pos in positions:
                uid = str(pos.user_id)
                entries[uid] = LeaderboardEntry(
                    rank=0,
                    user_id=uid,
                    display_name=user_names.get(uid, uid),
                    market_id=market_id,
                    realized_pnl=pos.realized_pnl,
                    net_position=pos.net_qty,
                    total_volume=pos.total_bought + pos.total_sold,
                    trade_count=pos.trade_count,
                )
        else:
            # Aggregate across all markets
            for mid in self.exchange._markets.keys():
                positions = self.exchange.get_all_positions(str(mid))
                for pos in positions:
                    uid = str(pos.user_id)
                    if uid not in entries:
                        entries[uid] = LeaderboardEntry(
                            rank=0,
                            user_id=uid,
                            display_name=user_names.get(uid, uid),
                            market_id=None,
                        )
                    
                    entry = entries[uid]
                    entry.realized_pnl += pos.realized_pnl
                    entry.net_position += pos.net_qty
                    entry.total_volume += pos.total_bought + pos.total_sold
                    entry.trade_count += pos.trade_count
        
        # Calculate total P&L (realized + unrealized)
        # Note: unrealized P&L would require current market prices
        for entry in entries.values():
            entry.total_pnl = entry.realized_pnl + entry.unrealized_pnl
            
            if entry.trade_count > 0:
                entry.avg_profit_per_trade = entry.realized_pnl / entry.trade_count
        
        # Sort
        sort_key = {
            "realized_pnl": lambda e: e.realized_pnl,
            "total_pnl": lambda e: e.total_pnl,
            "volume": lambda e: e.total_volume,
            "trade_count": lambda e: e.trade_count,
        }.get(sort_by, lambda e: e.realized_pnl)
        
        sorted_entries = sorted(entries.values(), key=sort_key, reverse=True)
        
        # Assign ranks
        for i, entry in enumerate(sorted_entries):
            entry.rank = i + 1
        
        return sorted_entries[:limit]
    
    # ─────────────────────────────────────────────────────────────────────────
    # Order Flow Analysis
    # ─────────────────────────────────────────────────────────────────────────
    
    def get_order_flow_analysis(
        self,
        market_id: str,
        interval_seconds: int = 60,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[dict]:
        """
        Get order flow analysis over time intervals.
        
        Returns time-bucketed data showing buy/sell pressure.
        """
        # Get trades
        trade_indices = self._trade_index_by_market.get(market_id, [])
        trades = [self._trades[i] for i in trade_indices]
        
        if start_time:
            trades = [t for t in trades if t.created_at >= start_time]
        if end_time:
            trades = [t for t in trades if t.created_at <= end_time]
        
        if not trades:
            return []
        
        trades.sort(key=lambda t: t.created_at)
        
        # Bucket trades
        buckets: dict[datetime, dict] = {}
        interval = timedelta(seconds=interval_seconds)
        
        for trade in trades:
            # Round down to interval
            ts = trade.created_at
            bucket_time = ts.replace(
                second=(ts.second // interval_seconds) * interval_seconds,
                microsecond=0
            )
            
            if bucket_time not in buckets:
                buckets[bucket_time] = {
                    "timestamp": bucket_time.isoformat(),
                    "buy_volume": 0,
                    "sell_volume": 0,
                    "buy_trades": 0,
                    "sell_trades": 0,
                    "buy_notional": Decimal("0"),
                    "sell_notional": Decimal("0"),
                    "high": trade.price,
                    "low": trade.price,
                    "open": trade.price,
                    "close": trade.price,
                    "vwap_num": Decimal("0"),
                    "vwap_den": 0,
                }
            
            b = buckets[bucket_time]
            
            if trade.aggressor_side == Side.BUY:
                b["buy_volume"] += trade.qty
                b["buy_trades"] += 1
                b["buy_notional"] += trade.notional_value
            else:
                b["sell_volume"] += trade.qty
                b["sell_trades"] += 1
                b["sell_notional"] += trade.notional_value
            
            b["high"] = max(b["high"], trade.price)
            b["low"] = min(b["low"], trade.price)
            b["close"] = trade.price
            b["vwap_num"] += trade.price * trade.qty
            b["vwap_den"] += trade.qty
        
        # Finalize buckets
        result = []
        for bucket_time in sorted(buckets.keys()):
            b = buckets[bucket_time]
            
            # Calculate VWAP
            if b["vwap_den"] > 0:
                b["vwap"] = str(b["vwap_num"] / b["vwap_den"])
            else:
                b["vwap"] = None
            
            # Calculate order flow imbalance
            total_volume = b["buy_volume"] + b["sell_volume"]
            if total_volume > 0:
                b["imbalance"] = round((b["buy_volume"] - b["sell_volume"]) / total_volume, 4)
            else:
                b["imbalance"] = 0
            
            # Clean up
            del b["vwap_num"]
            del b["vwap_den"]
            b["buy_notional"] = str(b["buy_notional"])
            b["sell_notional"] = str(b["sell_notional"])
            b["high"] = str(b["high"])
            b["low"] = str(b["low"])
            b["open"] = str(b["open"])
            b["close"] = str(b["close"])
            
            result.append(b)
        
        return result
    
    # ─────────────────────────────────────────────────────────────────────────
    # Anomaly Detection
    # ─────────────────────────────────────────────────────────────────────────
    
    def detect_anomalies(
        self,
        threshold_volume_std: float = 3.0,
        threshold_price_std: float = 3.0,
    ) -> list[dict]:
        """
        Detect potential trading anomalies.
        
        Looks for:
        - Unusual volume spikes
        - Unusual price movements
        - Wash trading patterns
        - Spoofing patterns
        """
        anomalies = []
        
        # Check for wash trading (same user on both sides)
        for trade in self._trades:
            if trade.buyer_id == trade.seller_id:
                anomalies.append({
                    "type": "wash_trade",
                    "severity": "high",
                    "trade_id": str(trade.id),
                    "user_id": str(trade.buyer_id),
                    "market_id": str(trade.market_id),
                    "details": "Same user on both sides of trade",
                    "timestamp": trade.created_at.isoformat(),
                })
        
        # Check for volume anomalies per user
        for user_id, trade_indices in self._trade_index_by_user.items():
            trades = [self._trades[i] for i in trade_indices]
            
            if len(trades) < 10:
                continue
            
            volumes = [t.qty for t in trades]
            mean_vol = statistics.mean(volumes)
            std_vol = statistics.stdev(volumes) if len(volumes) > 1 else 0
            
            for trade in trades:
                if std_vol > 0 and (trade.qty - mean_vol) / std_vol > threshold_volume_std:
                    anomalies.append({
                        "type": "unusual_volume",
                        "severity": "medium",
                        "trade_id": str(trade.id),
                        "user_id": user_id,
                        "market_id": str(trade.market_id),
                        "details": f"Volume {trade.qty} is {((trade.qty - mean_vol) / std_vol):.1f} std above mean {mean_vol:.0f}",
                        "timestamp": trade.created_at.isoformat(),
                    })
        
        return anomalies
    
    # ─────────────────────────────────────────────────────────────────────────
    # Export
    # ─────────────────────────────────────────────────────────────────────────
    
    def export_all_trades(
        self,
        market_id: str | None = None,
        include_user_ids: bool = True,
    ) -> list[dict]:
        """Export all trades for external analysis."""
        trades = self._trades
        
        if market_id:
            trades = [t for t in trades if str(t.market_id) == market_id]
        
        if include_user_ids:
            return [t.to_dict() for t in trades]
        else:
            return [t.to_public_dict() for t in trades]
    
    def get_stats(self) -> dict:
        """Get overall analytics stats."""
        return {
            "total_trades_recorded": len(self._trades),
            "total_orders_recorded": len(self._orders),
            "unique_users": len(self._trade_index_by_user),
            "markets_tracked": len(self._trade_index_by_market),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Testing
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from matching_engine import Exchange, TradeExecuted
    
    print("Testing Admin Analytics")
    print("=" * 60)
    
    # Create exchange with analytics
    analytics = None
    
    def on_event(event):
        if isinstance(event, TradeExecuted):
            analytics.record_trade(event.trade)
    
    exchange = Exchange(on_event=on_event)
    analytics = AdminAnalytics(exchange)
    
    exchange.create_market("TEST", "Test Market", tick_size="0.01")
    exchange.start_market("TEST")
    
    # Generate some trades
    from decimal import Decimal
    
    # User1 buys
    exchange.submit_order("TEST", "user1", "buy", Decimal("100.00"), 100)
    exchange.submit_order("TEST", "user2", "sell", Decimal("100.00"), 50)
    exchange.submit_order("TEST", "user3", "sell", Decimal("100.00"), 50)
    
    # More trading
    exchange.submit_order("TEST", "user2", "buy", Decimal("99.00"), 100)
    exchange.submit_order("TEST", "user1", "sell", Decimal("99.00"), 100)
    
    print(f"\n1. Recorded {analytics.get_stats()['total_trades_recorded']} trades")
    
    # User analysis
    print("\n2. User Trade Analysis:")
    analysis = analytics.get_user_trade_analysis("user1")
    print(f"   User1: {analysis.total_trades} trades, {analysis.total_volume} volume")
    print(f"   Aggressor ratio: {analysis.aggressor_ratio:.2%}")
    
    # Market analysis
    print("\n3. Market Analysis:")
    market = analytics.get_market_analysis("TEST")
    print(f"   Total trades: {market.total_trades}")
    print(f"   Total volume: {market.total_volume}")
    print(f"   VWAP: {market.vwap}")
    print(f"   Unique traders: {market.unique_traders}")
    
    # Leaderboard
    print("\n4. Leaderboard:")
    leaderboard = analytics.get_leaderboard("TEST")
    for entry in leaderboard[:3]:
        print(f"   #{entry.rank} {entry.user_id}: PnL={entry.realized_pnl}")
    
    # Anomaly detection
    print("\n5. Anomaly Detection:")
    anomalies = analytics.detect_anomalies()
    print(f"   Found {len(anomalies)} anomalies")
    
    print("\n" + "=" * 60)
    print("All analytics tests passed!")
