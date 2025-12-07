# Trading Exchange API Documentation

## Overview

This document describes the WebSocket API for connecting a frontend to the trading exchange. The API provides:

- **Level 2 Order Book Data**: Aggregated quantity at each price level
- **Real-time Updates**: Push notifications when book changes
- **Rate-Limited Queries**: Separate rate limits for orders vs book requests

## Connection Flow

```
1. POST /auth/login         -> Get session cookie
2. GET /ws                  -> Establish WebSocket connection
3. subscribe to market(s)   -> Receive initial book snapshot
4. Receive real-time events -> BookDelta, trades, fills
```

## Authentication

### Login
```http
POST /auth/login
Content-Type: application/json

{
    "user_id": "player123"
}
```

Response:
```json
{
    "session_id": "...",
    "user_id": "player123",
    "expires_at": "2024-01-01T12:00:00Z"
}
```

Session is stored in a secure HTTP-only cookie.

## WebSocket Connection

Connect to `/ws` after login. The session cookie authenticates the connection.

## Message Format

All messages are JSON with this structure:

**Client -> Server:**
```json
{
    "type": "message_type",
    "data": { ... },
    "request_id": "optional-correlation-id"
}
```

**Server -> Client:**
```json
{
    "type": "message_type",
    "data": { ... },
    "request_id": "if-provided"
}
```

## Client Messages

### subscribe
Subscribe to a market's events (book updates, trades).

```json
{
    "type": "subscribe",
    "data": { "market_id": "AAPL" }
}
```

Response: `subscribed` + `book_snapshot` + `orders` + `position`

### unsubscribe
```json
{
    "type": "unsubscribe",
    "data": { "market_id": "AAPL" }
}
```

### order_submit
Submit a new order.

```json
{
    "type": "order_submit",
    "data": {
        "market_id": "AAPL",
        "side": "buy",           // "buy" or "sell"
        "price": "150.25",       // String decimal
        "qty": 100,              // Integer
        "time_in_force": "gtc",  // Optional: "gtc", "ioc", "fok"
        "client_order_id": "my-id-123"  // Optional correlation ID
    },
    "request_id": "req-001"
}
```

Responses: `order_accepted` or `order_rejected`

**Rate Limits:** 2 orders/second, 30/minute

### order_cancel
```json
{
    "type": "order_cancel",
    "data": {
        "market_id": "AAPL",
        "order_id": "ord_abc123"
    }
}
```

### order_cancel_all
Cancel all orders in a market (or all markets if no market_id).

```json
{
    "type": "order_cancel_all",
    "data": {
        "market_id": "AAPL"  // Optional
    }
}
```

### get_book
Get current order book snapshot. **Rate limited.**

```json
{
    "type": "get_book",
    "data": {
        "market_id": "AAPL",
        "depth": 20  // Optional, default 50
    }
}
```

**Rate Limits:** 10 requests/second with burst of 20

### get_level2
Get aggregated Level 2 data (price + quantity only).

```json
{
    "type": "get_level2",
    "data": {
        "market_id": "AAPL",
        "depth": 10
    }
}
```

### get_bbo
Get best bid/offer only (Level 1 data).

```json
{
    "type": "get_bbo",
    "data": { "market_id": "AAPL" }
}
```

### get_orders
Get user's open orders.

```json
{
    "type": "get_orders",
    "data": {
        "market_id": "AAPL"  // Optional, omit for all markets
    }
}
```

### get_position
Get user's position.

```json
{
    "type": "get_position",
    "data": {
        "market_id": "AAPL"  // Optional, omit for all markets
    }
}
```

### ping
Keep-alive message.

```json
{ "type": "ping" }
```

Response: `{ "type": "pong" }`

## Server Messages (Events)

### book_snapshot
Full order book state. Sent on subscribe and after get_book requests.

```json
{
    "type": "book_snapshot",
    "data": {
        "market_id": "AAPL",
        "bids": [
            { "price": "150.00", "qty": 500, "order_count": 3 },
            { "price": "149.99", "qty": 200, "order_count": 1 }
        ],
        "asks": [
            { "price": "150.01", "qty": 300, "order_count": 2 },
            { "price": "150.02", "qty": 100, "order_count": 1 }
        ],
        "best_bid": "150.00",
        "best_ask": "150.01",
        "spread": "0.01",
        "mid_price": "150.005",
        "sequence": 12345,
        "timestamp": "2024-01-01T12:00:00.123Z"
    }
}
```

**Frontend Usage:** Replace entire local book with this data.

### book_delta
Incremental book update. Sent when orders are added/removed/modified.

```json
{
    "type": "book_delta",
    "data": {
        "market_id": "AAPL",
        "changes": [
            {
                "action": "update",  // "add", "remove", "update"
                "side": "buy",
                "price": "150.00",
                "qty": 600,          // New total qty at this level
                "order_count": 4
            },
            {
                "action": "remove",
                "side": "sell",
                "price": "150.05",
                "qty": 0,
                "order_count": 0
            }
        ],
        "best_bid": "150.00",
        "best_ask": "150.01",
        "sequence": 12346,
        "timestamp": "2024-01-01T12:00:00.456Z"
    }
}
```

**Frontend Usage:** Apply changes to local book:
- `add`: Add new price level
- `update`: Update existing level's qty and order_count
- `remove`: Remove price level entirely

### trade
Broadcast when a trade executes.

```json
{
    "type": "trade",
    "data": {
        "id": "trd_xyz789",
        "market_id": "AAPL",
        "price": "150.00",
        "qty": 100,
        "aggressor_side": "buy",
        "sequence": 12347,
        "created_at": "2024-01-01T12:00:00.789Z"
    }
}
```

### order_accepted
Sent to order submitter when order is accepted.

```json
{
    "type": "order_accepted",
    "data": {
        "order_id": "ord_abc123",
        "market_id": "AAPL",
        "user_id": "player123",
        "side": "buy",
        "price": "150.00",
        "qty": 100,
        "time_in_force": "gtc",
        "client_order_id": "my-id-123",
        "created_at": "2024-01-01T12:00:00Z"
    }
}
```

### order_rejected
```json
{
    "type": "order_rejected",
    "data": {
        "market_id": "AAPL",
        "reason": "tick_size_violation",
        "message": "Price must be multiple of 0.01"
    }
}
```

### order_filled
Sent when your order gets filled (partial or complete).

```json
{
    "type": "order_filled",
    "data": {
        "order_id": "ord_abc123",
        "market_id": "AAPL",
        "trade_id": "trd_xyz789",
        "fill_price": "150.00",
        "fill_qty": 50,
        "remaining_qty": 50,
        "is_complete": false
    }
}
```

### order_cancelled
```json
{
    "type": "order_cancelled",
    "data": {
        "order_id": "ord_abc123",
        "market_id": "AAPL",
        "reason": "user_requested"
    }
}
```

### order_expired
Sent for IOC/FOK orders that expire unfilled.

```json
{
    "type": "order_expired",
    "data": {
        "order_id": "ord_abc123",
        "reason": "ioc_unfilled",
        "expired_qty": 50,
        "filled_qty": 50
    }
}
```

### position
User's position in a market.

```json
{
    "type": "position",
    "data": {
        "market_id": "AAPL",
        "position": {
            "net_qty": 100,
            "total_bought": 200,
            "total_sold": 100,
            "avg_buy_price": "150.00",
            "avg_sell_price": "151.00",
            "realized_pnl": "100.00",
            "trade_count": 3
        }
    }
}
```

### market_status
Market status change notification.

```json
{
    "type": "market_status",
    "data": {
        "market_id": "AAPL",
        "status": "active",  // "pending", "active", "halted", "closed"
        "previous_status": "pending"
    }
}
```

### error
```json
{
    "type": "error",
    "data": {
        "code": "rate_limited",
        "message": "Book request rate limit exceeded"
    },
    "request_id": "req-001"
}
```

## Frontend Implementation Guide

### Recommended Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    React Frontend                         │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  ┌─────────────────┐    ┌───────────────────────────┐   │
│  │  WebSocket      │───▶│  Order Book Store         │   │
│  │  Connection     │    │  (Local State)            │   │
│  │                 │    │                           │   │
│  │  - Receives     │    │  - Apply book_snapshot    │   │
│  │    events       │    │  - Apply book_delta       │   │
│  │  - Sends        │    │  - Computed: sorted       │   │
│  │    orders       │    │    bids/asks              │   │
│  └─────────────────┘    └───────────────────────────┘   │
│           │                        │                     │
│           ▼                        ▼                     │
│  ┌─────────────────┐    ┌───────────────────────────┐   │
│  │  Order Manager  │    │  Order Book Display       │   │
│  │                 │    │  (Level 2 Ladder)         │   │
│  │  - Submit order │    │                           │   │
│  │  - Cancel order │    │  Price | Bid Qty | Ask Qty│   │
│  │  - Track fills  │    │  150.05|        | 100     │   │
│  └─────────────────┘    │  150.04|        | 300     │   │
│                          │  150.03|        | 200     │   │
│                          │  ------ SPREAD ------    │   │
│                          │  150.00| 500    |        │   │
│                          │  149.99| 200    |        │   │
│                          └───────────────────────────┘   │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### Order Book State Management

```typescript
interface PriceLevel {
  price: string;
  qty: number;
  orderCount: number;
}

interface OrderBook {
  marketId: string;
  bids: Map<string, PriceLevel>;  // price -> level
  asks: Map<string, PriceLevel>;
  sequence: number;
}

// Apply full snapshot
function applySnapshot(book: OrderBook, snapshot: BookSnapshot) {
  book.bids.clear();
  book.asks.clear();
  
  for (const level of snapshot.bids) {
    book.bids.set(level.price, level);
  }
  for (const level of snapshot.asks) {
    book.asks.set(level.price, level);
  }
  book.sequence = snapshot.sequence;
}

// Apply incremental delta
function applyDelta(book: OrderBook, delta: BookDelta) {
  // Ignore stale updates
  if (delta.sequence <= book.sequence) return;
  
  for (const change of delta.changes) {
    const levels = change.side === 'buy' ? book.bids : book.asks;
    
    if (change.action === 'remove' || change.qty === 0) {
      levels.delete(change.price);
    } else {
      levels.set(change.price, {
        price: change.price,
        qty: change.qty,
        orderCount: change.order_count,
      });
    }
  }
  book.sequence = delta.sequence;
}
```

### Important Notes

1. **Don't Poll**: Subscribe and listen for `book_delta` events rather than polling `get_book`
2. **Sequence Numbers**: Track sequence to detect missed updates
3. **Reconnection**: On reconnect, re-subscribe to get fresh snapshot
4. **Rate Limits**: Book queries are rate-limited separately from order submission
5. **Decimal Precision**: All prices are strings to preserve decimal precision

## Rate Limits Summary

| Action | Limit | Burst |
|--------|-------|-------|
| Order Submit | 2/sec, 30/min | 5 |
| Order Cancel | 2/sec, 30/min | 5 |
| Get Book | 10/sec | 20 |
| Get BBO | 10/sec | 20 (costs 0.5) |
| Subscribe | No limit | - |

## Error Codes

| Code | Description |
|------|-------------|
| `rate_limited` | Too many requests |
| `invalid_market` | Market not found |
| `invalid_side` | Side must be 'buy' or 'sell' |
| `invalid_price` | Price must be positive |
| `invalid_qty` | Quantity must be positive integer |
| `tick_size_violation` | Price not on tick grid |
| `lot_size_violation` | Quantity not multiple of lot size |
| `self_trade_prevented` | Would trade with own order |
| `market_halted` | Market is not active |
| `order_not_found` | Order doesn't exist |
| `unauthorized` | Not logged in |
