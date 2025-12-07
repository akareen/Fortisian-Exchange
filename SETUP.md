# Fortisian Exchange - Complete Setup Guide

## Overview

This guide covers everything you need to run the Fortisian trading exchange system:
- **Backend**: Python WebSocket server with matching engine
- **Frontend**: Bloomberg-style React trading interface

---

## Prerequisites

### System Requirements
- Python 3.12+ (required for type hints used)
- Node.js 18+ (optional, only if you want to modify frontend)
- Modern web browser (Chrome, Firefox, Safari, Edge)

### Optional
- MongoDB 6.0+ (for persistence - system works without it using in-memory storage)

---

## Part 1: Backend Setup

### Step 1: Get the Code

If you don't have the exchange code, create a directory and place these files:

```bash
mkdir -p ~/fortisian/exchange/matching_engine
cd ~/fortisian/exchange
```

The backend consists of these files:
```
exchange/
├── server.py              # Main WebSocket server
├── auth.py                # User authentication
├── analytics.py           # Admin analytics
├── book_cache.py          # Rate-limited book queries
├── sweep_orders.py        # Sweep order functionality
├── persistence.py         # MongoDB persistence (optional)
├── persistence_memory.py  # In-memory fallback
├── requirements.txt       # Python dependencies
└── matching_engine/
    ├── __init__.py
    ├── engine.py          # Core matching logic
    ├── events.py          # Event types
    ├── exchange.py        # Exchange wrapper
    ├── models.py          # Data models
    └── order_book.py      # Order book implementation
```

### Step 2: Install Dependencies

```bash
cd ~/fortisian/exchange

# Create virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

**requirements.txt contents:**
```
sortedcontainers>=2.4.0
motor>=3.3.0
pymongo>=4.6.0
aiohttp>=3.9.0
aiohttp-session>=2.12.0
cryptography>=42.0.0
pytest>=8.0.0
pytest-asyncio>=0.23.0
```

### Step 3: Start the Server

**Basic start (no persistence):**
```bash
python server.py
```

**With custom port:**
```bash
python server.py --port 8080
```

**With custom admin token:**
```bash
python server.py --admin-token "MySecretAdminToken123"
```

You should see output like:
```
Starting trading server on 0.0.0.0:8080
Admin token: abc123def456...
Created default admin user: admin / abc123def456
```

**IMPORTANT:** Note the admin password shown - you'll need it to login!

### Step 4: Create User Accounts

The server creates a default admin account. To add more users, use Python:

```python
# create_users.py
import sys
sys.path.insert(0, '.')

from auth import create_auth_system

# Initialize auth system (loads existing users.json if present)
user_store, session_manager = create_auth_system(
    storage_path="users.json",
    create_default_admin=True,
    default_admin_password="admin123"  # Change this!
)

# Create student accounts
students = [
    ("student1", "pass1234", "Alice"),
    ("student2", "pass5678", "Bob"),
    ("student3", "secret99", "Charlie"),
    ("trader1", "trade123", "Professional Trader"),
]

for user_id, password, display_name in students:
    try:
        user_store.create_user(
            user_id=user_id,
            password=password,
            display_name=display_name,
            is_admin=False,
        )
        print(f"Created: {user_id} / {password} ({display_name})")
    except ValueError as e:
        print(f"Skipped {user_id}: {e}")

print(f"\nTotal users: {user_store.user_count()}")
```

Run it:
```bash
python create_users.py
```

### Step 5: Verify Server is Running

```bash
# Check health endpoint
curl http://localhost:8080/health

# Expected response:
# {"status": "healthy", "markets": ["AAPL", "BTCUSD", "GOLD"]}
```

---

## Part 2: Frontend Setup

### Option A: Simple (Recommended)

1. Download the `fortisian_exchange.html` file
2. Open it directly in your browser
3. **BUT** - you'll hit CORS issues since the HTML is served from `file://`

### Option B: Serve with Python (Easy)

```bash
# Navigate to where you saved the HTML files
cd ~/fortisian/frontend

# Start a simple HTTP server
python -m http.server 3000

# Open browser to: http://localhost:3000/fortisian_exchange.html
```

### Option C: Serve with Node.js

```bash
# Install serve globally
npm install -g serve

# Serve the directory
serve -s ~/fortisian/frontend -l 3000

# Open browser to: http://localhost:3000/fortisian_exchange.html
```

### Option D: Configure Backend for Direct File Access

Edit `server.py` to serve static files:

```python
# Add to _setup_routes() method:
self.app.router.add_static('/static/', path='./frontend', name='static')

# Or serve the HTML directly:
async def _serve_frontend(self, request):
    return web.FileResponse('./frontend/fortisian_exchange.html')

self.app.router.add_get('/', self._serve_frontend)
```

---

## Part 3: Connecting Frontend to Backend

### Configuration

The frontend is configured to connect to `localhost:8080` by default. This is set in the `CONFIG` object:

```javascript
const CONFIG = {
  API_URL: 'http://localhost:8080',
  WS_URL: 'ws://localhost:8080/ws',
  // ...
};
```

### If Running on Different Ports/Hosts

Edit the HTML file and change the CONFIG values:

```javascript
const CONFIG = {
  API_URL: 'http://your-server:8080',
  WS_URL: 'ws://your-server:8080/ws',
  // ...
};
```

### CORS Configuration

The backend is configured to accept connections from any origin by default. If you need to restrict this, edit `server.py`:

```python
# In ServerConfig class:
allowed_origins: set[str] = field(default_factory=lambda: {"http://localhost:3000", "http://localhost:8080"})
require_origin: bool = True
```

---

## Part 4: Testing the Full System

### Step 1: Start Backend
```bash
cd ~/fortisian/exchange
source venv/bin/activate
python server.py --port 8080
```

### Step 2: Start Frontend Server
```bash
cd ~/fortisian/frontend
python -m http.server 3000
```

### Step 3: Open Browser
Navigate to: `http://localhost:3000/fortisian_exchange.html`

### Step 4: Login
- **Admin**: `admin` / (password shown in server console)
- **Students**: Use accounts created in Step 4 of backend setup

### Step 5: Test Trading

1. **Submit a Buy Order:**
   - Price: 100.00
   - Quantity: 10
   - Click BUY

2. **Submit a Sell Order (different user or price):**
   - Price: 100.00
   - Quantity: 5
   - Click SELL

3. **Watch:**
   - Order book updates
   - Trade appears in Recent Trades
   - Position updates
   - Admin alerts (if logged in as admin)

---

## Part 5: Default Markets

The server creates three markets by default:

| Market | Name | Tick Size | Max Position |
|--------|------|-----------|--------------|
| AAPL | Apple Inc. | 0.01 | 1000 |
| BTCUSD | Bitcoin/USD | 0.01 | 10 |
| GOLD | Gold Futures | 0.10 | 500 |

### Adding Custom Markets

Edit `server.py` or create markets programmatically:

```python
from matching_engine import Exchange

exchange = Exchange()

exchange.create_market(
    market_id="TSLA",
    name="Tesla Inc.",
    description="Electric vehicle company",
    tick_size="0.01",
    lot_size=1,
    max_position=500,
)

exchange.start_market("TSLA")
```

---

## Part 6: WebSocket Message Reference

### Client → Server Messages

| Type | Data | Description |
|------|------|-------------|
| `subscribe` | `{market_id}` | Subscribe to market updates |
| `unsubscribe` | `{market_id}` | Unsubscribe from market |
| `order_submit` | `{market_id, side, price, qty, time_in_force}` | Submit new order |
| `order_cancel` | `{market_id, order_id}` | Cancel specific order |
| `order_cancel_all` | `{market_id}` | Cancel all orders |
| `get_orders` | `{market_id}` | Get open orders |
| `get_position` | `{market_id}` | Get current position |
| `get_book` | `{market_id, depth}` | Get order book snapshot |
| `ping` | - | Keepalive |

### Server → Client Messages

| Type | Data | Description |
|------|------|-------------|
| `book_snapshot` | Full book state | Initial book on subscribe |
| `book_delta` | Changes only | Incremental book updates |
| `trade` | Trade details | New trade executed |
| `order_accepted` | Order details | Your order was accepted |
| `order_rejected` | Reason | Your order was rejected |
| `order_filled` | Fill details | Your order was filled |
| `order_cancelled` | Order ID | Your order was cancelled |
| `orders` | List of orders | Your open orders |
| `position` | Position details | Your current position |
| `market_status` | Status | Market state changed |
| `pong` | - | Heartbeat response |
| `error` | Message | Error occurred |

---

## Part 7: Troubleshooting

### "Connection refused" when starting server

**Cause:** Port already in use

**Fix:**
```bash
# Find process using port 8080
lsof -i :8080

# Kill it or use different port
python server.py --port 8081
```

### "WebSocket connection failed"

**Causes:**
1. Server not running
2. Wrong URL configured in frontend
3. CORS blocking

**Fixes:**
1. Verify server is running: `curl http://localhost:8080/health`
2. Check CONFIG values in HTML match server
3. Ensure frontend is served via HTTP (not file://)

### "Invalid credentials" on login

**Causes:**
1. Wrong password
2. User doesn't exist

**Fixes:**
1. Check server console for default admin password
2. Create users using the script above

### Orders not matching

**Causes:**
1. Market not started
2. Prices don't cross (bid < ask)

**Fixes:**
1. Market should auto-start, but verify in logs
2. To match: buy price >= sell price

### No sound effects

**Cause:** Browser blocking audio autoplay

**Fix:** Click anywhere on page first (user interaction required for audio)

---

## Part 8: Production Deployment

### Security Checklist

1. **Change default admin password**
2. **Use HTTPS** - Set `secure=True` in session config
3. **Restrict CORS origins** - Don't use `*`
4. **Use MongoDB** - For data persistence
5. **Enable rate limiting** - Already configured, but tune values
6. **Set up logging** - Configure logging to files
7. **Use reverse proxy** - nginx/Caddy for SSL termination

### Example Production Start

```bash
python server.py \
  --host 0.0.0.0 \
  --port 8080 \
  --admin-token "$(openssl rand -hex 32)"
```

### With Docker (example)

```dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
EXPOSE 8080

CMD ["python", "server.py", "--host", "0.0.0.0", "--port", "8080"]
```

```bash
docker build -t fortisian-exchange .
docker run -p 8080:8080 fortisian-exchange
```

---

## Quick Reference Card

```
┌─────────────────────────────────────────────────────────────┐
│                    FORTISIAN EXCHANGE                       │
├─────────────────────────────────────────────────────────────┤
│ Backend:  python server.py --port 8080                      │
│ Frontend: python -m http.server 3000                        │
│                                                             │
│ URLs:                                                       │
│   API:      http://localhost:8080                           │
│   WebSocket: ws://localhost:8080/ws                         │
│   Frontend: http://localhost:3000/fortisian_exchange.html   │
│                                                             │
│ Default Admin:                                              │
│   Username: admin                                           │
│   Password: (shown in server console)                       │
│                                                             │
│ Markets: AAPL, BTCUSD, GOLD                                 │
│                                                             │
│ Health Check: curl http://localhost:8080/health             │
└─────────────────────────────────────────────────────────────┘
```

---

## Files Provided

| File | Purpose |
|------|---------|
| `fortisian_exchange.html` | Full working frontend |
| `fortisian_demo.html` | Static demo (no backend needed) |
| `fortisian_login_demo.html` | Login page demo |
| `SETUP.md` | This guide |

---

## Support

For issues:
1. Check server console for errors
2. Check browser console (F12) for JavaScript errors
3. Verify WebSocket connection in Network tab
4. Ensure all services are running on correct ports
