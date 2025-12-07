#!/bin/bash
# Fortisian Exchange - Quick Start Script
# 
# This script starts both the backend server and a simple frontend server.
# 
# Prerequisites:
#   - Python 3.12+
#   - pip install -r requirements.txt (already done)
#
# Usage:
#   chmod +x start.sh
#   ./start.sh

set -e

echo "════════════════════════════════════════════════════════════"
echo "              FORTISIAN EXCHANGE - QUICK START              "
echo "════════════════════════════════════════════════════════════"
echo

# Check Python version
PYTHON_VERSION=$(python3 --version 2>&1 | cut -d' ' -f2 | cut -d'.' -f1,2)
echo "Python version: $PYTHON_VERSION"

# Directory setup
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Start backend server in background
echo
echo "Starting backend server on port 8080..."
python3 server.py --port 8080 &
BACKEND_PID=$!
echo "Backend PID: $BACKEND_PID"

# Wait for backend to start
sleep 2

# Check if backend is running
if ! kill -0 $BACKEND_PID 2>/dev/null; then
    echo "ERROR: Backend failed to start!"
    exit 1
fi

# Start frontend server
echo
echo "Starting frontend server on port 3000..."
cd "$SCRIPT_DIR"
python3 -m http.server 3000 --directory . &
FRONTEND_PID=$!
echo "Frontend PID: $FRONTEND_PID"

# Wait a moment
sleep 1

echo
echo "════════════════════════════════════════════════════════════"
echo "                    SERVERS RUNNING                         "
echo "════════════════════════════════════════════════════════════"
echo
echo "  Backend API:    http://localhost:8080"
echo "  WebSocket:      ws://localhost:8080/ws"
echo "  Frontend:       http://localhost:3000/fortisian_exchange.html"
echo
echo "  Health check:   curl http://localhost:8080/health"
echo
echo "  Press Ctrl+C to stop all servers"
echo
echo "════════════════════════════════════════════════════════════"

# Handle shutdown
cleanup() {
    echo
    echo "Shutting down..."
    kill $BACKEND_PID 2>/dev/null || true
    kill $FRONTEND_PID 2>/dev/null || true
    echo "Servers stopped."
    exit 0
}

trap cleanup SIGINT SIGTERM

# Keep script running
wait
