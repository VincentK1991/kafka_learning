#!/bin/bash

echo "🛑 Force shutting down all services..."

# Function to kill processes by port
kill_by_port() {
    local port=$1
    local service=$2
    echo "Killing $service on port $port..."
    
    # Find and kill process using the port
    PID=$(lsof -ti:$port 2>/dev/null)
    if [ ! -z "$PID" ]; then
        echo "  Found PID $PID for $service"
        kill -15 $PID 2>/dev/null  # Try graceful shutdown first
        sleep 1
        
        # Force kill if still running
        if kill -0 $PID 2>/dev/null; then
            echo "  Force killing $service (PID $PID)"
            kill -9 $PID 2>/dev/null
        fi
        echo "  ✅ $service stopped"
    else
        echo "  ℹ️  No $service process found"
    fi
}

# Kill all our services
kill_by_port $1