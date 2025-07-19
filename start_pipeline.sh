#!/bin/bash

echo "🚀 Starting Kafka Data Pipeline..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Start Kafka infrastructure
echo "📦 Starting Kafka infrastructure with Docker Compose..."
docker-compose up -d

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
sleep 10

# Install Python dependencies with uv
echo "🐍 Installing Python dependencies with uv (Python 3.12)..."
if ! command -v uv &> /dev/null; then
    echo "⚠️  uv not found. Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    source $HOME/.cargo/env
fi

# Check Python 3.12 availability
if ! command -v python3.12 &> /dev/null; then
    echo "⚠️  Python 3.12 not found. Please install Python 3.12 first."
    echo "   On macOS: brew install python@3.12"
    echo "   On Ubuntu: sudo apt install python3.12"
    exit 1
fi

# Create virtual environment and install dependencies
uv venv --python 3.12
source .venv/bin/activate
uv sync

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 15

# Check pipeline status
echo "🔍 Checking pipeline status..."
uv run python pipeline_manager.py status

echo ""
echo "✅ Pipeline setup complete!"
echo ""
echo "⚠️  Important: Set up your OpenAI API key for AI features:"
echo "   echo 'OPENAI_API_KEY=your_key_here' > .env"
echo ""
echo "🎯 Quick Start Commands:"
echo ""
echo "🤖 AI-Powered Services:"
echo "   Start API Producer:  uv run python services/producer/producer_api.py"
echo "   Start API Consumer:  uv run python services/consumer/consumer_api.py"
echo "   Start AI Agent:      uv run python services/ai-agent/ai_agent.py"
echo ""
echo "📊 Management & Monitoring:"
echo "   Check Status:        uv run python pipeline_manager.py status"
echo "   View Data:           uv run python pipeline_manager.py sample-data"
echo "   Analytics:           uv run python pipeline_manager.py analytics"
echo ""
echo "🔧 Legacy CLI Tools:"
echo "   CLI Producer:        uv run python scripts/producer.py"
echo "   CLI Consumer:        uv run python scripts/consumer.py"
echo ""
echo "🌐 Kafka UI: http://localhost:8080"
echo ""
echo "Happy learning! 🎉" 