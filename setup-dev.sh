#!/bin/bash
# Development Setup Script for Sentinel Dispatch
# This script helps set up the development environment
# Now uses Makefile commands for consistency

set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR" && pwd)"

# Change to project root
cd "$PROJECT_ROOT"

echo "🚀 Setting up Sentinel Dispatch development environment..."
echo ""

# Check if Makefile exists
if [ ! -f "Makefile" ]; then
    echo "❌ Makefile not found. Please run this script from the project root."
    exit 1
fi

# Use Makefile commands for setup
echo "📋 Checking prerequisites..."
if ! make check-prereqs; then
    echo ""
    echo "❌ Prerequisites check failed. Please install missing dependencies."
    exit 1
fi

echo ""
echo "📦 Installing dependencies..."
make install

echo ""
echo "🐳 Starting development containers..."
make up-no-build

echo ""
echo "✅ Development environment setup complete!"
echo ""
echo "📝 Next steps:"
echo "   1. Create a .env file in the project root with:"
echo "      GEMINI_API_KEY=your-key-here"
echo "      GOOGLE_MAPS_API_KEY=your-key-here"
echo "      NEXT_PUBLIC_API_URL=http://localhost:8000"
echo "      NEXT_PUBLIC_WS_URL=ws://localhost:8000/ws"
echo "      NEXT_PUBLIC_GOOGLE_MAPS_API_KEY=\"\${GOOGLE_MAPS_API_KEY}\""
echo "      FIRMS_API_KEY=your-key-here  # Optional"
echo ""
echo "   2. Source the .env file:"
echo "      source .env"
echo ""
echo "   3. Build Flink agents:"
echo "      make flink-build"
echo ""
echo "   4. Submit Flink agents:"
echo "      make flink-submit AGENT=CallProcessingAgent"
echo ""
echo "   5. Start frontend:"
echo "      make frontend-dev"
echo ""
echo "📚 Useful commands:"
echo "   - Check status: make status"
echo "   - View logs: make logs"
echo "   - See all commands: make help"
echo ""
echo "📚 See documentation/DEVELOPMENT_MODE.md for more details"

