#!/bin/bash
# Install script for Cohesity SDK v0.3 dependencies
# This script will install the modern cohesity_sdk from source

set -e  # Exit on any error

echo "🚀 Cohesity SDK Installation Script"
echo "=================================="
echo ""

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not found. Please install Python 3.6+ first."
    exit 1
fi

# Check if pip is available
if ! command -v pip &> /dev/null && ! command -v pip3 &> /dev/null; then
    echo "❌ pip is required but not found. Please install pip first."
    exit 1
fi

PIP_CMD="pip3"
if command -v pip &> /dev/null; then
    PIP_CMD="pip"
fi

echo "✅ Python 3 found: $(python3 --version)"
echo "✅ Pip found: $($PIP_CMD --version | head -1)"
echo ""

# Create a temporary directory for installation
TEMP_DIR=$(mktemp -d)
echo "📁 Using temporary directory: $TEMP_DIR"

# Navigate to temp directory
cd "$TEMP_DIR"

echo ""
echo "📦 Downloading Cohesity SDK from GitHub..."

# Clone the cohesity_sdk repository
if ! git clone https://github.com/cohesity/cohesity_sdk.git; then
    echo "❌ Failed to clone cohesity_sdk repository."
    echo "   Please check your internet connection and git installation."
    exit 1
fi

echo "✅ Repository cloned successfully"

# Navigate to the SDK directory
cd cohesity_sdk

echo ""
echo "🔧 Installing SDK dependencies..."

# Install requirements
if ! $PIP_CMD install -r requirements.txt; then
    echo "❌ Failed to install SDK requirements."
    echo "   You may need to run with sudo or use a virtual environment."
    exit 1
fi

echo "✅ Dependencies installed successfully"

echo ""
echo "📦 Installing Cohesity SDK..."

# Install the SDK
if ! python3 setup.py install; then
    echo "❌ Failed to install Cohesity SDK."
    echo "   You may need to run with sudo or use a virtual environment."
    exit 1
fi

echo "✅ Cohesity SDK installed successfully"

# Clean up
cd /
rm -rf "$TEMP_DIR"

echo ""
echo "🧪 Testing installation..."

# Test the installation
if python3 -c "import cohesity_sdk; print('✅ cohesity_sdk imported successfully')" 2>/dev/null; then
    echo "✅ Installation test passed"
else
    echo "❌ Installation test failed"
    echo "   The SDK may not be properly installed."
    exit 1
fi

echo ""
echo "🎉 Installation completed successfully!"
echo ""
echo "📋 Next steps:"
echo "   1. Run the Data Protection Manager:"
echo "      python3 dp_man_v3.py -s your-cluster.example.com -u admin"
echo ""
echo "   2. For help and options:"
echo "      python3 dp_man_v3.py --help"
echo ""
echo "   3. For troubleshooting, see the README.md file"
echo "" 