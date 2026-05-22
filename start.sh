#!/bin/bash
set -e

echo "=========================================="
echo "Flink PTF Demo - Startup Script"
echo "=========================================="
echo ""

# Check prerequisites
echo "Checking prerequisites..."
MISSING_DEPS=0

# Check Java
if ! command -v java &> /dev/null; then
    echo "❌ Java not found. Please install Java 11+ (https://adoptium.net/)"
    MISSING_DEPS=1
else
    JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
    if [ "$JAVA_VERSION" -lt 11 ]; then
        echo "❌ Java 11+ required (found version $JAVA_VERSION)"
        MISSING_DEPS=1
    else
        echo "✅ Java $JAVA_VERSION found"
    fi
fi

# Check Maven
if ! command -v mvn &> /dev/null; then
    echo "❌ Maven not found. Please install Maven 3.8+ (https://maven.apache.org/install.html)"
    MISSING_DEPS=1
else
    echo "✅ Maven found"
fi

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 not found. Please install Python 3.10+ (https://www.python.org/downloads/)"
    MISSING_DEPS=1
else
    PYTHON_VERSION=$(python3 --version 2>&1 | cut -d' ' -f2 | cut -d'.' -f1,2)
    echo "✅ Python $PYTHON_VERSION found"
fi

# Check Terraform
if ! command -v terraform &> /dev/null; then
    echo "❌ Terraform not found. Please install Terraform 1.5+ (https://developer.hashicorp.com/terraform/install)"
    MISSING_DEPS=1
else
    TERRAFORM_VERSION=$(terraform version -json 2>/dev/null | grep -o '"terraform_version":"[^"]*"' | cut -d'"' -f4)
    echo "✅ Terraform $TERRAFORM_VERSION found"
fi

if [ $MISSING_DEPS -eq 1 ]; then
    echo ""
    echo "❌ Missing required dependencies. Please install them and try again."
    exit 1
fi

echo ""

# Check if .env exists in terraform directory
if [ ! -f "terraform/.env" ]; then
    echo "❌ Error: terraform/.env not found!"
    echo "Please copy terraform/.env.example to terraform/.env and add your Confluent Cloud credentials."
    exit 1
fi

# Step 1: Build the PTF JAR
echo "Step 1/3: Building PTF JAR..."
cd flink-ptf
mvn -q clean package
if [ $? -ne 0 ]; then
    echo "❌ Maven build failed!"
    exit 1
fi
cd ..
echo "✅ JAR built successfully"
echo ""

# Step 2: Provision Confluent Cloud resources
echo "Step 2/3: Provisioning Confluent Cloud resources..."
cd terraform
source .env

if [ ! -d ".terraform" ]; then
    echo "Initializing Terraform..."
    terraform init
fi

echo "Running terraform apply..."
terraform apply -auto-approve
if [ $? -ne 0 ]; then
    echo "❌ Terraform apply failed!"
    cd ..
    exit 1
fi
cd ..
echo "✅ Confluent Cloud resources provisioned"
echo ""

# Step 3: Start the backend
echo "Step 3/3: Starting Flask backend..."
cd backend

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv .venv
fi

# Activate virtual environment
source .venv/bin/activate

# Install dependencies
echo "Installing Python dependencies..."
pip install -q -r requirements.txt

echo ""
echo "=========================================="
echo "✅ Demo is ready!"
echo "=========================================="
echo ""
echo "Starting Flask server on http://localhost:5001"
echo "Press [CTRL]-C to stop the server"
echo ""

# Start Flask (this will block until CTRL-C)
flask --app app run -p 5001
