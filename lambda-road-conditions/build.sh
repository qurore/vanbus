#!/bin/bash
# Build Lambda deployment package for road-conditions collector

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Building Lambda deployment package..."

# Clean previous build
rm -rf package deployment.zip

# Install dependencies
pip install -r requirements.txt -t package/ --quiet

# Create deployment package
cd package
zip -r ../deployment.zip . -q
cd ..

# Add Lambda function
zip deployment.zip lambda_function.py -q

echo "Created deployment.zip ($(du -h deployment.zip | cut -f1))"
echo ""
echo "To deploy:"
echo "  aws lambda update-function-code --function-name road-conditions-collector --zip-file fileb://deployment.zip"
