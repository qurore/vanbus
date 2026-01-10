#!/bin/bash
# Build Lambda deployment package using Docker

set -e

cd "$(dirname "$0")"

echo "Building Lambda package..."

# Use Docker to build for Lambda environment (Amazon Linux 2)
docker run --rm -v "$PWD":/var/task \
    public.ecr.aws/sam/build-python3.11:latest \
    /bin/bash -c "
        pip install -r requirements.txt -t package/ --platform manylinux2014_x86_64 --only-binary=:all: &&
        cp lambda_function.py package/
    "

# Create zip
cd package
zip -r ../lambda_package.zip .
cd ..

echo "Package created: lambda_package.zip"
ls -lh lambda_package.zip
