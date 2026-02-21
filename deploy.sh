#!/usr/bin/env bash
set -euo pipefail

source .env
FUNCTION_NAME="${LAMBDA_FUNCTION_NAME:?Set LAMBDA_FUNCTION_NAME in .env}"
REGION="${AWS_REGION:?Set AWS_REGION in .env}"

echo "==> Installing dependencies into package/"
rm -rf package
pip install -r requirements.txt -t package --quiet

echo "==> Creating deployment zip"
cd package
zip -r9 ../lambda_package.zip . --quiet
cd ..
zip -g lambda_package.zip lambda_function.py --quiet

echo "==> Deploying to Lambda ($FUNCTION_NAME in $REGION)"
aws lambda update-function-code \
  --function-name "$FUNCTION_NAME" \
  --region "$REGION" \
  --zip-file fileb://lambda_package.zip \
  --output text --query 'FunctionName'

echo "==> Done! Lambda updated successfully."
