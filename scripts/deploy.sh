#!/bin/bash
set -e


# Deployment Script to: Build, push, and deploy the Spotify-DE Docker image.
#
# This script automates the full deployment pipeline
#
# Usage:
#   bash scripts/deploy.sh           # Deploy with tag "latest"
#   bash scripts/deploy.sh 0.2.0     # Deploy with specific version
# ==============================================================================

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA_DIR="$PROJECT_ROOT/infra"
VERSION="${1:-latest}"

echo "=== Spotify-DE Deployment Script ==="
echo "Version: $VERSION"
echo ""

# Step 1: Validate prerequisites
echo "Checking prerequisites..."
command -v docker &> /dev/null || { echo "Docker is not installed."; exit 1; }
command -v az &> /dev/null || { echo "Azure CLI is not installed."; exit 1; }
command -v terraform &> /dev/null || { echo "Terraform is not installed."; exit 1; }

# Step 2: Get infrastructure values from Terraform
echo ""
echo "Retrieving infrastructure values from Terraform..."
cd "$INFRA_DIR"

RESOURCE_GROUP=$(terraform output -raw resource_group_name)
ACR_NAME=$(terraform output -raw acr_name)
ACR_LOGIN_SERVER=$(terraform output -raw acr_login_server)

echo "Resource Group: $RESOURCE_GROUP"
echo "ACR Name: $ACR_NAME"
echo "ACR Login Server: $ACR_LOGIN_SERVER"
echo ""

# Step 3: Authenticate with ACR
echo "Authenticating with Azure Container Registry..."
az acr login --name "$ACR_NAME"

# Step 4: Build Docker image
echo ""
echo "Building Docker image: spotify-platform:$VERSION"
cd "$PROJECT_ROOT"
docker build --platform linux/amd64 -t spotify-platform:$VERSION .
docker tag spotify-platform:$VERSION spotify-platform:latest

# Step 5: Tag and push to ACR
echo ""
echo "Tagging and pushing to ACR..."
docker tag spotify-platform:$VERSION "$ACR_LOGIN_SERVER/spotify-platform:$VERSION"
docker tag spotify-platform:latest "$ACR_LOGIN_SERVER/spotify-platform:latest"

docker push "$ACR_LOGIN_SERVER/spotify-platform:$VERSION"
docker push "$ACR_LOGIN_SERVER/spotify-platform:latest"

echo ""
echo "=== Image pushed: $ACR_LOGIN_SERVER/spotify-platform:$VERSION ==="

# Step 6: Apply Terraform (updates compute jobs with new image tag)
echo ""
echo "Applying Terraform to update Container Apps jobs..."
cd "$INFRA_DIR"
terraform apply -var="image_tag=$VERSION" -auto-approve

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "Scheduled jobs:"
echo "  - Ingest:           every hour at :05 (job-ingest-spotify-de-prod)"
echo "  - Transform + Gold: every hour at :20 (job-transform-spotify-de-prod)"
echo ""
echo "To:"
echo "  # View job executions"
echo "  az containerapp job execution list --name job-ingest-spotify-de-prod --resource-group $RESOURCE_GROUP -o table"
echo "  az containerapp job execution list --name job-transform-spotify-de-prod --resource-group $RESOURCE_GROUP -o table"
echo ""
echo "  # Trigger a manual run"
echo "  az containerapp job start --name job-transform-spotify-de-prod --resource-group $RESOURCE_GROUP"
echo ""
echo "  # View logs"
echo "  az containerapp job logs show --name job-ingest-spotify-de-prod --resource-group $RESOURCE_GROUP --follow"
