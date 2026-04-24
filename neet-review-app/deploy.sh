#!/bin/bash
# Deploy NEET Review Dashboard to Cloud Run
# Usage: bash deploy.sh

set -e

PROJECT="project-3639c8e1-b432-4a18-99f"
REGION="us-central1"
SERVICE="neet-review"

echo "Building and deploying to Cloud Run..."

gcloud run deploy $SERVICE \
  --project $PROJECT \
  --region $REGION \
  --source . \
  --allow-unauthenticated \
  --memory 512Mi \
  --timeout 60s \
  --set-env-vars GCP_PROJECT=$PROJECT \
  --min-instances 0 \
  --max-instances 3

echo ""
echo "Deployed! Get the URL:"
gcloud run services describe $SERVICE --project $PROJECT --region $REGION --format "value(status.url)"
