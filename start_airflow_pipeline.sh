#!/bin/bash

echo "🚀 Starting Fraud Detection ETL Pipeline with Airflow"
echo "=" * 60

# Stop any existing containers
echo "🛑 Stopping existing containers..."
docker compose -f docker-compose-airflow.yml down
docker compose -f docker-compose.yml down


# Start the services
echo "🔥 Starting all services..."
docker compose -f docker-compose.yml up -d
docker compose -f docker-compose-airflow.yml up -d

echo "⏳ Waiting for services to be ready..."
sleep 30

# Check service status
echo "📊 Service Status:"
docker compose -f docker-compose-airflow.yml ps

echo ""
echo "🎯 Access Points:"
echo "- MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
echo ""
echo "✅ Pipeline is starting up! Please wait a few minutes for all services to be ready." 