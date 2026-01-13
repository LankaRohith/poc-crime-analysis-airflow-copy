#!/bin/bash

# Apache Airflow Setup Script

echo "Setting up Apache Airflow for Crime Analysis..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Please start Docker first."
    exit 1
fi

# Check if Spring Boot app is running
if ! curl -s http://localhost:8080/api/simple-crimes/summary > /dev/null; then
    echo "Spring Boot application is not running on port 8080."
    echo "Please start it first with: mvn spring-boot:run"
    exit 1
fi

echo "Prerequisites check passed!"

# Create necessary directories
mkdir -p logs plugins

echo "Created necessary directories"

# Start Airflow infrastructure
echo "Starting Airflow infrastructure..."
docker-compose up -d

# Wait for Airflow to be ready
echo "Waiting for Airflow to be ready..."
sleep 30

# Check if Airflow is accessible
if curl -s http://localhost:8081 > /dev/null; then
    echo "Airflow is ready!"
    echo "Access Airflow UI at: http://localhost:8081"
    echo "Login with: admin / admin"
    echo ""
    echo "Your DAGs will run:"
    echo "   - Daily at 3:00 PM EST: crime_analysis_pipeline"
    echo "   - Every hour: crime_data_quality_check"
    echo ""
    echo "Available commands:"
    echo "   - View logs: docker-compose logs -f"
    echo "   - Stop Airflow: docker-compose down"
    echo "   - Restart Airflow: docker-compose restart"
else
    echo "Airflow failed to start. Check logs with: docker-compose logs"
fi
