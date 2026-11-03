# Environment Setup and Tool Configuration Guide
## Sentinel Dispatch: AI-Powered Climate-Aware Emergency Triage System

---

**Document Version:** 1.0  
**Last Updated:** October 2026  
**Project:** Stream Processing and Analytics (AI372IA)  
**Students:** SRIRAM VELUMURI (1RV23AI117), VIJAYKUMAR BK (1RV23AI118), SIRI KUMAR CS (1RV23AI102)

---

## Table of Contents

1. [System Prerequisites](#1-system-prerequisites)
2. [Environment Setup](#2-environment-setup)
3. [Infrastructure Configuration](#3-infrastructure-configuration)
4. [Apache Kafka Setup](#4-apache-kafka-setup)
5. [Apache Flink Deployment](#5-apache-flink-deployment)
6. [Python Services Configuration](#6-python-services-configuration)
7. [Frontend Setup](#7-frontend-setup)
8. [Data Generation and Ingestion](#8-data-generation-and-ingestion)
9. [Verification and Testing](#9-verification-and-testing)
10. [Common Operations](#10-common-operations)

---

## 1. System Prerequisites

### 1.1 Hardware Requirements

| Component | Minimum Specification | Recommended |
|-----------|----------------------|-------------|
| **CPU** | 4 cores | 8+ cores |
| **RAM** | 8 GB | 16+ GB |
| **Storage** | 20 GB free space | 50+ GB SSD |
| **Network** | Stable internet connection | High-speed broadband |

### 1.2 Software Requirements

#### Operating System
- **Linux:** Ubuntu 20.04+, Debian 11+, RHEL 8+
- **macOS:** 11.0 (Big Sur) or later
- **Windows:** 10/11 with WSL2 or Git Bash

#### Required Software Versions

```bash
# Verify installed versions
python --version          # Required: >=3.10.x, <3.13.0
node --version           # Required: >=18.x
docker --version         # Required: >=20.x
docker-compose --version # Required: v2.x
java --version           # Required: Java 21 (for Flink agents)
mvn --version            # Required: Maven 3.6+
```

### 1.3 Prerequisite Check Command

```bash
# Run comprehensive prerequisite check
make check-prereqs
```

**Expected Output:**
```
✓ Python 3.11.x detected
✓ Node.js 18.x detected
✓ Docker 24.x detected
✓ Docker Compose v2.x detected
✓ Java 21 detected
✓ Maven 3.9.x detected
✓ System libraries: geos, librdkafka installed
All prerequisites satisfied!
```

---

## 2. Environment Setup

### 2.1 System Dependencies Installation

#### Linux (Ubuntu/Debian)
```bash
# Update package index
sudo apt-get update

# Install system libraries for geospatial processing and Kafka
sudo apt-get install -y \
    libgeos-dev \
    librdkafka-dev \
    build-essential \
    git \
    curl \
    wget
```

#### Linux (RHEL/CentOS/Fedora)
```bash
# Install system libraries
sudo yum install -y \
    geos-devel \
    librdkafka-devel \
    gcc \
    gcc-c++ \
    make \
    git \
    curl \
    wget
```

#### macOS
```bash
# Install Homebrew (if not installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install system libraries
brew install geos librdkafka
```

#### Windows (WSL2/Git Bash)
```bash
# Use WSL2 with Ubuntu and follow Linux installation steps
wsl --install -d Ubuntu-22.04

# Or use Git Bash with pre-compiled binaries
# (System libraries handled by Python wheels on Windows)
```

### 2.2 Python Package Manager (uv) Installation

```bash
# Install uv (fast Python package manager)
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows (PowerShell)
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Verify installation
uv --version
# Expected: uv 0.4.x or later
```

### 2.3 Project Repository Setup

```bash
# Clone repository
git clone <repository-url>
cd sentinel-dispatch

# Verify directory structure
ls -la
# Expected: README.md, Makefile, pyproject.toml, docker-compose.yml, etc.
```

---

## 3. Infrastructure Configuration

### 3.1 Environment Variables Configuration

```bash
# Create .env file in project root
cat > .env << 'EOF'
# Operation Mode
SENTINEL_MODE=development

# API Keys
GEMINI_API_KEY=your-gemini-api-key-here
GOOGLE_MAPS_API_KEY=your-google-maps-api-key-here
FIRMS_API_KEY=your-firms-map-key-here

# Backend Configuration
BACKEND_HOST=localhost
BACKEND_PORT=8000
DATABASE_URL=sqlite:///./sentinel.db

# Frontend Environment Variables (NEXT_PUBLIC_ prefix required)
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_WS_URL=ws://localhost:8000/ws
NEXT_PUBLIC_GOOGLE_MAPS_API_KEY=${GOOGLE_MAPS_API_KEY}

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPICS_REPLICATION_FACTOR=1

# Flink Configuration
FLINK_JOBMANAGER_HOST=localhost
FLINK_JOBMANAGER_PORT=8081
EOF

# Activate environment variables
source .env

# Or export individually
export SENTINEL_MODE=development
export GEMINI_API_KEY=your-api-key
export GOOGLE_MAPS_API_KEY=your-api-key
export FIRMS_API_KEY=your-firms-key
export NEXT_PUBLIC_API_URL=http://localhost:8000
export NEXT_PUBLIC_WS_URL=ws://localhost:8000/ws
export NEXT_PUBLIC_GOOGLE_MAPS_API_KEY=${GOOGLE_MAPS_API_KEY}
```

### 3.2 Configuration File Setup

```bash
# Create configuration files from templates
make setup-config

# This creates:
# - backend/config.yaml (backend service configuration)
# - flink-agents/src/main/resources/application.conf (Flink config)
# - frontend/.env.local (Next.js environment variables)
```

---

## 4. Apache Kafka Setup

### 4.1 Kafka Infrastructure Deployment

```bash
# Start Kafka and Zookeeper containers
docker-compose up -d kafka zookeeper

# Verify Kafka is running
docker ps | grep kafka
# Expected: 2 containers (kafka, zookeeper) with status "Up"

# Check Kafka logs
docker-compose logs -f kafka

# Wait for Kafka to be ready (look for "Kafka Server started")
```

### 4.2 Kafka Topic Creation

```bash
# Create required Kafka topics
docker exec -it sentinel-kafka kafka-topics.sh \
    --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 8 \
    --topic raw-emergency-calls

docker exec -it sentinel-kafka kafka-topics.sh \
    --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 8 \
    --topic raw-weather-data

docker exec -it sentinel-kafka kafka-topics.sh \
    --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 8 \
    --topic raw-fire-data

docker exec -it sentinel-kafka kafka-topics.sh \
    --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 8 \
    --topic classified-calls

docker exec -it sentinel-kafka kafka-topics.sh \
    --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 8 \
    --topic enriched-calls

docker exec -it sentinel-kafka kafka-topics.sh \
    --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 8 \
    --topic risk-scored-calls

docker exec -it sentinel-kafka kafka-topics.sh \
    --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 8 \
    --topic generated-alerts
```

### 4.3 Kafka Topic Verification

```bash
# List all topics
make kafka-topics

# Or directly:
docker exec -it sentinel-kafka kafka-topics.sh \
    --list \
    --bootstrap-server localhost:9092

# Expected output:
# raw-emergency-calls
# raw-weather-data
# raw-fire-data
# classified-calls
# enriched-calls
# risk-scored-calls
# generated-alerts

# Describe a specific topic
docker exec -it sentinel-kafka kafka-topics.sh \
    --describe \
    --bootstrap-server localhost:9092 \
    --topic raw-emergency-calls
```

### 4.4 Kafka UI Deployment

```bash
# Start Kafka UI for monitoring
docker-compose up -d kafka-ui

# Access Kafka UI
# Browser: http://localhost:8080

# Verify Kafka UI is accessible
curl -I http://localhost:8080
# Expected: HTTP/1.1 200 OK
```

---

## 5. Apache Flink Deployment

### 5.1 Flink Cluster Startup

```bash
# Start Flink JobManager and TaskManager containers
docker-compose up -d flink-jobmanager flink-taskmanager

# Verify Flink cluster is running
docker ps | grep flink
# Expected: 2 containers (jobmanager, taskmanager) with status "Up"

# Check Flink logs
docker-compose logs -f flink-jobmanager

# Access Flink Dashboard
# Browser: http://localhost:8081

# Verify Flink REST API
curl http://localhost:8081/overview
# Expected: JSON response with cluster overview
```

### 5.2 Flink Agent Compilation

```bash
# Navigate to Flink agents directory
cd flink-agents

# Clean previous builds
mvn clean

# Compile all agents
mvn package

# Or use Makefile shortcut
cd ..
make flink-build

# Expected output:
# [INFO] BUILD SUCCESS
# [INFO] sentinel-dispatch-flink-agents.jar created in target/

# Verify JAR file
ls -lh flink-agents/target/*.jar
# Expected: sentinel-dispatch-flink-agents-1.0-SNAPSHOT.jar (~50-100 MB)
```

### 5.3 Flink Agent Deployment

```bash
# Submit CallProcessingAgent
make flink-submit AGENT=CallProcessingAgent

# Or manually:
docker exec -it sentinel-flink-jobmanager flink run \
    -c com.sentinel.agents.CallProcessingAgent \
    /opt/flink-agents/sentinel-dispatch-flink-agents.jar

# Submit DataEnrichmentAgent
make flink-submit AGENT=DataEnrichmentAgent

# Submit RiskScoringAgent
make flink-submit AGENT=RiskScoringAgent

# Submit AlertGenerationAgent
make flink-submit AGENT=AlertGenerationAgent

# Verify all jobs are running
make flink-jobs

# Or directly:
curl http://localhost:8081/jobs | jq
```

### 5.4 Flink Job Monitoring

```bash
# List running Flink jobs
make flink-jobs

# Check specific job details
curl http://localhost:8081/jobs/<job-id> | jq

# View Flink metrics
curl http://localhost:8081/jobs/<job-id>/metrics | jq

# Monitor Flink Dashboard
# Browser: http://localhost:8081
# Navigate to: Running Jobs → <Job Name> → Metrics
```

---

## 6. Python Services Configuration

### 6.1 Python Dependencies Installation

```bash
# Install Python dependencies using uv
make install-python

# Or manually:
uv sync --all-extras

# Verify virtual environment
uv run python --version
# Expected: Python 3.10.x or 3.11.x or 3.12.x

# Check installed packages
uv pip list

# Expected packages:
# - fastapi
# - uvicorn
# - kafka-python
# - google-generativeai
# - geopandas
# - shapely
# - requests
# - pydantic
# - websockets
```

### 6.2 ML/AI Service Deployment

```bash
# Start ML/AI FastAPI service container
docker-compose up -d ml-api-service

# Or run locally with uv
uv run uvicorn backend.ml_services.main:app \
    --host 0.0.0.0 \
    --port 8000 \
    --reload

# Verify service is running
curl http://localhost:8000/health
# Expected: {"status": "healthy"}

# Access API documentation
# Browser: http://localhost:8000/docs (Swagger UI)
# Browser: http://localhost:8000/redoc (ReDoc)

# Check API logs
make api-logs

# Or directly:
docker-compose logs -f ml-api-service
```

### 6.3 Backend API Service Deployment

```bash
# Start Backend API service
docker-compose up -d backend-api

# Or run locally
uv run uvicorn backend.api.main:app \
    --host 0.0.0.0 \
    --port 8001 \
    --reload

# Verify backend API
curl http://localhost:8001/api/v1/health
# Expected: {"status": "ok", "version": "1.0.0"}

# Test WebSocket endpoint
# Use wscat or browser console:
wscat -c ws://localhost:8001/ws

# Check backend logs
docker-compose logs -f backend-api
```

### 6.4 Database Initialization

```bash
# Initialize SQLite database schema
uv run python backend/db/init_db.py

# Or use Alembic migrations (if configured)
uv run alembic upgrade head

# Verify database file
ls -lh backend/sentinel.db
# Expected: sentinel.db created (~0 bytes initially)

# Check database schema
sqlite3 backend/sentinel.db ".schema"
# Expected: CREATE TABLE emergency_calls, alerts, etc.
```

---

## 7. Frontend Setup

### 7.1 Node.js Dependencies Installation

```bash
# Navigate to frontend directory
cd frontend

# Install dependencies
npm install

# Or use Makefile
cd ..
make install-frontend

# Verify node_modules
ls -lh frontend/node_modules | wc -l
# Expected: 500+ packages installed

# Return to project root
cd ..
```

### 7.2 Frontend Configuration

```bash
# Create frontend environment file
cat > frontend/.env.local << 'EOF'
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_WS_URL=ws://localhost:8000/ws
NEXT_PUBLIC_GOOGLE_MAPS_API_KEY=your-google-maps-api-key-here
EOF

# Verify configuration
cat frontend/.env.local
```

### 7.3 Frontend Development Server

```bash
# Start Next.js development server
cd frontend
npm run dev

# Or use Makefile
make frontend-dev

# Expected output:
# ▲ Next.js 14.x
# - Local:        http://localhost:3000
# - Ready in 2.3s

# Access frontend
# Browser: http://localhost:3000

# Check frontend logs
# Logs appear in terminal where npm run dev is running
```

### 7.4 Frontend Production Build

```bash
# Build optimized production bundle
cd frontend
npm run build

# Start production server
npm start

# Or use Docker
docker-compose up -d frontend

# Verify production build
curl http://localhost:3000
# Expected: HTML response
```

---

## 8. Data Generation and Ingestion

### 8.1 Synthetic Emergency Call Generation

```bash
# Generate synthetic Maui wildfire emergency calls
uv run python scripts/generate_911_calls.py \
    --num-calls 50 \
    --output-dir data/generated_calls \
    --event-type wildfire \
    --location maui

# With custom parameters
uv run python scripts/generate_911_calls.py \
    --num-calls 100 \
    --start-date "2023-08-08" \
    --end-date "2023-08-09" \
    --output-dir data/generated_calls/maui_2023 \
    --model gemini-pro

# Verify generated files
ls -lh data/generated_calls/
# Expected: maui_calls_<timestamp>.json (1-5 MB)

# Inspect generated data
cat data/generated_calls/maui_calls_*.json | jq '.[0]'
```

### 8.2 Data Ingestion to Kafka

```bash
# Ingest generated calls into Kafka
make ingest-dev CALLS_FILE=data/generated_calls/maui_calls_20231101_143022.json

# Or manually run ingestion script
uv run python scripts/ingest_data.py \
    --calls-file data/generated_calls/maui_calls_*.json \
    --kafka-broker localhost:9092 \
    --topic raw-emergency-calls

# Verify messages in Kafka topic
docker exec -it sentinel-kafka kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic raw-emergency-calls \
    --from-beginning \
    --max-messages 5

# Expected: JSON emergency call records
```

### 8.3 Weather Data Ingestion

```bash
# Start weather data ingestion service
uv run python ingestion/weather_ingestion.py \
    --api-key ${NOAA_API_KEY} \
    --interval 300 \
    --kafka-broker localhost:9092

# Or use background process
nohup uv run python ingestion/weather_ingestion.py > logs/weather_ingestion.log 2>&1 &

# Verify weather data in Kafka
docker exec -it sentinel-kafka kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic raw-weather-data \
    --from-beginning \
    --max-messages 5
```

### 8.4 Fire Data Ingestion

```bash
# Start FIRMS fire data ingestion service
uv run python ingestion/fire_ingestion.py \
    --firms-api-key ${FIRMS_API_KEY} \
    --region maui \
    --interval 3600 \
    --kafka-broker localhost:9092

# Or use background process
nohup uv run python ingestion/fire_ingestion.py > logs/fire_ingestion.log 2>&1 &

# Verify fire data in Kafka
docker exec -it sentinel-kafka kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic raw-fire-data \
    --from-beginning \
    --max-messages 5
```

---

## 9. Verification and Testing

### 9.1 Service Health Checks

```bash
# Check all service statuses
make status

# Individual health checks
curl http://localhost:8080        # Kafka UI
curl http://localhost:8081        # Flink Dashboard
curl http://localhost:8000/health # ML API Service
curl http://localhost:8001/api/v1/health  # Backend API
curl http://localhost:3000        # Frontend

# Check container status
docker-compose ps

# Expected: All services "Up" and healthy
```

### 9.2 End-to-End Pipeline Test

```bash
# Run end-to-end test
uv run python tests/integration/test_e2e_pipeline.py

# Expected output:
# ✓ Call ingested to Kafka
# ✓ CallProcessingAgent classified call
# ✓ DataEnrichmentAgent enriched with weather/fire data
# ✓ RiskScoringAgent calculated risk score
# ✓ AlertGenerationAgent generated alert
# ✓ Backend API received processed call
# ✓ Frontend WebSocket received update
# End-to-end latency: 8.42 seconds
```

### 9.3 Unit Tests

```bash
# Run Python unit tests
make test

# Or directly:
uv run pytest tests/unit/ -v

# Run with coverage
uv run pytest tests/unit/ --cov=backend --cov-report=html

# View coverage report
open htmlcov/index.html
```

### 9.4 Performance Testing

```bash
# Run load test (1000 calls)
uv run python tests/performance/load_test.py \
    --num-calls 1000 \
    --rate 10 \
    --duration 100

# Expected metrics:
# - Throughput: 10 calls/second
# - Average latency: <10 seconds
# - P95 latency: <15 seconds
# - P99 latency: <20 seconds
```

---

## 10. Common Operations

### 10.1 Complete System Startup

```bash
# One-command setup and start
make setup

# This executes:
# 1. Check prerequisites
# 2. Install Python dependencies
# 3. Install frontend dependencies
# 4. Start all Docker containers
# 5. Create Kafka topics
# 6. Start frontend dev server

# Verify all services
make status
```

### 10.2 Log Monitoring

```bash
# View all container logs
make logs

# View specific service logs
make api-logs          # ML API service
docker-compose logs -f kafka          # Kafka logs
docker-compose logs -f flink-jobmanager  # Flink logs
docker-compose logs -f backend-api    # Backend API logs

# Follow logs in real-time
docker-compose logs -f --tail=100
```

### 10.3 Data Reset

```bash
# Stop all services
make stop

# Remove all containers and volumes
docker-compose down -v

# Clean database
rm -f backend/sentinel.db

# Clean generated data
rm -rf data/generated_calls/*

# Restart fresh
make setup
```

### 10.4 Code Quality and Formatting

```bash
# Run linters
make lint

# Format Python code
make format

# Or manually:
uv run ruff check .           # Linting
uv run ruff format .          # Formatting
uv run mypy backend/          # Type checking
```

### 10.5 Kafka Consumer Monitoring

```bash
# List consumer groups
docker exec -it sentinel-kafka kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --list

# Describe consumer group lag
docker exec -it sentinel-kafka kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe \
    --group flink-consumer-group
```

### 10.6 Flink Savepoint Management

```bash
# Create savepoint
curl -X POST http://localhost:8081/jobs/<job-id>/savepoints \
    -H "Content-Type: application/json" \
    -d '{"target-directory": "/tmp/savepoints", "cancel-job": false}'

# Stop job with savepoint
curl -X PATCH http://localhost:8081/jobs/<job-id>?mode=cancel

# Restore from savepoint
docker exec -it sentinel-flink-jobmanager flink run \
    -s /tmp/savepoints/<savepoint-id> \
    -c com.sentinel.agents.CallProcessingAgent \
    /opt/flink-agents/sentinel-dispatch-flink-agents.jar
```

### 10.7 System Shutdown

```bash
# Stop all services gracefully
make stop

# Or manually:
docker-compose down

# Stop and remove volumes (data loss!)
docker-compose down -v

# Stop frontend dev server
# Press Ctrl+C in terminal running npm run dev
```

---

## Appendix A: Makefile Command Reference

| Command | Description |
|---------|-------------|
| `make help` | Show all available commands |
| `make check-prereqs` | Verify system prerequisites |
| `make install` | Install all dependencies (Python + Frontend) |
| `make install-python` | Install Python dependencies only |
| `make install-frontend` | Install frontend dependencies only |
| `make setup-config` | Create configuration files |
| `make setup` | Complete setup (install + start services) |
| `make start` | Start all Docker containers |
| `make stop` | Stop all Docker containers |
| `make restart` | Restart all services |
| `make status` | Check service status |
| `make logs` | View all container logs |
| `make api-logs` | View ML API service logs |
| `make kafka-topics` | List Kafka topics |
| `make flink-build` | Build Flink agents |
| `make flink-submit AGENT=<name>` | Submit Flink agent |
| `make flink-jobs` | List running Flink jobs |
| `make ingest-dev CALLS_FILE=<path>` | Ingest test data |
| `make test` | Run unit tests |
| `make lint` | Run linters |
| `make format` | Format code |
| `make clean` | Clean build artifacts |

---

## Appendix B: Port Reference

| Service | Port | URL |
|---------|------|-----|
| Kafka | 9092 | kafka://localhost:9092 |
| Kafka UI | 8080 | http://localhost:8080 |
| Flink Dashboard | 8081 | http://localhost:8081 |
| ML API Service | 8000 | http://localhost:8000 |
| ML API Docs | 8000 | http://localhost:8000/docs |
| Backend API | 8001 | http://localhost:8001 |
| Backend WebSocket | 8001 | ws://localhost:8001/ws |
| Frontend | 3000 | http://localhost:3000 |
| Zookeeper | 2181 | localhost:2181 |

---

## Appendix C: Troubleshooting

### Issue: Docker containers fail to start
```bash
# Check Docker daemon
sudo systemctl status docker

# Restart Docker
sudo systemctl restart docker

# Check port conflicts
netstat -tuln | grep -E "9092|8080|8081|8000|3000"
```

### Issue: Kafka topic creation fails
```bash
# Wait for Kafka to be fully ready (2-3 minutes)
docker-compose logs kafka | grep "Kafka Server started"

# Manually recreate topics
docker exec -it sentinel-kafka kafka-topics.sh --delete --topic raw-emergency-calls --bootstrap-server localhost:9092
docker exec -it sentinel-kafka kafka-topics.sh --create --topic raw-emergency-calls --bootstrap-server localhost:9092 --partitions 8 --replication-factor 1
```

### Issue: Flink agent submission fails
```bash
# Verify JAR exists
ls -lh flink-agents/target/*.jar

# Check Flink JobManager logs
docker-compose logs flink-jobmanager

# Rebuild agents
cd flink-agents && mvn clean package
```

### Issue: Frontend fails to connect to backend
```bash
# Verify CORS configuration in backend
# Check backend/.env has correct ALLOWED_ORIGINS

# Verify WebSocket connection
wscat -c ws://localhost:8001/ws

# Check browser console for errors
```

---

**End of Document**
