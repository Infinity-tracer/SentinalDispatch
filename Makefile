.PHONY: help build up down stop restart logs clean test lint format type-check frontend-dev frontend-stop frontend-build flink-build flink-submit kafka-topics kafka-logs api-logs flink-logs shell-api shell-kafka shell-flink install install-python install-frontend setup-config check-prereqs ingest-dev ingest-weather ingest-fire ingest-calls

# Variables
COMPOSE_FILE := docker-compose.dev.yml
SERVICE_NAME := ml-api-service
FLINK_JAR := flink-agents/target/flink-agents-1.0.0.jar
FLINK_JM := sentinel-dispatch-flink-jobmanager

# Colors for output
CYAN := \033[0;36m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(CYAN)Sentinel Dispatch - Development Commands$(NC)"
	@echo ""
	@echo "$(GREEN)Usage:$(NC) make [target]"
	@echo ""
	@echo "$(GREEN)Available targets:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-20s$(NC) %s\n", $$1, $$2}'

# Docker Compose Commands
build: ## Build the ml-api-service Docker image
	@echo "$(CYAN)Building ml-api-service Docker image...$(NC)"
	docker-compose -f $(COMPOSE_FILE) build $(SERVICE_NAME)
	@echo "$(GREEN)✓ Build complete$(NC)"

up: build ## Start all development containers (builds ml-api-service first)
	@echo "$(CYAN)Starting development containers...$(NC)"
	docker-compose -f $(COMPOSE_FILE) up -d
	@echo "$(GREEN)✓ Containers started$(NC)"
	@echo "$(YELLOW)Waiting for services to be healthy...$(NC)"
	@echo "$(CYAN)Services:$(NC)"
	@echo "  - Kafka: http://localhost:9092"
	@echo "  - Kafka UI: http://localhost:8080"
	@echo "  - Flink: http://localhost:8081"
	@echo "  - ML API: http://localhost:8000"
	@echo "  - API Docs: http://localhost:8000/docs"

up-no-build: ## Start containers without building (use existing images)
	@echo "$(CYAN)Starting development containers (no build)...$(NC)"
	docker-compose -f $(COMPOSE_FILE) up -d
	@echo "$(GREEN)✓ Containers started$(NC)"

down: ## Stop all containers
	@echo "$(CYAN)Stopping containers...$(NC)"
	docker-compose -f $(COMPOSE_FILE) down
	@echo "$(GREEN)✓ Containers stopped$(NC)"

stop: down frontend-stop ## Stop containers and frontend dev server

restart: down up ## Restart all containers

clean: ## Stop containers and remove volumes (⚠️ deletes Kafka data)
	@echo "$(RED)⚠️  This will delete all Kafka data!$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose -f $(COMPOSE_FILE) down -v; \
		echo "$(GREEN)✓ Containers and volumes removed$(NC)"; \
	fi

# Logs
logs: ## Show logs from all containers
	docker-compose -f $(COMPOSE_FILE) logs -f

api-logs: ## Show logs from ml-api-service
	docker-compose -f $(COMPOSE_FILE) logs -f $(SERVICE_NAME)

kafka-logs: ## Show logs from Kafka
	docker-compose -f $(COMPOSE_FILE) logs -f kafka

flink-logs: ## Show logs from Flink JobManager
	docker-compose -f $(COMPOSE_FILE) logs -f flink-jobmanager

# Shell access
shell-api: ## Open shell in ml-api-service container
	docker exec -it $(SERVICE_NAME) /bin/bash

shell-kafka: ## Open shell in Kafka container
	docker exec -it sentinel-dispatch-kafka /bin/bash

shell-flink: ## Open shell in Flink JobManager container
	docker exec -it $(FLINK_JM) /bin/bash

# Kafka Commands
kafka-topics: ## List all Kafka topics
	docker exec sentinel-dispatch-kafka kafka-topics --list --bootstrap-server localhost:9092

kafka-consume: ## Consume messages from a topic (usage: make kafka-consume TOPIC=emergency-calls)
	@if [ -z "$(TOPIC)" ]; then \
		echo "$(RED)Error: TOPIC not specified$(NC)"; \
		echo "Usage: make kafka-consume TOPIC=emergency-calls"; \
		exit 1; \
	fi
	docker exec -it sentinel-dispatch-kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic $(TOPIC) \
		--from-beginning

kafka-produce: ## Produce messages to a topic (usage: make kafka-produce TOPIC=emergency-calls)
	@if [ -z "$(TOPIC)" ]; then \
		echo "$(RED)Error: TOPIC not specified$(NC)"; \
		echo "Usage: make kafka-produce TOPIC=emergency-calls"; \
		exit 1; \
	fi
	docker exec -it sentinel-dispatch-kafka kafka-console-producer \
		--bootstrap-server localhost:9092 \
		--topic $(TOPIC)

# Flink Commands
flink-build: ## Build Flink agents JAR
	@echo "$(CYAN)Building Flink agents...$(NC)"
	cd flink-agents && mvn clean package
	@echo "$(GREEN)✓ JAR built: $(FLINK_JAR)$(NC)"

flink-submit: ## Submit Flink agent (usage: make flink-submit AGENT=CallProcessingAgent)
	@if [ -z "$(AGENT)" ]; then \
		echo "$(RED)Error: AGENT not specified$(NC)"; \
		echo "Usage: make flink-submit AGENT=CallProcessingAgent"; \
		echo "Available agents: CallProcessingAgent, DataEnrichmentAgent, RiskScoringAgent, AlertGenerationAgent"; \
		exit 1; \
	fi
	@if [ ! -f "$(FLINK_JAR)" ]; then \
		echo "$(YELLOW)JAR not found, building...$(NC)"; \
		$(MAKE) flink-build; \
	fi
	@echo "$(CYAN)Copying JAR to Flink container...$(NC)"
	docker cp $(FLINK_JAR) $(FLINK_JM):/opt/flink/lib/
	@echo "$(CYAN)Submitting $(AGENT)...$(NC)"
	docker exec $(FLINK_JM) /opt/flink/bin/flink run \
		--class com.sentineldispatch.agents.$(AGENT) \
		/opt/flink/lib/flink-agents-1.0.0.jar
	@echo "$(GREEN)✓ Agent submitted$(NC)"

flink-jobs: ## List running Flink jobs
	docker exec $(FLINK_JM) /opt/flink/bin/flink list

flink-cancel: ## Cancel a Flink job (usage: make flink-cancel JOB_ID=<job-id>)
	@if [ -z "$(JOB_ID)" ]; then \
		echo "$(RED)Error: JOB_ID not specified$(NC)"; \
		echo "Usage: make flink-cancel JOB_ID=<job-id>"; \
		echo "Run 'make flink-jobs' to see job IDs"; \
		exit 1; \
	fi
	docker exec $(FLINK_JM) /opt/flink/bin/flink cancel $(JOB_ID)
	@echo "$(GREEN)✓ Job cancelled$(NC)"

# Testing
test: ## Run Python tests
	@echo "$(CYAN)Running Python tests...$(NC)"
	python -m pytest tests/ -v
	@echo "$(GREEN)✓ Tests complete$(NC)"

test-unit: ## Run unit tests only
	python -m pytest tests/ -v -k "not integration"

test-integration: ## Run integration tests only
	python -m pytest tests/integration_tests/ -v

test-api: ## Run API tests
	python -m pytest tests/api/ -v

lint: ## Run Python linters
	@echo "$(CYAN)Running linters...$(NC)"
	@if command -v ruff > /dev/null; then \
		ruff check sentinel_dispatch/ tests/; \
	else \
		echo "$(YELLOW)ruff not installed, skipping...$(NC)"; \
	fi
	@if command -v mypy > /dev/null; then \
		mypy sentinel_dispatch/ --ignore-missing-imports; \
	else \
		echo "$(YELLOW)mypy not installed, skipping...$(NC)"; \
	fi

format: ## Format Python code
	@echo "$(CYAN)Formatting code...$(NC)"
	@if command -v ruff > /dev/null; then \
		ruff format sentinel_dispatch/ tests/; \
		echo "$(GREEN)✓ Code formatted$(NC)"; \
	else \
		echo "$(YELLOW)ruff not installed$(NC)"; \
	fi

# Frontend Commands
frontend-dev: ## Start frontend development server
	@echo "$(CYAN)Starting frontend dev server...$(NC)"
	cd frontend && npm run dev

frontend-stop: ## Stop frontend development server
	@echo "$(CYAN)Stopping frontend dev server...$(NC)"
	@pkill -f "npm run dev" || pkill -f "next dev" || true
	@if pgrep -f "npm run dev" > /dev/null || pgrep -f "next dev" > /dev/null; then \
		echo "$(YELLOW)⚠️  Frontend dev server may still be running$(NC)"; \
	else \
		echo "$(GREEN)✓ Frontend dev server stopped$(NC)"; \
	fi

frontend-build: ## Build frontend for production
	@echo "$(CYAN)Building frontend...$(NC)"
	cd frontend && npm run build
	@echo "$(GREEN)✓ Frontend built$(NC)"

frontend-type-check: ## Type check frontend TypeScript
	cd frontend && npm run type-check

# API Development
api-dev: ## Run API locally (not in Docker)
	@echo "$(CYAN)Starting API in development mode...$(NC)"
	python scripts/run_api.py

api-test: ## Test API endpoints
	@echo "$(CYAN)Testing API endpoints...$(NC)"
	bash tests/integration_tests/test_api_endpoints.sh

# Data Ingestion
ingest-calls: ## Ingest emergency calls from file (usage: make ingest-calls CALLS_FILE=path/to/file.json)
	@if [ -z "$(CALLS_FILE)" ]; then \
		echo "$(RED)Error: CALLS_FILE not specified$(NC)"; \
		echo "Usage: make ingest-calls CALLS_FILE=path/to/calls.json"; \
		echo "Example: make ingest-calls CALLS_FILE=data/generated_calls/maui_calls.json"; \
		exit 1; \
	fi
	@if [ ! -f "$(CALLS_FILE)" ]; then \
		echo "$(RED)Error: File not found: $(CALLS_FILE)$(NC)"; \
		exit 1; \
	fi
	@echo "$(CYAN)Ingesting emergency calls from $(CALLS_FILE)...$(NC)"
	@SENTINEL_MODE=development uv run python -m sentinel_dispatch.services.ingest_calls --file "$(CALLS_FILE)"
	@echo "$(GREEN)✓ Emergency calls ingested$(NC)"

ingest-weather: ## Run weather ingestion once
	@echo "$(CYAN)Ingesting weather data...$(NC)"
	@SENTINEL_MODE=development uv run python -m sentinel_dispatch.services.ingest_weather --once
	@echo "$(GREEN)✓ Weather data ingested$(NC)"

ingest-fire: ## Run fire ingestion once
	@echo "$(CYAN)Ingesting fire data...$(NC)"
	@SENTINEL_MODE=development uv run python -m sentinel_dispatch.services.ingest_fire --once
	@echo "$(GREEN)✓ Fire data ingested$(NC)"

ingest-dev: ingest-weather ingest-fire ## Run fire and weather ingestion once, optionally ingest calls (usage: make ingest-dev [CALLS_FILE=path/to/file.json])
	@if [ -n "$(CALLS_FILE)" ]; then \
		echo ""; \
		echo "$(CYAN)Ingesting calls...$(NC)"; \
		$(MAKE) ingest-calls CALLS_FILE="$(CALLS_FILE)"; \
	fi
	@echo ""
	@echo "$(GREEN)✓ Ingestion complete$(NC)"

# Utility Commands
status: ## Show status of all containers
	@echo "$(CYAN)Container Status:$(NC)"
	docker-compose -f $(COMPOSE_FILE) ps
	@echo ""
	@echo "$(CYAN)Service URLs:$(NC)"
	@echo "  Kafka: localhost:9092"
	@echo "  Kafka UI: http://localhost:8080"
	@echo "  Flink Dashboard: http://localhost:8081"
	@echo "  ML API: http://localhost:8000"
	@echo "  API Docs: http://localhost:8000/docs"
	@echo "  WebSocket: ws://localhost:8000/ws"

health: ## Check health of all services
	@echo "$(CYAN)Checking service health...$(NC)"
	@echo ""
	@echo "$(CYAN)ML API Service:$(NC)"
	@curl -s http://localhost:8000/health | python -m json.tool || echo "$(RED)✗ Not responding$(NC)"
	@echo ""
	@echo "$(CYAN)Flink JobManager:$(NC)"
	@curl -s http://localhost:8081/overview | python -m json.tool 2>/dev/null || echo "$(RED)✗ Not responding$(NC)"
	@echo ""
	@echo "$(CYAN)Kafka:$(NC)"
	@docker exec sentinel-dispatch-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1 && echo "$(GREEN)✓ Healthy$(NC)" || echo "$(RED)✗ Not responding$(NC)"

# Installation Commands
check-prereqs: ## Check if all prerequisites are installed
	@echo "$(CYAN)Checking prerequisites...$(NC)"
	@missing=0; \
	if ! command -v docker &> /dev/null; then \
		echo "$(RED)✗ Docker is not installed$(NC)"; \
		missing=1; \
	else \
		echo "$(GREEN)✓ Docker$(NC)"; \
	fi; \
	if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null 2>&1; then \
		echo "$(RED)✗ Docker Compose is not installed$(NC)"; \
		missing=1; \
	else \
		echo "$(GREEN)✓ Docker Compose$(NC)"; \
	fi; \
	if ! command -v uv &> /dev/null; then \
		echo "$(YELLOW)⚠ uv is not installed (will install if needed)$(NC)"; \
	else \
		echo "$(GREEN)✓ uv$(NC)"; \
	fi; \
	if ! command -v node &> /dev/null; then \
		echo "$(RED)✗ Node.js is not installed$(NC)"; \
		missing=1; \
	else \
		echo "$(GREEN)✓ Node.js$(NC)"; \
	fi; \
	if ! command -v mvn &> /dev/null; then \
		echo "$(YELLOW)⚠ Maven is not installed (needed for Flink agents)$(NC)"; \
	else \
		echo "$(GREEN)✓ Maven$(NC)"; \
	fi; \
	if [ $$missing -eq 1 ]; then \
		echo "$(RED)Some prerequisites are missing. Please install them first.$(NC)"; \
		exit 1; \
	fi; \
	echo "$(GREEN)✓ All required prerequisites are installed$(NC)"

install-uv: ## Install uv if not already installed
	@if ! command -v uv &> /dev/null; then \
		echo "$(CYAN)Installing uv...$(NC)"; \
		curl -LsSf https://astral.sh/uv/install.sh | sh; \
		echo "$(GREEN)✓ uv installed$(NC)"; \
	else \
		echo "$(GREEN)✓ uv already installed$(NC)"; \
	fi

setup-config: ## Create config directory and development config file
	@echo "$(CYAN)Setting up config...$(NC)"
	@mkdir -p config
	@if [ ! -f "config/config.development.json" ]; then \
		echo "$(CYAN)Creating config/config.development.json from template...$(NC)"; \
		cp config.example.json config/config.development.json; \
		echo "$(GREEN)✓ Config file created$(NC)"; \
		echo "$(YELLOW)⚠️  Set the following environment variables:$(NC)"; \
		echo "   - GEMINI_API_KEY (required for call generation)"; \
		echo "   - GOOGLE_MAPS_API_KEY (required for frontend maps)"; \
		echo "   - FIRMS_API_KEY (optional)"; \
	else \
		echo "$(GREEN)✓ config/config.development.json already exists$(NC)"; \
	fi

install-python: install-uv setup-config ## Install Python dependencies
	@echo "$(CYAN)Installing Python dependencies...$(NC)"
	uv sync --active
	@echo "$(GREEN)✓ Python dependencies installed$(NC)"

install-frontend: ## Install frontend dependencies
	@echo "$(CYAN)Installing frontend dependencies...$(NC)"
	cd frontend && npm install
	@echo "$(GREEN)✓ Frontend dependencies installed$(NC)"

install: check-prereqs install-python install-frontend ## Install all dependencies (Python and frontend)
	@echo "$(GREEN)✓ All dependencies installed$(NC)"

# Quick Start
dev: up ## Quick start: build and start all services
	@echo "$(GREEN)✓ Development environment ready!$(NC)"
	@echo ""
	@echo "$(CYAN)Next steps:$(NC)"
	@echo "  1. Build Flink agents: $(CYAN)make flink-build$(NC)"
	@echo "  2. Submit agents: $(CYAN)make flink-submit AGENT=CallProcessingAgent$(NC)"
	@echo "  3. View logs: $(CYAN)make logs$(NC)"
	@echo "  4. Check status: $(CYAN)make status$(NC)"

setup: install up ## Complete setup: install dependencies, start containers, and start frontend dev server
	@echo "$(CYAN)Starting frontend dev server in background...$(NC)"
	@$(MAKE) frontend-dev > /dev/null 2>&1 &
	@echo "$(GREEN)✓ Frontend dev server started in background$(NC)"
	@echo ""
	@echo "$(GREEN)✓ Development environment setup complete!$(NC)"
	@echo ""
	@echo "$(CYAN)Next steps:$(NC)"
	@echo "  1. Build Flink agents: $(CYAN)make flink-build$(NC)"
	@echo "  2. Submit agents: $(CYAN)make flink-submit AGENT=CallProcessingAgent$(NC)"
	@echo ""
	@echo "$(CYAN)Service URLs:$(NC)"
	@echo "  - Kafka: localhost:9092"
	@echo "  - Kafka UI: http://localhost:8080"
	@echo "  - Flink: http://localhost:8081"
	@echo "  - ML API: http://localhost:8000"
	@echo "  - API Docs: http://localhost:8000/docs"
	@echo "  - Frontend: http://localhost:3000"

# Default target
.DEFAULT_GOAL := help

