# Dockerfile for Sentinel Dispatch FastAPI Service
FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    # GEOS library for shapely
    libgeos-dev \
    # librdkafka for confluent-kafka
    librdkafka-dev \
    # Build tools for compiling Python packages
    gcc \
    g++ \
    # curl for health checks
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy dependency files and application code (needed for package installation)
COPY pyproject.toml ./
COPY README.md* ./
COPY sentinel_dispatch/ ./sentinel_dispatch/
COPY config/ ./config/

# Install Python dependencies using pip
# Upgrade pip and install build tools first, then install from pyproject.toml
RUN pip install --no-cache-dir --upgrade pip setuptools wheel hatchling && \
    pip install --no-cache-dir -e .

# Expose port
EXPOSE 8000

# Health check (using urllib since requests may not be available)
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "from urllib.request import urlopen; urlopen('http://localhost:8000/health').read()" || exit 1

# Run the FastAPI service
# Use sh to allow environment variable substitution for log level
CMD sh -c 'uvicorn sentinel_dispatch.api.main:app --host 0.0.0.0 --port 8000 --log-level ${LOG_LEVEL:-info}'

