# Data Ingestion Services

This directory contains the data ingestion services for Phase 1 of the Sentinel Dispatch project.

**Note**: These services are part of the `sentinel_dispatch` package and should be run using the module syntax.

## Centralized Ingestion Service

**Recommended**: Use the centralized `ingest.py` service to run all ingestion services sequentially:

```bash
# Set mode via environment variable
export SENTINEL_MODE=development

# Run all ingestion services (weather + fire, calls optional)
uv run python -m sentinel_dispatch.services.ingest

# With calls file
uv run python -m sentinel_dispatch.services.ingest --calls-file path/to/calls.json

# Continuous mode (runs weather and fire every 15 minutes)
uv run python -m sentinel_dispatch.services.ingest --continuous

# Run only specific services
uv run python -m sentinel_dispatch.services.ingest --weather-only
uv run python -m sentinel_dispatch.services.ingest --fire-only
uv run python -m sentinel_dispatch.services.ingest --calls-only --calls-file path/to/calls.json
```

## Individual Services

You can also run individual services separately if needed:

## Services

### 1. `ingest_calls.py` - 911 Call Ingestion

Ingests emergency calls from JSON files and publishes them to the `emergency-calls` Kafka topic.

**Usage:**
```bash
# Set mode via environment variable
export SENTINEL_MODE=development

# Ingest calls from a JSON file
uv run python -m sentinel_dispatch.services.ingest_calls --file path/to/calls.json

# The JSON file should contain calls in this format:
# [
#   {
#     "callId": "call-123",
#     "timestamp": "2023-08-08T10:30:00Z",
#     "callerLocation": {"lat": 20.90, "lon": -156.68},
#     "transcript": "There's smoke everywhere, I can't breathe!",
#     "metadata": {
#       "phoneNumber": "+1-808-555-1234",
#       "callDuration": 45
#     }
#   }
# ]
```

**Options:**
- `--file`: Path to JSON file containing calls to ingest
- `--topic`: Kafka topic name (default: `emergency-calls`)

**Environment Variables:**
- `SENTINEL_MODE`: Mode to run in (`development` or `production`, default: `development`)

### 2. `ingest_weather.py` - Weather Data Ingestion

Fetches weather data from NOAA API and publishes to the `weather-data` Kafka topic.

**Usage:**
```bash
# Set mode via environment variable
export SENTINEL_MODE=development

# Fetch weather data once
uv run python -m sentinel_dispatch.services.ingest_weather --once

# Continuous polling (every 15 minutes)
uv run python -m sentinel_dispatch.services.ingest_weather --interval 900

# For a specific location
uv run python -m sentinel_dispatch.services.ingest_weather --lat 20.90 --lon -156.68 --once
```

**Options:**
- `--topic`: Kafka topic name (default: `weather-data`)
- `--lat`: Latitude for weather data (default: Maui island bounding box)
- `--lon`: Longitude for weather data (default: Maui island bounding box)
- `--interval`: Polling interval in seconds (default: 900 = 15 minutes)
- `--once`: Fetch once and exit (default: continuous polling)

### 3. `ingest_fire.py` - Fire Data Ingestion

Fetches fire detection data from FIRMS API (NASA) and publishes to the `fire-data` Kafka topic.

**Usage:**
```bash
# Set mode via environment variable
export SENTINEL_MODE=development

# Fetch fire data once
uv run python -m sentinel_dispatch.services.ingest_fire --once

# Continuous polling (every hour)
uv run python -m sentinel_dispatch.services.ingest_fire --interval 3600
```

**Options:**
- `--topic`: Kafka topic name (default: `fire-data`)
- `--interval`: Polling interval in seconds (default: 3600 = 1 hour)
- `--once`: Fetch once and exit (default: continuous polling)

## Prerequisites

1. **Kafka must be running:**
   ```bash
   docker-compose -f docker-compose.dev.yml up -d
   ```

2. **Configuration file must exist:**
   ```bash
   cp config.example.json config/config.development.json
   # Edit config/config.development.json with your API keys
   ```

3. **Required API keys:**
   - NOAA API: No key required (public API)
   - FIRMS API: **MAP_KEY required** - Get a free key from https://firms.modaps.eosdis.nasa.gov/api/
   - Gemini API: Required for call generation (not for ingestion services)

## Running All Services

In development mode, you can run all ingestion services in separate terminals:

```bash
# Set mode via environment variable (or export in your shell)
export SENTINEL_MODE=development

# Terminal 1: Weather data (continuous)
uv run python -m sentinel_dispatch.services.ingest_weather

# Terminal 2: Fire data (continuous)
uv run python -m sentinel_dispatch.services.ingest_fire

# Terminal 3: Ingest calls (one-time, when you have call data)
uv run python -m sentinel_dispatch.services.ingest_calls --file path/to/calls.json
```

## Data Schemas

### Emergency Calls (`emergency-calls` topic)
```json
{
  "callId": "string",
  "timestamp": "datetime (ISO 8601)",
  "callerLocation": {"lat": "float", "lon": "float"},
  "transcript": "string",
  "metadata": {
    "phoneNumber": "string",
    "callDuration": "int",
    "callerAge": "int (optional)",
    "knownConditions": ["string (optional)"]
  }
}
```

### Weather Data (`weather-data` topic)
```json
{
  "timestamp": "datetime (ISO 8601)",
  "location": {"lat": "float", "lon": "float"},
  "temperature": "float",
  "windSpeed": "float",
  "windDirection": "float",
  "humidity": "float",
  "airQualityIndex": "int",
  "visibility": "string"
}
```

### Fire Data (`fire-data` topic)
```json
{
  "timestamp": "datetime (ISO 8601)",
  "fireId": "string",
  "location": {"lat": "float", "lon": "float"},
  "confidence": "float",
  "brightness": "float",
  "fireType": "string",
  "perimeter": [{"lat": "float", "lon": "float"}],
  "spreadRate": "float",
  "estimatedSize": "float"
}
```

## Troubleshooting

### Kafka Connection Errors
- Ensure Kafka is running: `docker-compose -f docker-compose.dev.yml ps`
- Check bootstrap servers in config: Should be `localhost:9092` for development
- Verify topics exist: `docker exec -it sentinel-dispatch-kafka kafka-topics --list --bootstrap-server localhost:9092`

### API Errors
- **NOAA API**: Check internet connection, API may be rate-limited
- **FIRMS API**: Verify bounding box coordinates are valid
- Check logs for detailed error messages

### Configuration Errors
- Ensure `config/config.development.json` exists and is valid JSON
- Verify all required fields are present in config file

