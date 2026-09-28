# Sentinel Dispatch AI/ML Services API

FastAPI REST API for AI/ML services used by Streaming Agents and the application backend.

## Services

### 1. Call Classification Service
**Endpoint**: `POST /api/classify-call`

Classifies emergency call transcripts using Gemini NLP.

**Request**:
```json
{
  "transcript": "I need help! The fire is getting close to my house...",
  "call_id": "CALL-001",
  "metadata": {}
}
```

**Response**:
```json
{
  "call_id": "CALL-001",
  "classification": {
    "urgency": 5,
    "incident_type": "fire",
    "entities": {...},
    "sentiment": 0.9,
    "confidence": 0.95
  },
  "timestamp": "2024-08-08T14:23:45Z"
}
```

### 2. Data Enrichment Service
**Endpoint**: `POST /api/enrich-call`

Enriches processed calls with weather and fire data using spatial-temporal joins.

**Request**:
```json
{
  "processed_call": {
    "call_id": "CALL-001",
    "timestamp": "2024-08-08T14:23:45Z",
    "caller_location": {"lat": 20.8783, "lon": -156.6825},
    "transcript": "...",
    "classification": {...}
  },
  "weather_data_list": [...],
  "fire_data_list": [...]
}
```

**Response**:
```json
{
  "enriched_call": {
    "call_id": "CALL-001",
    "fire_analysis": {...},
    "weather_enrichment": {...}
  },
  "timestamp": "2024-08-08T14:23:50Z"
}
```

### 3. Risk Scoring Service
**Endpoint**: `POST /api/score-risk`

Calculates risk scores using hybrid approach (Vertex AI or rule-based).

**Request**:
```json
{
  "call_id": "CALL-001",
  "call_timestamp": "2024-08-08T14:23:45Z",
  "caller_location": {"lat": 20.8783, "lon": -156.6825},
  "call_classification": {...},
  "nearest_fire": {...},
  "weather_data": {...},
  "vulnerability_factors": {...}
}
```

**Response**:
```json
{
  "risk_score": {
    "call_id": "CALL-001",
    "risk_score": 85.0,
    "confidence": 0.95,
    "factors": {...},
    "recommendation": "IMMEDIATE_DISPATCH",
    "priority": 1
  },
  "timestamp": "2024-08-08T14:23:50Z"
}
```

### 4. Alert Generation Service
**Endpoint**: `POST /api/generate-alert`

Generates alerts based on risk scores and contextual data.

**Request**:
```json
{
  "risk_score": {...},
  "enriched_call": {...}
}
```

**Response**:
```json
{
  "alerts": [
    {
      "alertId": "...",
      "severity": "critical",
      "message": "...",
      "alert_type": "risk_threshold"
    }
  ],
  "alert_count": 1,
  "timestamp": "2024-08-08T14:23:50Z"
}
```

## Running the API

### Development Mode

```bash
# Using uvicorn directly
uv run uvicorn sentinel_dispatch.api.main:app --reload --host 0.0.0.0 --port 8000

# Or using Python
uv run python -m sentinel_dispatch.api.main
```

### API Documentation

Once running, access:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/openapi.json

## Health Checks

Each service has a health check endpoint:
- `GET /api/classify-call/health`
- `GET /api/enrich-call/health`
- `GET /api/score-risk/health`
- `GET /api/generate-alert/health`

## Error Handling

All endpoints return standardized error responses:

```json
{
  "error": "Error message",
  "detail": "Detailed error information",
  "timestamp": "2024-08-08T14:23:45Z"
}
```

## Integration with Streaming Agents

These services are designed to be called by Flink Streaming Agents via HTTP Async I/O (External Service Pattern). See [EXTERNAL_SERVICE_PATTERN.md](../documentation/EXTERNAL_SERVICE_PATTERN.md) for details.

## Dashboard API

The API includes REST endpoints for the web dashboard that consume data from Streaming Agent output topics via a Kafka consumer.

### Kafka Consumer

The API automatically starts a Kafka consumer on startup that consumes from the following Streaming Agent output topics:

- **processed-calls**: Processed emergency calls with classification results
- **enriched-calls**: Enriched calls with weather and fire data
- **risk-scores**: Risk score calculations
- **alerts**: Generated alerts

The consumer populates in-memory data stores that are used by the dashboard REST endpoints:

- `GET /api/calls` - List all emergency calls
- `GET /api/calls/{call_id}` - Get call details
- `GET /api/risk-scores` - Get risk scores
- `GET /api/prioritized-cases` - Get prioritized cases for triage
- `GET /api/alerts` - Get active alerts
- `POST /api/alerts/{alert_id}/acknowledge` - Acknowledge an alert

### Configuration

The Kafka consumer is configured via environment variables:

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers (default: `localhost:9092`)

Example:
```bash
export KAFKA_BOOTSTRAP_SERVERS=kafka:29092  # For Docker
# or
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092  # For local development
```

## WebSocket Support

The API includes WebSocket support for real-time updates to the dashboard.

### WebSocket Endpoint

**Endpoint**: `ws://localhost:8000/ws`

### Message Types

The server sends the following message types to connected clients:

- **`risk_score_update`**: Risk score updates for emergency calls
- **`new_call`**: New emergency calls (from processed-calls and enriched-calls topics)
- **`alert`**: Alert notifications
- **`fire_update`**: Fire data updates (future enhancement)
- **`weather_update`**: Weather data updates (future enhancement)

### Client Commands

Clients can send the following commands:

- **`acknowledge_alert`**: Acknowledge an alert
  ```json
  {
    "type": "acknowledge_alert",
    "alert_id": "ALERT-001"
  }
  ```

- **`ping`**: Connection health check (server responds with `pong`)
  ```json
  {
    "type": "ping",
    "timestamp": "2024-08-08T14:23:45Z"
  }
  ```

### Message Format

All messages follow this structure:

```json
{
  "type": "risk_score_update",
  "timestamp": "2024-08-08T14:23:50Z",
  "data": {
    "callId": "CALL-001",
    "riskScore": 85.0,
    "priority": 5,
    ...
  }
}
```

### Real-Time Updates

When the Kafka consumer receives messages from Streaming Agent output topics, it automatically:
1. Updates the in-memory data stores
2. Broadcasts the update to all connected WebSocket clients

This ensures that the dashboard receives real-time updates as soon as data arrives from the streaming agents.

### Connection Status

Check WebSocket connection status:
- `GET /ws/status` - Returns the number of active WebSocket connections

### Future Enhancements

**Persistent Database Storage**: Currently, the dashboard API uses in-memory data stores populated by the Kafka consumer. Future enhancements will include:

- **Database Integration**: Store processed calls, risk scores, and alerts in a persistent database (e.g., PostgreSQL, MongoDB) for:
  - Historical data retention and analysis
  - Improved reliability (data survives API restarts)
  - Query capabilities for complex filtering and aggregation
  - Support for large-scale deployments

- **Caching Layer**: Add Redis or similar caching layer for frequently accessed data to improve response times

- **Data Archival**: Implement archival strategies for old data to maintain performance while preserving historical records

The in-memory approach is suitable for development and small-scale deployments, but a persistent database will be essential for production use.

