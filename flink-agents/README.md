# Flink Agents (Java)

Java Flink streaming agents for Sentinel Dispatch. These agents call Python FastAPI services via HTTP using Flink's Async I/O.

## Architecture

```
Kafka Topic → Flink Agent (Java) → HTTP → FastAPI Service (Python) → Response → Kafka Topic
```

## Agents

1. **CallProcessingAgent** - Classifies emergency calls
   - Input: `emergency-calls`
   - Service: `/api/classify-call`
   - Output: `processed-calls`

2. **DataEnrichmentAgent** - Enriches calls with weather/fire data
   - Input: `processed-calls`
   - Service: `/api/enrich-call`
   - Output: `enriched-calls`

3. **RiskScoringAgent** - Calculates risk scores
   - Input: `enriched-calls`
   - Service: `/api/score-risk`
   - Output: `risk-scores`

4. **AlertGenerationAgent** - Generates alerts
   - Input: `risk-scores`
   - Service: `/api/generate-alert`
   - Output: `alerts`

## Prerequisites

- **Java 21** (required for Flink 2.1.1)
- **Maven 3.6+** (for building)
- **Flink 2.1.1** (local installation or Docker)
- **Kafka** (running locally or in Docker)
- **FastAPI Services** (Python services must be running)

## Quick Start

### 1. Build the JAR

```bash
cd flink-agents
mvn clean package
```

This creates `target/flink-agents-1.0.0.jar`

### 2. Start Services

```bash
# Start FastAPI service (in another terminal)
python scripts/run_api.py

# Start Kafka and Flink (if using Docker)
docker-compose -f docker-compose.dev.yml up -d kafka flink-jobmanager flink-taskmanager
```

### 3. Submit Agent to Flink

**Local Flink:**
```bash
export FLINK_HOME=~/flink-2.1.1
$FLINK_HOME/bin/flink run \
  --class com.sentineldispatch.agents.CallProcessingAgent \
  target/flink-agents-1.0.0.jar
```

**Docker Flink:**
```bash
docker cp target/flink-agents-1.0.0.jar sentinel-dispatch-flink-jobmanager:/opt/flink/lib/
docker exec sentinel-dispatch-flink-jobmanager \
  /opt/flink/bin/flink run \
  --class com.sentineldispatch.agents.CallProcessingAgent \
  /opt/flink/lib/flink-agents-1.0.0.jar
```

**Or use the helper script:**
```bash
./scripts/submit_java_agent.sh call-processing
```

## Configuration

Agents can be configured via environment variables:

```bash
# Kafka configuration
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Service URLs
export CLASSIFY_SERVICE_URL=http://localhost:8000/api/classify-call
export ENRICH_SERVICE_URL=http://localhost:8000/api/enrich-call
export SCORE_SERVICE_URL=http://localhost:8000/api/score-risk
export ALERT_SERVICE_URL=http://localhost:8000/api/generate-alert
```

## Local Testing

### Test with Local Flink

1. **Start local Flink cluster:**
   ```bash
   export FLINK_HOME=~/flink-2.1.1
   $FLINK_HOME/bin/start-cluster.sh
   ```

2. **Build and submit:**
   ```bash
   mvn clean package
   $FLINK_HOME/bin/flink run \
     --class com.sentineldispatch.agents.CallProcessingAgent \
     target/flink-agents-1.0.0.jar
   ```

3. **Check Flink dashboard:**
   - http://localhost:8081

### Test with Docker Flink

1. **Start services:**
   ```bash
   docker-compose -f docker-compose.dev.yml up -d
   ```

2. **Build and submit:**
   ```bash
   mvn clean package
   ./scripts/submit_java_agent.sh call-processing
   ```

## Development

### Project Structure

```
flink-agents/
  src/main/java/com/sentineldispatch/agents/
    CallProcessingAgent.java      # Main agent class
    DataEnrichmentAgent.java      # (to be implemented)
    RiskScoringAgent.java         # (to be implemented)
    AlertGenerationAgent.java     # (to be implemented)
    common/
      HttpAsyncFunction.java      # Base class for HTTP calls
  pom.xml                         # Maven configuration
```

### Adding a New Agent

1. Create agent class extending the pattern:
   ```java
   public class MyAgent {
       public static void main(String[] args) {
           // Setup Flink environment
           // Create Kafka source
           // Process with AsyncDataStream
           // Create Kafka sink
           // Execute
       }
   }
   ```

2. Create async function:
   ```java
   private static class MyAsyncFunction extends HttpAsyncFunction<String, String> {
       // Implement buildRequestBody, parseResponse, handleError
   }
   ```

3. Update `pom.xml` main class if needed

4. Build and test

## Troubleshooting

### Service Not Available

**Error:** `Connection refused` or `timeout`

**Solution:**
- Ensure FastAPI service is running: `python scripts/run_api.py`
- Check service URL: `curl http://localhost:8000/health`
- For Docker Flink, use `host.docker.internal:8000` instead of `localhost:8000`

### Kafka Connection Failed

**Error:** `Failed to connect to Kafka`

**Solution:**
- Ensure Kafka is running
- Check `KAFKA_BOOTSTRAP_SERVERS` environment variable
- For Docker Flink, use `kafka:9092` instead of `localhost:9092`

### Class Not Found

**Error:** `ClassNotFoundException`

**Solution:**
- Ensure JAR is built: `mvn clean package`
- Check JAR includes dependencies (shaded JAR)
- Verify class name in `flink run --class` command

## Performance Tuning

### Parallelism

Increase parallelism for higher throughput:

```java
env.setParallelism(4); // Process 4 calls in parallel
```

### Async I/O Capacity

Increase capacity for more concurrent HTTP requests:

```java
AsyncDataStream.unorderedWait(
    calls,
    new ClassifyCallAsyncFunction(SERVICE_URL),
    30, TimeUnit.SECONDS,
    200 // Increase from 100 to 200
);
```

### Timeout Configuration

Adjust timeout based on service response time:

```java
new ClassifyCallAsyncFunction(SERVICE_URL, 60) // 60 second timeout
```

## See Also

- [External Service Pattern](../documentation/EXTERNAL_SERVICE_PATTERN.md)
- [Local to Confluent Cloud](../documentation/LOCAL_TO_CONFLUENT_CLOUD.md)

