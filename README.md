# Sentinel Dispatch
## AI-Powered Climate-Aware Emergency Triage System

Sentinel Dispatch is a real-time emergency dispatch system that leverages AI/ML to improve triage decision-making during climate-amplified disasters, with an initial focus on wildfires.

## Overview

This system integrates multiple data streams (emergency calls, weather, fire spread) to provide dispatchers with continuously updated risk scores during rapidly evolving wildfire events.

### Project Status

🚧 **In Development** - This is a proof-of-concept implementation focusing on the Lahaina, Maui wildfire event (August 2023).

## Key Features

- **Real-Time Risk Scoring**: Continuous updates as conditions change
- **Intelligent Prioritization**: Dynamic ranking based on call urgency, climate hazard proximity, and vulnerability factors
- **Interactive Visualization**: Real-time map with overlays for fires, calls, and weather

## Architecture Overview

Sentinel Dispatch follows an event-driven microservices architecture with the following components:

### System Components

1. **Data Ingestion Services** (Python)
   - Ingest emergency calls, weather data, and fire data from external APIs
   - Publish raw data to Kafka topics

2. **Streaming Processing Pipeline** (Java Flink Agents)
   - **CallProcessingAgent**: Classifies emergency calls using Gemini NLP
   - **DataEnrichmentAgent**: Enriches calls with spatial-temporal weather and fire data
   - **RiskScoringAgent**: Calculates risk scores using hybrid ML/rule-based approach
   - **AlertGenerationAgent**: Generates alerts based on risk thresholds

3. **ML/AI Services** (Python FastAPI)
   - Call classification using Google Gemini API
   - Data enrichment with geospatial joins
   - Risk scoring with rule-based and ML models
   - Alert generation

4. **Backend API** (Python FastAPI)
   - REST API for dashboard data
   - WebSocket server for real-time updates
   - Kafka consumer for streaming agent outputs

5. **Frontend** (Next.js/React)
   - Interactive map visualization
   - Real-time dashboard
   - WebSocket client for live updates

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         Data Sources                             │
│  Emergency Calls  │  Weather (NOAA)  │  Fire Data (FIRMS)      │
└──────────┬────────┴────────┬─────────┴──────────┬──────────────┘
           │                  │                     │
           └──────────────────┼─────────────────────┘
                              │
                    ┌─────────▼──────────┐
                    │  Ingestion Layer   │
                    │   (Python)         │
                    └─────────┬──────────┘
                              │
                    ┌─────────▼──────────┐
                    │   Kafka Topics     │
                    │  (Event Streaming) │
                    └─────────┬──────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
┌───────▼────────┐   ┌────────▼────────┐   ┌───────▼────────┐
│ Flink Agents   │   │  ML/AI Services │   │  Backend API   │
│    (Java)      │──▶│   (FastAPI)     │◀──│   (FastAPI)    │
│                │   │                 │   │                │
│ • Classify     │   │ • NLP           │   │ • REST API     │
│ • Enrich       │   │ • Enrichment    │   │ • WebSocket    │
│ • Score Risk   │   │ • Risk Scoring  │   │ • Database     │
│ • Generate     │   │ • Alerts        │   │                │
│   Alerts       │   │                 │   │                │
└───────┬────────┘   └─────────────────┘   └───────┬────────┘
        │                                          │
        └──────────────────┬───────────────────────┘
                           │
                  ┌────────▼────────┐
                  │    Frontend     │
                  │  (Next.js/React)│
                  │                 │
                  │ • Interactive   │
                  │   Map           │
                  │ • Dashboard     │
                  │ • Real-time     │
                  │   Updates       │
                  └─────────────────┘
```

**Flow:**
1. Data sources → Ingestion → Kafka
2. Kafka → Flink Agents → ML/AI Services → Kafka
3. Kafka → Backend API → Database
4. Backend API → Frontend (REST + WebSocket)

### Data Flow

1. **Ingestion**: External data (calls, weather, fires) is ingested and published to Kafka topics
2. **Processing**: Flink agents consume from Kafka, call Python ML services via HTTP, and publish results back to Kafka
3. **Storage**: Backend API consumes processed data and stores it in SQLite
4. **Visualization**: Frontend connects via REST API and WebSocket for real-time updates

## Technology Stack

- **Streaming**: Apache Kafka for event streaming, Apache Flink for stream processing
- **Backend**: Python 3.10-3.12, FastAPI, SQLite
- **Frontend**: Next.js, React, TypeScript, Google Maps API
- **AI/ML**: Google Gemini API (NLP), Vertex AI (optional ML models)
- **Data Sources**: FIRMS (fire data), NOAA (weather data)
- **Infrastructure**: Docker, Docker Compose for local development

## Requirements

### Prerequisites

- **Python**: >=3.10.x,<3.13.0 (3.10, 3.11, or 3.12)
- **Node.js**: >=18.x (for frontend)
- **Docker**: >=20.x and Docker Compose (for Kafka, Flink, and services)
- **Java**: 21 (for building Flink agents)
- **Maven**: 3.6+ (for building Flink agents)

### System Dependencies

Install the following system libraries:

**macOS:**
```bash
brew install geos librdkafka
```

**Linux (Ubuntu/Debian):**
```bash
sudo apt-get update
sudo apt-get install -y libgeos-dev librdkafka-dev
```

**Linux (RHEL/CentOS):**
```bash
sudo yum install -y geos-devel librdkafka-devel
```

### Package Manager

Install [uv](https://github.com/astral-sh/uv) (fast Python package manager):

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Installation

Use the Makefile commands for easy setup:

1. **Check prerequisites:**
   ```bash
   make check-prereqs
   ```

2. **Install all dependencies:**
   ```bash
   make install
   ```
   This will:
   - Check prerequisites
   - Install uv if needed
   - Set up configuration files
   - Install Python dependencies
   - Install frontend dependencies

3. **Or install individually:**
   ```bash
   make install-python    # Install Python dependencies only
   make install-frontend  # Install frontend dependencies only
   make setup-config      # Create config files
   ```

## Quick Start

> **Note**: Currently, only **development mode** is supported. Production deployment with Confluent Cloud is planned for future releases.

### Development Mode Setup

1. **Create `.env` file** in the project root:
   ```bash
   SENTINEL_MODE=development
   # Backend API keys
   GEMINI_API_KEY=your-gemini-api-key
   GOOGLE_MAPS_API_KEY=your-google-maps-api-key
   FIRMS_API_KEY=your-firms-map-key  # Get free MAP_KEY from https://firms.modaps.eosdis.nasa.gov/api/
   
   # Frontend environment variables (must be prefixed with NEXT_PUBLIC_)
   NEXT_PUBLIC_API_URL=http://localhost:8000
   NEXT_PUBLIC_WS_URL=ws://localhost:8000/ws
   NEXT_PUBLIC_GOOGLE_MAPS_API_KEY="${GOOGLE_MAPS_API_KEY}"
   ```

2. **Activate environment variables:**
   ```bash
   source .env
   # Or export them individually:
   # export SENTINEL_MODE=development
   # export GEMINI_API_KEY=your-gemini-api-key
   # export GOOGLE_MAPS_API_KEY=your-google-maps-api-key
   # export FIRMS_API_KEY=your-firms-map-key
   # export NEXT_PUBLIC_API_URL=http://localhost:8000
   # export NEXT_PUBLIC_WS_URL=ws://localhost:8000/ws
   # export NEXT_PUBLIC_GOOGLE_MAPS_API_KEY="${GOOGLE_MAPS_API_KEY}"
   ```

3. **Install dependencies and start all services:**
   ```bash
   make setup
   ```
   This will:
   - Check prerequisites
   - Install Python and frontend dependencies
   - Start all containers (Kafka, Flink, ML API Service)
   - Start frontend dev server
   
   Services available at:
   - Kafka: localhost:9092
   - Kafka UI: http://localhost:8080
   - Flink Dashboard: http://localhost:8081
   - ML API: http://localhost:8000
   - API Docs: http://localhost:8000/docs
   - Frontend: http://localhost:3000

4. **Build Flink agents:**
   ```bash
   make flink-build
   ```

5. **Submit Flink agents** (in order):
   ```bash
   make flink-submit AGENT=CallProcessingAgent
   make flink-submit AGENT=DataEnrichmentAgent
   make flink-submit AGENT=RiskScoringAgent
   make flink-submit AGENT=AlertGenerationAgent
   ```

6. **Generate synthetic emergency call data:**
   ```bash
   uv run python scripts/generate_911_calls.py --num-calls 50
   ```
   
   This generates realistic emergency call transcripts using Google Gemini API based on the August 8-9, 2023 Maui wildfire event. The synthetic data includes:
   - Realistic call transcripts with natural speech patterns
   - Various scenario types (medical emergencies, fire proximity, evacuations, etc.)
   - Location data from Lahaina and other Maui areas
   - Timestamps distributed across the event timeline
   - Vulnerability factors (age, medical conditions, mobility)
   
   **Why synthetic data?** Since we don't have access to real emergency call data for privacy and security reasons, we generate realistic synthetic data that mimics the characteristics of actual emergency calls during a wildfire event. This allows us to test and demonstrate the system's capabilities safely.

7. **Ingest test data:**
   ```bash
   make ingest-dev CALLS_FILE=data/generated_calls/maui_calls_*.json
   ```
   Note: Replace `maui_calls_*.json` with the actual generated filename, or use the latest file in `data/generated_calls/`.

### Useful Makefile Commands

- `make help` - Show all available commands
- `make status` - Check service status
- `make logs` - View all container logs
- `make api-logs` - View API service logs
- `make kafka-topics` - List Kafka topics
- `make flink-jobs` - List running Flink jobs
- `make test` - Run Python tests
- `make lint` - Run linters
- `make format` - Format code

See `make help` for the complete list of commands.

## Future Enhancements

The following enhancements are planned for future releases:

### Production Deployment
- **Confluent Cloud Integration**: Migrate from local Kafka/Flink to Confluent Cloud for production-grade streaming infrastructure
- **Cloud Deployment**: Deploy services to cloud platforms (GCP, AWS, Azure) with auto-scaling and high availability
- **Container Orchestration**: Kubernetes deployment with Helm charts for production environments

### Enhanced AI/ML Capabilities
- **Advanced ML Models**: Integrate Vertex AI for more sophisticated risk prediction models
- **Multi-Model Ensemble**: Combine multiple ML models for improved accuracy and robustness
- **Continuous Learning**: Implement model retraining pipelines based on historical data and outcomes
- **Predictive Analytics**: Forecast fire spread and resource needs using historical patterns

### Expanded Data Sources
- **Additional Weather APIs**: Integrate more weather data sources for comprehensive coverage
- **Satellite Imagery**: Real-time satellite data for fire detection and monitoring
- **Social Media Integration**: Monitor social media for early warning signals and situational awareness
- **Traffic Data**: Real-time traffic patterns to optimize evacuation routes
- **Infrastructure Data**: Power grid, water systems, and communication network status

### Multi-Hazard Support
- **Extended Hazard Types**: Support for hurricanes, floods, earthquakes, and other climate disasters
- **Hazard-Specific Models**: Specialized risk scoring models for different disaster types
- **Cross-Hazard Analysis**: Identify cascading effects and compound risks

### Advanced Features
- **Resource Optimization**: AI-powered resource allocation and dispatch recommendations
- **Evacuation Planning**: Automated evacuation route planning and optimization
- **Communication Integration**: Integration with emergency communication systems (EAS, IPAWS)
- **Historical Analysis**: Deep analytics and reporting on past events for learning and improvement
- **Real-Time Collaboration**: Multi-user collaboration features for dispatch centers

### Performance & Scalability
- **Horizontal Scaling**: Support for processing thousands of concurrent calls
- **Edge Computing**: Deploy processing closer to data sources for reduced latency
- **Caching Layer**: Implement Redis or similar for frequently accessed data
- **Database Optimization**: Migrate to PostgreSQL or similar for better performance at scale

### Security & Compliance
- **Enhanced Security**: End-to-end encryption, role-based access control, audit logging
- **HIPAA Compliance**: Ensure compliance with healthcare data regulations
- **Data Privacy**: Enhanced privacy controls and data anonymization
- **Disaster Recovery**: Comprehensive backup and disaster recovery procedures

## License

This project is provided for **non-commercial use only**. 

**Commercial use is prohibited** without explicit written permission from the project maintainers. This includes, but is not limited to:

- Using this software in any commercial product or service
- Integrating this software into commercial applications
- Providing services based on this software for commercial gain
- Reselling or redistributing this software for commercial purposes

For commercial licensing inquiries, please contact the project maintainers.

**Non-commercial use** includes:
- Educational and research purposes
- Personal projects and learning
- Open source contributions and development
- Non-profit organizations (subject to approval)

This license restriction is in place to protect the project's development and ensure appropriate use of emergency dispatch technology.
