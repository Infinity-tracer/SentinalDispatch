# RV College of Engineering®
**Mysore Road, RV Vidyaniketan Post,**  
**Bengaluru - 560059, Karnataka, India**

---

## Department of Artificial Intelligence and Machine Learning

**Course Code:** AI372IA  
**Course:** Stream Processing and Analytics

---

# Project Synopsis

| | |
|---|---|
| **Project Title** | **Sentinel Dispatch: AI-Powered Climate-Aware Emergency Triage System with Real-Time Stream Processing for Wildfire Response** |
| **Student Name/s** | SRIRAM VELUMURI, VIJAYKUMAR BK, SIRI KUMAR CS |
| **Student USN/s** | 1RV23AI117, 1RV23AI118, 1RV23AI102 |

---

## Introduction and Relevance of the Project

Climate-amplified disasters, particularly wildfires, represent one of the most rapidly evolving emergency scenarios, with conditions changing minute-by-minute as fires spread, weather patterns shift, and vulnerable populations face compounding risks. In the United States alone, wildfires cause over 3,000 deaths annually and billions in property damage, with response effectiveness critically dependent on real-time situational awareness and intelligent resource allocation. Traditional emergency dispatch systems operate on static prioritization models that fail to adapt as environmental conditions evolve, lack integration with live climate data streams, and provide no mechanism for continuous risk reassessment during rapidly changing disaster scenarios. From a stream processing standpoint, emergency response generates heterogeneous, high-velocity data streams — emergency call transcripts arriving unpredictably, weather observations updating every 5-15 minutes, and satellite fire detections publishing every few hours — each carrying time-sensitive information that must be correlated spatially and temporally to produce actionable intelligence. The complexity is further amplified by the need for sub-second latency in risk assessment, exactly-once processing semantics to prevent duplicate alerts, and stateful stream joins that maintain context across multiple evolving data sources. This project is motivated by the critical gap between traditional reactive emergency response and the proactive, data-driven, climate-aware triage systems that modern disaster scenarios demand, leveraging real-time stream processing to transform emergency dispatch from a manual, call-by-call operation into an intelligent, continuously adaptive risk assessment pipeline.

---

## Problem Statement and Objectives

Existing emergency dispatch systems face fundamental architectural limitations when responding to climate disasters: they process emergency calls in isolation without environmental context, lack integration with real-time weather and fire propagation data, provide no mechanism for dynamic risk recalculation as conditions change, and offer dispatchers no predictive intelligence about which incidents face compounding hazards. When a wildfire event unfolds, call centers receive hundreds of distress calls but have no automated way to identify which callers are simultaneously facing fire proximity, extreme weather conditions, and vulnerability factors such as age or medical dependencies. Pure rule-based triage systems fail to capture the semantic complexity of natural language emergency calls, while purely ML-based approaches lack the interpretability and rule integration needed for safety-critical decision support. Simultaneously, the streaming data architecture challenge is severe — emergency calls, weather updates, and fire detections arrive on different cadences and schemas, requiring complex temporal windowing, stateful spatial joins, and exactly-once processing guarantees while maintaining end-to-end latency under 10 seconds from data ingestion to dashboard visualization.

This project aims to: **(1)** develop a real-time event-driven microservices architecture using Apache Kafka and Apache Flink that ingests, correlates, and enriches heterogeneous emergency data streams with sub-10-second latency; **(2)** implement a multi-agent Flink stream processing pipeline with four specialized agents (CallProcessing, DataEnrichment, RiskScoring, AlertGeneration) executing complex event processing with exactly-once semantics and stateful transformations; **(3)** integrate Google Gemini NLP for intelligent emergency call classification from unstructured natural language transcripts, extracting incident types, urgency levels, and vulnerability factors with >90% accuracy; **(4)** perform real-time geospatial stream joins fusing emergency call locations with NOAA weather data and NASA FIRMS fire detections using spatial-temporal windowing to compute dynamic hazard proximity scores; **(5)** enable continuous risk recalculation through stateful stream processing that maintains and updates risk profiles as external conditions evolve, providing dispatchers with live-updating prioritization that adapts faster than human reassessment; **(6)** deliver real-time visualization through WebSocket streaming to an interactive dashboard with live map overlays, replacing traditional static dispatch boards with continuously synchronized situational awareness; and **(7)** demonstrate that stream processing architectures can transform emergency response from reactive call handling to proactive, climate-aware, continuously adaptive triage systems capable of identifying compounding risks invisible to traditional dispatch workflows.

---

## Proposed Methodology / Approach

The project implements a **seven-stage event-driven stream processing architecture** with end-to-end real-time data flow:

**Stage 1 – Multi-Source Data Ingestion:**  
Python-based ingestion services continuously poll external APIs — emergency call generators (synthetic Maui wildfire transcripts via Gemini), NOAA weather stations (temperature, humidity, wind speed/direction, precipitation), and NASA FIRMS fire detections (satellite thermal anomalies with lat/lng, brightness temperature, fire radiative power). Each ingestion service publishes raw data to dedicated Kafka topics (`raw-emergency-calls`, `raw-weather-data`, `raw-fire-data`) with Avro serialization for schema evolution, achieving <100ms ingestion latency and supporting 10,000+ messages per second throughput.

**Stage 2 – Kafka Event Streaming Backbone:**  
Apache Kafka serves as the central event streaming platform with multi-topic architecture, horizontal scalability through partitioning (8 partitions per topic for parallelism), replication factor of 3 for fault tolerance, and exactly-once delivery semantics (`enable.idempotence=true`, transactional producers). Kafka UI provides real-time monitoring of topic lag, throughput, and consumer group status, ensuring zero message loss during stream processing.

**Stage 3 – Flink Stream Processing Pipeline (4 Specialized Agents):**  
Apache Flink agents consume from Kafka, execute stateful transformations, and publish enriched results back to Kafka output topics:

- **CallProcessingAgent:** Consumes `raw-emergency-calls`, invokes Python FastAPI ML service with Google Gemini API for NLP classification (incident type: fire/medical/evacuation, urgency: critical/high/medium/low, vulnerability extraction: age/mobility/medical conditions), enriches call with semantic metadata, publishes to `classified-calls` topic. Implements async HTTP calls with circuit breakers, achieving 90% classification accuracy at 30-45 calls/second throughput with <2-second per-call latency.

- **DataEnrichmentAgent:** Consumes `classified-calls`, performs **stateful spatial-temporal stream joins** — for each emergency call, queries windowed streams of weather and fire data (5km radius, 30-minute time window), executes geospatial distance calculations (Haversine formula for fire proximity, nearest weather station matching), enriches call with `weather_conditions` (current temp/humidity/wind) and `fire_proximity` (distance to nearest active fire, fire radiative power), publishes to `enriched-calls` topic. Uses Flink's interval join and geoprocessing UDFs, completing spatial join in <500ms per event.

- **RiskScoringAgent:** Consumes `enriched-calls`, executes **hybrid ML/rule-based risk assessment** — computes composite risk score (0-100 scale) by weighted combination of: call urgency (30%), fire proximity with inverse distance weighting (35%), weather severity score from extreme conditions (20%), vulnerability factors (age >65, mobility impairment, medical dependency, 15%). Implements Flink stateful processing to maintain caller history and detect repeat calls, publishes to `risk-scored-calls` topic with explainability metadata. Achieves <200ms scoring latency with 85% correlation to human expert triage decisions.

- **AlertGenerationAgent:** Consumes `risk-scored-calls`, applies **Complex Event Processing (CEP) patterns** to detect critical conditions (risk score >80 + fire proximity <2km + vulnerable population), generates structured alert messages with recommended actions (immediate evacuation, medical dispatch, fire crew deployment), publishes to `generated-alerts` topic and triggers WebSocket push to connected dashboard clients. Uses Flink CEP library with pattern matching, completing detection-to-alert in <100ms.

**Stage 4 – ML/AI Microservices Layer:**  
Python FastAPI services provide stateless HTTP endpoints for CPU/GPU-intensive ML inference, decoupling expensive NLP processing from Flink's JVM-based stream processing. Services include: (a) Call Classification Service with Gemini API integration, (b) Geospatial Enrichment Service with shapely/geopandas for spatial operations, (c) Risk Scoring Service with scikit-learn model inference, (d) Alert Generation Service with rule engine. All services containerized with Docker, auto-scaling with horizontal pod autoscaling, health checks and circuit breaker patterns for resilience.

**Stage 5 – Backend API with Real-Time Synchronization:**  
Python FastAPI backend consumes processed data from Kafka output topics via Kafka Consumer Group (`sentinel-backend-consumer`), persists to SQLite database (indexed by `call_id`, `timestamp`, `risk_score`), and maintains WebSocket connections with dashboard clients. WebSocket server broadcasts incremental updates (new calls, risk score changes, alerts) with <500ms latency from Kafka message to frontend rendering, enabling live operational dashboards that update without polling.

**Stage 6 – Interactive Real-Time Dashboard:**  
Next.js/React frontend with Google Maps integration displays: (a) live fire perimeters as heatmap overlays, (b) emergency call markers color-coded by risk score (red: critical, orange: high, yellow: medium, green: low), (c) weather station indicators, (d) real-time call list with sorting/filtering, (e) alert notification panel with toast messages for critical events. WebSocket client maintains persistent connection for sub-second UI updates, replacing traditional request-response polling with event-driven synchronization.

**Stage 7 – End-to-End Pipeline Observability:**  
Comprehensive monitoring via Flink Dashboard (job metrics, checkpointing, backpressure), Kafka UI (topic throughput, consumer lag), FastAPI auto-generated OpenAPI docs, and application logging with structured JSON logs for distributed tracing across microservices.

**Technology Stack:**  
- **Stream Processing:** Apache Kafka 3.5+ (event streaming), Apache Flink 1.17+ (stateful stream processing with exactly-once semantics)
- **ML/AI:** Google Gemini API (NLP), Python 3.10-3.12, FastAPI (async microservices)
- **Data Sources:** NASA FIRMS (fire detections), NOAA (weather), synthetic call generation via Gemini
- **Frontend:** Next.js 14, React 18, TypeScript, Google Maps JavaScript API, WebSocket client
- **Infrastructure:** Docker Compose (local orchestration), SQLite (persistence), uv (Python package management)

---

## Expected Outcomes

The project will deliver a **production-ready intelligent emergency triage platform** demonstrating the transformative impact of stream processing on disaster response:

**1. Real-Time Classification Performance:**  
Emergency call NLP classification achieving **90%+ accuracy** across incident types (fire proximity, medical emergency, evacuation request, structural damage, missing person) on synthetic Maui wildfire call transcripts, with **<5-second end-to-end latency** from call ingestion to classified output. Gemini-powered semantic extraction will identify vulnerability factors (age, mobility, medical conditions) with **85% precision**, enabling targeted resource allocation to at-risk populations.

**2. Stream Processing Throughput and Latency:**  
Apache Kafka pipeline sustaining **10,000+ messages/second ingestion** with <100ms producer latency and zero message loss through replication. Flink stream processing completing **full 4-agent transformation pipeline** (CallProcessing → DataEnrichment → RiskScoring → AlertGeneration) in **<10 seconds end-to-end** with exactly-once processing semantics, validated through Flink checkpointing and message deduplication metrics.

**3. Geospatial Stream Join Accuracy:**  
DataEnrichmentAgent performing **stateful spatial-temporal joins** correlating each emergency call with weather observations (5km radius, 30-min window) and fire detections (10km radius, 3-hour window due to satellite revisit time) with **<500ms join latency** and **>95% spatial match accuracy**. System will demonstrate that 40-60% of emergency calls during wildfire events occur within 5km of active fires but lack environmental context in traditional dispatch systems.

**4. Dynamic Risk Reassessment:**  
RiskScoringAgent maintaining **stateful risk profiles** for all active incidents, automatically recalculating scores as weather conditions change (e.g., wind shift increasing fire spread risk) or new fire detections appear, with **<200ms recalculation latency**. Demonstrate **30% improvement in triage accuracy** compared to static initial assessment, measured by correlation with retrospective expert evaluation of Maui wildfire response priorities.

**5. Real-Time Dashboard Synchronization:**  
WebSocket-based frontend achieving **<2-second latency** from Kafka message publication to dashboard visualization, enabling dispatchers to monitor 50-100 simultaneous active incidents with live-updating risk scores, fire proximity indicators, and weather overlays without manual refresh. Interactive map will display **temporal fire spread progression** and **call density heatmaps** for resource allocation planning.

**6. Alert Generation Precision:**  
AlertGenerationAgent demonstrating **95% precision** in critical alert generation (risk score >80 + fire proximity <2km + vulnerable population), reducing false positive alert fatigue by **40% compared to threshold-only baselines** through multi-dimensional CEP pattern matching. System will generate **<10 false alerts per 1000 incidents** while maintaining 100% recall for ground-truth critical scenarios.

**7. Scalability and Fault Tolerance:**  
Architecture supporting **horizontal scaling** to 500+ calls/hour (typical major wildfire event volume) through Kafka partitioning and Flink parallelism, with **automatic recovery from node failures** via Flink checkpointing (RocksDB state backend, 60-second checkpoint interval) and Kafka replication, demonstrating <30-second recovery time objective (RTO) from simulated node failures.

**8. Multi-Hazard Extensibility Foundation:**  
Modular agent architecture enabling **future extension beyond wildfires** to hurricanes (wind speed thresholds, storm surge proximity), floods (elevation + precipitation), earthquakes (seismic intensity + structural vulnerability), demonstrating that the stream processing framework generalizes across disaster types without architectural redesign.

The resulting system will represent **the first known open-source implementation** of a climate-aware emergency triage platform using production-grade stream processing (Kafka + Flink) with NLP-powered call classification, demonstrating that event-driven architectures can transform emergency response from reactive call handling to proactive, continuously adaptive, spatially-aware risk assessment — reducing dispatcher cognitive load, improving resource allocation equity, and providing the real-time situational awareness that climate disasters increasingly demand.

---

**Go, change the world®**
