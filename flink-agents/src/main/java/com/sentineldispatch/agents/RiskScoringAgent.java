package com.sentineldispatch.agents;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sentineldispatch.agents.common.HttpAsyncFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Risk Scoring Agent - Calculates risk scores for enriched calls.
 * 
 * Pipeline:
 *   enriched-calls (Kafka) → ScoreRiskAsyncFunction → risk-scores (Kafka)
 * 
 * The agent calls the Python FastAPI service at /api/score-risk
 * to calculate risk scores using hybrid approach (Vertex AI or rule-based).
 */
public class RiskScoringAgent {
    
    private static final Logger LOG = LoggerFactory.getLogger(RiskScoringAgent.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    private static final String DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_SERVICE_URL = "http://localhost:8000/api/score-risk";
    private static final String INPUT_TOPIC = "enriched-calls";
    private static final String OUTPUT_TOPIC = "risk-scores";
    private static final String CONSUMER_GROUP_ID = "risk-scoring-agent";
    
    public static void main(String[] args) throws Exception {
        // Parse command line arguments
        String kafkaBootstrapServers = DEFAULT_KAFKA_BOOTSTRAP_SERVERS;
        String serviceUrl = DEFAULT_SERVICE_URL;
        
        for (int i = 0; i < args.length; i++) {
            if ("--kafka-bootstrap-servers".equals(args[i]) && i + 1 < args.length) {
                kafkaBootstrapServers = args[i + 1];
                i++;
            } else if ("--service-url".equals(args[i]) && i + 1 < args.length) {
                serviceUrl = args[i + 1];
                i++;
            }
        }
        
        // Use environment variables as fallback
        if (kafkaBootstrapServers.equals(DEFAULT_KAFKA_BOOTSTRAP_SERVERS)) {
            String envKafka = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
            if (envKafka != null && !envKafka.isEmpty()) {
                kafkaBootstrapServers = envKafka;
            }
        }
        if (serviceUrl.equals(DEFAULT_SERVICE_URL)) {
            String envService = System.getenv("SCORE_SERVICE_URL");
            if (envService != null && !envService.isEmpty()) {
                serviceUrl = envService;
            }
        }
        
        LOG.info("Starting Risk Scoring Agent");
        LOG.info("Configuration:");
        LOG.info("  Kafka bootstrap servers: {}", kafkaBootstrapServers);
        LOG.info("  Service URL: {}", serviceUrl);
        LOG.info("  Input topic: {}", INPUT_TOPIC);
        LOG.info("  Output topic: {}", OUTPUT_TOPIC);
        
        // Create Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // Create Table environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        // Create Kafka source table
        String sourceTableDDL = String.format(
            "CREATE TABLE enriched_calls (\n" +
            "  enriched_call_json STRING\n" +
            ") WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = '%s',\n" +
            "  'properties.bootstrap.servers' = '%s',\n" +
            "  'properties.group.id' = '%s',\n" +
            "  'format' = 'raw',\n" +
            "  'raw.charset' = 'UTF-8',\n" +
            "  'scan.startup.mode' = 'latest-offset',\n" +
            "  'properties.request.timeout.ms' = '30000',\n" +
            "  'properties.metadata.max.age.ms' = '30000'\n" +
            ")",
            INPUT_TOPIC, kafkaBootstrapServers, CONSUMER_GROUP_ID
        );
        tableEnv.executeSql(sourceTableDDL);
        
        // Convert to DataStream for async processing
        Table sourceTable = tableEnv.from("enriched_calls");
        DataStream<String> enrichedCalls = tableEnv.toDataStream(sourceTable, String.class);
        
        // Process calls with async HTTP function
        DataStream<String> riskScores = AsyncDataStream.unorderedWait(
            enrichedCalls,
            new ScoreRiskAsyncFunction(serviceUrl),
            30,
            TimeUnit.SECONDS,
            100
        );
        
        // Convert back to Table for Kafka sink
        Table riskScoresTable = tableEnv.fromDataStream(riskScores);
        tableEnv.createTemporaryView("risk_scores_stream", riskScoresTable);
        
        // Create Kafka sink table
        String sinkTableDDL = String.format(
            "CREATE TABLE risk_scores (\n" +
            "  risk_score_json STRING\n" +
            ") WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = '%s',\n" +
            "  'properties.bootstrap.servers' = '%s',\n" +
            "  'format' = 'raw',\n" +
            "  'raw.charset' = 'UTF-8',\n" +
            "  'properties.request.timeout.ms' = '30000'\n" +
            ")",
            OUTPUT_TOPIC, kafkaBootstrapServers
        );
        tableEnv.executeSql(sinkTableDDL);
        
        // Insert into sink
        tableEnv.executeSql("INSERT INTO risk_scores SELECT * FROM risk_scores_stream");
    }
    
    /**
     * Async function to score risk via HTTP.
     */
    private static class ScoreRiskAsyncFunction extends HttpAsyncFunction<String, String> {
        
        public ScoreRiskAsyncFunction(String serviceUrl) {
            super(serviceUrl, 30);
        }
        
        @Override
        protected String buildRequestBody(String input) throws Exception {
            // Parse enriched call JSON
            JsonNode enrichedCallJson = OBJECT_MAPPER.readTree(input);
            
            // Extract required fields from enriched call
            String callId = enrichedCallJson.path("call_id").asText("");
            if (callId.isEmpty()) {
                throw new IllegalArgumentException("call_id is required in enriched_call: " + input);
            }
            
            String timestamp = enrichedCallJson.path("timestamp").asText("");
            if (timestamp.isEmpty()) {
                timestamp = enrichedCallJson.path("enrichment_timestamp").asText("");
            }
            if (timestamp.isEmpty()) {
                timestamp = java.time.Instant.now().toString();
            }
            
            // Extract caller location (required)
            JsonNode callerLocation = enrichedCallJson.path("caller_location");
            if (callerLocation.isMissingNode()) {
                callerLocation = enrichedCallJson.path("processed_call").path("caller_location");
            }
            
            // Extract classification (required)
            JsonNode classification = enrichedCallJson.path("classification");
            if (classification.isMissingNode()) {
                classification = enrichedCallJson.path("processed_call").path("classification");
            }
            if (classification.isMissingNode() || classification.isNull()) {
                // Create a default classification if missing (better than failing)
                LOG.warn("Classification is null or missing in enriched_call, creating default. Input: {}", 
                    input.length() > 500 ? input.substring(0, 500) + "..." : input);
                ObjectNode defaultClassification = OBJECT_MAPPER.createObjectNode();
                defaultClassification.put("urgency", 3);
                defaultClassification.put("incident_type", "unknown");
                defaultClassification.put("confidence", 0.0);
                defaultClassification.set("entities", OBJECT_MAPPER.createObjectNode());
                defaultClassification.put("sentiment", 0.5);
                defaultClassification.set("key_phrases", OBJECT_MAPPER.createArrayNode());
                classification = defaultClassification;
            }
            
            // Extract fire analysis and convert to nearest_fire format for risk scorer
            JsonNode fireAnalysis = enrichedCallJson.path("fire_analysis");
            ObjectNode nearestFire = null;
            if (!fireAnalysis.isMissingNode() && !fireAnalysis.isNull()) {
                // FireProximityAnalysis now contains: distance_km, location, frp, spread_rate, etc.
                // Risk scorer needs: distance_km, location (lat/lon), frp, spread_rate
                nearestFire = OBJECT_MAPPER.createObjectNode();
                
                // Extract distance_km (required for fire_proximity calculation)
                if (fireAnalysis.has("distance_km")) {
                    double distanceKm = fireAnalysis.path("distance_km").asDouble(0.0);
                    nearestFire.put("distance_km", distanceKm);
                }
                
                // Extract fire_location (lat/lon) - included in FireProximityAnalysis
                if (fireAnalysis.has("fire_location") && !fireAnalysis.path("fire_location").isNull()) {
                    JsonNode locationNode = fireAnalysis.path("fire_location");
                    ObjectNode locationObj = OBJECT_MAPPER.createObjectNode();
                    if (locationNode.has("lat")) {
                        locationObj.put("lat", locationNode.path("lat").asDouble(0.0));
                    }
                    if (locationNode.has("lon")) {
                        locationObj.put("lon", locationNode.path("lon").asDouble(0.0));
                    }
                    if (locationObj.size() > 0) {
                        nearestFire.set("location", locationObj); // Risk scorer expects "location"
                    }
                }
                
                // Extract FRP (Fire Radiative Power) - now included in FireProximityAnalysis
                if (fireAnalysis.has("frp")) {
                    double frp = fireAnalysis.path("frp").asDouble(0.0);
                    if (frp > 0) {
                        nearestFire.put("frp", frp);
                        nearestFire.put("FRP", frp); // Support both formats
                    }
                }
                
                // Extract spread_rate (for fire_spread_rate calculation)
                if (fireAnalysis.has("spread_rate")) {
                    double spreadRate = fireAnalysis.path("spread_rate").asDouble(0.0);
                    if (spreadRate > 0) {
                        nearestFire.put("spread_rate", spreadRate);
                        nearestFire.put("spreadRate", spreadRate); // Support both formats
                    }
                }
                
                // Extract fire_id for reference
                if (fireAnalysis.has("nearest_fire_id")) {
                    String fireId = fireAnalysis.path("nearest_fire_id").asText("");
                    if (!fireId.isEmpty()) {
                        nearestFire.put("fire_id", fireId);
                        nearestFire.put("fireId", fireId); // Support both formats
                    }
                }
                
                // Only add if we have at least distance_km (minimum required for fire proximity)
                if (!nearestFire.has("distance_km") || nearestFire.path("distance_km").asDouble(0.0) == 0.0) {
                    nearestFire = null; // Don't include if no distance data
                }
            }
            
            // Extract weather enrichment
            JsonNode weatherEnrichment = enrichedCallJson.path("weather_enrichment");
            JsonNode weatherData = null;
            if (!weatherEnrichment.isMissingNode()) {
                weatherData = weatherEnrichment;
            }
            
            // Build request payload for risk scoring service
            ObjectNode requestPayload = OBJECT_MAPPER.createObjectNode();
            requestPayload.put("call_id", callId);
            requestPayload.put("call_timestamp", timestamp);
            
            // Add caller location (required) - ensure proper format
            ObjectNode locationNode = OBJECT_MAPPER.createObjectNode();
            if (!callerLocation.isMissingNode() && !callerLocation.isNull()) {
                if (callerLocation.has("lat")) {
                    locationNode.put("lat", callerLocation.path("lat").asDouble(0.0));
                } else if (callerLocation.has("latitude")) {
                    locationNode.put("lat", callerLocation.path("latitude").asDouble(0.0));
                } else {
                    locationNode.put("lat", 0.0);
                }
                if (callerLocation.has("lon")) {
                    locationNode.put("lon", callerLocation.path("lon").asDouble(0.0));
                } else if (callerLocation.has("longitude")) {
                    locationNode.put("lon", callerLocation.path("longitude").asDouble(0.0));
                } else {
                    locationNode.put("lon", 0.0);
                }
            } else {
                // Default location if missing
                locationNode.put("lat", 0.0);
                locationNode.put("lon", 0.0);
            }
            requestPayload.set("caller_location", locationNode);
            
            // Add classification (required) - ensure it has all required fields
            if (!classification.isMissingNode() && !classification.isNull()) {
                // Validate classification has required fields
                ObjectNode validatedClassification = OBJECT_MAPPER.createObjectNode();
                if (classification.has("urgency")) {
                    validatedClassification.put("urgency", classification.path("urgency").asInt(3));
                } else {
                    validatedClassification.put("urgency", 3);
                }
                if (classification.has("incident_type")) {
                    validatedClassification.put("incident_type", classification.path("incident_type").asText("unknown"));
                } else {
                    validatedClassification.put("incident_type", "unknown");
                }
                if (classification.has("confidence")) {
                    validatedClassification.put("confidence", classification.path("confidence").asDouble(0.5));
                } else {
                    validatedClassification.put("confidence", 0.5);
                }
                if (classification.has("entities")) {
                    validatedClassification.set("entities", classification.path("entities"));
                } else {
                    validatedClassification.set("entities", OBJECT_MAPPER.createObjectNode());
                }
                if (classification.has("sentiment")) {
                    validatedClassification.put("sentiment", classification.path("sentiment").asDouble(0.5));
                } else {
                    validatedClassification.put("sentiment", 0.5);
                }
                if (classification.has("key_phrases")) {
                    validatedClassification.set("key_phrases", classification.path("key_phrases"));
                } else {
                    validatedClassification.set("key_phrases", OBJECT_MAPPER.createArrayNode());
                }
                requestPayload.set("call_classification", validatedClassification);
            } else {
                throw new IllegalArgumentException("classification is required in enriched_call: " + input);
            }
            
            // Add optional fields
            if (nearestFire != null && !nearestFire.isMissingNode()) {
                requestPayload.set("nearest_fire", nearestFire);
            }
            if (weatherData != null && !weatherData.isMissingNode()) {
                requestPayload.set("weather_data", weatherData);
            }
            
            // Extract vulnerability factors and metadata from enriched call
            JsonNode metadata = enrichedCallJson.path("metadata");
            if (metadata.isMissingNode()) {
                // Try to get from processed_call if available
                metadata = enrichedCallJson.path("processed_call").path("metadata");
            }
            
            if (!metadata.isMissingNode() && !metadata.isNull() && metadata.isObject()) {
                ObjectNode vulnerabilityFactors = OBJECT_MAPPER.createObjectNode();
                boolean hasVulnerabilityData = false;
                
                // Extract age (callerAge)
                if (metadata.has("callerAge")) {
                    int age = metadata.path("callerAge").asInt(0);
                    if (age > 0) {
                        vulnerabilityFactors.put("age", age);
                        hasVulnerabilityData = true;
                    }
                }
                
                // Extract medical conditions (knownConditions)
                if (metadata.has("knownConditions")) {
                    JsonNode conditions = metadata.path("knownConditions");
                    if (conditions.isArray() && conditions.size() > 0) {
                        vulnerabilityFactors.set("medical_conditions", conditions);
                        hasVulnerabilityData = true;
                    }
                }
                
                // Extract mobility if available
                if (metadata.has("mobility")) {
                    String mobility = metadata.path("mobility").asText("normal");
                    vulnerabilityFactors.put("mobility", mobility);
                    // Mobility alone (if not "normal") is also vulnerability data
                    if (!mobility.equals("normal")) {
                        hasVulnerabilityData = true;
                    }
                }
                
                // If no explicit vulnerability data, infer from scenarioType
                if (!hasVulnerabilityData && metadata.has("scenarioType")) {
                    String scenarioType = metadata.path("scenarioType").asText("").toLowerCase();
                    
                    if (scenarioType.equals("elderly_resident")) {
                        // Infer elderly resident: age 75, possible medical conditions, limited mobility
                        vulnerabilityFactors.put("age", 75);
                        com.fasterxml.jackson.databind.node.ArrayNode conditionsArray = 
                            OBJECT_MAPPER.createArrayNode();
                        conditionsArray.add("heart_disease"); // Common in elderly
                        vulnerabilityFactors.set("medical_conditions", conditionsArray);
                        vulnerabilityFactors.put("mobility", "limited");
                        hasVulnerabilityData = true;
                    } else if (scenarioType.equals("child_separation") || scenarioType.equals("child")) {
                        // Infer child: age < 18
                        vulnerabilityFactors.put("age", 8);
                        hasVulnerabilityData = true;
                    } else if (scenarioType.equals("medical_emergency") || scenarioType.equals("smoke_inhalation")) {
                        // Medical emergency suggests possible conditions
                        com.fasterxml.jackson.databind.node.ArrayNode conditionsArray = 
                            OBJECT_MAPPER.createArrayNode();
                        conditionsArray.add("respiratory"); // Common for smoke inhalation
                        vulnerabilityFactors.set("medical_conditions", conditionsArray);
                        hasVulnerabilityData = true;
                    }
                }
                
                // Only add vulnerability_factors if we have at least age or conditions
                if (hasVulnerabilityData) {
                    requestPayload.set("vulnerability_factors", vulnerabilityFactors);
                }
                
                // Extract phase and scenarioType for risk scoring adjustments
                // These can be used to adjust urgency or risk factors
                if (metadata.has("phase")) {
                    String phase = metadata.path("phase").asText("");
                    if (!phase.isEmpty()) {
                        requestPayload.put("phase", phase); // Pass to risk scorer for potential adjustments
                    }
                }
                if (metadata.has("scenarioType")) {
                    String scenarioType = metadata.path("scenarioType").asText("");
                    if (!scenarioType.isEmpty()) {
                        requestPayload.put("scenario_type", scenarioType); // Pass to risk scorer
                    }
                }
            }
            
            // Add current timestamp (optional)
            requestPayload.put("current_timestamp", java.time.Instant.now().toString());
            
            return OBJECT_MAPPER.writeValueAsString(requestPayload);
        }
        
        @Override
        protected String parseResponse(String responseBody, String input) throws Exception {
            // Parse service response
            JsonNode responseJson = OBJECT_MAPPER.readTree(responseBody);
            
            // Return risk score from response
            JsonNode riskScore = responseJson.path("risk_score");
            if (riskScore.isMissingNode()) {
                // Fallback: return error response
                ObjectNode errorResponse = OBJECT_MAPPER.createObjectNode();
                errorResponse.put("error", "Risk score not found in response");
                errorResponse.put("call_id", OBJECT_MAPPER.readTree(input).path("call_id").asText(""));
                return OBJECT_MAPPER.writeValueAsString(errorResponse);
            }
            
            return OBJECT_MAPPER.writeValueAsString(riskScore);
        }
        
        @Override
        protected String handleError(String input, int statusCode, String errorMessage) {
            // On error, return error response with call_id
            try {
                JsonNode enrichedCallJson = OBJECT_MAPPER.readTree(input);
                ObjectNode errorResponse = OBJECT_MAPPER.createObjectNode();
                errorResponse.put("call_id", enrichedCallJson.path("call_id").asText(""));
                errorResponse.put("error", "Risk scoring failed: " + errorMessage);
                errorResponse.put("risk_score", 0.0);
                errorResponse.put("confidence", 0.0);
                errorResponse.put("recommendation", "ERROR");
                errorResponse.put("priority", 99);
                
                return OBJECT_MAPPER.writeValueAsString(errorResponse);
            } catch (Exception e) {
                LOG.error("Error handling failure for input: {}", input, e);
                return input; // Return as-is if we can't parse
            }
        }
    }
}

