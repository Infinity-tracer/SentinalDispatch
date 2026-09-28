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

import java.util.concurrent.TimeUnit;

/**
 * Alert Generation Agent - Generates alerts based on risk scores.
 * 
 * Pipeline:
 *   risk-scores (Kafka) → GenerateAlertAsyncFunction → alerts (Kafka)
 * 
 * The agent calls the Python FastAPI service at /api/generate-alert
 * to generate alerts based on risk scores and contextual data.
 */
public class AlertGenerationAgent {
    
    private static final Logger LOG = LoggerFactory.getLogger(AlertGenerationAgent.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    private static final String DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_SERVICE_URL = "http://localhost:8000/api/generate-alert";
    private static final String INPUT_TOPIC = "risk-scores";
    private static final String OUTPUT_TOPIC = "alerts";
    private static final String CONSUMER_GROUP_ID = "alert-generation-agent";
    
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
            String envService = System.getenv("ALERT_SERVICE_URL");
            if (envService != null && !envService.isEmpty()) {
                serviceUrl = envService;
            }
        }
        
        LOG.info("Starting Alert Generation Agent");
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
            "CREATE TABLE risk_scores (\n" +
            "  risk_score_json STRING\n" +
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
        Table sourceTable = tableEnv.from("risk_scores");
        DataStream<String> riskScores = tableEnv.toDataStream(sourceTable, String.class);
        
        // Process risk scores with async HTTP function
        DataStream<String> alerts = AsyncDataStream.unorderedWait(
            riskScores,
            new GenerateAlertAsyncFunction(serviceUrl),
            30,
            TimeUnit.SECONDS,
            100
        );
        
        // Convert back to Table for Kafka sink
        Table alertsTable = tableEnv.fromDataStream(alerts);
        tableEnv.createTemporaryView("alerts_stream", alertsTable);
        
        // Create Kafka sink table
        String sinkTableDDL = String.format(
            "CREATE TABLE alerts (\n" +
            "  alert_json STRING\n" +
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
        tableEnv.executeSql("INSERT INTO alerts SELECT * FROM alerts_stream");
    }
    
    /**
     * Async function to generate alerts via HTTP.
     */
    private static class GenerateAlertAsyncFunction extends HttpAsyncFunction<String, String> {
        
        public GenerateAlertAsyncFunction(String serviceUrl) {
            super(serviceUrl, 30);
        }
        
        @Override
        protected String buildRequestBody(String input) throws Exception {
            // Handle empty or null input
            if (input == null || input.trim().isEmpty()) {
                LOG.warn("AlertGenerationAgent received empty input, creating default request");
                ObjectNode requestPayload = OBJECT_MAPPER.createObjectNode();
                ObjectNode defaultRiskScore = OBJECT_MAPPER.createObjectNode();
                defaultRiskScore.put("call_id", "unknown-" + System.currentTimeMillis());
                defaultRiskScore.put("timestamp", java.time.Instant.now().toString());
                defaultRiskScore.put("risk_score", 0.0);
                defaultRiskScore.put("confidence", 0.0);
                defaultRiskScore.put("recommendation", "MONITOR");
                defaultRiskScore.put("priority", 5);
                ObjectNode defaultFactors = OBJECT_MAPPER.createObjectNode();
                defaultFactors.put("call_urgency", 0.5);
                defaultFactors.put("fire_proximity", 0.0);
                defaultFactors.put("vulnerability", 0.3);
                defaultRiskScore.set("factors", defaultFactors);
                requestPayload.set("risk_score", defaultRiskScore);
                return OBJECT_MAPPER.writeValueAsString(requestPayload);
            }
            
            // Parse risk score JSON
            JsonNode riskScoreJson;
            try {
                riskScoreJson = OBJECT_MAPPER.readTree(input);
            } catch (Exception e) {
                LOG.error("Failed to parse input as JSON: {}", input, e);
                throw new IllegalArgumentException("Invalid JSON input: " + e.getMessage());
            }
            
            // Check if this is an error response (from risk scoring failure)
            if (riskScoreJson.has("error")) {
                // If risk scoring failed, create a minimal request
                // The service will handle this gracefully
                ObjectNode requestPayload = OBJECT_MAPPER.createObjectNode();
                ObjectNode errorRiskScore = OBJECT_MAPPER.createObjectNode();
                String callId = riskScoreJson.path("call_id").asText("");
                if (callId.isEmpty()) {
                    callId = "unknown-" + System.currentTimeMillis();
                }
                errorRiskScore.put("call_id", callId);
                errorRiskScore.put("timestamp", java.time.Instant.now().toString());
                errorRiskScore.put("risk_score", riskScoreJson.path("risk_score").asDouble(0.0));
                errorRiskScore.put("confidence", riskScoreJson.path("confidence").asDouble(0.0));
                errorRiskScore.put("recommendation", riskScoreJson.path("recommendation").asText("ERROR"));
                errorRiskScore.put("priority", riskScoreJson.path("priority").asInt(99));
                // Add required fields for RiskScoreResult - factors must have required fields
                ObjectNode errorFactors = OBJECT_MAPPER.createObjectNode();
                errorFactors.put("call_urgency", 0.5);
                errorFactors.put("fire_proximity", 0.0);
                errorFactors.put("vulnerability", 0.3);
                errorRiskScore.set("factors", errorFactors);
                requestPayload.set("risk_score", errorRiskScore);
                // enriched_call is optional, leave it null
                return OBJECT_MAPPER.writeValueAsString(requestPayload);
            }
            
            // Validate and normalize risk_score to match RiskScoreResult schema
            // Required fields: call_id, risk_score, confidence, recommendation, priority, factors
            ObjectNode normalizedRiskScore = OBJECT_MAPPER.createObjectNode();
            
            String callId = riskScoreJson.path("call_id").asText("");
            if (callId.isEmpty()) {
                throw new IllegalArgumentException("call_id is required in risk_score: " + input);
            }
            normalizedRiskScore.put("call_id", callId);
            
            // timestamp (required)
            String timestamp = riskScoreJson.path("timestamp").asText("");
            if (timestamp.isEmpty()) {
                timestamp = java.time.Instant.now().toString();
            }
            normalizedRiskScore.put("timestamp", timestamp);
            
            // risk_score (required)
            if (riskScoreJson.has("risk_score")) {
                normalizedRiskScore.put("risk_score", riskScoreJson.path("risk_score").asDouble(0.0));
            } else {
                throw new IllegalArgumentException("risk_score is required: " + input);
            }
            
            // confidence (required)
            if (riskScoreJson.has("confidence")) {
                normalizedRiskScore.put("confidence", riskScoreJson.path("confidence").asDouble(0.0));
            } else {
                normalizedRiskScore.put("confidence", 0.0);
            }
            
            // recommendation (required)
            if (riskScoreJson.has("recommendation")) {
                normalizedRiskScore.put("recommendation", riskScoreJson.path("recommendation").asText("MONITOR"));
            } else {
                normalizedRiskScore.put("recommendation", "MONITOR");
            }
            
            // priority (required)
            if (riskScoreJson.has("priority")) {
                normalizedRiskScore.put("priority", riskScoreJson.path("priority").asInt(3));
            } else {
                normalizedRiskScore.put("priority", 3);
            }
            
            // factors (required) - must have call_urgency, fire_proximity, and vulnerability
            ObjectNode factorsNode;
            if (riskScoreJson.has("factors") && !riskScoreJson.path("factors").isMissingNode()) {
                JsonNode factorsJson = riskScoreJson.path("factors");
                factorsNode = OBJECT_MAPPER.createObjectNode();
                
                // Copy existing fields
                if (factorsJson.has("call_urgency")) {
                    factorsNode.put("call_urgency", factorsJson.path("call_urgency").asDouble(0.5));
                } else {
                    factorsNode.put("call_urgency", 0.5); // Default
                }
                
                if (factorsJson.has("fire_proximity")) {
                    factorsNode.put("fire_proximity", factorsJson.path("fire_proximity").asDouble(0.0));
                } else {
                    factorsNode.put("fire_proximity", 0.0); // Default
                }
                
                if (factorsJson.has("vulnerability")) {
                    factorsNode.put("vulnerability", factorsJson.path("vulnerability").asDouble(0.3));
                } else {
                    factorsNode.put("vulnerability", 0.3); // Default
                }
                
                // Optional fields
                if (factorsJson.has("fire_spread_rate")) {
                    factorsNode.put("fire_spread_rate", factorsJson.path("fire_spread_rate").asDouble(0.0));
                }
                if (factorsJson.has("weather_risk")) {
                    factorsNode.put("weather_risk", factorsJson.path("weather_risk").asDouble(0.0));
                }
                if (factorsJson.has("time_since_call")) {
                    factorsNode.put("time_since_call", factorsJson.path("time_since_call").asDouble(0.0));
                }
            } else {
                // Create default factors if missing
                factorsNode = OBJECT_MAPPER.createObjectNode();
                factorsNode.put("call_urgency", 0.5);
                factorsNode.put("fire_proximity", 0.0);
                factorsNode.put("vulnerability", 0.3);
            }
            normalizedRiskScore.set("factors", factorsNode);
            
            // Build request payload for alert generation service
            ObjectNode requestPayload = OBJECT_MAPPER.createObjectNode();
            requestPayload.set("risk_score", normalizedRiskScore);
            // enriched_call is optional and not available in risk-scores topic
            // The service can work with just the risk_score
            
            String requestBody = OBJECT_MAPPER.writeValueAsString(requestPayload);
            LOG.info("Built alert generation request: {}", requestBody);
            return requestBody;
        }
        
        @Override
        protected String parseResponse(String responseBody, String input) throws Exception {
            // Parse service response
            JsonNode responseJson = OBJECT_MAPPER.readTree(responseBody);
            
            // Extract alerts array
            JsonNode alertsArray = responseJson.path("alerts");
            if (alertsArray.isMissingNode() || !alertsArray.isArray()) {
                // Fallback: return error response
                ObjectNode errorResponse = OBJECT_MAPPER.createObjectNode();
                errorResponse.put("error", "Alerts not found in response");
                JsonNode riskScoreJson = OBJECT_MAPPER.readTree(input);
                errorResponse.put("call_id", riskScoreJson.path("call_id").asText(""));
                return OBJECT_MAPPER.writeValueAsString(errorResponse);
            }
            
            // Return the full response (includes alerts, alert_count, timestamp)
            // This allows downstream consumers to see all alert metadata
            return OBJECT_MAPPER.writeValueAsString(responseJson);
        }
        
        @Override
        protected String handleError(String input, int statusCode, String errorMessage) {
            // On error, return error response with call_id
            try {
                JsonNode riskScoreJson = OBJECT_MAPPER.readTree(input);
                ObjectNode errorResponse = OBJECT_MAPPER.createObjectNode();
                errorResponse.put("call_id", riskScoreJson.path("call_id").asText(""));
                errorResponse.put("error", "Alert generation failed: " + errorMessage);
                errorResponse.set("alerts", OBJECT_MAPPER.createArrayNode());
                errorResponse.put("alert_count", 0);
                
                return OBJECT_MAPPER.writeValueAsString(errorResponse);
            } catch (Exception e) {
                LOG.error("Error handling failure for input: {}", input, e);
                return input; // Return as-is if we can't parse
            }
        }
    }
}

