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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Call Processing Agent - Classifies emergency calls using Gemini NLP.
 * 
 * Pipeline:
 *   emergency-calls (Kafka) → ClassifyCallAsyncFunction → processed-calls (Kafka)
 * 
 * The agent calls the Python FastAPI service at /api/classify-call
 * to classify each emergency call transcript.
 */
public class CallProcessingAgent {
    
    private static final Logger LOG = LoggerFactory.getLogger(CallProcessingAgent.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    // Configuration - can be overridden via environment variables
    // Defaults: localhost for local Flink, kafka:29092 for Docker Flink
    private static final String DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_SERVICE_URL = "http://localhost:8000/api/classify-call";
    private static final String INPUT_TOPIC = "emergency-calls";
    private static final String OUTPUT_TOPIC = "processed-calls";
    private static final String CONSUMER_GROUP_ID = "call-processing-agent";
    
    public static void main(String[] args) throws Exception {
        // Parse command line arguments (format: --kafka-bootstrap-servers kafka:29092 --service-url http://...)
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
        
        // Use environment variables as fallback if not set via command line
        // Command-line arguments take precedence
        if (kafkaBootstrapServers.equals(DEFAULT_KAFKA_BOOTSTRAP_SERVERS)) {
            String envKafka = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
            if (envKafka != null && !envKafka.isEmpty()) {
                kafkaBootstrapServers = envKafka;
            }
        }
        if (serviceUrl.equals(DEFAULT_SERVICE_URL)) {
            String envService = System.getenv("CLASSIFY_SERVICE_URL");
            if (envService != null && !envService.isEmpty()) {
                serviceUrl = envService;
            }
        }
        
        LOG.info("Starting Call Processing Agent");
        LOG.info("Configuration:");
        LOG.info("  Kafka bootstrap servers: {}", kafkaBootstrapServers);
        LOG.info("  Service URL: {}", serviceUrl);
        LOG.info("  Input topic: {}", INPUT_TOPIC);
        LOG.info("  Output topic: {}", OUTPUT_TOPIC);
        
        // Test service connectivity at startup
        try {
            java.net.http.HttpClient testClient = java.net.http.HttpClient.newBuilder()
                .connectTimeout(java.time.Duration.ofSeconds(5))
                .build();
            java.net.http.HttpRequest testRequest = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create(serviceUrl.replace("/api/classify-call", "/health")))
                .GET()
                .timeout(java.time.Duration.ofSeconds(5))
                .build();
            java.net.http.HttpResponse<String> testResponse = testClient.send(testRequest, 
                java.net.http.HttpResponse.BodyHandlers.ofString());
            if (testResponse.statusCode() == 200) {
                LOG.info("Service health check passed: {}", serviceUrl);
            } else {
                LOG.warn("Service health check returned status: {}", testResponse.statusCode());
            }
        } catch (Exception e) {
            LOG.warn("Service health check failed (service may not be ready yet): {}", e.getMessage());
        }
        
        // Create Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Can be increased for parallelism
        
        // Create Table environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        // Create Kafka source table
        // Note: Using 'raw' format since we're reading JSON strings directly
        String sourceTableDDL = String.format(
            "CREATE TABLE emergency_calls (\n" +
            "  call_json STRING\n" +
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
        Table sourceTable = tableEnv.from("emergency_calls");
        DataStream<String> calls = tableEnv.toDataStream(sourceTable, String.class);
        
        // Process calls with async HTTP function
        LOG.info("Creating async HTTP function for classification with service URL: {}", serviceUrl);
        ClassifyCallAsyncFunction classifyFunction = new ClassifyCallAsyncFunction(serviceUrl);
        LOG.info("ClassifyCallAsyncFunction created successfully");
        
        // Increase timeout for long transcripts (Gemini API can take time for complex calls)
        DataStream<String> processedCalls = AsyncDataStream.unorderedWait(
            calls,
            classifyFunction,
            60, // timeout seconds (increased from 30 for long transcripts)
            TimeUnit.SECONDS,
            100 // capacity (max concurrent requests)
        );
        LOG.info("AsyncDataStream created for classification processing");
        
        // Convert back to Table for Kafka sink
        // Need to specify schema explicitly to match sink table structure
        Table processedTable = tableEnv.fromDataStream(processedCalls).as("processed_call_json");
        tableEnv.createTemporaryView("processed_calls_stream", processedTable);
        LOG.info("Created temporary view 'processed_calls_stream' from DataStream");
        
        // Create Kafka sink table
        String sinkTableDDL = String.format(
            "CREATE TABLE processed_calls (\n" +
            "  processed_call_json STRING\n" +
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
        tableEnv.executeSql("INSERT INTO processed_calls SELECT * FROM processed_calls_stream");
    }
    
    /**
     * Async function to classify calls via HTTP.
     */
    private static class ClassifyCallAsyncFunction extends HttpAsyncFunction<String, String> {
        
        public ClassifyCallAsyncFunction(String serviceUrl) {
            super(serviceUrl, 60); // Increased timeout for long transcripts
        }
        
        @Override
        protected String buildRequestBody(String input) throws Exception {
            // Parse input JSON
            JsonNode callJson = OBJECT_MAPPER.readTree(input);
            
            // Extract transcript - try different possible field names
            String transcript = callJson.path("transcript").asText("");
            if (transcript.isEmpty()) {
                transcript = callJson.path("call_transcript").asText("");
            }
            if (transcript.isEmpty()) {
                transcript = callJson.path("text").asText("");
            }
            
            // Validate transcript is not empty (required field)
            if (transcript.isEmpty()) {
                throw new IllegalArgumentException("Transcript is required but was empty or missing in input: " + input);
            }
            
            // Extract call_id - try different possible field names
            String callId = callJson.path("callId").asText("");
            if (callId.isEmpty()) {
                callId = callJson.path("call_id").asText("");
            }
            if (callId.isEmpty()) {
                callId = callJson.path("id").asText("");
            }
            
            // Build request payload for classification service
            // ClassifyCallRequest requires: transcript (required), call_id (optional), metadata (optional)
            ObjectNode requestPayload = OBJECT_MAPPER.createObjectNode();
            requestPayload.put("transcript", transcript);
            
            // call_id is optional - only add if present
            if (!callId.isEmpty()) {
                requestPayload.put("call_id", callId);
            }
            
            // metadata is optional - only add if present and not null
            JsonNode metadata = callJson.path("metadata");
            if (!metadata.isMissingNode() && !metadata.isNull() && metadata.isObject()) {
                // Only add if it's a non-empty object or has meaningful content
                if (metadata.size() > 0) {
                    requestPayload.set("metadata", metadata);
                }
            }
            // If metadata is missing or empty, don't include it (optional field)
            
            String requestBody = OBJECT_MAPPER.writeValueAsString(requestPayload);
            LOG.info("Built classification request - Input (first 200 chars): {} | Request: {}", 
                input.length() > 200 ? input.substring(0, 200) + "..." : input, 
                requestBody);
            return requestBody;
        }
        
        @Override
        protected String parseResponse(String responseBody, String input) throws Exception {
            // Parse service response
            JsonNode responseJson = OBJECT_MAPPER.readTree(responseBody);
            JsonNode callJson = OBJECT_MAPPER.readTree(input);
            
            // Log response for debugging
            LOG.info("Classification response received. Full response body: {}", responseBody);
            
            // Extract classification from response
            JsonNode classification = responseJson.path("classification");
            if (classification.isMissingNode() || classification.isNull()) {
                // Log all available fields in response for debugging
                StringBuilder fields = new StringBuilder();
                responseJson.fieldNames().forEachRemaining(field -> {
                    if (fields.length() > 0) fields.append(", ");
                    fields.append(field);
                });
                LOG.error("Classification missing or null in response! Response body: {}. Response JSON fields: [{}]", 
                    responseBody, fields.toString());
                // This should not happen if API call succeeded - investigate why
            }
            
            // Combine original call data with classification
            // ProcessedCall requires: call_id, timestamp, caller_location, transcript, classification
            ObjectNode processedCall = OBJECT_MAPPER.createObjectNode();
            
            // call_id (required)
            String callId = responseJson.path("call_id").asText("");
            if (callId.isEmpty()) {
                callId = callJson.path("callId").asText("");
            }
            if (callId.isEmpty()) {
                callId = callJson.path("call_id").asText("");
            }
            if (callId.isEmpty()) {
                callId = "unknown-" + System.currentTimeMillis();
            }
            processedCall.put("call_id", callId);
            
            // timestamp (required)
            String timestamp = callJson.path("timestamp").asText("");
            if (timestamp.isEmpty()) {
                timestamp = java.time.Instant.now().toString();
            }
            processedCall.put("timestamp", timestamp);
            
            // caller_location (required) - convert from camelCase to snake_case if needed
            JsonNode callerLocation = callJson.path("callerLocation");
            if (callerLocation.isMissingNode()) {
                callerLocation = callJson.path("caller_location");
            }
            if (callerLocation.isMissingNode()) {
                callerLocation = callJson.path("location");
            }
            if (!callerLocation.isMissingNode() && !callerLocation.isNull()) {
                // Ensure it has lat and lon
                ObjectNode locationNode = OBJECT_MAPPER.createObjectNode();
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
                processedCall.set("caller_location", locationNode);
            } else {
                // Default location if missing
                ObjectNode defaultLocation = OBJECT_MAPPER.createObjectNode();
                defaultLocation.put("lat", 0.0);
                defaultLocation.put("lon", 0.0);
                processedCall.set("caller_location", defaultLocation);
            }
            
            // transcript (required)
            String transcript = callJson.path("transcript").asText("");
            if (transcript.isEmpty()) {
                transcript = callJson.path("text").asText("");
            }
            if (transcript.isEmpty()) {
                transcript = "[No transcript available]";
            }
            processedCall.put("transcript", transcript);
            
            // classification (required) - must not be null
            if (!classification.isMissingNode() && !classification.isNull()) {
                processedCall.set("classification", classification);
            } else {
                // Create a default classification if missing (better than null)
                // This should not happen if the API call succeeds, but handle gracefully
                ObjectNode defaultClassification = OBJECT_MAPPER.createObjectNode();
                defaultClassification.put("urgency", 3);
                defaultClassification.put("incident_type", "unknown");
                defaultClassification.put("confidence", 0.0);
                defaultClassification.set("entities", OBJECT_MAPPER.createObjectNode());
                defaultClassification.put("sentiment", 0.5);
                defaultClassification.set("key_phrases", OBJECT_MAPPER.createArrayNode());
                defaultClassification.putNull("people_affected");
                processedCall.set("classification", defaultClassification);
                LOG.warn("Classification missing in response, using default for call_id: {}. Response was: {}", callId, responseBody);
            }
            
            // metadata (optional)
            JsonNode metadata = callJson.path("metadata");
            if (!metadata.isMissingNode() && !metadata.isNull()) {
                processedCall.set("metadata", metadata);
            }
            
            return OBJECT_MAPPER.writeValueAsString(processedCall);
        }
        
        @Override
        protected String handleError(String input, int statusCode, String errorMessage) {
            // On error, return original call with default classification (not null)
            // ProcessedCall requires classification to be non-null
            try {
                JsonNode callJson = OBJECT_MAPPER.readTree(input);
                ObjectNode processedCall = OBJECT_MAPPER.createObjectNode();
                
                String callId = callJson.path("callId").asText("");
                if (callId.isEmpty()) {
                    callId = callJson.path("call_id").asText("");
                }
                processedCall.put("call_id", callId);
                processedCall.put("timestamp", callJson.path("timestamp").asText(""));
                
                // Handle caller_location
                JsonNode callerLocation = callJson.path("callerLocation");
                if (callerLocation.isMissingNode()) {
                    callerLocation = callJson.path("caller_location");
                }
                if (!callerLocation.isMissingNode() && !callerLocation.isNull()) {
                    ObjectNode locationNode = OBJECT_MAPPER.createObjectNode();
                    locationNode.put("lat", callerLocation.path("lat").asDouble(0.0));
                    locationNode.put("lon", callerLocation.path("lon").asDouble(0.0));
                    processedCall.set("caller_location", locationNode);
                } else {
                    ObjectNode defaultLocation = OBJECT_MAPPER.createObjectNode();
                    defaultLocation.put("lat", 0.0);
                    defaultLocation.put("lon", 0.0);
                    processedCall.set("caller_location", defaultLocation);
                }
                
                processedCall.put("transcript", callJson.path("transcript").asText(""));
                
                // Create default classification (required field, cannot be null)
                ObjectNode defaultClassification = OBJECT_MAPPER.createObjectNode();
                defaultClassification.put("urgency", 3);
                defaultClassification.put("incident_type", "unknown");
                defaultClassification.put("confidence", 0.0);
                defaultClassification.set("entities", OBJECT_MAPPER.createObjectNode());
                defaultClassification.put("sentiment", 0.5);
                defaultClassification.set("key_phrases", OBJECT_MAPPER.createArrayNode());
                defaultClassification.putNull("people_affected");
                processedCall.set("classification", defaultClassification);
                
                processedCall.set("metadata", callJson.path("metadata"));
                
                LOG.warn("Classification API call failed (status: {}), using default classification for call_id: {}. Error: {}", 
                    statusCode, callId, errorMessage);
                
                return OBJECT_MAPPER.writeValueAsString(processedCall);
            } catch (Exception e) {
                LOG.error("Error handling failure for input (first 500 chars): {}", 
                    input.length() > 500 ? input.substring(0, 500) + "..." : input, e);
                // Even in error case, try to create a valid processed call
                try {
                    ObjectNode processedCall = OBJECT_MAPPER.createObjectNode();
                    processedCall.put("call_id", "error-" + System.currentTimeMillis());
                    processedCall.put("timestamp", java.time.Instant.now().toString());
                    ObjectNode defaultLocation = OBJECT_MAPPER.createObjectNode();
                    defaultLocation.put("lat", 0.0);
                    defaultLocation.put("lon", 0.0);
                    processedCall.set("caller_location", defaultLocation);
                    processedCall.put("transcript", "[Error processing call]");
                    ObjectNode defaultClassification = OBJECT_MAPPER.createObjectNode();
                    defaultClassification.put("urgency", 3);
                    defaultClassification.put("incident_type", "unknown");
                    defaultClassification.put("confidence", 0.0);
                    defaultClassification.set("entities", OBJECT_MAPPER.createObjectNode());
                    defaultClassification.put("sentiment", 0.5);
                    defaultClassification.set("key_phrases", OBJECT_MAPPER.createArrayNode());
                    processedCall.set("classification", defaultClassification);
                    return OBJECT_MAPPER.writeValueAsString(processedCall);
                } catch (Exception e2) {
                    LOG.error("Failed to create error response, returning minimal JSON", e2);
                    // Last resort: return minimal valid JSON
                    try {
                        ObjectNode minimal = OBJECT_MAPPER.createObjectNode();
                        minimal.put("call_id", "error-" + System.currentTimeMillis());
                        minimal.put("timestamp", java.time.Instant.now().toString());
                        ObjectNode loc = OBJECT_MAPPER.createObjectNode();
                        loc.put("lat", 0.0);
                        loc.put("lon", 0.0);
                        minimal.set("caller_location", loc);
                        minimal.put("transcript", "[Error]");
                        ObjectNode cls = OBJECT_MAPPER.createObjectNode();
                        cls.put("urgency", 3);
                        cls.put("incident_type", "unknown");
                        cls.put("confidence", 0.0);
                        cls.set("entities", OBJECT_MAPPER.createObjectNode());
                        cls.put("sentiment", 0.5);
                        cls.set("key_phrases", OBJECT_MAPPER.createArrayNode());
                        minimal.set("classification", cls);
                        return OBJECT_MAPPER.writeValueAsString(minimal);
                    } catch (Exception e3) {
                        LOG.error("Complete failure in handleError, returning empty JSON", e3);
                        return "{\"error\":\"complete_failure\"}";
                    }
                }
            }
        }
    }
}

