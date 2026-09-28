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
 * Data Enrichment Agent - Enriches processed calls with weather and fire data.
 * 
 * Pipeline:
 *   processed-calls (Kafka) → EnrichCallAsyncFunction → enriched-calls (Kafka)
 * 
 * The agent calls the Python FastAPI service at /api/enrich-call
 * to enrich processed calls with spatial-temporal weather and fire data.
 */
public class DataEnrichmentAgent {
    
    private static final Logger LOG = LoggerFactory.getLogger(DataEnrichmentAgent.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    private static final String DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_SERVICE_URL = "http://localhost:8000/api/enrich-call";
    private static final String INPUT_TOPIC = "processed-calls";
    private static final String OUTPUT_TOPIC = "enriched-calls";
    private static final String CONSUMER_GROUP_ID = "data-enrichment-agent";
    
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
            String envService = System.getenv("ENRICH_SERVICE_URL");
            if (envService != null && !envService.isEmpty()) {
                serviceUrl = envService;
            }
        }
        
        LOG.info("Starting Data Enrichment Agent");
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
            "CREATE TABLE processed_calls (\n" +
            "  processed_call_json STRING\n" +
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
        Table sourceTable = tableEnv.from("processed_calls");
        DataStream<String> processedCalls = tableEnv.toDataStream(sourceTable, String.class);
        
        // Process calls with async HTTP function
        // Increased timeout to 60 seconds for data enrichment (weather/fire data lookups can take time)
        DataStream<String> enrichedCalls = AsyncDataStream.unorderedWait(
            processedCalls,
            new EnrichCallAsyncFunction(serviceUrl),
            60,
            TimeUnit.SECONDS,
            100
        );
        
        // Convert back to Table for Kafka sink
        Table enrichedTable = tableEnv.fromDataStream(enrichedCalls);
        tableEnv.createTemporaryView("enriched_calls_stream", enrichedTable);
        
        // Create Kafka sink table
        String sinkTableDDL = String.format(
            "CREATE TABLE enriched_calls (\n" +
            "  enriched_call_json STRING\n" +
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
        tableEnv.executeSql("INSERT INTO enriched_calls SELECT * FROM enriched_calls_stream");
    }
    
    /**
     * Async function to enrich calls via HTTP.
     */
    private static class EnrichCallAsyncFunction extends HttpAsyncFunction<String, String> {
        
        public EnrichCallAsyncFunction(String serviceUrl) {
            super(serviceUrl, 60); // Increased timeout to 60 seconds for data enrichment
        }
        
        @Override
        protected String buildRequestBody(String input) throws Exception {
            // Parse processed call JSON
            JsonNode processedCallJson = OBJECT_MAPPER.readTree(input);
            
            // Validate and normalize processed_call to match ProcessedCall schema
            // Required fields: call_id, timestamp, caller_location, transcript, classification
            ObjectNode normalizedProcessedCall = OBJECT_MAPPER.createObjectNode();
            
            // call_id (required)
            String callId = processedCallJson.path("call_id").asText("");
            if (callId.isEmpty()) {
                callId = processedCallJson.path("callId").asText("");
            }
            if (callId.isEmpty()) {
                throw new IllegalArgumentException("call_id is required in processed_call: " + input);
            }
            normalizedProcessedCall.put("call_id", callId);
            
            // timestamp (required)
            String timestamp = processedCallJson.path("timestamp").asText("");
            if (timestamp.isEmpty()) {
                timestamp = java.time.Instant.now().toString();
            }
            normalizedProcessedCall.put("timestamp", timestamp);
            
            // caller_location (required) - ensure it's in the correct format
            JsonNode callerLocation = processedCallJson.path("caller_location");
            if (callerLocation.isMissingNode()) {
                callerLocation = processedCallJson.path("callerLocation");
            }
            if (!callerLocation.isMissingNode() && !callerLocation.isNull()) {
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
                normalizedProcessedCall.set("caller_location", locationNode);
            } else {
                throw new IllegalArgumentException("caller_location is required in processed_call: " + input);
            }
            
            // transcript (required)
            String transcript = processedCallJson.path("transcript").asText("");
            if (transcript.isEmpty()) {
                throw new IllegalArgumentException("transcript is required in processed_call: " + input);
            }
            normalizedProcessedCall.put("transcript", transcript);
            
            // classification (required)
            JsonNode classification = processedCallJson.path("classification");
            if (classification.isMissingNode() || classification.isNull()) {
                throw new IllegalArgumentException("classification is required in processed_call: " + input);
            }
            normalizedProcessedCall.set("classification", classification);
            
            // metadata (optional)
            JsonNode metadata = processedCallJson.path("metadata");
            if (!metadata.isMissingNode() && !metadata.isNull()) {
                normalizedProcessedCall.set("metadata", metadata);
            }
            
            // Build request payload for enrichment service
            ObjectNode requestPayload = OBJECT_MAPPER.createObjectNode();
            requestPayload.set("processed_call", normalizedProcessedCall);
            // Optional: pass empty arrays for weather/fire data (service will fetch if needed)
            requestPayload.set("weather_data_list", OBJECT_MAPPER.createArrayNode());
            requestPayload.set("fire_data_list", OBJECT_MAPPER.createArrayNode());
            
            return OBJECT_MAPPER.writeValueAsString(requestPayload);
        }
        
        @Override
        protected String parseResponse(String responseBody, String input) throws Exception {
            // Parse service response
            JsonNode responseJson = OBJECT_MAPPER.readTree(responseBody);
            
            // Return enriched call from response
            JsonNode enrichedCall = responseJson.path("enriched_call");
            if (enrichedCall.isMissingNode()) {
                // Fallback: return original processed call if enrichment failed
                return input;
            }
            
            return OBJECT_MAPPER.writeValueAsString(enrichedCall);
        }
        
        @Override
        protected String handleError(String input, int statusCode, String errorMessage) {
            // On error, return original processed call
            LOG.warn("Enrichment failed for call, returning original: {}", errorMessage);
            return input;
        }
    }
}
