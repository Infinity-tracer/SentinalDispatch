package com.sentineldispatch.agents.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for async HTTP functions that call Python FastAPI services.
 * 
 * This allows Flink agents to call external Python services via HTTP
 * without blocking the stream processing.
 */
public abstract class HttpAsyncFunction<T, R> extends RichAsyncFunction<T, R> {
    
    private static final Logger LOG = LoggerFactory.getLogger(HttpAsyncFunction.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    protected transient HttpClient httpClient;
    protected final String serviceUrl;
    protected final int timeoutSeconds;
    
    /**
     * Safely complete the ResultFuture, ensuring it's only completed once.
     * This prevents "Could not complete the stream element" errors when
     * multiple code paths try to complete the same future.
     * 
     * Uses AtomicBoolean per invocation to track completion state.
     */
    protected void safeComplete(ResultFuture<R> resultFuture, R result, AtomicBoolean completed) {
        if (resultFuture == null || completed == null) {
            LOG.warn("ResultFuture or completed flag is null, cannot safely complete");
            return;
        }
        
        // Atomically check and set completion flag
        if (completed.compareAndSet(false, true)) {
            try {
                resultFuture.complete(Collections.singleton(result));
            } catch (IllegalStateException e) {
                // Future was already completed - this is OK, just log it
                LOG.debug("ResultFuture already completed (IllegalStateException), ignoring: {}", e.getMessage());
            } catch (Exception e) {
                // Unexpected error - log it but don't throw
                // The future might already be completed or in an invalid state
                LOG.error("Unexpected error completing ResultFuture: {}", e.getMessage(), e);
            }
        } else {
            // Already completed by another thread - this is OK, just log it
            LOG.debug("ResultFuture already completed, ignoring duplicate completion attempt");
        }
    }
    
    public HttpAsyncFunction(String serviceUrl) {
        this(serviceUrl, 30);
    }
    
    public HttpAsyncFunction(String serviceUrl, int timeoutSeconds) {
        this.serviceUrl = serviceUrl;
        this.timeoutSeconds = timeoutSeconds;
    }
    
    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        // Force HTTP/1.1 to avoid HTTP/2 upgrade issues with Uvicorn
        this.httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)  // Force HTTP/1.1 (Uvicorn doesn't support HTTP/2 upgrade)
            .connectTimeout(Duration.ofSeconds(5))
            .build();
        LOG.info("Initialized HTTP client for service: {} (HTTP/1.1)", serviceUrl);
    }
    
    @Override
    public void asyncInvoke(T input, ResultFuture<R> resultFuture) {
        // Track completion state for this invocation
        // Store in a way that timeout() can access it
        final AtomicBoolean completed = new AtomicBoolean(false);
        final T inputCopy = input;  // Final reference for timeout handler
        
        try {
            // Build request payload from input
            String requestBody = buildRequestBody(input);
            LOG.info("Calling service: {} with request body: {}", serviceUrl, requestBody);
            
            // Create HTTP request
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(serviceUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .build();
            
            // Make async HTTP call
            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenAccept(response -> {
                    try {
                        LOG.info("Service response status: {} for URL: {}", response.statusCode(), serviceUrl);
                        String responseBody = response.body();
                        if (responseBody == null || responseBody.isEmpty()) {
                            LOG.error("Empty response body from service: {}", serviceUrl);
                            R errorResult = handleError(inputCopy, response.statusCode(), "Empty response body");
                            safeComplete(resultFuture, errorResult, completed);
                            return;
                        }
                        LOG.info("Service response body (first 500 chars): {}", 
                            responseBody.length() > 500 ? responseBody.substring(0, 500) + "..." : responseBody);
                        if (response.statusCode() == 200) {
                            try {
                                R result = parseResponse(responseBody, inputCopy);
                                safeComplete(resultFuture, result, completed);
                            } catch (Exception parseEx) {
                                LOG.error("Error parsing response from service: {}. Response body: {}. Parse error: {}", 
                                    serviceUrl, responseBody, parseEx.getMessage(), parseEx);
                                R errorResult = handleError(inputCopy, response.statusCode(), "Parse error: " + parseEx.getMessage());
                                safeComplete(resultFuture, errorResult, completed);
                            }
                        } else {
                            LOG.warn("Service returned error status: {} for URL: {}. Full response body: {}", 
                                response.statusCode(), serviceUrl, responseBody);
                            try {
                                R errorResult = handleError(inputCopy, response.statusCode(), responseBody);
                                safeComplete(resultFuture, errorResult, completed);
                            } catch (Exception handleEx) {
                                LOG.error("Error in handleError for non-200 response. Status: {}. Error: {}", 
                                    response.statusCode(), handleEx.getMessage(), handleEx);
                                // Last resort: try to return input as-is
                                safeComplete(resultFuture, (R) inputCopy, completed);
                            }
                        }
                    } catch (Exception e) {
                        LOG.error("Error processing response from service: {}. Error: {}", 
                            serviceUrl, e.getMessage(), e);
                        try {
                            R errorResult = handleError(inputCopy, -1, e.getMessage());
                            safeComplete(resultFuture, errorResult, completed);
                        } catch (Exception handleEx) {
                            LOG.error("Error in handleError during response processing. Error: {}", 
                                handleEx.getMessage(), handleEx);
                            // Last resort: try to return input as-is
                            safeComplete(resultFuture, (R) inputCopy, completed);
                        }
                    }
                })
                .exceptionally(throwable -> {
                    try {
                        LOG.error("Error calling service: {}. Error: {}", serviceUrl, throwable.getMessage(), throwable);
                        R errorResult = handleError(inputCopy, -1, throwable.getMessage());
                        safeComplete(resultFuture, errorResult, completed);
                    } catch (Exception handleEx) {
                        LOG.error("Error in handleError for service: {}. Original error: {}. HandleError error: {}", 
                            serviceUrl, throwable.getMessage(), handleEx.getMessage(), handleEx);
                        // Last resort: try to return input as-is
                        safeComplete(resultFuture, (R) inputCopy, completed);
                    }
                    return null;
                });
                
        } catch (Exception e) {
            try {
                LOG.error("Error building request for service: {}", serviceUrl, e);
                R errorResult = handleError(inputCopy, -1, e.getMessage());
                safeComplete(resultFuture, errorResult, completed);
            } catch (Exception handleEx) {
                LOG.error("Error in handleError during request building for service: {}. Original error: {}. HandleError error: {}", 
                    serviceUrl, e.getMessage(), handleEx.getMessage(), handleEx);
                // Last resort: try to return input as-is
                safeComplete(resultFuture, (R) inputCopy, completed);
            }
        }
    }
    
    @Override
    public void timeout(T input, ResultFuture<R> resultFuture) throws Exception {
        // This is called by Flink when the async operation times out
        // We need to complete the ResultFuture with an error result
        LOG.warn("Async operation timed out for service: {}, completing with error result", serviceUrl);
        try {
            R errorResult = handleError(input, -1, "Async operation timed out after " + timeoutSeconds + " seconds");
            // Use a new AtomicBoolean since we don't have access to the original one
            // This is safe because timeout() is only called if the future hasn't been completed
            AtomicBoolean timeoutCompleted = new AtomicBoolean(false);
            safeComplete(resultFuture, errorResult, timeoutCompleted);
        } catch (Exception e) {
            LOG.error("Error in timeout handler for service: {}", serviceUrl, e);
            // Last resort: try to return input as-is
            AtomicBoolean timeoutCompleted = new AtomicBoolean(false);
            safeComplete(resultFuture, (R) input, timeoutCompleted);
        }
    }
    
    /**
     * Build JSON request body from input.
     * Subclasses should implement this to format the request.
     */
    protected abstract String buildRequestBody(T input) throws Exception;
    
    /**
     * Parse HTTP response and create result object.
     * Subclasses should implement this to parse the service response.
     */
    protected abstract R parseResponse(String responseBody, T input) throws Exception;
    
    /**
     * Handle errors (non-200 status or exceptions).
     * Subclasses can override to provide custom error handling.
     */
    protected R handleError(T input, int statusCode, String errorMessage) {
        // Default: return input as-is (pass through on error)
        // Subclasses should override for better error handling
        LOG.warn("Error handling not implemented, returning input as-is");
        return (R) input;
    }
    
    protected ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }
}

