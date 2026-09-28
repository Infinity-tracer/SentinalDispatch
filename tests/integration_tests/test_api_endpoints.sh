#!/usr/bin/env bash
# Test script to diagnose 422 errors in API endpoints

set -e

API_URL="${API_URL:-http://localhost:8000}"

echo "Testing API endpoints..."
echo "API URL: $API_URL"
echo ""

# Function to test an endpoint
test_endpoint() {
    local name=$1
    local endpoint=$2
    local data=$3
    
    echo "=========================================="
    echo "Testing $name: $endpoint"
    echo "=========================================="
    
    # Save response to temp files
    local temp_file=$(mktemp)
    local status_file=$(mktemp)
    
    # Get response body and status code separately
    # -o writes body to file, -w writes status code to stdout (redirected to status_file)
    curl -X POST "$API_URL$endpoint" \
      -H "Content-Type: application/json" \
      -d "$data" \
      -s \
      -o "$temp_file" \
      -w "%{http_code}" > "$status_file"
    
    # Read response body and status code
    local response_body=$(cat "$temp_file")
    local http_status=$(cat "$status_file")
    
    echo "HTTP Status: $http_status"
    echo ""
    echo "Raw Response:"
    echo "$response_body"
    echo ""
    
    # Try to parse as JSON if status is 200
    if [ "$http_status" = "200" ]; then
        echo "Parsed JSON:"
        echo "$response_body" | jq '.' 2>/dev/null || echo "  (Not valid JSON)"
    else
        echo "Error Response:"
        echo "$response_body" | jq '.' 2>/dev/null || echo "$response_body"
    fi
    
    rm -f "$temp_file" "$status_file"
    echo ""
}

# Test classify-call endpoint
test_endpoint "1. /api/classify-call" "/api/classify-call" '{
  "transcript": "I need help! There is smoke everywhere and I cannot breathe!",
  "call_id": "test-call-001"
}'

# Test enrich-call endpoint
test_endpoint "2. /api/enrich-call" "/api/enrich-call" '{
  "processed_call": {
    "call_id": "test-call-001",
    "timestamp": "2023-08-08T10:30:00Z",
    "caller_location": {"lat": 20.90, "lon": -156.68},
    "transcript": "I need help! There is smoke everywhere!",
    "classification": {
      "urgency": 5,
      "incident_type": "fire",
      "entities": {},
      "sentiment": 0.9,
      "confidence": 0.95
    }
  },
  "weather_data_list": [],
  "fire_data_list": []
}'

# Test score-risk endpoint
test_endpoint "3. /api/score-risk" "/api/score-risk" '{
  "call_id": "test-call-001",
  "call_timestamp": "2023-08-08T10:30:00Z",
  "caller_location": {"lat": 20.90, "lon": -156.68},
  "call_classification": {
    "urgency": 5,
    "incident_type": "fire",
    "entities": {},
    "sentiment": 0.9,
    "confidence": 0.95
  }
}'

# Test generate-alert endpoint
test_endpoint "4. /api/generate-alert" "/api/generate-alert" '{
  "risk_score": {
    "call_id": "test-call-001",
    "timestamp": "2023-08-08T10:30:00Z",
    "risk_score": 85.0,
    "confidence": 0.95,
    "factors": {
      "call_urgency": 1.0,
      "fire_proximity": 0.8,
      "vulnerability": 0.5,
      "fire_spread_rate": 0.6,
      "weather_risk": 0.4
    },
    "recommendation": "IMMEDIATE_DISPATCH",
    "priority": 1
  }
}'

echo "Done testing endpoints."

