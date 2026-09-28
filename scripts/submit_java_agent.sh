#!/usr/bin/env bash
#
# Script to submit Java Flink agents to Flink cluster.
#
# Usage:
#   ./scripts/submit_java_agent.sh [agent_name]
#
#   If no agent_name is provided, all agents are submitted.
#
#   Available agents:
#   - call-processing
#   - data-enrichment
#   - risk-scoring
#   - alert-generation
#   - all (explicitly submit all agents)
#
# Environment Variables:
#   - FLINK_HOME: Path to Flink installation (default: ~/flink-2.1.1)
#   - FLINK_JOBMANAGER: JobManager address (default: localhost:8081)
#   - USE_DOCKER: Set to "true" to force Docker mode
#   - KAFKA_BOOTSTRAP_SERVERS: Kafka bootstrap servers
#   - CLASSIFY_SERVICE_URL: Classification service URL
#   - ENRICH_SERVICE_URL: Enrichment service URL
#   - SCORE_SERVICE_URL: Risk scoring service URL
#   - ALERT_SERVICE_URL: Alert generation service URL

set -eo pipefail

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FLINK_AGENTS_DIR="$PROJECT_ROOT/flink-agents"

# Default values
AGENT_NAME="${1:-all}"
FLINK_HOME="${FLINK_HOME:-$HOME/flink-2.1.1}"
FLINK_JOBMANAGER="${FLINK_JOBMANAGER:-localhost:8081}"

# List of all available agents
ALL_AGENTS=("call-processing" "data-enrichment" "risk-scoring" "alert-generation")

# Agent configuration (using function instead of associative array for portability)
get_agent_class() {
    case "$1" in
        call-processing)
            echo "com.sentineldispatch.agents.CallProcessingAgent"
            ;;
        data-enrichment)
            echo "com.sentineldispatch.agents.DataEnrichmentAgent"
            ;;
        risk-scoring)
            echo "com.sentineldispatch.agents.RiskScoringAgent"
            ;;
        alert-generation)
            echo "com.sentineldispatch.agents.AlertGenerationAgent"
            ;;
        *)
            echo ""
            ;;
    esac
}

# Function to submit a single agent
submit_agent() {
    local agent_name="$1"
    local agent_class=$(get_agent_class "$agent_name")
    
    if [[ -z "$agent_class" ]]; then
        echo -e "${RED}Error: Unknown agent '$agent_name'${NC}" >&2
        return 1
    fi
    
    # Check if JAR exists
    local jar_path="$FLINK_AGENTS_DIR/target/flink-agents-1.0.0.jar"
    if [[ ! -f "$jar_path" ]]; then
        echo -e "${YELLOW}JAR not found. Building...${NC}"
        cd "$FLINK_AGENTS_DIR"
        mvn clean package
        cd "$PROJECT_ROOT"
        
        if [[ ! -f "$jar_path" ]]; then
            echo -e "${RED}Error: Failed to build JAR${NC}" >&2
            return 1
        fi
    fi
    
    # Check if using Docker Flink
    local use_docker=false
    if [[ "${USE_DOCKER:-false}" == "true" ]]; then
        use_docker=true
    elif docker ps --format '{{.Names}}' | grep -qE '(sentinel-dispatch-flink-jobmanager|flink-jobmanager)'; then
        use_docker=true
    fi
    
    # Submit to Flink
    if [[ "$use_docker" == "true" ]]; then
        # Determine container name
        local jobmanager_container=""
        if docker ps --format '{{.Names}}' | grep -q "sentinel-dispatch-flink-jobmanager"; then
            jobmanager_container="sentinel-dispatch-flink-jobmanager"
        elif docker ps --format '{{.Names}}' | grep -q "flink-jobmanager"; then
            jobmanager_container="flink-jobmanager"
        else
            echo -e "${RED}Error: Flink JobManager container not found${NC}" >&2
            return 1
        fi
        
        echo -e "${GREEN}Submitting $agent_name to Docker Flink cluster...${NC}"
        echo "  Class: $agent_class"
        echo "  JAR: $jar_path"
        
        # Copy JAR to container
        docker cp "$jar_path" "$jobmanager_container:/tmp/flink-agents-$agent_name.jar"
        
        # Set Kafka and service URLs for Docker Flink
        local kafka_bootstrap_servers="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
        
        # Determine service URL based on agent type
        local service_url=""
        case "$agent_name" in
            call-processing)
                service_url="${CLASSIFY_SERVICE_URL:-http://ml-api-service:8000/api/classify-call}"
                ;;
            data-enrichment)
                service_url="${ENRICH_SERVICE_URL:-http://ml-api-service:8000/api/enrich-call}"
                ;;
            risk-scoring)
                service_url="${SCORE_SERVICE_URL:-http://ml-api-service:8000/api/score-risk}"
                ;;
            alert-generation)
                service_url="${ALERT_SERVICE_URL:-http://ml-api-service:8000/api/generate-alert}"
                ;;
            *)
                service_url="${CLASSIFY_SERVICE_URL:-http://ml-api-service:8000/api/classify-call}"
                ;;
        esac
        
        echo "  Kafka: $kafka_bootstrap_servers"
        echo "  Service: $service_url"
        echo ""
        
        # Submit job with configuration via command line arguments
        docker exec "$jobmanager_container" \
            /opt/flink/bin/flink run \
            --jobmanager localhost:8081 \
            --class "$agent_class" \
            /tmp/flink-agents-$agent_name.jar \
            --kafka-bootstrap-servers "$kafka_bootstrap_servers" \
            --service-url "$service_url"
    else
        # Local Flink submission
        if [[ ! -d "$FLINK_HOME" ]]; then
            echo -e "${RED}Error: Flink not found at $FLINK_HOME${NC}" >&2
            echo "Please set FLINK_HOME or install Flink 2.1.1" >&2
            return 1
        fi
        
        local flink_cli="$FLINK_HOME/bin/flink"
        if [[ ! -f "$flink_cli" ]]; then
            echo -e "${RED}Error: Flink CLI not found at $flink_cli${NC}" >&2
            return 1
        fi
        
        echo -e "${GREEN}Submitting $agent_name to local Flink cluster...${NC}"
        echo "  Class: $agent_class"
        echo "  Flink: $FLINK_HOME"
        echo "  JAR: $jar_path"
        echo ""
        
        "$flink_cli" run \
            --class "$agent_class" \
            "$jar_path"
    fi
    
    return 0
}

# Handle "all" case
if [[ "$AGENT_NAME" == "all" ]]; then
    echo -e "${GREEN}Submitting all agents...${NC}"
    echo ""
    
    failed=0
    for agent in "${ALL_AGENTS[@]}"; do
        if ! submit_agent "$agent"; then
            echo -e "${RED}Failed to submit $agent${NC}" >&2
            failed=1
        else
            echo -e "${GREEN}✓ $agent submitted successfully${NC}"
        fi
        echo ""
    done
    
    if [[ $failed -eq 0 ]]; then
        echo -e "${GREEN}✓ All agents submitted successfully${NC}"
        echo "  Check Flink dashboard: http://localhost:8081"
    else
        echo -e "${YELLOW}⚠ Some agents failed to submit${NC}" >&2
        exit 1
    fi
    exit 0
fi

# Single agent submission
AGENT_CLASS=$(get_agent_class "$AGENT_NAME")

# Check if agent exists
if [[ -z "$AGENT_CLASS" ]]; then
    echo -e "${RED}Error: Unknown agent '$AGENT_NAME'${NC}" >&2
    echo "" >&2
    echo "Available agents:" >&2
    echo "  - call-processing" >&2
    echo "  - data-enrichment" >&2
    echo "  - risk-scoring" >&2
    echo "  - alert-generation" >&2
    echo "  - all (submit all agents)" >&2
    exit 1
fi

# Submit single agent
if submit_agent "$AGENT_NAME"; then
    echo ""
    echo -e "${GREEN}✓ Agent '$AGENT_NAME' submitted successfully${NC}"
    echo "  Check Flink dashboard: http://localhost:8081"
else
    echo -e "${RED}Failed to submit agent '$AGENT_NAME'${NC}" >&2
    exit 1
fi
