"""
Data Enrichment API router.
"""

import json
import os
from datetime import datetime, timezone
from typing import List, Dict, Any

from confluent_kafka import Consumer, KafkaError, TopicPartition
from fastapi import APIRouter, HTTPException, status

from sentinel_dispatch.ai_ml import enrich_call
from sentinel_dispatch.api.schemas import EnrichCallRequest, EnrichCallResponse, ErrorResponse
from sentinel_dispatch.utils.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/enrich-call", tags=["enrichment"])


def _fetch_recent_kafka_messages(
    topic: str,
    bootstrap_servers: str,
    max_messages: int = 1000,  # Increased to get more historical data
    timeout_seconds: float = 5.0,  # Increased timeout for more messages
) -> List[Dict[str, Any]]:
    """
    Fetch recent messages from a Kafka topic.
    
    Args:
        topic: Kafka topic name
        bootstrap_servers: Kafka bootstrap servers
        max_messages: Maximum number of messages to fetch
        timeout_seconds: Timeout for consuming messages
        
    Returns:
        List of message values (parsed JSON)
    """
    messages = []
    consumer = None
    try:
        # Use a unique consumer group ID with timestamp to avoid conflicts
        consumer_group_id = f'enrichment-service-{topic}-{int(datetime.now().timestamp() * 1000)}'
        
        consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': consumer_group_id,
            'auto.offset.reset': 'earliest',  # Start from earliest to get all available messages
            'enable.auto.commit': False,
        })
        
        # Subscribe to topic
        consumer.subscribe([topic])
        
        # Wait for assignment
        assignment_timeout = 5.0
        start_assignment = datetime.now()
        while True:
            if (datetime.now() - start_assignment).total_seconds() > assignment_timeout:
                logger.warning(
                    "sentinel_dispatch.api.routers.enrich._fetch_recent_kafka_messages.assignment_timeout",
                    topic=topic
                )
                break
            
            partitions = consumer.assignment()
            if partitions:
                break
            consumer.poll(timeout=0.1)
        
        # Get all partitions and seek to beginning to get all available messages
        # Then we'll collect the most recent ones
        partitions = consumer.assignment()
        for partition in partitions:
            try:
                # Get partition number and topic name
                partition_num = partition.partition if hasattr(partition, 'partition') else partition
                topic_name = partition.topic if hasattr(partition, 'topic') else topic
                
                # Create TopicPartition object for get_watermark_offsets
                topic_partition = TopicPartition(topic_name, partition_num)
                low, high = consumer.get_watermark_offsets(topic_partition, timeout=1.0)
                
                # Seek to beginning to get all available messages
                # We'll collect up to max_messages, prioritizing the most recent
                if high > 0:
                    # Seek to beginning to get all messages, then we'll take the last max_messages
                    seek_offset = low
                    # seek() takes a TopicPartition object with offset set
                    topic_partition.offset = seek_offset
                    consumer.seek(topic_partition)
                    logger.debug(
                        "sentinel_dispatch.api.routers.enrich._fetch_recent_kafka_messages.seek",
                        topic=topic_name,
                        partition=partition_num,
                        offset=seek_offset,
                        low=low,
                        high=high
                    )
            except Exception as e:
                logger.warning(
                    "sentinel_dispatch.api.routers.enrich._fetch_recent_kafka_messages.seek_failed",
                    topic=topic,
                    partition=partition.partition if hasattr(partition, 'partition') else partition,
                    error=str(e)
                )
        
        # Consume ALL available messages (not just recent ones)
        # The time window filtering in the enricher will match based on call timestamp
        start_time = datetime.now()
        all_messages = []
        consecutive_nulls = 0
        max_consecutive_nulls = 10  # Stop after 10 consecutive nulls
        
        while len(all_messages) < max_messages:
            if (datetime.now() - start_time).total_seconds() > timeout_seconds:
                break
                
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                consecutive_nulls += 1
                if consecutive_nulls >= max_consecutive_nulls:
                    # No more messages available
                    break
                continue
            
            consecutive_nulls = 0  # Reset counter when we get a message
                    
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Reached end of partition - we've consumed all available messages
                    logger.debug(
                        "sentinel_dispatch.api.routers.enrich._fetch_recent_kafka_messages.partition_eof",
                        topic=topic,
                        messages_collected=len(all_messages)
                    )
                    break
                logger.warning(
                    "sentinel_dispatch.api.routers.enrich._fetch_recent_kafka_messages.consumer_error",
                    topic=topic,
                    error=str(msg.error())
                )
                break
            
            try:
                value = json.loads(msg.value().decode('utf-8'))
                all_messages.append(value)
            except Exception as e:
                logger.warning(
                    "sentinel_dispatch.api.routers.enrich._fetch_recent_kafka_messages.parse_error",
                    topic=topic,
                    error=str(e)
                )
        
        # Return all messages - the enricher will filter by time window relative to call timestamp
        messages = all_messages
        
        logger.info(
            "sentinel_dispatch.api.routers.enrich._fetch_recent_kafka_messages.completed",
            topic=topic,
            message_count=len(messages)
        )
    except Exception as e:
        logger.error(
            "sentinel_dispatch.api.routers.enrich._fetch_recent_kafka_messages.error",
            topic=topic,
            error=str(e),
            exc_info=True
        )
    finally:
        if consumer:
            try:
                consumer.close()
            except Exception:
                pass
    
    return messages


@router.post(
    "",
    response_model=EnrichCallResponse,
    status_code=status.HTTP_200_OK,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid request"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def enrich_call_endpoint(request: EnrichCallRequest) -> EnrichCallResponse:
    """
    Enrich a processed call with weather and fire data using spatial-temporal joins.
    
    Args:
        request: Enrichment request with processed call and optional weather/fire data
        
    Returns:
        Enriched call with weather and fire analysis
        
    Raises:
        HTTPException: If enrichment fails
    """
    call_id = request.processed_call.call_id
    
    logger.info(
        "sentinel_dispatch.api.routers.enrich.enrich_call_endpoint.received",
        call_id=call_id,
        has_weather_data=request.weather_data_list is not None and len(request.weather_data_list) > 0,
        has_fire_data=request.fire_data_list is not None and len(request.fire_data_list) > 0
    )
    
    try:
        # If weather/fire data lists are empty, fetch from Kafka topics
        weather_data_list = request.weather_data_list or []
        fire_data_list = request.fire_data_list or []
        
        if not weather_data_list or not fire_data_list:
            bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
            
            if not weather_data_list:
                logger.info(
                    "sentinel_dispatch.api.routers.enrich.enrich_call_endpoint.fetching_weather",
                    call_id=call_id
                )
                weather_data_list = _fetch_recent_kafka_messages("weather-data", bootstrap_servers)
            
            if not fire_data_list:
                logger.info(
                    "sentinel_dispatch.api.routers.enrich.enrich_call_endpoint.fetching_fire",
                    call_id=call_id
                )
                fire_data_list = _fetch_recent_kafka_messages("fire-data", bootstrap_servers)
        
        # Log sample data structure for debugging
        if weather_data_list:
            sample_weather = weather_data_list[0]
            logger.debug(
                "sentinel_dispatch.api.routers.enrich.enrich_call_endpoint.sample_weather",
                sample_keys=list(sample_weather.keys()),
                has_location="location" in sample_weather,
                has_timestamp="timestamp" in sample_weather
            )
        if fire_data_list:
            sample_fire = fire_data_list[0]
            logger.debug(
                "sentinel_dispatch.api.routers.enrich.enrich_call_endpoint.sample_fire",
                sample_keys=list(sample_fire.keys()),
                has_location="location" in sample_fire,
                has_timestamp="timestamp" in sample_fire
            )
        
        logger.info(
            "sentinel_dispatch.api.routers.enrich.enrich_call_endpoint.enriching",
            call_id=call_id,
            weather_count=len(weather_data_list),
            fire_count=len(fire_data_list)
        )
        
        # Enrich the call
        enriched_call = enrich_call(
            request.processed_call,
            weather_data_list=weather_data_list,
            fire_data_list=fire_data_list,
        )
        
        logger.info(
            "sentinel_dispatch.api.routers.enrich.enrich_call_endpoint.completed",
            call_id=call_id,
            has_fire_analysis=enriched_call.fire_analysis is not None,
            has_weather_enrichment=enriched_call.weather_enrichment is not None
        )
        
        return EnrichCallResponse(
            enriched_call=enriched_call,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        
    except ValueError as e:
        logger.error(
            "sentinel_dispatch.api.routers.enrich.enrich_call_endpoint.validation_error",
            call_id=call_id,
            error=str(e)
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request: {str(e)}"
        )
    except Exception as e:
        logger.error(
            "sentinel_dispatch.api.routers.enrich.enrich_call_endpoint.error",
            call_id=call_id,
            error=str(e),
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Enrichment failed: {str(e)}"
        )


@router.get("/health")
async def health_check() -> dict:
    """Health check endpoint."""
    return {"status": "healthy", "service": "data-enrichment"}

