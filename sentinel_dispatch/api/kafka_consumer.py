"""
Kafka consumer service for Streaming Agent output topics.

This service consumes messages from Flink Streaming Agent output topics and
populates the in-memory data stores used by the dashboard REST endpoints.

Topics consumed:
- processed-calls: Processed emergency calls with classification
- enriched-calls: Enriched calls with weather and fire data
- risk-scores: Risk score calculations
- alerts: Generated alerts
"""

import asyncio
import json
import os
from typing import Optional
from datetime import datetime

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient

from sentinel_dispatch.api.schemas import (
    EmergencyCall,
    CallerLocation,
    CallMetadata,
    DashboardRiskScore,
    Alert,
)
from sentinel_dispatch.api.websocket_manager import get_connection_manager
from sentinel_dispatch.utils.logging_config import get_logger
from sqlalchemy.orm import Session

logger = get_logger(__name__)


class StreamingAgentKafkaConsumer:
    """Kafka consumer for Streaming Agent output topics."""
    
    def __init__(
        self,
        bootstrap_servers: str,
        consumer_group_id: str = "sentinel-dispatch-dashboard-api",
        event_loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        """
        Initialize Kafka consumer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers (e.g., "localhost:9092" or "kafka:29092")
            consumer_group_id: Consumer group ID for Kafka
            event_loop: Optional event loop for WebSocket broadcasting (if None, will get from asyncio)
        """
        self.bootstrap_servers = bootstrap_servers
        self.consumer_group_id = consumer_group_id
        self.consumer: Optional[Consumer] = None
        self.running = False
        self._task: Optional[asyncio.Task] = None
        self._event_loop = event_loop
        
        # Topics to consume from
        self.topics = [
            "processed-calls",
            "enriched-calls",
            "risk-scores",
            "alerts",
        ]
        
        logger.info(
            "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer.__init__.initialized",
            bootstrap_servers=bootstrap_servers,
            consumer_group_id=consumer_group_id,
            topics=self.topics,
        )
    
    def _create_consumer(self) -> Consumer:
        """Create and configure Kafka consumer."""
        # Check if we should reset offsets (useful for development/debugging)
        reset_offsets = os.getenv("KAFKA_RESET_OFFSETS", "false").lower() == "true"
        
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.consumer_group_id,
            "auto.offset.reset": "earliest",  # Start from earliest messages to process all historical data
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 300000,
        }
        
        consumer = Consumer(config)
        
        if reset_offsets:
            logger.warning(
                "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._create_consumer.resetting_offsets",
                message="KAFKA_RESET_OFFSETS=true, will reset consumer group offsets",
            )
        
        logger.info(
            "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._create_consumer.created",
            config={k: v for k, v in config.items() if k != "bootstrap.servers"},  # Don't log full bootstrap servers
            reset_offsets=reset_offsets,
        )
        return consumer
    
    def _check_topics_exist(self) -> bool:
        """Check if all required topics exist."""
        try:
            admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers})
            metadata = admin_client.list_topics(timeout=10)
            existing_topics = set(metadata.topics.keys())
            
            missing_topics = set(self.topics) - existing_topics
            if missing_topics:
                logger.warning(
                    "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._check_topics_exist.missing_topics",
                    missing_topics=list(missing_topics),
                    existing_topics=list(existing_topics),
                )
                return False
            
            logger.info(
                "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._check_topics_exist.all_topics_exist",
                topics=self.topics,
            )
            return True
        except Exception as e:
            logger.error(
                "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._check_topics_exist.error",
                error=str(e),
                exc_info=True,
            )
            return False
    
    def _parse_processed_call(self, message_value: str) -> Optional[EmergencyCall]:
        """Parse processed call message and convert to EmergencyCall schema."""
        try:
            data = json.loads(message_value)
            
            # Extract fields (handle both camelCase and snake_case)
            call_id = data.get("call_id") or data.get("callId", "")
            timestamp = data.get("timestamp", "")
            caller_location = data.get("caller_location") or data.get("callerLocation", {})
            transcript = data.get("transcript", "")
            metadata = data.get("metadata", {})
            
            if not call_id or not timestamp:
                logger.warning(
                    "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._parse_processed_call.missing_fields",
                    call_id=call_id,
                    has_timestamp=bool(timestamp),
                )
                return None
            
            return EmergencyCall(
                callId=call_id,
                timestamp=timestamp,
                callerLocation=CallerLocation(
                    lat=caller_location.get("lat", 0.0),
                    lon=caller_location.get("lon", 0.0),
                ),
                transcript=transcript,
                metadata=CallMetadata(
                    phoneNumber=metadata.get("phoneNumber"),
                    callDuration=metadata.get("callDuration"),
                    callerAge=metadata.get("callerAge"),
                    knownConditions=metadata.get("knownConditions"),
                ) if metadata else None,
            )
        except Exception as e:
            logger.error(
                "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._parse_processed_call.error",
                error=str(e),
                message_preview=message_value[:200] if message_value else None,
                exc_info=True,
            )
            return None
    
    def _parse_risk_score(self, message_value: str) -> Optional[DashboardRiskScore]:
        """Parse risk score message and convert to DashboardRiskScore schema."""
        try:
            data = json.loads(message_value)
            
            # Handle both direct risk_score object and nested structure
            # If "risk_score" key exists and is a dict, use it; otherwise use data itself
            if "risk_score" in data and isinstance(data.get("risk_score"), dict):
                risk_score_data = data.get("risk_score")
            else:
                risk_score_data = data
            
            call_id = risk_score_data.get("call_id") or risk_score_data.get("callId", "")
            timestamp = risk_score_data.get("timestamp", "")
            risk_score = risk_score_data.get("risk_score", 0.0)
            confidence = risk_score_data.get("confidence", 0.0)
            factors = risk_score_data.get("factors", {})
            recommendation = risk_score_data.get("recommendation")
            priority = risk_score_data.get("priority")
            
            if not call_id or not timestamp:
                logger.warning(
                    "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._parse_risk_score.missing_fields",
                    call_id=call_id,
                    has_timestamp=bool(timestamp),
                )
                return None
            
            return DashboardRiskScore(
                callId=call_id,
                timestamp=timestamp,
                riskScore=float(risk_score),
                confidence=float(confidence),
                factors={
                    "callUrgency": factors.get("call_urgency"),
                    "fireProximity": factors.get("fire_proximity"),
                    "fireSpreadRate": factors.get("fire_spread_rate"),
                    "weatherRisk": factors.get("weather_risk"),
                    "vulnerability": factors.get("vulnerability"),
                    "timeSinceCall": factors.get("time_since_call"),
                },
                recommendation=recommendation,
                priority=priority,
            )
        except Exception as e:
            logger.error(
                "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._parse_risk_score.error",
                error=str(e),
                message_preview=message_value[:200] if message_value else None,
                exc_info=True,
            )
            return None
    
    def _parse_alerts(self, message_value: str) -> list[Alert]:
        """
        Parse alert message and convert to list of Alert schemas.
        
        Handles both single alert objects and arrays of alerts.
        The alert generation agent returns {"alerts": [...], "alert_count": N, "timestamp": "..."}
        """
        try:
            data = json.loads(message_value)
            
            # Handle both direct alert object and nested structure (alerts array)
            # The alert generation agent returns {"alerts": [...], "alert_count": N, "timestamp": "..."}
            alerts_array = data.get("alerts", [])
            
            if isinstance(alerts_array, list) and len(alerts_array) > 0:
                # Process all alerts in the array
                parsed_alerts = []
                for alert_data in alerts_array:
                    alert_id = alert_data.get("alertId") or alert_data.get("alert_id", "")
                    if not alert_id:
                        # Generate alert ID if missing (use timestamp to ensure uniqueness)
                        alert_id = f"ALERT-{datetime.now().isoformat()}-{len(parsed_alerts)}"
                    
                    parsed_alert = Alert(
                        alertId=alert_id,
                        timestamp=alert_data.get("timestamp") or data.get("timestamp", datetime.now().isoformat()),
                        severity=alert_data.get("severity", "medium"),
                        message=alert_data.get("message", ""),
                        alert_type=alert_data.get("alert_type") or alert_data.get("type"),
                        callId=alert_data.get("callId") or alert_data.get("call_id"),
                        acknowledged=False,
                    )
                    parsed_alerts.append(parsed_alert)
                
                logger.info(
                    "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._parse_alerts.parsed_array",
                    alert_count=len(parsed_alerts),
                )
                return parsed_alerts
            else:
                # Try to parse as single alert object
                alert_id = data.get("alertId") or data.get("alert_id", "")
                if not alert_id:
                    logger.warning(
                        "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._parse_alerts.no_alert_id",
                        message_preview=message_value[:200] if message_value else None,
                    )
                    return []
                
                parsed_alert = Alert(
                    alertId=alert_id,
                    timestamp=data.get("timestamp", datetime.now().isoformat()),
                    severity=data.get("severity", "medium"),
                    message=data.get("message", ""),
                    alert_type=data.get("alert_type") or data.get("type"),
                    callId=data.get("callId") or data.get("call_id"),
                    acknowledged=False,
                )
                logger.info(
                    "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._parse_alerts.parsed_single",
                    alert_id=alert_id,
                )
                return [parsed_alert]
        except Exception as e:
            logger.error(
                "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._parse_alerts.error",
                error=str(e),
                message_preview=message_value[:200] if message_value else None,
                exc_info=True,
            )
            return []
    
    def _save_with_retry(self, db: Session, model, max_retries: int = 3) -> None:
        """
        Save model to database with retry logic for SQLite lock errors.
        
        Args:
            db: Database session
            model: SQLAlchemy model instance to save
            max_retries: Maximum number of retry attempts
        """
        from sqlalchemy.exc import OperationalError
        import time
        import random
        
        delay = 0.1  # Initial delay in seconds
        max_delay = 1.0
        
        for attempt in range(max_retries + 1):
            try:
                db.merge(model)
                db.commit()
                return
            except OperationalError as e:
                error_msg = str(e).lower()
                is_lock_error = (
                    "database is locked" in error_msg or
                    "database locked" in error_msg or
                    "timeout" in error_msg or
                    "locked" in error_msg
                )
                
                if not is_lock_error:
                    # Not a lock error, re-raise immediately
                    db.rollback()
                    raise
                
                if attempt < max_retries:
                    db.rollback()
                    # Add jitter to prevent thundering herd
                    jitter = random.uniform(0, delay * 0.1)
                    time.sleep(delay + jitter)
                    delay = min(delay * 2.0, max_delay)
                    logger.warning(
                        "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._save_with_retry.retry",
                        attempt=attempt + 1,
                        max_retries=max_retries,
                        error=str(e),
                    )
                else:
                    # Max retries exceeded
                    db.rollback()
                    logger.error(
                        "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._save_with_retry.max_retries_exceeded",
                        error=str(e),
                        exc_info=True,
                    )
                    raise
    
    def _process_message(self, topic: str, message_value: str) -> None:
        """Process a message from a Kafka topic and update database."""
        from sentinel_dispatch.api.database import (
            get_db_sync,
            emergency_call_schema_to_model,
            risk_score_schema_to_model,
            alert_schema_to_model,
            EmergencyCallModel,
        )
        
        try:
            connection_manager = get_connection_manager()

            # Get event loop for WebSocket broadcasting (from main thread)
            # Since we're in a thread, we need to use the event loop passed during initialization
            loop = self._event_loop

            # Get database session
            db = get_db_sync()
            try:
                if topic == "processed-calls":
                    emergency_call = self._parse_processed_call(message_value)
                    if emergency_call:
                        # Convert to model and save to database
                        call_model = emergency_call_schema_to_model(emergency_call, is_enriched=False)
                        # Use merge to handle existing records (with retry for lock errors)
                        self._save_with_retry(db, call_model)
                        
                        logger.debug(
                            "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._process_message.processed_call",
                            call_id=emergency_call.callId,
                        )
                        # Broadcast new call via WebSocket
                        if loop:
                            asyncio.run_coroutine_threadsafe(
                                connection_manager.broadcast_new_call(emergency_call.model_dump()),
                                loop
                            )
                
                elif topic == "enriched-calls":
                    # Enriched calls can also be stored as emergency calls (they contain the same base data)
                    emergency_call = self._parse_processed_call(message_value)
                    if emergency_call:
                        # Convert to model with is_enriched=True
                        call_model = emergency_call_schema_to_model(emergency_call, is_enriched=True)
                        
                        # Check if call already exists
                        existing_call = db.query(EmergencyCallModel).filter_by(call_id=emergency_call.callId).first()
                        
                        if existing_call:
                            # Update existing record - explicitly set is_enriched=True and update other fields
                            existing_call.timestamp = call_model.timestamp
                            existing_call.caller_lat = call_model.caller_lat
                            existing_call.caller_lon = call_model.caller_lon
                            existing_call.transcript = call_model.transcript
                            existing_call.call_metadata = call_model.call_metadata
                            existing_call.is_enriched = True  # Explicitly set to True
                            # Use _save_with_retry to ensure proper error handling
                            self._save_with_retry(db, existing_call)
                            logger.info(
                                "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._process_message.existing_call_updated",
                                call_id=emergency_call.callId,
                                is_enriched=existing_call.is_enriched,
                            )
                        else:
                            # Create new record with is_enriched=True
                            self._save_with_retry(db, call_model)
                            logger.info(
                                "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._process_message.new_enriched_call_created",
                                call_id=emergency_call.callId,
                            )
                        
                        logger.info(
                            "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._process_message.enriched_call",
                            call_id=emergency_call.callId,
                            is_enriched=True,
                            was_existing=existing_call is not None,
                        )
                        # Broadcast new call via WebSocket (enriched calls are also new calls)
                        if loop:
                            asyncio.run_coroutine_threadsafe(
                                connection_manager.broadcast_new_call(emergency_call.model_dump()),
                                loop
                            )
                    else:
                        logger.warning(
                            "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._process_message.enriched_call_parse_failed",
                            message_preview=message_value[:200] if message_value else None,
                        )
                
                elif topic == "risk-scores":
                    risk_score = self._parse_risk_score(message_value)
                    if risk_score:
                        # Convert to model and save to database
                        risk_score_model = risk_score_schema_to_model(risk_score)
                        # Use merge to handle existing records (with retry for lock errors)
                        self._save_with_retry(db, risk_score_model)
                        
                        # Verify the call exists and is enriched
                        call_exists = db.query(EmergencyCallModel).filter_by(call_id=risk_score.callId).first()
                        logger.info(
                            "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._process_message.risk_score",
                            call_id=risk_score.callId,
                            risk_score=risk_score.riskScore,
                            priority=risk_score.priority,
                            call_exists=call_exists is not None,
                            call_is_enriched=call_exists.is_enriched if call_exists else None,
                        )
                        # Broadcast risk score update via WebSocket
                        if loop:
                            asyncio.run_coroutine_threadsafe(
                                connection_manager.broadcast_risk_score_update(risk_score.model_dump()),
                                loop
                            )
                
                elif topic == "alerts":
                    # Parse alerts - can be single alert or array of alerts
                    alerts = self._parse_alerts(message_value)
                    if alerts:
                        for alert in alerts:
                            # Convert to model and save to database
                            alert_model = alert_schema_to_model(alert)
                            # Use merge to handle existing records (with retry for lock errors)
                            self._save_with_retry(db, alert_model)
                            
                            logger.info(
                                "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._process_message.alert",
                                alert_id=alert.alertId,
                                severity=alert.severity,
                                call_id=alert.callId,
                            )
                            # Broadcast alert via WebSocket
                            if loop:
                                asyncio.run_coroutine_threadsafe(
                                    connection_manager.broadcast_alert(alert.model_dump()),
                                    loop
                                )
            except Exception:
                db.rollback()
                raise
            finally:
                db.close()
            
        except Exception as e:
            logger.error(
                "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._process_message.error",
                topic=topic,
                error=str(e),
                exc_info=True,
            )
    
    def _consume_sync(self) -> None:
        """Synchronous consumption loop (runs in thread)."""
        logger.info(
            "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._consume_sync.started",
            topics=self.topics,
        )
        
        # Check if topics exist
        if not self._check_topics_exist():
            logger.warning(
                "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._consume_sync.topics_missing",
                message="Some topics are missing, consumer will retry on next poll",
            )
        
        # Create consumer
        try:
            self.consumer = self._create_consumer()
            self.consumer.subscribe(self.topics)
            logger.info(
                "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._consume_sync.subscribed",
                topics=self.topics,
                consumer_group_id=self.consumer_group_id,
            )
            
            # Do an initial poll to trigger partition assignment
            # Kafka assigns partitions asynchronously after the first poll
            import time
            initial_poll_timeout = 5.0
            msg = self.consumer.poll(timeout=initial_poll_timeout)
            
            # Check partition assignments after initial poll
            partitions = self.consumer.assignment()
            if partitions:
                logger.info(
                    "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._consume_sync.assigned_partitions",
                    partitions=[f"{p.topic}:{p.partition}" for p in partitions],
                )
                
                # If reset offsets is enabled, seek to beginning of all assigned partitions
                reset_offsets = os.getenv("KAFKA_RESET_OFFSETS", "false").lower() == "true"
                if reset_offsets:
                    logger.info(
                        "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._consume_sync.seeking_to_beginning",
                        partitions=[f"{p.topic}:{p.partition}" for p in partitions],
                    )
                    # Seek to beginning of all assigned partitions
                    for partition in partitions:
                        self.consumer.seek(partition, 0)
                    logger.info(
                        "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._consume_sync.seeked_to_beginning",
                        message="All partitions seeked to beginning",
                    )
            else:
                logger.warning(
                    "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._consume_sync.no_partitions_assigned",
                    message="Consumer did not receive partition assignments after initial poll, will retry during consumption",
                )
            
            # Process the message from initial poll if any
            if msg and not msg.error():
                message_value = msg.value().decode("utf-8")
                logger.info(
                    "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._consume_sync.initial_poll_message",
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                )
                self._process_message(msg.topic(), message_value)
        except Exception as e:
            logger.error(
                "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._consume_sync.subscribe_error",
                error=str(e),
                exc_info=True,
            )
            return
        
        # Main consumption loop
        poll_count = 0
        message_count = 0
        while self.running:
            try:
                # Poll for messages (non-blocking, with timeout)
                msg = self.consumer.poll(timeout=1.0)
                poll_count += 1
                
                # Log every 100 polls to show the consumer is alive
                if poll_count % 100 == 0:
                    logger.info(
                        "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._consume_sync.polling",
                        poll_count=poll_count,
                        messages_received=message_count,
                    )
                
                if msg is None:
                    continue
                
                message_count += 1
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event (not an error)
                        logger.debug(
                            "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._consume_sync.partition_eof",
                            topic=msg.topic(),
                            partition=msg.partition(),
                        )
                    else:
                        logger.error(
                            "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._consume_sync.kafka_error",
                            error=msg.error(),
                            topic=msg.topic(),
                        )
                    continue
                
                # Process message
                message_value = msg.value().decode("utf-8")
                logger.info(
                    "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._consume_sync.message_received",
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    message_size=len(message_value),
                    total_messages=message_count,
                )
                self._process_message(msg.topic(), message_value)
                
            except KeyboardInterrupt:
                logger.info(
                    "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._consume_sync.interrupted",
                )
                break
            except Exception as e:
                logger.error(
                    "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._consume_sync.error",
                    error=str(e),
                    exc_info=True,
                )
                # Continue processing despite errors
                import time
                time.sleep(1)
        
        logger.info(
            "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer._consume_sync.stopped",
        )
    
    async def _consume_loop(self) -> None:
        """Main consumption loop (runs in background task, wraps sync consumer in thread)."""
        # Run the synchronous consumer in a thread
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._consume_sync)
    
    async def start(self) -> None:
        """Start the Kafka consumer."""
        if self.running:
            logger.warning(
                "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer.start.already_running",
            )
            return
        
        self.running = True
        self._task = asyncio.create_task(self._consume_loop())
        logger.info(
            "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer.start.started",
        )
    
    async def stop(self) -> None:
        """Stop the Kafka consumer."""
        if not self.running:
            return
        
        logger.info(
            "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer.stop.stopping",
        )
        
        self.running = False
        
        # Cancel the task
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        
        # Close consumer
        if self.consumer:
            try:
                self.consumer.close()
            except Exception as e:
                logger.error(
                    "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer.stop.close_error",
                    error=str(e),
                )
        
        logger.info(
            "sentinel_dispatch.api.kafka_consumer.StreamingAgentKafkaConsumer.stop.stopped",
        )


# Global consumer instance (will be initialized in lifespan)
_kafka_consumer: Optional[StreamingAgentKafkaConsumer] = None


def get_kafka_consumer() -> Optional[StreamingAgentKafkaConsumer]:
    """Get the global Kafka consumer instance."""
    return _kafka_consumer


async def start_kafka_consumer(bootstrap_servers: str) -> None:
    """Start the global Kafka consumer."""
    global _kafka_consumer
    
    if _kafka_consumer is not None:
        logger.warning(
            "sentinel_dispatch.api.kafka_consumer.start_kafka_consumer.already_started",
        )
        return
    
    # Get the current event loop for WebSocket broadcasting
    try:
        event_loop = asyncio.get_running_loop()
    except RuntimeError:
        event_loop = None

    _kafka_consumer = StreamingAgentKafkaConsumer(bootstrap_servers, event_loop=event_loop)
    await _kafka_consumer.start()
    logger.info(
        "sentinel_dispatch.api.kafka_consumer.start_kafka_consumer.started",
        bootstrap_servers=bootstrap_servers,
    )


async def stop_kafka_consumer() -> None:
    """Stop the global Kafka consumer."""
    global _kafka_consumer
    
    if _kafka_consumer is None:
        return
    
    await _kafka_consumer.stop()
    _kafka_consumer = None
    logger.info(
        "sentinel_dispatch.api.kafka_consumer.stop_kafka_consumer.stopped",
    )

