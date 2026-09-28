import json
import asyncio
from unittest.mock import Mock, patch
import pytest

from sentinel_dispatch.api.kafka_consumer import (
    StreamingAgentKafkaConsumer,
    start_kafka_consumer,
    stop_kafka_consumer,
    get_kafka_consumer,
)
from sentinel_dispatch.api.database import (
    get_db_manager,
    DatabaseManager,
    EmergencyCallModel,
    RiskScoreModel,
    AlertModel,
)


@pytest.fixture(autouse=True)
def setup_test_db():
    """Set up in-memory test database before each test."""
    import sentinel_dispatch.api.database as db_module
    db_module._db_manager = DatabaseManager("sqlite:///:memory:")
    db_module._db_manager.init_db()
    
    yield
    
    # Cleanup after test
    db_module._db_manager.close()
    db_module._db_manager = None


class TestStreamingAgentKafkaConsumer:
    """Test cases for StreamingAgentKafkaConsumer."""
    
    # No setup_method - database starts empty via fixture
    
    def test_consumer_initialization(self):
        """Test consumer initialization."""
        consumer = StreamingAgentKafkaConsumer(
            bootstrap_servers="localhost:9092",
            consumer_group_id="test-group",
        )
        
        assert consumer.bootstrap_servers == "localhost:9092"
        assert consumer.consumer_group_id == "test-group"
        assert consumer.running is False
        assert consumer.consumer is None
        assert consumer._task is None
        assert len(consumer.topics) == 4
        assert "processed-calls" in consumer.topics
        assert "enriched-calls" in consumer.topics
        assert "risk-scores" in consumer.topics
        assert "alerts" in consumer.topics
    
    @patch("sentinel_dispatch.api.kafka_consumer.Consumer")
    def test_create_consumer(self, mock_consumer_class):
        """Test consumer creation."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        created_consumer = consumer._create_consumer()
        
        assert created_consumer == mock_consumer
        mock_consumer_class.assert_called_once()
        config = mock_consumer_class.call_args[0][0]
        assert config["bootstrap.servers"] == "localhost:9092"
        assert config["group.id"] == "sentinel-dispatch-dashboard-api"
        assert config["auto.offset.reset"] == "earliest"
    
    @patch("sentinel_dispatch.api.kafka_consumer.AdminClient")
    def test_check_topics_exist_all_exist(self, mock_admin_client_class):
        """Test topic existence check when all topics exist."""
        mock_admin_client = Mock()
        mock_admin_client_class.return_value = mock_admin_client
        
        # Mock metadata with all topics
        mock_metadata = Mock()
        mock_metadata.topics = {
            "processed-calls": Mock(),
            "enriched-calls": Mock(),
            "risk-scores": Mock(),
            "alerts": Mock(),
        }
        mock_admin_client.list_topics.return_value = mock_metadata
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        result = consumer._check_topics_exist()
        
        assert result is True
        mock_admin_client.list_topics.assert_called_once_with(timeout=10)
    
    @patch("sentinel_dispatch.api.kafka_consumer.AdminClient")
    def test_check_topics_exist_missing_topics(self, mock_admin_client_class):
        """Test topic existence check when some topics are missing."""
        mock_admin_client = Mock()
        mock_admin_client_class.return_value = mock_admin_client
        
        # Mock metadata with only some topics
        mock_metadata = Mock()
        mock_metadata.topics = {
            "processed-calls": Mock(),
            "enriched-calls": Mock(),
        }
        mock_admin_client.list_topics.return_value = mock_metadata
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        result = consumer._check_topics_exist()
        
        assert result is False
    
    @patch("sentinel_dispatch.api.kafka_consumer.AdminClient")
    def test_check_topics_exist_error(self, mock_admin_client_class):
        """Test topic existence check when admin client raises error."""
        mock_admin_client = Mock()
        mock_admin_client_class.return_value = mock_admin_client
        mock_admin_client.list_topics.side_effect = Exception("Connection error")
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        result = consumer._check_topics_exist()
        
        assert result is False
    
    def test_parse_processed_call_success(self):
        """Test parsing a processed call message."""
        message = json.dumps({
            "call_id": "CALL-001",
            "timestamp": "2024-08-08T14:23:45Z",
            "caller_location": {"lat": 20.8783, "lon": -156.6825},
            "transcript": "I need help!",
            "metadata": {
                "phoneNumber": "+1-808-555-0123",
                "callDuration": 127,
                "callerAge": 65,
                "knownConditions": ["asthma"],
            },
        })
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        result = consumer._parse_processed_call(message)
        
        assert result is not None
        assert result.callId == "CALL-001"
        assert result.timestamp == "2024-08-08T14:23:45Z"
        assert result.callerLocation.lat == 20.8783
        assert result.callerLocation.lon == -156.6825
        assert result.transcript == "I need help!"
        assert result.metadata.phoneNumber == "+1-808-555-0123"
        assert result.metadata.callerAge == 65
    
    def test_parse_processed_call_camel_case(self):
        """Test parsing a processed call with camelCase fields."""
        message = json.dumps({
            "callId": "CALL-002",
            "timestamp": "2024-08-08T14:25:00Z",
            "callerLocation": {"lat": 20.8800, "lon": -156.6800},
            "transcript": "Smoke everywhere!",
            "metadata": None,
        })
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        result = consumer._parse_processed_call(message)
        
        assert result is not None
        assert result.callId == "CALL-002"
        assert result.metadata is None
    
    def test_parse_processed_call_missing_fields(self):
        """Test parsing a processed call with missing required fields."""
        message = json.dumps({
            "transcript": "I need help!",
            # Missing call_id and timestamp
        })
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        result = consumer._parse_processed_call(message)
        
        assert result is None
    
    def test_parse_processed_call_invalid_json(self):
        """Test parsing an invalid JSON message."""
        message = "not valid json"
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        result = consumer._parse_processed_call(message)
        
        assert result is None
    
    def test_parse_risk_score_success(self):
        """Test parsing a risk score message."""
        message = json.dumps({
            "call_id": "CALL-001",
            "timestamp": "2024-08-08T14:23:50Z",
            "risk_score": 85.0,
            "confidence": 0.95,
            "factors": {
                "call_urgency": 0.9,
                "fire_proximity": 0.8,
                "fire_spread_rate": 0.7,
                "weather_risk": 0.6,
                "vulnerability": 0.9,
                "time_since_call": 0.5,
            },
            "recommendation": "IMMEDIATE_DISPATCH",
            "priority": 5,
        })
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        result = consumer._parse_risk_score(message)
        
        assert result is not None
        assert result.callId == "CALL-001"
        assert result.riskScore == 85.0
        assert result.confidence == 0.95
        assert result.priority == 5
        assert result.recommendation == "IMMEDIATE_DISPATCH"
        assert result.factors["callUrgency"] == 0.9
        assert result.factors["fireProximity"] == 0.8
    
    def test_parse_risk_score_nested_structure(self):
        """Test parsing a risk score with nested risk_score object."""
        message = json.dumps({
            "risk_score": {
                "call_id": "CALL-002",
                "timestamp": "2024-08-08T14:25:05Z",
                "risk_score": 65.0,
                "confidence": 0.85,
                "factors": {
                    "call_urgency": 0.7,
                    "fire_proximity": 0.6,
                },
                "priority": 4,
            },
        })
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        result = consumer._parse_risk_score(message)
        
        assert result is not None
        assert result.callId == "CALL-002"
        assert result.riskScore == 65.0
        assert result.priority == 4
    
    def test_parse_risk_score_missing_fields(self):
        """Test parsing a risk score with missing required fields."""
        message = json.dumps({
            "risk_score": 85.0,
            # Missing call_id and timestamp
        })
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        result = consumer._parse_risk_score(message)
        
        assert result is None
    
    def test_parse_alert_success_single_alert(self):
        """Test parsing an alert message with single alert."""
        message = json.dumps({
            "alertId": "ALERT-001",
            "timestamp": "2024-08-08T14:24:00Z",
            "severity": "critical",
            "message": "High risk case",
            "alert_type": "risk_threshold",
            "callId": "CALL-001",
        })
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        results = consumer._parse_alerts(message)
        
        assert len(results) == 1
        result = results[0]
        assert result.alertId == "ALERT-001"
        assert result.severity == "critical"
        assert result.message == "High risk case"
        assert result.callId == "CALL-001"
        assert result.acknowledged is False
    
    def test_parse_alert_success_alerts_array(self):
        """Test parsing an alert message with alerts array (from alert generation agent)."""
        message = json.dumps({
            "alerts": [
                {
                    "alertId": "ALERT-001",
                    "timestamp": "2024-08-08T14:24:00Z",
                    "severity": "critical",
                    "message": "High risk case",
                    "alert_type": "risk_threshold",
                    "callId": "CALL-001",
                }
            ],
            "alert_count": 1,
            "timestamp": "2024-08-08T14:24:00Z",
        })
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        results = consumer._parse_alerts(message)
        
        assert len(results) == 1
        result = results[0]
        assert result.alertId == "ALERT-001"
        assert result.severity == "critical"
        assert result.callId == "CALL-001"
    
    def test_parse_alert_empty_alerts_array(self):
        """Test parsing an alert message with empty alerts array."""
        message = json.dumps({
            "alerts": [],
            "alert_count": 0,
            "timestamp": "2024-08-08T14:24:00Z",
        })
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        results = consumer._parse_alerts(message)
        
        assert len(results) == 0
    
    def test_parse_alert_missing_alert_id(self):
        """Test parsing an alert message with missing alert ID."""
        message = json.dumps({
            "timestamp": "2024-08-08T14:24:00Z",
            "severity": "critical",
            "message": "High risk case",
            # Missing alertId
        })
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        results = consumer._parse_alerts(message)
        
        assert len(results) == 0
    
    def test_process_message_processed_calls(self):
        """Test processing a processed-calls message."""
        message = json.dumps({
            "call_id": "CALL-001",
            "timestamp": "2024-08-08T14:23:45Z",
            "caller_location": {"lat": 20.8783, "lon": -156.6825},
            "transcript": "I need help!",
            "metadata": None,
        })
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        consumer._process_message("processed-calls", message)
        
        # Verify call was saved to database
        db = get_db_manager().get_session()
        try:
            call_model = db.query(EmergencyCallModel).filter_by(call_id="CALL-001").first()
            assert call_model is not None
            assert call_model.call_id == "CALL-001"
            assert call_model.is_enriched is False
        finally:
            db.close()
    
    def test_process_message_enriched_calls(self):
        """Test processing an enriched-calls message."""
        message = json.dumps({
            "call_id": "CALL-002",
            "timestamp": "2024-08-08T14:25:00Z",
            "caller_location": {"lat": 20.8800, "lon": -156.6800},
            "transcript": "Smoke everywhere!",
            "metadata": None,
        })
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        consumer._process_message("enriched-calls", message)
        
        # Verify call was saved to database with is_enriched=True
        db = get_db_manager().get_session()
        try:
            call_model = db.query(EmergencyCallModel).filter_by(call_id="CALL-002").first()
            assert call_model is not None
            assert call_model.call_id == "CALL-002"
            assert call_model.is_enriched is True
        finally:
            db.close()
    
    def test_process_message_risk_scores(self):
        """Test processing a risk-scores message."""
        message = json.dumps({
            "call_id": "CALL-001",
            "timestamp": "2024-08-08T14:23:50Z",
            "risk_score": 85.0,
            "confidence": 0.95,
            "factors": {
                "call_urgency": 0.9,
                "fire_proximity": 0.8,
            },
            "priority": 5,
        })
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        consumer._process_message("risk-scores", message)
        
        # Verify risk score was saved to database
        db = get_db_manager().get_session()
        try:
            score_model = db.query(RiskScoreModel).filter_by(call_id="CALL-001").first()
            assert score_model is not None
            assert score_model.call_id == "CALL-001"
            assert score_model.risk_score == 85.0
            assert score_model.priority == 5
        finally:
            db.close()
    
    def test_process_message_alerts(self):
        """Test processing an alerts message."""
        message = json.dumps({
            "alerts": [
                {
                    "alertId": "ALERT-001",
                    "timestamp": "2024-08-08T14:24:00Z",
                    "severity": "critical",
                    "message": "High risk case",
                    "alert_type": "risk_threshold",
                    "callId": "CALL-001",
                }
            ],
            "alert_count": 1,
            "timestamp": "2024-08-08T14:24:00Z",
        })
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        consumer._process_message("alerts", message)
        
        # Verify alert was saved to database
        db = get_db_manager().get_session()
        try:
            alert_model = db.query(AlertModel).filter_by(alert_id="ALERT-001").first()
            assert alert_model is not None
            assert alert_model.alert_id == "ALERT-001"
            assert alert_model.severity == "critical"
            assert alert_model.call_id == "CALL-001"
        finally:
            db.close()
    
    def test_process_message_alerts_multiple(self):
        """Test processing an alerts message with multiple alerts."""
        message = json.dumps({
            "alerts": [
                {
                    "alertId": "ALERT-001",
                    "timestamp": "2024-08-08T14:24:00Z",
                    "severity": "critical",
                    "message": "High risk case",
                    "alert_type": "risk_threshold",
                    "callId": "CALL-001",
                },
                {
                    "alertId": "ALERT-002",
                    "timestamp": "2024-08-08T14:24:01Z",
                    "severity": "high",
                    "message": "Fire proximity alert",
                    "alert_type": "fire_proximity",
                    "callId": "CALL-001",
                }
            ],
            "alert_count": 2,
            "timestamp": "2024-08-08T14:24:00Z",
        })
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        consumer._process_message("alerts", message)
        
        # Verify both alerts were saved to database
        db = get_db_manager().get_session()
        try:
            alert1 = db.query(AlertModel).filter_by(alert_id="ALERT-001").first()
            assert alert1 is not None
            assert alert1.alert_id == "ALERT-001"
            assert alert1.severity == "critical"
            
            alert2 = db.query(AlertModel).filter_by(alert_id="ALERT-002").first()
            assert alert2 is not None
            assert alert2.alert_id == "ALERT-002"
            assert alert2.severity == "high"
            assert alert2.alert_type == "fire_proximity"
        finally:
            db.close()
    
    def test_process_message_invalid_topic(self):
        """Test processing a message from an unknown topic."""
        message = json.dumps({"test": "data"})
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        # Should not raise an error, just do nothing
        consumer._process_message("unknown-topic", message)
        
        # Database should remain empty
        db = get_db_manager().get_session()
        try:
            assert db.query(EmergencyCallModel).count() == 0
            assert db.query(RiskScoreModel).count() == 0
            assert db.query(AlertModel).count() == 0
        finally:
            db.close()
    
    def test_process_message_parse_error(self):
        """Test processing a message that fails to parse."""
        invalid_message = "not valid json"
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        # Should not raise an error, just log and continue
        consumer._process_message("processed-calls", invalid_message)
        
        # Database should remain empty
        db = get_db_manager().get_session()
        try:
            assert db.query(EmergencyCallModel).count() == 0
        finally:
            db.close()
    
    @patch("sentinel_dispatch.api.kafka_consumer.AdminClient")
    @patch("sentinel_dispatch.api.kafka_consumer.Consumer")
    def test_consume_sync_success(self, mock_consumer_class, mock_admin_client_class):
        """Test successful synchronous consumption loop."""
        # Setup mocks
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        # Mock partition assignments (empty list initially, then populated after poll)
        mock_partition1 = Mock()
        mock_partition1.topic.return_value = "processed-calls"
        mock_partition1.partition = 0
        mock_partition2 = Mock()
        mock_partition2.topic.return_value = "risk-scores"
        mock_partition2.partition = 0
        
        # assignment() returns empty list initially, then populated after poll
        mock_consumer.assignment.side_effect = [[], [mock_partition1, mock_partition2]]
        
        mock_admin_client = Mock()
        mock_admin_client_class.return_value = mock_admin_client
        mock_metadata = Mock()
        mock_metadata.topics = {
            "processed-calls": Mock(),
            "enriched-calls": Mock(),
            "risk-scores": Mock(),
            "alerts": Mock(),
        }
        mock_admin_client.list_topics.return_value = mock_metadata
        
        # Create mock messages
        mock_msg1 = Mock()
        mock_msg1.error.return_value = None
        mock_msg1.topic.return_value = "processed-calls"
        mock_msg1.partition.return_value = 0
        mock_msg1.offset.return_value = 0
        mock_msg1.value.return_value = json.dumps({
            "call_id": "CALL-001",
            "timestamp": "2024-08-08T14:23:45Z",
            "caller_location": {"lat": 20.8783, "lon": -156.6825},
            "transcript": "I need help!",
            "metadata": None,
        }).encode("utf-8")
        
        mock_msg2 = Mock()
        mock_msg2.error.return_value = None
        mock_msg2.topic.return_value = "risk-scores"
        mock_msg2.partition.return_value = 0
        mock_msg2.offset.return_value = 0
        mock_msg2.value.return_value = json.dumps({
            "call_id": "CALL-001",
            "timestamp": "2024-08-08T14:23:50Z",
            "risk_score": 85.0,
            "confidence": 0.95,
            "factors": {"call_urgency": 0.9},
            "priority": 5,
        }).encode("utf-8")
        
        # Mock poll to return messages then None (to exit loop)
        # First poll is for initial assignment, then messages
        mock_consumer.poll.side_effect = [mock_msg1, mock_msg2, None, None]
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        consumer.running = True
        
        # Run consume_sync (will exit after None messages)
        import threading
        import time
        
        def run_consumer():
            consumer._consume_sync()
        
        thread = threading.Thread(target=run_consumer)
        thread.start()
        time.sleep(0.1)  # Give it time to process
        consumer.running = False
        thread.join(timeout=2)
        
        # Verify messages were processed
        # Verify data was saved to database
        db = get_db_manager().get_session()
        try:
            call_model = db.query(EmergencyCallModel).filter_by(call_id="CALL-001").first()
            assert call_model is not None
            score_model = db.query(RiskScoreModel).filter_by(call_id="CALL-001").first()
            assert score_model is not None
        finally:
            db.close()
        mock_consumer.subscribe.assert_called_once()
    
    @patch("sentinel_dispatch.api.kafka_consumer.AdminClient")
    @patch("sentinel_dispatch.api.kafka_consumer.Consumer")
    def test_consume_sync_partition_eof(self, mock_consumer_class, mock_admin_client_class):
        """Test consumption loop handling partition EOF."""
        from confluent_kafka import KafkaError
        
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        mock_admin_client = Mock()
        mock_admin_client_class.return_value = mock_admin_client
        mock_metadata = Mock()
        mock_metadata.topics = {
            "processed-calls": Mock(),
            "enriched-calls": Mock(),
            "risk-scores": Mock(),
            "alerts": Mock(),
        }
        mock_admin_client.list_topics.return_value = mock_metadata
        
        # Create mock message with partition EOF error
        # KafkaError._PARTITION_EOF is -191
        mock_msg = Mock()
        mock_error = Mock()
        mock_error.code.return_value = KafkaError._PARTITION_EOF
        mock_msg.error.return_value = mock_error
        mock_msg.topic.return_value = "processed-calls"
        mock_msg.partition.return_value = 0
        
        mock_consumer.poll.side_effect = [mock_msg, None, None]
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        consumer.running = True
        
        import threading
        import time
        
        def run_consumer():
            consumer._consume_sync()
        
        thread = threading.Thread(target=run_consumer)
        thread.start()
        time.sleep(0.1)
        consumer.running = False
        thread.join(timeout=2)
        
        # Should not raise an error, just log
        mock_consumer.subscribe.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_start_stop_consumer(self):
        """Test starting and stopping the consumer."""
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        
        assert consumer.running is False
        
        # Mock the consume loop to avoid actual Kafka connection
        async def mock_consume_loop():
            while consumer.running:
                await asyncio.sleep(0.1)
        
        consumer._consume_loop = mock_consume_loop
        
        # Start consumer
        await consumer.start()
        
        assert consumer.running is True
        assert consumer._task is not None
        
        # Stop consumer
        await consumer.stop()
        
        assert consumer.running is False
        assert consumer._task is None or consumer._task.cancelled()


class TestKafkaConsumerGlobalFunctions:
    """Test cases for global Kafka consumer functions."""
    
    def setup_method(self):
        """Set up test data before each test."""
        # Clear global consumer
        import sentinel_dispatch.api.kafka_consumer as kafka_consumer_module
        kafka_consumer_module._kafka_consumer = None
    
    @pytest.mark.asyncio
    async def test_start_kafka_consumer(self):
        """Test starting the global Kafka consumer."""
        await start_kafka_consumer("localhost:9092")
        
        consumer = get_kafka_consumer()
        assert consumer is not None
        assert consumer.bootstrap_servers == "localhost:9092"
        
        # Cleanup
        await stop_kafka_consumer()
    
    @pytest.mark.asyncio
    async def test_start_kafka_consumer_already_started(self):
        """Test starting consumer when already started."""
        await start_kafka_consumer("localhost:9092")
        
        # Try to start again (should not create duplicate)
        await start_kafka_consumer("localhost:9092")
        
        consumer = get_kafka_consumer()
        assert consumer is not None
        
        # Cleanup
        await stop_kafka_consumer()
    
    @pytest.mark.asyncio
    async def test_stop_kafka_consumer(self):
        """Test stopping the global Kafka consumer."""
        await start_kafka_consumer("localhost:9092")
        
        assert get_kafka_consumer() is not None
        
        await stop_kafka_consumer()
        
        assert get_kafka_consumer() is None
    
    @pytest.mark.asyncio
    async def test_stop_kafka_consumer_not_started(self):
        """Test stopping consumer when not started."""
        # Should not raise an error
        await stop_kafka_consumer()
        
        assert get_kafka_consumer() is None
    
    def test_get_kafka_consumer_none(self):
        """Test getting consumer when not initialized."""
        # Clear global consumer
        import sentinel_dispatch.api.kafka_consumer as kafka_consumer_module
        kafka_consumer_module._kafka_consumer = None
        
        consumer = get_kafka_consumer()
        assert consumer is None

