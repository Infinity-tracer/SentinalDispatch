"""
Integration tests for Kafka consumer and WebSocket integration.

These tests verify that Kafka consumer messages trigger WebSocket broadcasts
to connected clients in real-time.
"""

import json
from unittest.mock import Mock, patch
import pytest

from sentinel_dispatch.api.kafka_consumer import StreamingAgentKafkaConsumer
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


class TestKafkaConsumerWebSocketIntegration:
    """Test cases for Kafka consumer and WebSocket integration."""
    
    # No setup_method - database starts empty via fixture
    
    @patch("sentinel_dispatch.api.kafka_consumer.get_connection_manager")
    @patch("sentinel_dispatch.api.kafka_consumer.asyncio.run_coroutine_threadsafe")
    def test_process_message_broadcasts_risk_score(self, mock_run_coroutine, mock_get_manager):
        """Test that risk score messages trigger WebSocket broadcast."""
        mock_manager = Mock()
        mock_get_manager.return_value = mock_manager
        mock_manager.broadcast_risk_score_update = Mock()
        
        # Mock event loop
        mock_loop = Mock()
        
        message = json.dumps({
            "call_id": "CALL-001",
            "timestamp": "2024-08-08T14:23:50Z",
            "risk_score": 85.0,
            "confidence": 0.95,
            "factors": {"call_urgency": 0.9},
            "priority": 5,
        })
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092", event_loop=mock_loop)
        consumer._process_message("risk-scores", message)
        
        # Verify risk score was saved to database
        db = get_db_manager().get_session()
        try:
            score_model = db.query(RiskScoreModel).filter_by(call_id="CALL-001").first()
            assert score_model is not None
            assert score_model.risk_score == 85.0
        finally:
            db.close()
        
        # Verify WebSocket broadcast was scheduled
        mock_run_coroutine.assert_called_once()
    
    @patch("sentinel_dispatch.api.kafka_consumer.get_connection_manager")
    @patch("sentinel_dispatch.api.kafka_consumer.asyncio.run_coroutine_threadsafe")
    def test_process_message_broadcasts_new_call(self, mock_run_coroutine, mock_get_manager):
        """Test that processed call messages trigger WebSocket broadcast."""
        mock_manager = Mock()
        mock_get_manager.return_value = mock_manager
        mock_manager.broadcast_new_call = Mock()
        
        # Mock event loop
        mock_loop = Mock()
        
        message = json.dumps({
            "call_id": "CALL-001",
            "timestamp": "2024-08-08T14:23:45Z",
            "caller_location": {"lat": 20.8783, "lon": -156.6825},
            "transcript": "I need help!",
            "metadata": None,
        })
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092", event_loop=mock_loop)
        consumer._process_message("processed-calls", message)
        
        # Verify call was saved to database
        db = get_db_manager().get_session()
        try:
            call_model = db.query(EmergencyCallModel).filter_by(call_id="CALL-001").first()
            assert call_model is not None
        finally:
            db.close()
        
        # Verify WebSocket broadcast was scheduled
        mock_run_coroutine.assert_called_once()
    
    @patch("sentinel_dispatch.api.kafka_consumer.get_connection_manager")
    @patch("sentinel_dispatch.api.kafka_consumer.asyncio.run_coroutine_threadsafe")
    def test_process_message_broadcasts_alert(self, mock_run_coroutine, mock_get_manager):
        """Test that alert messages trigger WebSocket broadcast."""
        mock_manager = Mock()
        mock_get_manager.return_value = mock_manager
        mock_manager.broadcast_alert = Mock()
        
        # Mock event loop
        mock_loop = Mock()
        
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
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092", event_loop=mock_loop)
        consumer._process_message("alerts", message)
        
        # Verify alert was saved to database
        db = get_db_manager().get_session()
        try:
            alert_model = db.query(AlertModel).filter_by(alert_id="ALERT-001").first()
            assert alert_model is not None
        finally:
            db.close()
        
        # Verify WebSocket broadcast was scheduled
        mock_run_coroutine.assert_called_once()
    
    @patch("sentinel_dispatch.api.kafka_consumer.get_connection_manager")
    @patch("sentinel_dispatch.api.kafka_consumer.asyncio.run_coroutine_threadsafe")
    def test_process_message_no_event_loop(self, mock_run_coroutine, mock_get_manager):
        """Test that messages are processed even without event loop (no WebSocket broadcast)."""
        mock_manager = Mock()
        mock_get_manager.return_value = mock_manager
        
        message = json.dumps({
            "call_id": "CALL-001",
            "timestamp": "2024-08-08T14:23:45Z",
            "caller_location": {"lat": 20.8783, "lon": -156.6825},
            "transcript": "I need help!",
            "metadata": None,
        })
        
        # Create consumer without event loop
        consumer = StreamingAgentKafkaConsumer("localhost:9092", event_loop=None)
        consumer._process_message("processed-calls", message)
        
        # Verify call was saved to database
        db = get_db_manager().get_session()
        try:
            call_model = db.query(EmergencyCallModel).filter_by(call_id="CALL-001").first()
            assert call_model is not None
        finally:
            db.close()
        
        # Verify WebSocket broadcast was NOT scheduled (no event loop)
        mock_run_coroutine.assert_not_called()

