"""
Tests for database retry logic and race condition handling.
"""

import pytest
from unittest.mock import patch
from sqlalchemy.exc import OperationalError

from sentinel_dispatch.api.database import (
    DatabaseManager,
    EmergencyCallModel,
    get_db_manager,
)
from sentinel_dispatch.api.schemas import (
    EmergencyCall,
    CallerLocation,
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


class TestDatabaseRetryLogic:
    """Test cases for database retry logic in Kafka consumer."""
    
    def test_save_with_retry_success(self):
        """Test successful save without retry."""
        from sentinel_dispatch.api.kafka_consumer import StreamingAgentKafkaConsumer
        from sentinel_dispatch.api.database import emergency_call_schema_to_model
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        
        call = EmergencyCall(
            callId="CALL-001",
            timestamp="2024-08-08T14:23:45Z",
            callerLocation=CallerLocation(lat=20.8783, lon=-156.6825),
            transcript="Test call",
            metadata=None,
        )
        
        db = get_db_manager().get_session()
        try:
            call_model = emergency_call_schema_to_model(call, is_enriched=False)
            consumer._save_with_retry(db, call_model)
            
            # Verify call was saved
            saved_call = db.query(EmergencyCallModel).filter_by(call_id="CALL-001").first()
            assert saved_call is not None
            assert saved_call.call_id == "CALL-001"
        finally:
            db.close()
    
    def test_save_with_retry_lock_error(self):
        """Test retry logic on database lock error."""
        from sentinel_dispatch.api.kafka_consumer import StreamingAgentKafkaConsumer
        from sentinel_dispatch.api.database import emergency_call_schema_to_model
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        
        call = EmergencyCall(
            callId="CALL-001",
            timestamp="2024-08-08T14:23:45Z",
            callerLocation=CallerLocation(lat=20.8783, lon=-156.6825),
            transcript="Test call",
            metadata=None,
        )
        
        db = get_db_manager().get_session()
        try:
            call_model = emergency_call_schema_to_model(call, is_enriched=False)
            
            # Mock OperationalError with lock error, then success
            # We need to store the original commit method to call it on success
            original_commit = db.commit
            commit_call_count = [0]  # Use list to allow modification in nested function
            
            def mock_commit():
                commit_call_count[0] += 1
                if commit_call_count[0] == 1:
                    # First call raises lock error
                    raise OperationalError("database is locked", None, None)
                else:
                    # Second call succeeds - call the real commit
                    return original_commit()
            
            with patch.object(db, 'commit', side_effect=mock_commit):
                consumer._save_with_retry(db, call_model, max_retries=3)
            
            # Verify call was saved after retry
            saved_call = db.query(EmergencyCallModel).filter_by(call_id="CALL-001").first()
            assert saved_call is not None
            assert saved_call.call_id == "CALL-001"
        finally:
            db.close()
    
    def test_save_with_retry_max_retries_exceeded(self):
        """Test that max retries exceeded raises exception."""
        from sentinel_dispatch.api.kafka_consumer import StreamingAgentKafkaConsumer
        from sentinel_dispatch.api.database import emergency_call_schema_to_model
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        
        call = EmergencyCall(
            callId="CALL-001",
            timestamp="2024-08-08T14:23:45Z",
            callerLocation=CallerLocation(lat=20.8783, lon=-156.6825),
            transcript="Test call",
            metadata=None,
        )
        
        db = get_db_manager().get_session()
        try:
            call_model = emergency_call_schema_to_model(call, is_enriched=False)
            
            # Mock OperationalError with lock error for all retries
            with patch.object(db, 'commit', side_effect=OperationalError("database is locked", None, None)):
                with pytest.raises(OperationalError):
                    consumer._save_with_retry(db, call_model, max_retries=2)
        finally:
            db.close()
    
    def test_save_with_retry_non_lock_error(self):
        """Test that non-lock errors are not retried."""
        from sentinel_dispatch.api.kafka_consumer import StreamingAgentKafkaConsumer
        from sentinel_dispatch.api.database import emergency_call_schema_to_model
        
        consumer = StreamingAgentKafkaConsumer("localhost:9092")
        
        call = EmergencyCall(
            callId="CALL-001",
            timestamp="2024-08-08T14:23:45Z",
            callerLocation=CallerLocation(lat=20.8783, lon=-156.6825),
            transcript="Test call",
            metadata=None,
        )
        
        db = get_db_manager().get_session()
        try:
            call_model = emergency_call_schema_to_model(call, is_enriched=False)
            
            # Mock OperationalError with non-lock error
            with patch.object(db, 'commit', side_effect=OperationalError("other error", None, None)):
                with pytest.raises(OperationalError):
                    consumer._save_with_retry(db, call_model, max_retries=3)
        finally:
            db.close()


class TestDatabaseConcurrency:
    """Test cases for database concurrency scenarios."""
    
    def test_concurrent_writes_same_record(self):
        """Test that concurrent writes to the same record are handled."""
        from sentinel_dispatch.api.database import emergency_call_schema_to_model
        
        call1 = EmergencyCall(
            callId="CALL-001",
            timestamp="2024-08-08T14:23:45Z",
            callerLocation=CallerLocation(lat=20.8783, lon=-156.6825),
            transcript="First version",
            metadata=None,
        )
        
        call2 = EmergencyCall(
            callId="CALL-001",  # Same call ID
            timestamp="2024-08-08T14:24:00Z",
            callerLocation=CallerLocation(lat=20.8783, lon=-156.6825),
            transcript="Second version",
            metadata=None,
        )
        
        # Use merge to handle concurrent updates (idempotent)
        db1 = get_db_manager().get_session()
        db2 = get_db_manager().get_session()
        
        try:
            model1 = emergency_call_schema_to_model(call1, is_enriched=False)
            model2 = emergency_call_schema_to_model(call2, is_enriched=False)
            
            # Simulate concurrent writes
            db1.merge(model1)
            db1.commit()
            
            db2.merge(model2)
            db2.commit()
            
            # Last write should win (merge updates existing record)
            final_call = db1.query(EmergencyCallModel).filter_by(call_id="CALL-001").first()
            assert final_call is not None
            assert final_call.transcript == "Second version"
        finally:
            db1.close()
            db2.close()
    
    def test_is_enriched_flag_persistence(self):
        """Test that is_enriched flag is correctly persisted."""
        from sentinel_dispatch.api.database import emergency_call_schema_to_model
        
        # Add non-enriched call
        call1 = EmergencyCall(
            callId="CALL-001",
            timestamp="2024-08-08T14:23:45Z",
            callerLocation=CallerLocation(lat=20.8783, lon=-156.6825),
            transcript="Test call",
            metadata=None,
        )
        
        db = get_db_manager().get_session()
        try:
            model1 = emergency_call_schema_to_model(call1, is_enriched=False)
            db.merge(model1)
            db.commit()
            
            # Verify is_enriched is False
            saved_call = db.query(EmergencyCallModel).filter_by(call_id="CALL-001").first()
            assert saved_call.is_enriched is False
            
            # Update to enriched
            model2 = emergency_call_schema_to_model(call1, is_enriched=True)
            db.merge(model2)
            db.commit()
            
            # Verify is_enriched is now True
            saved_call = db.query(EmergencyCallModel).filter_by(call_id="CALL-001").first()
            assert saved_call.is_enriched is True
        finally:
            db.close()

