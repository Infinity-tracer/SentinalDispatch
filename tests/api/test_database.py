"""
Tests for database models and session management.
"""

import pytest
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from datetime import datetime

from sentinel_dispatch.api.database import (
    DatabaseManager,
    EmergencyCallModel,
    RiskScoreModel,
    AlertModel,
    get_db_manager,
    init_db,
    get_db,
    get_db_sync,
    _create_engine,
    _create_session_factory,
    _dispose_engine,
    emergency_call_model_to_schema,
    emergency_call_schema_to_model,
    risk_score_model_to_schema,
    risk_score_schema_to_model,
    alert_model_to_schema,
    alert_schema_to_model,
)
from sentinel_dispatch.api.schemas import (
    EmergencyCall,
    DashboardRiskScore,
    Alert,
    CallerLocation,
)


class TestDatabaseManager:
    """Test cases for DatabaseManager."""
    
    def setup_method(self):
        """Set up test data before each test."""
        # Use in-memory SQLite for testing
        self.db_manager = DatabaseManager("sqlite:///:memory:")
    
    def teardown_method(self):
        """Clean up after each test."""
        self.db_manager.close()
    
    def test_initialization(self):
        """Test DatabaseManager initialization."""
        assert self.db_manager.database_url == "sqlite:///:memory:"
        assert self.db_manager.engine is None
        assert self.db_manager.session_factory is None
    
    def test_engine_property_lazy_initialization(self):
        """Test that engine property initializes on first access."""
        # Engine should be None initially
        assert self.db_manager.engine is None
        
        # Initialize engine
        self.db_manager.initialize_engine()
        
        # Engine should now be available
        assert self.db_manager.engine is not None
        assert isinstance(self.db_manager.engine, type(create_engine("sqlite:///:memory:")))
    
    def test_engine_setter(self):
        """Test setting engine via property setter."""
        # Initialize original engine
        self.db_manager.initialize_engine()
        original_engine = self.db_manager.engine
        
        # Set a new engine
        new_engine = create_engine("sqlite:///:memory:")
        self.db_manager.engine = new_engine
        
        # Verify new engine is set
        assert self.db_manager.engine is new_engine
        assert self.db_manager.engine is not original_engine
        
        # Session factory should be reset
        assert self.db_manager.session_factory is None
    
    def test_session_factory_property_lazy_initialization(self):
        """Test that session_factory property initializes on first access."""
        # Session factory should be None initially
        assert self.db_manager.session_factory is None
        
        # Initialize session factory
        self.db_manager.initialize_session_factory()
        
        # Session factory should now be available
        assert self.db_manager.session_factory is not None
    
    def test_session_factory_setter(self):
        """Test setting session_factory via property setter."""
        # Initialize original session factory
        self.db_manager.initialize_session_factory()
        original_factory = self.db_manager.session_factory
        
        # Set a new session factory
        new_engine = create_engine("sqlite:///:memory:")
        new_factory = _create_session_factory(new_engine)
        self.db_manager.session_factory = new_factory
        
        # Verify new factory is set
        assert self.db_manager.session_factory is new_factory
        assert self.db_manager.session_factory is not original_factory
    
    def test_get_session(self):
        """Test getting a database session."""
        session = self.db_manager.get_session()
        
        assert isinstance(session, Session)
        assert session.is_active
        
        # Cleanup
        session.close()
    
    def test_init_db(self):
        """Test database initialization (table creation)."""
        self.db_manager.init_db()
        
        # Verify tables exist by querying them
        session = self.db_manager.get_session()
        try:
            # These queries will fail if tables don't exist
            session.query(EmergencyCallModel).first()
            session.query(RiskScoreModel).first()
            session.query(AlertModel).first()
        finally:
            session.close()
    
    def test_close(self):
        """Test closing database connections."""
        self.db_manager.initialize_engine()
        self.db_manager.initialize_session_factory()
        
        assert self.db_manager.engine is not None
        assert self.db_manager.session_factory is not None
        
        self.db_manager.close()
        
        assert self.db_manager.engine is None
        assert self.db_manager.session_factory is None


class TestDatabaseUtilityFunctions:
    """Test cases for database utility functions."""
    
    def test_create_engine(self):
        """Test engine creation utility function."""
        engine = _create_engine("sqlite:///:memory:")
        
        assert engine is not None
        assert engine.url.database == ":memory:"
        
        # Cleanup
        _dispose_engine(engine)
    
    def test_create_session_factory(self):
        """Test session factory creation utility function."""
        engine = _create_engine("sqlite:///:memory:")
        try:
            session_factory = _create_session_factory(engine)
            
            assert session_factory is not None
            # Verify the session factory is bound to the engine by creating a session
            session = session_factory()
            try:
                assert session.bind is engine
            finally:
                session.close()
        finally:
            _dispose_engine(engine)
    
    def test_dispose_engine(self):
        """Test engine disposal utility function."""
        engine = _create_engine("sqlite:///:memory:")
        
        # Should not raise
        _dispose_engine(engine)
        _dispose_engine(None)  # Should handle None gracefully


class TestDatabaseModelConversions:
    """Test cases for model-to-schema conversions."""
    
    def test_emergency_call_model_to_schema(self):
        """Test converting EmergencyCallModel to EmergencyCall schema."""
        model = EmergencyCallModel(
            call_id="CALL-001",
            timestamp=datetime.utcnow(),
            caller_lat=20.8783,
            caller_lon=-156.6825,
            transcript="Test transcript",
            call_metadata={"phoneNumber": "+1-808-555-0123"},  # Use call_metadata, not metadata
            is_enriched=True,
        )
        
        schema = emergency_call_model_to_schema(model)
        
        assert schema.callId == "CALL-001"
        assert schema.callerLocation.lat == 20.8783
        assert schema.callerLocation.lon == -156.6825
        assert schema.transcript == "Test transcript"
        # metadata is a CallMetadata object, access as attribute
        assert schema.metadata is not None
        assert schema.metadata.phoneNumber == "+1-808-555-0123"
    
    def test_emergency_call_schema_to_model(self):
        """Test converting EmergencyCall schema to EmergencyCallModel."""
        from sentinel_dispatch.api.schemas import CallMetadata
        
        schema = EmergencyCall(
            callId="CALL-001",
            timestamp="2024-08-08T14:23:45Z",
            callerLocation=CallerLocation(lat=20.8783, lon=-156.6825),
            transcript="Test transcript",
            metadata=CallMetadata(phoneNumber="+1-808-555-0123"),  # Use CallMetadata object
        )
        
        model = emergency_call_schema_to_model(schema, is_enriched=True)
        
        assert model.call_id == "CALL-001"
        assert model.caller_lat == 20.8783
        assert model.caller_lon == -156.6825
        assert model.transcript == "Test transcript"
        assert model.is_enriched is True
    
    def test_risk_score_model_to_schema(self):
        """Test converting RiskScoreModel to DashboardRiskScore schema."""
        model = RiskScoreModel(
            call_id="CALL-001",
            timestamp=datetime.utcnow(),
            risk_score=85.0,
            confidence=0.95,
            factors={"call_urgency": 0.9},
            recommendation="IMMEDIATE_DISPATCH",
            priority=5,
        )
        
        schema = risk_score_model_to_schema(model)
        
        assert schema.callId == "CALL-001"
        assert schema.riskScore == 85.0
        assert schema.confidence == 0.95
        assert schema.factors == {"call_urgency": 0.9}
        assert schema.recommendation == "IMMEDIATE_DISPATCH"
        assert schema.priority == 5
    
    def test_risk_score_schema_to_model(self):
        """Test converting DashboardRiskScore schema to RiskScoreModel."""
        schema = DashboardRiskScore(
            callId="CALL-001",
            timestamp="2024-08-08T14:23:50Z",
            riskScore=85.0,
            confidence=0.95,
            factors={"call_urgency": 0.9},
            recommendation="IMMEDIATE_DISPATCH",
            priority=5,
        )
        
        model = risk_score_schema_to_model(schema)
        
        assert model.call_id == "CALL-001"
        assert model.risk_score == 85.0
        assert model.confidence == 0.95
        assert model.priority == 5
    
    def test_alert_model_to_schema(self):
        """Test converting AlertModel to Alert schema."""
        model = AlertModel(
            alert_id="ALERT-001",
            timestamp=datetime.utcnow(),
            severity="critical",
            message="High risk case",
            alert_type="risk_threshold",
            call_id="CALL-001",
            acknowledged=False,
        )
        
        schema = alert_model_to_schema(model)
        
        assert schema.alertId == "ALERT-001"
        assert schema.severity == "critical"
        assert schema.message == "High risk case"
        assert schema.callId == "CALL-001"
        assert schema.acknowledged is False
    
    def test_alert_schema_to_model(self):
        """Test converting Alert schema to AlertModel."""
        schema = Alert(
            alertId="ALERT-001",
            timestamp="2024-08-08T14:24:00Z",
            severity="critical",
            message="High risk case",
            alert_type="risk_threshold",
            callId="CALL-001",
            acknowledged=False,
        )
        
        model = alert_schema_to_model(schema)
        
        assert model.alert_id == "ALERT-001"
        assert model.severity == "critical"
        assert model.message == "High risk case"
        assert model.call_id == "CALL-001"
        assert model.acknowledged is False


class TestGlobalDatabaseFunctions:
    """Test cases for global database functions."""
    
    def setup_method(self):
        """Set up test data before each test."""
        # Reset global database manager
        import sentinel_dispatch.api.database as db_module
        db_module._db_manager = None
    
    def teardown_method(self):
        """Clean up after each test."""
        # Cleanup global database manager
        import sentinel_dispatch.api.database as db_module
        if db_module._db_manager:
            db_module._db_manager.close()
        db_module._db_manager = None
    
    def test_get_db_manager(self):
        """Test getting the global database manager."""
        db_manager = get_db_manager()
        
        assert db_manager is not None
        assert isinstance(db_manager, DatabaseManager)
        
        # Should return the same instance on subsequent calls
        db_manager2 = get_db_manager()
        assert db_manager is db_manager2
    
    def test_init_db(self):
        """Test database initialization."""
        # Use in-memory database for testing
        import sentinel_dispatch.api.database as db_module
        db_module._db_manager = DatabaseManager("sqlite:///:memory:")
        
        init_db()
        
        # Verify tables exist
        db_manager = get_db_manager()
        session = db_manager.get_session()
        try:
            session.query(EmergencyCallModel).first()
            session.query(RiskScoreModel).first()
            session.query(AlertModel).first()
        finally:
            session.close()
    
    def test_get_db_sync(self):
        """Test getting a synchronous database session."""
        import sentinel_dispatch.api.database as db_module
        db_module._db_manager = DatabaseManager("sqlite:///:memory:")
        db_module._db_manager.init_db()
        
        session = get_db_sync()
        
        assert isinstance(session, Session)
        assert session.is_active
        
        # Cleanup
        session.close()
    
    @pytest.mark.asyncio
    async def test_get_db(self):
        """Test getting a database session for FastAPI dependency injection."""
        import sentinel_dispatch.api.database as db_module
        db_module._db_manager = DatabaseManager("sqlite:///:memory:")
        db_module._db_manager.init_db()
        
        # get_db is a generator, iterate it
        db_gen = get_db()
        session = next(db_gen)
        
        assert isinstance(session, Session)
        assert session.is_active
        
        # Cleanup (generator will close session)
        try:
            next(db_gen)
        except StopIteration:
            pass

