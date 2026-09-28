"""
Database models and session management for SQLite storage.

Replaces in-memory stores with persistent SQLite database.

Architecture:
- DatabaseManager: Encapsulates engine creation and lifecycle
- Session Factory: Shared across all components (FastAPI, Kafka, WebSocket)
- Per-Request Sessions: Each component gets its own session (best practice)
  - FastAPI endpoints: Request-scoped sessions via dependency injection
  - Kafka consumer: Per-message sessions (thread-safe)
  - WebSocket handlers: Per-operation sessions (async-safe)

Why NOT share a single session:
1. Thread Safety: SQLite doesn't handle concurrent writes with a single connection
2. Transaction Isolation: Different operations need different transaction boundaries
3. FastAPI Best Practice: Request-scoped sessions prevent connection leaks
4. Error Isolation: One failed transaction shouldn't affect others
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Generator, Callable, TypeVar, Any, TYPE_CHECKING
from sqlalchemy import create_engine, Column, String, Float, Integer, Boolean, JSON, DateTime, Text, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, IntegrityError
import os
import time
import random

if TYPE_CHECKING:
    from sentinel_dispatch.api.schemas import EmergencyCall, DashboardRiskScore, Alert

T = TypeVar('T')

Base = declarative_base()


class EmergencyCallModel(Base):
    """SQLite model for emergency calls."""
    __tablename__ = "emergency_calls"
    
    call_id = Column(String, primary_key=True, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    caller_lat = Column(Float, nullable=False)
    caller_lon = Column(Float, nullable=False)
    transcript = Column(Text, nullable=False)
    call_metadata = Column("metadata", JSON, nullable=True)  # Python attr 'call_metadata', DB column 'metadata'
    is_enriched = Column(Boolean, default=False, index=True)  # Track if call has been fully enriched
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class RiskScoreModel(Base):
    """SQLite model for risk scores."""
    __tablename__ = "risk_scores"
    
    call_id = Column(String, primary_key=True, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    risk_score = Column(Float, nullable=False, index=True)
    confidence = Column(Float, nullable=False)
    factors = Column(JSON, nullable=False)
    recommendation = Column(String, nullable=True)  # Allow None to match schema
    priority = Column(Integer, nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AlertModel(Base):
    """SQLite model for alerts."""
    __tablename__ = "alerts"
    
    alert_id = Column(String, primary_key=True, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    severity = Column(String, nullable=False, index=True)
    message = Column(Text, nullable=False)
    alert_type = Column(String, nullable=True)
    call_id = Column(String, nullable=True, index=True)
    event = Column(String, nullable=True)
    headline = Column(String, nullable=True)
    description = Column(Text, nullable=True)
    urgency = Column(String, nullable=True)
    certainty = Column(String, nullable=True)
    effective = Column(DateTime, nullable=True)
    expires = Column(DateTime, nullable=True)
    status = Column(String, nullable=True)
    acknowledged = Column(Boolean, default=False, index=True)
    alert_metadata = Column("metadata", JSON, nullable=True)  # Python attr 'alert_metadata', DB column 'metadata'
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


def _create_engine(database_url: str) -> Engine:
    """
    Create and configure SQLAlchemy engine.
    
    Args:
        database_url: Database URL
        
    Returns:
        Configured SQLAlchemy engine
    """
    # SQLite-specific configuration
    connect_args = {}
    poolclass = None
    
    if "sqlite" in database_url:
        # SQLite: Allow multiple threads, use StaticPool for connection reuse
        # Set timeout to handle concurrent writes better (default is 5 seconds)
        connect_args = {
            "check_same_thread": False,
            "timeout": 10.0,  # Increase timeout for concurrent writes
        }
        poolclass = StaticPool
    
    engine = create_engine(
        database_url,
        connect_args=connect_args,
        poolclass=poolclass,
        echo=False,  # Set to True for SQL query logging
    )
    
    # Enable WAL mode for SQLite (better concurrent read/write performance)
    if "sqlite" in database_url:
        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(dbapi_conn, connection_record):
            """Enable WAL mode for better SQLite concurrency."""
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")  # Balance between safety and speed
            cursor.execute("PRAGMA busy_timeout=10000")  # 10 second timeout
            cursor.close()
    
    return engine


def _create_session_factory(engine: Engine) -> sessionmaker:
    """
    Create session factory bound to the given engine.
    
    Args:
        engine: SQLAlchemy engine
        
    Returns:
        SQLAlchemy sessionmaker
    """
    return sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine
    )


def _dispose_engine(engine: Optional[Engine]) -> None:
    """
    Dispose of a database engine and cleanup resources.
    
    Args:
        engine: Engine to dispose (can be None)
    """
    if engine is not None:
        engine.dispose()


class DatabaseManager:
    """
    Encapsulates database engine creation and session management.
    
    This class provides a centralized way to:
    - Create and configure the database engine
    - Initialize database tables
    - Provide session factories for different use cases
    - Manage database lifecycle
    
    The engine and session factory are shared across all components,
    but each component creates its own session instances (best practice).
    """
    
    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize database manager.
        
        Args:
            database_url: Database URL (defaults to DATABASE_URL env var or sqlite:///./sentinel_dispatch.db)
        """
        self.database_url = database_url or os.getenv("DATABASE_URL", "sqlite:///./sentinel_dispatch.db")
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None
    
    def initialize_engine(self) -> None:
        """
        Initialize the database engine.
        
        This should be called before accessing the engine property,
        or the engine can be set directly via the property setter.
        """
        if self.engine is not None:
            return
        
        self.engine = _create_engine(self.database_url)
    
    def initialize_session_factory(self) -> None:
        """
        Initialize the session factory.
        
        This will also initialize the engine if it hasn't been initialized yet.
        """
        if self.session_factory is not None:
            return
        
        if self.engine is None:
            self.initialize_engine()
        
        self.session_factory = _create_session_factory(self.engine)
    
    @property
    def engine(self) -> Optional[Engine]:
        """
        Get the database engine.
        
        Returns:
            Configured SQLAlchemy engine, or None if not initialized
            
        Note:
            Use initialize_engine() to create the engine, or set it directly
            via the property setter.
        """
        return self._engine
    
    @engine.setter
    def engine(self, value: Engine) -> None:
        """
        Set the database engine explicitly.
        
        This is useful for testing or when you want to provide a custom engine.
        Setting the engine will also reset the session factory.
        
        Args:
            value: SQLAlchemy engine instance
        """
        _dispose_engine(self.engine)
        self._engine = value
        # Reset session factory since it's bound to the engine
        self._session_factory = None
    
    @property
    def session_factory(self) -> Optional[sessionmaker]:
        """
        Get the session factory.
        
        Returns:
            SQLAlchemy sessionmaker, or None if not initialized
            
        Note:
            Use initialize_session_factory() to create the session factory,
            or set it directly via the property setter.
        """
        return self._session_factory
    
    @session_factory.setter
    def session_factory(self, value: sessionmaker) -> None:
        """
        Set the session factory explicitly.
        
        This is useful for testing or when you want to provide a custom session factory.
        
        Args:
            value: SQLAlchemy sessionmaker instance
        """
        self._session_factory = value
    
    def init_db(self) -> None:
        """
        Initialize database by creating all tables.
        
        This should be called during application startup.
        It will initialize the engine if it hasn't been initialized yet.
        """
        if self.engine is None:
            self.initialize_engine()
        Base.metadata.create_all(bind=self.engine)
    
    def get_session(self) -> Session:
        """
        Get a new database session.
        
        This method ensures the session factory is initialized and returns
        a new session instance. The caller is responsible for closing the session.
        
        Returns:
            Database session (must be closed by caller)
        """
        self.initialize_session_factory()
        return self.session_factory()  # Call session_factory to create a new Session instance
    
    def close(self) -> None:
        """Close database engine and cleanup resources."""
        _dispose_engine(self.engine)
        self.engine = None
        self.session_factory = None


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_db_manager() -> DatabaseManager:
    """
    Get or create the global database manager instance.
    
    This ensures a single engine and session factory are shared across
    all components (FastAPI, Kafka consumer, WebSocket handlers).
    
    Returns:
        DatabaseManager instance
    """
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


def init_db() -> Engine:
    """
    Initialize database (creates tables).
    
    Should be called during FastAPI app startup.
    
    Returns:
        Database engine
    """
    return get_db_manager().init_db()


def get_db() -> Generator[Session, None, None]:
    """
    Get database session for FastAPI dependency injection.
    
    This creates a request-scoped session that is automatically
    closed after the request completes.
    
    Usage:
        @router.get("/endpoint")
        async def endpoint(db: Session = Depends(get_db)):
            # Use db session
            pass
    
    Yields:
        Database session
    """
    db = get_db_manager().get_session()
    try:
        yield db
    finally:
        db.close()


def get_db_sync() -> Session:
    """
    Get database session synchronously (for non-FastAPI code like Kafka consumer).
    
    IMPORTANT: The caller is responsible for closing the session!
    
    Usage:
        db = get_db_sync()
        try:
            # Use db session
            pass
        finally:
            db.close()
    
    Returns:
        Database session (must be closed by caller)
    """
    return get_db_manager().get_session()


def with_db_retry(
    max_retries: int = 3,
    initial_delay: float = 0.1,
    max_delay: float = 1.0,
    backoff_factor: float = 2.0,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator to retry database operations on lock/timeout errors.
    
    SQLite can experience "database is locked" errors when multiple sessions
    try to write simultaneously. This decorator retries the operation with
    exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds before first retry
        max_delay: Maximum delay between retries
        backoff_factor: Multiplier for exponential backoff
    
    Usage:
        @with_db_retry(max_retries=5)
        def save_call(db: Session, call_data):
            db.merge(call_model)
            db.commit()
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args: Any, **kwargs: Any) -> T:
            delay = initial_delay
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (OperationalError, IntegrityError) as e:
                    # Check if it's a lock/timeout error
                    error_msg = str(e).lower()
                    is_lock_error = (
                        "database is locked" in error_msg or
                        "database locked" in error_msg or
                        "timeout" in error_msg or
                        "locked" in error_msg
                    )
                    
                    if not is_lock_error:
                        # Not a lock error, re-raise immediately
                        raise
                    
                    if attempt < max_retries:
                        last_exception = e
                        # Add jitter to prevent thundering herd
                        jitter = random.uniform(0, delay * 0.1)
                        time.sleep(delay + jitter)
                        delay = min(delay * backoff_factor, max_delay)
                    else:
                        # Max retries exceeded
                        raise
                except Exception:
                    # Non-database errors, re-raise immediately
                    raise
            
            # Should never reach here, but just in case
            if last_exception:
                raise last_exception
            raise RuntimeError("Unexpected retry loop exit")
        
        return wrapper
    return decorator


# Helper functions to convert between database models and API schemas
def emergency_call_model_to_schema(model: EmergencyCallModel) -> 'EmergencyCall':
    """Convert EmergencyCallModel to EmergencyCall schema."""
    from sentinel_dispatch.api.schemas import EmergencyCall, CallerLocation, CallMetadata
    
    # Convert metadata dict back to CallMetadata object if present
    metadata_obj = None
    if model.call_metadata:
        if isinstance(model.call_metadata, dict):
            # Convert dict to CallMetadata Pydantic model
            metadata_obj = CallMetadata(**model.call_metadata)
        else:
            # Already an object or None
            metadata_obj = model.call_metadata
    
    return EmergencyCall(
        callId=model.call_id,
        timestamp=model.timestamp.isoformat() if isinstance(model.timestamp, datetime) else model.timestamp,
        callerLocation=CallerLocation(
            lat=model.caller_lat,
            lon=model.caller_lon,
        ),
        transcript=model.transcript,
        metadata=metadata_obj,  # Convert back to CallMetadata object
    )


def emergency_call_schema_to_model(schema: 'EmergencyCall', is_enriched: bool = False) -> EmergencyCallModel:
    """Convert EmergencyCall schema to EmergencyCallModel."""
    from datetime import datetime as dt
    
    timestamp = schema.timestamp
    if isinstance(timestamp, str):
        try:
            timestamp = dt.fromisoformat(timestamp.replace("Z", "+00:00"))
        except Exception:
            timestamp = dt.utcnow()
    
    # Handle callerLocation - it can be a CallerLocation object or a dict
    if hasattr(schema.callerLocation, 'lat'):
        # It's a CallerLocation Pydantic model
        caller_lat = schema.callerLocation.lat
        caller_lon = schema.callerLocation.lon
    elif isinstance(schema.callerLocation, dict):
        # It's a dictionary
        caller_lat = schema.callerLocation["lat"]
        caller_lon = schema.callerLocation["lon"]
    else:
        # Fallback
        caller_lat = 0.0
        caller_lon = 0.0
    
    # Handle metadata - convert Pydantic model to dict for JSON storage
    metadata_dict = None
    if schema.metadata is not None:
        if hasattr(schema.metadata, 'model_dump'):
            # It's a Pydantic model (CallMetadata)
            metadata_dict = schema.metadata.model_dump()
        elif hasattr(schema.metadata, 'dict'):
            # Fallback for older Pydantic versions
            metadata_dict = schema.metadata.dict()
        elif isinstance(schema.metadata, dict):
            # Already a dict
            metadata_dict = schema.metadata
        else:
            # Try to convert to dict
            metadata_dict = dict(schema.metadata) if schema.metadata else None
    
    return EmergencyCallModel(
        call_id=schema.callId,
        timestamp=timestamp,
        caller_lat=caller_lat,
        caller_lon=caller_lon,
        transcript=schema.transcript,
        call_metadata=metadata_dict,  # Store as dict for JSON column
        is_enriched=is_enriched,
    )


def risk_score_model_to_schema(model: RiskScoreModel) -> 'DashboardRiskScore':
    """Convert RiskScoreModel to DashboardRiskScore schema."""
    from sentinel_dispatch.api.schemas import DashboardRiskScore
    
    return DashboardRiskScore(
        callId=model.call_id,
        timestamp=model.timestamp.isoformat() if isinstance(model.timestamp, datetime) else model.timestamp,
        riskScore=model.risk_score,
        confidence=model.confidence,
        factors=model.factors,
        recommendation=model.recommendation,
        priority=model.priority,
    )


def risk_score_schema_to_model(schema: 'DashboardRiskScore') -> RiskScoreModel:
    """Convert DashboardRiskScore schema to RiskScoreModel."""
    from datetime import datetime as dt
    
    timestamp = schema.timestamp
    if isinstance(timestamp, str):
        try:
            timestamp = dt.fromisoformat(timestamp.replace("Z", "+00:00"))
        except Exception:
            timestamp = dt.utcnow()
    
    return RiskScoreModel(
        call_id=schema.callId,
        timestamp=timestamp,
        risk_score=schema.riskScore,
        confidence=schema.confidence,
        factors=schema.factors,
        recommendation=schema.recommendation,
        priority=schema.priority,
    )


def alert_model_to_schema(model: AlertModel) -> 'Alert':
    """Convert AlertModel to Alert schema."""
    from sentinel_dispatch.api.schemas import Alert
    
    # Build alert data with only non-None optional fields
    # The Alert schema only requires: alertId, timestamp, severity, message, alert_type, callId, acknowledged
    alert_data = {
        "alertId": model.alert_id,
        "timestamp": model.timestamp.isoformat() if isinstance(model.timestamp, datetime) else model.timestamp,
        "severity": model.severity,
        "message": model.message,
        "alert_type": model.alert_type,
        "callId": model.call_id,
        "acknowledged": model.acknowledged,
    }
    
    # Add optional fields only if they have values (Alert schema allows extra fields)
    if model.event:
        alert_data["event"] = model.event
    if model.headline:
        alert_data["headline"] = model.headline
    if model.description:
        alert_data["description"] = model.description
    if model.urgency:
        alert_data["urgency"] = model.urgency
    if model.certainty:
        alert_data["certainty"] = model.certainty
    if model.effective:
        alert_data["effective"] = model.effective.isoformat() if isinstance(model.effective, datetime) else model.effective
    if model.expires:
        alert_data["expires"] = model.expires.isoformat() if isinstance(model.expires, datetime) else model.expires
    if model.status:
        alert_data["status"] = model.status
    if model.alert_metadata:
        alert_data["metadata"] = model.alert_metadata
    
    return Alert(**alert_data)


def alert_schema_to_model(schema: 'Alert') -> AlertModel:
    """Convert Alert schema to AlertModel."""
    from datetime import datetime as dt
    
    def parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
        if not dt_str:
            return None
        if isinstance(dt_str, datetime):
            return dt_str
        try:
            return dt.fromisoformat(dt_str.replace("Z", "+00:00"))
        except Exception:
            return None
    
    timestamp = parse_datetime(schema.timestamp) or dt.utcnow()
    
    # Extract optional fields (Alert schema allows extra fields via model_config)
    # Use getattr with default None for fields that may not exist
    event = getattr(schema, "event", None)
    headline = getattr(schema, "headline", None)
    description = getattr(schema, "description", None)
    urgency = getattr(schema, "urgency", None)
    certainty = getattr(schema, "certainty", None)
    effective = getattr(schema, "effective", None)
    expires = getattr(schema, "expires", None)
    status = getattr(schema, "status", None)
    
    # Extract metadata if present (Alert schema allows extra fields)
    metadata = None
    if hasattr(schema, "metadata"):
        metadata = schema.metadata
    elif hasattr(schema, "__dict__") and "metadata" in schema.__dict__:
        metadata = schema.__dict__["metadata"]
    
    return AlertModel(
        alert_id=schema.alertId,
        timestamp=timestamp,
        severity=schema.severity,
        message=schema.message,
        alert_type=schema.alert_type,
        call_id=schema.callId,
        event=event,
        headline=headline,
        description=description,
        urgency=urgency,
        certainty=certainty,
        effective=parse_datetime(effective),
        expires=parse_datetime(expires),
        status=status,
        acknowledged=schema.acknowledged or False,
        alert_metadata=metadata,  # Use the mapped attribute name
    )
