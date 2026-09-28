"""
FastAPI main application for Sentinel Dispatch AI/ML Services.

This application provides REST API endpoints for:
- Call classification (Gemini NLP)
- Data enrichment (spatial-temporal joins)
- Risk scoring (Vertex AI or rule-based)
- Alert generation (threshold-based triggers)
"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

from sentinel_dispatch.api.routers import alerts, classify, enrich, score, dashboard, websocket
from sentinel_dispatch.api.kafka_consumer import start_kafka_consumer, stop_kafka_consumer
from sentinel_dispatch.utils.logging_config import configure_logging, get_logger
import os

# Configure logging - use DEBUG level if LOG_LEVEL env var is set, otherwise INFO
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
configure_logging(level=log_level)
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Lifespan context manager for FastAPI application.
    
    Handles startup and shutdown events.
    """
    # Startup
    logger.info(
        "sentinel_dispatch.api.main.lifespan.startup",
        message="Starting Sentinel Dispatch AI/ML Services"
    )

    # Initialize database
    try:
        from sentinel_dispatch.api.database import init_db
        init_db()
        logger.info(
            "sentinel_dispatch.api.main.lifespan.database_initialized",
            message="Database initialized successfully"
        )
    except Exception as e:
        logger.error(
            "sentinel_dispatch.api.main.lifespan.database_init_error",
            error=str(e),
            exc_info=True,
            message="Database initialization failed, but API will continue",
        )

    # Start Kafka consumer for Streaming Agent output topics
    kafka_bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092"  # Default for local development
    )
    try:
        await start_kafka_consumer(kafka_bootstrap_servers)
        logger.info(
            "sentinel_dispatch.api.main.lifespan.kafka_consumer_started",
            bootstrap_servers=kafka_bootstrap_servers,
        )
    except Exception as e:
        logger.error(
            "sentinel_dispatch.api.main.lifespan.kafka_consumer_start_error",
            error=str(e),
            exc_info=True,
            message="Kafka consumer failed to start, but API will continue without real-time updates",
        )

    yield

    # Shutdown
    logger.info(
        "sentinel_dispatch.api.main.lifespan.shutdown",
        message="Shutting down Sentinel Dispatch AI/ML Services"
    )

    # Stop Kafka consumer
    try:
        await stop_kafka_consumer()
        logger.info(
            "sentinel_dispatch.api.main.lifespan.kafka_consumer_stopped",
        )
    except Exception as e:
        logger.error(
            "sentinel_dispatch.api.main.lifespan.kafka_consumer_stop_error",
            error=str(e),
            exc_info=True,
        )
    
    # Close database connections
    try:
        from sentinel_dispatch.api.database import get_db_manager
        get_db_manager().close()
        logger.info(
            "sentinel_dispatch.api.main.lifespan.database_closed",
            message="Database connections closed"
        )
    except Exception as e:
        logger.error(
            "sentinel_dispatch.api.main.lifespan.database_close_error",
            error=str(e),
            exc_info=True,
        )


# Create FastAPI application
app = FastAPI(
    title="Sentinel Dispatch AI/ML Services",
    description="REST API for emergency call classification, data enrichment, risk scoring, and alert generation",
    version="0.1.0",
    lifespan=lifespan,
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify allowed origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(classify.router)
app.include_router(enrich.router)
app.include_router(score.router)
app.include_router(alerts.router)
app.include_router(dashboard.router)
app.include_router(websocket.router)


@app.get("/")
async def root() -> dict:
    """Root endpoint."""
    return {
        "service": "Sentinel Dispatch AI/ML Services",
        "version": "0.1.0",
        "endpoints": {
            "classify": "/api/classify-call",
            "enrich": "/api/enrich-call",
            "score": "/api/score-risk",
            "alerts": "/api/generate-alert",
            "dashboard": {
                "calls": "/api/calls",
                "call_detail": "/api/calls/{call_id}",
                "risk_scores": "/api/risk-scores",
                "prioritized_cases": "/api/prioritized-cases",
                "alerts": "/api/alerts",
                "acknowledge_alert": "/api/alerts/{alert_id}/acknowledge",
            },
            "websocket": {
                "endpoint": "/ws",
                "status": "/ws/status",
            },
        },
    }


@app.get("/health")
async def health_check() -> dict:
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "sentinel-dispatch-ai-ml-services",
    }



@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Handle request validation errors with detailed logging."""
    # Get request body if available
    try:
        body = await request.body()
        body_str = body.decode('utf-8') if body else "No body"
    except Exception:
        body_str = "Could not read body"
    
    logger.error(
        "sentinel_dispatch.api.main.validation_error",
        path=request.url.path,
        method=request.method,
        errors=exc.errors(),
        body=body_str,
        headers=dict(request.headers)
    )
    
    return JSONResponse(
        status_code=422,
        content={
            "detail": exc.errors(),
            "body_received": body_str[:500] if len(body_str) > 500 else body_str
        },
    )


@app.exception_handler(Exception)
async def global_exception_handler(request, exc: Exception) -> JSONResponse:
    """Global exception handler."""
    logger.error(
        "sentinel_dispatch.api.main.global_exception_handler.error",
        path=request.url.path,
        method=request.method,
        error=str(exc),
        exc_info=True
    )
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc),
        },
    )


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "sentinel_dispatch.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )

