"""
Alert Generation API router.
"""

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, status

from sentinel_dispatch.ai_ml import generate_alerts
from sentinel_dispatch.api.schemas import GenerateAlertRequest, GenerateAlertResponse, ErrorResponse
from sentinel_dispatch.utils.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/generate-alert", tags=["alerts"])


@router.post(
    "",
    response_model=GenerateAlertResponse,
    status_code=status.HTTP_200_OK,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid request"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def generate_alert_endpoint(request: GenerateAlertRequest) -> GenerateAlertResponse:
    """
    Generate alerts based on risk score and enriched call data.
    
    Args:
        request: Alert generation request with risk score and optional enriched call
        
    Returns:
        List of generated alerts with severity and metadata
        
    Raises:
        HTTPException: If alert generation fails
    """
    call_id = request.risk_score.call_id
    
    logger.info(
        "sentinel_dispatch.api.routers.alerts.generate_alert_endpoint.received",
        call_id=call_id,
        risk_score=request.risk_score.risk_score,
        has_enriched_call=request.enriched_call is not None
    )
    
    try:
        # Generate alerts
        alerts = generate_alerts(
            request.risk_score,
            enriched_call=request.enriched_call,
        )
        
        # Convert alerts to dictionaries
        alert_dicts = [alert.to_dict() for alert in alerts]
        
        logger.info(
            "sentinel_dispatch.api.routers.alerts.generate_alert_endpoint.completed",
            call_id=call_id,
            alert_count=len(alerts)
        )
        
        return GenerateAlertResponse(
            alerts=alert_dicts,
            alert_count=len(alerts),
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        
    except ValueError as e:
        logger.error(
            "sentinel_dispatch.api.routers.alerts.generate_alert_endpoint.validation_error",
            call_id=call_id,
            error=str(e)
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request: {str(e)}"
        )
    except Exception as e:
        logger.error(
            "sentinel_dispatch.api.routers.alerts.generate_alert_endpoint.error",
            call_id=call_id,
            error=str(e),
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Alert generation failed: {str(e)}"
        )


@router.get("/health")
async def health_check() -> dict:
    """Health check endpoint."""
    return {"status": "healthy", "service": "alert-generation"}

