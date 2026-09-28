"""
Risk Scoring API router.
"""

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, status

from sentinel_dispatch.ai_ml import RiskScoreInput, calculate_risk_score
from sentinel_dispatch.api.schemas import ScoreRiskRequest, ScoreRiskResponse, ErrorResponse
from sentinel_dispatch.utils.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/score-risk", tags=["risk-scoring"])


@router.post(
    "",
    response_model=ScoreRiskResponse,
    status_code=status.HTTP_200_OK,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid request"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def score_risk_endpoint(request: ScoreRiskRequest) -> ScoreRiskResponse:
    """
    Calculate risk score for an emergency call using hybrid approach (Vertex AI or rule-based).
    
    Args:
        request: Risk scoring request with call data and contextual information
        
    Returns:
        Risk score result with factors, recommendation, and priority
        
    Raises:
        HTTPException: If risk scoring fails
    """
    logger.info(
        "sentinel_dispatch.api.routers.score.score_risk_endpoint.received",
        call_id=request.call_id,
        has_fire_data=request.nearest_fire is not None,
        has_weather_data=request.weather_data is not None,
        has_vulnerability_data=request.vulnerability_factors is not None
    )
    
    try:
        # Create risk score input
        input_data = RiskScoreInput(
            call_id=request.call_id,
            call_timestamp=request.call_timestamp,
            caller_location=request.caller_location,
            call_classification=request.call_classification,
            nearest_fire=request.nearest_fire,
            weather_data=request.weather_data,
            vulnerability_factors=request.vulnerability_factors,
            current_timestamp=request.current_timestamp or datetime.now(timezone.utc).isoformat(),
        )
        
        # Calculate risk score
        risk_score = calculate_risk_score(input_data)
        
        logger.info(
            "sentinel_dispatch.api.routers.score.score_risk_endpoint.completed",
            call_id=request.call_id,
            risk_score=risk_score.risk_score,
            recommendation=risk_score.recommendation,
            priority=risk_score.priority
        )
        
        return ScoreRiskResponse(
            risk_score=risk_score,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        
    except ValueError as e:
        logger.error(
            "sentinel_dispatch.api.routers.score.score_risk_endpoint.validation_error",
            call_id=request.call_id,
            error=str(e)
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request: {str(e)}"
        )
    except Exception as e:
        logger.error(
            "sentinel_dispatch.api.routers.score.score_risk_endpoint.error",
            call_id=request.call_id,
            error=str(e),
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Risk scoring failed: {str(e)}"
        )


@router.get("/health")
async def health_check() -> dict:
    """Health check endpoint."""
    return {"status": "healthy", "service": "risk-scoring"}

