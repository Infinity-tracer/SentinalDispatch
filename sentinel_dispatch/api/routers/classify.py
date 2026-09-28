"""
Call Classification API router.
"""

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, status

from sentinel_dispatch.ai_ml import classify_call
from sentinel_dispatch.api.schemas import ClassifyCallRequest, ClassifyCallResponse, ErrorResponse
from sentinel_dispatch.utils.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/classify-call", tags=["classification"])


@router.post(
    "",
    response_model=ClassifyCallResponse,
    status_code=status.HTTP_200_OK,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid request"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def classify_call_endpoint(request: ClassifyCallRequest) -> ClassifyCallResponse:
    """
    Classify an emergency call transcript using Gemini NLP.
    
    Args:
        request: Classification request with transcript and optional metadata
        
    Returns:
        Classification result with urgency, incident type, entities, etc.
        
    Raises:
        HTTPException: If classification fails
    """
    call_id = request.call_id or f"call-{int(datetime.now(timezone.utc).timestamp())}"
    
    logger.debug(
        "sentinel_dispatch.api.routers.classify.classify_call_endpoint.request_received",
        call_id=call_id,
        transcript=request.transcript,
        transcript_length=len(request.transcript),
        metadata=request.metadata
    )
    
    logger.info(
        "sentinel_dispatch.api.routers.classify.classify_call_endpoint.received",
        call_id=call_id,
        transcript_length=len(request.transcript)
    )
    
    try:
        # Classify the call
        logger.debug(
            "sentinel_dispatch.api.routers.classify.classify_call_endpoint.calling_classify",
            call_id=call_id,
            transcript_preview=request.transcript[:100] if len(request.transcript) > 100 else request.transcript
        )
        classification = classify_call(request.transcript)
        
        logger.debug(
            "sentinel_dispatch.api.routers.classify.classify_call_endpoint.classification_received",
            call_id=call_id,
            classification=classification.model_dump() if hasattr(classification, 'model_dump') else str(classification)
        )
        
        logger.info(
            "sentinel_dispatch.api.routers.classify.classify_call_endpoint.completed",
            call_id=call_id,
            urgency=classification.urgency,
            incident_type=classification.incident_type,
            confidence=classification.confidence
        )
        
        response = ClassifyCallResponse(
            call_id=call_id,
            classification=classification,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        
        logger.debug(
            "sentinel_dispatch.api.routers.classify.classify_call_endpoint.response",
            call_id=call_id,
            response=response.model_dump() if hasattr(response, 'model_dump') else str(response)
        )
        
        return response
        
    except ValueError as e:
        error_msg = str(e)
        logger.error(
            "sentinel_dispatch.api.routers.classify.classify_call_endpoint.validation_error",
            call_id=call_id,
            error=error_msg,
            transcript_preview=request.transcript[:100] if request.transcript else ""
        )
        # Check if it's a Gemini API key error (should be 500)
        if "API key" in error_msg or "GEMINI_API_KEY" in error_msg or "api_key" in error_msg.lower():
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Gemini API key is not configured. Please set GEMINI_API_KEY environment variable."
            )
        # Other ValueError exceptions should be 400 (bad request)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request: {error_msg}"
        )
    except Exception as e:
        error_msg = str(e)
        logger.error(
            "sentinel_dispatch.api.routers.classify.classify_call_endpoint.error",
            call_id=call_id,
            error=error_msg,
            exc_info=True
        )
        # Check if it's a Gemini API key error
        if "API key" in error_msg or "GEMINI_API_KEY" in error_msg or "api_key" in error_msg.lower():
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Gemini API key is not configured. Please set GEMINI_API_KEY environment variable."
            )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Classification failed: {error_msg}"
        )


@router.get("/health")
async def health_check() -> dict:
    """Health check endpoint."""
    return {"status": "healthy", "service": "call-classification"}

