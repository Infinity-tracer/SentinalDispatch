"""
Dashboard API router for web frontend.

This router provides REST endpoints for the web dashboard to:
- List and retrieve emergency calls
- Get risk scores
- Get prioritized cases for triage
- Manage alerts
"""

from typing import List, Optional

from fastapi import APIRouter, HTTPException, status, Query, Depends
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from sentinel_dispatch.api.schemas import (
    CallsResponse,
    EmergencyCall,
    DashboardRiskScore,
    PrioritizedCasesResponse,
    PrioritizedCase,
    Alert,
    ErrorResponse,
)
from sentinel_dispatch.api.database import (
    get_db,
    EmergencyCallModel,
    RiskScoreModel,
    AlertModel,
    emergency_call_model_to_schema,
    risk_score_model_to_schema,
    alert_model_to_schema,
)
from sentinel_dispatch.utils.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api", tags=["dashboard"])


@router.get(
    "/calls",
    response_model=CallsResponse,
    status_code=status.HTTP_200_OK,
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def get_calls(
    limit: Optional[int] = Query(None, ge=1, le=1000, description="Maximum number of calls to return"),
    offset: Optional[int] = Query(0, ge=0, description="Number of calls to skip"),
    db: Session = Depends(get_db),
) -> CallsResponse:
    """
    List all emergency calls.
    
    Args:
        limit: Maximum number of calls to return (default: all)
        offset: Number of calls to skip (for pagination)
        db: Database session
        
    Returns:
        List of emergency calls with total count
    """
    logger.info(
        "sentinel_dispatch.api.routers.dashboard.get_calls.requested",
        limit=limit,
        offset=offset,
    )
    
    try:
        # Query all calls, sorted by timestamp (most recent first)
        query = db.query(EmergencyCallModel).order_by(EmergencyCallModel.timestamp.desc())
        
        # Get total count
        total = query.count()
        
        # Apply pagination
        if offset is not None:
            query = query.offset(offset)
        if limit is not None:
            query = query.limit(limit)
        
        # Convert models to schemas
        calls_list = [emergency_call_model_to_schema(model) for model in query.all()]
        
        return CallsResponse(
            calls=calls_list,
            total=total,
        )
    except Exception as e:
        logger.error(
            "sentinel_dispatch.api.routers.dashboard.get_calls.error",
            error=str(e),
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve calls: {str(e)}",
        )


@router.get(
    "/calls/{call_id}",
    response_model=EmergencyCall,
    status_code=status.HTTP_200_OK,
    responses={
        404: {"model": ErrorResponse, "description": "Call not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def get_call(
    call_id: str,
    db: Session = Depends(get_db),
) -> EmergencyCall:
    """
    Get detailed information for a specific emergency call.
    
    Args:
        call_id: Call identifier
        db: Database session
        
    Returns:
        Emergency call details
        
    Raises:
        HTTPException: If call is not found
    """
    logger.info(
        "sentinel_dispatch.api.routers.dashboard.get_call.requested",
        call_id=call_id,
    )
    
    try:
        call_model = db.query(EmergencyCallModel).filter(EmergencyCallModel.call_id == call_id).first()
        
        if not call_model:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Call {call_id} not found",
            )
        
        return emergency_call_model_to_schema(call_model)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "sentinel_dispatch.api.routers.dashboard.get_call.error",
            call_id=call_id,
            error=str(e),
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve call: {str(e)}",
        )


@router.get(
    "/risk-scores",
    response_model=List[DashboardRiskScore],
    status_code=status.HTTP_200_OK,
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def get_risk_scores(
    call_id: Optional[str] = Query(None, description="Filter by call ID"),
    limit: Optional[int] = Query(None, ge=1, le=1000, description="Maximum number of scores to return"),
    db: Session = Depends(get_db),
) -> List[DashboardRiskScore]:
    """
    Get risk scores for emergency calls.
    
    Args:
        call_id: Optional call ID to filter by (if provided, returns single score)
        limit: Maximum number of scores to return (default: all)
        db: Database session
        
    Returns:
        List of risk scores
    """
    logger.info(
        "sentinel_dispatch.api.routers.dashboard.get_risk_scores.requested",
        call_id=call_id,
        limit=limit,
    )
    
    try:
        # Build query - only include scores for calls that have been fully processed (is_enriched=True)
        query = db.query(RiskScoreModel).join(
            EmergencyCallModel,
            RiskScoreModel.call_id == EmergencyCallModel.call_id
        ).filter(
            EmergencyCallModel.is_enriched == True # noqa: E712
        )
        
        if call_id:
            # Return single score if call_id is provided
            query = query.filter(RiskScoreModel.call_id == call_id)
        
        # Sort by priority (highest first), then risk score (highest first)
        query = query.order_by(
            RiskScoreModel.priority.desc().nulls_last(),
            RiskScoreModel.risk_score.desc()
        )
        
        if limit is not None:
            query = query.limit(limit)
        
        # Convert models to schemas
        scores_list = [risk_score_model_to_schema(model) for model in query.all()]
        
        return scores_list
    except Exception as e:
        logger.error(
            "sentinel_dispatch.api.routers.dashboard.get_risk_scores.error",
            error=str(e),
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve risk scores: {str(e)}",
        )


@router.get(
    "/prioritized-cases",
    response_model=PrioritizedCasesResponse,
    status_code=status.HTTP_200_OK,
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def get_prioritized_cases(
    limit: Optional[int] = Query(None, ge=1, le=1000, description="Maximum number of cases to return"),
    min_priority: Optional[int] = Query(None, ge=1, le=5, description="Minimum priority level (5=critical, 1=low)"),
    db: Session = Depends(get_db),
) -> PrioritizedCasesResponse:
    """
    Get prioritized cases for triage decision-making.
    
    Cases are sorted by priority (highest first) and risk score, with associated alerts.
    
    Args:
        limit: Maximum number of cases to return (default: all)
        min_priority: Minimum priority level to include (default: all)
        db: Database session
        
    Returns:
        List of prioritized cases with alerts
    """
    logger.info(
        "sentinel_dispatch.api.routers.dashboard.get_prioritized_cases.requested",
        limit=limit,
        min_priority=min_priority,
    )
    
    try:
        # Query calls that have been fully processed (is_enriched=True) and have risk scores
        query = db.query(EmergencyCallModel, RiskScoreModel).join(
            RiskScoreModel,
            EmergencyCallModel.call_id == RiskScoreModel.call_id
        ).filter(
            EmergencyCallModel.is_enriched == True # noqa: E712
        )
        
        # Log query details for debugging
        total_calls = db.query(EmergencyCallModel).count()
        enriched_calls = db.query(EmergencyCallModel).filter(EmergencyCallModel.is_enriched == True).count() # noqa: E712
        total_risk_scores = db.query(RiskScoreModel).count()
        
        logger.info(
            "sentinel_dispatch.api.routers.dashboard.get_prioritized_cases.query_stats",
            total_calls=total_calls,
            enriched_calls=enriched_calls,
            total_risk_scores=total_risk_scores,
            min_priority=min_priority,
        )
        
        # Filter by minimum priority if specified
        if min_priority is not None:
            query = query.filter(
                or_(
                    RiskScoreModel.priority >= min_priority,
                    RiskScoreModel.priority.is_(None)  # Include null priorities if min_priority is 1
                )
            )
        
        # Sort by priority (highest first), then risk score (highest first)
        query = query.order_by(
            RiskScoreModel.priority.desc().nulls_last(),
            RiskScoreModel.risk_score.desc()
        )
        
        # Get all results (before limit) for total count
        all_results = query.all()
        total = len(all_results)
        
        logger.info(
            "sentinel_dispatch.api.routers.dashboard.get_prioritized_cases.query_results",
            total_results=total,
            limit=limit,
        )
        
        # Apply limit
        if limit is not None:
            all_results = all_results[:limit]
        
        # Build prioritized cases
        cases: List[PrioritizedCase] = []
        for call_model, risk_score_model in all_results:
            call_id = call_model.call_id
            
            # Get associated unacknowledged alerts
            alerts_query = db.query(AlertModel).filter(
                and_(
                    AlertModel.call_id == call_id,
                    AlertModel.acknowledged == False  # noqa: E712
                )
            )
            alert_models = alerts_query.all()
            alerts = [alert_model_to_schema(alert_model) for alert_model in alert_models] if alert_models else None
            
            cases.append(
                PrioritizedCase(
                    callId=call_id,
                    emergencyCall=emergency_call_model_to_schema(call_model),
                    riskScore=risk_score_model_to_schema(risk_score_model),
                    alerts=alerts,
                    hasFireWeatherWarning=False,  # TODO: Implement fire weather warning check
                    priority=risk_score_model.priority if risk_score_model.priority else 1,
                )
            )
        
        return PrioritizedCasesResponse(
            cases=cases,
            total=total,
        )
    except Exception as e:
        logger.error(
            "sentinel_dispatch.api.routers.dashboard.get_prioritized_cases.error",
            error=str(e),
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve prioritized cases: {str(e)}",
        )


@router.get(
    "/debug/database-stats",
    status_code=status.HTTP_200_OK,
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def get_database_stats(
    db: Session = Depends(get_db),
) -> dict:
    """
    Get database statistics for debugging.
    
    Returns:
        Dictionary with database statistics
    """
    try:
        total_calls = db.query(EmergencyCallModel).count()
        enriched_calls = db.query(EmergencyCallModel).filter(EmergencyCallModel.is_enriched == True).count() # noqa: E712
        unenriched_calls = db.query(EmergencyCallModel).filter(EmergencyCallModel.is_enriched == False).count() # noqa: E712
        total_risk_scores = db.query(RiskScoreModel).count()
        total_alerts = db.query(AlertModel).count()
        
        # Get calls with risk scores
        calls_with_scores = db.query(EmergencyCallModel).join(
            RiskScoreModel,
            EmergencyCallModel.call_id == RiskScoreModel.call_id
        ).count()
        
        # Get enriched calls with risk scores (what prioritized cases needs)
        enriched_with_scores = db.query(EmergencyCallModel).join(
            RiskScoreModel,
            EmergencyCallModel.call_id == RiskScoreModel.call_id
        ).filter(
            EmergencyCallModel.is_enriched == True # noqa: E712
        ).count()
        
        # Get sample call IDs
        sample_calls = db.query(EmergencyCallModel.call_id, EmergencyCallModel.is_enriched).limit(5).all()
        sample_risk_scores = db.query(RiskScoreModel.call_id, RiskScoreModel.risk_score, RiskScoreModel.priority).limit(5).all()
        
        return {
            "total_calls": total_calls,
            "enriched_calls": enriched_calls,
            "unenriched_calls": unenriched_calls,
            "total_risk_scores": total_risk_scores,
            "total_alerts": total_alerts,
            "calls_with_scores": calls_with_scores,
            "enriched_with_scores": enriched_with_scores,
            "sample_calls": [{"call_id": c.call_id, "is_enriched": c.is_enriched} for c in sample_calls],
            "sample_risk_scores": [{"call_id": r.call_id, "risk_score": r.risk_score, "priority": r.priority} for r in sample_risk_scores],
        }
    except Exception as e:
        logger.error(
            "sentinel_dispatch.api.routers.dashboard.get_database_stats.error",
            error=str(e),
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve database stats: {str(e)}",
        )


@router.get(
    "/alerts",
    response_model=List[Alert],
    status_code=status.HTTP_200_OK,
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def get_alerts(
    call_id: Optional[str] = Query(None, description="Filter by call ID"),
    severity: Optional[str] = Query(None, description="Filter by severity: low, medium, high, critical"),
    acknowledged: Optional[bool] = Query(None, description="Filter by acknowledged status"),
    limit: Optional[int] = Query(None, ge=1, le=1000, description="Maximum number of alerts to return"),
    db: Session = Depends(get_db),
) -> List[Alert]:
    """
    Get active alerts and notifications.
    
    Args:
        call_id: Optional call ID to filter by
        severity: Optional severity level to filter by
        acknowledged: Optional filter for acknowledged status
        limit: Maximum number of alerts to return (default: all)
        db: Database session
        
    Returns:
        List of alerts
    """
    logger.info(
        "sentinel_dispatch.api.routers.dashboard.get_alerts.requested",
        call_id=call_id,
        severity=severity,
        acknowledged=acknowledged,
        limit=limit,
    )
    
    try:
        query = db.query(AlertModel)
        
        # Apply filters
        if call_id:
            query = query.filter(AlertModel.call_id == call_id)
        if severity:
            query = query.filter(AlertModel.severity == severity)
        if acknowledged is not None:
            query = query.filter(AlertModel.acknowledged == acknowledged)
        
        # Log query details for debugging
        total_alerts = db.query(AlertModel).count()
        unacknowledged_alerts = db.query(AlertModel).filter(AlertModel.acknowledged == False).count() # noqa: E712
        
        logger.info(
            "sentinel_dispatch.api.routers.dashboard.get_alerts.query_stats",
            total_alerts=total_alerts,
            unacknowledged_alerts=unacknowledged_alerts,
            call_id=call_id,
            severity=severity,
            acknowledged=acknowledged,
        )
        
        # Sort by severity (critical first), then timestamp (most recent first)
        severity_order = {"critical": 4, "high": 3, "medium": 2, "low": 1}
        # Note: SQLite doesn't support custom sort orders easily, so we'll sort in Python
        alert_models = query.order_by(AlertModel.timestamp.desc()).all()
        
        # Convert to schemas and sort by severity
        alerts_list = [alert_model_to_schema(model) for model in alert_models]
        alerts_list.sort(
            key=lambda x: (
                severity_order.get(x.severity, 0),
                x.timestamp,
            ),
            reverse=True,
        )
        
        if limit is not None:
            alerts_list = alerts_list[:limit]
        
        return alerts_list
    except Exception as e:
        logger.error(
            "sentinel_dispatch.api.routers.dashboard.get_alerts.error",
            error=str(e),
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve alerts: {str(e)}",
        )


# Alert acknowledgment is handled via WebSocket (see websocket.py)
# This keeps all real-time updates consistent and reduces code duplication

