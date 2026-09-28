"""
Risk scoring utility functions.

This module contains shared utility functions for risk scoring that are used
by both rule-based and Vertex AI scorers to avoid circular imports.
"""

from datetime import datetime, timezone
from typing import Optional

from sentinel_dispatch.ai_ml.models import RiskFactors, RiskScoreResult


def determine_recommendation(risk_score: float) -> str:
    """
    Determine dispatch recommendation based on risk score.
    
    Args:
        risk_score: Overall risk score (0-100)
        
    Returns:
        Recommendation string
    """
    if risk_score >= 80:
        return "IMMEDIATE_DISPATCH"
    elif risk_score >= 60:
        return "HIGH_PRIORITY"
    elif risk_score >= 40:
        return "MODERATE_PRIORITY"
    elif risk_score >= 20:
        return "LOW_PRIORITY"
    else:
        return "MONITOR"


def determine_priority(risk_score: float) -> int:
    """
    Determine priority level (1-5) based on risk score.

    Priority scale matches urgency scale:
    - Priority 5: Critical/Immediate (highest urgency, life-threatening)
    - Priority 4: High priority (fire approaching, evacuation needed)
    - Priority 3: Moderate priority (fire nearby, moderate urgency)
    - Priority 2: Low-moderate priority (monitoring, precautionary)
    - Priority 1: Low priority (informational, resolved, lowest urgency)
    
    Args:
        risk_score: Overall risk score (0-100)
        
    Returns:
        Priority level (5=critical/immediate, 1=low priority)
    """
    if risk_score >= 80:
        return 5  # Critical/Immediate - matches urgency 5
    elif risk_score >= 60:
        return 4  # High priority - matches urgency 4
    elif risk_score >= 40:
        return 3  # Moderate priority - matches urgency 3
    elif risk_score >= 20:
        return 2  # Low-moderate priority - matches urgency 2
    else:
        return 1  # Low priority - matches urgency 1


def create_risk_score_result(
    call_id: str,
    risk_score: float,
    confidence: float,
    factors: RiskFactors,
    current_timestamp: Optional[str] = None,
) -> RiskScoreResult:
    """
    Create a RiskScoreResult with recommendation and priority determined from risk score.
    
    This function extracts the common logic for creating RiskScoreResult objects
    used by both rule-based and Vertex AI scorers.
    
    Args:
        call_id: Call identifier
        risk_score: Overall risk score (0-100)
        confidence: Confidence in the score (0.0-1.0)
        factors: Risk factors object
        current_timestamp: Optional timestamp (defaults to current UTC time)
        
    Returns:
        RiskScoreResult with recommendation and priority determined
    """
    # Determine recommendation and priority
    recommendation = determine_recommendation(risk_score)
    priority = determine_priority(risk_score)
    
    # Use current timestamp if not provided
    timestamp = current_timestamp or datetime.now(timezone.utc).isoformat()
    
    return RiskScoreResult(
        call_id=call_id,
        timestamp=timestamp,
        risk_score=round(risk_score, 2),
        confidence=round(confidence, 3),
        factors=factors,
        recommendation=recommendation,
        priority=priority,
    )

