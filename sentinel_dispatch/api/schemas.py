"""
Pydantic schemas for API request/response models.
"""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from sentinel_dispatch.ai_ml.models import (
    CallClassificationResult,
    ProcessedCall,
    EnrichedCall,
    RiskScoreResult,
)


# Call Classification Schemas
class ClassifyCallRequest(BaseModel):
    """Request schema for call classification."""

    transcript: str = Field(..., min_length=1, description="Emergency call transcript text (non-empty)")
    call_id: Optional[str] = Field(None, description="Optional call identifier")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Optional call metadata")


class ClassifyCallResponse(BaseModel):
    """Response schema for call classification."""

    call_id: str = Field(..., description="Call identifier")
    classification: CallClassificationResult = Field(..., description="Classification result")
    timestamp: str = Field(..., description="Classification timestamp (ISO format)")


# Data Enrichment Schemas
class EnrichCallRequest(BaseModel):
    """Request schema for call enrichment."""

    processed_call: ProcessedCall = Field(..., description="Processed call with classification")
    weather_data_list: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="List of weather data records for spatial-temporal join"
    )
    fire_data_list: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="List of fire data records for spatial-temporal join"
    )


class EnrichCallResponse(BaseModel):
    """Response schema for call enrichment."""

    enriched_call: EnrichedCall = Field(..., description="Enriched call with weather and fire data")
    timestamp: str = Field(..., description="Enrichment timestamp (ISO format)")


# Risk Scoring Schemas
class ScoreRiskRequest(BaseModel):
    """Request schema for risk scoring."""

    call_id: str = Field(..., description="Call identifier")
    call_timestamp: str = Field(..., description="Call timestamp (ISO format)")
    caller_location: Dict[str, float] = Field(..., description="Caller location: {'lat': float, 'lon': float}")
    call_classification: CallClassificationResult = Field(..., description="Call classification result")
    nearest_fire: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Nearest fire data: location, distance_km, spread_rate, etc."
    )
    weather_data: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Weather data: wind_speed, wind_direction, air_quality_index, etc."
    )
    vulnerability_factors: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Vulnerability: age, medical_conditions, mobility, etc."
    )
    current_timestamp: Optional[str] = Field(
        default=None,
        description="Current timestamp for time_since_call calculation (ISO format)"
    )
    phase: Optional[str] = Field(
        default=None,
        description="Emergency phase: 'early', 'developing', 'critical', 'peak', 'aftermath'"
    )
    scenario_type: Optional[str] = Field(
        default=None,
        description="Scenario type: 'elderly_resident', 'child_separation', 'medical_emergency', etc."
    )


class ScoreRiskResponse(BaseModel):
    """Response schema for risk scoring."""

    risk_score: RiskScoreResult = Field(..., description="Risk score result")
    timestamp: str = Field(..., description="Scoring timestamp (ISO format)")


# Alert Generation Schemas
class GenerateAlertRequest(BaseModel):
    """Request schema for alert generation."""

    risk_score: RiskScoreResult = Field(..., description="Risk score result")
    enriched_call: Optional[EnrichedCall] = Field(
        default=None,
        description="Enriched call data (optional, for additional context)"
    )


class GenerateAlertResponse(BaseModel):
    """Response schema for alert generation."""

    alerts: List[Dict[str, Any]] = Field(..., description="List of generated alerts")
    alert_count: int = Field(..., description="Number of alerts generated")
    timestamp: str = Field(..., description="Alert generation timestamp (ISO format)")


# Error Response Schema
class ErrorResponse(BaseModel):
    """Error response schema."""

    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")
    timestamp: str = Field(..., description="Error timestamp (ISO format)")


# Dashboard API Schemas
class CallerLocation(BaseModel):
    """Caller location schema."""

    lat: float = Field(..., description="Latitude")
    lon: float = Field(..., description="Longitude")


class CallMetadata(BaseModel):
    """Call metadata schema."""

    phoneNumber: Optional[str] = Field(None, description="Phone number")
    callDuration: Optional[int] = Field(None, description="Call duration in seconds")
    callerAge: Optional[int] = Field(None, description="Caller age")
    knownConditions: Optional[List[str]] = Field(None, description="Known medical conditions")


class EmergencyCall(BaseModel):
    """Emergency call schema for dashboard."""

    callId: str = Field(..., description="Call identifier")
    timestamp: str = Field(..., description="Call timestamp (ISO format)")
    callerLocation: CallerLocation = Field(..., description="Caller location")
    transcript: str = Field(..., description="Call transcript")
    metadata: Optional[CallMetadata] = Field(None, description="Call metadata")


class CallsResponse(BaseModel):
    """Response schema for calls list."""

    calls: List[EmergencyCall] = Field(..., description="List of emergency calls")
    total: int = Field(..., description="Total number of calls")


class DashboardRiskScore(BaseModel):
    """Risk score schema for dashboard."""

    callId: str = Field(..., description="Call identifier")
    timestamp: str = Field(..., description="Risk score timestamp (ISO format)")
    riskScore: float = Field(..., ge=0.0, le=100.0, description="Overall risk score (0-100)")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence in the risk score")
    factors: Dict[str, Any] = Field(..., description="Risk factors breakdown")
    recommendation: Optional[str] = Field(None, description="Dispatch recommendation")
    priority: Optional[int] = Field(None, ge=1, le=5, description="Priority level (5=critical, 1=low)")


class Alert(BaseModel):
    """Alert schema for dashboard."""

    model_config = {"extra": "allow"}  # Allow extra fields from Kafka

    alertId: str = Field(..., description="Alert identifier")
    timestamp: str = Field(..., description="Alert timestamp (ISO format)")
    severity: str = Field(..., description="Alert severity: low, medium, high, critical")
    message: str = Field(..., description="Alert message")
    alert_type: Optional[str] = Field(None, description="Alert type")
    callId: Optional[str] = Field(None, description="Associated call ID")
    acknowledged: bool = Field(False, description="Whether alert has been acknowledged")


class PrioritizedCase(BaseModel):
    """Prioritized case schema for triage decision-making."""

    callId: str = Field(..., description="Call identifier")
    emergencyCall: EmergencyCall = Field(..., description="Emergency call data")
    riskScore: DashboardRiskScore = Field(..., description="Risk score data")
    alerts: Optional[List[Alert]] = Field(None, description="Associated alerts")
    hasFireWeatherWarning: Optional[bool] = Field(False, description="Has fire weather warning")
    priority: int = Field(..., ge=1, le=5, description="Priority level (5=critical, 1=low)")


class PrioritizedCasesResponse(BaseModel):
    """Response schema for prioritized cases."""

    cases: List[PrioritizedCase] = Field(..., description="List of prioritized cases")
    total: int = Field(..., description="Total number of cases")
