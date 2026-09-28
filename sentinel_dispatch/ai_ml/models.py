"""
Pydantic models for AI/ML module outputs.
"""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict


class CallClassificationResult(BaseModel):
    """Structured result from call classification."""
    
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    urgency: int = Field(
        ge=1, le=5,
        description="Urgency level from 1 (low) to 5 (critical)"
    )
    incident_type: str = Field(
        description="Type of incident: medical, fire, evacuation, other"
    )
    entities: Dict[str, Any] = Field(
        default_factory=dict,
        description="Extracted entities: locations, symptoms, hazards, people_mentioned"
    )
    sentiment: float = Field(
        ge=0.0, le=1.0,
        description="Sentiment/panic level from 0.0 (calm) to 1.0 (extreme panic)"
    )
    key_phrases: List[str] = Field(
        default_factory=list,
        description="Key phrases indicating urgency or critical information"
    )
    people_affected: Optional[int] = Field(
        default=None,
        description="Estimated number of people affected (counted from context if not explicitly stated)"
    )
    operator_questions: List[str] = Field(
        default_factory=list,
        description="Suggested questions for the operator to ask the caller (e.g., age, medical conditions)"
    )
    confidence: float = Field(
        ge=0.0, le=1.0,
        default=0.8,
        description="Confidence in the classification"
    )


class LocationEntity(BaseModel):
    """Location entity extracted from call."""
    
    name: str
    lat: Optional[float] = None
    lon: Optional[float] = None
    context: Optional[str] = None  # e.g., "near", "at", "between"


class SymptomEntity(BaseModel):
    """Medical symptom or condition mentioned."""
    
    symptom: str
    severity: Optional[str] = None  # e.g., "mild", "severe", "critical"


class HazardEntity(BaseModel):
    """Hazard or danger mentioned."""
    
    hazard: str
    proximity: Optional[str] = None  # e.g., "close", "approaching", "immediate"


class PeopleEntity(BaseModel):
    """Person or people mentioned in the call."""
    
    role: str  # e.g., "caller", "neighbor", "family member", "trapped person"
    status: Optional[str] = None  # e.g., "safe", "in danger", "trapped", "injured"
    location: Optional[str] = None  # Where they are mentioned to be


# Risk Scoring Models

class RiskFactors(BaseModel):
    """Individual risk factor scores (0.0-1.0)."""

    call_urgency: float = Field(
        ge=0.0, le=1.0,
        description="Normalized call urgency score (0.0=low, 1.0=critical)"
    )
    fire_proximity: float = Field(
        ge=0.0, le=1.0,
        description="Fire proximity risk (0.0=far, 1.0=immediate danger)"
    )
    fire_spread_rate: Optional[float] = Field(
        default=None,
        ge=0.0, le=1.0,
        description="Fire spread rate toward caller (0.0=not spreading, 1.0=rapid spread)"
    )
    weather_risk: Optional[float] = Field(
        default=None,
        ge=0.0, le=1.0,
        description="Weather conditions risk (wind, smoke, air quality)"
    )
    vulnerability: float = Field(
        ge=0.0, le=1.0,
        description="Vulnerability score based on age, medical conditions, mobility"
    )
    time_since_call: Optional[float] = Field(
        default=None,
        ge=0.0, le=1.0,
        description="Time urgency factor (0.0=just called, 1.0=long delay)"
    )


class RiskScoreInput(BaseModel):
    """Input data for risk scoring."""

    call_id: str
    call_timestamp: str  # ISO format datetime
    caller_location: Dict[str, float]  # {"lat": float, "lon": float}
    call_classification: CallClassificationResult
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


class RiskScoreResult(BaseModel):
    """Risk score calculation result."""

    call_id: str
    timestamp: str  # ISO format datetime
    risk_score: float = Field(
        ge=0.0, le=100.0,
        description="Overall risk score (0-100)"
    )
    confidence: float = Field(
        ge=0.0, le=1.0,
        description="Confidence in the risk score (0.0-1.0)"
    )
    factors: RiskFactors
    recommendation: str = Field(
        description="Dispatch recommendation: IMMEDIATE_DISPATCH, HIGH_PRIORITY, MODERATE_PRIORITY, LOW_PRIORITY, MONITOR"
    )
    priority: int = Field(
        ge=1, le=5,
        description="Priority level (5=critical/immediate, 1=low priority) - matches urgency scale"
    )


# Data Enrichment Models

class ProcessedCall(BaseModel):
    """Processed call with classification result."""

    call_id: str
    timestamp: str  # ISO format datetime
    caller_location: Dict[str, float]  # {"lat": float, "lon": float}
    transcript: str
    classification: CallClassificationResult
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Call metadata: phoneNumber, callDuration, callerAge, knownConditions, etc."
    )


class FireProximityAnalysis(BaseModel):
    """Analysis of fire proximity to caller location."""

    nearest_fire_id: Optional[str] = Field(
        default=None,
        description="ID of the nearest fire"
    )
    distance_km: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Distance to nearest fire in kilometers"
    )
    bearing_degrees: Optional[float] = Field(
        default=None,
        ge=0.0, le=360.0,
        description="Bearing from caller to fire in degrees (0=North, 90=East)"
    )
    spread_rate: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Fire spread rate in km/h"
    )
    spread_toward_caller: Optional[bool] = Field(
        default=None,
        description="Whether fire is spreading toward caller location"
    )
    estimated_time_to_reach: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Estimated time for fire to reach caller location in hours (if spreading toward caller)"
    )
    fire_location: Optional[Dict[str, float]] = Field(
        default=None,
        description="Location of nearest fire: {'lat': float, 'lon': float}"
    )
    frp: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Fire Radiative Power (FRP) in MW from FIRMS API"
    )


class WeatherEnrichment(BaseModel):
    """Weather data enrichment for a call."""

    temperature: Optional[float] = Field(
        default=None,
        description="Temperature in Fahrenheit or Celsius"
    )
    wind_speed: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Wind speed (mph or m/s)"
    )
    wind_direction: Optional[float] = Field(
        default=None,
        ge=0.0, le=360.0,
        description="Wind direction in degrees (0=North, 90=East)"
    )
    humidity: Optional[float] = Field(
        default=None,
        ge=0.0, le=100.0,
        description="Humidity percentage"
    )
    air_quality_index: Optional[int] = Field(
        default=None,
        ge=0,
        description="Air Quality Index (0-500+)"
    )
    visibility: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Visibility in miles or km"
    )
    weather_location: Optional[Dict[str, float]] = Field(
        default=None,
        description="Location of weather station: {'lat': float, 'lon': float}"
    )
    weather_distance_km: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Distance to weather station in kilometers"
    )
    wind_toward_fire: Optional[bool] = Field(
        default=None,
        description="Whether wind is blowing toward nearest fire (if fire exists)"
    )


class EnrichedCall(BaseModel):
    """Enriched call with weather and fire data."""

    call_id: str
    timestamp: str  # ISO format datetime
    caller_location: Dict[str, float]
    transcript: str
    classification: CallClassificationResult
    metadata: Optional[Dict[str, Any]] = None
    fire_analysis: Optional[FireProximityAnalysis] = Field(
        default=None,
        description="Fire proximity analysis"
    )
    weather_enrichment: Optional[WeatherEnrichment] = Field(
        default=None,
        description="Weather data enrichment"
    )
    enrichment_timestamp: str = Field(
        description="Timestamp when enrichment was performed (ISO format)"
    )

