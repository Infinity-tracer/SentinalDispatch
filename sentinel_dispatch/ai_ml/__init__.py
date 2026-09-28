"""
AI/ML modules for Sentinel Dispatch.

This package contains reusable AI/ML logic modules that will be integrated into
Streaming Agents (for production).
"""

from sentinel_dispatch.ai_ml.alert_generator import (
    Alert,
    AlertGenerator,
    AlertSeverity,
    AlertType,
    generate_alerts,
)
from sentinel_dispatch.ai_ml.call_classifier import CallClassifier, classify_call
from sentinel_dispatch.ai_ml.models import (
    CallClassificationResult,
    LocationEntity,
    SymptomEntity,
    HazardEntity,
    PeopleEntity,
    RiskFactors,
    RiskScoreInput,
    RiskScoreResult,
    ProcessedCall,
    EnrichedCall,
    FireProximityAnalysis,
    WeatherEnrichment,
)
from sentinel_dispatch.ai_ml.risk_scorer import RiskScorer, calculate_risk_score
from sentinel_dispatch.ai_ml.risk_utils import (
    create_risk_score_result,
    determine_recommendation,
    determine_priority,
)
from sentinel_dispatch.ai_ml.data_enricher import DataEnricher, enrich_call
from sentinel_dispatch.ai_ml.vertex_ai_scorer import VertexAIRiskScorer

__all__ = [
    "Alert",
    "AlertGenerator",
    "AlertSeverity",
    "AlertType",
    "generate_alerts",
    "CallClassifier",
    "classify_call",
    "CallClassificationResult",
    "LocationEntity",
    "SymptomEntity",
    "HazardEntity",
    "PeopleEntity",
    "RiskScorer",
    "calculate_risk_score",
    "VertexAIRiskScorer",
    "DataEnricher",
    "enrich_call",
    "create_risk_score_result",
    "determine_recommendation",
    "determine_priority",
    "RiskFactors",
    "RiskScoreInput",
    "RiskScoreResult",
    "ProcessedCall",
    "EnrichedCall",
    "FireProximityAnalysis",
    "WeatherEnrichment",
]
