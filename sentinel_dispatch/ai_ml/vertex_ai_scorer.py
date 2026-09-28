"""
Vertex AI Risk Scorer Module.

This module provides risk scoring using Vertex AI ML models. It can be used
as a primary scorer or as part of a hybrid approach with rule-based scoring.
"""

import os
from typing import Optional, Dict, Any
from datetime import datetime, timezone

from google.cloud import aiplatform

from sentinel_dispatch.ai_ml.models import (
    RiskFactors,
    RiskScoreInput,
    RiskScoreResult,
)
from sentinel_dispatch.ai_ml.risk_utils import create_risk_score_result
from sentinel_dispatch.utils.logging_config import get_logger

logger = get_logger(__name__)


class VertexAIRiskScorer:
    """Risk scorer using Vertex AI ML models."""
    
    def __init__(
        self,
        project_id: Optional[str] = None,
        location: str = "us-central1",
        endpoint_id: Optional[str] = None,
        model_name: Optional[str] = None,
    ):
        """
        Initialize Vertex AI risk scorer.
        
        Args:
            project_id: GCP project ID (defaults to GOOGLE_CLOUD_PROJECT env var)
            location: GCP region (default: us-central1)
            endpoint_id: Vertex AI endpoint ID (defaults to VERTEX_AI_ENDPOINT_ID env var)
            model_name: Model name/version (defaults to VERTEX_AI_MODEL_NAME env var)
        """
        self.project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
        self.location = location
        self.endpoint_id = endpoint_id or os.getenv("VERTEX_AI_ENDPOINT_ID")
        self.model_name = model_name or os.getenv("VERTEX_AI_MODEL_NAME")
        
        if not self.project_id:
            raise ValueError(
                "GCP project ID is required. Set GOOGLE_CLOUD_PROJECT environment variable "
                "or pass project_id parameter."
            )
        
        if not self.endpoint_id and not self.model_name:
            raise ValueError(
                "Either endpoint_id or model_name is required. Set VERTEX_AI_ENDPOINT_ID "
                "or VERTEX_AI_MODEL_NAME environment variable, or pass as parameter."
            )
        
        # Initialize Vertex AI
        aiplatform.init(project=self.project_id, location=self.location)
        
        # Initialize endpoint reference
        self.endpoint = None
        if self.endpoint_id:
            try:
                # Get endpoint by resource name
                endpoint_resource_name = (
                    f"projects/{self.project_id}/locations/{self.location}/endpoints/{self.endpoint_id}"
                )
                self.endpoint = aiplatform.Endpoint(endpoint_resource_name)
            except Exception as e:
                logger.warning(
                    "sentinel_dispatch.ai_ml.vertex_ai_scorer.VertexAIRiskScorer.__init__.endpoint_init_warning",
                    error=str(e),
                    endpoint_id=self.endpoint_id
                )
        elif self.model_name:
            try:
                # Get model and use its deployed endpoint
                model_resource_name = (
                    f"projects/{self.project_id}/locations/{self.location}/models/{self.model_name}"
                )
                model = aiplatform.Model(model_resource_name)
                # Get the first deployed endpoint for this model
                # Note: This assumes the model is already deployed
                # In production, you might want to deploy it first or use a specific endpoint
                endpoints = model.list_endpoints()
                if endpoints:
                    self.endpoint = endpoints[0]
                else:
                    raise ValueError(
                        f"Model {self.model_name} is not deployed to any endpoint. "
                        "Deploy the model first or use endpoint_id instead."
                    )
            except Exception as e:
                logger.warning(
                    "sentinel_dispatch.ai_ml.vertex_ai_scorer.VertexAIRiskScorer.__init__.model_init_warning",
                    error=str(e),
                    model_name=self.model_name
                )
        
        logger.info(
            "sentinel_dispatch.ai_ml.vertex_ai_scorer.VertexAIRiskScorer.__init__.initialized",
            project_id=self.project_id,
            location=self.location,
            endpoint_id=self.endpoint_id,
            model_name=self.model_name,
            endpoint_available=self.endpoint is not None
        )
    
    def _prepare_features(self, input_data: RiskScoreInput) -> Dict[str, Any]:
        """
        Prepare feature vector for Vertex AI model.
        
        Args:
            input_data: Risk score input data
            
        Returns:
            Feature dictionary for model inference
        """
        # Extract features from input data
        features = {
            # Call classification features
            "urgency": float(input_data.call_classification.urgency),
            "sentiment": input_data.call_classification.sentiment,
            "confidence": input_data.call_classification.confidence,
            "people_affected": float(input_data.call_classification.people_affected or 0),
            
            # Fire features
            "fire_distance_km": 0.0,
            "fire_spread_rate": 0.0,
            "has_fire_data": 0.0,
            
            # Weather features
            "wind_speed": 0.0,
            "air_quality_index": 0.0,
            "humidity": 0.0,
            "has_weather_data": 0.0,
            
            # Vulnerability features
            "age": 0.0,
            "medical_conditions_count": 0.0,
            "mobility_limited": 0.0,
            "has_vulnerability_data": 0.0,
            
            # Time features
            "time_since_call_minutes": 0.0,
        }
        
        # Fire data
        if input_data.nearest_fire:
            features["has_fire_data"] = 1.0
            features["fire_distance_km"] = input_data.nearest_fire.get("distance_km", 0.0)
            # Use FRP (Fire Radiative Power) from FIRMS API, fallback to spread_rate
            frp = input_data.nearest_fire.get("frp") or input_data.nearest_fire.get("FRP", 0.0)
            spread_rate = input_data.nearest_fire.get("spread_rate") or input_data.nearest_fire.get("spreadRate", 0.0)
            # Use FRP if available, otherwise use spread_rate
            # For ML model, we can use FRP directly (in MW) or convert to spread rate estimate
            features["fire_spread_rate"] = frp if frp > 0 else spread_rate
            features["fire_frp"] = frp  # Include FRP as separate feature
        
        # Weather data
        if input_data.weather_data:
            features["has_weather_data"] = 1.0
            features["wind_speed"] = input_data.weather_data.get("wind_speed", 0.0)
            features["air_quality_index"] = float(input_data.weather_data.get("air_quality_index", 0))
            features["humidity"] = input_data.weather_data.get("humidity", 0.0)
        
        # Vulnerability data
        if input_data.vulnerability_factors:
            features["has_vulnerability_data"] = 1.0
            features["age"] = float(input_data.vulnerability_factors.get("age", 0))
            conditions = input_data.vulnerability_factors.get("medical_conditions", [])
            features["medical_conditions_count"] = float(len(conditions))
            mobility = input_data.vulnerability_factors.get("mobility", "normal")
            features["mobility_limited"] = 1.0 if mobility in ["limited", "wheelchair", "immobile"] else 0.0
        
        # Time since call
        try:
            call_time = datetime.fromisoformat(
                input_data.call_timestamp.replace("Z", "+00:00")
            )
            current_time = datetime.now(timezone.utc)
            if input_data.current_timestamp:
                current_time = datetime.fromisoformat(
                    input_data.current_timestamp.replace("Z", "+00:00")
                )
            time_diff = (current_time - call_time).total_seconds() / 60.0
            features["time_since_call_minutes"] = time_diff
        except Exception:
            features["time_since_call_minutes"] = 0.0
        
        return features
    
    def _call_vertex_ai_endpoint(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """
        Call Vertex AI endpoint for prediction.
        
        Args:
            features: Feature dictionary
            
        Returns:
            Prediction result with risk_score and confidence
        """
        if not self.endpoint:
            raise ValueError("Vertex AI endpoint not initialized")
        
        # Prepare instance for prediction
        # Convert features dict to list format expected by Vertex AI
        instance = [features]
        
        # Call endpoint.predict() method
        response = self.endpoint.predict(instances=instance)
        
        # Parse response
        # response.predictions is a list of prediction results
        predictions = response.predictions
        if not predictions or len(predictions) == 0:
            raise ValueError("Empty prediction response from Vertex AI")
        
        prediction_result = predictions[0]
        
        # Extract risk score and confidence
        # Model output format may vary, adjust based on your model
        if isinstance(prediction_result, dict):
            risk_score = prediction_result.get("risk_score", prediction_result.get("score", 0.0))
            confidence = prediction_result.get("confidence", 0.8)
        elif isinstance(prediction_result, (list, tuple)):
            risk_score = float(prediction_result[0]) if len(prediction_result) > 0 else 0.0
            confidence = float(prediction_result[1]) if len(prediction_result) > 1 else 0.8
        else:
            risk_score = float(prediction_result)
            confidence = 0.8
        
        return {
            "risk_score": min(100.0, max(0.0, risk_score * 100.0 if risk_score <= 1.0 else risk_score)),
            "confidence": min(1.0, max(0.0, confidence)),
        }
    
    def calculate_risk_score(self, input_data: RiskScoreInput) -> RiskScoreResult:
        """
        Calculate risk score using Vertex AI model.
        
        Args:
            input_data: Risk score input data
            
        Returns:
            Risk score result
        """
        logger.debug(
            "sentinel_dispatch.ai_ml.vertex_ai_scorer.VertexAIRiskScorer.calculate_risk_score.starting",
            call_id=input_data.call_id
        )
        
        try:
            # Prepare features
            features = self._prepare_features(input_data)
            
            # Call Vertex AI
            prediction_result = self._call_vertex_ai_endpoint(features)
            
            risk_score = prediction_result["risk_score"]
            confidence = prediction_result["confidence"]
            
            # Create factors (for compatibility, we'll estimate from features)
            # In a full implementation, the model might return factor contributions
            factors = RiskFactors(
                call_urgency=features["urgency"] / 5.0,  # Normalize 1-5 to 0-1
                fire_proximity=1.0 - min(1.0, features["fire_distance_km"] / 20.0),
                fire_spread_rate=min(1.0, features["fire_spread_rate"] / 10.0) if features["fire_spread_rate"] > 0 else None,
                weather_risk=self._estimate_weather_risk(features) if features["has_weather_data"] > 0 else None,
                vulnerability=self._estimate_vulnerability(features),
                time_since_call=min(1.0, features["time_since_call_minutes"] / 60.0) if features["time_since_call_minutes"] > 0 else None,
            )
            
            # Create result with recommendation and priority
            result = create_risk_score_result(
                call_id=input_data.call_id,
                risk_score=risk_score,
                confidence=confidence,
                factors=factors,
                current_timestamp=input_data.current_timestamp,
            )
            
            logger.info(
                "sentinel_dispatch.ai_ml.vertex_ai_scorer.VertexAIRiskScorer.calculate_risk_score.completed",
                call_id=input_data.call_id,
                risk_score=result.risk_score,
                recommendation=result.recommendation,
                priority=result.priority
            )
            
            return result
            
        except Exception as e:
            logger.error(
                "sentinel_dispatch.ai_ml.vertex_ai_scorer.VertexAIRiskScorer.calculate_risk_score.error",
                error=str(e),
                error_type=type(e).__name__,
                call_id=input_data.call_id
            )
            raise
    
    @staticmethod
    def _estimate_weather_risk(features: Dict[str, Any]) -> float:
        """Estimate weather risk from features (for factor display)."""
        wind_risk = min(1.0, features["wind_speed"] / 40.0)
        aqi_risk = min(1.0, features["air_quality_index"] / 200.0)
        humidity_risk = 1.0 - (features["humidity"] / 100.0) if features["humidity"] < 30 else 0.0
        return (wind_risk + aqi_risk + humidity_risk) / 3.0
    
    @staticmethod
    def _estimate_vulnerability(features: Dict[str, Any]) -> float:
        """Estimate vulnerability from features (for factor display)."""
        age_risk = min(1.0, features["age"] / 100.0) if features["age"] > 65 else features["age"] / 65.0
        conditions_risk = min(1.0, features["medical_conditions_count"] / 3.0)
        mobility_risk = features["mobility_limited"]
        return min(1.0, (age_risk * 0.4 + conditions_risk * 0.3 + mobility_risk * 0.3))
    
    def is_available(self) -> bool:
        """
        Check if Vertex AI scorer is available and configured.
        
        Returns:
            True if Vertex AI is available and endpoint is initialized
        """
        return (
            self.project_id is not None
            and (self.endpoint_id is not None or self.model_name is not None)
        )

