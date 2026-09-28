"""
Tests for Vertex AI risk scoring module.
"""

import os
import pytest
from unittest.mock import Mock, patch

from sentinel_dispatch.ai_ml.models import (
    CallClassificationResult,
    RiskScoreInput,
    RiskFactors,
)


class TestVertexAIRiskScorer:
    """Test cases for VertexAIRiskScorer."""
    
    @patch('sentinel_dispatch.ai_ml.vertex_ai_scorer.aiplatform')
    def test_scorer_initialization_with_endpoint_id(self, mock_aiplatform):
        """Test Vertex AI scorer initialization with endpoint_id."""
        from sentinel_dispatch.ai_ml.vertex_ai_scorer import VertexAIRiskScorer
        
        # Mock aiplatform
        mock_aiplatform.init.return_value = None
        mock_endpoint = Mock()
        mock_aiplatform.Endpoint.return_value = mock_endpoint
        
        scorer = VertexAIRiskScorer(
            project_id="test-project",
            endpoint_id="test-endpoint-id"
        )
        
        assert scorer.project_id == "test-project"
        assert scorer.endpoint_id == "test-endpoint-id"
        assert scorer.endpoint is not None
        mock_aiplatform.init.assert_called_once_with(
            project="test-project",
            location="us-central1"
        )
    
    @patch('sentinel_dispatch.ai_ml.vertex_ai_scorer.aiplatform')
    def test_scorer_initialization_with_model_name(self, mock_aiplatform):
        """Test Vertex AI scorer initialization with model_name."""
        from sentinel_dispatch.ai_ml.vertex_ai_scorer import VertexAIRiskScorer
        
        # Mock aiplatform
        mock_aiplatform.init.return_value = None
        mock_model = Mock()
        mock_endpoint = Mock()
        mock_model.list_endpoints.return_value = [mock_endpoint]
        mock_aiplatform.Model.return_value = mock_model
        
        scorer = VertexAIRiskScorer(
            project_id="test-project",
            model_name="test-model-name"
        )
        
        assert scorer.project_id == "test-project"
        assert scorer.model_name == "test-model-name"
        assert scorer.endpoint is not None
    
    @patch('sentinel_dispatch.ai_ml.vertex_ai_scorer.aiplatform')
    def test_scorer_initialization_missing_project_id(self, mock_aiplatform):
        """Test Vertex AI scorer initialization fails without project_id."""
        from sentinel_dispatch.ai_ml.vertex_ai_scorer import VertexAIRiskScorer
        
        with pytest.raises(ValueError, match="GCP project ID is required"):
            VertexAIRiskScorer()
    
    @patch('sentinel_dispatch.ai_ml.vertex_ai_scorer.aiplatform')
    def test_scorer_initialization_missing_endpoint_and_model(self, mock_aiplatform):
        """Test Vertex AI scorer initialization fails without endpoint_id or model_name."""
        from sentinel_dispatch.ai_ml.vertex_ai_scorer import VertexAIRiskScorer
        
        with patch.dict(os.environ, {"GOOGLE_CLOUD_PROJECT": "test-project"}, clear=False):
            with pytest.raises(ValueError, match="Either endpoint_id or model_name is required"):
                VertexAIRiskScorer()
    
    @patch('sentinel_dispatch.ai_ml.vertex_ai_scorer.aiplatform')
    def test_prepare_features(self, mock_aiplatform):
        """Test feature preparation."""
        from sentinel_dispatch.ai_ml.vertex_ai_scorer import VertexAIRiskScorer
        
        mock_aiplatform.init.return_value = None
        mock_endpoint = Mock()
        mock_aiplatform.Endpoint.return_value = mock_endpoint
        
        scorer = VertexAIRiskScorer(
            project_id="test-project",
            endpoint_id="test-endpoint-id"
        )
        
        classification = CallClassificationResult(
            urgency=5,
            incident_type="fire",
            entities={},
            sentiment=0.9,
            key_phrases=[],
            people_affected=2,
            confidence=0.95
        )
        
        input_data = RiskScoreInput(
            call_id="CALL-001",
            call_timestamp="2024-08-08T14:23:45Z",
            caller_location={"lat": 20.8783, "lon": -156.6825},
            call_classification=classification,
            nearest_fire={
                "location": {"lat": 20.8790, "lon": -156.6830},
                "distance_km": 0.5,
                "frp": 80.0,  # Fire Radiative Power in MW (primary)
                "spread_rate": 12.0  # Fallback
            },
            weather_data={
                "wind_speed": 40.0,
                "air_quality_index": 200,
                "humidity": 20.0
            },
            vulnerability_factors={
                "age": 75,
                "medical_conditions": ["asthma"],
                "mobility": "limited"
            }
        )
        
        features = scorer._prepare_features(input_data)
        
        assert features["urgency"] == 5.0
        assert features["sentiment"] == 0.9
        assert features["has_fire_data"] == 1.0
        assert features["fire_distance_km"] == 0.5
        # Should use frp (80.0) if available, otherwise spread_rate (12.0)
        assert features["fire_spread_rate"] == 80.0  # Uses frp when available
        assert features["fire_frp"] == 80.0
        assert features["has_weather_data"] == 1.0
        assert features["wind_speed"] == 40.0
        assert features["air_quality_index"] == 200.0
        assert features["has_vulnerability_data"] == 1.0
        assert features["age"] == 75.0
        assert features["medical_conditions_count"] == 1.0
        assert features["mobility_limited"] == 1.0
    
    @patch('sentinel_dispatch.ai_ml.vertex_ai_scorer.aiplatform')
    def test_prepare_features_missing_data(self, mock_aiplatform):
        """Test feature preparation with missing optional data."""
        from sentinel_dispatch.ai_ml.vertex_ai_scorer import VertexAIRiskScorer
        
        mock_aiplatform.init.return_value = None
        mock_endpoint = Mock()
        mock_aiplatform.Endpoint.return_value = mock_endpoint
        
        scorer = VertexAIRiskScorer(
            project_id="test-project",
            endpoint_id="test-endpoint-id"
        )
        
        classification = CallClassificationResult(
            urgency=3,
            incident_type="medical",
            entities={},
            sentiment=0.5,
            key_phrases=[],
            confidence=0.8
        )
        
        input_data = RiskScoreInput(
            call_id="CALL-002",
            call_timestamp="2024-08-08T14:23:45Z",
            caller_location={"lat": 20.8783, "lon": -156.6825},
            call_classification=classification
        )
        
        features = scorer._prepare_features(input_data)
        
        assert features["has_fire_data"] == 0.0
        assert features["has_weather_data"] == 0.0
        assert features["has_vulnerability_data"] == 0.0
        assert features["fire_distance_km"] == 0.0
        assert features["wind_speed"] == 0.0
    
    @patch('sentinel_dispatch.ai_ml.vertex_ai_scorer.aiplatform')
    def test_call_vertex_ai_endpoint(self, mock_aiplatform):
        """Test calling Vertex AI endpoint."""
        from sentinel_dispatch.ai_ml.vertex_ai_scorer import VertexAIRiskScorer
        
        mock_aiplatform.init.return_value = None
        mock_endpoint = Mock()
        mock_response = Mock()
        mock_response.predictions = [{"risk_score": 0.85, "confidence": 0.92}]
        mock_endpoint.predict.return_value = mock_response
        mock_aiplatform.Endpoint.return_value = mock_endpoint
        
        scorer = VertexAIRiskScorer(
            project_id="test-project",
            endpoint_id="test-endpoint-id"
        )
        
        features = {
            "urgency": 5.0,
            "sentiment": 0.9,
            "fire_distance_km": 0.5,
            "wind_speed": 40.0,
        }
        
        result = scorer._call_vertex_ai_endpoint(features)
        
        assert result["risk_score"] == 85.0  # 0.85 * 100
        assert result["confidence"] == 0.92
        mock_endpoint.predict.assert_called_once_with(instances=[features])
    
    @patch('sentinel_dispatch.ai_ml.vertex_ai_scorer.aiplatform')
    def test_call_vertex_ai_endpoint_list_response(self, mock_aiplatform):
        """Test calling Vertex AI endpoint with list response."""
        from sentinel_dispatch.ai_ml.vertex_ai_scorer import VertexAIRiskScorer
        
        mock_aiplatform.init.return_value = None
        mock_endpoint = Mock()
        mock_response = Mock()
        mock_response.predictions = [[0.87, 0.95]]  # List format
        mock_endpoint.predict.return_value = mock_response
        mock_aiplatform.Endpoint.return_value = mock_endpoint
        
        scorer = VertexAIRiskScorer(
            project_id="test-project",
            endpoint_id="test-endpoint-id"
        )
        
        features = {"urgency": 5.0}
        result = scorer._call_vertex_ai_endpoint(features)
        
        assert result["risk_score"] == 87.0
        assert result["confidence"] == 0.95
    
    @patch('sentinel_dispatch.ai_ml.vertex_ai_scorer.aiplatform')
    def test_calculate_risk_score(self, mock_aiplatform):
        """Test risk score calculation."""
        from sentinel_dispatch.ai_ml.vertex_ai_scorer import VertexAIRiskScorer
        
        mock_aiplatform.init.return_value = None
        mock_endpoint = Mock()
        mock_response = Mock()
        mock_response.predictions = [{"risk_score": 0.88, "confidence": 0.94}]
        mock_endpoint.predict.return_value = mock_response
        mock_aiplatform.Endpoint.return_value = mock_endpoint
        
        scorer = VertexAIRiskScorer(
            project_id="test-project",
            endpoint_id="test-endpoint-id"
        )
        
        classification = CallClassificationResult(
            urgency=5,
            incident_type="fire",
            entities={},
            sentiment=0.9,
            key_phrases=[],
            confidence=0.95
        )
        
        input_data = RiskScoreInput(
            call_id="CALL-003",
            call_timestamp="2024-08-08T14:23:45Z",
            caller_location={"lat": 20.8783, "lon": -156.6825},
            call_classification=classification,
            nearest_fire={
                "distance_km": 0.5,
                "frp": 80.0,  # Fire Radiative Power in MW
                "spread_rate": 12.0  # Fallback
            },
            weather_data={
                "wind_speed": 40.0,
                "air_quality_index": 200,
                "humidity": 20.0
            },
            vulnerability_factors={
                "age": 75,
                "medical_conditions": ["asthma"],
                "mobility": "limited"
            }
        )
        
        result = scorer.calculate_risk_score(input_data)
        
        assert result.call_id == "CALL-003"
        assert result.risk_score == 88.0
        assert result.confidence == 0.94
        assert result.recommendation == "IMMEDIATE_DISPATCH"
        assert result.priority == 5  # High risk (88.0) -> priority 5 (critical/immediate), not 1
        assert isinstance(result.factors, RiskFactors)
    
    @patch('sentinel_dispatch.ai_ml.vertex_ai_scorer.aiplatform')
    def test_calculate_risk_score_endpoint_error(self, mock_aiplatform):
        """Test risk score calculation handles endpoint errors."""
        from sentinel_dispatch.ai_ml.vertex_ai_scorer import VertexAIRiskScorer
        
        mock_aiplatform.init.return_value = None
        mock_endpoint = Mock()
        mock_endpoint.predict.side_effect = Exception("Endpoint error")
        mock_aiplatform.Endpoint.return_value = mock_endpoint
        
        scorer = VertexAIRiskScorer(
            project_id="test-project",
            endpoint_id="test-endpoint-id"
        )
        
        classification = CallClassificationResult(
            urgency=3,
            incident_type="medical",
            entities={},
            sentiment=0.5,
            key_phrases=[],
            confidence=0.8
        )
        
        input_data = RiskScoreInput(
            call_id="CALL-004",
            call_timestamp="2024-08-08T14:23:45Z",
            caller_location={"lat": 20.8783, "lon": -156.6825},
            call_classification=classification
        )
        
        with pytest.raises(Exception, match="Endpoint error"):
            scorer.calculate_risk_score(input_data)
    
    @patch('sentinel_dispatch.ai_ml.vertex_ai_scorer.aiplatform')
    def test_is_available(self, mock_aiplatform):
        """Test is_available method."""
        from sentinel_dispatch.ai_ml.vertex_ai_scorer import VertexAIRiskScorer
        
        mock_aiplatform.init.return_value = None
        mock_endpoint = Mock()
        mock_aiplatform.Endpoint.return_value = mock_endpoint
        
        scorer = VertexAIRiskScorer(
            project_id="test-project",
            endpoint_id="test-endpoint-id"
        )
        
        assert scorer.is_available() is True

    @patch('sentinel_dispatch.ai_ml.vertex_ai_scorer.aiplatform')
    def test_estimate_weather_risk(self, mock_aiplatform):
        """Test weather risk estimation."""
        from sentinel_dispatch.ai_ml.vertex_ai_scorer import VertexAIRiskScorer
        
        mock_aiplatform.init.return_value = None
        mock_endpoint = Mock()
        mock_aiplatform.Endpoint.return_value = mock_endpoint
        
        scorer = VertexAIRiskScorer(
            project_id="test-project",
            endpoint_id="test-endpoint-id"
        )
        
        features = {
            "wind_speed": 45.0,
            "air_quality_index": 180.0,
            "humidity": 25.0
        }
        
        risk = scorer._estimate_weather_risk(features)
        assert 0.0 <= risk <= 1.0
        assert risk > 0.7  # Should be high
    
    @patch('sentinel_dispatch.ai_ml.vertex_ai_scorer.aiplatform')
    def test_estimate_vulnerability(self, mock_aiplatform):
        """Test vulnerability estimation."""
        from sentinel_dispatch.ai_ml.vertex_ai_scorer import VertexAIRiskScorer
        
        mock_aiplatform.init.return_value = None
        mock_endpoint = Mock()
        mock_aiplatform.Endpoint.return_value = mock_endpoint
        
        scorer = VertexAIRiskScorer(
            project_id="test-project",
            endpoint_id="test-endpoint-id"
        )
        
        features = {
            "age": 75.0,
            "medical_conditions_count": 2.0,
            "mobility_limited": 1.0
        }
        
        vulnerability = scorer._estimate_vulnerability(features)
        assert 0.0 <= vulnerability <= 1.0
        assert vulnerability > 0.7  # Should be high

