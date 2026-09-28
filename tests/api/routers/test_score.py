"""
Tests for Risk Scoring API endpoint.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch

from sentinel_dispatch.api.main import app
from sentinel_dispatch.ai_ml.models import (
    RiskScoreResult,
    RiskFactors,
)

client = TestClient(app)


class TestScoreRiskEndpoint:
    """Test cases for /api/score-risk endpoint."""
    
    @pytest.fixture
    def sample_request(self):
        """Create a sample risk scoring request."""
        return {
            "call_id": "CALL-001",
            "call_timestamp": "2024-08-08T14:23:45Z",
            "caller_location": {"lat": 20.8783, "lon": -156.6825},
            "call_classification": {
                "urgency": 5,
                "incident_type": "fire",
                "entities": {},
                "sentiment": 0.9,
                "key_phrases": [],
                "confidence": 0.95
            },
            "nearest_fire": {
                "location": {"lat": 20.8790, "lon": -156.6830},
                "distance_km": 0.5,
                "frp": 80.0,  # Fire Radiative Power in MW (primary indicator)
                "spread_rate": 12.0  # Fallback if frp not available
            },
            "weather_data": {
                "wind_speed": 25.0,
                "wind_direction": 270.0,
                "humidity": 45.0
            },
            "vulnerability_factors": {
                "age": 75,
                "medical_conditions": ["asthma"],
                "mobility": "limited"
            }
        }
    
    def test_score_risk_success(self, sample_request):
        """Test successful risk scoring."""
        mock_risk_score = RiskScoreResult(
            call_id="CALL-001",
            timestamp="2024-08-08T14:23:50Z",
            risk_score=85.0,
            confidence=0.95,
            factors=RiskFactors(
                call_urgency=0.9,
                fire_proximity=0.8,
                fire_spread_rate=0.7,
                weather_risk=0.6,
                vulnerability=0.8,
                time_since_call=0.3,
            ),
            recommendation="IMMEDIATE_DISPATCH",
            priority=1,
        )
        
        with patch("sentinel_dispatch.api.routers.score.calculate_risk_score", return_value=mock_risk_score):
            response = client.post(
                "/api/score-risk",
                json=sample_request
            )
        
        assert response.status_code == 200
        data = response.json()
        assert data["risk_score"]["call_id"] == "CALL-001"
        assert data["risk_score"]["risk_score"] == 85.0
        assert data["risk_score"]["recommendation"] == "IMMEDIATE_DISPATCH"
        assert data["risk_score"]["priority"] == 1
        assert "timestamp" in data
    
    def test_score_risk_minimal_data(self):
        """Test risk scoring with minimal required data."""
        mock_risk_score = RiskScoreResult(
            call_id="CALL-002",
            timestamp="2024-08-08T14:23:50Z",
            risk_score=45.0,
            confidence=0.8,
            factors=RiskFactors(
                call_urgency=0.5,
                fire_proximity=0.0,
                vulnerability=0.3,
            ),
            recommendation="MODERATE_PRIORITY",
            priority=3,
        )
        
        request = {
            "call_id": "CALL-002",
            "call_timestamp": "2024-08-08T14:23:45Z",
            "caller_location": {"lat": 20.8783, "lon": -156.6825},
            "call_classification": {
                "urgency": 3,
                "incident_type": "medical",
                "entities": {},
                "sentiment": 0.5,
                "key_phrases": [],
                "confidence": 0.8
            }
        }
        
        with patch("sentinel_dispatch.api.routers.score.calculate_risk_score", return_value=mock_risk_score):
            response = client.post(
                "/api/score-risk",
                json=request
            )
        
        assert response.status_code == 200
        data = response.json()
        assert data["risk_score"]["risk_score"] == 45.0
    
    def test_score_risk_missing_required_fields(self):
        """Test risk scoring with missing required fields (should fail validation)."""
        response = client.post(
            "/api/score-risk",
            json={
                "call_id": "CALL-003"
            }
        )
        
        assert response.status_code == 422  # Validation error
    
    def test_score_risk_invalid_location(self):
        """Test risk scoring with invalid location format."""
        request = {
            "call_id": "CALL-004",
            "call_timestamp": "2024-08-08T14:23:45Z",
            "caller_location": {"lat": "invalid", "lon": -156.6825},  # Invalid lat
            "call_classification": {
                "urgency": 3,
                "incident_type": "medical",
                "entities": {},
                "sentiment": 0.5,
                "key_phrases": [],
                "confidence": 0.8
            }
        }
        
        response = client.post(
            "/api/score-risk",
            json=request
        )
        
        assert response.status_code == 422  # Validation error
    
    def test_score_risk_validation_error(self, sample_request):
        """Test handling of validation errors."""
        with patch("sentinel_dispatch.api.routers.score.calculate_risk_score", side_effect=ValueError("Invalid input")):
            response = client.post(
                "/api/score-risk",
                json=sample_request
            )
        
        assert response.status_code == 400
        data = response.json()
        assert "error" in data or "detail" in data
    
    def test_score_risk_internal_error(self, sample_request):
        """Test handling of internal errors."""
        with patch("sentinel_dispatch.api.routers.score.calculate_risk_score", side_effect=Exception("Unexpected error")):
            response = client.post(
                "/api/score-risk",
                json=sample_request
            )
        
        assert response.status_code == 500
        data = response.json()
        assert "error" in data or "detail" in data
    
    def test_score_risk_health_check(self):
        """Test health check endpoint."""
        response = client.get("/api/score-risk/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "risk-scoring"

